// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Adapter conformance: compare actual replay with an index-free reference table.

use super::*;
use crate::{
    AdvisoryPath, CommittedFrontierGuard, FramingEncoding, FramingOnDecodeError,
    MaxLogSizeBehavior, MultilineMode, RemoveFile, encode_checkpoint, encode_transaction,
};

const NAMESPACE: &str = "transaction-conformance";

fn profile() -> FramingProfileParams {
    FramingProfileParams {
        fingerprint_profile_version: 1,
        fingerprint_bytes: 32,
        ignored_header_bytes: 0,
        encoding: FramingEncoding::Utf8,
        on_decode_error: FramingOnDecodeError::PreserveRaw,
        multiline_mode: MultilineMode::Newline,
        max_line_bytes: 1024,
        max_record_bytes: 1024,
        max_log_size_behavior: MaxLogSizeBehavior::Split,
        max_multiline_lines: 100,
        force_flush_period_millis: 0,
    }
}

fn record(id: u8, locator: u8) -> SnapshotRecord {
    SnapshotRecord {
        file_id: FileId::from_bytes([id; 16]),
        file_epoch: 1,
        committed_offset: 0,
        committed_frontier_guard: CommittedFrontierGuard::empty(),
        fingerprint: vec![id],
        ignored_header_bytes: 0,
        locator: Locator::PosixDevIno {
            dev: 1,
            ino: locator.into(),
        },
        framing_profile_version: 1,
        framing_profile_digest: profile().digest().expect("profile"),
        framing_resume: FramingResume::Clean,
        lifecycle_state: LifecycleState::Active,
        quarantine_evidence: None,
        last_seen_time_unix_nano: 7,
        advisory_path: AdvisoryPath::from_unix_bytes(b"/logs/conformance").expect("path"),
    }
}

fn state(records: &[SnapshotRecord], limit: u32) -> ReplayState {
    let config = ReplayConfig::new(limit, 1024 * 1024, &profile()).expect("config");
    let bytes = encode_checkpoint(9, NAMESPACE, records).expect("snapshot");
    ReplayState::from_checkpoint(&bytes, NAMESPACE, config)
        .expect("state")
        .0
}

#[derive(Debug, Clone, Copy)]
enum Action {
    Register(u8, u8),
    Remove(u8, bool),
}

impl Action {
    fn operation(self) -> Operation {
        match self {
            Self::Register(id, locator) => {
                let r = record(id, locator);
                Operation::RegisterFile(RegisterFile {
                    file_id: r.file_id,
                    file_epoch: r.file_epoch,
                    committed_offset: r.committed_offset,
                    committed_frontier_guard: r.committed_frontier_guard,
                    fingerprint: r.fingerprint,
                    ignored_header_bytes: r.ignored_header_bytes,
                    locator: r.locator,
                    framing_profile_version: r.framing_profile_version,
                    framing_profile_digest: r.framing_profile_digest,
                    framing_resume: r.framing_resume,
                    last_seen_time_unix_nano: r.last_seen_time_unix_nano,
                    advisory_path: r.advisory_path,
                })
            }
            Self::Remove(id, administrative) => Operation::RemoveFile(RemoveFile {
                file_id: FileId::from_bytes([id; 16]),
                expected_file_epoch: 1,
                expected_prior_state: LifecycleState::Active,
                removal_reason: 1,
                removal_time_unix_nano: 9,
                administrative,
                namespace_id: administrative.then(|| NAMESPACE.to_owned()),
                audit_reason: administrative.then(|| "conformance".to_owned()),
            }),
        }
    }
}

// This model scans complete records; it has no locator index or tombstone overlay.
// Its small command vocabulary deliberately does not call production transition helpers.
fn reference(
    initial: &[SnapshotRecord],
    actions: &[Action],
    limit: u32,
) -> Option<Vec<SnapshotRecord>> {
    let mut rows = initial.to_vec();
    let mut replacements = Vec::new();
    let mut registrations = Vec::new();
    for &action in actions {
        match action {
            Action::Register(id, locator) => {
                let new = record(id, locator);
                if let Some(old) = rows.iter().find(|r| r.file_id == new.file_id) {
                    if old != &new {
                        return None;
                    }
                } else {
                    if rows.iter().any(|r| {
                        r.lifecycle_state != LifecycleState::RotatedFinalized
                            && r.locator == new.locator
                    }) {
                        return None;
                    }
                    registrations.push(new.file_id);
                    rows.push(new);
                }
            }
            Action::Remove(id, administrative) => {
                let id = FileId::from_bytes([id; 16]);
                if let Some(index) = rows.iter().position(|r| r.file_id == id) {
                    let old = &rows[index];
                    if old.lifecycle_state != LifecycleState::Active || old.file_epoch != 1 {
                        return None;
                    }
                    if !administrative {
                        replacements.push((id, old.locator));
                    }
                    let _ = rows.remove(index);
                } else if !administrative {
                    return None;
                }
            }
        }
    }
    if rows.len() > limit as usize {
        return None;
    }
    for (removed, locator) in replacements {
        if !rows.iter().any(|r| {
            r.locator == locator
                && r.lifecycle_state != LifecycleState::RotatedFinalized
                && r.file_id != removed
                && registrations.contains(&r.file_id)
                && !initial.iter().any(|old| old.file_id == r.file_id)
        }) {
            return None;
        }
    }
    Some(rows)
}

fn assert_index(state: &ReplayState) {
    let mut expected = HashMap::new();
    for (id, record) in &state.records {
        assert_eq!(*id, record.file_id);
        if record.lifecycle_state != LifecycleState::RotatedFinalized {
            assert!(
                expected.insert(record.locator, *id).is_none(),
                "duplicate live locator"
            );
        }
    }
    assert_eq!(
        state.live_locators, expected,
        "index must match records in both directions"
    );
}

fn contents(state: &ReplayState) -> Vec<SnapshotRecord> {
    let mut rows: Vec<_> = state.records.values().cloned().collect();
    rows.sort_by_key(|r| r.file_id);
    rows
}

/// Scenario: All command sequences up to length three run over active, quarantined, finalized, and full initial tables.
/// Guarantees: Ordered replacements, tombstones, admission, idempotency, rollback, and the private locator index match an independent record-scanning model.
#[test]
fn short_transactions_conform_to_record_scanning_model() {
    let a = record(1, 1);
    let mut finalized = a.clone();
    finalized.lifecycle_state = LifecycleState::RotatedFinalized;
    let mut quarantined = a.clone();
    quarantined.lifecycle_state = LifecycleState::Quarantined;
    quarantined.quarantine_evidence = Some(QuarantineEvidence {
        reason_code: 1,
        observed_size: 10,
        quarantine_epoch: 1,
        quarantine_time_unix_nano: 3,
    });
    let initial_tables = [
        vec![],
        vec![a.clone()],
        vec![quarantined],
        vec![finalized],
        vec![a, record(2, 2)],
    ];
    let commands = [
        Action::Register(1, 1),
        Action::Register(1, 2),
        Action::Register(2, 1),
        Action::Register(2, 2),
        Action::Register(3, 1),
        Action::Remove(1, true),
        Action::Remove(2, true),
        Action::Remove(3, true),
        Action::Remove(1, false),
        Action::Remove(2, false),
    ];
    let mut cases = 0;
    for initial in &initial_tables {
        for limit in [initial.len().max(1) as u32, 3] {
            for width in 1..=3 {
                for mut number in 0..commands.len().pow(width) {
                    let mut actions = Vec::new();
                    for _ in 0..width {
                        actions.push(commands[number % commands.len()]);
                        number /= commands.len();
                    }
                    let mut actual = state(initial, limit);
                    let before = contents(&actual);
                    let owners = actual.live_locators.clone();
                    let bytes = encode_transaction(&Transaction {
                        sequence: 1,
                        operations: actions.iter().map(|a| a.operation()).collect(),
                    })
                    .expect("transaction");
                    let result = actual.replay_next(&bytes);
                    match reference(initial, &actions, limit) {
                        Some(mut expected) => {
                            assert!(result.is_ok(), "actions={actions:?}, error={result:?}");
                            expected.sort_by_key(|r| r.file_id);
                            assert_eq!(contents(&actual), expected, "actions={actions:?}");
                            assert_eq!(actual.last_sequence, 1);
                        }
                        None => {
                            assert!(result.is_err(), "actions={actions:?}");
                            assert_eq!(contents(&actual), before, "actions={actions:?}");
                            assert_eq!(actual.live_locators, owners, "actions={actions:?}");
                            assert_eq!(actual.last_sequence, 0);
                        }
                    }
                    assert_eq!(actual.generation, 9);
                    assert_index(&actual);
                    cases += 1;
                }
            }
        }
    }
    assert_eq!(cases, 11_100);
}

/// Scenario: A valid prefix precedes a namespace failure, and another request arrives after sequence exhaustion.
/// Guarantees: Late audit rejection and exhausted sequence preserve every record, claim, and replay sequence.
#[test]
fn late_namespace_and_sequence_failures_preserve_authoritative_state() {
    let a = record(1, 1);
    let mut actual = state(std::slice::from_ref(&a), 2);
    let mut invalid = Action::Remove(1, true).operation();
    if let Operation::RemoveFile(op) = &mut invalid {
        op.namespace_id = Some("wrong".into());
    }
    let bytes = encode_transaction(&Transaction {
        sequence: 1,
        operations: vec![Action::Register(2, 2).operation(), invalid],
    })
    .expect("transaction");
    assert!(matches!(
        actual.replay_next(&bytes),
        Err(ReplayError::NamespaceMismatch { .. })
    ));
    assert_eq!(contents(&actual), vec![a.clone()]);
    assert_eq!(actual.last_sequence, 0);
    assert_index(&actual);
    actual.last_sequence = u64::MAX;
    assert_eq!(
        actual.replay_next(&bytes),
        Err(ReplayError::SequenceExhausted)
    );
    assert_eq!(contents(&actual), vec![a]);
    assert_eq!(actual.last_sequence, u64::MAX);
    assert_index(&actual);
}
