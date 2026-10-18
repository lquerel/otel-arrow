// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use crate::{transitions, *};

fn guard(offset: u64) -> CommittedFrontierGuard {
    if offset == 0 {
        CommittedFrontierGuard::empty()
    } else {
        CommittedFrontierGuard {
            window_len: offset.min(64) as u16,
            digest: [9; 32],
        }
    }
}

fn record(
    epoch: u32,
    offset: u64,
    resume: FramingResume,
    lifecycle: LifecycleState,
) -> SnapshotRecord {
    SnapshotRecord {
        file_id: FileId([1; 16]),
        file_epoch: epoch,
        committed_offset: offset,
        committed_frontier_guard: guard(offset),
        fingerprint: vec![1, 2, 3],
        ignored_header_bytes: 0,
        locator: Locator::PosixDevIno { dev: 1, ino: 2 },
        framing_profile_version: 1,
        framing_profile_digest: [3; 32],
        framing_resume: resume,
        lifecycle_state: lifecycle,
        quarantine_evidence: (lifecycle == LifecycleState::Quarantined).then_some(
            QuarantineEvidence {
                reason_code: 1,
                observed_size: offset,
                quarantine_epoch: epoch,
                quarantine_time_unix_nano: 5,
            },
        ),
        last_seen_time_unix_nano: 10,
        advisory_path: AdvisoryPath {
            kind: AdvisoryPathKind::Unavailable,
            truncated: false,
            full_path_len: 0,
            stored_path_bytes: vec![],
            full_path_digest: [0; 32],
        },
    }
}

fn update(record: &SnapshotRecord, offset: u64, resume: FramingResume) -> UpdateProgress {
    UpdateProgress {
        file_id: record.file_id,
        expected_committed_offset: record.committed_offset,
        expected_file_epoch: record.file_epoch,
        new_committed_offset: offset,
        new_committed_frontier_guard: guard(offset),
        new_framing_resume: resume,
        new_last_seen_time_unix_nano: 20,
        finalize: false,
    }
}

fn truncate(record: &SnapshotRecord, next_epoch: u32) -> ResetAfterTruncate {
    ResetAfterTruncate {
        file_id: record.file_id,
        expected_active_epoch: record.file_epoch,
        observed_truncated_size: 0,
        resulting_epoch: next_epoch,
        new_committed_offset: 0,
        new_framing_resume: FramingResume::Clean,
        new_fingerprint: vec![42],
        reset_time_unix_nano: 30,
        reason_code: 1,
    }
}

/// Scenario: progress arrives inside a known record, followed by truncation and delayed old-epoch work.
/// Guarantees: continuation cannot close early, accepted progress preserves its epoch, and reset rejects stale work.
#[test]
fn continuation_and_reset_epoch_isolation() {
    let resume = FramingResume::Continuation {
        record_start_offset: 80,
        record_end_offset: 150,
        next_fragment_index: 3,
    };
    let mut record = record(7, 100, resume, LifecycleState::Active);
    let before = record.clone();
    let mut progress = update(&record, 120, FramingResume::Clean);
    assert!(transitions::check_progress(&record, &progress).is_err());
    assert_eq!(record, before);
    progress.new_framing_resume = FramingResume::Continuation {
        record_start_offset: 80,
        record_end_offset: 150,
        next_fragment_index: 4,
    };
    assert!(transitions::check_progress(&record, &progress).is_ok());
    transitions::apply_progress_fields(&mut record, &progress);
    assert_eq!((record.file_epoch, record.committed_offset), (7, 120));
    let reset = truncate(&record, 8);
    transitions::reset_after_truncate(&mut record, reset).expect("valid reset");
    assert_eq!((record.file_epoch, record.committed_offset), (8, 0));
    assert_eq!(record.fingerprint, vec![42]);
    assert!(transitions::check_progress(&record, &progress).is_err());
}

/// Scenario: the final epoch increment succeeds and the next reset attempts to wrap.
/// Guarantees: exhausted epochs reject without changing any record field.
#[test]
fn epoch_exhaustion_preserves_record() {
    let mut record = record(
        u32::MAX - 1,
        u64::MAX,
        FramingResume::Clean,
        LifecycleState::Active,
    );
    let reset = truncate(&record, u32::MAX);
    transitions::reset_after_truncate(&mut record, reset).expect("last valid epoch");
    let before = record.clone();
    let reset = truncate(&record, 0);
    assert!(transitions::reset_after_truncate(&mut record, reset).is_err());
    assert_eq!(record, before);
}

/// Scenario: keep_failed carries a different audit time and then attempts individual evidence mutations.
/// Guarantees: valid keep_failed changes nothing, and each attempted operational mutation is rejected unchanged.
#[test]
fn keep_failed_preserves_every_field() {
    let mut record = record(7, 100, FramingResume::Clean, LifecycleState::Quarantined);
    let op = ResetQuarantinedFile {
        file_id: record.file_id,
        expected_quarantine_epoch: 7,
        action: ResetQuarantineAction::KeepFailed,
        resulting_epoch: 7,
        resulting_offset: 100,
        new_committed_frontier_guard: record.committed_frontier_guard,
        new_framing_resume: record.framing_resume,
        new_fingerprint: record.fingerprint.clone(),
        action_time_unix_nano: u64::MAX,
        namespace_id: "test".into(),
        audit_reason: "keep".into(),
    };
    let before = record.clone();
    transitions::reset_quarantined(&mut record, op.clone()).expect("unchanged keep_failed");
    assert_eq!(record, before);
    for field in 0..5 {
        let mut bad = op.clone();
        match field {
            0 => bad.resulting_epoch += 1,
            1 => bad.resulting_offset += 1,
            2 => bad.new_committed_frontier_guard.digest[0] ^= 1,
            3 => bad.new_fingerprint.push(9),
            _ => {
                bad.new_framing_resume = FramingResume::Continuation {
                    record_start_offset: 1,
                    record_end_offset: 0,
                    next_fragment_index: 1,
                }
            }
        }
        assert!(transitions::reset_quarantined(&mut record, bad).is_err());
        assert_eq!(record, before);
    }
}

/// Scenario: a zero-delta update changes evidence, then valid zero-delta finalization is followed by more progress.
/// Guarantees: unchanged offsets preserve evidence and finalized files reject subsequent ordinary progress.
#[test]
fn zero_delta_and_finalization() {
    let mut record = record(7, 100, FramingResume::Clean, LifecycleState::Active);
    let mut op = update(&record, 100, FramingResume::Clean);
    op.new_committed_frontier_guard.digest[0] ^= 1;
    assert!(transitions::check_progress(&record, &op).is_err());
    op.new_committed_frontier_guard = record.committed_frontier_guard;
    op.finalize = true;
    assert!(transitions::check_progress(&record, &op).is_ok());
    transitions::apply_progress_fields(&mut record, &op);
    assert_eq!(record.lifecycle_state, LifecycleState::RotatedFinalized);
    assert!(
        transitions::check_progress(&record, &update(&record, 110, FramingResume::Clean)).is_err()
    );
}
