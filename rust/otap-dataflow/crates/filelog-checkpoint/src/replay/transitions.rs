// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared production progress and reset transitions, independent of codecs and storage.

use crate::{
    CommittedFrontierGuard, FramingResume, LifecycleState, ResetAfterTruncate,
    ResetQuarantineAction, ResetQuarantinedFile, SnapshotRecord, UpdateProgress,
};
#[cfg(creusot)]
use creusot_std::prelude::*;

#[cfg_attr(not(creusot), derive(Debug, PartialEq, Eq))]
pub(super) enum TransitionError {
    StoredState,
    Progress,
    Truncate,
    QuarantineReset,
    KeepFailed,
}

#[cfg_attr(creusot, ensures((result == Ok(())) == (record.lifecycle_state.deep_model() == state.deep_model() && record.file_epoch == epoch)))]
fn require(
    record: &SnapshotRecord,
    state: LifecycleState,
    epoch: u32,
) -> Result<(), TransitionError> {
    if record.lifecycle_state != state || record.file_epoch != epoch {
        return Err(TransitionError::StoredState);
    }
    Ok(())
}

#[cfg_attr(creusot, ensures((result == Ok(())) == progress_allowed(*record, *op)))]
pub(super) fn check_progress(
    record: &SnapshotRecord,
    op: &UpdateProgress,
) -> Result<(), TransitionError> {
    require(record, LifecycleState::Active, op.expected_file_epoch)?;
    let reject = || TransitionError::Progress;
    if !op
        .new_committed_frontier_guard
        .valid_for_offset(op.new_committed_offset)
        || !op
            .new_framing_resume
            .valid_for_offset(op.new_committed_offset)
        || (op.finalize && op.new_framing_resume != FramingResume::Clean)
    {
        return Err(reject());
    }
    if record.committed_offset != op.expected_committed_offset
        || op.new_committed_offset < record.committed_offset
    {
        return Err(reject());
    }
    if op.new_committed_offset == record.committed_offset {
        if op.new_committed_frontier_guard != record.committed_frontier_guard
            || op.new_framing_resume != record.framing_resume
        {
            return Err(reject());
        }
        return Ok(());
    }
    let next_start = match op.new_framing_resume {
        FramingResume::Clean => None,
        FramingResume::Continuation {
            record_start_offset,
            ..
        } => Some(record_start_offset),
    };
    match record.framing_resume {
        FramingResume::Clean => {
            if next_start.is_some_and(|start| start < record.committed_offset) {
                return Err(reject());
            }
        }
        FramingResume::Continuation {
            record_start_offset: start,
            record_end_offset: end,
            next_fragment_index: index,
        } => {
            let same = matches!(op.new_framing_resume,
                FramingResume::Continuation { record_start_offset, record_end_offset, next_fragment_index }
                if record_start_offset == start && record_end_offset == end && next_fragment_index > index);
            if end != 0 && op.new_committed_offset < end {
                if !same {
                    return Err(reject());
                }
            } else if end != 0 {
                if next_start.is_some_and(|start| start < end) {
                    return Err(reject());
                }
            } else if !same && next_start.is_some_and(|start| start < record.committed_offset) {
                return Err(reject());
            }
            // For scan-to-LF state, only the caller can prove the LF boundary.
            // A clean or later continuation is legal after that source proof.
        }
    }
    Ok(())
}

#[cfg_attr(creusot, ensures(immutable_evidence(*record, ^record)))]
#[cfg_attr(creusot, ensures((^record).file_epoch == record.file_epoch && (^record).fingerprint@ == record.fingerprint@))]
#[cfg_attr(creusot, ensures((^record).quarantine_evidence == record.quarantine_evidence))]
#[cfg_attr(creusot, ensures((^record).committed_offset == op.new_committed_offset &&
    (^record).committed_frontier_guard.deep_model() == op.new_committed_frontier_guard.deep_model() &&
    (^record).framing_resume.deep_model() == op.new_framing_resume.deep_model() &&
    (^record).last_seen_time_unix_nano == op.new_last_seen_time_unix_nano))]
#[cfg_attr(creusot, ensures((^record).lifecycle_state.deep_model() == if op.finalize {
    LifecycleState::RotatedFinalized.deep_model() } else { record.lifecycle_state.deep_model() }))]
pub(super) fn apply_progress_fields(record: &mut SnapshotRecord, op: &UpdateProgress) {
    record.committed_offset = op.new_committed_offset;
    record.committed_frontier_guard = op.new_committed_frontier_guard;
    record.framing_resume = op.new_framing_resume;
    record.last_seen_time_unix_nano = op.new_last_seen_time_unix_nano;
    if op.finalize {
        record.lifecycle_state = LifecycleState::RotatedFinalized;
    }
}

#[cfg_attr(creusot, ensures((result == Ok(())) == truncate_allowed(*record, op)))]
#[cfg_attr(creusot, ensures(result != Ok(()) ==> ^record == *record))]
#[cfg_attr(creusot, ensures(immutable_evidence(*record, ^record)))]
#[cfg_attr(creusot, ensures(result == Ok(()) ==> (^record).file_epoch@ == record.file_epoch@ + 1 &&
    (^record).committed_offset@ == 0 && (^record).committed_frontier_guard.well_formed(0u64) &&
    (^record).framing_resume.deep_model() == FramingResume::Clean.deep_model() &&
    (^record).fingerprint@ == op.new_fingerprint@ &&
    (^record).last_seen_time_unix_nano == op.reset_time_unix_nano &&
    (^record).lifecycle_state.deep_model() == record.lifecycle_state.deep_model() &&
    (^record).quarantine_evidence == record.quarantine_evidence))]
pub(super) fn reset_after_truncate(
    record: &mut SnapshotRecord,
    op: ResetAfterTruncate,
) -> Result<(), TransitionError> {
    require(record, LifecycleState::Active, op.expected_active_epoch)?;
    if record.file_epoch.checked_add(1) != Some(op.resulting_epoch)
        || op.new_committed_offset != 0
        || op.new_framing_resume != FramingResume::Clean
        || op.reason_code != 1
    {
        return Err(TransitionError::Truncate);
    }
    record.file_epoch = op.resulting_epoch;
    record.committed_offset = 0;
    record.committed_frontier_guard = CommittedFrontierGuard::empty();
    record.framing_resume = FramingResume::Clean;
    record.fingerprint = op.new_fingerprint;
    record.last_seen_time_unix_nano = op.reset_time_unix_nano;
    Ok(())
}

#[cfg_attr(creusot, ensures((result == Ok(())) == quarantine_reset_allowed(*record, op)))]
#[cfg_attr(creusot, ensures((result != Ok(())) || op.action.deep_model() == ResetQuarantineAction::KeepFailed.deep_model() ==> ^record == *record))]
#[cfg_attr(creusot, ensures(immutable_evidence(*record, ^record)))]
#[cfg_attr(creusot, ensures((result == Ok(())) && op.action.deep_model() != ResetQuarantineAction::KeepFailed.deep_model() ==>
    (^record).file_epoch@ == record.file_epoch@ + 1 &&
    (^record).lifecycle_state.deep_model() == LifecycleState::Active.deep_model() && (^record).quarantine_evidence == None &&
    (^record).committed_offset == op.resulting_offset &&
    (^record).committed_frontier_guard.deep_model() == op.new_committed_frontier_guard.deep_model() &&
    (^record).framing_resume.deep_model() == FramingResume::Clean.deep_model() &&
    (^record).fingerprint@ == op.new_fingerprint@ &&
    (^record).last_seen_time_unix_nano == op.action_time_unix_nano))]
pub(super) fn reset_quarantined(
    record: &mut SnapshotRecord,
    op: ResetQuarantinedFile,
) -> Result<(), TransitionError> {
    require(
        record,
        LifecycleState::Quarantined,
        op.expected_quarantine_epoch,
    )?;
    if op.action == ResetQuarantineAction::KeepFailed {
        if op.resulting_epoch != record.file_epoch
            || op.resulting_offset != record.committed_offset
            || op.new_committed_frontier_guard != record.committed_frontier_guard
            || op.new_framing_resume != record.framing_resume
            || op.new_fingerprint != record.fingerprint
        {
            return Err(TransitionError::KeepFailed);
        }
        return Ok(());
    }
    if record.file_epoch.checked_add(1) != Some(op.resulting_epoch)
        || op.new_framing_resume != FramingResume::Clean
        || (op.action == ResetQuarantineAction::ResetToBeginning && op.resulting_offset != 0)
    {
        return Err(TransitionError::QuarantineReset);
    }
    record.lifecycle_state = LifecycleState::Active;
    record.quarantine_evidence = None;
    record.file_epoch = op.resulting_epoch;
    record.committed_offset = op.resulting_offset;
    record.committed_frontier_guard = op.new_committed_frontier_guard;
    record.framing_resume = op.new_framing_resume;
    record.fingerprint = op.new_fingerprint;
    record.last_seen_time_unix_nano = op.action_time_unix_nano;
    Ok(())
}

// Independent declarative requirements from the durable checkpoint format.
#[cfg(creusot)]
#[logic(open(crate))]
pub(crate) fn progress_allowed(record: SnapshotRecord, op: UpdateProgress) -> bool {
    pearlite! {
        record.lifecycle_state.deep_model() == LifecycleState::Active.deep_model() && record.file_epoch == op.expected_file_epoch &&
        record.committed_offset == op.expected_committed_offset &&
        record.committed_offset@ <= op.new_committed_offset@ &&
        op.new_committed_frontier_guard.well_formed(op.new_committed_offset) &&
        op.new_framing_resume.well_formed(op.new_committed_offset) &&
        (!op.finalize || op.new_framing_resume.deep_model() == FramingResume::Clean.deep_model()) &&
        if op.new_committed_offset == record.committed_offset {
            op.new_committed_frontier_guard.deep_model() == record.committed_frontier_guard.deep_model() &&
            op.new_framing_resume.deep_model() == record.framing_resume.deep_model()
        } else { follows(record.framing_resume, record.committed_offset, op.new_framing_resume, op.new_committed_offset) }
    }
}

#[cfg(creusot)]
#[logic(open(crate))]
pub(crate) fn follows(
    old: FramingResume,
    old_offset: u64,
    new: FramingResume,
    new_offset: u64,
) -> bool {
    pearlite! {
    match (old, new) {
        (FramingResume::Clean, FramingResume::Clean) => true,
        (FramingResume::Clean, FramingResume::Continuation { record_start_offset, .. }) => record_start_offset@ >= old_offset@,
        (FramingResume::Continuation { record_end_offset, .. }, FramingResume::Clean) =>
            record_end_offset@ == 0 || new_offset@ >= record_end_offset@,
        (FramingResume::Continuation { record_start_offset: a, record_end_offset: b, next_fragment_index: c },
         FramingResume::Continuation { record_start_offset: x, record_end_offset: y, next_fragment_index: z }) => {
            if b@ != 0 && new_offset@ < b@ { a == x && b == y && z@ > c@ }
            else if b@ != 0 { x@ >= b@ }
            else { (a == x && b == y && z@ > c@) || x@ >= old_offset@ }
        }
    }
    }
}

#[cfg(creusot)]
#[logic(open(crate))]
pub(crate) fn immutable_evidence(before: SnapshotRecord, after: SnapshotRecord) -> bool {
    pearlite! {
        before.file_id == after.file_id && before.ignored_header_bytes == after.ignored_header_bytes &&
        before.locator == after.locator && before.framing_profile_version == after.framing_profile_version &&
        before.framing_profile_digest == after.framing_profile_digest && before.advisory_path == after.advisory_path
    }
}

#[cfg(creusot)]
#[logic(open(crate))]
pub(crate) fn truncate_allowed(record: SnapshotRecord, op: ResetAfterTruncate) -> bool {
    pearlite! {
        record.lifecycle_state.deep_model() == LifecycleState::Active.deep_model() && record.file_epoch == op.expected_active_epoch &&
        record.file_epoch@ + 1 == op.resulting_epoch@ && op.new_committed_offset@ == 0 &&
        op.new_framing_resume.deep_model() == FramingResume::Clean.deep_model() && op.reason_code@ == 1
    }
}

#[cfg(creusot)]
#[logic(open(crate))]
pub(crate) fn quarantine_reset_allowed(record: SnapshotRecord, op: ResetQuarantinedFile) -> bool {
    pearlite! {
        record.lifecycle_state.deep_model() == LifecycleState::Quarantined.deep_model() && record.file_epoch == op.expected_quarantine_epoch &&
        match op.action {
            ResetQuarantineAction::KeepFailed =>
                op.resulting_epoch == record.file_epoch && op.resulting_offset == record.committed_offset &&
                op.new_committed_frontier_guard.deep_model() == record.committed_frontier_guard.deep_model() &&
                op.new_framing_resume.deep_model() == record.framing_resume.deep_model() && op.new_fingerprint@ == record.fingerprint@,
            ResetQuarantineAction::ResetToBeginning => record.file_epoch@ + 1 == op.resulting_epoch@ &&
                op.resulting_offset@ == 0 && op.new_framing_resume.deep_model() == FramingResume::Clean.deep_model(),
            ResetQuarantineAction::ResetToEnd => record.file_epoch@ + 1 == op.resulting_epoch@ &&
                op.new_framing_resume.deep_model() == FramingResume::Clean.deep_model(),
        }
    }
}
