// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Requirement consumers exercise the guarantees supplied by the production kernels.

use crate::{
    FramingResume, ResetAfterTruncate, ResetQuarantineAction, ResetQuarantinedFile, SnapshotRecord,
    UpdateProgress, transitions,
};
use creusot_std::prelude::*;

/// An accepted ordinary progress update preserves its epoch and applies its cursor together.
#[requires(transitions::progress_allowed(*record, *op))]
#[ensures((^record).file_epoch == record.file_epoch)]
#[ensures((^record).committed_offset == op.new_committed_offset)]
#[ensures((^record).committed_offset@ >= record.committed_offset@)]
#[ensures((^record).committed_frontier_guard.deep_model() == op.new_committed_frontier_guard.deep_model())]
#[ensures((^record).framing_resume.deep_model() == op.new_framing_resume.deep_model())]
fn accepted_progress_applies_atomically(record: &mut SnapshotRecord, op: &UpdateProgress) {
    if transitions::check_progress(record, op).is_ok() {
        transitions::apply_progress_fields(record, op);
    }
}

/// A known unfinished record cannot lose its continuation on progress.
#[requires(match record.framing_resume {
    FramingResume::Continuation { record_end_offset, .. } =>
        record_end_offset@ > 0 && op.new_committed_offset@ < record_end_offset@,
    _ => false,
})]
#[requires(op.new_framing_resume == FramingResume::Clean)]
#[ensures(result)]
fn refuses_early_close(record: &SnapshotRecord, op: &UpdateProgress) -> bool {
    transitions::check_progress(record, op).is_err()
}

/// Truncation advances the epoch, invalidating updates authorized against the old epoch.
#[requires(transitions::truncate_allowed(*record, reset))]
#[requires(late.expected_file_epoch == record.file_epoch)]
#[ensures(result)]
fn reset_rejects_late_progress(
    record: &mut SnapshotRecord,
    reset: ResetAfterTruncate,
    late: &UpdateProgress,
) -> bool {
    let _ = transitions::reset_after_truncate(record, reset);
    transitions::check_progress(record, late).is_err()
}

/// Keeping a quarantine unchanged preserves every operational field even on rejection.
#[requires(op.action == ResetQuarantineAction::KeepFailed)]
#[ensures(^record == *record)]
fn keep_failed_is_noop(record: &mut SnapshotRecord, op: ResetQuarantinedFile) {
    let _ = transitions::reset_quarantined(record, op);
}

/// Finalization makes all later ordinary progress ineligible.
#[requires(transitions::progress_allowed(*record, *finalize))]
#[requires(finalize.finalize)]
#[ensures(result)]
fn finalized_progress_is_terminal(
    record: &mut SnapshotRecord,
    finalize: &UpdateProgress,
    later: &UpdateProgress,
) -> bool {
    if transitions::check_progress(record, finalize).is_ok() {
        transitions::apply_progress_fields(record, finalize);
    }
    transitions::check_progress(record, later).is_err()
}

/// Rejected resets cannot modify the record, regardless of their failure reason.
#[ensures(result ==> ^record == *record)]
fn rejected_reset_preserves_record(record: &mut SnapshotRecord, reset: ResetAfterTruncate) -> bool {
    transitions::reset_after_truncate(record, reset).is_err()
}

/// The progress contract guarantees valid continuation and frontier shape on acceptance.
#[requires(transitions::progress_allowed(*record, *op))]
#[ensures((^record).committed_frontier_guard.well_formed((^record).committed_offset))]
#[ensures((^record).framing_resume.well_formed((^record).committed_offset))]
fn accepted_cursor_has_valid_shape(record: &mut SnapshotRecord, op: &UpdateProgress) {
    accepted_progress_applies_atomically(record, op);
}
