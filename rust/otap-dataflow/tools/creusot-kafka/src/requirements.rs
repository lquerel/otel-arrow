// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Independent consumers of the shared runtime contracts.
use crate::*;
use creusot_std::prelude::*;

#[requires(generation@ < cursor.generation@)]
#[ensures(^cursor == *cursor)]
fn stale_track_preserves_cursor(cursor: &mut PartitionCursor, generation: u64) {
    let _ = cursor.prepare_track(generation);
}

#[requires(generation@ > cursor.generation@)]
#[ensures((^cursor).generation == generation && (^cursor).high_water_mark == None && (^cursor).last_lowest == None)]
fn reassignment_discards_old_evidence(cursor: &mut PartitionCursor, generation: u64) {
    let _ = cursor.prepare_track(generation);
}

#[ensures((^cursor).last_lowest == Some(rewind))]
#[ensures(match (^cursor).high_water_mark { None => true, Some(high) => high@ < rewind@ })]
fn rewind_preserves_commit_barrier(cursor: &mut PartitionCursor, rewind: i64) {
    cursor.rewound(rewind);
}

#[requires(current != feedback)]
#[ensures(^phase == *phase)]
fn stale_replay_cannot_start(phase: &mut RetryPhase<u64>, current: u64, feedback: u64) {
    let _ = phase.mark_replaying(current, feedback);
}

#[requires(current != feedback)]
#[ensures(!result)]
fn stale_feedback_cannot_complete(
    phase: RetryPhase<u64>,
    current: u64,
    feedback: u64,
    rewind: i64,
    offset: i64,
) -> bool {
    phase.completes(current, feedback, rewind, offset)
}

#[requires(generation@ < u64::MAX@)]
#[ensures(result@ > generation@)]
fn fresh_before_exhaustion(generation: u64) -> u64 {
    next_delivery_generation(generation)
}

#[requires(feedback@ < assigned@)]
#[ensures(result.deep_model() == OffsetFeedbackAction::DropStale.deep_model())]
fn reassignment_fences_old_feedback(
    feedback: u64,
    tracked: Option<u64>,
    assigned: u64,
) -> OffsetFeedbackAction {
    classify_offset_feedback(feedback, tracked, assigned, true)
}
