// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Requirements stated as consumers of the production transition contracts.
use crate::*;
use creusot_std::prelude::*;

#[requires(cursor.worker != Some(stale))]
#[ensures(^cursor == *cursor)]
fn stale_worker_promotion_is_noop(cursor: &mut RecoveryCursor, stale: u64, target: u64) {
    let _ = cursor.promote(stale, target, false, true);
}

#[requires(owner.global_shutdown)]
#[ensures(^cursor == *cursor)]
fn shutdown_fences_recovery(
    owner: Ownership,
    cursor: &mut RecoveryCursor,
    worker: u64,
    target: u64,
) {
    let _ = cursor.promote(worker, target, owner.preempts_recovery(), true);
}

#[requires(owner.reservation != None || owner.engine_operation || owner.rollout || owner.shutdown)]
#[ensures(^cursor == *cursor)]
fn explicit_operation_fences_recovery(
    owner: Ownership,
    cursor: &mut RecoveryCursor,
    worker: u64,
    target: u64,
) {
    let _ = cursor.promote(worker, target, owner.preempts_recovery(), true);
}

#[requires(cursor.cancelled)]
#[ensures(^cursor == *cursor)]
fn cancelled_worker_cannot_spend_restart(
    cursor: &mut RecoveryCursor,
    worker: u64,
    target: u64,
    attempt: usize,
) {
    let _ = cursor.commit_launch(worker, target, attempt, false);
}

#[requires(cursor.candidate != Some(target))]
#[ensures(^cursor == *cursor)]
fn launch_requires_matching_candidate(
    cursor: &mut RecoveryCursor,
    worker: u64,
    target: u64,
    attempt: usize,
) {
    let _ = cursor.commit_launch(worker, target, attempt, false);
}

#[requires(cursor.worker != Some(stale))]
#[ensures(^cursor == *cursor)]
fn stale_cleanup_preserves_new_owner(cursor: &mut RecoveryCursor, stale: u64) {
    let _ = cursor.clear_candidate(stale);
    let _ = cursor.release_worker(stale);
}

#[ensures(^cursor == *cursor)]
fn exited_candidate_cannot_be_promoted(cursor: &mut RecoveryCursor, worker: u64, target: u64) {
    let _ = cursor.promote(worker, target, false, false);
}

#[requires(owner.global_shutdown || owner.shutdown || owner.reservation == Some(OperationKind::Shutdown))]
#[ensures(!result)]
fn shutdown_does_not_defer_recovery(owner: Ownership) -> bool {
    owner.defer_cancelled_failure()
}

#[requires(owner.rollout || owner.shutdown || owner.engine_operation || owner.reservation != None)]
#[ensures(!result)]
fn conflicting_pipeline_reservation_is_denied(owner: Ownership) -> bool {
    owner.permits_pipeline_reservation()
}

#[ensures(!result)]
fn foreign_engine_operation_is_denied() -> bool {
    engine_operation_authorized(true, true, false)
}

#[ensures(!result)]
fn active_instance_prevents_completion(active_count: usize) -> bool {
    !lifecycle_work_pending(active_count, true, false, false, false, false, 0)
}
