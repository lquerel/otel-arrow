// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
use crate::*;

fn cursor() -> RecoveryCursor {
    RecoveryCursor {
        serving: 0,
        worker: Some(7),
        candidate: Some(1),
        cancelled: false,
        restarts: 0,
    }
}

/// Scenario: Every explicit ownership kind and the global shutdown latch preempt recovery.
/// Guarantees: Recovery is fenced while rollout failures remain deferable and shutdown-owned failures do not.
#[test]
fn explicit_owner_and_shutdown_have_distinct_failure_policies() {
    for reservation in [
        None,
        Some(OperationKind::Rollout),
        Some(OperationKind::Shutdown),
    ] {
        for mask in 0..16 {
            let owner = Ownership {
                global_shutdown: mask & 1 != 0,
                engine_operation: mask & 2 != 0,
                rollout: mask & 4 != 0,
                shutdown: mask & 8 != 0,
                reservation,
            };
            assert_eq!(
                owner.preempts_recovery(),
                mask != 0 || reservation.is_some()
            );
            assert_eq!(
                owner.shutdown_owns_failures(),
                mask & 9 != 0 || reservation == Some(OperationKind::Shutdown)
            );
            assert_eq!(
                owner.defer_cancelled_failure(),
                owner.preempts_recovery() && !owner.shutdown_owns_failures()
            );
        }
    }
}

/// Scenario: Cancelled, preempted, stale, and mismatched attempts reach the launch transition.
/// Guarantees: Rejected launches preserve all scalar state and only an exact next attempt spends the restart budget.
#[test]
fn launch_checks_identity_candidate_and_exact_ordinal() {
    let original = cursor();
    for (worker, target, attempt, preempted) in [
        (8, 1, 1, false),
        (7, 2, 1, false),
        (7, 1, 2, false),
        (7, 1, 1, true),
    ] {
        let mut state = original;
        assert!(!state.commit_launch(worker, target, attempt, preempted));
        assert_eq!(state, original);
    }
    let mut state = original;
    assert!(state.commit_launch(7, 1, 1, false));
    assert_eq!(state.restarts, 1);
    state.restarts = usize::MAX;
    let before = state;
    assert!(!state.commit_launch(7, 1, 0, false));
    assert_eq!(state, before);
}

/// Scenario: A cancelled old worker attempts promotion and cleanup after replacement ownership is installed.
/// Guarantees: Old tokens leave the replacement's serving identity and candidate untouched.
#[test]
fn stale_workers_cannot_promote_or_release_new_owner() {
    let mut state = cursor();
    state.worker = Some(8);
    let before = state;
    assert!(!state.promote(7, 1, false, true));
    assert!(!state.clear_candidate(7));
    assert!(!state.release_worker(7));
    assert_eq!(state, before);
    assert!(state.release_worker(8));
    assert_eq!(state.worker, None);
    assert_eq!(state.candidate, None);
}

/// Scenario: Promotion observes dead, preempted, and live replacement candidates.
/// Guarantees: Only the current live candidate under unpreempted ownership changes serving state and releases its worker token.
#[test]
fn promotion_requires_current_worker_and_liveness() {
    for (preempted, active) in [(true, true), (false, false)] {
        let mut state = cursor();
        let before = state;
        assert!(!state.promote(7, 1, preempted, active));
        assert_eq!(state, before);
    }
    let mut state = cursor();
    assert!(state.promote(7, 1, false, true));
    assert_eq!(state.serving, 1);
    assert_eq!(state.worker, None);
    assert_eq!(state.candidate, None);
}

/// Scenario: A zero cached active count coexists with an active record or outstanding lifecycle work.
/// Guarantees: Each authoritative source independently prevents a false completed-shutdown decision.
#[test]
fn completion_checks_every_lifecycle_source() {
    assert!(!lifecycle_work_pending(
        0, false, false, false, false, false, 0
    ));
    for mask in 1..128 {
        assert!(lifecycle_work_pending(
            (mask & 1) as usize,
            mask & 2 != 0,
            mask & 4 != 0,
            mask & 8 != 0,
            mask & 16 != 0,
            mask & 32 != 0,
            (mask & 64) as usize
        ));
    }
}
