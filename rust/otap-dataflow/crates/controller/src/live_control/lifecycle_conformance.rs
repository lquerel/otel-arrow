// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Finite adapter checks over explicit snapshots, including inconsistent cache
//! snapshots. These tests do not claim every snapshot is reachable.
use super::*;

fn set_owners(
    state: &mut ControllerRuntimeState,
    key: &PipelineKey,
    mask: u8,
    kind: Option<PipelineOperationKind>,
) {
    state.global_shutdown_requested = mask & 1 != 0;
    state.active_engine_operation = (mask & 2 != 0).then(|| "engine-owner".into());
    state.active_rollouts.clear();
    state.active_shutdowns.clear();
    state.pipeline_operation_reservations.clear();
    if mask & 4 != 0 {
        let _ = state
            .active_rollouts
            .insert(key.clone(), "rollout-owner".into());
    }
    if mask & 8 != 0 {
        let _ = state
            .active_shutdowns
            .insert(key.clone(), "shutdown-owner".into());
    }
    if let Some(kind) = kind {
        let _ = state.pipeline_operation_reservations.insert(
            key.clone(),
            PipelineOperationReservationState {
                reservation_id: 9,
                kind,
            },
        );
    }
}

fn recovery(
    state: &ControllerRuntimeState,
    worker: u64,
    cancelled: bool,
    restarts: usize,
) -> RuntimeRecoveryState {
    RuntimeRecoveryState {
        serving_generation: 0,
        context_bindings: Arc::clone(&state.latest_context_bindings),
        restart_count: restarts,
        ready_since: Some(Instant::now()),
        worker_id: Some(worker),
        candidate_generation: Some(1),
        cancel_requested: cancelled,
    }
}

fn assert_frame(
    actual: &RuntimeRecoveryState,
    before: &RuntimeRecoveryState,
    serving: u64,
    worker: Option<u64>,
    candidate: Option<u64>,
    restarts: usize,
) {
    assert_eq!(actual.serving_generation, serving);
    assert_eq!(actual.worker_id, worker);
    assert_eq!(actual.candidate_generation, candidate);
    assert_eq!(actual.restart_count, restarts);
    assert_eq!(actual.cancel_requested, before.cancel_requested);
    assert!(Arc::ptr_eq(
        &actual.context_bindings,
        &before.context_bindings
    ));
}

/// Scenario: All 48 ownership snapshots are read from actual pipeline maps, with a different pipeline queried alongside them.
/// Guarantees: Local rollout, shutdown, and reservation owners fence their own recovery, while global and engine owners fence both pipelines.
#[test]
fn ownership_maps_bind_the_shared_policy_to_the_correct_pipeline() {
    let runtime = test_runtime(&empty_engine_config());
    let key = PipelineKey::new("g1".into(), "p1".into());
    let other = PipelineKey::new("g1".into(), "p2".into());
    let mut count = 0;
    for kind in [
        None,
        Some(PipelineOperationKind::Rollout),
        Some(PipelineOperationKind::Shutdown),
    ] {
        for mask in 0..16 {
            let mut state = runtime.state.lock().unwrap();
            set_owners(&mut state, &key, mask, kind);
            assert_eq!(state.recovery_preempted(&key), mask != 0 || kind.is_some());
            assert_eq!(
                state.recovery_in_shutdown_context(&key),
                mask & 9 != 0 || kind == Some(PipelineOperationKind::Shutdown)
            );
            assert_eq!(state.recovery_preempted(&other), mask & 3 != 0);
            assert_eq!(state.recovery_in_shutdown_context(&other), mask & 1 != 0);
            count += 1;
        }
    }
    assert_eq!(count, 48);
}

/// Scenario: 4,608 real launch and promotion calls vary owners, worker identity, cancellation, liveness, restart boundaries, candidate identity, and attempt ordinal.
/// Guarantees: Only authorized calls change their intended fields; rejected calls and sibling-core state retain identity, context bindings, counters, and timestamps.
#[test]
fn recovery_adapters_match_independent_transition_oracles() {
    let runtime = test_runtime(&empty_engine_config());
    let key = PipelineKey::new("g1".into(), "p1".into());
    let target = deployed_key("g1", "p1", 0, 1);
    let _rx =
        register_runtime_instance(&runtime, "g1", "p1", 0, 1, RuntimeInstanceLifecycle::Active);
    let mut count = 0;
    for kind in [
        None,
        Some(PipelineOperationKind::Rollout),
        Some(PipelineOperationKind::Shutdown),
    ] {
        for mask in 0..16 {
            for worker in [7, 8] {
                for cancelled in [false, true] {
                    for active in [false, true] {
                        for restarts in [0, 1, usize::MAX] {
                            let before = {
                                let mut state = runtime.state.lock().unwrap();
                                set_owners(&mut state, &key, mask, kind);
                                state.runtime_instances.get_mut(&target).unwrap().lifecycle =
                                    if active {
                                        RuntimeInstanceLifecycle::Active
                                    } else {
                                        RuntimeInstanceLifecycle::Exited(
                                            RuntimeInstanceExit::Success,
                                        )
                                    };
                                let before = recovery(&state, worker, cancelled, restarts);
                                let sibling = recovery(&state, 99, false, 42);
                                let _ = state.runtime_recoveries.insert((key.clone(), 1), sibling);
                                before
                            };
                            let allowed = worker == 7 && !cancelled && mask == 0 && kind.is_none();
                            for (candidate, attempt) in [
                                (1, restarts.saturating_add(1)),
                                (2, restarts.saturating_add(1)),
                                (1, 0),
                            ] {
                                {
                                    let mut state = runtime.state.lock().unwrap();
                                    let _ = state
                                        .runtime_recoveries
                                        .insert((key.clone(), 0), before.clone());
                                }
                                let accepted = runtime
                                    .commit_runtime_recovery_launch(&key, 0, 7, attempt, candidate);
                                let expected = allowed
                                    && candidate == 1
                                    && restarts.checked_add(1) == Some(attempt);
                                assert_eq!(accepted, expected);
                                let state = runtime.state.lock().unwrap();
                                let actual = &state.runtime_recoveries[&(key.clone(), 0)];
                                assert_frame(
                                    actual,
                                    &before,
                                    0,
                                    Some(worker),
                                    Some(1),
                                    if expected { attempt } else { restarts },
                                );
                                assert_eq!(actual.ready_since, before.ready_since);
                                count += 1;
                            }
                            {
                                let mut state = runtime.state.lock().unwrap();
                                let _ = state
                                    .runtime_recoveries
                                    .insert((key.clone(), 0), before.clone());
                            }
                            let expected = allowed && active;
                            assert_eq!(
                                runtime.accept_runtime_recovery_promotion(&key, 0, 7, &target),
                                Some(expected)
                            );
                            let state = runtime.state.lock().unwrap();
                            let actual = &state.runtime_recoveries[&(key.clone(), 0)];
                            assert_frame(
                                actual,
                                &before,
                                u64::from(expected),
                                if expected { None } else { Some(worker) },
                                if expected { None } else { Some(1) },
                                restarts,
                            );
                            if expected {
                                assert!(actual.ready_since >= before.ready_since);
                            } else {
                                assert_eq!(actual.ready_since, before.ready_since);
                            }
                            let sibling = &state.runtime_recoveries[&(key.clone(), 1)];
                            assert_eq!(sibling.worker_id, Some(99));
                            assert_eq!(sibling.restart_count, 42);
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(count, 4608);
}

/// Scenario: A reservation guard is superseded by a newer token before its destructor runs.
/// Guarantees: Old cleanup preserves the newer shutdown reservation and its recovery fence.
#[test]
fn stale_reservation_guard_cannot_release_new_shutdown_owner() {
    let runtime = test_runtime(&empty_engine_config());
    let key = PipelineKey::new("g1".into(), "p1".into());
    let guard = runtime
        .begin_pipeline_operation_reservation(key.clone(), PipelineOperationKind::Rollout)
        .unwrap();
    {
        let mut state = runtime.state.lock().unwrap();
        let owner = state.pipeline_operation_reservations.get_mut(&key).unwrap();
        owner.reservation_id += 1;
        owner.kind = PipelineOperationKind::Shutdown;
    }
    drop(guard);
    let state = runtime.state.lock().unwrap();
    assert!(state.pipeline_operation_reservations.contains_key(&key));
    assert!(state.recovery_preempted(&key));
    assert!(state.recovery_in_shutdown_context(&key));
}

/// Scenario: Every combination of seven real lifecycle-work sources is present, including an active record with a zero cached count.
/// Guarantees: Each source independently prevents premature completion, and only an empty snapshot reports no pending work.
#[test]
fn completion_adapter_checks_maps_records_and_coordinators() {
    let runtime = test_runtime(&empty_engine_config());
    let key = PipelineKey::new("g1".into(), "p1".into());
    let target = deployed_key("g1", "p1", 0, 1);
    let _rx =
        register_runtime_instance(&runtime, "g1", "p1", 0, 1, RuntimeInstanceLifecycle::Active);
    for mask in 0..128 {
        let mut state = runtime.state.lock().unwrap();
        set_owners(&mut state, &key, 0, None);
        state.active_instances = usize::from(mask & 1 != 0);
        state.runtime_instances.get_mut(&target).unwrap().lifecycle = if mask & 2 != 0 {
            RuntimeInstanceLifecycle::Active
        } else {
            RuntimeInstanceLifecycle::Exited(RuntimeInstanceExit::Success)
        };
        if mask & 4 != 0 {
            let _ = state.active_rollouts.insert(key.clone(), "rollout".into());
        }
        if mask & 8 != 0 {
            let _ = state
                .active_shutdowns
                .insert(key.clone(), "shutdown".into());
        }
        state.active_engine_operation = (mask & 16 != 0).then(|| "engine".into());
        if mask & 32 != 0 {
            let _ = state.pipeline_operation_reservations.insert(
                key.clone(),
                PipelineOperationReservationState {
                    reservation_id: 9,
                    kind: PipelineOperationKind::Shutdown,
                },
            );
        }
        state.global_shutdown_coordinators = usize::from(mask & 64 != 0);
        assert_eq!(state.has_pending_lifecycle_work(), mask != 0);
    }
}
