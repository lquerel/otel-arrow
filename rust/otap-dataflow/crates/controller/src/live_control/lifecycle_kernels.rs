// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared scalar decisions for controller lifecycle ownership.
//! Collection binding, worker scheduling, readiness observations, and status
//! publication remain in the controller adapters.

#[cfg(creusot)]
use creusot_std::prelude::*;
#[cfg(creusot)]
use creusot_std::prelude::{Clone, PartialEq};

#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
#[cfg_attr(creusot, derive(DeepModel, Clone, Copy, PartialEq, Eq))]
pub enum OperationKind {
    Rollout,
    Shutdown,
}

/// Snapshot of the explicit owners relevant to one logical pipeline.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
#[cfg_attr(creusot, derive(DeepModel, Clone, Copy, PartialEq, Eq))]
pub struct Ownership {
    pub global_shutdown: bool,
    pub engine_operation: bool,
    pub rollout: bool,
    pub shutdown: bool,
    pub reservation: Option<OperationKind>,
}

impl Ownership {
    #[cfg_attr(creusot, ensures(result == (self.global_shutdown || self.engine_operation || self.rollout || self.shutdown || self.reservation != None)))]
    pub fn preempts_recovery(self) -> bool {
        self.global_shutdown
            || self.engine_operation
            || self.rollout
            || self.shutdown
            || self.reservation.is_some()
    }

    #[cfg_attr(creusot, ensures(result == (self.global_shutdown || self.shutdown || self.reservation == Some(OperationKind::Shutdown))))]
    pub fn shutdown_owns_failures(self) -> bool {
        self.global_shutdown || self.shutdown || self.reservation == Some(OperationKind::Shutdown)
    }

    /// Exact existing admission policy; global shutdown is not consulted here.
    #[cfg_attr(creusot, ensures(result == (!self.engine_operation && !self.rollout && !self.shutdown && self.reservation == None)))]
    pub fn permits_pipeline_reservation(self) -> bool {
        !self.engine_operation && !self.rollout && !self.shutdown && self.reservation.is_none()
    }

    #[cfg_attr(creusot, ensures(result == ((self.global_shutdown || self.engine_operation || self.rollout || self.shutdown || self.reservation != None) && !(self.global_shutdown || self.shutdown || self.reservation == Some(OperationKind::Shutdown)))))]
    pub fn defer_cancelled_failure(self) -> bool {
        self.preempts_recovery() && !self.shutdown_owns_failures()
    }
}

/// Exact existing engine admission policy, whose missing shutdown fence is
/// evaluated separately as an unresolved policy requirement.
#[cfg_attr(creusot, ensures(result == (!engine_operation && !any_reservation)))]
pub fn permits_engine_operation(engine_operation: bool, any_reservation: bool) -> bool {
    !engine_operation && !any_reservation
}

/// Equality is computed by the adapter over its actual operation identifiers.
#[cfg_attr(creusot, ensures(result == ((!active && !requested) || (active && requested && equal))))]
pub fn engine_operation_authorized(active: bool, requested: bool, equal: bool) -> bool {
    (!active && !requested) || (active && requested && equal)
}

#[cfg_attr(creusot, ensures(result == (current == Some(expected))))]
pub fn owns_token(current: Option<u64>, expected: u64) -> bool {
    current == Some(expected)
}

/// Fields changed by the real recovery identity transitions under the mutex.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
#[cfg_attr(creusot, derive(DeepModel, Clone, Copy, PartialEq, Eq))]
pub struct RecoveryCursor {
    pub serving: u64,
    pub worker: Option<u64>,
    pub candidate: Option<u64>,
    pub cancelled: bool,
    pub restarts: usize,
}

impl RecoveryCursor {
    #[cfg_attr(creusot, ensures(result == (self.worker == Some(worker) && !self.cancelled && !preempted)))]
    pub fn may_prepare(self, worker: u64, preempted: bool) -> bool {
        owns_token(self.worker, worker) && !self.cancelled && !preempted
    }

    #[cfg_attr(creusot, ensures(result == (self.worker == Some(worker) && !self.cancelled && !preempted && self.candidate == Some(target) && self.restarts@ < usize::MAX@ && attempt@ == self.restarts@ + 1)))]
    #[cfg_attr(creusot, ensures(if result { (^self).restarts == attempt && (^self).serving == self.serving && (^self).worker == self.worker && (^self).candidate == self.candidate && (^self).cancelled == self.cancelled } else { ^self == *self }))]
    pub fn commit_launch(
        &mut self,
        worker: u64,
        target: u64,
        attempt: usize,
        preempted: bool,
    ) -> bool {
        if !self.may_prepare(worker, preempted)
            || self.candidate != Some(target)
            || self.restarts.checked_add(1) != Some(attempt)
        {
            return false;
        }
        self.restarts = attempt;
        true
    }

    /// Readiness and the target-to-worker binding are supplied by the worker's
    /// ordered call path. The current guard independently checks liveness.
    #[cfg_attr(creusot, ensures(result == (self.worker == Some(worker) && !self.cancelled && !preempted && target_active)))]
    #[cfg_attr(creusot, ensures(if result { (^self).serving == target && (^self).worker == None && (^self).candidate == None && (^self).restarts == self.restarts && (^self).cancelled == self.cancelled } else { ^self == *self }))]
    pub fn promote(
        &mut self,
        worker: u64,
        target: u64,
        preempted: bool,
        target_active: bool,
    ) -> bool {
        if !self.may_prepare(worker, preempted) || !target_active {
            return false;
        }
        self.serving = target;
        self.worker = None;
        self.candidate = None;
        true
    }

    #[cfg_attr(creusot, ensures(result == (self.worker == Some(worker))))]
    #[cfg_attr(creusot, ensures(if result { (^self).candidate == None && (^self).worker == self.worker && (^self).serving == self.serving && (^self).restarts == self.restarts && (^self).cancelled == self.cancelled } else { ^self == *self }))]
    pub fn clear_candidate(&mut self, worker: u64) -> bool {
        if !owns_token(self.worker, worker) {
            return false;
        }
        self.candidate = None;
        true
    }

    #[cfg_attr(creusot, ensures(result == (self.worker == Some(worker))))]
    #[cfg_attr(creusot, ensures(if result { (^self).candidate == None && (^self).worker == None && (^self).serving == self.serving && (^self).restarts == self.restarts && (^self).cancelled == self.cancelled } else { ^self == *self }))]
    pub fn release_worker(&mut self, worker: u64) -> bool {
        if !owns_token(self.worker, worker) {
            return false;
        }
        self.worker = None;
        self.candidate = None;
        true
    }
}

/// The completion predicate accounts for both the cache and authoritative
/// active-record scan, plus outstanding lifecycle owners and coordinators.
#[cfg_attr(creusot, ensures(result == (active_count@ > 0 || active_record || rollout || shutdown || engine_operation || reservation || coordinators@ > 0)))]
pub fn lifecycle_work_pending(
    active_count: usize,
    active_record: bool,
    rollout: bool,
    shutdown: bool,
    engine_operation: bool,
    reservation: bool,
    coordinators: usize,
) -> bool {
    active_count > 0
        || active_record
        || rollout
        || shutdown
        || engine_operation
        || reservation
        || coordinators > 0
}
