// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Broker-independent production offset and replay decisions.

#[cfg(creusot)]
use creusot_std::prelude::DeepModel as ClockValue;
#[cfg(creusot)]
use creusot_std::prelude::*;
#[cfg(creusot)]
use creusot_std::prelude::{Clone, PartialEq};
#[cfg(not(creusot))]
pub trait ClockValue {}
#[cfg(not(creusot))]
impl<T> ClockValue for T {}

/// The relationship of one delivery's ownership period to tracked state.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
#[cfg_attr(creusot, derive(DeepModel, Clone, Copy, PartialEq, Eq))]
pub enum GenerationChange {
    Stale,
    Same,
    New,
}

/// Scalar state accompanying the concrete pending-offset set.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
#[cfg_attr(creusot, derive(DeepModel, Clone, Copy, PartialEq, Eq))]
pub struct PartitionCursor {
    pub generation: u64,
    pub high_water_mark: Option<i64>,
    pub last_lowest: Option<i64>,
}

impl PartitionCursor {
    #[cfg_attr(creusot, ensures(result.generation == generation && result.high_water_mark == None && result.last_lowest == None))]
    pub fn new(generation: u64) -> Self {
        Self {
            generation,
            high_water_mark: None,
            last_lowest: None,
        }
    }

    #[cfg_attr(creusot, ensures(match result {
        GenerationChange::Stale => generation@ < self.generation@ && ^self == *self,
        GenerationChange::Same => generation == self.generation && ^self == *self,
        GenerationChange::New => generation@ > self.generation@ &&
            (^self).generation == generation && (^self).high_water_mark == None && (^self).last_lowest == None,
    }))]
    pub fn prepare_track(&mut self, generation: u64) -> GenerationChange {
        if generation < self.generation {
            return GenerationChange::Stale;
        }
        if generation > self.generation {
            self.generation = generation;
            self.high_water_mark = None;
            self.last_lowest = None;
            return GenerationChange::New;
        }
        GenerationChange::Same
    }

    #[cfg_attr(creusot, ensures((^self).generation == self.generation && (^self).high_water_mark == self.high_water_mark))]
    #[cfg_attr(creusot, ensures((^self).last_lowest == Some(match self.last_lowest {
        None => offset, Some(old) => if offset@ < old@ { offset } else { old },
    })))]
    pub fn tracked(&mut self, offset: i64) {
        match self.last_lowest {
            None => self.last_lowest = Some(offset),
            Some(previous) if offset < previous => self.last_lowest = Some(offset),
            _ => {}
        }
    }

    #[cfg_attr(creusot, ensures((^self).generation == self.generation && (^self).last_lowest == new_lowest))]
    #[cfg_attr(creusot, ensures(result == (new_lowest != self.last_lowest)))]
    #[cfg_attr(creusot, ensures((^self).high_water_mark == Some(match self.high_water_mark {
        None => offset, Some(old) => if offset@ > old@ { offset } else { old },
    })))]
    pub fn acknowledged(&mut self, offset: i64, new_lowest: Option<i64>) -> bool {
        match self.high_water_mark {
            None => self.high_water_mark = Some(offset),
            Some(high) if offset > high => self.high_water_mark = Some(offset),
            _ => {}
        }
        let advanced = new_lowest != self.last_lowest;
        self.last_lowest = new_lowest;
        advanced
    }

    #[cfg_attr(creusot, ensures((^self).generation == self.generation && (^self).last_lowest == Some(rewind)))]
    #[cfg_attr(creusot, ensures(match self.high_water_mark {
        None => (^self).high_water_mark == None,
        Some(high) => if rewind@ == i64::MIN@ { (^self).high_water_mark == None }
            else { match (^self).high_water_mark { None => false, Some(cap) => cap@ == (if high@ < rewind@ - 1 { high@ } else { rewind@ - 1 }) } },
    }))]
    pub fn rewound(&mut self, rewind: i64) {
        self.last_lowest = Some(rewind);
        self.high_water_mark = self
            .high_water_mark
            .zip(rewind.checked_sub(1))
            .map(|(high, cap)| high.min(cap));
    }

    #[cfg_attr(creusot, ensures(result == (pending || match self.high_water_mark {
        None => false, Some(high) => offset@ <= high@,
    })))]
    pub fn is_known(&self, pending: bool, offset: i64) -> bool {
        pending || self.high_water_mark.is_some_and(|high| offset <= high)
    }
}

#[cfg_attr(creusot, ensures(result == (tracked@ <= revoked@)))]
pub fn revoke_allowed(tracked: u64, revoked: u64) -> bool {
    tracked <= revoked
}

/// Exact implemented allocation policy, including saturation at exhaustion.
#[cfg_attr(creusot, ensures(result@ == if generation@ < u64::MAX@ { generation@ + 1 } else { u64::MAX@ }))]
pub const fn next_delivery_generation(generation: u64) -> u64 {
    generation.saturating_add(1)
}

#[cfg_attr(creusot, ensures(result@ == if attempts@ < u32::MAX@ { attempts@ + 1 } else { u32::MAX@ }))]
pub const fn next_attempt(attempts: u32) -> u32 {
    attempts.saturating_add(1)
}

/// Ownership classification; delivery-token validity is checked by the caller.
#[cfg_attr(not(creusot), derive(Debug, PartialEq, Eq))]
#[cfg_attr(creusot, derive(DeepModel, PartialEq, Eq))]
pub enum OffsetFeedbackAction {
    Commit,
    DropStale,
    DropLateAck { purge: bool },
}

#[cfg_attr(creusot, ensures(result.deep_model() == (if feedback@ < assigned@ ||
    (match tracked { None => false, Some(generation) => feedback@ < generation@ }) {
        OffsetFeedbackAction::DropStale
    } else if !is_assigned {
        OffsetFeedbackAction::DropLateAck { purge: match tracked { None => false, Some(generation) => generation@ <= feedback@ } }
    } else { OffsetFeedbackAction::Commit }).deep_model()))]
pub fn classify_offset_feedback(
    feedback: u64,
    tracked: Option<u64>,
    assigned: u64,
    is_assigned: bool,
) -> OffsetFeedbackAction {
    let current = tracked.unwrap_or(0).max(assigned);
    if current > 0 && feedback < current {
        return OffsetFeedbackAction::DropStale;
    }
    if !is_assigned {
        let purge = tracked.is_some_and(|generation| generation <= feedback);
        return OffsetFeedbackAction::DropLateAck { purge };
    }
    OffsetFeedbackAction::Commit
}

/// Mutually exclusive stages; production instantiates the opaque clock with Instant.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq))]
#[cfg_attr(creusot, derive(DeepModel, Clone, Copy))]
pub enum RetryPhase<D: Copy + ClockValue> {
    Backoff { deadline: D, paused: bool },
    Due { paused: bool },
    Replaying,
}

impl<D: Copy + ClockValue> RetryPhase<D> {
    #[cfg_attr(creusot, ensures(result == match self { Self::Backoff { paused, .. } | Self::Due { paused } => paused, Self::Replaying => false }))]
    pub const fn paused(self) -> bool {
        match self {
            Self::Backoff { paused, .. } | Self::Due { paused } => paused,
            Self::Replaying => false,
        }
    }

    #[cfg_attr(creusot, ensures(result == match self { Self::Backoff { deadline, .. } => Some(deadline), _ => None }))]
    pub const fn deadline(self) -> Option<D> {
        match self {
            Self::Backoff { deadline, .. } => Some(deadline),
            _ => None,
        }
    }

    #[cfg_attr(creusot, ensures(result == (current == feedback && match *self { Self::Due { .. } => true, _ => false })))]
    #[cfg_attr(creusot, ensures(if result { ^self == Self::Replaying } else { ^self == *self }))]
    pub fn mark_replaying(&mut self, current: u64, feedback: u64) -> bool {
        if current != feedback || !matches!(self, Self::Due { .. }) {
            return false;
        }
        *self = Self::Replaying;
        true
    }

    #[cfg_attr(creusot, ensures(result == (current == feedback && rewind == offset && match self { Self::Replaying => true, _ => false })))]
    pub fn completes(self, current: u64, feedback: u64, rewind: i64, offset: i64) -> bool {
        current == feedback && rewind == offset && matches!(self, Self::Replaying)
    }
}

impl<D: Copy + ClockValue + PartialEq> RetryPhase<D> {
    #[cfg_attr(creusot, ensures(match result {
        None => ^self == *self && !(current == feedback && match *self { Self::Backoff { deadline, .. } => deadline.deep_model() == observed.deep_model(), _ => false }),
        Some(paused) => current == feedback && (match *self { Self::Backoff { deadline, paused: old } => deadline.deep_model() == observed.deep_model() && paused == old, _ => false }) && ^self == Self::Due { paused },
    }))]
    pub fn mark_due(&mut self, current: u64, feedback: u64, observed: D) -> Option<bool> {
        if current != feedback {
            return None;
        }
        let Self::Backoff { deadline, paused } = *self else {
            return None;
        };
        if deadline != observed {
            return None;
        }
        *self = Self::Due { paused };
        Some(paused)
    }
}
