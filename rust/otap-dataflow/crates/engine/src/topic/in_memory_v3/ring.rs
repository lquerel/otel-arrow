// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

const EMPTY_SEQUENCE: u64 = u64::MAX;

struct RingSlotState<T> {
    sequence: u64,
    payload: Option<T>,
}

struct RingSlot<T> {
    state: Mutex<RingSlotState<T>>,
}

impl<T> RingSlot<T> {
    fn new() -> Self {
        Self {
            state: Mutex::new(RingSlotState {
                sequence: EMPTY_SEQUENCE,
                payload: None,
            }),
        }
    }
}

pub(super) enum RingRead<T> {
    Available(T),
    Pending,
    Overwritten { observed_sequence: u64 },
}

/// Fixed-size sequence-indexed ring used by the v3 runtime.
///
/// Producer assigns monotonically increasing sequence numbers and writes payload
/// into `sequence % capacity` slots. Consumers track their own sequence cursors.
pub(super) struct DisruptorRing<T> {
    capacity: u64,
    index_mask: Option<u64>,
    slots: Box<[RingSlot<T>]>,
    next_sequence: AtomicU64,
    notify: Notify,
}

impl<T> DisruptorRing<T> {
    pub(super) fn new(capacity: usize) -> Self {
        debug_assert!(capacity > 0, "ring capacity must be greater than zero");
        let slots = (0..capacity).map(|_| RingSlot::new()).collect::<Vec<_>>();
        let capacity_u64 = u64::try_from(capacity).expect("usize to u64 conversion should succeed");
        let index_mask = if capacity.is_power_of_two() {
            Some(capacity_u64 - 1)
        } else {
            None
        };
        Self {
            capacity: capacity_u64,
            index_mask,
            slots: slots.into_boxed_slice(),
            next_sequence: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    pub(super) fn capacity(&self) -> u64 {
        self.capacity
    }

    pub(super) fn next_sequence(&self) -> u64 {
        self.next_sequence.load(Ordering::Acquire)
    }

    pub(super) fn reserve_next_sequence(&self) -> u64 {
        self.next_sequence.fetch_add(1, Ordering::AcqRel)
    }

    pub(super) fn try_reserve_next_sequence(&self, expected: u64) -> Result<u64, u64> {
        self.next_sequence
            .compare_exchange_weak(expected, expected + 1, Ordering::AcqRel, Ordering::Acquire)
            .map(|reserved| reserved)
    }

    pub(super) fn oldest_available_sequence(&self) -> u64 {
        self.next_sequence().saturating_sub(self.capacity)
    }

    pub(super) fn write(&self, sequence: u64, payload: T) {
        self.write_no_notify(sequence, payload);
        self.notify.notify_waiters();
    }

    pub(super) fn write_no_notify(&self, sequence: u64, payload: T) {
        let slot = self.slot(sequence);
        let mut state = slot.state.lock();
        state.sequence = sequence;
        state.payload = Some(payload);
    }

    pub(super) fn read(&self, sequence: u64) -> RingRead<T>
    where
        T: Clone,
    {
        let slot = self.slot(sequence);
        let state = slot.state.lock();
        let observed_sequence = state.sequence;
        if observed_sequence == sequence {
            match state.payload.as_ref() {
                Some(payload) => RingRead::Available(payload.clone()),
                None => RingRead::Pending,
            }
        } else if observed_sequence == EMPTY_SEQUENCE {
            RingRead::Pending
        } else {
            RingRead::Overwritten { observed_sequence }
        }
    }

    pub(super) fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }

    fn slot(&self, sequence: u64) -> &RingSlot<T> {
        let index = if let Some(mask) = self.index_mask {
            sequence & mask
        } else {
            sequence % self.capacity
        };
        let index = usize::try_from(index).expect("ring index should fit into usize");
        &self.slots[index]
    }
}
