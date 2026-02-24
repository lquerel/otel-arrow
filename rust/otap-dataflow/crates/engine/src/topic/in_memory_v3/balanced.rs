// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::ring::{DisruptorRing, RingRead};
use crate::topic::{TopicDelivery, TopicRuntimeError};
use otap_df_config::topic::TopicBalancedOnFullPolicy;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;

pub(super) enum SendAttemptOutcome {
    Delivered,
    Dropped,
}

/// One balanced consumer group plane in v3.
///
/// Messages are published into a fixed-size ring and subscribers in the same
/// group compete for the next unread sequence.
pub(super) struct BalancedGroup<T> {
    ring: Arc<DisruptorRing<T>>,
    next_sequence: AtomicU64,
    // Separate notifiers avoid wake-all behavior on balanced hot paths.
    not_empty: Notify,
    not_full: Notify,
}

impl<T> BalancedGroup<T> {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            ring: Arc::new(DisruptorRing::new(capacity)),
            next_sequence: AtomicU64::new(0),
            not_empty: Notify::new(),
            not_full: Notify::new(),
        }
    }
}

impl<T> BalancedGroup<T>
where
    T: Clone + Send + 'static,
{
    pub(super) async fn publish(
        &self,
        payload: T,
        on_full: &TopicBalancedOnFullPolicy,
    ) -> SendAttemptOutcome {
        let mut payload_slot = Some(payload);
        loop {
            let wait_for_space = self.not_full.notified();
            let next_publish = self.ring.next_sequence();
            let next_consume = self.next_sequence.load(Ordering::Acquire);
            let backlog = next_publish.saturating_sub(next_consume);

            if backlog >= self.ring.capacity() {
                match on_full {
                    TopicBalancedOnFullPolicy::DropNewest => return SendAttemptOutcome::Dropped,
                    TopicBalancedOnFullPolicy::Block => {
                        wait_for_space.await;
                        continue;
                    }
                }
            }

            match self.ring.try_reserve_next_sequence(next_publish) {
                Ok(sequence) => {
                    self.ring.write_no_notify(
                        sequence,
                        payload_slot
                            .take()
                            .expect("payload must remain available until publish succeeds"),
                    );
                    self.not_empty.notify_one();
                    return SendAttemptOutcome::Delivered;
                }
                Err(_actual) => continue,
            }
        }
    }

    pub(super) async fn recv(&self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        loop {
            let wait_for_data = self.not_empty.notified();
            let next_sequence = self.next_sequence.load(Ordering::Acquire);
            let next_publish = self.ring.next_sequence();
            if next_sequence >= next_publish {
                wait_for_data.await;
                continue;
            }

            let was_full = next_publish.saturating_sub(next_sequence) == self.ring.capacity();
            match self.ring.read(next_sequence) {
                RingRead::Available(payload) => {
                    if self
                        .next_sequence
                        .compare_exchange(
                            next_sequence,
                            next_sequence + 1,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        if was_full {
                            self.not_full.notify_one();
                        }
                        return Ok(TopicDelivery::new_without_ack(payload));
                    }
                }
                RingRead::Pending => {
                    wait_for_data.await;
                }
                RingRead::Overwritten { observed_sequence } => {
                    let current_next = self.next_sequence.load(Ordering::Acquire);
                    if current_next != next_sequence {
                        // Another consumer already claimed this sequence; retry.
                        continue;
                    }
                    return Err(TopicRuntimeError::Internal {
                        message: format!(
                            "balanced ring slot overwritten unexpectedly (expected={next_sequence}, observed={observed_sequence})"
                        ),
                    });
                }
            }
        }
    }
}
