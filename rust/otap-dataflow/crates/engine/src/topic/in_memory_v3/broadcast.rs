// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::ring::{DisruptorRing, RingRead};
use crate::topic::{TopicDelivery, TopicRuntimeError};
use otap_df_config::TopicName;
use otap_df_config::topic::TopicBroadcastOnLagPolicy;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Broadcast plane backed by one shared ring with one cursor per subscriber.
pub(super) struct BroadcastPlane<T> {
    topic_name: TopicName,
    ring: Arc<DisruptorRing<T>>,
    on_lag: TopicBroadcastOnLagPolicy,
    subscriber_count: AtomicUsize,
}

impl<T> BroadcastPlane<T> {
    pub(super) fn new(
        topic_name: TopicName,
        capacity: usize,
        on_lag: TopicBroadcastOnLagPolicy,
    ) -> Self {
        Self {
            topic_name,
            ring: Arc::new(DisruptorRing::new(capacity)),
            on_lag,
            subscriber_count: AtomicUsize::new(0),
        }
    }

    pub(super) fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(Ordering::Acquire)
    }

    pub(super) fn subscribe(self: &Arc<Self>) -> BroadcastSubscriber<T> {
        _ = self.subscriber_count.fetch_add(1, Ordering::AcqRel);
        BroadcastSubscriber {
            plane: self.clone(),
            next_sequence: self.ring.next_sequence(),
        }
    }
}

impl<T> BroadcastPlane<T>
where
    T: Clone + Send + 'static,
{
    pub(super) fn publish(&self, payload: T) {
        let sequence = self.ring.reserve_next_sequence();
        self.ring.write(sequence, payload);
    }
}

pub(super) struct BroadcastSubscriber<T> {
    plane: Arc<BroadcastPlane<T>>,
    next_sequence: u64,
}

impl<T> Drop for BroadcastSubscriber<T> {
    fn drop(&mut self) {
        _ = self.plane.subscriber_count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<T> BroadcastSubscriber<T>
where
    T: Clone + Send + 'static,
{
    pub(super) async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        loop {
            let wait_for_data = self.plane.ring.notified();
            let published = self.plane.ring.next_sequence();
            if self.next_sequence >= published {
                wait_for_data.await;
                continue;
            }

            let oldest_available = self.plane.ring.oldest_available_sequence();
            if self.next_sequence < oldest_available {
                match self.plane.on_lag {
                    TopicBroadcastOnLagPolicy::DropOldest => {
                        self.next_sequence = oldest_available;
                        continue;
                    }
                    TopicBroadcastOnLagPolicy::Disconnect => {
                        return Err(TopicRuntimeError::ChannelClosed {
                            topic: self.plane.topic_name.clone(),
                        });
                    }
                }
            }

            match self.plane.ring.read(self.next_sequence) {
                RingRead::Available(payload) => {
                    self.next_sequence += 1;
                    return Ok(TopicDelivery::new_without_ack(payload));
                }
                RingRead::Pending => {
                    wait_for_data.await;
                }
                RingRead::Overwritten { observed_sequence } => {
                    // Handle race where overwrite happens between oldest check and slot read.
                    let oldest_available = self.plane.ring.oldest_available_sequence();
                    if self.next_sequence < oldest_available {
                        match self.plane.on_lag {
                            TopicBroadcastOnLagPolicy::DropOldest => {
                                self.next_sequence = oldest_available;
                            }
                            TopicBroadcastOnLagPolicy::Disconnect => {
                                return Err(TopicRuntimeError::ChannelClosed {
                                    topic: self.plane.topic_name.clone(),
                                });
                            }
                        }
                    } else {
                        return Err(TopicRuntimeError::Internal {
                            message: format!(
                                "broadcast ring slot state inconsistent (expected={}, observed={observed_sequence})",
                                self.next_sequence
                            ),
                        });
                    }
                }
            }
        }
    }
}
