// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::outcome_tracker::PublishOutcomeTracker;
use super::topic_state::InMemoryTopic;
use crate::topic::{
    DeliveryAckHandler, TopicDelivery, TopicOutcomeInterest, TopicOutcomeNack, TopicRuntimeError,
    TopicSubscriber,
};
use async_trait::async_trait;
use otap_df_config::TopicName;
use otap_df_config::topic::TopicBroadcastOnLagPolicy;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

const BROADCAST_LAG_NACK_REASON: &str = "broadcast subscriber lagged: drop_oldest";
const BROADCAST_SUBSCRIBER_DROPPED_NACK_REASON: &str = "broadcast subscriber dropped";
const BROADCAST_CHANNEL_CLOSED_NACK_REASON: &str = "broadcast channel closed";

/// Per-subscriber settlement queue for broadcast deliveries.
///
/// This mirrors receiver progress in publish order and keeps one entry per
/// broadcast message for that subscriber. Entries carry optional publisher
/// outcome trackers.
pub(super) struct BroadcastSubscriberState {
    pending_trackers: Mutex<VecDeque<Option<Arc<PublishOutcomeTracker>>>>,
    capacity: usize,
}

impl BroadcastSubscriberState {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            pending_trackers: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Enqueues one settlement entry for this subscriber.
    ///
    /// When queue capacity is exceeded, the oldest pending entry is dropped and
    /// nacked to reflect `broadcast_on_lag=drop_oldest`.
    pub(super) fn enqueue(
        &self,
        outcome_tracker: Option<Arc<PublishOutcomeTracker>>,
    ) -> Result<(), TopicRuntimeError> {
        let mut pending =
            self.pending_trackers
                .lock()
                .map_err(|_| TopicRuntimeError::Internal {
                    message: "broadcast pending-settlement queue lock poisoned".to_owned(),
                })?;

        if pending.len() >= self.capacity {
            if let Some(Some(dropped_tracker)) = pending.pop_front() {
                dropped_tracker.on_nack(TopicOutcomeNack::transient(BROADCAST_LAG_NACK_REASON));
            }
        }
        pending.push_back(outcome_tracker);
        Ok(())
    }

    /// Pops settlement metadata for the next successfully received payload.
    pub(super) fn pop_next_tracker(
        &self,
        topic_name: &TopicName,
    ) -> Result<Option<Arc<PublishOutcomeTracker>>, TopicRuntimeError> {
        let mut pending =
            self.pending_trackers
                .lock()
                .map_err(|_| TopicRuntimeError::Internal {
                    message: "broadcast pending-settlement queue lock poisoned".to_owned(),
                })?;
        pending
            .pop_front()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: format!(
                    "broadcast pending-settlement entry missing for topic `{topic_name}`"
                ),
            })
    }

    /// Nacks all currently pending settlement entries for this subscriber.
    pub(super) fn nack_all_pending(&self, reason: &'static str) {
        if let Ok(mut pending) = self.pending_trackers.lock() {
            for tracker in pending.drain(..).flatten() {
                tracker.on_nack(TopicOutcomeNack::transient(reason));
            }
        }
    }
}

pub(super) struct InMemoryBroadcastSubscriber<T> {
    pub(super) topic_name: TopicName,
    pub(super) receiver: broadcast::Receiver<T>,
    // Copied from topic policy at subscription time.
    pub(super) on_lag: TopicBroadcastOnLagPolicy,
    pub(super) topic: Arc<InMemoryTopic<T>>,
    pub(super) subscriber_id: u64,
    pub(super) state: Arc<BroadcastSubscriberState>,
}

impl<T> Drop for InMemoryBroadcastSubscriber<T> {
    fn drop(&mut self) {
        let _ignore = self
            .topic
            .unregister_broadcast_subscriber(self.subscriber_id);
        self.state
            .nack_all_pending(BROADCAST_SUBSCRIBER_DROPPED_NACK_REASON);
    }
}

#[async_trait]
impl<T> TopicSubscriber<T> for InMemoryBroadcastSubscriber<T>
where
    T: Clone + Send + 'static,
{
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        let payload = loop {
            match self.receiver.recv().await {
                Ok(payload) => break payload,
                Err(broadcast::error::RecvError::Closed) => {
                    self.state
                        .nack_all_pending(BROADCAST_CHANNEL_CLOSED_NACK_REASON);
                    return Err(TopicRuntimeError::ChannelClosed {
                        topic: self.topic_name.clone(),
                    });
                }
                // Lag is detected on receiver side in Tokio broadcast.
                // For `drop_oldest`, settlement queue overflow already nacks skipped
                // entries, so we keep reading until next payload is available.
                Err(broadcast::error::RecvError::Lagged(_)) => match &self.on_lag {
                    TopicBroadcastOnLagPolicy::DropOldest => continue,
                    TopicBroadcastOnLagPolicy::Disconnect => {
                        self.state
                            .nack_all_pending(BROADCAST_CHANNEL_CLOSED_NACK_REASON);
                        return Err(TopicRuntimeError::ChannelClosed {
                            topic: self.topic_name.clone(),
                        });
                    }
                },
            }
        };

        let outcome_tracker = self.state.pop_next_tracker(&self.topic_name)?;
        Ok(TopicDelivery::new(
            payload,
            Box::new(BroadcastAckHandler { outcome_tracker }),
        ))
    }
}

struct BroadcastAckHandler {
    outcome_tracker: Option<Arc<PublishOutcomeTracker>>,
}

#[async_trait]
impl<T> DeliveryAckHandler<T> for BroadcastAckHandler
where
    T: Send + 'static,
{
    fn outcome_interest(&self) -> TopicOutcomeInterest {
        self.outcome_tracker
            .as_ref()
            .map_or(TopicOutcomeInterest::None, |tracker| tracker.interest())
    }

    async fn ack(self: Box<Self>, _payload: T) -> Result<(), TopicRuntimeError> {
        if let Some(tracker) = &self.outcome_tracker {
            tracker.on_ack();
        }
        Ok(())
    }

    async fn nack(
        self: Box<Self>,
        _payload: T,
        nack: TopicOutcomeNack,
    ) -> Result<(), TopicRuntimeError> {
        if let Some(tracker) = &self.outcome_tracker {
            tracker.on_nack(nack);
        }
        Ok(())
    }
}
