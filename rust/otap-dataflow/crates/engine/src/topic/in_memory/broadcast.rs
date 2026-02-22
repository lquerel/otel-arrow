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
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::broadcast;

const BROADCAST_LAG_NACK_REASON: &str = "broadcast subscriber lagged: drop_oldest";
const BROADCAST_SUBSCRIBER_DROPPED_NACK_REASON: &str = "broadcast subscriber dropped";
const BROADCAST_CHANNEL_CLOSED_NACK_REASON: &str = "broadcast channel closed";

#[derive(Clone)]
pub(super) struct BroadcastEnvelope<T> {
    pub(super) sequence: u64,
    pub(super) payload: T,
}

struct TrackedSettlement {
    sequence: u64,
    tracker: Arc<PublishOutcomeTracker>,
}

/// Per-subscriber settlement queue for broadcast deliveries.
///
/// This mirrors receiver progress in publish order and keeps one entry per
/// broadcast message for that subscriber. Entries carry optional publisher
/// outcome trackers.
pub(super) struct BroadcastSubscriberState {
    pending_trackers: Mutex<VecDeque<TrackedSettlement>>,
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
    pub(super) fn enqueue(&self, sequence: u64, outcome_tracker: Arc<PublishOutcomeTracker>) {
        let mut pending = self.pending_trackers.lock();

        if pending.len() >= self.capacity {
            if let Some(dropped) = pending.pop_front() {
                dropped
                    .tracker
                    .on_nack(TopicOutcomeNack::transient(BROADCAST_LAG_NACK_REASON));
            }
        }
        pending.push_back(TrackedSettlement {
            sequence,
            tracker: outcome_tracker,
        });
    }

    /// Resolves settlement metadata for one successfully received payload.
    ///
    /// Tracked entries older than `received_sequence` are considered lag-dropped
    /// for this subscriber and nacked.
    pub(super) fn resolve_tracker_for_sequence(
        &self,
        received_sequence: u64,
        topic_name: &TopicName,
    ) -> Result<Option<Arc<PublishOutcomeTracker>>, TopicRuntimeError> {
        let mut pending = self.pending_trackers.lock();
        while let Some(front) = pending.front() {
            if front.sequence >= received_sequence {
                break;
            }
            let dropped = pending.pop_front().expect("front was present");
            dropped
                .tracker
                .on_nack(TopicOutcomeNack::transient(BROADCAST_LAG_NACK_REASON));
        }

        match pending.front() {
            Some(front) if front.sequence == received_sequence => {
                let settlement = pending.pop_front().expect("front was present");
                Ok(Some(settlement.tracker))
            }
            Some(front) if front.sequence < received_sequence => Err(TopicRuntimeError::Internal {
                message: format!(
                    "broadcast tracked settlement out-of-order for topic `{topic_name}`: front_seq={} recv_seq={received_sequence}",
                    front.sequence
                ),
            }),
            _ => Ok(None),
        }
    }

    /// Nacks all currently pending settlement entries for this subscriber.
    pub(super) fn nack_all_pending(&self, reason: &'static str) {
        let mut pending = self.pending_trackers.lock();
        for tracked in pending.drain(..) {
            tracked.tracker.on_nack(TopicOutcomeNack::transient(reason));
        }
    }
}

pub(super) struct InMemoryBroadcastSubscriber<T> {
    pub(super) topic_name: TopicName,
    pub(super) receiver: broadcast::Receiver<BroadcastEnvelope<T>>,
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

impl<T> InMemoryBroadcastSubscriber<T>
where
    T: Clone + Send + 'static,
{
    pub(super) async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        let envelope = loop {
            match self.receiver.recv().await {
                Ok(envelope) => break envelope,
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

        let outcome_tracker = self
            .state
            .resolve_tracker_for_sequence(envelope.sequence, &self.topic_name)?;
        match outcome_tracker {
            Some(outcome_tracker) => Ok(TopicDelivery::new(
                envelope.payload,
                Box::new(BroadcastAckHandler { outcome_tracker }),
            )),
            None => Ok(TopicDelivery::new_without_ack(envelope.payload)),
        }
    }
}

#[async_trait]
impl<T> TopicSubscriber<T> for InMemoryBroadcastSubscriber<T>
where
    T: Clone + Send + 'static,
{
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        InMemoryBroadcastSubscriber::recv(self).await
    }
}

struct BroadcastAckHandler {
    outcome_tracker: Arc<PublishOutcomeTracker>,
}

#[async_trait]
impl<T> DeliveryAckHandler<T> for BroadcastAckHandler
where
    T: Send + 'static,
{
    fn outcome_interest(&self) -> TopicOutcomeInterest {
        self.outcome_tracker.interest()
    }

    async fn ack(self: Box<Self>, _payload: T) -> Result<(), TopicRuntimeError> {
        self.outcome_tracker.on_ack();
        Ok(())
    }

    async fn nack(
        self: Box<Self>,
        _payload: T,
        nack: TopicOutcomeNack,
    ) -> Result<(), TopicRuntimeError> {
        self.outcome_tracker.on_nack(nack);
        Ok(())
    }
}
