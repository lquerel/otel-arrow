// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::outcome_tracker::PublishOutcomeTracker;
use super::topic_state::InMemoryTopic;
use crate::topic::{
    DeliveryAckHandler, TopicDelivery, TopicOutcomeInterest, TopicOutcomeNack, TopicPublishResult,
    TopicPublisher, TopicRuntimeError, TopicSubscriber,
};
use async_trait::async_trait;
use otap_df_config::TopicName;
use otap_df_config::topic::{SubscriptionGroupName, TopicBalancedOnFullPolicy};
use std::collections::hash_map::Entry;
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct BalancedEnvelope<T> {
    pub(super) payload: T,
    pub(super) outcome_tracker: Option<Arc<PublishOutcomeTracker>>,
}

pub(super) struct BalancedGroupState<T> {
    pub(super) sender: flume::Sender<BalancedEnvelope<T>>,
    // Cloned by all subscribers of the same group; each message is consumed once.
    pub(super) receiver: flume::Receiver<BalancedEnvelope<T>>,
}

impl<T> BalancedGroupState<T> {
    /// Constructs one bounded competing-consumer queue for a balanced group.
    pub(super) fn new(capacity: usize) -> Self {
        let (sender, receiver) = flume::bounded(capacity);
        Self { sender, receiver }
    }
}

pub(super) enum SendAttemptOutcome {
    Delivered,
    Dropped,
    Closed,
}

/// Sends a payload to one balanced queue according to `balanced_on_full`.
///
/// This is intentionally policy-focused and side-effect free outside channel
/// operations so publish logic can aggregate outcomes consistently.
pub(super) async fn send_balanced_with_policy<T>(
    sender: &flume::Sender<T>,
    payload: T,
    balanced_on_full: &TopicBalancedOnFullPolicy,
) -> SendAttemptOutcome {
    match balanced_on_full {
        // Backpressure: await queue space.
        TopicBalancedOnFullPolicy::Block => {
            if sender.send_async(payload).await.is_ok() {
                SendAttemptOutcome::Delivered
            } else {
                SendAttemptOutcome::Closed
            }
        }
        // Drop at enqueue time when queue is full.
        TopicBalancedOnFullPolicy::DropNewest => match sender.try_send(payload) {
            Ok(()) => SendAttemptOutcome::Delivered,
            Err(flume::TrySendError::Full(_)) => SendAttemptOutcome::Dropped,
            Err(flume::TrySendError::Disconnected(_)) => SendAttemptOutcome::Closed,
        },
    }
}

pub(super) struct InMemoryTopicPublisher<T> {
    pub(super) topic: Arc<InMemoryTopic<T>>,
    pub(super) balanced_on_full: TopicBalancedOnFullPolicy,
    pub(super) outcome_interest: TopicOutcomeInterest,
}

impl<T> InMemoryTopicPublisher<T>
where
    T: Clone + Send + 'static,
{
    pub(super) async fn publish(
        &self,
        payload: T,
    ) -> Result<TopicPublishResult, TopicRuntimeError> {
        self.topic
            .publish(payload, &self.balanced_on_full, self.outcome_interest)
            .await
    }
}

#[async_trait]
impl<T> TopicPublisher<T> for InMemoryTopicPublisher<T>
where
    T: Clone + Send + 'static,
{
    async fn publish(&self, payload: T) -> Result<TopicPublishResult, TopicRuntimeError> {
        InMemoryTopicPublisher::publish(self, payload).await
    }
}

pub(super) struct InMemoryBalancedSubscriber<T> {
    pub(super) topic_name: TopicName,
    pub(super) receiver: flume::Receiver<BalancedEnvelope<T>>,
}

impl<T> InMemoryBalancedSubscriber<T>
where
    T: Send + 'static,
{
    pub(super) async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        let envelope =
            self.receiver
                .recv_async()
                .await
                .map_err(|_| TopicRuntimeError::ChannelClosed {
                    topic: self.topic_name.clone(),
                })?;

        match envelope.outcome_tracker {
            Some(outcome_tracker) => Ok(TopicDelivery::new(
                envelope.payload,
                Box::new(BalancedAckHandler { outcome_tracker }),
            )),
            None => Ok(TopicDelivery::new_without_ack(envelope.payload)),
        }
    }
}

#[async_trait]
impl<T> TopicSubscriber<T> for InMemoryBalancedSubscriber<T>
where
    T: Send + 'static,
{
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        InMemoryBalancedSubscriber::recv(self).await
    }
}

struct BalancedAckHandler {
    outcome_tracker: Arc<PublishOutcomeTracker>,
}

#[async_trait]
impl<T> DeliveryAckHandler<T> for BalancedAckHandler
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

impl<T> InMemoryTopic<T>
where
    T: Clone,
{
    /// Creates a balanced subscriber bound to a group queue.
    ///
    /// The queue is lazily created on first subscription for that group and
    /// reused by subsequent subscribers in the same group.
    pub(super) fn subscribe_balanced(
        self: &Arc<Self>,
        group: SubscriptionGroupName,
    ) -> Result<InMemoryBalancedSubscriber<T>, TopicRuntimeError> {
        let group_state = {
            let mut groups = self.lock_balanced_groups()?;
            match groups.entry(group) {
                Entry::Occupied(entry) => entry.get().clone(),
                Entry::Vacant(entry) => {
                    let group_state = entry
                        .insert(Arc::new(BalancedGroupState::new(
                            self.policies().balanced_group_queue_capacity,
                        )))
                        .clone();
                    self.rebuild_balanced_sender_snapshot(&groups);
                    group_state
                }
            }
        };

        Ok(InMemoryBalancedSubscriber {
            topic_name: self.name().clone(),
            receiver: group_state.receiver.clone(),
        })
    }
}
