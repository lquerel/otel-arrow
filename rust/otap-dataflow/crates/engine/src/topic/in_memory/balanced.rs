// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::outcome_tracker::PublishOutcomeTracker;
use super::topic_state::InMemoryTopic;
use crate::topic::{
    DeliveryAckHandler, TopicDelivery, TopicOutcomeInterest, TopicOutcomeNack, TopicPublishReport,
    TopicPublishReportMode, TopicPublishResult, TopicPublisher, TopicPublisherRouteMode,
    TopicRuntimeError, TopicSubscriber,
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
    pub(super) report_mode: TopicPublishReportMode,
    pub(super) route_mode: TopicPublisherRouteMode,
    pub(super) frozen_balanced_senders: Option<Arc<Vec<flume::Sender<BalancedEnvelope<T>>>>>,
}

impl<T> InMemoryTopicPublisher<T>
where
    T: Clone + Send + 'static,
{
    fn fast_path_enabled(&self) -> bool {
        matches!(self.outcome_interest, TopicOutcomeInterest::None)
            && matches!(self.report_mode, TopicPublishReportMode::Minimal)
    }

    async fn publish_frozen_balanced_fast(
        &self,
        payload: T,
        senders: &Arc<Vec<flume::Sender<BalancedEnvelope<T>>>>,
    ) -> Result<(), TopicRuntimeError> {
        if senders.is_empty() {
            return Ok(());
        }

        let mut payload_slot = Some(payload);

        for (idx, sender) in senders.iter().enumerate() {
            let msg = if idx + 1 == senders.len() {
                payload_slot
                    .take()
                    .expect("payload must exist before sending to final frozen destination")
            } else {
                payload_slot
                    .as_ref()
                    .expect("payload must exist before final frozen destination")
                    .clone()
            };

            match send_balanced_with_policy(
                sender,
                BalancedEnvelope {
                    payload: msg,
                    outcome_tracker: None,
                },
                &self.balanced_on_full,
            )
            .await
            {
                SendAttemptOutcome::Delivered | SendAttemptOutcome::Dropped => {}
                SendAttemptOutcome::Closed => {
                    return Err(TopicRuntimeError::ChannelClosed {
                        topic: self.topic.name().clone(),
                    });
                }
            }
        }

        Ok(())
    }

    async fn publish_frozen_balanced(
        &self,
        payload: T,
        senders: &Arc<Vec<flume::Sender<BalancedEnvelope<T>>>>,
    ) -> Result<TopicPublishResult, TopicRuntimeError> {
        if senders.is_empty() {
            return Ok(TopicPublishResult {
                report: TopicPublishReport::default(),
                outcome: None,
            });
        }

        let mut payload_slot = Some(payload);
        let mut delivered = 0usize;
        let mut dropped = 0usize;

        for (idx, sender) in senders.iter().enumerate() {
            let msg = if idx + 1 == senders.len() {
                payload_slot
                    .take()
                    .expect("payload must exist before sending to final frozen destination")
            } else {
                payload_slot
                    .as_ref()
                    .expect("payload must exist before final frozen destination")
                    .clone()
            };

            match send_balanced_with_policy(
                sender,
                BalancedEnvelope {
                    payload: msg,
                    outcome_tracker: None,
                },
                &self.balanced_on_full,
            )
            .await
            {
                SendAttemptOutcome::Delivered => delivered += 1,
                SendAttemptOutcome::Dropped => dropped += 1,
                SendAttemptOutcome::Closed => {
                    return Err(TopicRuntimeError::ChannelClosed {
                        topic: self.topic.name().clone(),
                    });
                }
            }
        }

        let report = match self.report_mode {
            TopicPublishReportMode::Full => TopicPublishReport {
                attempted_subscribers: senders.len(),
                delivered_subscribers: delivered,
                dropped_subscribers: dropped,
            },
            TopicPublishReportMode::Minimal => TopicPublishReport {
                dropped_subscribers: usize::from(dropped > 0),
                ..TopicPublishReport::default()
            },
        };

        Ok(TopicPublishResult {
            report,
            outcome: None,
        })
    }

    pub(super) async fn publish_fast(&self, payload: T) -> Result<(), TopicRuntimeError> {
        if !self.fast_path_enabled() {
            return self.publish(payload).await.map(|_| ());
        }

        if matches!(self.route_mode, TopicPublisherRouteMode::FrozenBalancedOnly)
            && let Some(senders) = &self.frozen_balanced_senders
        {
            return self.publish_frozen_balanced_fast(payload, senders).await;
        }

        self.topic
            .publish_fast(payload, &self.balanced_on_full)
            .await
    }

    pub(super) async fn publish(
        &self,
        payload: T,
    ) -> Result<TopicPublishResult, TopicRuntimeError> {
        if matches!(self.route_mode, TopicPublisherRouteMode::FrozenBalancedOnly)
            && matches!(self.outcome_interest, TopicOutcomeInterest::None)
            && let Some(senders) = &self.frozen_balanced_senders
        {
            return self.publish_frozen_balanced(payload, senders).await;
        }

        self.topic
            .publish(
                payload,
                &self.balanced_on_full,
                self.outcome_interest,
                self.report_mode,
            )
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
