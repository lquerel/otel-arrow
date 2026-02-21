// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::balanced::{
    BalancedEnvelope, BalancedGroupState, SendAttemptOutcome, send_balanced_with_policy,
};
use super::broadcast::{BroadcastSubscriberState, InMemoryBroadcastSubscriber};
use super::outcome_tracker::{
    PublishOutcomeTracker, immediate_outcome_future, tracker_outcome_future,
};
use crate::topic::{
    TopicOutcomeInterest, TopicPublishOutcome, TopicPublishReport, TopicPublishResult,
    TopicRuntimeError,
};
use otap_df_config::TopicName;
use otap_df_config::topic::{SubscriptionGroupName, TopicBalancedOnFullPolicy, TopicPolicies};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Default)]
struct BroadcastRegistry {
    next_subscriber_id: u64,
    subscribers: HashMap<u64, Arc<BroadcastSubscriberState>>,
}

pub(super) struct InMemoryTopic<T> {
    name: TopicName,
    policies: TopicPolicies,
    // One queue per balanced consumer group. Group members compete for the same queue.
    balanced_groups: Mutex<HashMap<SubscriptionGroupName, Arc<BalancedGroupState<T>>>>,
    // One shared ring for all broadcast subscribers in this topic.
    broadcast_sender: broadcast::Sender<T>,
    // Registry of active broadcast subscribers and their settlement state.
    broadcast_registry: Mutex<BroadcastRegistry>,
}

impl<T> InMemoryTopic<T> {
    fn lock_broadcast_registry(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, BroadcastRegistry>, TopicRuntimeError> {
        self.broadcast_registry
            .lock()
            .map_err(|_| TopicRuntimeError::Internal {
                message: format!("topic `{}` broadcast-registry lock poisoned", self.name),
            })
    }

    fn snapshot_broadcast_subscribers(
        &self,
    ) -> Result<Vec<Arc<BroadcastSubscriberState>>, TopicRuntimeError> {
        let registry = self.lock_broadcast_registry()?;
        Ok(registry.subscribers.values().cloned().collect())
    }

    fn register_broadcast_subscriber(
        &self,
        state: Arc<BroadcastSubscriberState>,
    ) -> Result<u64, TopicRuntimeError> {
        let mut registry = self.lock_broadcast_registry()?;
        let id = registry.next_subscriber_id;
        registry.next_subscriber_id = registry.next_subscriber_id.wrapping_add(1);
        let previous = registry.subscribers.insert(id, state);
        debug_assert!(previous.is_none());
        Ok(id)
    }

    pub(super) fn unregister_broadcast_subscriber(
        &self,
        subscriber_id: u64,
    ) -> Result<Option<Arc<BroadcastSubscriberState>>, TopicRuntimeError> {
        let mut registry = self.lock_broadcast_registry()?;
        Ok(registry.subscribers.remove(&subscriber_id))
    }
}

impl<T> InMemoryTopic<T>
where
    T: Clone,
{
    /// Builds a topic runtime instance with:
    /// - no balanced groups yet
    /// - a pre-created Tokio broadcast ring sized from policy
    pub(super) fn new(name: TopicName, policies: TopicPolicies) -> Self {
        let (broadcast_sender, _unused_receiver) =
            broadcast::channel(policies.broadcast_subscriber_queue_capacity);
        Self {
            name,
            policies,
            balanced_groups: Mutex::new(HashMap::new()),
            broadcast_sender,
            broadcast_registry: Mutex::new(BroadcastRegistry::default()),
        }
    }

    pub(super) fn name(&self) -> &TopicName {
        &self.name
    }

    pub(super) fn policies(&self) -> &TopicPolicies {
        &self.policies
    }

    pub(super) fn balanced_on_full(&self) -> TopicBalancedOnFullPolicy {
        self.policies.balanced_on_full.clone()
    }

    /// Returns the balanced-group map lock guard.
    ///
    /// Each entry in this map represents one shared queue used by all consumers
    /// in the same balanced subscription group.
    pub(super) fn lock_balanced_groups(
        &self,
    ) -> Result<
        std::sync::MutexGuard<'_, HashMap<SubscriptionGroupName, Arc<BalancedGroupState<T>>>>,
        TopicRuntimeError,
    > {
        self.balanced_groups
            .lock()
            .map_err(|_| TopicRuntimeError::Internal {
                message: format!("topic `{}` balanced-group lock poisoned", self.name),
            })
    }

    /// Publishes one payload to both destination families:
    /// - balanced groups (one enqueue attempt per group queue)
    /// - broadcast subscribers (one broadcast send fan-out)
    ///
    /// Returns enqueue-time report plus optional downstream outcome handle.
    pub(super) async fn publish(
        &self,
        payload: T,
        balanced_on_full: &TopicBalancedOnFullPolicy,
        outcome_interest: TopicOutcomeInterest,
    ) -> Result<TopicPublishResult, TopicRuntimeError>
    where
        T: Clone + Send + 'static,
    {
        // Snapshot current destinations up front so publish is lock-free while sending.
        let balanced_senders: Vec<flume::Sender<BalancedEnvelope<T>>> = {
            let groups = self.lock_balanced_groups()?;
            groups.values().map(|group| group.sender.clone()).collect()
        };
        // Snapshot currently attached broadcast subscribers.
        let broadcast_subscribers = self.snapshot_broadcast_subscribers()?;
        let broadcast_receivers = broadcast_subscribers.len();

        let mut report = TopicPublishReport {
            // For balanced mode, this is "number of groups", not number of subscribers.
            attempted_subscribers: balanced_senders.len() + broadcast_receivers,
            delivered_subscribers: 0,
            dropped_subscribers: 0,
        };

        if report.attempted_subscribers == 0 {
            let outcome = if outcome_interest.is_enabled() {
                Some(immediate_outcome_future(TopicPublishOutcome::Ack))
            } else {
                None
            };
            return Ok(TopicPublishResult { report, outcome });
        }

        let (outcome_tracker, mut outcome) = if outcome_interest.is_enabled() {
            let (tracker, receiver) = PublishOutcomeTracker::new(outcome_interest);
            (Some(tracker), Some(tracker_outcome_future(receiver)))
        } else {
            (None, None)
        };

        let mut delivered_balanced = 0usize;
        // Avoid an extra clone by moving the original payload into the final destination.
        let mut payload_slot = Some(payload);
        let total_destinations = report.attempted_subscribers;
        let mut destination_index = 0usize;

        for sender in &balanced_senders {
            let envelope = if destination_index + 1 == total_destinations {
                payload_slot.take().expect(
                    "payload must still exist before delivering to the last destination queue",
                )
            } else {
                payload_slot
                    .as_ref()
                    .expect("payload must exist before reaching the last destination queue")
                    .clone()
            };
            let envelope = BalancedEnvelope {
                payload: envelope,
                outcome_tracker: outcome_tracker.clone(),
            };
            destination_index += 1;

            if let Some(tracker) = &outcome_tracker {
                tracker.reserve_delivery();
            }

            match send_balanced_with_policy(sender, envelope, balanced_on_full).await {
                SendAttemptOutcome::Delivered => {
                    report.delivered_subscribers += 1;
                    delivered_balanced += 1;
                }
                SendAttemptOutcome::Dropped => {
                    report.dropped_subscribers += 1;
                    if let Some(tracker) = &outcome_tracker {
                        tracker.on_publish_drop("balanced queue full: drop_newest");
                    }
                }
                SendAttemptOutcome::Closed => {
                    return Err(TopicRuntimeError::ChannelClosed {
                        topic: self.name.clone(),
                    });
                }
            }
        }

        if broadcast_receivers > 0 {
            for subscriber in &broadcast_subscribers {
                if let Some(tracker) = &outcome_tracker {
                    tracker.reserve_delivery();
                }
                subscriber.enqueue(outcome_tracker.clone())?;
            }

            let msg = if destination_index + 1 == total_destinations {
                payload_slot.take().expect(
                    "payload must still exist before delivering to the last destination queue",
                )
            } else {
                payload_slot
                    .as_ref()
                    .expect("payload must exist before reaching the last destination queue")
                    .clone()
            };
            let delivered = match self.broadcast_sender.send(msg) {
                Ok(receiver_count) => receiver_count,
                Err(_send_error) => 0,
            };
            // Note: this is a send-time snapshot only. Later lag handling on recv
            // (Tokio `Lagged`) is not reflected in this publish report.
            report.delivered_subscribers += delivered;
            report.dropped_subscribers += broadcast_receivers.saturating_sub(delivered);
        }

        if let Some(tracker) = &outcome_tracker {
            // No balanced deliveries were accepted for this publish, and no drop-triggered
            // Nack completed the tracker: resolve as Ack to avoid dangling outcomes.
            if delivered_balanced == 0 {
                tracker.complete_if_idle();
            }
        }

        if outcome_interest.is_enabled() && outcome.is_none() {
            outcome = Some(immediate_outcome_future(TopicPublishOutcome::Ack));
        }

        Ok(TopicPublishResult { report, outcome })
    }

    /// Creates a broadcast subscriber from a fresh receiver cursor in the
    /// shared Tokio broadcast ring.
    pub(super) fn subscribe_broadcast(
        self: &Arc<Self>,
    ) -> Result<InMemoryBroadcastSubscriber<T>, TopicRuntimeError> {
        let state = Arc::new(BroadcastSubscriberState::new(
            self.policies.broadcast_subscriber_queue_capacity,
        ));
        let subscriber_id = self.register_broadcast_subscriber(state.clone())?;

        Ok(InMemoryBroadcastSubscriber {
            topic_name: self.name.clone(),
            receiver: self.broadcast_sender.subscribe(),
            on_lag: self.policies.broadcast_on_lag.clone(),
            topic: Arc::clone(self),
            subscriber_id,
            state,
        })
    }
}
