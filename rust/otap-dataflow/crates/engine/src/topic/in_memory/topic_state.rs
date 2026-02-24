// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::balanced::{
    BalancedEnvelope, BalancedGroupState, SendAttemptOutcome, send_balanced_with_policy,
};
use super::broadcast::{BroadcastEnvelope, BroadcastSubscriberState, InMemoryBroadcastSubscriber};
use super::outcome_tracker::{
    PublishOutcomeTracker, immediate_outcome_future, tracker_outcome_future,
};
use crate::topic::{
    TopicOutcomeInterest, TopicPublishOutcome, TopicPublishReport, TopicPublishReportMode,
    TopicPublishResult, TopicRuntimeError,
};
use arc_swap::ArcSwap;
use otap_df_config::TopicName;
use otap_df_config::topic::{SubscriptionGroupName, TopicBalancedOnFullPolicy, TopicPolicies};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
    // Cached sender snapshot rebuilt only when the balanced group set changes.
    balanced_sender_snapshot: ArcSwap<Vec<flume::Sender<BalancedEnvelope<T>>>>,
    // One shared ring for all broadcast subscribers in this topic.
    broadcast_sender: broadcast::Sender<BroadcastEnvelope<T>>,
    // Global sequence for broadcast deliveries.
    broadcast_sequence: AtomicU64,
    // Registry of active broadcast subscribers and their settlement state.
    broadcast_registry: Mutex<BroadcastRegistry>,
    // Cached broadcast subscriber settlement-state snapshot rebuilt on subscribe/unsubscribe.
    broadcast_subscriber_snapshot: ArcSwap<Vec<Arc<BroadcastSubscriberState>>>,
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

    fn read_balanced_sender_snapshot(&self) -> Arc<Vec<flume::Sender<BalancedEnvelope<T>>>> {
        self.balanced_sender_snapshot.load_full()
    }

    pub(super) fn balanced_sender_snapshot(&self) -> Arc<Vec<flume::Sender<BalancedEnvelope<T>>>> {
        self.read_balanced_sender_snapshot()
    }

    pub(super) fn rebuild_balanced_sender_snapshot(
        &self,
        groups: &HashMap<SubscriptionGroupName, Arc<BalancedGroupState<T>>>,
    ) {
        let snapshot = Arc::new(
            groups
                .values()
                .map(|group| group.sender.clone())
                .collect::<Vec<_>>(),
        );
        self.balanced_sender_snapshot.store(snapshot);
    }

    fn read_broadcast_subscriber_snapshot(&self) -> Arc<Vec<Arc<BroadcastSubscriberState>>> {
        self.broadcast_subscriber_snapshot.load_full()
    }

    fn rebuild_broadcast_subscriber_snapshot(&self, registry: &BroadcastRegistry) {
        let snapshot = Arc::new(registry.subscribers.values().cloned().collect::<Vec<_>>());
        self.broadcast_subscriber_snapshot.store(snapshot);
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
        self.rebuild_broadcast_subscriber_snapshot(&registry);
        Ok(id)
    }

    pub(super) fn unregister_broadcast_subscriber(
        &self,
        subscriber_id: u64,
    ) -> Result<Option<Arc<BroadcastSubscriberState>>, TopicRuntimeError> {
        let mut registry = self.lock_broadcast_registry()?;
        let removed = registry.subscribers.remove(&subscriber_id);
        self.rebuild_broadcast_subscriber_snapshot(&registry);
        Ok(removed)
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
            balanced_sender_snapshot: ArcSwap::from_pointee(Vec::new()),
            broadcast_sender,
            broadcast_sequence: AtomicU64::new(0),
            broadcast_registry: Mutex::new(BroadcastRegistry::default()),
            broadcast_subscriber_snapshot: ArcSwap::from_pointee(Vec::new()),
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

    fn next_broadcast_sequence(&self) -> u64 {
        self.broadcast_sequence.fetch_add(1, Ordering::Relaxed)
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
        report_mode: TopicPublishReportMode,
    ) -> Result<TopicPublishResult, TopicRuntimeError>
    where
        T: Clone + Send + 'static,
    {
        if !outcome_interest.is_enabled() {
            return self
                .publish_without_outcome(payload, balanced_on_full, report_mode)
                .await;
        }

        self.publish_with_outcome(payload, balanced_on_full, outcome_interest, report_mode)
            .await
    }

    /// Publishes one payload on the low-overhead path used by concrete
    /// in-memory publishers when no outcome/report information is requested.
    pub(super) async fn publish_fast(
        &self,
        payload: T,
        balanced_on_full: &TopicBalancedOnFullPolicy,
    ) -> Result<(), TopicRuntimeError>
    where
        T: Clone + Send + 'static,
    {
        let balanced_senders = self.read_balanced_sender_snapshot();
        let broadcast_receivers = self.broadcast_sender.receiver_count();

        if balanced_senders.is_empty() && broadcast_receivers == 0 {
            return Ok(());
        }

        // Fast path: single balanced destination and no broadcast subscribers.
        if broadcast_receivers == 0 && balanced_senders.len() == 1 {
            match send_balanced_with_policy(
                &balanced_senders[0],
                BalancedEnvelope {
                    payload,
                    outcome_tracker: None,
                },
                balanced_on_full,
            )
            .await
            {
                SendAttemptOutcome::Delivered | SendAttemptOutcome::Dropped => return Ok(()),
                SendAttemptOutcome::Closed => {
                    return Err(TopicRuntimeError::ChannelClosed {
                        topic: self.name.clone(),
                    });
                }
            }
        }

        // Fast path: broadcast-only publish.
        if balanced_senders.is_empty() && broadcast_receivers > 0 {
            let _ = self.broadcast_sender.send(BroadcastEnvelope {
                sequence: self.next_broadcast_sequence(),
                payload,
            });
            return Ok(());
        }

        let mut payload_slot = Some(payload);
        let total_destinations = balanced_senders.len() + usize::from(broadcast_receivers > 0);
        let mut destination_index = 0usize;

        for sender in balanced_senders.iter() {
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
            destination_index += 1;

            match send_balanced_with_policy(
                sender,
                BalancedEnvelope {
                    payload: msg,
                    outcome_tracker: None,
                },
                balanced_on_full,
            )
            .await
            {
                SendAttemptOutcome::Delivered | SendAttemptOutcome::Dropped => {}
                SendAttemptOutcome::Closed => {
                    return Err(TopicRuntimeError::ChannelClosed {
                        topic: self.name.clone(),
                    });
                }
            }
        }

        if broadcast_receivers > 0 {
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
            let _ = self.broadcast_sender.send(BroadcastEnvelope {
                sequence: self.next_broadcast_sequence(),
                payload: msg,
            });
        }

        Ok(())
    }

    async fn publish_without_outcome(
        &self,
        payload: T,
        balanced_on_full: &TopicBalancedOnFullPolicy,
        report_mode: TopicPublishReportMode,
    ) -> Result<TopicPublishResult, TopicRuntimeError>
    where
        T: Clone + Send + 'static,
    {
        let full_report = matches!(report_mode, TopicPublishReportMode::Full);
        let balanced_senders = self.read_balanced_sender_snapshot();
        let broadcast_receivers = self.broadcast_sender.receiver_count();
        let attempted_subscribers = balanced_senders.len() + broadcast_receivers;
        let mut report = if full_report {
            TopicPublishReport {
                attempted_subscribers,
                delivered_subscribers: 0,
                dropped_subscribers: 0,
            }
        } else {
            TopicPublishReport::default()
        };

        if attempted_subscribers == 0 {
            return Ok(TopicPublishResult {
                report,
                outcome: None,
            });
        }

        let mut dropped_any = false;

        // Fast path: single balanced destination and no broadcast subscribers.
        if broadcast_receivers == 0 && balanced_senders.len() == 1 {
            match send_balanced_with_policy(
                &balanced_senders[0],
                BalancedEnvelope {
                    payload,
                    outcome_tracker: None,
                },
                balanced_on_full,
            )
            .await
            {
                SendAttemptOutcome::Delivered => {
                    if full_report {
                        report.delivered_subscribers = 1;
                    }
                }
                SendAttemptOutcome::Dropped => {
                    if full_report {
                        report.dropped_subscribers = 1;
                    } else {
                        dropped_any = true;
                    }
                }
                SendAttemptOutcome::Closed => {
                    return Err(TopicRuntimeError::ChannelClosed {
                        topic: self.name.clone(),
                    });
                }
            }

            if !full_report {
                report.dropped_subscribers = usize::from(dropped_any);
            }

            return Ok(TopicPublishResult {
                report,
                outcome: None,
            });
        }

        // Fast path: broadcast-only publish.
        if balanced_senders.is_empty() && broadcast_receivers > 0 {
            let delivered = match self.broadcast_sender.send(BroadcastEnvelope {
                sequence: self.next_broadcast_sequence(),
                payload,
            }) {
                Ok(receiver_count) => receiver_count,
                Err(_send_error) => 0,
            };
            let dropped = broadcast_receivers.saturating_sub(delivered);
            if full_report {
                report.delivered_subscribers = delivered;
                report.dropped_subscribers = dropped;
            } else if dropped > 0 {
                dropped_any = true;
            }
            if !full_report {
                report.dropped_subscribers = usize::from(dropped_any);
            }
            return Ok(TopicPublishResult {
                report,
                outcome: None,
            });
        }

        let mut payload_slot = Some(payload);
        let total_destinations = balanced_senders.len() + usize::from(broadcast_receivers > 0);
        let mut destination_index = 0usize;

        for sender in balanced_senders.iter() {
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
            destination_index += 1;

            match send_balanced_with_policy(
                sender,
                BalancedEnvelope {
                    payload: msg,
                    outcome_tracker: None,
                },
                balanced_on_full,
            )
            .await
            {
                SendAttemptOutcome::Delivered => {
                    if full_report {
                        report.delivered_subscribers += 1;
                    }
                }
                SendAttemptOutcome::Dropped => {
                    if full_report {
                        report.dropped_subscribers += 1;
                    } else {
                        dropped_any = true;
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
            let sequence = self.next_broadcast_sequence();

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
            let delivered = match self.broadcast_sender.send(BroadcastEnvelope {
                sequence,
                payload: msg,
            }) {
                Ok(receiver_count) => receiver_count,
                Err(_send_error) => 0,
            };
            let dropped = broadcast_receivers.saturating_sub(delivered);
            if full_report {
                report.delivered_subscribers += delivered;
                report.dropped_subscribers += dropped;
            } else if dropped > 0 {
                dropped_any = true;
            }
        }

        if !full_report {
            report.dropped_subscribers = usize::from(dropped_any);
        }

        Ok(TopicPublishResult {
            report,
            outcome: None,
        })
    }

    async fn publish_with_outcome(
        &self,
        payload: T,
        balanced_on_full: &TopicBalancedOnFullPolicy,
        outcome_interest: TopicOutcomeInterest,
        report_mode: TopicPublishReportMode,
    ) -> Result<TopicPublishResult, TopicRuntimeError>
    where
        T: Clone + Send + 'static,
    {
        let full_report = matches!(report_mode, TopicPublishReportMode::Full);
        // Snapshot current destinations up front so publish is lock-free while sending.
        let balanced_senders = self.read_balanced_sender_snapshot();
        // Snapshot currently attached broadcast subscribers.
        let broadcast_subscribers = self.read_broadcast_subscriber_snapshot();
        let broadcast_receivers = broadcast_subscribers.len();

        let attempted_subscribers = balanced_senders.len() + broadcast_receivers;
        let mut report = if full_report {
            TopicPublishReport {
                // For balanced mode, this is "number of groups", not number of subscribers.
                attempted_subscribers,
                delivered_subscribers: 0,
                dropped_subscribers: 0,
            }
        } else {
            TopicPublishReport::default()
        };

        if attempted_subscribers == 0 {
            return Ok(TopicPublishResult {
                report,
                outcome: Some(immediate_outcome_future(TopicPublishOutcome::Ack)),
            });
        }

        let (outcome_tracker, receiver) = PublishOutcomeTracker::new(outcome_interest);
        let outcome = Some(tracker_outcome_future(receiver));

        let mut delivered_balanced = 0usize;
        let mut dropped_any = false;
        // Avoid an extra clone by moving the original payload into the final destination.
        let mut payload_slot = Some(payload);
        let total_destinations = balanced_senders.len() + usize::from(broadcast_receivers > 0);
        let mut destination_index = 0usize;

        for sender in balanced_senders.iter() {
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
                outcome_tracker: Some(outcome_tracker.clone()),
            };
            destination_index += 1;

            outcome_tracker.reserve_delivery();

            match send_balanced_with_policy(sender, envelope, balanced_on_full).await {
                SendAttemptOutcome::Delivered => {
                    if full_report {
                        report.delivered_subscribers += 1;
                    }
                    delivered_balanced += 1;
                }
                SendAttemptOutcome::Dropped => {
                    if full_report {
                        report.dropped_subscribers += 1;
                    } else {
                        dropped_any = true;
                    }
                    outcome_tracker.on_publish_drop("balanced queue full: drop_newest");
                }
                SendAttemptOutcome::Closed => {
                    return Err(TopicRuntimeError::ChannelClosed {
                        topic: self.name.clone(),
                    });
                }
            }
        }

        if broadcast_receivers > 0 {
            let sequence = self.next_broadcast_sequence();
            for subscriber in broadcast_subscribers.iter() {
                outcome_tracker.reserve_delivery();
                subscriber.enqueue(sequence, outcome_tracker.clone());
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
            let delivered = match self.broadcast_sender.send(BroadcastEnvelope {
                sequence,
                payload: msg,
            }) {
                Ok(receiver_count) => receiver_count,
                Err(_send_error) => 0,
            };
            // Note: this is a send-time snapshot only. Later lag handling on recv
            // (Tokio `Lagged`) is not reflected in this publish report.
            let dropped = broadcast_receivers.saturating_sub(delivered);
            if full_report {
                report.delivered_subscribers += delivered;
                report.dropped_subscribers += dropped;
            } else if dropped > 0 {
                dropped_any = true;
            }
        }

        // No balanced deliveries were accepted for this publish, and no drop-triggered
        // Nack completed the tracker: resolve as Ack to avoid dangling outcomes.
        if delivered_balanced == 0 {
            outcome_tracker.complete_if_idle();
        }

        if !full_report {
            report.dropped_subscribers = usize::from(dropped_any);
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
