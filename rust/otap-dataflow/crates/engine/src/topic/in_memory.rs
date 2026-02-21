// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! In-memory runtime topic backend.
//!
//! Design notes:
//! - Balanced subscriptions use one shared queue per topic/group (`flume`).
//! - Broadcast subscriptions use one shared Tokio broadcast ring per topic.
//! - Delivery always returns an ack/nack handle so backend semantics can evolve
//!   to persistent/distributed implementations without changing node code.
//! - This backend intentionally fails fast on unsupported policies through the
//!   runtime capability contract (for example `broadcast_on_lag=disconnect`).

use super::{
    DeliveryAckHandler, TopicDelivery, TopicPublishReport, TopicPublisher, TopicRuntime,
    TopicRuntimeCapabilities, TopicRuntimeError, TopicSubscriber, TopicSubscription,
    validate_topic_policy_support,
};
use async_trait::async_trait;
use otap_df_config::TopicName;
use otap_df_config::topic::{
    SubscriptionGroupName, TopicBalancedOnFullPolicy, TopicBroadcastOnLagPolicy, TopicPolicies,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::broadcast;

#[derive(Default)]
struct InMemoryTopicRuntimeState<T> {
    // Global runtime registry: one in-memory topic instance per topic name.
    topics: HashMap<TopicName, Arc<InMemoryTopic<T>>>,
}

/// In-memory implementation of [`TopicRuntime`].
///
/// This backend is process-local and ephemeral.
pub struct InMemoryTopicRuntime<T> {
    state: RwLock<InMemoryTopicRuntimeState<T>>,
}

impl<T> Default for InMemoryTopicRuntime<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> InMemoryTopicRuntime<T> {
    /// Creates an empty runtime.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: RwLock::new(InMemoryTopicRuntimeState {
                topics: HashMap::new(),
            }),
        }
    }

    /// Returns a read guard over the runtime topic registry.
    ///
    /// Mapping lock poisoning to `TopicRuntimeError` keeps backend failures
    /// explicit at the API boundary.
    fn read_state(
        &self,
    ) -> Result<std::sync::RwLockReadGuard<'_, InMemoryTopicRuntimeState<T>>, TopicRuntimeError>
    {
        self.state.read().map_err(|_| TopicRuntimeError::Internal {
            message: "topic runtime state lock poisoned".to_owned(),
        })
    }

    /// Returns a write guard over the runtime topic registry.
    ///
    /// All topic create operations flow through this lock to keep the in-memory
    /// registry consistent and avoid duplicate insert races.
    fn write_state(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, InMemoryTopicRuntimeState<T>>, TopicRuntimeError>
    {
        self.state.write().map_err(|_| TopicRuntimeError::Internal {
            message: "topic runtime state lock poisoned".to_owned(),
        })
    }
}

#[async_trait]
impl<T> TopicRuntime<T> for InMemoryTopicRuntime<T>
where
    T: Clone + Send + 'static,
{
    fn capabilities(&self) -> TopicRuntimeCapabilities {
        TopicRuntimeCapabilities {
            backend_name: "in_memory_tokio_broadcast",
            // Tokio broadcast naturally supports lag by skipping old entries.
            supports_broadcast_on_lag_drop_oldest: true,
            // "Disconnect lagging subscriber" is not a native behavior in Tokio broadcast;
            // we reject this policy at topic creation time for this backend.
            supports_broadcast_on_lag_disconnect: false,
        }
    }

    async fn create_topic(
        &self,
        topic_name: TopicName,
        policies: TopicPolicies,
    ) -> Result<(), TopicRuntimeError> {
        if policies.balanced_group_queue_capacity == 0 {
            return Err(TopicRuntimeError::InvalidTopicConfig {
                topic: topic_name,
                reason: "balanced_group_queue_capacity must be greater than 0".to_owned(),
            });
        }
        if policies.broadcast_subscriber_queue_capacity == 0 {
            return Err(TopicRuntimeError::InvalidTopicConfig {
                topic: topic_name,
                reason: "broadcast_subscriber_queue_capacity must be greater than 0".to_owned(),
            });
        }
        // Fail fast when config requests semantics unsupported by this backend.
        validate_topic_policy_support(&topic_name, &policies, self.capabilities())?;

        let mut state = self.write_state()?;
        if state.topics.contains_key(&topic_name) {
            return Err(TopicRuntimeError::TopicAlreadyExists { topic: topic_name });
        }

        let previous = state.topics.insert(
            topic_name.clone(),
            Arc::new(InMemoryTopic::new(topic_name, policies)),
        );
        debug_assert!(previous.is_none());
        Ok(())
    }

    async fn publisher(
        &self,
        topic_name: &TopicName,
        balanced_on_full_override: Option<TopicBalancedOnFullPolicy>,
    ) -> Result<Arc<dyn TopicPublisher<T>>, TopicRuntimeError> {
        let state = self.read_state()?;
        let topic = state.topics.get(topic_name).cloned().ok_or_else(|| {
            TopicRuntimeError::TopicNotFound {
                topic: topic_name.clone(),
            }
        })?;

        let balanced_on_full =
            balanced_on_full_override.unwrap_or_else(|| topic.policies.balanced_on_full.clone());

        Ok(Arc::new(InMemoryTopicPublisher {
            topic,
            balanced_on_full,
        }))
    }

    async fn subscribe(
        &self,
        topic_name: &TopicName,
        subscription: TopicSubscription,
    ) -> Result<Box<dyn TopicSubscriber<T>>, TopicRuntimeError> {
        let state = self.read_state()?;
        let topic = state.topics.get(topic_name).cloned().ok_or_else(|| {
            TopicRuntimeError::TopicNotFound {
                topic: topic_name.clone(),
            }
        })?;
        drop(state);

        match subscription {
            TopicSubscription::Broadcast => Ok(Box::new(topic.subscribe_broadcast()?)),
            TopicSubscription::Balanced { group } => Ok(Box::new(topic.subscribe_balanced(group)?)),
        }
    }
}

struct InMemoryTopic<T> {
    name: TopicName,
    policies: TopicPolicies,
    // One queue per balanced consumer group. Group members compete for the same queue.
    balanced_groups: Mutex<HashMap<SubscriptionGroupName, Arc<BalancedGroupState<T>>>>,
    // One shared ring for all broadcast subscribers in this topic.
    broadcast_sender: broadcast::Sender<T>,
}

impl<T> InMemoryTopic<T>
where
    T: Clone,
{
    /// Builds a topic runtime instance with:
    /// - no balanced groups yet
    /// - a pre-created Tokio broadcast ring sized from policy
    fn new(name: TopicName, policies: TopicPolicies) -> Self {
        let (broadcast_sender, _unused_receiver) =
            broadcast::channel(policies.broadcast_subscriber_queue_capacity);
        Self {
            name,
            policies,
            balanced_groups: Mutex::new(HashMap::new()),
            broadcast_sender,
        }
    }

    /// Returns the balanced-group map lock guard.
    ///
    /// Each entry in this map represents one shared queue used by all consumers
    /// in the same balanced subscription group.
    fn lock_balanced_groups(
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
    /// Returns a send-time snapshot report (`TopicPublishReport`).
    async fn publish(
        &self,
        payload: T,
        balanced_on_full: &TopicBalancedOnFullPolicy,
    ) -> Result<TopicPublishReport, TopicRuntimeError>
    where
        T: Clone + Send + 'static,
    {
        // Snapshot current destinations up front so publish is lock-free while sending.
        let balanced_senders: Vec<flume::Sender<T>> = {
            let groups = self.lock_balanced_groups()?;
            groups.values().map(|group| group.sender.clone()).collect()
        };
        // Number of currently attached broadcast subscribers.
        let broadcast_receivers = self.broadcast_sender.receiver_count();

        let mut report = TopicPublishReport {
            // For balanced mode, this is "number of groups", not number of subscribers.
            attempted_subscribers: balanced_senders.len() + broadcast_receivers,
            delivered_subscribers: 0,
            dropped_subscribers: 0,
        };

        if report.attempted_subscribers == 0 {
            return Ok(report);
        }

        // Avoid an extra clone by moving the original payload into the final destination.
        let mut payload_slot = Some(payload);
        let total_destinations = report.attempted_subscribers;
        let mut destination_index = 0usize;

        for sender in &balanced_senders {
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

            match send_balanced_with_policy(sender, msg, balanced_on_full).await {
                SendAttemptOutcome::Delivered => {
                    report.delivered_subscribers += 1;
                }
                SendAttemptOutcome::Dropped => {
                    report.dropped_subscribers += 1;
                }
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
            let delivered = match self.broadcast_sender.send(msg) {
                Ok(receiver_count) => receiver_count,
                Err(_send_error) => 0,
            };
            // Note: this is a send-time snapshot only. Later lag handling on recv
            // (Tokio `Lagged`) is not reflected in this publish report.
            report.delivered_subscribers += delivered;
            report.dropped_subscribers += broadcast_receivers.saturating_sub(delivered);
        }

        Ok(report)
    }

    /// Creates a balanced subscriber bound to a group queue.
    ///
    /// The queue is lazily created on first subscription for that group and
    /// reused by subsequent subscribers in the same group.
    fn subscribe_balanced(
        self: &Arc<Self>,
        group: SubscriptionGroupName,
    ) -> Result<InMemoryBalancedSubscriber<T>, TopicRuntimeError> {
        let group_state = {
            let mut groups = self.lock_balanced_groups()?;
            groups
                .entry(group)
                .or_insert_with(|| {
                    Arc::new(BalancedGroupState::new(
                        self.policies.balanced_group_queue_capacity,
                    ))
                })
                .clone()
        };

        Ok(InMemoryBalancedSubscriber {
            topic_name: self.name.clone(),
            receiver: group_state.receiver.clone(),
        })
    }

    /// Creates a broadcast subscriber from a fresh receiver cursor in the
    /// shared Tokio broadcast ring.
    fn subscribe_broadcast(
        self: &Arc<Self>,
    ) -> Result<InMemoryBroadcastSubscriber<T>, TopicRuntimeError> {
        Ok(InMemoryBroadcastSubscriber {
            topic_name: self.name.clone(),
            receiver: self.broadcast_sender.subscribe(),
            on_lag: self.policies.broadcast_on_lag.clone(),
            retry_queue: Arc::new(Mutex::new(VecDeque::new())),
        })
    }
}

struct BalancedGroupState<T> {
    sender: flume::Sender<T>,
    // Cloned by all subscribers of the same group; each message is consumed once.
    receiver: flume::Receiver<T>,
}

impl<T> BalancedGroupState<T> {
    /// Constructs one bounded competing-consumer queue for a balanced group.
    fn new(capacity: usize) -> Self {
        let (sender, receiver) = flume::bounded(capacity);
        Self { sender, receiver }
    }
}

enum SendAttemptOutcome {
    Delivered,
    Dropped,
    Closed,
}

/// Sends a payload to one balanced queue according to `balanced_on_full`.
///
/// This is intentionally policy-focused and side-effect free outside channel
/// operations so publish logic can aggregate outcomes consistently.
async fn send_balanced_with_policy<T>(
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

struct InMemoryTopicPublisher<T> {
    topic: Arc<InMemoryTopic<T>>,
    balanced_on_full: TopicBalancedOnFullPolicy,
}

#[async_trait]
impl<T> TopicPublisher<T> for InMemoryTopicPublisher<T>
where
    T: Clone + Send + 'static,
{
    async fn publish(&self, payload: T) -> Result<TopicPublishReport, TopicRuntimeError> {
        self.topic.publish(payload, &self.balanced_on_full).await
    }
}

struct InMemoryBalancedSubscriber<T> {
    topic_name: TopicName,
    receiver: flume::Receiver<T>,
}

#[async_trait]
impl<T> TopicSubscriber<T> for InMemoryBalancedSubscriber<T>
where
    T: Send + 'static,
{
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        let payload =
            self.receiver
                .recv_async()
                .await
                .map_err(|_| TopicRuntimeError::ChannelClosed {
                    topic: self.topic_name.clone(),
                })?;

        Ok(TopicDelivery::new(payload, Box::new(BalancedAckHandler {})))
    }
}

struct InMemoryBroadcastSubscriber<T> {
    topic_name: TopicName,
    receiver: broadcast::Receiver<T>,
    // Copied from topic policy at subscription time.
    on_lag: TopicBroadcastOnLagPolicy,
    // Subscriber-local redelivery queue used only for nack().
    retry_queue: Arc<Mutex<VecDeque<T>>>,
}

impl<T> InMemoryBroadcastSubscriber<T> {
    /// Pops one locally nacked payload queued for subscriber-local redelivery.
    fn pop_retry_payload(&self) -> Result<Option<T>, TopicRuntimeError> {
        let mut queue = self
            .retry_queue
            .lock()
            .map_err(|_| TopicRuntimeError::Internal {
                message: "broadcast retry queue lock poisoned".to_owned(),
            })?;
        Ok(queue.pop_front())
    }
}

#[async_trait]
impl<T> TopicSubscriber<T> for InMemoryBroadcastSubscriber<T>
where
    T: Clone + Send + 'static,
{
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        if let Some(payload) = self.pop_retry_payload()? {
            return Ok(TopicDelivery::new(
                payload,
                Box::new(BroadcastAckHandler {
                    retry_queue: self.retry_queue.clone(),
                }),
            ));
        }

        let payload = loop {
            match self.receiver.recv().await {
                Ok(payload) => break payload,
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(TopicRuntimeError::ChannelClosed {
                        topic: self.topic_name.clone(),
                    });
                }
                // Lag is detected on receiver side in Tokio broadcast.
                // For `drop_oldest`, we keep reading until a valid next payload is available.
                Err(broadcast::error::RecvError::Lagged(_)) => match &self.on_lag {
                    TopicBroadcastOnLagPolicy::DropOldest => continue,
                    TopicBroadcastOnLagPolicy::Disconnect => {
                        return Err(TopicRuntimeError::ChannelClosed {
                            topic: self.topic_name.clone(),
                        });
                    }
                },
            }
        };

        Ok(TopicDelivery::new(
            payload,
            Box::new(BroadcastAckHandler {
                retry_queue: self.retry_queue.clone(),
            }),
        ))
    }
}

struct BalancedAckHandler {}

#[async_trait]
impl<T> DeliveryAckHandler<T> for BalancedAckHandler
where
    T: Send + 'static,
{
    async fn ack(self: Box<Self>, _payload: T) -> Result<(), TopicRuntimeError> {
        Ok(())
    }

    async fn nack(self: Box<Self>, _payload: T) -> Result<(), TopicRuntimeError> {
        // Balanced delivery retry/backoff policy is controlled by the publish caller.
        // The topic runtime intentionally avoids implicit requeue on NACK.
        Ok(())
    }
}

struct BroadcastAckHandler<T> {
    retry_queue: Arc<Mutex<VecDeque<T>>>,
}

#[async_trait]
impl<T> DeliveryAckHandler<T> for BroadcastAckHandler<T>
where
    T: Send + 'static,
{
    async fn ack(self: Box<Self>, _payload: T) -> Result<(), TopicRuntimeError> {
        Ok(())
    }

    async fn nack(self: Box<Self>, payload: T) -> Result<(), TopicRuntimeError> {
        // Broadcast nack is modeled as local redelivery for this subscriber only.
        let mut queue = self
            .retry_queue
            .lock()
            .map_err(|_| TopicRuntimeError::Internal {
                message: "broadcast retry queue lock poisoned".to_owned(),
            })?;
        queue.push_back(payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    fn topic_name(raw: &str) -> TopicName {
        TopicName::parse(raw).expect("topic name should be valid")
    }

    fn group_name(raw: &str) -> SubscriptionGroupName {
        SubscriptionGroupName::parse(raw).expect("group name should be valid")
    }

    fn policies(
        balanced_group_queue_capacity: usize,
        balanced_on_full: TopicBalancedOnFullPolicy,
        broadcast_subscriber_queue_capacity: usize,
        broadcast_on_lag: TopicBroadcastOnLagPolicy,
    ) -> TopicPolicies {
        TopicPolicies {
            balanced_group_queue_capacity,
            balanced_on_full,
            broadcast_subscriber_queue_capacity,
            broadcast_on_lag,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_duplicate_topic_creation() {
        let runtime = InMemoryTopicRuntime::<u64>::new();
        let topic = topic_name("raw");
        let policies = policies(
            8,
            TopicBalancedOnFullPolicy::Block,
            8,
            TopicBroadcastOnLagPolicy::DropOldest,
        );

        runtime
            .create_topic(topic.clone(), policies.clone())
            .await
            .expect("first create should succeed");
        let err = runtime
            .create_topic(topic.clone(), policies)
            .await
            .expect_err("second create should fail");
        assert_eq!(err, TopicRuntimeError::TopicAlreadyExists { topic });
    }

    #[tokio::test(flavor = "current_thread")]
    async fn balanced_nack_does_not_implicitly_requeue() {
        let runtime = InMemoryTopicRuntime::<u64>::new();
        let topic = topic_name("raw");
        runtime
            .create_topic(
                topic.clone(),
                policies(
                    8,
                    TopicBalancedOnFullPolicy::Block,
                    8,
                    TopicBroadcastOnLagPolicy::DropOldest,
                ),
            )
            .await
            .expect("topic should be created");

        let publisher = runtime
            .publisher(&topic, None)
            .await
            .expect("publisher should be created");
        let mut subscriber = runtime
            .subscribe(
                &topic,
                TopicSubscription::Balanced {
                    group: group_name("workers"),
                },
            )
            .await
            .expect("balanced subscriber should be created");

        let first_publish = publisher.publish(10).await.expect("publish should succeed");
        assert_eq!(first_publish.delivered_subscribers, 1);
        assert_eq!(first_publish.dropped_subscribers, 0);

        let first_delivery = subscriber.recv().await.expect("message should be received");
        assert_eq!(*first_delivery.payload(), 10);
        first_delivery.nack().await.expect("nack should succeed");

        assert!(
            timeout(Duration::from_millis(50), subscriber.recv())
                .await
                .is_err(),
            "balanced NACK should not implicitly requeue in the topic runtime"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn balanced_group_is_shared_across_subscribers() {
        let runtime = InMemoryTopicRuntime::<u64>::new();
        let topic = topic_name("shared");
        runtime
            .create_topic(
                topic.clone(),
                policies(
                    8,
                    TopicBalancedOnFullPolicy::Block,
                    8,
                    TopicBroadcastOnLagPolicy::DropOldest,
                ),
            )
            .await
            .expect("topic should be created");

        let publisher = runtime
            .publisher(&topic, None)
            .await
            .expect("publisher should be created");
        let mut subscriber_1 = runtime
            .subscribe(
                &topic,
                TopicSubscription::Balanced {
                    group: group_name("workers"),
                },
            )
            .await
            .expect("first balanced subscriber should be created");
        let mut subscriber_2 = runtime
            .subscribe(
                &topic,
                TopicSubscription::Balanced {
                    group: group_name("workers"),
                },
            )
            .await
            .expect("second balanced subscriber should be created");

        let _publish_1 = publisher.publish(1).await.expect("publish should succeed");
        let _publish_2 = publisher.publish(2).await.expect("publish should succeed");

        let delivery_1 = subscriber_1
            .recv()
            .await
            .expect("first recv should succeed");
        let delivery_2 = subscriber_2
            .recv()
            .await
            .expect("second recv should succeed");

        let mut values = vec![*delivery_1.payload(), *delivery_2.payload()];
        values.sort_unstable();
        assert_eq!(values, vec![1, 2]);

        delivery_1.ack().await.expect("ack should succeed");
        delivery_2.ack().await.expect("ack should succeed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn broadcast_delivers_to_all_and_nack_is_subscriber_local() {
        let runtime = InMemoryTopicRuntime::<u64>::new();
        let topic = topic_name("broadcast");
        runtime
            .create_topic(
                topic.clone(),
                policies(
                    8,
                    TopicBalancedOnFullPolicy::Block,
                    8,
                    TopicBroadcastOnLagPolicy::DropOldest,
                ),
            )
            .await
            .expect("topic should be created");

        let publisher = runtime
            .publisher(&topic, None)
            .await
            .expect("publisher should be created");
        let mut subscriber_1 = runtime
            .subscribe(&topic, TopicSubscription::Broadcast)
            .await
            .expect("first broadcast subscriber should be created");
        let mut subscriber_2 = runtime
            .subscribe(&topic, TopicSubscription::Broadcast)
            .await
            .expect("second broadcast subscriber should be created");

        let _publish = publisher.publish(42).await.expect("publish should succeed");

        let delivery_1 = subscriber_1.recv().await.expect("recv should succeed");
        let delivery_2 = subscriber_2.recv().await.expect("recv should succeed");
        assert_eq!(*delivery_1.payload(), 42);
        assert_eq!(*delivery_2.payload(), 42);

        delivery_1.nack().await.expect("nack should succeed");
        delivery_2.ack().await.expect("ack should succeed");

        let redelivery = subscriber_1
            .recv()
            .await
            .expect("nacked message should be re-delivered");
        assert_eq!(*redelivery.payload(), 42);
        redelivery.ack().await.expect("ack should succeed");

        assert!(
            timeout(Duration::from_millis(50), subscriber_2.recv())
                .await
                .is_err(),
            "second subscriber should not receive redelivery triggered by first subscriber nack"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drop_newest_drops_when_balanced_queue_is_full() {
        let runtime = InMemoryTopicRuntime::<u64>::new();
        let topic = topic_name("drop-newest");
        runtime
            .create_topic(
                topic.clone(),
                policies(
                    1,
                    TopicBalancedOnFullPolicy::DropNewest,
                    8,
                    TopicBroadcastOnLagPolicy::DropOldest,
                ),
            )
            .await
            .expect("topic should be created");

        let publisher = runtime
            .publisher(&topic, None)
            .await
            .expect("publisher should be created");
        let mut subscriber = runtime
            .subscribe(
                &topic,
                TopicSubscription::Balanced {
                    group: group_name("workers"),
                },
            )
            .await
            .expect("balanced subscriber should be created");

        let first = publisher
            .publish(1)
            .await
            .expect("first publish should succeed");
        assert_eq!(first.delivered_subscribers, 1);
        assert_eq!(first.dropped_subscribers, 0);

        let second = publisher
            .publish(2)
            .await
            .expect("second publish should succeed");
        assert_eq!(second.delivered_subscribers, 0);
        assert_eq!(second.dropped_subscribers, 1);

        let delivery = subscriber
            .recv()
            .await
            .expect("first value should be delivered");
        assert_eq!(*delivery.payload(), 1);
        delivery.ack().await.expect("ack should succeed");

        assert!(
            timeout(Duration::from_millis(50), subscriber.recv())
                .await
                .is_err(),
            "second value should be dropped when queue is full"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn block_policy_applies_backpressure_until_space_is_available() {
        let runtime = InMemoryTopicRuntime::<u64>::new();
        let topic = topic_name("block");
        runtime
            .create_topic(
                topic.clone(),
                policies(
                    1,
                    TopicBalancedOnFullPolicy::Block,
                    8,
                    TopicBroadcastOnLagPolicy::DropOldest,
                ),
            )
            .await
            .expect("topic should be created");

        let publisher = runtime
            .publisher(&topic, None)
            .await
            .expect("publisher should be created");
        let mut subscriber = runtime
            .subscribe(
                &topic,
                TopicSubscription::Balanced {
                    group: group_name("workers"),
                },
            )
            .await
            .expect("balanced subscriber should be created");

        let _first_publish = publisher
            .publish(1)
            .await
            .expect("first publish should fit into queue");

        let publisher_for_task = Arc::clone(&publisher);
        let publish_task = tokio::spawn(async move { publisher_for_task.publish(2).await });

        sleep(Duration::from_millis(25)).await;
        assert!(
            !publish_task.is_finished(),
            "second publish should block until the queue has space"
        );

        let first = subscriber
            .recv()
            .await
            .expect("first value should be available");
        assert_eq!(*first.payload(), 1);
        first.ack().await.expect("ack should succeed");

        let second_publish = timeout(Duration::from_secs(1), publish_task)
            .await
            .expect("blocked publish should complete after queue is drained")
            .expect("join should succeed")
            .expect("publish should succeed");
        assert_eq!(second_publish.delivered_subscribers, 1);
        assert_eq!(second_publish.dropped_subscribers, 0);

        let second = subscriber
            .recv()
            .await
            .expect("second value should be available");
        assert_eq!(*second.payload(), 2);
        second.ack().await.expect("ack should succeed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn broadcast_drop_oldest_keeps_newest_for_lagging_subscriber() {
        let runtime = InMemoryTopicRuntime::<u64>::new();
        let topic = topic_name("broadcast-drop-oldest");
        runtime
            .create_topic(
                topic.clone(),
                policies(
                    8,
                    TopicBalancedOnFullPolicy::Block,
                    1,
                    TopicBroadcastOnLagPolicy::DropOldest,
                ),
            )
            .await
            .expect("topic should be created");

        let publisher = runtime
            .publisher(&topic, None)
            .await
            .expect("publisher should be created");
        let mut fast_subscriber = runtime
            .subscribe(&topic, TopicSubscription::Broadcast)
            .await
            .expect("fast subscriber should be created");
        let mut slow_subscriber = runtime
            .subscribe(&topic, TopicSubscription::Broadcast)
            .await
            .expect("slow subscriber should be created");

        let _first_publish = publisher
            .publish(1)
            .await
            .expect("first publish should succeed");

        let fast_first = fast_subscriber
            .recv()
            .await
            .expect("fast subscriber should receive first message");
        assert_eq!(*fast_first.payload(), 1);
        fast_first.ack().await.expect("ack should succeed");

        let second_report = publisher
            .publish(2)
            .await
            .expect("second publish should succeed");
        assert_eq!(second_report.delivered_subscribers, 2);
        assert_eq!(second_report.dropped_subscribers, 0);

        let fast_second = fast_subscriber
            .recv()
            .await
            .expect("fast subscriber should receive second message");
        assert_eq!(*fast_second.payload(), 2);
        fast_second.ack().await.expect("ack should succeed");

        let slow_latest = slow_subscriber
            .recv()
            .await
            .expect("slow subscriber should receive latest message after lag handling");
        assert_eq!(*slow_latest.payload(), 2);
        slow_latest.ack().await.expect("ack should succeed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn create_topic_rejects_unsupported_broadcast_disconnect_policy() {
        let runtime = InMemoryTopicRuntime::<u64>::new();
        let topic = topic_name("broadcast-disconnect");
        let err = runtime
            .create_topic(
                topic.clone(),
                policies(
                    8,
                    TopicBalancedOnFullPolicy::Block,
                    1,
                    TopicBroadcastOnLagPolicy::Disconnect,
                ),
            )
            .await
            .expect_err("disconnect policy should be rejected for this backend");

        assert!(matches!(
            err,
            TopicRuntimeError::UnsupportedTopicPolicy {
                topic: _,
                backend: "in_memory_tokio_broadcast",
                policy: "broadcast_on_lag",
                ..
            }
        ));
    }
}
