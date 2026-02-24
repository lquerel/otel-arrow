// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::balanced::{BalancedGroup, SendAttemptOutcome};
use super::broadcast::{BroadcastPlane, BroadcastSubscriber};
use crate::topic::{
    TopicDelivery, TopicOutcomeInterest, TopicPublishReport, TopicPublishReportMode,
    TopicPublishResult, TopicPublisher, TopicPublisherOptions, TopicPublisherRouteMode,
    TopicRuntime, TopicRuntimeCapabilities, TopicRuntimeError, TopicSubscriber, TopicSubscription,
    validate_topic_policy_support,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use otap_df_config::TopicName;
use otap_df_config::topic::{
    SubscriptionGroupName, TopicBackend, TopicBalancedOnFullPolicy, TopicPolicies,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

const BACKEND_NAME: &str = "in_memory_v3_disruptor_experimental";

struct InMemoryTopicV3<T> {
    name: TopicName,
    policies: TopicPolicies,
    balanced_groups: Mutex<HashMap<SubscriptionGroupName, Arc<BalancedGroup<T>>>>,
    balanced_snapshot: ArcSwap<Vec<Arc<BalancedGroup<T>>>>,
    broadcast: Arc<BroadcastPlane<T>>,
}

impl<T> InMemoryTopicV3<T> {
    fn new(name: TopicName, policies: TopicPolicies) -> Self {
        Self {
            broadcast: Arc::new(BroadcastPlane::new(
                name.clone(),
                policies.broadcast_subscriber_queue_capacity,
                policies.broadcast_on_lag.clone(),
            )),
            name,
            policies,
            balanced_groups: Mutex::new(HashMap::new()),
            balanced_snapshot: ArcSwap::from_pointee(Vec::new()),
        }
    }

    fn balanced_on_full(&self) -> TopicBalancedOnFullPolicy {
        self.policies.balanced_on_full.clone()
    }

    fn balanced_snapshot(&self) -> Arc<Vec<Arc<BalancedGroup<T>>>> {
        self.balanced_snapshot.load_full()
    }

    fn rebuild_balanced_snapshot(
        &self,
        groups: &HashMap<SubscriptionGroupName, Arc<BalancedGroup<T>>>,
    ) {
        let snapshot = Arc::new(groups.values().cloned().collect::<Vec<_>>());
        self.balanced_snapshot.store(snapshot);
    }

    fn subscribe_balanced(
        self: &Arc<Self>,
        group: SubscriptionGroupName,
    ) -> InMemoryBalancedSubscriberV3<T> {
        let plane = {
            let mut groups = self.balanced_groups.lock();
            if let Some(existing) = groups.get(&group) {
                existing.clone()
            } else {
                let created = Arc::new(BalancedGroup::new(
                    self.policies.balanced_group_queue_capacity,
                ));
                let _previous = groups.insert(group, created.clone());
                self.rebuild_balanced_snapshot(&groups);
                created
            }
        };
        InMemoryBalancedSubscriberV3 { group: plane }
    }

    fn subscribe_broadcast(self: &Arc<Self>) -> InMemoryBroadcastSubscriberV3<T> {
        InMemoryBroadcastSubscriberV3 {
            inner: self.broadcast.subscribe(),
        }
    }
}

impl<T> InMemoryTopicV3<T>
where
    T: Clone + Send + 'static,
{
    async fn publish_fast_with_groups(
        &self,
        balanced_groups: &[Arc<BalancedGroup<T>>],
        payload: T,
        balanced_on_full: &TopicBalancedOnFullPolicy,
        include_broadcast: bool,
    ) {
        let broadcast_subscribers = if include_broadcast {
            self.broadcast.subscriber_count()
        } else {
            0
        };

        if balanced_groups.is_empty() && broadcast_subscribers == 0 {
            return;
        }

        if broadcast_subscribers == 0 && balanced_groups.len() == 1 {
            let _ = balanced_groups[0].publish(payload, balanced_on_full).await;
            return;
        }

        if balanced_groups.is_empty() && broadcast_subscribers > 0 {
            self.broadcast.publish(payload);
            return;
        }

        let total_destinations = balanced_groups.len() + usize::from(broadcast_subscribers > 0);
        let mut destination_index = 0usize;
        let mut payload_slot = Some(payload);

        for group in balanced_groups {
            let message = if destination_index + 1 == total_destinations {
                payload_slot
                    .take()
                    .expect("payload must exist for final destination")
            } else {
                payload_slot
                    .as_ref()
                    .expect("payload must exist before final destination")
                    .clone()
            };
            destination_index += 1;
            let _ = group.publish(message, balanced_on_full).await;
        }

        if broadcast_subscribers > 0 {
            let message = if destination_index + 1 == total_destinations {
                payload_slot
                    .take()
                    .expect("payload must exist for final destination")
            } else {
                payload_slot
                    .as_ref()
                    .expect("payload must exist before final destination")
                    .clone()
            };
            self.broadcast.publish(message);
        }
    }

    async fn publish_without_outcome_with_groups(
        &self,
        balanced_groups: &[Arc<BalancedGroup<T>>],
        payload: T,
        balanced_on_full: &TopicBalancedOnFullPolicy,
        report_mode: TopicPublishReportMode,
        include_broadcast: bool,
    ) -> TopicPublishResult {
        let full_report = matches!(report_mode, TopicPublishReportMode::Full);
        let broadcast_subscribers = if include_broadcast {
            self.broadcast.subscriber_count()
        } else {
            0
        };
        let attempted = balanced_groups.len() + broadcast_subscribers;

        let mut report = if full_report {
            TopicPublishReport {
                attempted_subscribers: attempted,
                delivered_subscribers: 0,
                dropped_subscribers: 0,
            }
        } else {
            TopicPublishReport::default()
        };

        if attempted == 0 {
            return TopicPublishResult {
                report,
                outcome: None,
            };
        }

        let mut dropped_any = false;
        let total_destinations = balanced_groups.len() + usize::from(broadcast_subscribers > 0);
        let mut destination_index = 0usize;
        let mut payload_slot = Some(payload);

        for group in balanced_groups {
            let message = if destination_index + 1 == total_destinations {
                payload_slot
                    .take()
                    .expect("payload must exist for final destination")
            } else {
                payload_slot
                    .as_ref()
                    .expect("payload must exist before final destination")
                    .clone()
            };
            destination_index += 1;

            match group.publish(message, balanced_on_full).await {
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
            }
        }

        if broadcast_subscribers > 0 {
            let message = if destination_index + 1 == total_destinations {
                payload_slot
                    .take()
                    .expect("payload must exist for final destination")
            } else {
                payload_slot
                    .as_ref()
                    .expect("payload must exist before final destination")
                    .clone()
            };
            self.broadcast.publish(message);
            if full_report {
                report.delivered_subscribers += broadcast_subscribers;
            }
        }

        if !full_report {
            report.dropped_subscribers = usize::from(dropped_any);
        }

        TopicPublishResult {
            report,
            outcome: None,
        }
    }
}

struct InMemoryBalancedSubscriberV3<T> {
    group: Arc<BalancedGroup<T>>,
}

impl<T> InMemoryBalancedSubscriberV3<T>
where
    T: Clone + Send + 'static,
{
    async fn recv(&self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        self.group.recv().await
    }
}

struct InMemoryBroadcastSubscriberV3<T> {
    inner: BroadcastSubscriber<T>,
}

impl<T> InMemoryBroadcastSubscriberV3<T>
where
    T: Clone + Send + 'static,
{
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        self.inner.recv().await
    }
}

enum InMemorySubscriberInnerV3<T> {
    Balanced(InMemoryBalancedSubscriberV3<T>),
    Broadcast(InMemoryBroadcastSubscriberV3<T>),
}

/// Concrete subscriber handle for [`InMemoryTopicRuntimeV3`].
pub struct InMemorySubscriberV3<T> {
    inner: InMemorySubscriberInnerV3<T>,
}

impl<T> InMemorySubscriberV3<T>
where
    T: Clone + Send + 'static,
{
    /// Receives the next topic delivery.
    pub async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        match &mut self.inner {
            InMemorySubscriberInnerV3::Balanced(subscriber) => subscriber.recv().await,
            InMemorySubscriberInnerV3::Broadcast(subscriber) => subscriber.recv().await,
        }
    }
}

#[async_trait]
impl<T> TopicSubscriber<T> for InMemorySubscriberV3<T>
where
    T: Clone + Send + 'static,
{
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        InMemorySubscriberV3::recv(self).await
    }
}

struct InMemoryTopicPublisherV3<T> {
    topic: Arc<InMemoryTopicV3<T>>,
    balanced_on_full: TopicBalancedOnFullPolicy,
    outcome_interest: TopicOutcomeInterest,
    report_mode: TopicPublishReportMode,
    route_mode: TopicPublisherRouteMode,
    frozen_balanced_groups: Option<Arc<Vec<Arc<BalancedGroup<T>>>>>,
}

impl<T> InMemoryTopicPublisherV3<T>
where
    T: Clone + Send + 'static,
{
    fn fast_path_enabled(&self) -> bool {
        matches!(self.outcome_interest, TopicOutcomeInterest::None)
            && matches!(self.report_mode, TopicPublishReportMode::Minimal)
    }

    async fn publish_fast(&self, payload: T) {
        if matches!(self.route_mode, TopicPublisherRouteMode::FrozenBalancedOnly)
            && let Some(groups) = &self.frozen_balanced_groups
        {
            self.topic
                .publish_fast_with_groups(groups.as_slice(), payload, &self.balanced_on_full, false)
                .await;
            return;
        }

        let groups = self.topic.balanced_snapshot();
        self.topic
            .publish_fast_with_groups(groups.as_slice(), payload, &self.balanced_on_full, true)
            .await;
    }

    async fn publish(&self, payload: T) -> Result<TopicPublishResult, TopicRuntimeError> {
        if self.outcome_interest.is_enabled() {
            return Err(TopicRuntimeError::UnsupportedTopicPolicy {
                topic: self.topic.name.clone(),
                backend: BACKEND_NAME,
                policy: "publisher.outcome_interest",
                value: outcome_interest_value(self.outcome_interest).to_owned(),
            });
        }

        if self.fast_path_enabled() {
            self.publish_fast(payload).await;
            return Ok(TopicPublishResult {
                report: TopicPublishReport::default(),
                outcome: None,
            });
        }

        if matches!(self.route_mode, TopicPublisherRouteMode::FrozenBalancedOnly)
            && let Some(groups) = &self.frozen_balanced_groups
        {
            return Ok(self
                .topic
                .publish_without_outcome_with_groups(
                    groups.as_slice(),
                    payload,
                    &self.balanced_on_full,
                    self.report_mode,
                    false,
                )
                .await);
        }

        let groups = self.topic.balanced_snapshot();
        Ok(self
            .topic
            .publish_without_outcome_with_groups(
                groups.as_slice(),
                payload,
                &self.balanced_on_full,
                self.report_mode,
                true,
            )
            .await)
    }
}

#[async_trait]
impl<T> TopicPublisher<T> for InMemoryTopicPublisherV3<T>
where
    T: Clone + Send + 'static,
{
    async fn publish(&self, payload: T) -> Result<TopicPublishResult, TopicRuntimeError> {
        InMemoryTopicPublisherV3::publish(self, payload).await
    }
}

/// Concrete publisher handle for [`InMemoryTopicRuntimeV3`].
pub struct InMemoryPublisherV3<T> {
    inner: Arc<InMemoryTopicPublisherV3<T>>,
}

impl<T> Clone for InMemoryPublisherV3<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> InMemoryPublisherV3<T>
where
    T: Clone + Send + 'static,
{
    /// Publishes one payload and returns report information.
    pub async fn publish(&self, payload: T) -> Result<TopicPublishResult, TopicRuntimeError> {
        self.inner.publish(payload).await
    }

    /// Publishes one payload on the low-overhead fast path.
    pub async fn publish_fast(&self, payload: T) -> Result<(), TopicRuntimeError> {
        if self.inner.outcome_interest.is_enabled() {
            return Err(TopicRuntimeError::UnsupportedTopicPolicy {
                topic: self.inner.topic.name.clone(),
                backend: BACKEND_NAME,
                policy: "publisher.outcome_interest",
                value: outcome_interest_value(self.inner.outcome_interest).to_owned(),
            });
        }
        self.inner.publish_fast(payload).await;
        Ok(())
    }
}

#[derive(Default)]
struct InMemoryTopicRuntimeV3State<T> {
    topics: HashMap<TopicName, Arc<InMemoryTopicV3<T>>>,
}

/// Experimental in-memory topic runtime v3 based on disruptor-style rings.
pub struct InMemoryTopicRuntimeV3<T> {
    state: RwLock<InMemoryTopicRuntimeV3State<T>>,
}

impl<T> Default for InMemoryTopicRuntimeV3<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> InMemoryTopicRuntimeV3<T> {
    /// Creates an empty v3 runtime.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: RwLock::new(InMemoryTopicRuntimeV3State {
                topics: HashMap::new(),
            }),
        }
    }
}

impl<T> InMemoryTopicRuntimeV3<T>
where
    T: Clone + Send + 'static,
{
    fn read_state(
        &self,
    ) -> Result<std::sync::RwLockReadGuard<'_, InMemoryTopicRuntimeV3State<T>>, TopicRuntimeError>
    {
        self.state.read().map_err(|_| TopicRuntimeError::Internal {
            message: "topic runtime v3 state lock poisoned".to_owned(),
        })
    }

    fn write_state(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, InMemoryTopicRuntimeV3State<T>>, TopicRuntimeError>
    {
        self.state.write().map_err(|_| TopicRuntimeError::Internal {
            message: "topic runtime v3 state lock poisoned".to_owned(),
        })
    }

    fn build_publisher(
        &self,
        topic_name: &TopicName,
        options: TopicPublisherOptions,
    ) -> Result<Arc<InMemoryTopicPublisherV3<T>>, TopicRuntimeError> {
        let state = self.read_state()?;
        let topic = state.topics.get(topic_name).cloned().ok_or_else(|| {
            TopicRuntimeError::TopicNotFound {
                topic: topic_name.clone(),
            }
        })?;

        let balanced_on_full = options
            .balanced_on_full_override
            .unwrap_or_else(|| topic.balanced_on_full());

        let frozen_balanced_groups = if matches!(
            options.route_mode,
            TopicPublisherRouteMode::FrozenBalancedOnly
        ) {
            Some(topic.balanced_snapshot())
        } else {
            None
        };

        Ok(Arc::new(InMemoryTopicPublisherV3 {
            topic,
            balanced_on_full,
            outcome_interest: options.outcome_interest,
            report_mode: options.report_mode,
            route_mode: options.route_mode,
            frozen_balanced_groups,
        }))
    }

    fn build_subscriber(
        &self,
        topic_name: &TopicName,
        subscription: TopicSubscription,
    ) -> Result<InMemorySubscriberV3<T>, TopicRuntimeError> {
        let state = self.read_state()?;
        let topic = state.topics.get(topic_name).cloned().ok_or_else(|| {
            TopicRuntimeError::TopicNotFound {
                topic: topic_name.clone(),
            }
        })?;
        drop(state);

        match subscription {
            TopicSubscription::Balanced { group } => Ok(InMemorySubscriberV3 {
                inner: InMemorySubscriberInnerV3::Balanced(topic.subscribe_balanced(group)),
            }),
            TopicSubscription::Broadcast => Ok(InMemorySubscriberV3 {
                inner: InMemorySubscriberInnerV3::Broadcast(topic.subscribe_broadcast()),
            }),
        }
    }

    /// Creates a concrete publisher handle.
    pub async fn publisher(
        &self,
        topic_name: &TopicName,
        options: TopicPublisherOptions,
    ) -> Result<InMemoryPublisherV3<T>, TopicRuntimeError> {
        Ok(InMemoryPublisherV3 {
            inner: self.build_publisher(topic_name, options)?,
        })
    }

    /// Creates a concrete subscriber handle.
    pub async fn subscribe(
        &self,
        topic_name: &TopicName,
        subscription: TopicSubscription,
    ) -> Result<InMemorySubscriberV3<T>, TopicRuntimeError> {
        self.build_subscriber(topic_name, subscription)
    }
}

#[async_trait]
impl<T> TopicRuntime<T> for InMemoryTopicRuntimeV3<T>
where
    T: Clone + Send + 'static,
{
    fn capabilities(&self) -> TopicRuntimeCapabilities {
        TopicRuntimeCapabilities {
            backend_name: BACKEND_NAME,
            supports_broadcast_on_lag_drop_oldest: true,
            supports_broadcast_on_lag_disconnect: true,
        }
    }

    async fn create_topic(
        &self,
        topic_name: TopicName,
        backend: TopicBackend,
        policies: TopicPolicies,
    ) -> Result<(), TopicRuntimeError> {
        if backend != TopicBackend::InMemory {
            return Err(TopicRuntimeError::UnsupportedTopicBackend {
                topic: topic_name,
                runtime: self.capabilities().backend_name,
                backend,
            });
        }
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
        validate_topic_policy_support(&topic_name, &policies, self.capabilities())?;

        let mut state = self.write_state()?;
        if state.topics.contains_key(&topic_name) {
            return Err(TopicRuntimeError::TopicAlreadyExists { topic: topic_name });
        }

        let _previous = state.topics.insert(
            topic_name.clone(),
            Arc::new(InMemoryTopicV3::new(topic_name, policies)),
        );
        Ok(())
    }

    async fn publisher(
        &self,
        topic_name: &TopicName,
        options: TopicPublisherOptions,
    ) -> Result<Arc<dyn TopicPublisher<T>>, TopicRuntimeError> {
        Ok(self.build_publisher(topic_name, options)? as Arc<dyn TopicPublisher<T>>)
    }

    async fn subscribe(
        &self,
        topic_name: &TopicName,
        subscription: TopicSubscription,
    ) -> Result<Box<dyn TopicSubscriber<T>>, TopicRuntimeError> {
        Ok(Box::new(self.build_subscriber(topic_name, subscription)?))
    }
}

fn outcome_interest_value(interest: TopicOutcomeInterest) -> &'static str {
    match interest {
        TopicOutcomeInterest::None => "none",
        TopicOutcomeInterest::Ack => "ack",
        TopicOutcomeInterest::Nack => "nack",
        TopicOutcomeInterest::AckOrNack => "ack_or_nack",
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryTopicRuntimeV3;
    use crate::topic::{
        TopicOutcomeInterest, TopicPublisherOptions, TopicRuntime, TopicRuntimeError,
        TopicSubscription,
    };
    use otap_df_config::TopicName;
    use otap_df_config::topic::{
        SubscriptionGroupName, TopicBackend, TopicBalancedOnFullPolicy, TopicBroadcastOnLagPolicy,
        TopicPolicies,
    };
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

    /// Scenario:
    /// 1. Given one balanced subscriber.
    /// 2. When one payload is published.
    /// 3. Then the subscriber receives the payload.
    #[tokio::test(flavor = "current_thread")]
    async fn balanced_roundtrip() {
        let runtime = InMemoryTopicRuntimeV3::<u64>::new();
        let topic = topic_name("v3-balanced");
        runtime
            .create_topic(
                topic.clone(),
                TopicBackend::InMemory,
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
            .publisher(&topic, TopicPublisherOptions::default())
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
            .expect("subscriber should be created");

        let publish = publisher.publish(10).await.expect("publish should succeed");
        assert_eq!(publish.report.delivered_subscribers, 1);
        let delivery = subscriber.recv().await.expect("delivery should succeed");
        assert_eq!(*delivery.payload(), 10);
    }

    /// Scenario:
    /// 1. Given balanced policy `drop_newest` and capacity 1.
    /// 2. When two payloads are published without draining.
    /// 3. Then second publish is dropped.
    #[tokio::test(flavor = "current_thread")]
    async fn balanced_drop_newest_when_full() {
        let runtime = InMemoryTopicRuntimeV3::<u64>::new();
        let topic = topic_name("v3-drop");
        runtime
            .create_topic(
                topic.clone(),
                TopicBackend::InMemory,
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
            .publisher(&topic, TopicPublisherOptions::default())
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
            .expect("subscriber should be created");

        let first = publisher
            .publish(1)
            .await
            .expect("first publish should succeed");
        let second = publisher
            .publish(2)
            .await
            .expect("second publish should succeed");
        assert_eq!(first.report.delivered_subscribers, 1);
        assert_eq!(second.report.dropped_subscribers, 1);

        let delivery = subscriber.recv().await.expect("delivery should succeed");
        assert_eq!(*delivery.payload(), 1);
        assert!(
            timeout(Duration::from_millis(50), subscriber.recv())
                .await
                .is_err()
        );
    }

    /// Scenario:
    /// 1. Given balanced policy `block` and capacity 1.
    /// 2. When a second publish happens while queue is full.
    /// 3. Then second publish resumes only after one receive.
    #[tokio::test(flavor = "current_thread")]
    async fn balanced_block_applies_backpressure() {
        let runtime = InMemoryTopicRuntimeV3::<u64>::new();
        let topic = topic_name("v3-block");
        runtime
            .create_topic(
                topic.clone(),
                TopicBackend::InMemory,
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
            .publisher(&topic, TopicPublisherOptions::default())
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
            .expect("subscriber should be created");

        let _first = publisher
            .publish(1)
            .await
            .expect("first publish should succeed");
        let publisher_for_task = publisher.clone();
        let blocked_publish = tokio::spawn(async move { publisher_for_task.publish(2).await });
        sleep(Duration::from_millis(25)).await;
        assert!(
            !blocked_publish.is_finished(),
            "second publish should block while queue is full"
        );

        let first_delivery = subscriber
            .recv()
            .await
            .expect("first receive should succeed");
        assert_eq!(*first_delivery.payload(), 1);

        let second_publish = timeout(Duration::from_secs(1), blocked_publish)
            .await
            .expect("blocked publish should complete")
            .expect("join should succeed")
            .expect("publish should succeed");
        assert_eq!(second_publish.report.delivered_subscribers, 1);

        let second_delivery = subscriber
            .recv()
            .await
            .expect("second receive should succeed");
        assert_eq!(*second_delivery.payload(), 2);
    }

    /// Scenario:
    /// 1. Given two broadcast subscribers.
    /// 2. When one payload is published.
    /// 3. Then both subscribers receive the payload.
    #[tokio::test(flavor = "current_thread")]
    async fn broadcast_fanout() {
        let runtime = InMemoryTopicRuntimeV3::<u64>::new();
        let topic = topic_name("v3-broadcast");
        runtime
            .create_topic(
                topic.clone(),
                TopicBackend::InMemory,
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
            .publisher(&topic, TopicPublisherOptions::default())
            .await
            .expect("publisher should be created");
        let mut subscriber_1 = runtime
            .subscribe(&topic, TopicSubscription::Broadcast)
            .await
            .expect("subscriber 1 should be created");
        let mut subscriber_2 = runtime
            .subscribe(&topic, TopicSubscription::Broadcast)
            .await
            .expect("subscriber 2 should be created");

        let publish = publisher.publish(7).await.expect("publish should succeed");
        assert_eq!(publish.report.delivered_subscribers, 2);
        let delivery_1 = subscriber_1.recv().await.expect("delivery should succeed");
        let delivery_2 = subscriber_2.recv().await.expect("delivery should succeed");
        assert_eq!(*delivery_1.payload(), 7);
        assert_eq!(*delivery_2.payload(), 7);
    }

    /// Scenario:
    /// 1. Given broadcast capacity 1 with `drop_oldest`.
    /// 2. When one subscriber lags and newer data arrives.
    /// 3. Then lagging subscriber receives latest payload.
    #[tokio::test(flavor = "current_thread")]
    async fn broadcast_lag_drop_oldest() {
        let runtime = InMemoryTopicRuntimeV3::<u64>::new();
        let topic = topic_name("v3-broadcast-lag");
        runtime
            .create_topic(
                topic.clone(),
                TopicBackend::InMemory,
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
            .publisher(&topic, TopicPublisherOptions::default())
            .await
            .expect("publisher should be created");
        let mut fast = runtime
            .subscribe(&topic, TopicSubscription::Broadcast)
            .await
            .expect("fast subscriber should be created");
        let mut slow = runtime
            .subscribe(&topic, TopicSubscription::Broadcast)
            .await
            .expect("slow subscriber should be created");

        let _first = publisher
            .publish(1)
            .await
            .expect("first publish should succeed");
        let fast_first = fast.recv().await.expect("fast should receive first");
        assert_eq!(*fast_first.payload(), 1);

        let _second = publisher
            .publish(2)
            .await
            .expect("second publish should succeed");
        let fast_second = fast.recv().await.expect("fast should receive second");
        assert_eq!(*fast_second.payload(), 2);
        let slow_latest = slow.recv().await.expect("slow should receive latest");
        assert_eq!(*slow_latest.payload(), 2);
    }

    /// Scenario:
    /// 1. Given broadcast lag policy `disconnect`.
    /// 2. When subscriber lags beyond ring capacity.
    /// 3. Then recv returns `ChannelClosed`.
    #[tokio::test(flavor = "current_thread")]
    async fn broadcast_lag_disconnect() {
        let runtime = InMemoryTopicRuntimeV3::<u64>::new();
        let topic = topic_name("v3-disconnect");
        runtime
            .create_topic(
                topic.clone(),
                TopicBackend::InMemory,
                policies(
                    8,
                    TopicBalancedOnFullPolicy::Block,
                    1,
                    TopicBroadcastOnLagPolicy::Disconnect,
                ),
            )
            .await
            .expect("topic should be created");

        let publisher = runtime
            .publisher(&topic, TopicPublisherOptions::default())
            .await
            .expect("publisher should be created");
        let mut slow = runtime
            .subscribe(&topic, TopicSubscription::Broadcast)
            .await
            .expect("subscriber should be created");

        let _first = publisher
            .publish(1)
            .await
            .expect("first publish should succeed");
        let _second = publisher
            .publish(2)
            .await
            .expect("second publish should succeed");

        let error = match slow.recv().await {
            Ok(_delivery) => panic!("lagging subscriber should disconnect"),
            Err(error) => error,
        };
        assert!(matches!(error, TopicRuntimeError::ChannelClosed { .. }));
    }

    /// Scenario:
    /// 1. Given v3 publisher options requesting outcome tracking.
    /// 2. When publish is called.
    /// 3. Then operation fails fast because outcomes are unsupported in v3.
    #[tokio::test(flavor = "current_thread")]
    async fn outcome_interest_is_rejected() {
        let runtime = InMemoryTopicRuntimeV3::<u64>::new();
        let topic = topic_name("v3-outcome");
        runtime
            .create_topic(
                topic.clone(),
                TopicBackend::InMemory,
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
            .publisher(
                &topic,
                TopicPublisherOptions {
                    outcome_interest: TopicOutcomeInterest::AckOrNack,
                    ..TopicPublisherOptions::default()
                },
            )
            .await
            .expect("publisher should be created");

        let error = match publisher.publish(1).await {
            Ok(_publish) => panic!("outcome should be rejected"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            TopicRuntimeError::UnsupportedTopicPolicy {
                policy: "publisher.outcome_interest",
                ..
            }
        ));
    }
}
