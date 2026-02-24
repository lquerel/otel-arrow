// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use crate::topic::{
    DeliveryAckHandler, TopicDelivery, TopicOutcomeInterest, TopicOutcomeNack, TopicPublishOutcome,
    TopicPublishOutcomeFuture, TopicPublishReport, TopicPublishReportMode, TopicPublishResult,
    TopicPublisher, TopicPublisherOptions, TopicPublisherRouteMode, TopicRuntime,
    TopicRuntimeCapabilities, TopicRuntimeError, TopicSubscriber, TopicSubscription,
    validate_topic_policy_support,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use otap_df_config::TopicName;
use otap_df_config::topic::{
    SubscriptionGroupName, TopicBackend, TopicBalancedOnFullPolicy, TopicPolicies,
};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::Notify;
use tokio::sync::oneshot;

#[derive(Clone)]
struct BalancedEnvelope<T> {
    payload: T,
    outcome_tracker: Option<Arc<PublishOutcomeTracker>>,
}

enum SendAttemptOutcome {
    Delivered,
    Dropped,
}

struct BalancedQueue<T> {
    state: Mutex<VecDeque<BalancedEnvelope<T>>>,
    capacity: usize,
    not_empty: Notify,
    not_full: Notify,
}

impl<T> BalancedQueue<T> {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            not_empty: Notify::new(),
            not_full: Notify::new(),
        }
    }

    async fn send_with_policy(
        &self,
        envelope: BalancedEnvelope<T>,
        balanced_on_full: &TopicBalancedOnFullPolicy,
    ) -> SendAttemptOutcome {
        match balanced_on_full {
            TopicBalancedOnFullPolicy::Block => {
                let mut envelope = Some(envelope);
                loop {
                    let wait_for_not_full = {
                        let mut queue = self.state.lock();
                        if queue.len() < self.capacity {
                            let was_empty = queue.is_empty();
                            queue.push_back(
                                envelope
                                    .take()
                                    .expect("envelope must exist until successful enqueue"),
                            );
                            drop(queue);
                            if was_empty {
                                self.not_empty.notify_one();
                            }
                            return SendAttemptOutcome::Delivered;
                        }
                        self.not_full.notified()
                    };
                    wait_for_not_full.await;
                }
            }
            TopicBalancedOnFullPolicy::DropNewest => {
                let mut queue = self.state.lock();
                if queue.len() >= self.capacity {
                    SendAttemptOutcome::Dropped
                } else {
                    let was_empty = queue.is_empty();
                    queue.push_back(envelope);
                    drop(queue);
                    if was_empty {
                        self.not_empty.notify_one();
                    }
                    SendAttemptOutcome::Delivered
                }
            }
        }
    }

    async fn recv(&self) -> Result<BalancedEnvelope<T>, TopicRuntimeError> {
        loop {
            let wait_for_not_empty = {
                let mut queue = self.state.lock();
                if let Some(envelope) = queue.pop_front() {
                    let was_full = queue.len() + 1 == self.capacity;
                    drop(queue);
                    if was_full {
                        self.not_full.notify_one();
                    }
                    return Ok(envelope);
                }
                self.not_empty.notified()
            };
            wait_for_not_empty.await;
        }
    }
}

struct PublishOutcomeTracker {
    state: Mutex<PublishOutcomeTrackerState>,
}

struct PublishOutcomeTrackerState {
    interest: TopicOutcomeInterest,
    pending_deliveries: usize,
    completed: bool,
    tx: Option<oneshot::Sender<TopicPublishOutcome>>,
}

impl PublishOutcomeTracker {
    fn new(interest: TopicOutcomeInterest) -> (Arc<Self>, oneshot::Receiver<TopicPublishOutcome>) {
        let (tx, rx) = oneshot::channel();
        (
            Arc::new(Self {
                state: Mutex::new(PublishOutcomeTrackerState {
                    interest,
                    pending_deliveries: 0,
                    completed: false,
                    tx: Some(tx),
                }),
            }),
            rx,
        )
    }

    fn interest(&self) -> TopicOutcomeInterest {
        self.state.lock().interest
    }

    fn reserve_delivery(&self) {
        let mut state = self.state.lock();
        if !state.completed {
            state.pending_deliveries += 1;
        }
    }

    fn complete_if_idle(&self) {
        let mut state = self.state.lock();
        if state.completed || state.pending_deliveries != 0 {
            return;
        }
        if let Some(tx) = state.tx.take() {
            let _ignore = tx.send(TopicPublishOutcome::Ack);
        }
        state.completed = true;
    }

    fn on_ack(&self) {
        let mut state = self.state.lock();
        if state.completed {
            return;
        }
        if state.pending_deliveries > 0 {
            state.pending_deliveries -= 1;
        }
        if state.pending_deliveries == 0 {
            if let Some(tx) = state.tx.take() {
                let _ignore = tx.send(TopicPublishOutcome::Ack);
            }
            state.completed = true;
        }
    }

    fn on_nack(&self, nack: TopicOutcomeNack) {
        let mut state = self.state.lock();
        if state.completed {
            return;
        }
        if let Some(tx) = state.tx.take() {
            let _ignore = tx.send(TopicPublishOutcome::Nack(nack));
        }
        state.completed = true;
        state.pending_deliveries = 0;
    }

    fn on_publish_drop(&self, reason: impl Into<String>) {
        self.on_nack(TopicOutcomeNack::transient(reason));
    }
}

fn immediate_outcome_future(outcome: TopicPublishOutcome) -> TopicPublishOutcomeFuture {
    Box::pin(async move { Ok(outcome) })
}

fn tracker_outcome_future(rx: oneshot::Receiver<TopicPublishOutcome>) -> TopicPublishOutcomeFuture {
    Box::pin(async move {
        rx.await.map_err(|_| TopicRuntimeError::Internal {
            message: "topic publish outcome receiver unexpectedly closed".to_owned(),
        })
    })
}

struct InMemoryTopicV2<T> {
    policies: TopicPolicies,
    groups: Mutex<HashMap<SubscriptionGroupName, Arc<BalancedQueue<T>>>>,
    group_snapshot: ArcSwap<Vec<Arc<BalancedQueue<T>>>>,
}

impl<T> InMemoryTopicV2<T>
where
    T: Clone + Send + 'static,
{
    fn new(_name: TopicName, policies: TopicPolicies) -> Self {
        Self {
            policies,
            groups: Mutex::new(HashMap::new()),
            group_snapshot: ArcSwap::from_pointee(Vec::new()),
        }
    }

    fn balanced_on_full(&self) -> TopicBalancedOnFullPolicy {
        self.policies.balanced_on_full.clone()
    }

    fn queue_snapshot(&self) -> Arc<Vec<Arc<BalancedQueue<T>>>> {
        self.group_snapshot.load_full()
    }

    fn rebuild_group_snapshot(
        &self,
        groups: &HashMap<SubscriptionGroupName, Arc<BalancedQueue<T>>>,
    ) {
        let snapshot = Arc::new(groups.values().cloned().collect::<Vec<_>>());
        self.group_snapshot.store(snapshot);
    }

    fn subscribe_balanced(
        self: &Arc<Self>,
        group: SubscriptionGroupName,
    ) -> InMemoryBalancedSubscriberV2<T> {
        let queue = {
            let mut groups = self.groups.lock();
            if let Some(queue) = groups.get(&group) {
                queue.clone()
            } else {
                let queue = Arc::new(BalancedQueue::new(
                    self.policies.balanced_group_queue_capacity,
                ));
                let _previous = groups.insert(group, queue.clone());
                self.rebuild_group_snapshot(&groups);
                queue
            }
        };

        InMemoryBalancedSubscriberV2 { queue }
    }

    async fn publish_fast_with_queues(
        &self,
        queues: &[Arc<BalancedQueue<T>>],
        payload: T,
        balanced_on_full: &TopicBalancedOnFullPolicy,
    ) -> Result<(), TopicRuntimeError> {
        if queues.is_empty() {
            return Ok(());
        }
        if queues.len() == 1 {
            let _ = queues[0]
                .send_with_policy(
                    BalancedEnvelope {
                        payload,
                        outcome_tracker: None,
                    },
                    balanced_on_full,
                )
                .await;
            return Ok(());
        }

        let mut payload_slot = Some(payload);
        for (index, queue) in queues.iter().enumerate() {
            let message = if index + 1 == queues.len() {
                payload_slot
                    .take()
                    .expect("payload must exist for last balanced queue")
            } else {
                payload_slot
                    .as_ref()
                    .expect("payload must exist before last balanced queue")
                    .clone()
            };

            let _ = queue
                .send_with_policy(
                    BalancedEnvelope {
                        payload: message,
                        outcome_tracker: None,
                    },
                    balanced_on_full,
                )
                .await;
        }
        Ok(())
    }

    async fn publish_fast(
        &self,
        payload: T,
        balanced_on_full: &TopicBalancedOnFullPolicy,
    ) -> Result<(), TopicRuntimeError> {
        let queues = self.queue_snapshot();
        self.publish_fast_with_queues(queues.as_slice(), payload, balanced_on_full)
            .await
    }

    async fn publish_with_queues(
        &self,
        queues: &[Arc<BalancedQueue<T>>],
        payload: T,
        balanced_on_full: &TopicBalancedOnFullPolicy,
        outcome_interest: TopicOutcomeInterest,
        report_mode: TopicPublishReportMode,
    ) -> Result<TopicPublishResult, TopicRuntimeError> {
        let full_report = matches!(report_mode, TopicPublishReportMode::Full);
        let mut report = if full_report {
            TopicPublishReport {
                attempted_subscribers: queues.len(),
                delivered_subscribers: 0,
                dropped_subscribers: 0,
            }
        } else {
            TopicPublishReport::default()
        };

        if queues.is_empty() {
            let outcome = if outcome_interest.is_enabled() {
                Some(immediate_outcome_future(TopicPublishOutcome::Ack))
            } else {
                None
            };
            return Ok(TopicPublishResult { report, outcome });
        }

        if !outcome_interest.is_enabled() {
            if queues.len() == 1 {
                match queues[0]
                    .send_with_policy(
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
                        report.dropped_subscribers = 1;
                    }
                }
                return Ok(TopicPublishResult {
                    report,
                    outcome: None,
                });
            }

            let mut payload_slot = Some(payload);
            let mut dropped_any = false;
            for (index, queue) in queues.iter().enumerate() {
                let message = if index + 1 == queues.len() {
                    payload_slot
                        .take()
                        .expect("payload must exist for last balanced queue")
                } else {
                    payload_slot
                        .as_ref()
                        .expect("payload must exist before last balanced queue")
                        .clone()
                };

                match queue
                    .send_with_policy(
                        BalancedEnvelope {
                            payload: message,
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

        let (outcome_tracker, receiver) = PublishOutcomeTracker::new(outcome_interest);
        if queues.len() == 1 {
            outcome_tracker.reserve_delivery();
            let mut dropped = false;
            match queues[0]
                .send_with_policy(
                    BalancedEnvelope {
                        payload,
                        outcome_tracker: Some(outcome_tracker.clone()),
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
                    dropped = true;
                    outcome_tracker.on_publish_drop("balanced queue full: drop_newest");
                    report.dropped_subscribers = 1;
                }
            }
            if !full_report {
                report.dropped_subscribers = usize::from(dropped);
            }
            if dropped {
                outcome_tracker.complete_if_idle();
            }
            return Ok(TopicPublishResult {
                report,
                outcome: Some(tracker_outcome_future(receiver)),
            });
        }

        let mut payload_slot = Some(payload);
        let mut delivered = 0usize;
        let mut dropped_any = false;

        for (index, queue) in queues.iter().enumerate() {
            let message = if index + 1 == queues.len() {
                payload_slot
                    .take()
                    .expect("payload must exist for last balanced queue")
            } else {
                payload_slot
                    .as_ref()
                    .expect("payload must exist before last balanced queue")
                    .clone()
            };

            outcome_tracker.reserve_delivery();
            match queue
                .send_with_policy(
                    BalancedEnvelope {
                        payload: message,
                        outcome_tracker: Some(outcome_tracker.clone()),
                    },
                    balanced_on_full,
                )
                .await
            {
                SendAttemptOutcome::Delivered => {
                    delivered += 1;
                    if full_report {
                        report.delivered_subscribers += 1;
                    }
                }
                SendAttemptOutcome::Dropped => {
                    outcome_tracker.on_publish_drop("balanced queue full: drop_newest");
                    if full_report {
                        report.dropped_subscribers += 1;
                    } else {
                        dropped_any = true;
                    }
                }
            }
        }

        if delivered == 0 {
            outcome_tracker.complete_if_idle();
        }

        if !full_report {
            report.dropped_subscribers = usize::from(dropped_any);
        }

        Ok(TopicPublishResult {
            report,
            outcome: Some(tracker_outcome_future(receiver)),
        })
    }

    async fn publish(
        &self,
        payload: T,
        balanced_on_full: &TopicBalancedOnFullPolicy,
        outcome_interest: TopicOutcomeInterest,
        report_mode: TopicPublishReportMode,
    ) -> Result<TopicPublishResult, TopicRuntimeError> {
        let queues = self.queue_snapshot();
        self.publish_with_queues(
            queues.as_slice(),
            payload,
            balanced_on_full,
            outcome_interest,
            report_mode,
        )
        .await
    }
}

struct BalancedAckHandlerV2 {
    outcome_tracker: Arc<PublishOutcomeTracker>,
}

#[async_trait]
impl<T> DeliveryAckHandler<T> for BalancedAckHandlerV2
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

struct InMemoryBalancedSubscriberV2<T> {
    queue: Arc<BalancedQueue<T>>,
}

impl<T> InMemoryBalancedSubscriberV2<T>
where
    T: Send + 'static,
{
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        let envelope = self.queue.recv().await?;
        match envelope.outcome_tracker {
            Some(outcome_tracker) => Ok(TopicDelivery::new(
                envelope.payload,
                Box::new(BalancedAckHandlerV2 { outcome_tracker }),
            )),
            None => Ok(TopicDelivery::new_without_ack(envelope.payload)),
        }
    }
}

#[async_trait]
impl<T> TopicSubscriber<T> for InMemoryBalancedSubscriberV2<T>
where
    T: Send + 'static,
{
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        InMemoryBalancedSubscriberV2::recv(self).await
    }
}

struct InMemoryTopicPublisherV2<T> {
    topic: Arc<InMemoryTopicV2<T>>,
    balanced_on_full: TopicBalancedOnFullPolicy,
    outcome_interest: TopicOutcomeInterest,
    report_mode: TopicPublishReportMode,
    route_mode: TopicPublisherRouteMode,
    frozen_queues: Option<Arc<Vec<Arc<BalancedQueue<T>>>>>,
}

impl<T> InMemoryTopicPublisherV2<T>
where
    T: Clone + Send + 'static,
{
    fn fast_path_enabled(&self) -> bool {
        matches!(self.outcome_interest, TopicOutcomeInterest::None)
            && matches!(self.report_mode, TopicPublishReportMode::Minimal)
    }

    async fn publish_fast(&self, payload: T) -> Result<(), TopicRuntimeError> {
        if matches!(self.route_mode, TopicPublisherRouteMode::FrozenBalancedOnly)
            && let Some(queues) = &self.frozen_queues
        {
            return self
                .topic
                .publish_fast_with_queues(queues.as_slice(), payload, &self.balanced_on_full)
                .await;
        }
        self.topic
            .publish_fast(payload, &self.balanced_on_full)
            .await
    }

    async fn publish(&self, payload: T) -> Result<TopicPublishResult, TopicRuntimeError> {
        if self.fast_path_enabled() {
            self.publish_fast(payload).await?;
            return Ok(TopicPublishResult {
                report: TopicPublishReport::default(),
                outcome: None,
            });
        }

        if matches!(self.route_mode, TopicPublisherRouteMode::FrozenBalancedOnly)
            && let Some(queues) = &self.frozen_queues
        {
            return self
                .topic
                .publish_with_queues(
                    queues.as_slice(),
                    payload,
                    &self.balanced_on_full,
                    self.outcome_interest,
                    self.report_mode,
                )
                .await;
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
impl<T> TopicPublisher<T> for InMemoryTopicPublisherV2<T>
where
    T: Clone + Send + 'static,
{
    async fn publish(&self, payload: T) -> Result<TopicPublishResult, TopicRuntimeError> {
        InMemoryTopicPublisherV2::publish(self, payload).await
    }
}

/// Concrete publisher handle for [`InMemoryTopicRuntimeV2`].
pub struct InMemoryPublisherV2<T> {
    inner: Arc<InMemoryTopicPublisherV2<T>>,
}

impl<T> Clone for InMemoryPublisherV2<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> InMemoryPublisherV2<T>
where
    T: Clone + Send + 'static,
{
    /// Publishes one payload and returns publish report/outcome information.
    pub async fn publish(&self, payload: T) -> Result<TopicPublishResult, TopicRuntimeError> {
        self.inner.publish(payload).await
    }

    /// Publishes one payload on the low-overhead fast path.
    pub async fn publish_fast(&self, payload: T) -> Result<(), TopicRuntimeError> {
        self.inner.publish_fast(payload).await
    }
}

/// Concrete subscriber handle for [`InMemoryTopicRuntimeV2`].
pub struct InMemorySubscriberV2<T> {
    inner: InMemoryBalancedSubscriberV2<T>,
}

impl<T> InMemorySubscriberV2<T>
where
    T: Send + 'static,
{
    /// Receives the next balanced delivery.
    pub async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        self.inner.recv().await
    }
}

#[async_trait]
impl<T> TopicSubscriber<T> for InMemorySubscriberV2<T>
where
    T: Send + 'static,
{
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        InMemorySubscriberV2::recv(self).await
    }
}

#[derive(Default)]
struct InMemoryTopicRuntimeV2State<T> {
    topics: HashMap<TopicName, Arc<InMemoryTopicV2<T>>>,
}

/// In-memory topic runtime v2 with native balanced queue implementation.
pub struct InMemoryTopicRuntimeV2<T> {
    state: RwLock<InMemoryTopicRuntimeV2State<T>>,
}

impl<T> Default for InMemoryTopicRuntimeV2<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> InMemoryTopicRuntimeV2<T> {
    /// Creates an empty runtime instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: RwLock::new(InMemoryTopicRuntimeV2State {
                topics: HashMap::new(),
            }),
        }
    }
}

impl<T> InMemoryTopicRuntimeV2<T>
where
    T: Clone + Send + 'static,
{
    fn read_state(
        &self,
    ) -> Result<std::sync::RwLockReadGuard<'_, InMemoryTopicRuntimeV2State<T>>, TopicRuntimeError>
    {
        self.state.read().map_err(|_| TopicRuntimeError::Internal {
            message: "topic runtime v2 state lock poisoned".to_owned(),
        })
    }

    fn write_state(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, InMemoryTopicRuntimeV2State<T>>, TopicRuntimeError>
    {
        self.state.write().map_err(|_| TopicRuntimeError::Internal {
            message: "topic runtime v2 state lock poisoned".to_owned(),
        })
    }

    fn build_publisher(
        &self,
        topic_name: &TopicName,
        options: TopicPublisherOptions,
    ) -> Result<Arc<InMemoryTopicPublisherV2<T>>, TopicRuntimeError> {
        let state = self.read_state()?;
        let topic = state.topics.get(topic_name).cloned().ok_or_else(|| {
            TopicRuntimeError::TopicNotFound {
                topic: topic_name.clone(),
            }
        })?;

        let balanced_on_full = options
            .balanced_on_full_override
            .unwrap_or_else(|| topic.balanced_on_full());

        let frozen_queues = if matches!(
            options.route_mode,
            TopicPublisherRouteMode::FrozenBalancedOnly
        ) {
            Some(topic.queue_snapshot())
        } else {
            None
        };

        Ok(Arc::new(InMemoryTopicPublisherV2 {
            topic,
            balanced_on_full,
            outcome_interest: options.outcome_interest,
            report_mode: options.report_mode,
            route_mode: options.route_mode,
            frozen_queues,
        }))
    }

    fn build_subscriber(
        &self,
        topic_name: &TopicName,
        subscription: TopicSubscription,
    ) -> Result<InMemorySubscriberV2<T>, TopicRuntimeError> {
        let state = self.read_state()?;
        let topic = state.topics.get(topic_name).cloned().ok_or_else(|| {
            TopicRuntimeError::TopicNotFound {
                topic: topic_name.clone(),
            }
        })?;
        drop(state);

        match subscription {
            TopicSubscription::Balanced { group } => Ok(InMemorySubscriberV2 {
                inner: topic.subscribe_balanced(group),
            }),
            TopicSubscription::Broadcast => Err(TopicRuntimeError::UnsupportedTopicPolicy {
                topic: topic_name.clone(),
                backend: self.capabilities().backend_name,
                policy: "subscription.mode",
                value: "broadcast".to_owned(),
            }),
        }
    }

    /// Creates a concrete publisher handle.
    pub async fn publisher(
        &self,
        topic_name: &TopicName,
        options: TopicPublisherOptions,
    ) -> Result<InMemoryPublisherV2<T>, TopicRuntimeError> {
        Ok(InMemoryPublisherV2 {
            inner: self.build_publisher(topic_name, options)?,
        })
    }

    /// Creates a concrete subscriber handle.
    pub async fn subscribe(
        &self,
        topic_name: &TopicName,
        subscription: TopicSubscription,
    ) -> Result<InMemorySubscriberV2<T>, TopicRuntimeError> {
        self.build_subscriber(topic_name, subscription)
    }
}

#[async_trait]
impl<T> TopicRuntime<T> for InMemoryTopicRuntimeV2<T>
where
    T: Clone + Send + 'static,
{
    fn capabilities(&self) -> TopicRuntimeCapabilities {
        TopicRuntimeCapabilities {
            backend_name: "in_memory_v2_native_balanced",
            supports_broadcast_on_lag_drop_oldest: true,
            supports_broadcast_on_lag_disconnect: false,
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
            Arc::new(InMemoryTopicV2::new(topic_name, policies)),
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

#[cfg(test)]
mod tests {
    use super::InMemoryTopicRuntimeV2;
    use crate::topic::{
        TopicOutcomeInterest, TopicPublishOutcome, TopicPublishReportMode, TopicPublisherOptions,
        TopicRuntime, TopicSubscription,
    };
    use otap_df_config::TopicName;
    use otap_df_config::topic::{
        SubscriptionGroupName, TopicBackend, TopicBalancedOnFullPolicy, TopicBroadcastOnLagPolicy,
        TopicPolicies,
    };

    fn topic_name(raw: &str) -> TopicName {
        TopicName::parse(raw).expect("topic name should be valid")
    }

    fn group_name(raw: &str) -> SubscriptionGroupName {
        SubscriptionGroupName::parse(raw).expect("group should be valid")
    }

    fn policies() -> TopicPolicies {
        TopicPolicies {
            balanced_group_queue_capacity: 8,
            balanced_on_full: TopicBalancedOnFullPolicy::Block,
            broadcast_subscriber_queue_capacity: 8,
            broadcast_on_lag: TopicBroadcastOnLagPolicy::DropOldest,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v2_balanced_roundtrip_fast_publish() {
        let runtime = InMemoryTopicRuntimeV2::<u64>::new();
        let topic = topic_name("v2");
        runtime
            .create_topic(topic.clone(), TopicBackend::InMemory, policies())
            .await
            .expect("topic should be created");

        let mut subscriber = runtime
            .subscribe(
                &topic,
                TopicSubscription::Balanced {
                    group: group_name("workers"),
                },
            )
            .await
            .expect("subscriber should be created");

        let publisher = runtime
            .publisher(
                &topic,
                TopicPublisherOptions {
                    outcome_interest: TopicOutcomeInterest::None,
                    report_mode: TopicPublishReportMode::Minimal,
                    ..TopicPublisherOptions::default()
                },
            )
            .await
            .expect("publisher should be created");

        publisher
            .publish_fast(11)
            .await
            .expect("publish should succeed");
        let delivery = subscriber.recv().await.expect("receive should succeed");
        assert_eq!(*delivery.payload(), 11);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn v2_balanced_outcome_ack() {
        let runtime = InMemoryTopicRuntimeV2::<u64>::new();
        let topic = topic_name("v2-outcome");
        runtime
            .create_topic(topic.clone(), TopicBackend::InMemory, policies())
            .await
            .expect("topic should be created");

        let mut subscriber = runtime
            .subscribe(
                &topic,
                TopicSubscription::Balanced {
                    group: group_name("workers"),
                },
            )
            .await
            .expect("subscriber should be created");

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

        let publish = publisher.publish(22).await.expect("publish should succeed");
        let delivery = subscriber.recv().await.expect("receive should succeed");
        delivery.ack().await.expect("ack should succeed");
        let outcome = publish
            .outcome
            .expect("outcome should be present")
            .await
            .expect("outcome should resolve");
        assert_eq!(outcome, TopicPublishOutcome::Ack);
    }
}
