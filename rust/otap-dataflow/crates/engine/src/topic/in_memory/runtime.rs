// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::balanced::{InMemoryBalancedSubscriber, InMemoryTopicPublisher};
use super::broadcast::InMemoryBroadcastSubscriber;
use super::topic_state::InMemoryTopic;
use crate::topic::{
    TopicDelivery, TopicPublisher, TopicPublisherOptions, TopicRuntime, TopicRuntimeCapabilities,
    TopicRuntimeError, TopicSubscriber, TopicSubscription, validate_topic_policy_support,
};
use async_trait::async_trait;
use otap_df_config::TopicName;
use otap_df_config::topic::{TopicBackend, TopicPolicies};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Concrete in-memory publisher handle using static dispatch.
pub struct InMemoryPublisher<T> {
    inner: Arc<InMemoryTopicPublisher<T>>,
}

impl<T> Clone for InMemoryPublisher<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> InMemoryPublisher<T>
where
    T: Clone + Send + 'static,
{
    /// Publishes one payload to the target topic.
    pub async fn publish(
        &self,
        payload: T,
    ) -> Result<crate::topic::TopicPublishResult, TopicRuntimeError> {
        self.inner.publish(payload).await
    }
}

/// Concrete in-memory subscriber handle using static dispatch.
pub struct InMemorySubscriber<T> {
    inner: InMemorySubscriberInner<T>,
}

enum InMemorySubscriberInner<T> {
    Balanced(InMemoryBalancedSubscriber<T>),
    Broadcast(InMemoryBroadcastSubscriber<T>),
}

impl<T> InMemorySubscriber<T>
where
    T: Clone + Send + 'static,
{
    /// Receives the next available topic delivery.
    pub async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        match &mut self.inner {
            InMemorySubscriberInner::Balanced(subscriber) => subscriber.recv().await,
            InMemorySubscriberInner::Broadcast(subscriber) => subscriber.recv().await,
        }
    }
}

#[async_trait]
impl<T> TopicSubscriber<T> for InMemorySubscriber<T>
where
    T: Clone + Send + 'static,
{
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        InMemorySubscriber::recv(self).await
    }
}

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

impl<T> InMemoryTopicRuntime<T>
where
    T: Clone + Send + 'static,
{
    fn build_publisher(
        &self,
        topic_name: &TopicName,
        options: TopicPublisherOptions,
    ) -> Result<Arc<InMemoryTopicPublisher<T>>, TopicRuntimeError> {
        let state = self.read_state()?;
        let topic = state.topics.get(topic_name).cloned().ok_or_else(|| {
            TopicRuntimeError::TopicNotFound {
                topic: topic_name.clone(),
            }
        })?;

        let balanced_on_full = options
            .balanced_on_full_override
            .unwrap_or_else(|| topic.balanced_on_full());

        Ok(Arc::new(InMemoryTopicPublisher {
            topic,
            balanced_on_full,
            outcome_interest: options.outcome_interest,
        }))
    }

    fn build_subscriber(
        &self,
        topic_name: &TopicName,
        subscription: TopicSubscription,
    ) -> Result<InMemorySubscriber<T>, TopicRuntimeError> {
        let state = self.read_state()?;
        let topic = state.topics.get(topic_name).cloned().ok_or_else(|| {
            TopicRuntimeError::TopicNotFound {
                topic: topic_name.clone(),
            }
        })?;
        drop(state);

        match subscription {
            TopicSubscription::Broadcast => Ok(InMemorySubscriber {
                inner: InMemorySubscriberInner::Broadcast(topic.subscribe_broadcast()?),
            }),
            TopicSubscription::Balanced { group } => Ok(InMemorySubscriber {
                inner: InMemorySubscriberInner::Balanced(topic.subscribe_balanced(group)?),
            }),
        }
    }

    /// Creates a concrete publisher handle for the in-memory backend.
    pub async fn publisher(
        &self,
        topic_name: &TopicName,
        options: TopicPublisherOptions,
    ) -> Result<InMemoryPublisher<T>, TopicRuntimeError> {
        Ok(InMemoryPublisher {
            inner: self.build_publisher(topic_name, options)?,
        })
    }

    /// Creates a concrete subscriber handle for the in-memory backend.
    pub async fn subscribe(
        &self,
        topic_name: &TopicName,
        subscription: TopicSubscription,
    ) -> Result<InMemorySubscriber<T>, TopicRuntimeError> {
        self.build_subscriber(topic_name, subscription)
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
