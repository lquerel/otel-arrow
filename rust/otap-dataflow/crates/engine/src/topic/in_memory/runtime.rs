// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::balanced::InMemoryTopicPublisher;
use super::topic_state::InMemoryTopic;
use crate::topic::{
    TopicPublisher, TopicPublisherOptions, TopicRuntime, TopicRuntimeCapabilities,
    TopicRuntimeError, TopicSubscriber, TopicSubscription, validate_topic_policy_support,
};
use async_trait::async_trait;
use otap_df_config::TopicName;
use otap_df_config::topic::{TopicBackend, TopicPolicies};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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
