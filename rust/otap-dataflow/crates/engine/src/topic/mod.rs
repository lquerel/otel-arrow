// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Backend-agnostic runtime topic API.
//!
//! This module defines the public abstraction layer used by topic-aware nodes.
//! Backends can be in-memory (for single-process deployments) or persistent/distributed
//! (for example Kafka/Quiver in future work).

use async_trait::async_trait;
use otap_df_config::TopicName;
use otap_df_config::topic::{
    SubscriptionGroupName, TopicBalancedOnFullPolicy, TopicBroadcastOnLagPolicy, TopicPolicies,
};
use std::sync::Arc;

pub mod in_memory;

pub use in_memory::InMemoryTopicRuntime;

/// Subscription semantic requested by a topic receiver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicSubscription {
    /// Each subscriber receives an independent full stream.
    Broadcast,
    /// Subscribers in the same group compete for one shared stream.
    Balanced {
        /// Balanced group name.
        group: SubscriptionGroupName,
    },
}

/// Publish result snapshot returned by a topic publisher.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TopicPublishReport {
    /// Number of destination queues evaluated for this publish.
    pub attempted_subscribers: usize,
    /// Number of destination queues that accepted this message.
    pub delivered_subscribers: usize,
    /// Number of destination queues that dropped this message.
    pub dropped_subscribers: usize,
}

impl TopicPublishReport {
    /// Returns `true` when this publish dropped at least one destination delivery.
    #[must_use]
    pub const fn has_drops(&self) -> bool {
        self.dropped_subscribers > 0
    }
}

/// Backend capability contract for runtime topic semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicRuntimeCapabilities {
    /// Backend identifier.
    pub backend_name: &'static str,
    /// Support for `broadcast_on_lag: drop_oldest`.
    pub supports_broadcast_on_lag_drop_oldest: bool,
    /// Support for `broadcast_on_lag: disconnect`.
    pub supports_broadcast_on_lag_disconnect: bool,
}

impl TopicRuntimeCapabilities {
    /// Returns whether this backend supports the given broadcast lag policy.
    #[must_use]
    pub const fn supports_broadcast_on_lag(&self, policy: &TopicBroadcastOnLagPolicy) -> bool {
        match policy {
            TopicBroadcastOnLagPolicy::DropOldest => self.supports_broadcast_on_lag_drop_oldest,
            TopicBroadcastOnLagPolicy::Disconnect => self.supports_broadcast_on_lag_disconnect,
        }
    }
}

pub(crate) fn validate_topic_policy_support(
    topic_name: &TopicName,
    policies: &TopicPolicies,
    capabilities: TopicRuntimeCapabilities,
) -> Result<(), TopicRuntimeError> {
    if !capabilities.supports_broadcast_on_lag(&policies.broadcast_on_lag) {
        return Err(TopicRuntimeError::UnsupportedTopicPolicy {
            topic: topic_name.clone(),
            backend: capabilities.backend_name,
            policy: "broadcast_on_lag",
            value: broadcast_on_lag_policy_value(&policies.broadcast_on_lag).to_owned(),
        });
    }
    Ok(())
}

fn broadcast_on_lag_policy_value(policy: &TopicBroadcastOnLagPolicy) -> &'static str {
    match policy {
        TopicBroadcastOnLagPolicy::DropOldest => "drop_oldest",
        TopicBroadcastOnLagPolicy::Disconnect => "disconnect",
    }
}

/// Errors produced by runtime topic operations.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum TopicRuntimeError {
    /// Topic creation failed because the topic already exists.
    #[error("topic `{topic}` already exists")]
    TopicAlreadyExists {
        /// Existing topic name.
        topic: TopicName,
    },
    /// Topic lookup failed.
    #[error("topic `{topic}` does not exist")]
    TopicNotFound {
        /// Missing topic name.
        topic: TopicName,
    },
    /// Topic configuration is invalid for runtime creation.
    #[error("invalid runtime topic configuration for `{topic}`: {reason}")]
    InvalidTopicConfig {
        /// Topic name that failed validation.
        topic: TopicName,
        /// Validation error details.
        reason: String,
    },
    /// Topic policy is not supported by this runtime backend.
    #[error("unsupported topic policy for `{topic}` on backend `{backend}`: `{policy}={value}`")]
    UnsupportedTopicPolicy {
        /// Topic name where policy is unsupported.
        topic: TopicName,
        /// Backend name.
        backend: &'static str,
        /// Policy key.
        policy: &'static str,
        /// Unsupported policy value.
        value: String,
    },
    /// Runtime channel for the topic was unexpectedly closed.
    #[error("topic runtime channel unexpectedly closed for `{topic}`")]
    ChannelClosed {
        /// Topic name for the closed channel.
        topic: TopicName,
    },
    /// Internal synchronization failure.
    #[error("internal topic runtime error: {message}")]
    Internal {
        /// Additional internal error context.
        message: String,
    },
}

/// Publisher-side API for runtime topics.
#[async_trait]
pub trait TopicPublisher<T>: Send + Sync {
    /// Publishes a message and returns the destination-level delivery report.
    async fn publish(&self, payload: T) -> Result<TopicPublishReport, TopicRuntimeError>;
}

/// Subscriber-side API for runtime topics.
#[async_trait]
pub trait TopicSubscriber<T>: Send {
    /// Receives the next available topic delivery.
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError>;
}

/// Backend-agnostic runtime topic service.
#[async_trait]
pub trait TopicRuntime<T>: Send + Sync
where
    T: Clone + Send + 'static,
{
    /// Returns backend capability declarations.
    fn capabilities(&self) -> TopicRuntimeCapabilities;

    /// Creates a runtime topic.
    async fn create_topic(
        &self,
        topic_name: TopicName,
        policies: TopicPolicies,
    ) -> Result<(), TopicRuntimeError>;

    /// Creates a publisher handle for an existing topic.
    ///
    /// `balanced_on_full_override`, when present, overrides the topic-level
    /// `balanced_on_full` policy for this publisher.
    async fn publisher(
        &self,
        topic_name: &TopicName,
        balanced_on_full_override: Option<TopicBalancedOnFullPolicy>,
    ) -> Result<Arc<dyn TopicPublisher<T>>, TopicRuntimeError>;

    /// Creates a subscriber handle for an existing topic.
    async fn subscribe(
        &self,
        topic_name: &TopicName,
        subscription: TopicSubscription,
    ) -> Result<Box<dyn TopicSubscriber<T>>, TopicRuntimeError>;
}

#[async_trait]
pub(crate) trait DeliveryAckHandler<T>: Send {
    /// Resolves delivery as acknowledged.
    async fn ack(self: Box<Self>, payload: T) -> Result<(), TopicRuntimeError>;
    /// Resolves delivery as rejected.
    async fn nack(self: Box<Self>, payload: T) -> Result<(), TopicRuntimeError>;
}

/// Delivered message wrapper that carries acknowledgement operations.
pub struct TopicDelivery<T> {
    payload: Option<T>,
    ack_handler: Option<Box<dyn DeliveryAckHandler<T>>>,
}

impl<T> TopicDelivery<T> {
    /// Creates a runtime delivery object.
    pub(crate) fn new(payload: T, ack_handler: Box<dyn DeliveryAckHandler<T>>) -> Self {
        Self {
            payload: Some(payload),
            ack_handler: Some(ack_handler),
        }
    }

    /// Returns the delivered payload.
    #[must_use]
    pub fn payload(&self) -> &T {
        self.payload
            .as_ref()
            .expect("payload should be present until ack/nack consumes the delivery")
    }

    /// Resolves this delivery as acknowledged.
    pub async fn ack(mut self) -> Result<(), TopicRuntimeError> {
        let payload = self
            .payload
            .take()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: "delivery payload missing during ack".to_owned(),
            })?;
        let handler = self
            .ack_handler
            .take()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: "delivery ack handler missing during ack".to_owned(),
            })?;
        handler.ack(payload).await
    }

    /// Resolves this delivery as rejected.
    pub async fn nack(mut self) -> Result<(), TopicRuntimeError> {
        let payload = self
            .payload
            .take()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: "delivery payload missing during nack".to_owned(),
            })?;
        let handler = self
            .ack_handler
            .take()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: "delivery ack handler missing during nack".to_owned(),
            })?;
        handler.nack(payload).await
    }
}
