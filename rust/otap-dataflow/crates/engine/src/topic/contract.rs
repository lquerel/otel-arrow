// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::{TopicDelivery, TopicRuntimeCapabilities, TopicRuntimeError};
use async_trait::async_trait;
use otap_df_config::TopicName;
use otap_df_config::topic::{
    SubscriptionGroupName, TopicBackend, TopicBalancedOnFullPolicy, TopicPolicies,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

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

/// Publisher interest in downstream delivery outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TopicOutcomeInterest {
    /// Publisher does not request downstream Ack/Nack outcome reporting.
    #[default]
    None,
    /// Publisher requests an Ack outcome.
    Ack,
    /// Publisher requests a Nack outcome.
    Nack,
    /// Publisher requests either Ack or Nack outcome.
    AckOrNack,
}

impl TopicOutcomeInterest {
    /// Returns `true` when this value requests any outcome reporting.
    #[must_use]
    pub const fn is_enabled(self) -> bool {
        !matches!(self, Self::None)
    }
}

/// Structured Nack details for topic publish outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicOutcomeNack {
    /// Human-readable reason.
    pub reason: String,
    /// Whether this Nack is permanent.
    pub permanent: bool,
}

impl TopicOutcomeNack {
    /// Builds a non-permanent Nack outcome.
    #[must_use]
    pub fn transient(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            permanent: false,
        }
    }

    /// Builds a permanent Nack outcome.
    #[must_use]
    pub fn permanent(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            permanent: true,
        }
    }
}

/// Final publish outcome reported back to the publisher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicPublishOutcome {
    /// All relevant downstream deliveries acknowledged.
    Ack,
    /// At least one downstream delivery rejected.
    Nack(TopicOutcomeNack),
}

/// Asynchronous publisher outcome handle.
pub type TopicPublishOutcomeFuture =
    Pin<Box<dyn Future<Output = Result<TopicPublishOutcome, TopicRuntimeError>> + Send + 'static>>;

/// Full result returned by publisher-side publish operations.
pub struct TopicPublishResult {
    /// Enqueue-time delivery report snapshot.
    ///
    /// When publisher options use `report_mode=Minimal`, this report can be
    /// intentionally partial or unavailable (zeroed fields) to reduce overhead.
    pub report: TopicPublishReport,
    /// Optional asynchronous downstream Ack/Nack outcome.
    ///
    /// Present when `TopicPublisherOptions::outcome_interest` is enabled.
    pub outcome: Option<TopicPublishOutcomeFuture>,
}

/// Publisher options used to create a topic publisher handle.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopicPublisherOptions {
    /// Optional local override for balanced queue full behavior.
    pub balanced_on_full_override: Option<TopicBalancedOnFullPolicy>,
    /// Publisher interest for downstream Ack/Nack outcomes.
    pub outcome_interest: TopicOutcomeInterest,
    /// Publish report collection mode.
    pub report_mode: TopicPublishReportMode,
    /// Route snapshot mode.
    pub route_mode: TopicPublisherRouteMode,
}

/// Publish report collection mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TopicPublishReportMode {
    /// Return full per-publish enqueue report.
    #[default]
    Full,
    /// Allow partial/unavailable report fields for lower overhead.
    Minimal,
}

/// Publisher route resolution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TopicPublisherRouteMode {
    /// Resolve destinations dynamically at publish time.
    #[default]
    Dynamic,
    /// Freeze balanced-group destinations at publisher creation.
    ///
    /// For in-memory backend this mode does not include broadcast fan-out and
    /// is intended for balanced-only hot paths.
    FrozenBalancedOnly,
}

/// Publisher-side API for runtime topics.
#[async_trait]
pub trait TopicPublisher<T>: Send + Sync {
    /// Publishes a message and returns enqueue-time report plus optional outcome handle.
    async fn publish(&self, payload: T) -> Result<TopicPublishResult, TopicRuntimeError>;
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

    /// Creates a runtime topic for the given backend selector.
    async fn create_topic(
        &self,
        topic_name: TopicName,
        backend: TopicBackend,
        policies: TopicPolicies,
    ) -> Result<(), TopicRuntimeError>;

    /// Creates a publisher handle for an existing topic.
    async fn publisher(
        &self,
        topic_name: &TopicName,
        options: TopicPublisherOptions,
    ) -> Result<Arc<dyn TopicPublisher<T>>, TopicRuntimeError>;

    /// Creates a subscriber handle for an existing topic.
    async fn subscribe(
        &self,
        topic_name: &TopicName,
        subscription: TopicSubscription,
    ) -> Result<Box<dyn TopicSubscriber<T>>, TopicRuntimeError>;
}
