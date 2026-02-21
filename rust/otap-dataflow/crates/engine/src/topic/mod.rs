// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Backend-agnostic runtime topic API.
//!
//! This module defines the public abstraction layer used by topic-aware nodes.
//! Backends can be in-memory (for single-process deployments) or persistent/distributed
//! (for example Quiver in future work).

mod capabilities;
mod contract;
mod delivery;
mod error;
pub mod in_memory;

pub use contract::{
    TopicOutcomeInterest, TopicOutcomeNack, TopicPublishOutcome, TopicPublishOutcomeFuture,
    TopicPublishReport, TopicPublishResult, TopicPublisher, TopicPublisherOptions, TopicRuntime,
    TopicSubscriber, TopicSubscription,
};
pub use delivery::{TopicDelivery, TopicDeliveryOutcomeHandle};
pub use error::TopicRuntimeError;

pub use capabilities::TopicRuntimeCapabilities;
pub(crate) use capabilities::validate_topic_policy_support;
pub(crate) use delivery::DeliveryAckHandler;
pub use in_memory::InMemoryTopicRuntime;
