// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Topic configuration specification.

use crate::Description;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Topic configuration at any scope.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TopicConfig {
    /// Optional description of the topic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Description>,

    /// Topic policy definitions.
    #[serde(default)]
    pub policy: TopicPolicy,
}

/// Topic policy definitions.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TopicPolicy {
    /// Buffer behavior for the topic.
    #[serde(default)]
    pub buffer: TopicBufferPolicy,

    /// Slow consumer handling (not yet enforced).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slow_consumer: Option<TopicSlowConsumerPolicy>,

    /// Persistence settings (not yet enforced).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence: Option<TopicPersistencePolicy>,
}

impl Default for TopicPolicy {
    fn default() -> Self {
        Self {
            buffer: TopicBufferPolicy::default(),
            slow_consumer: None,
            persistence: None,
        }
    }
}

/// Buffer policy for a topic.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TopicBufferPolicy {
    /// Maximum number of signals buffered in memory.
    #[serde(default = "default_topic_buffer_capacity")]
    pub capacity: usize,

    /// Overflow behavior when the buffer is full.
    #[serde(default)]
    pub overflow: TopicOverflowPolicy,
}

impl Default for TopicBufferPolicy {
    fn default() -> Self {
        Self {
            capacity: default_topic_buffer_capacity(),
            overflow: TopicOverflowPolicy::default(),
        }
    }
}

fn default_topic_buffer_capacity() -> usize {
    100
}

/// Buffer overflow behavior.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TopicOverflowPolicy {
    /// Discard the oldest signals in the buffer.
    DropOldest,
    /// Discard the incoming signal.
    DropNewest,
    /// Block the publisher until space is available.
    Block,
    /// Return an error to the publisher.
    Error,
}

impl Default for TopicOverflowPolicy {
    fn default() -> Self {
        TopicOverflowPolicy::Block
    }
}

/// Slow consumer policy (parsed but not enforced yet).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TopicSlowConsumerPolicy {
    /// Lag threshold for considering a consumer as slow.
    pub lag_threshold: usize,

    /// Action to take when a slow consumer is detected.
    pub action: TopicSlowConsumerAction,
}

/// Actions taken for slow consumers.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TopicSlowConsumerAction {
    /// Log a warning and continue delivering.
    Warn,
    /// Drop signals for the slow consumer.
    Drop,
}

/// Persistence policy (parsed but not enforced yet).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TopicPersistencePolicy {
    /// Enable persistence for this topic.
    #[serde(default)]
    pub enabled: bool,

    /// Path for persistent storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,

    /// Sync mode configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_mode: Option<TopicPersistenceSyncMode>,

    /// Maximum total size of persisted data.
    #[serde(
        default,
        deserialize_with = "crate::byte_units::deserialize_u64",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_size: Option<u64>,

    /// Maximum segment size.
    #[serde(
        default,
        deserialize_with = "crate::byte_units::deserialize_u64",
        skip_serializing_if = "Option::is_none"
    )]
    pub segment_size: Option<u64>,

    /// Retention period.
    #[serde(default, with = "humantime_serde")]
    #[schemars(with = "Option<String>")]
    pub retention: Option<Duration>,
}

/// Sync mode for persistence.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TopicPersistenceSyncMode {
    /// Flush on a fixed interval.
    Interval {
        /// Interval between flushes.
        #[serde(with = "humantime_serde")]
        #[schemars(with = "String")]
        every: Duration,
    },
}
