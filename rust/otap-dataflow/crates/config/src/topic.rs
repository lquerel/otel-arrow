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
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct TopicPolicy {
    /// Buffer behavior for the topic.
    #[serde(default)]
    pub buffer: TopicBufferPolicy,
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
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum TopicOverflowPolicy {
    /// Discard the incoming signal.
    DropNewest,
    /// Block the publisher until space is available.
    #[default]
    Block,
    /// Return an error to the publisher.
    Error,
}