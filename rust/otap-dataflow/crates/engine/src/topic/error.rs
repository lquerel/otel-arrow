// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otap_df_config::TopicName;
use otap_df_config::topic::TopicBackend;

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
    /// Topic backend selector is not supported by this runtime.
    #[error("unsupported topic backend for `{topic}` on runtime `{runtime}`: `{backend}`")]
    UnsupportedTopicBackend {
        /// Topic name where backend is unsupported.
        topic: TopicName,
        /// Runtime name handling this request.
        runtime: &'static str,
        /// Backend selector from topic declaration.
        backend: TopicBackend,
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
