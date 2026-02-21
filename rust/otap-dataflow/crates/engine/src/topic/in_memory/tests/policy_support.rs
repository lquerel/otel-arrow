// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Scenario:
/// 1. Given topic creation targeting the in-memory backend.
/// 2. When `broadcast_on_lag=disconnect` is configured.
/// 3. Then creation fails fast with `UnsupportedTopicPolicy`.
#[tokio::test(flavor = "current_thread")]
async fn create_topic_rejects_unsupported_broadcast_disconnect_policy() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("broadcast-disconnect");
    let err = runtime
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
        .expect_err("disconnect policy should be rejected for this backend");

    assert!(matches!(
        err,
        TopicRuntimeError::UnsupportedTopicPolicy {
            topic: _,
            backend: "in_memory_tokio_broadcast",
            policy: "broadcast_on_lag",
            ..
        }
    ));
}

/// Scenario:
/// 1. Given the in-memory topic runtime.
/// 2. When a topic is created with a different backend selector.
/// 3. Then creation fails fast with `UnsupportedTopicBackend`.
#[tokio::test(flavor = "current_thread")]
async fn create_topic_rejects_non_in_memory_backend() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("quiver-topic");

    let err = runtime
        .create_topic(
            topic.clone(),
            TopicBackend::Quiver,
            policies(
                8,
                TopicBalancedOnFullPolicy::Block,
                8,
                TopicBroadcastOnLagPolicy::DropOldest,
            ),
        )
        .await
        .expect_err("non-in-memory backend selector should be rejected");

    assert!(matches!(
        err,
        TopicRuntimeError::UnsupportedTopicBackend {
            topic: _,
            runtime: "in_memory_tokio_broadcast",
            backend: TopicBackend::Quiver,
        }
    ));
}
