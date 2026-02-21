// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Scenario:
/// 1. Given an empty runtime and a valid topic specification.
/// 2. When the same topic is created twice.
/// 3. Then the second create call returns `TopicAlreadyExists`.
#[tokio::test(flavor = "current_thread")]
async fn rejects_duplicate_topic_creation() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("raw");
    let policies = policies(
        8,
        TopicBalancedOnFullPolicy::Block,
        8,
        TopicBroadcastOnLagPolicy::DropOldest,
    );

    runtime
        .create_topic(topic.clone(), TopicBackend::InMemory, policies.clone())
        .await
        .expect("first create should succeed");
    let err = runtime
        .create_topic(topic.clone(), TopicBackend::InMemory, policies)
        .await
        .expect_err("second create should fail");
    assert_eq!(err, TopicRuntimeError::TopicAlreadyExists { topic });
}
