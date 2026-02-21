// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::time::Duration;
use tokio::time::timeout;

/// Scenario:
/// 1. Given a balanced publisher with outcome interest enabled.
/// 2. When the receiver acknowledges the delivery.
/// 3. Then publisher outcome resolves to Ack.
#[tokio::test(flavor = "current_thread")]
async fn balanced_outcome_resolves_ack_when_delivery_is_acked() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("balanced-outcome-ack");
    runtime
        .create_topic(
            topic.clone(),
            TopicBackend::InMemory,
            policies(
                8,
                TopicBalancedOnFullPolicy::Block,
                8,
                TopicBroadcastOnLagPolicy::DropOldest,
            ),
        )
        .await
        .expect("topic should be created");

    let publisher = runtime
        .publisher(
            &topic,
            TopicPublisherOptions {
                outcome_interest: TopicOutcomeInterest::AckOrNack,
                ..TopicPublisherOptions::default()
            },
        )
        .await
        .expect("publisher should be created");
    let mut subscriber = runtime
        .subscribe(
            &topic,
            TopicSubscription::Balanced {
                group: group_name("workers"),
            },
        )
        .await
        .expect("balanced subscriber should be created");

    let publish_result = publisher.publish(10).await.expect("publish should succeed");
    let outcome = publish_result
        .outcome
        .expect("publish outcome should be available");
    let delivery = subscriber.recv().await.expect("message should be received");
    assert_eq!(
        delivery.publisher_outcome_interest(),
        TopicOutcomeInterest::AckOrNack
    );
    delivery.ack().await.expect("ack should succeed");

    let resolved = timeout(Duration::from_millis(200), outcome)
        .await
        .expect("outcome should resolve in time")
        .expect("outcome should resolve successfully");
    assert_eq!(resolved, TopicPublishOutcome::Ack);
}

/// Scenario:
/// 1. Given a balanced publisher with outcome interest enabled.
/// 2. When the receiver rejects the delivery with a reason.
/// 3. Then publisher outcome resolves to Nack with that reason.
#[tokio::test(flavor = "current_thread")]
async fn balanced_outcome_resolves_nack_when_delivery_is_nacked() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("balanced-outcome-nack");
    runtime
        .create_topic(
            topic.clone(),
            TopicBackend::InMemory,
            policies(
                8,
                TopicBalancedOnFullPolicy::Block,
                8,
                TopicBroadcastOnLagPolicy::DropOldest,
            ),
        )
        .await
        .expect("topic should be created");

    let publisher = runtime
        .publisher(
            &topic,
            TopicPublisherOptions {
                outcome_interest: TopicOutcomeInterest::AckOrNack,
                ..TopicPublisherOptions::default()
            },
        )
        .await
        .expect("publisher should be created");
    let mut subscriber = runtime
        .subscribe(
            &topic,
            TopicSubscription::Balanced {
                group: group_name("workers"),
            },
        )
        .await
        .expect("balanced subscriber should be created");

    let publish_result = publisher.publish(99).await.expect("publish should succeed");
    let outcome = publish_result
        .outcome
        .expect("publish outcome should be available");
    let delivery = subscriber.recv().await.expect("message should be received");
    delivery
        .nack_with_reason(TopicOutcomeNack::transient("downstream rejected"))
        .await
        .expect("nack should succeed");

    let resolved = timeout(Duration::from_millis(200), outcome)
        .await
        .expect("outcome should resolve in time")
        .expect("outcome should resolve successfully");
    assert_eq!(
        resolved,
        TopicPublishOutcome::Nack(TopicOutcomeNack::transient("downstream rejected"))
    );
}
