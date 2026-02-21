// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::time::Duration;
use tokio::time::timeout;

/// Scenario:
/// 1. Given two broadcast subscribers on the same topic and publisher outcome tracking enabled.
/// 2. When one subscriber NACKs and the other ACKs the same delivery.
/// 3. Then publish outcome resolves to Nack and there is no subscriber-local redelivery.
#[tokio::test(flavor = "current_thread")]
async fn broadcast_nack_resolves_publish_outcome_without_local_redelivery() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("broadcast");
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
    let mut subscriber_1 = runtime
        .subscribe(&topic, TopicSubscription::Broadcast)
        .await
        .expect("first broadcast subscriber should be created");
    let mut subscriber_2 = runtime
        .subscribe(&topic, TopicSubscription::Broadcast)
        .await
        .expect("second broadcast subscriber should be created");

    let publish = publisher.publish(42).await.expect("publish should succeed");
    let outcome = publish.outcome.expect("outcome should be available");

    let delivery_1 = subscriber_1.recv().await.expect("recv should succeed");
    let delivery_2 = subscriber_2.recv().await.expect("recv should succeed");
    assert_eq!(*delivery_1.payload(), 42);
    assert_eq!(*delivery_2.payload(), 42);

    delivery_1
        .nack_with_reason(TopicOutcomeNack::transient("subscriber rejected"))
        .await
        .expect("nack should succeed");
    delivery_2.ack().await.expect("ack should succeed");

    let resolved = timeout(Duration::from_millis(200), outcome)
        .await
        .expect("outcome should resolve in time")
        .expect("outcome should resolve successfully");
    assert_eq!(
        resolved,
        TopicPublishOutcome::Nack(TopicOutcomeNack::transient("subscriber rejected"))
    );

    assert!(
        timeout(Duration::from_millis(50), subscriber_1.recv())
            .await
            .is_err(),
        "broadcast nack should not trigger subscriber-local redelivery"
    );
    assert!(
        timeout(Duration::from_millis(50), subscriber_2.recv())
            .await
            .is_err(),
        "broadcast ack should not trigger additional delivery"
    );
}

/// Scenario:
/// 1. Given broadcast subscribers with per-subscriber capacity 1 and `drop_oldest`.
/// 2. When one subscriber lags and a newer message arrives.
/// 3. Then the lagging subscriber drops its oldest queued item and receives the latest message.
#[tokio::test(flavor = "current_thread")]
async fn broadcast_drop_oldest_keeps_newest_for_lagging_subscriber() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("broadcast-drop-oldest");
    runtime
        .create_topic(
            topic.clone(),
            TopicBackend::InMemory,
            policies(
                8,
                TopicBalancedOnFullPolicy::Block,
                1,
                TopicBroadcastOnLagPolicy::DropOldest,
            ),
        )
        .await
        .expect("topic should be created");

    let publisher = runtime
        .publisher(&topic, TopicPublisherOptions::default())
        .await
        .expect("publisher should be created");
    let mut fast_subscriber = runtime
        .subscribe(&topic, TopicSubscription::Broadcast)
        .await
        .expect("fast subscriber should be created");
    let mut slow_subscriber = runtime
        .subscribe(&topic, TopicSubscription::Broadcast)
        .await
        .expect("slow subscriber should be created");

    let _first_publish = publisher
        .publish(1)
        .await
        .expect("first publish should succeed");

    let fast_first = fast_subscriber
        .recv()
        .await
        .expect("fast subscriber should receive first message");
    assert_eq!(*fast_first.payload(), 1);
    fast_first.ack().await.expect("ack should succeed");

    let second_report = publisher
        .publish(2)
        .await
        .expect("second publish should succeed");
    assert_eq!(second_report.report.delivered_subscribers, 2);
    assert_eq!(second_report.report.dropped_subscribers, 0);

    let fast_second = fast_subscriber
        .recv()
        .await
        .expect("fast subscriber should receive second message");
    assert_eq!(*fast_second.payload(), 2);
    fast_second.ack().await.expect("ack should succeed");

    let slow_latest = slow_subscriber
        .recv()
        .await
        .expect("slow subscriber should receive latest message after lag handling");
    assert_eq!(*slow_latest.payload(), 2);
    slow_latest.ack().await.expect("ack should succeed");
}

/// Scenario:
/// 1. Given broadcast outcome tracking and one lagging subscriber.
/// 2. When the lagging subscriber overflows its per-subscriber queue under `drop_oldest`.
/// 3. Then the dropped older publish outcome resolves to Nack.
#[tokio::test(flavor = "current_thread")]
async fn broadcast_lag_nacks_older_publish_outcome() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("broadcast-outcome-lag");
    runtime
        .create_topic(
            topic.clone(),
            TopicBackend::InMemory,
            policies(
                8,
                TopicBalancedOnFullPolicy::Block,
                1,
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
    let mut fast_subscriber = runtime
        .subscribe(&topic, TopicSubscription::Broadcast)
        .await
        .expect("fast subscriber should be created");
    let mut slow_subscriber = runtime
        .subscribe(&topic, TopicSubscription::Broadcast)
        .await
        .expect("slow subscriber should be created");

    let first_publish = publisher
        .publish(1)
        .await
        .expect("first publish should succeed");
    let first_outcome = first_publish
        .outcome
        .expect("first publish outcome should be available");

    let fast_first = fast_subscriber
        .recv()
        .await
        .expect("fast subscriber should receive first message");
    assert_eq!(*fast_first.payload(), 1);
    fast_first.ack().await.expect("ack should succeed");

    // Slow subscriber does not receive the first message before second publish.
    let second_publish = publisher
        .publish(2)
        .await
        .expect("second publish should succeed");
    let second_outcome = second_publish
        .outcome
        .expect("second publish outcome should be available");

    let first_resolved = timeout(Duration::from_millis(200), first_outcome)
        .await
        .expect("first outcome should resolve in time")
        .expect("first outcome should resolve successfully");
    assert_eq!(
        first_resolved,
        TopicPublishOutcome::Nack(TopicOutcomeNack::transient(
            "broadcast subscriber lagged: drop_oldest"
        ))
    );

    let fast_second = fast_subscriber
        .recv()
        .await
        .expect("fast subscriber should receive second message");
    assert_eq!(*fast_second.payload(), 2);
    fast_second.ack().await.expect("ack should succeed");

    let slow_second = slow_subscriber
        .recv()
        .await
        .expect("slow subscriber should receive second message");
    assert_eq!(*slow_second.payload(), 2);
    slow_second.ack().await.expect("ack should succeed");

    let second_resolved = timeout(Duration::from_millis(200), second_outcome)
        .await
        .expect("second outcome should resolve in time")
        .expect("second outcome should resolve successfully");
    assert_eq!(second_resolved, TopicPublishOutcome::Ack);
}

/// Scenario:
/// 1. Given broadcast outcome tracking with one active subscriber.
/// 2. When the subscriber is dropped before settling the delivery.
/// 3. Then publish outcome resolves to Nack.
#[tokio::test(flavor = "current_thread")]
async fn broadcast_subscriber_drop_nacks_pending_outcome() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("broadcast-outcome-drop");
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
    let subscriber = runtime
        .subscribe(&topic, TopicSubscription::Broadcast)
        .await
        .expect("broadcast subscriber should be created");

    let publish = publisher.publish(7).await.expect("publish should succeed");
    let outcome = publish.outcome.expect("outcome should be available");

    drop(subscriber);

    let resolved = timeout(Duration::from_millis(200), outcome)
        .await
        .expect("outcome should resolve in time")
        .expect("outcome should resolve successfully");
    assert_eq!(
        resolved,
        TopicPublishOutcome::Nack(TopicOutcomeNack::transient("broadcast subscriber dropped"))
    );
}
