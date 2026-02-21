// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::time::Duration;
use tokio::time::timeout;

/// Scenario:
/// 1. Given two broadcast subscribers on the same topic.
/// 2. When a message is published and one subscriber NACKs it.
/// 3. Then both subscribers receive the original message, and only the NACKing subscriber sees redelivery.
#[tokio::test(flavor = "current_thread")]
async fn broadcast_delivers_to_all_and_nack_is_subscriber_local() {
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
        .publisher(&topic, TopicPublisherOptions::default())
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

    let _publish = publisher.publish(42).await.expect("publish should succeed");

    let delivery_1 = subscriber_1.recv().await.expect("recv should succeed");
    let delivery_2 = subscriber_2.recv().await.expect("recv should succeed");
    assert_eq!(*delivery_1.payload(), 42);
    assert_eq!(*delivery_2.payload(), 42);

    delivery_1.nack().await.expect("nack should succeed");
    delivery_2.ack().await.expect("ack should succeed");

    let redelivery = subscriber_1
        .recv()
        .await
        .expect("nacked message should be re-delivered");
    assert_eq!(*redelivery.payload(), 42);
    redelivery.ack().await.expect("ack should succeed");

    assert!(
        timeout(Duration::from_millis(50), subscriber_2.recv())
            .await
            .is_err(),
        "second subscriber should not receive redelivery triggered by first subscriber nack"
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
