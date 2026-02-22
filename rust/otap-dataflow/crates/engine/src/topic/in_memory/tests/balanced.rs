// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Scenario:
/// 1. Given one balanced subscriber that receives a message.
/// 2. When that delivery is NACKed.
/// 3. Then the topic runtime does not implicitly requeue/redeliver it.
#[tokio::test(flavor = "current_thread")]
async fn balanced_nack_does_not_implicitly_requeue() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("raw");
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
    let mut subscriber = runtime
        .subscribe(
            &topic,
            TopicSubscription::Balanced {
                group: group_name("workers"),
            },
        )
        .await
        .expect("balanced subscriber should be created");

    let first_publish = publisher.publish(10).await.expect("publish should succeed");
    assert_eq!(first_publish.report.delivered_subscribers, 1);
    assert_eq!(first_publish.report.dropped_subscribers, 0);

    let first_delivery = subscriber.recv().await.expect("message should be received");
    assert_eq!(*first_delivery.payload(), 10);
    first_delivery.nack().await.expect("nack should succeed");

    assert!(
        timeout(Duration::from_millis(50), subscriber.recv())
            .await
            .is_err(),
        "balanced NACK should not implicitly requeue in the topic runtime"
    );
}

/// Scenario:
/// 1. Given two balanced subscribers in the same group.
/// 2. When two messages are published.
/// 3. Then the shared group queue distributes one message to each subscriber.
#[tokio::test(flavor = "current_thread")]
async fn balanced_group_is_shared_across_subscribers() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("shared");
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
        .subscribe(
            &topic,
            TopicSubscription::Balanced {
                group: group_name("workers"),
            },
        )
        .await
        .expect("first balanced subscriber should be created");
    let mut subscriber_2 = runtime
        .subscribe(
            &topic,
            TopicSubscription::Balanced {
                group: group_name("workers"),
            },
        )
        .await
        .expect("second balanced subscriber should be created");

    let _publish_1 = publisher.publish(1).await.expect("publish should succeed");
    let _publish_2 = publisher.publish(2).await.expect("publish should succeed");

    let delivery_1 = subscriber_1
        .recv()
        .await
        .expect("first recv should succeed");
    let delivery_2 = subscriber_2
        .recv()
        .await
        .expect("second recv should succeed");

    let mut values = vec![*delivery_1.payload(), *delivery_2.payload()];
    values.sort_unstable();
    assert_eq!(values, vec![1, 2]);

    delivery_1.ack().await.expect("ack should succeed");
    delivery_2.ack().await.expect("ack should succeed");
}

/// Scenario:
/// 1. Given a balanced group queue with capacity 1 and `drop_newest`.
/// 2. When a second message is published while the first is still queued.
/// 3. Then the second message is dropped and only the first is delivered.
#[tokio::test(flavor = "current_thread")]
async fn drop_newest_drops_when_balanced_queue_is_full() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("drop-newest");
    runtime
        .create_topic(
            topic.clone(),
            TopicBackend::InMemory,
            policies(
                1,
                TopicBalancedOnFullPolicy::DropNewest,
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
    let mut subscriber = runtime
        .subscribe(
            &topic,
            TopicSubscription::Balanced {
                group: group_name("workers"),
            },
        )
        .await
        .expect("balanced subscriber should be created");

    let first = publisher
        .publish(1)
        .await
        .expect("first publish should succeed");
    assert_eq!(first.report.delivered_subscribers, 1);
    assert_eq!(first.report.dropped_subscribers, 0);

    let second = publisher
        .publish(2)
        .await
        .expect("second publish should succeed");
    assert_eq!(second.report.delivered_subscribers, 0);
    assert_eq!(second.report.dropped_subscribers, 1);

    let delivery = subscriber
        .recv()
        .await
        .expect("first value should be delivered");
    assert_eq!(*delivery.payload(), 1);
    delivery.ack().await.expect("ack should succeed");

    assert!(
        timeout(Duration::from_millis(50), subscriber.recv())
            .await
            .is_err(),
        "second value should be dropped when queue is full"
    );
}

/// Scenario:
/// 1. Given a balanced group queue with capacity 1 and `block`.
/// 2. When a second publish occurs while the queue is full.
/// 3. Then publish blocks until the subscriber drains one item, then succeeds.
#[tokio::test(flavor = "current_thread")]
async fn block_policy_applies_backpressure_until_space_is_available() {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("block");
    runtime
        .create_topic(
            topic.clone(),
            TopicBackend::InMemory,
            policies(
                1,
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
    let mut subscriber = runtime
        .subscribe(
            &topic,
            TopicSubscription::Balanced {
                group: group_name("workers"),
            },
        )
        .await
        .expect("balanced subscriber should be created");

    let _first_publish = publisher
        .publish(1)
        .await
        .expect("first publish should fit into queue");

    let publisher_for_task = publisher.clone();
    let publish_task = tokio::spawn(async move { publisher_for_task.publish(2).await });

    sleep(Duration::from_millis(25)).await;
    assert!(
        !publish_task.is_finished(),
        "second publish should block until the queue has space"
    );

    let first = subscriber
        .recv()
        .await
        .expect("first value should be available");
    assert_eq!(*first.payload(), 1);
    first.ack().await.expect("ack should succeed");

    let second_publish = timeout(Duration::from_secs(1), publish_task)
        .await
        .expect("blocked publish should complete after queue is drained")
        .expect("join should succeed")
        .expect("publish should succeed");
    assert_eq!(second_publish.report.delivered_subscribers, 1);
    assert_eq!(second_publish.report.dropped_subscribers, 0);

    let second = subscriber
        .recv()
        .await
        .expect("second value should be available");
    assert_eq!(*second.payload(), 2);
    second.ack().await.expect("ack should succeed");
}
