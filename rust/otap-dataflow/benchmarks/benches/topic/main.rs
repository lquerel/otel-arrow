// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Benchmarks for topic runtime modes compared to underlying channel primitives.
//!
//! Scenarios:
//! - balanced (ack/nack outcomes) vs flume MPMC vs balanced (no ack/nack)
//! - broadcast (ack/nack outcomes) vs Tokio broadcast vs broadcast (no ack/nack)

#![allow(missing_docs)]

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use otap_df_config::TopicName;
use otap_df_config::topic::{
    SubscriptionGroupName, TopicBackend, TopicBalancedOnFullPolicy, TopicBroadcastOnLagPolicy,
    TopicPolicies,
};
use otap_df_engine::topic::{
    InMemoryTopicRuntime, TopicOutcomeInterest, TopicPublishOutcome, TopicPublisherOptions,
    TopicRuntime, TopicSubscription,
};
use std::hint::black_box;
use tokio::sync::broadcast;

#[cfg(not(windows))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(windows))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const BALANCED_MSG_COUNT: usize = 50_000;
const BALANCED_CHANNEL_CAPACITY: usize = 256;

const BROADCAST_MSG_COUNT: usize = 20_000;
const BROADCAST_SUBSCRIBERS: usize = 3;
const BROADCAST_CHANNEL_CAPACITY: usize = BROADCAST_MSG_COUNT + 1;

fn topic_name(raw: &str) -> TopicName {
    TopicName::parse(raw).expect("topic name should be valid")
}

fn subscription_group(raw: &str) -> SubscriptionGroupName {
    SubscriptionGroupName::parse(raw).expect("subscription group name should be valid")
}

fn topic_policies(balanced_capacity: usize, broadcast_capacity: usize) -> TopicPolicies {
    TopicPolicies {
        balanced_group_queue_capacity: balanced_capacity,
        balanced_on_full: TopicBalancedOnFullPolicy::Block,
        broadcast_subscriber_queue_capacity: broadcast_capacity,
        broadcast_on_lag: TopicBroadcastOnLagPolicy::DropOldest,
    }
}

async fn run_flume_mpmc_balanced(msg_count: usize, capacity: usize) {
    let (tx, rx) = flume::bounded::<u64>(capacity);
    let consumer = tokio::spawn(async move {
        let mut checksum = 0u64;
        for _ in 0..msg_count {
            let value = rx.recv_async().await.expect("receiver should remain open");
            checksum ^= value;
        }
        _ = black_box(checksum);
    });

    for i in 0..msg_count {
        tx.send_async(i as u64)
            .await
            .expect("sender should remain open");
    }
    drop(tx);
    consumer.await.expect("consumer task should complete");
}

async fn run_topic_balanced(msg_count: usize, with_outcome: bool) {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("bench-balanced");
    runtime
        .create_topic(
            topic.clone(),
            TopicBackend::InMemory,
            topic_policies(BALANCED_CHANNEL_CAPACITY, 1),
        )
        .await
        .expect("topic should be created");

    let publisher = runtime
        .publisher(
            &topic,
            TopicPublisherOptions {
                outcome_interest: if with_outcome {
                    TopicOutcomeInterest::AckOrNack
                } else {
                    TopicOutcomeInterest::None
                },
                ..TopicPublisherOptions::default()
            },
        )
        .await
        .expect("publisher should be created");

    let mut subscriber = runtime
        .subscribe(
            &topic,
            TopicSubscription::Balanced {
                group: subscription_group("workers"),
            },
        )
        .await
        .expect("subscriber should be created");

    let consumer = tokio::spawn(async move {
        let mut checksum = 0u64;
        for _ in 0..msg_count {
            let delivery = subscriber.recv().await.expect("receive should succeed");
            checksum ^= *delivery.payload();
            if with_outcome {
                delivery.ack().await.expect("ack should succeed");
            }
        }
        _ = black_box(checksum);
    });

    for i in 0..msg_count {
        let publish = publisher
            .publish(i as u64)
            .await
            .expect("publish should succeed");
        if with_outcome {
            let outcome = publish
                .outcome
                .expect("outcome future should exist when requested");
            let resolved = outcome.await.expect("outcome should resolve");
            assert!(
                matches!(resolved, TopicPublishOutcome::Ack),
                "balanced benchmark expects ack outcomes only"
            );
            _ = black_box(resolved);
        } else {
            _ = black_box(publish.report.delivered_subscribers);
        }
    }

    consumer.await.expect("consumer task should complete");
}

async fn run_tokio_broadcast(msg_count: usize, subscriber_count: usize, capacity: usize) {
    let (tx, _unused_rx) = broadcast::channel::<u64>(capacity);
    let mut consumers = Vec::with_capacity(subscriber_count);

    for idx in 0..subscriber_count {
        let mut rx = tx.subscribe();
        consumers.push(tokio::spawn(async move {
            let mut checksum = 0u64;
            for _ in 0..msg_count {
                let value = rx.recv().await.expect("broadcast receive should succeed");
                checksum ^= value;
            }
            _ = black_box((idx, checksum));
        }));
    }

    for i in 0..msg_count {
        let delivered = tx.send(i as u64).expect("broadcast send should succeed");
        _ = black_box(delivered);
    }
    drop(tx);

    for consumer in consumers {
        consumer.await.expect("consumer task should complete");
    }
}

async fn run_topic_broadcast(msg_count: usize, subscriber_count: usize, with_outcome: bool) {
    let runtime = InMemoryTopicRuntime::<u64>::new();
    let topic = topic_name("bench-broadcast");
    runtime
        .create_topic(
            topic.clone(),
            TopicBackend::InMemory,
            topic_policies(1, BROADCAST_CHANNEL_CAPACITY),
        )
        .await
        .expect("topic should be created");

    let publisher = runtime
        .publisher(
            &topic,
            TopicPublisherOptions {
                outcome_interest: if with_outcome {
                    TopicOutcomeInterest::AckOrNack
                } else {
                    TopicOutcomeInterest::None
                },
                ..TopicPublisherOptions::default()
            },
        )
        .await
        .expect("publisher should be created");

    let mut consumers = Vec::with_capacity(subscriber_count);
    for idx in 0..subscriber_count {
        let mut subscriber = runtime
            .subscribe(&topic, TopicSubscription::Broadcast)
            .await
            .expect("subscriber should be created");

        consumers.push(tokio::spawn(async move {
            let mut checksum = 0u64;
            for _ in 0..msg_count {
                let delivery = subscriber.recv().await.expect("receive should succeed");
                checksum ^= *delivery.payload();
                if with_outcome {
                    delivery.ack().await.expect("ack should succeed");
                }
            }
            _ = black_box((idx, checksum));
        }));
    }

    for i in 0..msg_count {
        let publish = publisher
            .publish(i as u64)
            .await
            .expect("publish should succeed");
        if with_outcome {
            let outcome = publish
                .outcome
                .expect("outcome future should exist when requested");
            let resolved = outcome.await.expect("outcome should resolve");
            assert!(
                matches!(resolved, TopicPublishOutcome::Ack),
                "broadcast benchmark expects ack outcomes only"
            );
            _ = black_box(resolved);
        } else {
            _ = black_box(publish.report.delivered_subscribers);
        }
    }

    for consumer in consumers {
        consumer.await.expect("consumer task should complete");
    }
}

fn bench_topic_balanced_ack_vs_flume_vs_no_ack(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build Tokio runtime");

    let mut group = c.benchmark_group("topic_compare/balanced");
    _ = group.throughput(Throughput::Elements(BALANCED_MSG_COUNT as u64));

    _ = group.bench_function("flume_mpmc_baseline", |b| {
        b.to_async(&rt)
            .iter(|| run_flume_mpmc_balanced(BALANCED_MSG_COUNT, BALANCED_CHANNEL_CAPACITY));
    });

    _ = group.bench_function("topic_no_outcome", |b| {
        b.to_async(&rt)
            .iter(|| run_topic_balanced(BALANCED_MSG_COUNT, false));
    });

    _ = group.bench_function("topic_with_outcome", |b| {
        b.to_async(&rt)
            .iter(|| run_topic_balanced(BALANCED_MSG_COUNT, true));
    });

    group.finish();
}

fn bench_topic_broadcast_ack_vs_tokio_vs_no_ack(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build Tokio runtime");

    let mut group = c.benchmark_group("topic_compare/broadcast");
    _ = group.throughput(Throughput::Elements(BROADCAST_MSG_COUNT as u64));

    _ = group.bench_function("tokio_broadcast_baseline", |b| {
        b.to_async(&rt).iter(|| {
            run_tokio_broadcast(
                BROADCAST_MSG_COUNT,
                BROADCAST_SUBSCRIBERS,
                BROADCAST_CHANNEL_CAPACITY,
            )
        });
    });

    _ = group.bench_function("topic_no_outcome", |b| {
        b.to_async(&rt)
            .iter(|| run_topic_broadcast(BROADCAST_MSG_COUNT, BROADCAST_SUBSCRIBERS, false));
    });

    _ = group.bench_function("topic_with_outcome", |b| {
        b.to_async(&rt)
            .iter(|| run_topic_broadcast(BROADCAST_MSG_COUNT, BROADCAST_SUBSCRIBERS, true));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_topic_balanced_ack_vs_flume_vs_no_ack,
    bench_topic_broadcast_ack_vs_tokio_vs_no_ack
);
criterion_main!(benches);
