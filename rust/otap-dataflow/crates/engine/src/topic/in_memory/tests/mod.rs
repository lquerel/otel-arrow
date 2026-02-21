// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::InMemoryTopicRuntime;
use crate::topic::{
    TopicOutcomeInterest, TopicOutcomeNack, TopicPublishOutcome, TopicPublisherOptions,
    TopicRuntime, TopicRuntimeError, TopicSubscription,
};
use otap_df_config::TopicName;
use otap_df_config::topic::{
    SubscriptionGroupName, TopicBackend, TopicBalancedOnFullPolicy, TopicBroadcastOnLagPolicy,
    TopicPolicies,
};

pub(super) fn topic_name(raw: &str) -> TopicName {
    TopicName::parse(raw).expect("topic name should be valid")
}

pub(super) fn group_name(raw: &str) -> SubscriptionGroupName {
    SubscriptionGroupName::parse(raw).expect("group name should be valid")
}

pub(super) fn policies(
    balanced_group_queue_capacity: usize,
    balanced_on_full: TopicBalancedOnFullPolicy,
    broadcast_subscriber_queue_capacity: usize,
    broadcast_on_lag: TopicBroadcastOnLagPolicy,
) -> TopicPolicies {
    TopicPolicies {
        balanced_group_queue_capacity,
        balanced_on_full,
        broadcast_subscriber_queue_capacity,
        broadcast_on_lag,
    }
}

mod balanced;
mod broadcast;
mod lifecycle;
mod outcomes;
mod policy_support;
