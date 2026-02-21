// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Topic declarations for inter-pipeline communication.

use crate::Description;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Name of a topic declaration/reference.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(try_from = "String", into = "String")]
#[schemars(with = "String")]
pub struct TopicName(String);

impl TopicName {
    /// Parses and validates a topic name.
    pub fn parse(raw: &str) -> Result<Self, String> {
        if raw.trim().is_empty() {
            return Err("topic name must be non-empty".to_owned());
        }
        Ok(Self(raw.to_owned()))
    }

    /// Returns the topic name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the owned topic name.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for TopicName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::borrow::Borrow<str> for TopicName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for TopicName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<String> for TopicName {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value.as_str())
    }
}

impl From<TopicName> for String {
    fn from(value: TopicName) -> Self {
        value.0
    }
}

impl From<TopicName> for Cow<'static, str> {
    fn from(value: TopicName) -> Self {
        Cow::Owned(value.0)
    }
}

impl From<&TopicName> for Cow<'static, str> {
    fn from(value: &TopicName) -> Self {
        Cow::Owned(value.0.clone())
    }
}

impl From<&'static str> for TopicName {
    fn from(value: &'static str) -> Self {
        Self::parse(value).expect("invalid static topic name literal")
    }
}

/// Name of a balanced-subscription consumer group.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(try_from = "String", into = "String")]
#[schemars(with = "String")]
pub struct SubscriptionGroupName(String);

impl SubscriptionGroupName {
    /// Parses and validates a subscription group name.
    pub fn parse(raw: &str) -> Result<Self, String> {
        if raw.trim().is_empty() {
            return Err("subscription group name must be non-empty".to_owned());
        }
        Ok(Self(raw.to_owned()))
    }

    /// Returns the group name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the owned group name.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for SubscriptionGroupName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for SubscriptionGroupName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<String> for SubscriptionGroupName {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value.as_str())
    }
}

impl From<SubscriptionGroupName> for String {
    fn from(value: SubscriptionGroupName) -> Self {
        value.0
    }
}

/// Backend selector used by a topic declaration.
///
/// Only `in_memory` is implemented by the runtime at this stage.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TopicBackend {
    /// Process-local in-memory runtime backend.
    #[default]
    InMemory,
    /// Quiver backend selector (reserved for future implementation).
    Quiver,
}

impl std::fmt::Display for TopicBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::InMemory => "in_memory",
            Self::Quiver => "quiver",
        };
        f.write_str(value)
    }
}

/// A named topic specification.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct TopicSpec {
    /// Optional human-readable description of the topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<Description>,
    /// Runtime backend selector for this topic.
    #[serde(default)]
    pub backend: TopicBackend,
    /// Topic behavior policies.
    #[serde(default)]
    pub policies: TopicPolicies,
}

impl TopicSpec {
    /// Returns validation errors for this topic specification.
    #[must_use]
    pub fn validation_errors(&self, path_prefix: &str) -> Vec<String> {
        self.policies
            .validation_errors(&format!("{path_prefix}.policies"))
    }
}

/// Policies supported for topics.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TopicPolicies {
    /// Maximum number of queued messages for balanced subscriptions (per group).
    #[serde(default = "default_topic_balanced_group_queue_capacity")]
    pub balanced_group_queue_capacity: usize,
    /// Behavior when a balanced group queue reaches `balanced_group_queue_capacity`.
    #[serde(default)]
    pub balanced_on_full: TopicBalancedOnFullPolicy,
    /// Maximum number of queued messages for each broadcast subscriber.
    #[serde(default = "default_topic_broadcast_subscriber_queue_capacity")]
    pub broadcast_subscriber_queue_capacity: usize,
    /// Behavior for lagging broadcast subscribers when `broadcast_subscriber_queue_capacity` is reached.
    #[serde(default)]
    pub broadcast_on_lag: TopicBroadcastOnLagPolicy,
}

impl Default for TopicPolicies {
    fn default() -> Self {
        Self {
            balanced_group_queue_capacity: default_topic_balanced_group_queue_capacity(),
            balanced_on_full: TopicBalancedOnFullPolicy::default(),
            broadcast_subscriber_queue_capacity: default_topic_broadcast_subscriber_queue_capacity(
            ),
            broadcast_on_lag: TopicBroadcastOnLagPolicy::default(),
        }
    }
}

impl TopicPolicies {
    /// Returns validation errors for this policy set.
    #[must_use]
    pub fn validation_errors(&self, path_prefix: &str) -> Vec<String> {
        let mut errors = Vec::new();
        if self.balanced_group_queue_capacity == 0 {
            errors.push(format!(
                "{path_prefix}.balanced_group_queue_capacity must be greater than 0"
            ));
        }
        if self.broadcast_subscriber_queue_capacity == 0 {
            errors.push(format!(
                "{path_prefix}.broadcast_subscriber_queue_capacity must be greater than 0"
            ));
        }
        errors
    }
}

/// Behavior when a balanced queue reaches `balanced_group_queue_capacity`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TopicBalancedOnFullPolicy {
    /// Drop the incoming item and keep queued items untouched.
    DropNewest,
    /// Block the publisher until queue space is available.
    #[default]
    Block,
}

/// Behavior for lagging broadcast subscribers.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TopicBroadcastOnLagPolicy {
    /// Drop oldest queued items for this subscriber to keep newer traffic.
    #[default]
    DropOldest,
    /// Disconnect the lagging subscriber.
    Disconnect,
}

const fn default_topic_balanced_group_queue_capacity() -> usize {
    128
}

const fn default_topic_broadcast_subscriber_queue_capacity() -> usize {
    128
}

#[cfg(test)]
mod tests {
    use super::{
        SubscriptionGroupName, TopicBackend, TopicBalancedOnFullPolicy, TopicBroadcastOnLagPolicy,
        TopicName, TopicSpec,
    };
    use serde::Deserialize;
    use std::collections::HashMap;

    #[test]
    fn defaults_match_expected_values() {
        let topic = TopicSpec::default();
        assert_eq!(topic.backend, TopicBackend::InMemory);
        assert_eq!(topic.policies.balanced_group_queue_capacity, 128);
        assert_eq!(
            topic.policies.balanced_on_full,
            TopicBalancedOnFullPolicy::Block
        );
        assert_eq!(topic.policies.broadcast_subscriber_queue_capacity, 128);
        assert_eq!(
            topic.policies.broadcast_on_lag,
            TopicBroadcastOnLagPolicy::DropOldest
        );
    }

    #[test]
    fn validates_non_zero_queue_capacities() {
        let mut topic = TopicSpec::default();
        topic.policies.balanced_group_queue_capacity = 0;
        topic.policies.broadcast_subscriber_queue_capacity = 0;

        let errors = topic.validation_errors("topics.raw");
        assert_eq!(errors.len(), 2);
        assert!(
            errors
                .iter()
                .any(|error| error.contains(".balanced_group_queue_capacity"))
        );
        assert!(
            errors
                .iter()
                .any(|error| error.contains(".broadcast_subscriber_queue_capacity"))
        );
    }

    #[test]
    fn deserializes_mode_specific_policy_values() {
        let yaml = r#"
backend: quiver
policies:
  balanced_group_queue_capacity: 1
  balanced_on_full: drop_newest
  broadcast_subscriber_queue_capacity: 2
  broadcast_on_lag: disconnect
"#;

        let topic: TopicSpec = serde_yaml::from_str(yaml).expect("topic should parse");
        assert_eq!(topic.backend, TopicBackend::Quiver);
        assert_eq!(
            topic.policies.balanced_on_full,
            TopicBalancedOnFullPolicy::DropNewest
        );
        assert_eq!(
            topic.policies.broadcast_on_lag,
            TopicBroadcastOnLagPolicy::Disconnect
        );
    }

    #[test]
    fn topic_name_rejects_empty_values() {
        let err = TopicName::parse("   ").expect_err("empty topic names should fail");
        assert!(err.contains("non-empty"));
    }

    #[test]
    fn subscription_group_name_rejects_empty_values() {
        let err = SubscriptionGroupName::parse("   ").expect_err("empty group names should fail");
        assert!(err.contains("non-empty"));
    }

    #[test]
    fn topic_name_supports_hash_map_lookup_by_str() {
        #[derive(Debug, Deserialize)]
        struct TopicsDoc {
            topics: HashMap<TopicName, TopicSpec>,
        }

        let yaml = r#"
topics:
  raw:
    policies:
      balanced_group_queue_capacity: 1
"#;

        let doc: TopicsDoc = serde_yaml::from_str(yaml).expect("topics should parse");
        assert!(doc.topics.contains_key("raw"));
    }

    #[test]
    fn backend_selector_accepts_quiver() {
        let topic: TopicSpec = serde_yaml::from_str("backend: quiver").expect("topic should parse");
        assert_eq!(topic.backend, TopicBackend::Quiver);
    }
}
