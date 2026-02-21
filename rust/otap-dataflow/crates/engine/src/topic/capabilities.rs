// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::TopicRuntimeError;
use otap_df_config::TopicName;
use otap_df_config::topic::{TopicBroadcastOnLagPolicy, TopicPolicies};

/// Backend capability contract for runtime topic semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicRuntimeCapabilities {
    /// Backend identifier.
    pub backend_name: &'static str,
    /// Support for `broadcast_on_lag: drop_oldest`.
    pub supports_broadcast_on_lag_drop_oldest: bool,
    /// Support for `broadcast_on_lag: disconnect`.
    pub supports_broadcast_on_lag_disconnect: bool,
}

impl TopicRuntimeCapabilities {
    /// Returns whether this backend supports the given broadcast lag policy.
    #[must_use]
    pub const fn supports_broadcast_on_lag(&self, policy: &TopicBroadcastOnLagPolicy) -> bool {
        match policy {
            TopicBroadcastOnLagPolicy::DropOldest => self.supports_broadcast_on_lag_drop_oldest,
            TopicBroadcastOnLagPolicy::Disconnect => self.supports_broadcast_on_lag_disconnect,
        }
    }
}

pub(crate) fn validate_topic_policy_support(
    topic_name: &TopicName,
    policies: &TopicPolicies,
    capabilities: TopicRuntimeCapabilities,
) -> Result<(), TopicRuntimeError> {
    if !capabilities.supports_broadcast_on_lag(&policies.broadcast_on_lag) {
        return Err(TopicRuntimeError::UnsupportedTopicPolicy {
            topic: topic_name.clone(),
            backend: capabilities.backend_name,
            policy: "broadcast_on_lag",
            value: broadcast_on_lag_policy_value(&policies.broadcast_on_lag).to_owned(),
        });
    }
    Ok(())
}

fn broadcast_on_lag_policy_value(policy: &TopicBroadcastOnLagPolicy) -> &'static str {
    match policy {
        TopicBroadcastOnLagPolicy::DropOldest => "drop_oldest",
        TopicBroadcastOnLagPolicy::Disconnect => "disconnect",
    }
}
