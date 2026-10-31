// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Process-wide limits for native engine observation.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Limits for the one shared observation hub and its independent subscribers.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct EngineObserverSettings {
    /// Maximum simultaneously configured observer extensions/subscriptions (1..=32).
    pub max_observers: usize,
    /// Power-of-two capacity of the shared overwrite ring (1..=1024), not a queue per observer.
    pub channel_capacity: usize,
    /// Logical retained payload limit per observation (256 bytes..=1 MiB).
    pub max_payload_bytes: usize,
}
impl Default for EngineObserverSettings {
    fn default() -> Self {
        Self {
            max_observers: 8,
            channel_capacity: 256,
            max_payload_bytes: 64 * 1024,
        }
    }
}
impl EngineObserverSettings {
    /// Whether serialization can omit these default settings.
    #[must_use]
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
    /// Validate hard ceilings before allocating observer resources.
    pub fn validate(&self) -> Result<(), crate::error::Error> {
        if !(1..=32).contains(&self.max_observers)
            || !(1..=1024).contains(&self.channel_capacity)
            || !self.channel_capacity.is_power_of_two()
            || !(256..=1024 * 1024).contains(&self.max_payload_bytes)
        {
            return Err(crate::error::Error::InvalidUserConfig {
                error: concat!(
                    "engine.controller.observers requires max_observers=1..32, ",
                    "power-of-two channel_capacity=1..1024 and max_payload_bytes=256..1048576"
                )
                .into(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Scenario: observer limits are defaulted or exceed a hard resource ceiling.
    /// Guarantees: valid defaults round-trip and invalid capacities fail before allocation.
    #[test]
    fn validates_bounded_observer_settings() {
        let settings: EngineObserverSettings = serde_json::from_str("{}").unwrap();
        assert!(settings.is_default());
        assert!(settings.validate().is_ok());
        for invalid in [
            EngineObserverSettings {
                max_observers: 0,
                ..settings.clone()
            },
            EngineObserverSettings {
                channel_capacity: 3,
                ..settings.clone()
            },
            EngineObserverSettings {
                channel_capacity: 1025,
                ..settings.clone()
            },
            EngineObserverSettings {
                max_payload_bytes: usize::MAX,
                ..settings
            },
        ] {
            assert!(invalid.validate().is_err());
        }
    }
}
