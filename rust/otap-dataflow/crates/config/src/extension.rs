// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension configuration types.
//!
//! Extensions have a simpler configuration model than data-path nodes -- they
//! have no output ports, no wiring contracts, and no header policies.

pub use crate::extension_urn::ExtensionUrn;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// User configuration for an extension instance.
///
/// Unlike [`NodeUserConfig`](crate::node::NodeUserConfig), extensions have no
/// output ports, wiring contracts, or transport header policies -- they only
/// need a type URN and extension-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExtensionUserConfig {
    /// The extension type URN identifying the plugin (factory) to use.
    pub r#type: ExtensionUrn,

    /// An optional description of this extension.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Extension-specific configuration (interpreted by the extension itself).
    #[serde(default)]
    #[schemars(extend("x-kubernetes-preserve-unknown-fields" = true))]
    pub config: Value,

    /// Factory-owned typed configuration derived from `config`.
    #[serde(skip)]
    #[schemars(skip)]
    resolved_config: crate::resolved_config::ResolvedComponentConfig,
}

impl PartialEq for ExtensionUserConfig {
    fn eq(&self, other: &Self) -> bool {
        self.r#type == other.r#type
            && self.description == other.description
            && self.config == other.config
    }
}

impl ExtensionUserConfig {
    /// Creates a new `ExtensionUserConfig` with the specified type URN and config.
    #[must_use]
    pub fn new(r#type: ExtensionUrn, config: Value) -> Self {
        Self {
            r#type,
            description: None,
            config,
            resolved_config: crate::resolved_config::ResolvedComponentConfig::unresolved(),
        }
    }

    /// Creates a new `ExtensionUserConfig` with the specified type URN and
    /// default (null) config.
    #[must_use]
    pub fn with_type<U: Into<ExtensionUrn>>(r#type: U) -> Self {
        Self {
            r#type: r#type.into(),
            description: None,
            config: Value::Null,
            resolved_config: crate::resolved_config::ResolvedComponentConfig::unresolved(),
        }
    }

    /// Installs the typed configuration produced by this extension's factory.
    pub fn set_resolved_config(
        &mut self,
        resolved: crate::resolved_config::ResolvedComponentConfig,
    ) {
        self.resolved_config = resolved;
    }

    /// Returns whether this extension's factory has resolved its config.
    #[must_use]
    pub fn is_config_resolved(&self) -> bool {
        self.resolved_config.is_resolved()
    }

    /// Returns the factory-owned typed configuration.
    pub fn resolved_config<T>(&self) -> Result<&T, crate::error::Error>
    where
        T: Send + Sync + 'static,
    {
        self.resolved_config.typed::<T>()
    }

    /// Returns a snapshot using only the policy established by the factory.
    pub fn try_safe_snapshot(
        &self,
    ) -> Result<ExtensionUserConfig, crate::resolved_config::SnapshotError> {
        let mut snapshot = self.clone();
        snapshot.config = match self.resolved_config.snapshot()? {
            crate::resolved_config::ComponentSnapshot::Export(value) => value.clone(),
            crate::resolved_config::ComponentSnapshot::Omit => Value::Null,
        };
        snapshot.resolved_config = crate::resolved_config::ResolvedComponentConfig::unresolved();
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extension_user_config_deserialize() {
        let yaml = r#"
type: "urn:otap:extension:sample_kv_store"
config:
  capacity: 100
"#;
        let config: ExtensionUserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.r#type.id(), "sample_kv_store");
        assert_eq!(config.config["capacity"], 100);
    }

    #[test]
    fn test_extension_user_config_rejects_capabilities() {
        let yaml = r#"
type: "urn:otap:extension:auth"
capabilities:
  some_cap: "ext"
"#;
        let result: Result<ExtensionUserConfig, _> = serde_yaml::from_str(yaml);
        assert!(result.is_err());
    }

    /// Scenario: an extension config is intentionally omitted by its factory.
    /// Guarantees: snapshot traversal exports no extension-specific source JSON.
    #[test]
    fn explicit_omit_policy_removes_extension_config() {
        let mut config = ExtensionUserConfig::new(
            "urn:otap:extension:auth".into(),
            serde_json::json!({ "token": "cleartext" }),
        );
        config.set_resolved_config(crate::resolved_config::ResolvedComponentConfig::omit());

        let snapshot = config
            .try_safe_snapshot()
            .expect("explicit omit policy should produce a snapshot");
        assert_eq!(snapshot.config, Value::Null);
    }
}
