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
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionUserConfig {
    /// The extension type URN identifying the plugin (factory) to use.
    pub r#type: ExtensionUrn,

    /// An optional description of this extension.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Extension-specific configuration (interpreted by the extension itself).
    /// Omitting this field is equivalent to specifying an empty object.
    #[serde(default = "crate::default_component_config")]
    #[schemars(extend("x-kubernetes-preserve-unknown-fields" = true))]
    pub config: Value,
}

impl ExtensionUserConfig {
    /// Creates a new `ExtensionUserConfig` with the specified type URN and config.
    #[must_use]
    pub fn new(r#type: ExtensionUrn, config: Value) -> Self {
        Self {
            r#type,
            description: None,
            config,
        }
    }

    /// Creates a new `ExtensionUserConfig` with the specified type URN and
    /// default empty-object config.
    #[must_use]
    pub fn with_type<U: Into<ExtensionUrn>>(r#type: U) -> Self {
        Self {
            r#type: r#type.into(),
            description: None,
            config: crate::default_component_config(),
        }
    }

    /// Returns a clone of this extension config with credential header values
    /// redacted, for safe exposure through the admin/config snapshot APIs.
    ///
    /// Extension `config` is the same raw [`Value`] mechanism as
    /// [`NodeUserConfig::config`](crate::node::NodeUserConfig::config), so an
    /// extension that carries static `headers` (credentials) is redacted with
    /// the same policy as a node: every value under any `headers` object is
    /// replaced with
    /// [`REDACTED_HEADER_VALUE`](crate::node::REDACTED_HEADER_VALUE) while the
    /// keys are preserved. The stored config is left unchanged.
    #[must_use]
    pub fn redacted_for_snapshot(&self) -> ExtensionUserConfig {
        let mut redacted = self.clone();
        crate::node::redact_secret_headers(&mut redacted.config);
        redacted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: an extension omits the config field in YAML or JSON or uses the no-payload constructor.
    /// Guarantees: all paths match an explicit empty object, including serialization and the schema default.
    #[test]
    fn omitted_extension_config_is_empty_object() {
        let omitted = serde_json::json!({"type": "urn:test:extension:example"});
        let explicit = serde_json::json!({"type": "urn:test:extension:example", "config": {}});
        let expected: ExtensionUserConfig = serde_json::from_value(explicit).unwrap();
        let from_json: ExtensionUserConfig = serde_json::from_value(omitted.clone()).unwrap();
        let from_yaml: ExtensionUserConfig =
            serde_yaml::from_str(&serde_yaml::to_string(&omitted).unwrap()).unwrap();
        assert_eq!(from_json, expected);
        assert_eq!(from_yaml, expected);
        assert_eq!(
            ExtensionUserConfig::with_type(expected.r#type.clone()),
            expected
        );
        assert_eq!(
            serde_json::to_value(from_json).unwrap()["config"],
            serde_json::json!({})
        );
        let schema = serde_json::to_value(schemars::schema_for!(ExtensionUserConfig)).unwrap();
        assert_eq!(
            schema["properties"]["config"]["default"],
            serde_json::json!({})
        );
    }

    /// Scenario: an extension explicitly supplies null, an object, or a non-object component payload.
    /// Guarantees: defaulting only applies to absent fields and preserves existing supplied values.
    #[test]
    fn explicit_extension_config_is_preserved() {
        for payload in [
            Value::Null,
            serde_json::json!({}),
            serde_json::json!({"nested": {"enabled": true}}),
            serde_json::json!([1, 2]),
            serde_json::json!("text"),
            serde_json::json!(42),
            serde_json::json!(false),
        ] {
            let input =
                serde_json::json!({"type": "urn:test:extension:example", "config": payload});
            let from_json: ExtensionUserConfig = serde_json::from_value(input.clone()).unwrap();
            let from_yaml: ExtensionUserConfig =
                serde_yaml::from_str(&serde_yaml::to_string(&input).unwrap()).unwrap();
            assert_eq!(from_json.config, payload);
            assert_eq!(from_yaml.config, payload);
            assert_eq!(
                ExtensionUserConfig::new(from_json.r#type, payload.clone()).config,
                payload
            );
        }
        let blank: ExtensionUserConfig =
            serde_yaml::from_str("type: urn:test:extension:example\nconfig:").unwrap();
        assert_eq!(blank.config, Value::Null);
    }

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

    #[test]
    fn redacted_for_snapshot_masks_extension_headers() {
        let yaml = r#"
type: "urn:otap:extension:headers_setter"
config:
  headers:
    authorization: "Bearer ext-super-secret"
"#;
        let cfg: ExtensionUserConfig = serde_yaml::from_str(yaml).unwrap();
        let redacted = cfg.redacted_for_snapshot();
        assert_eq!(
            redacted.config["headers"]["authorization"],
            crate::node::REDACTED_HEADER_VALUE
        );
        // The original extension config is left untouched (redaction is a copy).
        assert_eq!(
            cfg.config["headers"]["authorization"],
            "Bearer ext-super-secret"
        );
    }
}
