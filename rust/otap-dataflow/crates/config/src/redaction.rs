// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Secret-bearing configuration value types.

use schemars::JsonSchema;
use secrecy::{ExposeSecret, SecretString};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Placeholder emitted for typed secrets in safe config snapshots.
pub const REDACTED_VALUE: &str = "[REDACTED]";

/// A string that remains cleartext in memory and always serializes redacted.
#[derive(Debug, Clone, JsonSchema)]
pub struct RedactedString(#[schemars(with = "String")] SecretString);

impl RedactedString {
    /// Returns the cleartext value for an explicit runtime use.
    #[must_use]
    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

impl<'de> Deserialize<'de> for RedactedString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value == REDACTED_VALUE {
            return Err(D::Error::custom(
                "the redaction placeholder cannot be used as a secret value; provide the secret again",
            ));
        }
        Ok(Self(SecretString::from(value)))
    }
}

impl Serialize for RedactedString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(REDACTED_VALUE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: cleartext is loaded into a typed secret and used by runtime code.
    /// Guarantees: explicit exposure returns the original secret while serialization redacts it.
    #[test]
    fn redacted_string_exposes_only_through_explicit_access() {
        let secret: RedactedString =
            serde_json::from_value(serde_json::json!("cleartext")).expect("secret should parse");
        assert_eq!(secret.expose(), "cleartext");
        assert_eq!(
            serde_json::to_value(&secret).expect("secret should serialize"),
            serde_json::json!(REDACTED_VALUE)
        );
        assert!(format!("{secret:?}").contains("REDACTED"));
    }

    /// Scenario: a safe snapshot marker is submitted as a live secret value.
    /// Guarantees: display-only snapshots cannot silently replace runtime credentials.
    #[test]
    fn redacted_string_rejects_snapshot_marker_on_input() {
        let error = serde_json::from_value::<RedactedString>(serde_json::json!(REDACTED_VALUE))
            .expect_err("snapshot marker must not become a runtime credential");
        assert!(error.to_string().contains("redaction placeholder"));
    }
}
