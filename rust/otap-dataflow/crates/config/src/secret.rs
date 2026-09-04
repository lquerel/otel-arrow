// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Secret-bearing configuration values with safe snapshot serialization.

use schemars::JsonSchema;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Marker emitted for a secret value in an effective configuration snapshot.
pub const REDACTED_VALUE: &str = "[REDACTED]";

/// Marker emitted when a component's snapshot policy omits its whole config.
pub const OMITTED_VALUE: &str = "[OMITTED]";

/// A secret string that is protected in memory and redacted when serialized.
///
/// Deserialization accepts cleartext submitted by the user, except for the
/// display-only redaction marker. Serialization never exposes cleartext.
#[derive(Clone, JsonSchema)]
#[schemars(transparent)]
pub struct RedactedString(#[schemars(with = "String")] SecretString);

impl RedactedString {
    /// Wraps a cleartext secret.
    #[must_use]
    pub fn new(value: impl Into<Box<str>>) -> Self {
        Self(SecretString::from(value.into()))
    }

    /// Explicitly exposes the cleartext value for its intended runtime use.
    #[must_use]
    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for RedactedString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RedactedString")
            .field(&REDACTED_VALUE)
            .finish()
    }
}

impl PartialEq for RedactedString {
    fn eq(&self, other: &Self) -> bool {
        self.expose() == other.expose()
    }
}

impl Eq for RedactedString {}

impl Serialize for RedactedString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(REDACTED_VALUE)
    }
}

impl<'de> Deserialize<'de> for RedactedString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value == REDACTED_VALUE || value == OMITTED_VALUE {
            return Err(serde::de::Error::custom(
                "snapshot display markers are not valid submitted secret values",
            ));
        }
        Ok(Self::new(value))
    }
}

impl From<String> for RedactedString {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for RedactedString {
    fn from(value: &str) -> Self {
        Self::new(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: A submitted cleartext secret is parsed and later serialized for a snapshot.
    /// Guarantees: Runtime access returns cleartext while serialization and Debug stay redacted.
    #[test]
    fn cleartext_is_available_only_through_explicit_exposure() {
        let value: RedactedString = serde_json::from_str("\"top-secret\"").unwrap();
        assert_eq!(value.expose(), "top-secret");
        assert_eq!(serde_json::to_string(&value).unwrap(), "\"[REDACTED]\"");
        assert!(!format!("{value:?}").contains("top-secret"));
    }

    /// Scenario: A redacted or omitted effective snapshot is submitted as source config.
    /// Guarantees: Display-only markers cannot silently replace an operational credential.
    #[test]
    fn snapshot_markers_are_rejected_on_input() {
        for marker in [REDACTED_VALUE, OMITTED_VALUE] {
            let encoded = serde_json::to_string(marker).unwrap();
            assert!(serde_json::from_str::<RedactedString>(&encoded).is_err());
        }
    }

    /// Scenario: Two resolved configurations contain equal or different secret strings.
    /// Guarantees: Runtime equivalence observes the real secret rather than its display marker.
    #[test]
    fn equality_uses_cleartext_value() {
        assert_eq!(RedactedString::from("same"), RedactedString::from("same"));
        assert_ne!(
            RedactedString::from("first"),
            RedactedString::from("second")
        );
    }
}
