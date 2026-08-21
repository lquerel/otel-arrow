// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Factory-owned component configuration resolution.

use crate::error::Error;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Snapshot policy established when a component factory resolves its config.
#[derive(Debug, Clone, PartialEq)]
pub enum ComponentSnapshot {
    /// Export this already-safe typed representation.
    Export(Value),
    /// Omit the component-specific config from generally exposed snapshots.
    Omit,
}

/// Type-erased component configuration resolved by the owning factory.
///
/// The immutable `Arc` is created during control-plane resolution and cloned
/// only while pipeline instances are built. Runtime hot paths do not touch it.
#[derive(Clone)]
pub struct ResolvedComponentConfig {
    typed: Option<Arc<dyn Any + Send + Sync>>,
    type_name: Option<&'static str>,
    snapshot: Option<Arc<ComponentSnapshot>>,
}

impl ResolvedComponentConfig {
    /// Creates an unresolved placeholder used only by source config envelopes.
    #[must_use]
    pub const fn unresolved() -> Self {
        Self {
            typed: None,
            type_name: None,
            snapshot: None,
        }
    }

    /// Stores a typed configuration whose `Serialize` contract is safe for
    /// admin and OpAMP snapshots.
    pub fn export_typed<T>(typed: T) -> Result<Self, Error>
    where
        T: Serialize + Send + Sync + 'static,
    {
        let snapshot = serde_json::to_value(&typed).map_err(|error| Error::InvalidUserConfig {
            error: format!("could not serialize resolved component config: {error}"),
        })?;
        Ok(Self::typed_with_snapshot(
            typed,
            ComponentSnapshot::Export(snapshot),
        ))
    }

    /// Stores a typed configuration with an explicit component-owned snapshot.
    #[must_use]
    pub fn typed_with_snapshot<T>(typed: T, snapshot: ComponentSnapshot) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            typed: Some(Arc::new(typed)),
            type_name: Some(std::any::type_name::<T>()),
            snapshot: Some(Arc::new(snapshot)),
        }
    }

    /// Stores a typed runtime configuration while omitting it from snapshots.
    #[must_use]
    pub fn omit_typed<T>(typed: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self::typed_with_snapshot(typed, ComponentSnapshot::Omit)
    }

    /// Creates a resolved config for a component with no runtime config value.
    #[must_use]
    pub fn export_value(snapshot: Value) -> Self {
        Self {
            typed: None,
            type_name: None,
            snapshot: Some(Arc::new(ComponentSnapshot::Export(snapshot))),
        }
    }

    /// Creates a resolved config that is intentionally omitted from snapshots.
    #[must_use]
    pub fn omit() -> Self {
        Self {
            typed: None,
            type_name: None,
            snapshot: Some(Arc::new(ComponentSnapshot::Omit)),
        }
    }

    /// Returns whether a factory has resolved this configuration.
    #[must_use]
    pub const fn is_resolved(&self) -> bool {
        self.snapshot.is_some()
    }

    /// Returns the factory-owned typed configuration.
    pub fn typed<T>(&self) -> Result<&T, Error>
    where
        T: Send + Sync + 'static,
    {
        self.typed
            .as_deref()
            .and_then(|value| value.downcast_ref::<T>())
            .ok_or_else(|| Error::InvalidUserConfig {
                error: format!(
                    "resolved component config type mismatch: expected `{}`",
                    std::any::type_name::<T>()
                ),
            })
    }

    /// Returns the snapshot policy established by the factory.
    pub fn snapshot(&self) -> Result<&ComponentSnapshot, SnapshotError> {
        self.snapshot.as_deref().ok_or(SnapshotError::Unresolved)
    }
}

impl Default for ResolvedComponentConfig {
    fn default() -> Self {
        Self::unresolved()
    }
}

impl fmt::Debug for ResolvedComponentConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedComponentConfig")
            .field("type_name", &self.type_name)
            .field("snapshot", &self.snapshot)
            .finish_non_exhaustive()
    }
}

/// A factory-owned config resolver function.
pub type ResolveComponentConfigFn = fn(&Value) -> Result<ResolvedComponentConfig, Error>;

/// A legacy validation function used by an explicit omit policy.
pub type ValidateComponentConfigFn = fn(&Value) -> Result<(), Error>;

/// Mandatory factory policy for resolving and safely snapshotting config.
#[derive(Clone, Copy)]
pub enum ComponentConfigResolver {
    /// Resolve a typed value and establish its component-owned snapshot.
    Typed(ResolveComponentConfigFn),
    /// Validate the source value but deliberately omit it from snapshots.
    Omit(ValidateComponentConfigFn),
}

impl ComponentConfigResolver {
    /// Creates a typed resolver policy.
    #[must_use]
    pub const fn typed(resolve: ResolveComponentConfigFn) -> Self {
        Self::Typed(resolve)
    }

    /// Creates an explicit omit policy for a component awaiting typed migration.
    #[must_use]
    pub const fn omit(validate: ValidateComponentConfigFn) -> Self {
        Self::Omit(validate)
    }

    /// Resolves and validates a source config exactly once.
    pub fn resolve(self, config: &Value) -> Result<ResolvedComponentConfig, Error> {
        match self {
            Self::Typed(resolve) => resolve(config),
            Self::Omit(validate) => {
                validate(config)?;
                Ok(ResolvedComponentConfig::omit())
            }
        }
    }

    /// Validates a source config through the same resolution contract.
    pub fn validate(self, config: &Value) -> Result<(), Error> {
        self.resolve(config).map(|_| ())
    }
}

/// Declares a factory config policy that validates but omits snapshots.
#[macro_export]
macro_rules! omit_component_config {
    ($validator:expr) => {
        $crate::resolved_config::ComponentConfigResolver::omit($validator)
    };
}

/// Declares a factory-owned typed resolver and safe snapshot policy.
#[macro_export]
macro_rules! resolve_component_config {
    ($resolver:expr) => {
        $crate::resolved_config::ComponentConfigResolver::typed($resolver)
    };
}

impl fmt::Debug for ComponentConfigResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Typed(_) => formatter.write_str("ComponentConfigResolver::Typed"),
            Self::Omit(_) => formatter.write_str("ComponentConfigResolver::Omit"),
        }
    }
}

/// Resolves a deserializable type but explicitly omits it from snapshots.
pub fn resolve_omitted_typed_config<T>(config: &Value) -> Result<ResolvedComponentConfig, Error>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    let typed: T =
        serde_json::from_value(config.clone()).map_err(|error| Error::InvalidUserConfig {
            error: error.to_string(),
        })?;
    Ok(ResolvedComponentConfig::omit_typed(typed))
}

/// Resolves a typed config whose serialization contract is snapshot-safe.
pub fn resolve_typed_config<T>(config: &Value) -> Result<ResolvedComponentConfig, Error>
where
    T: DeserializeOwned + Serialize + Send + Sync + 'static,
{
    let typed: T =
        serde_json::from_value(config.clone()).map_err(|error| Error::InvalidUserConfig {
            error: error.to_string(),
        })?;
    ResolvedComponentConfig::export_typed(typed)
}

/// Resolves a component that accepts no user config and exports an empty map.
pub fn resolve_no_config(config: &Value) -> Result<ResolvedComponentConfig, Error> {
    crate::validation::no_config(config)?;
    Ok(ResolvedComponentConfig::export_value(Value::Object(
        serde_json::Map::new(),
    )))
}

/// A sanitized failure while building a safe config snapshot.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotError {
    /// A raw config reached the snapshot boundary without factory resolution.
    #[error("component configuration has not been resolved by its factory")]
    Unresolved,
    /// Structural location added while a snapshot tree propagates a failure.
    #[error("{context}: {source}")]
    Context {
        /// Group, pipeline, node, or extension location.
        context: String,
        /// Sanitized underlying snapshot failure.
        #[source]
        source: Box<SnapshotError>,
    },
}

impl SnapshotError {
    /// Adds a non-secret structural location to this failure.
    #[must_use]
    pub fn at(self, context: impl Into<String>) -> Self {
        Self::Context {
            context: context.into(),
            source: Box::new(self),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redaction::{REDACTED_VALUE, RedactedString};
    use serde::{Deserialize, Serialize};

    #[derive(Deserialize, Serialize)]
    struct TestConfig {
        label: String,
        password: RedactedString,
    }

    /// Scenario: a factory resolves a typed config containing a secret wrapper.
    /// Guarantees: the retained runtime value is cleartext while its snapshot is redacted.
    #[test]
    fn typed_resolution_separates_runtime_secret_from_snapshot() {
        let resolved = resolve_typed_config::<TestConfig>(&serde_json::json!({
            "label": "production",
            "password": "cleartext",
        }))
        .expect("typed config should resolve");

        let typed = resolved
            .typed::<TestConfig>()
            .expect("resolved type should match");
        assert_eq!(typed.password.expose(), "cleartext");
        assert_eq!(
            resolved.snapshot().expect("snapshot policy should exist"),
            &ComponentSnapshot::Export(serde_json::json!({
                "label": "production",
                "password": REDACTED_VALUE,
            }))
        );
    }

    /// Scenario: a component has an explicit omit policy during migration.
    /// Guarantees: validation still runs and no raw config is made snapshot-exportable.
    #[test]
    fn omit_policy_validates_without_exporting_source_config() {
        let resolver = ComponentConfigResolver::omit(crate::validation::no_config);
        let resolved = resolver
            .resolve(&Value::Null)
            .expect("null no-config input should validate");
        assert_eq!(
            resolved.snapshot().expect("snapshot policy should exist"),
            &ComponentSnapshot::Omit
        );
        assert!(
            resolver
                .resolve(&serde_json::json!({ "secret": "value" }))
                .is_err()
        );
    }

    /// Scenario: a raw config reaches snapshot traversal before factory resolution.
    /// Guarantees: snapshot production fails closed instead of returning the raw value.
    #[test]
    fn unresolved_config_has_no_snapshot_policy() {
        let unresolved = ResolvedComponentConfig::unresolved();
        assert_eq!(unresolved.snapshot(), Err(SnapshotError::Unresolved));
    }
}
