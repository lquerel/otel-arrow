// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Factory-owned component configuration resolution.

use otel_arrow_dfe_config::error::Error as ConfigError;
use otel_arrow_dfe_config::extension::ExtensionUserConfig;
use otel_arrow_dfe_config::node::{NodeKind, NodeUserConfig};
use otel_arrow_dfe_config::pipeline::PipelineConfig;
use otel_arrow_dfe_config::secret::OMITTED_VALUE;
use otel_arrow_dfe_config::{ExtensionId, NodeId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::any::{Any, TypeId, type_name};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

type ErasedConfig = dyn Any + Send + Sync;
type EquivalentFn = dyn Fn(&ErasedConfig, &ErasedConfig) -> bool + Send + Sync;

/// The snapshot behavior explicitly declared by a component factory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigSnapshotPolicy {
    /// Serialize the resolved typed configuration using its safe `Serialize` implementation.
    TypedSafe,
    /// Use a component-owned safe serializer during resolution.
    CustomSafe,
    /// Replace the complete component config with [`OMITTED_VALUE`].
    Omit,
}

/// Factory callback that parses, validates, defaults, and snapshots a component config.
pub type ResolveConfigFn = fn(&Value) -> Result<ResolvedComponentConfig, ConfigError>;

/// A resolved component-specific configuration hidden behind a type-erased boundary.
///
/// The typed value is immutable and shared by all runtime instances. The safe
/// snapshot is materialized before admission, so later snapshot paths never
/// deserialize or inspect the submitted JSON again.
#[derive(Clone)]
pub struct ResolvedComponentConfig {
    value: Arc<ErasedConfig>,
    value_type: TypeId,
    value_type_name: &'static str,
    equivalent: Arc<EquivalentFn>,
    snapshot: Value,
    snapshot_policy: ConfigSnapshotPolicy,
}

impl fmt::Debug for ResolvedComponentConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedComponentConfig")
            .field("value_type", &self.value_type_name)
            .field("snapshot_policy", &self.snapshot_policy)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ResolvedComponentConfig {
    fn eq(&self, other: &Self) -> bool {
        self.value_type == other.value_type
            && (self.equivalent)(self.value.as_ref(), other.value.as_ref())
    }
}

impl ResolvedComponentConfig {
    /// Creates a resolved config whose typed serialization is safe for snapshots.
    pub fn typed_safe<T>(value: T) -> Result<Self, ConfigError>
    where
        T: Any + Send + Sync + PartialEq + Serialize,
    {
        let snapshot =
            serde_json::to_value(&value).map_err(|error| ConfigError::InvalidUserConfig {
                error: format!("failed to serialize resolved component config: {error}"),
            })?;
        Ok(Self::new(value, snapshot, ConfigSnapshotPolicy::TypedSafe))
    }

    /// Creates a safely serialized resolved config with component-owned equivalence.
    pub fn typed_safe_by<T>(value: T, equivalent: fn(&T, &T) -> bool) -> Result<Self, ConfigError>
    where
        T: Any + Send + Sync + Serialize,
    {
        let snapshot =
            serde_json::to_value(&value).map_err(|error| ConfigError::InvalidUserConfig {
                error: format!("failed to serialize resolved component config: {error}"),
            })?;
        Ok(Self::new_by(
            value,
            snapshot,
            ConfigSnapshotPolicy::TypedSafe,
            equivalent,
        ))
    }

    /// Creates a resolved config using a component-owned safe snapshot value.
    #[must_use]
    pub fn custom_safe<T>(value: T, snapshot: Value) -> Self
    where
        T: Any + Send + Sync + PartialEq,
    {
        Self::new(value, snapshot, ConfigSnapshotPolicy::CustomSafe)
    }

    /// Creates a component-snapshotted resolved config with component-owned equivalence.
    #[must_use]
    pub fn custom_safe_by<T>(value: T, snapshot: Value, equivalent: fn(&T, &T) -> bool) -> Self
    where
        T: Any + Send + Sync,
    {
        Self::new_by(
            value,
            snapshot,
            ConfigSnapshotPolicy::CustomSafe,
            equivalent,
        )
    }

    /// Creates a resolved config whose component-specific subtree is omitted.
    #[must_use]
    pub fn omitted<T>(value: T) -> Self
    where
        T: Any + Send + Sync + PartialEq,
    {
        Self::new(
            value,
            Value::String(OMITTED_VALUE.to_owned()),
            ConfigSnapshotPolicy::Omit,
        )
    }

    /// Creates an omitted resolved config with component-owned equivalence.
    #[must_use]
    pub fn omitted_by<T>(value: T, equivalent: fn(&T, &T) -> bool) -> Self
    where
        T: Any + Send + Sync,
    {
        Self::new_by(
            value,
            Value::String(OMITTED_VALUE.to_owned()),
            ConfigSnapshotPolicy::Omit,
            equivalent,
        )
    }

    fn new<T>(value: T, snapshot: Value, snapshot_policy: ConfigSnapshotPolicy) -> Self
    where
        T: Any + Send + Sync + PartialEq,
    {
        Self::new_by(value, snapshot, snapshot_policy, T::eq)
    }

    fn new_by<T>(
        value: T,
        snapshot: Value,
        snapshot_policy: ConfigSnapshotPolicy,
        equivalent: fn(&T, &T) -> bool,
    ) -> Self
    where
        T: Any + Send + Sync,
    {
        let erased_equivalent = move |left: &ErasedConfig, right: &ErasedConfig| match (
            left.downcast_ref::<T>(),
            right.downcast_ref::<T>(),
        ) {
            (Some(left), Some(right)) => equivalent(left, right),
            _ => false,
        };

        Self {
            value: Arc::new(value),
            value_type: TypeId::of::<T>(),
            value_type_name: type_name::<T>(),
            equivalent: Arc::new(erased_equivalent),
            snapshot,
            snapshot_policy,
        }
    }

    /// Returns the immutable resolved value as its concrete component-owned type.
    pub fn get<T>(&self) -> Result<Arc<T>, ConfigError>
    where
        T: Any + Send + Sync,
    {
        Arc::downcast::<T>(Arc::clone(&self.value)).map_err(|_| ConfigError::InvalidUserConfig {
            error: format!(
                "factory resolved config type mismatch: stored {}, requested {}",
                self.value_type_name,
                type_name::<T>()
            ),
        })
    }

    /// Returns the precomputed safe snapshot value.
    #[must_use]
    pub const fn snapshot(&self) -> &Value {
        &self.snapshot
    }

    /// Returns the policy used to construct the safe snapshot.
    #[must_use]
    pub const fn snapshot_policy(&self) -> ConfigSnapshotPolicy {
        self.snapshot_policy
    }
}

/// Resolves a serde-backed configuration and safely serializes its typed form.
pub fn resolve_typed_config<T>(raw: &Value) -> Result<ResolvedComponentConfig, ConfigError>
where
    T: DeserializeOwned + Serialize + PartialEq + Send + Sync + 'static,
{
    let value: T =
        serde_json::from_value(raw.clone()).map_err(|error| ConfigError::InvalidUserConfig {
            error: error.to_string(),
        })?;
    ResolvedComponentConfig::typed_safe(value)
}

/// Resolves a serde-backed configuration while omitting it from snapshots.
pub fn resolve_omitted_config<T>(raw: &Value) -> Result<ResolvedComponentConfig, ConfigError>
where
    T: DeserializeOwned + PartialEq + Send + Sync + 'static,
{
    let value: T =
        serde_json::from_value(raw.clone()).map_err(|error| ConfigError::InvalidUserConfig {
            error: error.to_string(),
        })?;
    Ok(ResolvedComponentConfig::omitted(value))
}

/// Resolves a component that accepts no configuration.
pub fn resolve_no_config(raw: &Value) -> Result<ResolvedComponentConfig, ConfigError> {
    otel_arrow_dfe_config::validation::no_config(raw)?;
    ResolvedComponentConfig::typed_safe(())
}

/// Runtime node envelope paired with its factory-resolved component config.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedNodeConfig {
    effective: Arc<NodeUserConfig>,
    component: ResolvedComponentConfig,
}

impl ResolvedNodeConfig {
    /// Builds a resolved node envelope after the selected factory accepts its config.
    pub fn new(
        source: &NodeUserConfig,
        component: ResolvedComponentConfig,
        declared_policy: ConfigSnapshotPolicy,
    ) -> Result<Self, ConfigError> {
        reject_omitted_marker(&source.config)?;
        if component.snapshot_policy() != declared_policy {
            return Err(ConfigError::InvalidUserConfig {
                error:
                    "factory resolver returned a snapshot policy different from its registration"
                        .to_owned(),
            });
        }
        let mut effective = source.clone();
        effective.config = component.snapshot().clone();
        Ok(Self {
            effective: Arc::new(effective),
            component,
        })
    }

    /// Returns the safe effective node envelope used by engine metadata paths.
    #[must_use]
    pub fn effective(&self) -> Arc<NodeUserConfig> {
        Arc::clone(&self.effective)
    }

    /// Returns the resolved component-specific configuration as `T`.
    pub fn component_config<T>(&self) -> Result<Arc<T>, ConfigError>
    where
        T: Any + Send + Sync,
    {
        self.component.get::<T>()
    }

    /// Returns the node kind.
    #[must_use]
    pub fn kind(&self) -> NodeKind {
        self.effective.kind()
    }

    /// Returns the precomputed component snapshot value.
    #[must_use]
    pub const fn component_snapshot(&self) -> &Value {
        self.component.snapshot()
    }
}

/// Runtime extension envelope paired with its factory-resolved component config.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedExtensionConfig {
    effective: Arc<ExtensionUserConfig>,
    component: ResolvedComponentConfig,
}

impl ResolvedExtensionConfig {
    /// Builds a resolved extension envelope after the selected factory accepts its config.
    pub fn new(
        source: &ExtensionUserConfig,
        component: ResolvedComponentConfig,
        declared_policy: ConfigSnapshotPolicy,
    ) -> Result<Self, ConfigError> {
        reject_omitted_marker(&source.config)?;
        if component.snapshot_policy() != declared_policy {
            return Err(ConfigError::InvalidUserConfig {
                error:
                    "factory resolver returned a snapshot policy different from its registration"
                        .to_owned(),
            });
        }
        let mut effective = source.clone();
        effective.config = component.snapshot().clone();
        Ok(Self {
            effective: Arc::new(effective),
            component,
        })
    }

    /// Returns the safe effective extension envelope.
    #[must_use]
    pub fn effective(&self) -> Arc<ExtensionUserConfig> {
        Arc::clone(&self.effective)
    }

    /// Returns the resolved extension-specific configuration as `T`.
    pub fn component_config<T>(&self) -> Result<Arc<T>, ConfigError>
    where
        T: Any + Send + Sync,
    {
        self.component.get::<T>()
    }

    /// Returns the precomputed component snapshot value.
    #[must_use]
    pub const fn component_snapshot(&self) -> &Value {
        self.component.snapshot()
    }
}

/// A pipeline whose component configs have all been resolved by their factories.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedPipelineConfig {
    effective: PipelineConfig,
    nodes: HashMap<NodeId, Arc<ResolvedNodeConfig>>,
    extensions: HashMap<ExtensionId, Arc<ResolvedExtensionConfig>>,
}

impl ResolvedPipelineConfig {
    /// Creates a resolved pipeline and materializes its safe effective representation.
    pub fn new(
        source: &PipelineConfig,
        nodes: HashMap<NodeId, Arc<ResolvedNodeConfig>>,
        extensions: HashMap<ExtensionId, Arc<ResolvedExtensionConfig>>,
    ) -> Result<Self, ConfigError> {
        let node_snapshots = nodes
            .iter()
            .map(|(id, resolved)| (id.clone(), resolved.component_snapshot().clone()))
            .collect();
        let extension_snapshots = extensions
            .iter()
            .map(|(id, resolved)| (id.clone(), resolved.component_snapshot().clone()))
            .collect();
        let mut effective = source.clone();
        effective.replace_component_config_snapshots(&node_snapshots, &extension_snapshots)?;
        Ok(Self {
            effective,
            nodes,
            extensions,
        })
    }

    /// Returns the safe effective pipeline representation.
    #[must_use]
    pub const fn effective(&self) -> &PipelineConfig {
        &self.effective
    }

    /// Compares runtime semantics while ignoring only the pipeline policy block.
    #[must_use]
    pub fn eq_ignoring_policies(&self, other: &Self) -> bool {
        self.effective.eq_ignoring_policies(&other.effective)
            && self.nodes == other.nodes
            && self.extensions == other.extensions
    }

    /// Produces per-core build state without repeating component resolution.
    pub(crate) fn into_build_parts(
        mut self,
    ) -> (
        PipelineConfig,
        HashMap<NodeId, Arc<ResolvedNodeConfig>>,
        HashMap<ExtensionId, Arc<ResolvedExtensionConfig>>,
        Vec<(NodeId, NodeKind)>,
    ) {
        let removed = self.effective.remove_unconnected_nodes();
        let removed_ids: HashSet<&NodeId> = removed.iter().map(|(id, _)| id).collect();
        self.nodes.retain(|id, _| !removed_ids.contains(id));
        (self.effective, self.nodes, self.extensions, removed)
    }
}

fn reject_omitted_marker(raw: &Value) -> Result<(), ConfigError> {
    if raw.as_str() == Some(OMITTED_VALUE) {
        return Err(ConfigError::InvalidUserConfig {
            error: "the snapshot-only [OMITTED] marker is not valid submitted config".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_config::secret::{REDACTED_VALUE, RedactedString};
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct TestConfig {
        value: String,
        #[serde(default = "default_count")]
        count: usize,
    }

    const fn default_count() -> usize {
        3
    }

    /// Scenario: A typed component config omits a field that has a serde default.
    /// Guarantees: Resolution materializes the default once and includes it in the snapshot.
    #[test]
    fn typed_resolution_materializes_effective_defaults() {
        let resolved =
            resolve_typed_config::<TestConfig>(&serde_json::json!({"value": "x"})).unwrap();
        let explicit = resolve_typed_config::<TestConfig>(&serde_json::json!({
            "value": "x",
            "count": 3
        }))
        .unwrap();
        assert_eq!(
            resolved.snapshot(),
            &serde_json::json!({"value": "x", "count": 3})
        );
        assert_eq!(resolved.get::<TestConfig>().unwrap().count, 3);
        assert_eq!(resolved, explicit);
    }

    #[derive(Deserialize, Serialize, PartialEq)]
    struct SecretConfig {
        token: RedactedString,
    }

    /// Scenario: Two typed configs differ only in a secret serialized as the same marker.
    /// Guarantees: Snapshots match, but typed runtime equivalence detects the secret change.
    #[test]
    fn secret_changes_are_not_hidden_from_runtime_equivalence() {
        let first =
            resolve_typed_config::<SecretConfig>(&serde_json::json!({"token": "first"})).unwrap();
        let second =
            resolve_typed_config::<SecretConfig>(&serde_json::json!({"token": "second"})).unwrap();
        assert_eq!(first.snapshot(), second.snapshot());
        assert_eq!(first.snapshot()["token"], REDACTED_VALUE);
        assert_ne!(first, second);
    }

    #[derive(PartialEq)]
    struct SerializationFailure;

    impl Serialize for SerializationFailure {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("intentional failure"))
        }
    }

    /// Scenario: A TypedSafe component cannot serialize its resolved runtime value.
    /// Guarantees: Resolution fails before the candidate can be admitted.
    #[test]
    fn typed_safe_serialization_failure_rejects_candidate() {
        assert!(ResolvedComponentConfig::typed_safe(SerializationFailure).is_err());
    }

    /// Scenario: A factory creation callback requests a type different from its resolver output.
    /// Guarantees: The checked accessor fails without formatting or exposing the submitted value.
    #[test]
    fn typed_accessor_mismatch_reports_only_type_names() {
        let resolved = ResolvedComponentConfig::omitted("submitted-secret".to_owned());
        let error = resolved.get::<usize>().unwrap_err().to_string();
        assert!(error.contains("String"));
        assert!(error.contains("usize"));
        assert!(!error.contains("submitted-secret"));
    }

    static RESOLUTION_COUNT: AtomicUsize = AtomicUsize::new(0);

    fn counted_resolver(raw: &Value) -> Result<ResolvedComponentConfig, ConfigError> {
        _ = RESOLUTION_COUNT.fetch_add(1, Ordering::Relaxed);
        resolve_typed_config::<TestConfig>(raw)
    }

    /// Scenario: One accepted config is cloned and read by multiple runtime and snapshot consumers.
    /// Guarantees: Resolution runs once; typed access and snapshot reads use precomputed state.
    #[test]
    fn resolved_state_is_reused_without_reinvoking_factory() {
        RESOLUTION_COUNT.store(0, Ordering::Relaxed);
        let resolved = counted_resolver(&serde_json::json!({"value": "shared"})).unwrap();
        let clones = [resolved.clone(), resolved.clone(), resolved];
        for config in &clones {
            assert_eq!(config.get::<TestConfig>().unwrap().value, "shared");
            assert_eq!(config.snapshot()["count"], 3);
        }
        assert_eq!(RESOLUTION_COUNT.load(Ordering::Relaxed), 1);
    }

    /// Scenario: An omitted component contains arbitrary configuration data.
    /// Guarantees: Runtime equality uses typed data while its entire snapshot subtree is hidden.
    #[test]
    fn omitted_resolution_keeps_typed_equality() {
        let first =
            resolve_omitted_config::<TestConfig>(&serde_json::json!({"value": "x"})).unwrap();
        let same = resolve_omitted_config::<TestConfig>(&serde_json::json!({
            "value": "x",
            "count": 3
        }))
        .unwrap();
        let changed =
            resolve_omitted_config::<TestConfig>(&serde_json::json!({"value": "y"})).unwrap();
        assert_eq!(first.snapshot(), OMITTED_VALUE);
        assert_eq!(first, same);
        assert_ne!(first, changed);
    }

    struct SemanticConfig {
        canonical: String,
        cache: usize,
    }

    fn semantically_equal(left: &SemanticConfig, right: &SemanticConfig) -> bool {
        left.canonical == right.canonical
    }

    /// Scenario: A runtime config cannot derive PartialEq because it contains derived state.
    /// Guarantees: Its component-owned comparator controls equivalence without using snapshots.
    #[test]
    fn component_owned_semantic_equivalence_ignores_derived_state() {
        let first = ResolvedComponentConfig::omitted_by(
            SemanticConfig {
                canonical: "same".to_owned(),
                cache: 1,
            },
            semantically_equal,
        );
        let second = ResolvedComponentConfig::omitted_by(
            SemanticConfig {
                canonical: "same".to_owned(),
                cache: 2,
            },
            semantically_equal,
        );
        assert_eq!(first, second);
        assert_eq!(first.get::<SemanticConfig>().unwrap().cache, 1);
    }

    /// Scenario: A snapshot with a whole-config omission marker is submitted as source config.
    /// Guarantees: Resolution rejects the marker before invoking a component factory.
    #[test]
    fn omitted_marker_is_not_replayable() {
        let source = NodeUserConfig::with_user_config(
            "urn:otel:exporter:test".into(),
            Value::String(OMITTED_VALUE.to_owned()),
        );
        let component = ResolvedComponentConfig::omitted(TestConfig {
            value: "hidden".to_owned(),
            count: 3,
        });
        assert!(ResolvedNodeConfig::new(&source, component, ConfigSnapshotPolicy::Omit).is_err());
    }
}
