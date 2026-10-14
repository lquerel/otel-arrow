// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Factory-owned component configuration resolution.

use otel_arrow_dfe_config::engine::OtelDataflowSpec;
use otel_arrow_dfe_config::error::Error as ConfigError;
use otel_arrow_dfe_config::extension::ExtensionUserConfig;
use otel_arrow_dfe_config::node::{NodeKind, NodeUserConfig};
use otel_arrow_dfe_config::pipeline::PipelineConfig;
use otel_arrow_dfe_config::pipeline_group::PipelineGroupConfig;
use otel_arrow_dfe_config::secret::OMITTED_VALUE;
use otel_arrow_dfe_config::{ExtensionId, NodeId, PipelineGroupId, PipelineId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::any::{Any, TypeId, type_name};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

type ErasedConfig = dyn Any + Send + Sync;
type EquivalentFn = dyn Fn(&ErasedConfig, &ErasedConfig) -> bool + Send + Sync;

/// Produces a control-plane diagnostic without trusting a component's error text.
/// Parser, validator, serializer, and creation errors can echo submitted values.
#[must_use]
pub fn safe_component_error(component: &str, phase: &str) -> ConfigError {
    ConfigError::InvalidUserConfig {
        error: format!(
            "component {component} failed {phase}; check its configuration against the component schema"
        ),
    }
}

/// A precomputed, privacy-preserving engine configuration view for reporting.
///
/// "Safe" means safe for control-plane reporting under the declared factory
/// snapshot policies. This value is not a runtime configuration or a replayable
/// source artifact, and it does not replace access control.
///
/// Serialization retains the existing engine/group/pipeline JSON envelopes.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SafeConfigSnapshot {
    config: OtelDataflowSpec,
}

impl SafeConfigSnapshot {
    /// Creates a conservative safe view with every component config omitted.
    /// Factory-produced typed snapshots can then be installed through the
    /// scoped methods below. There is no constructor for an unsanitized envelope.
    #[must_use]
    pub fn from_source_omitted(mut config: OtelDataflowSpec) -> Self {
        fn omit_pipeline(pipeline: &mut PipelineConfig) {
            let nodes = pipeline
                .node_iter()
                .map(|(id, _)| (id.clone(), Value::String(OMITTED_VALUE.to_owned())))
                .collect();
            let extensions = pipeline
                .extension_iter()
                .map(|(id, _)| (id.clone(), Value::String(OMITTED_VALUE.to_owned())))
                .collect();
            pipeline
                .replace_component_config_snapshots(&nodes, &extensions)
                .expect("omission maps contain exactly the source component IDs");
        }
        for group in config.groups.values_mut() {
            for pipeline in group.pipelines.values_mut() {
                omit_pipeline(pipeline);
            }
        }
        let mut observability = config
            .engine
            .observability
            .pipeline
            .clone()
            .into_pipeline_config();
        omit_pipeline(&mut observability);
        config.engine.observability.pipeline.nodes = observability.nodes().clone();
        config.engine.observability.pipeline.extensions = observability.extensions().clone();
        let extensions = config
            .engine
            .controller
            .extensions
            .iter()
            .map(|(id, _)| (id.clone(), Value::String(OMITTED_VALUE.to_owned())))
            .collect();
        config
            .engine
            .controller
            .extensions
            .replace_config_snapshots(&extensions)
            .expect("omission map contains exactly the controller extension IDs");
        for value in config.engine.custom.values_mut() {
            *value = Value::String(OMITTED_VALUE.to_owned());
        }
        Self { config }
    }

    /// Returns the reporting envelope, not a replayable source configuration.
    #[must_use]
    pub const fn as_config(&self) -> &OtelDataflowSpec {
        &self.config
    }

    /// Installs a factory-resolved pipeline snapshot, never a submitted pipeline.
    pub fn insert_pipeline(
        &mut self,
        group: PipelineGroupId,
        id: PipelineId,
        resolved: &ResolvedPipelineConfig,
    ) {
        _ = self
            .config
            .groups
            .entry(group)
            .or_default()
            .pipelines
            .insert(id, resolved.safe_config_snapshot().clone());
    }

    /// Installs factory-resolved engine-scoped components during candidate assembly.
    pub fn install_engine_components(
        &mut self,
        observability: &ResolvedPipelineConfig,
        extensions: &HashMap<ExtensionId, Arc<ResolvedExtensionConfig>>,
    ) -> Result<(), ConfigError> {
        self.config.engine.observability.pipeline.nodes =
            observability.safe_config_snapshot().nodes().clone();
        self.config.engine.observability.pipeline.extensions =
            observability.safe_config_snapshot().extensions().clone();
        let snapshots = extensions
            .iter()
            .map(|(id, config)| (id.clone(), config.safe_component_snapshot().clone()))
            .collect();
        self.config
            .engine
            .controller
            .extensions
            .replace_config_snapshots(&snapshots)
    }

    /// Adds public group metadata; non-empty groups must use resolved pipeline insertion.
    pub fn insert_empty_group(
        &mut self,
        id: PipelineGroupId,
        group: PipelineGroupConfig,
    ) -> Result<(), ConfigError> {
        if !group.pipelines.is_empty() {
            return Err(ConfigError::InvalidUserConfig {
                error: "safe group insertion requires an empty pipeline set".to_owned(),
            });
        }
        _ = self.config.groups.insert(id, group);
        Ok(())
    }

    /// Removes a drained pipeline from reporting.
    pub fn remove_pipeline(&mut self, group: &PipelineGroupId, id: &PipelineId) {
        if let Some(group) = self.config.groups.get_mut(group) {
            _ = group.pipelines.remove(id);
        }
    }

    /// Removes a drained group from reporting.
    pub fn remove_group(&mut self, id: &PipelineGroupId) {
        _ = self.config.groups.remove(id);
    }

    /// Merges only an already safe candidate after successful reconciliation.
    pub fn merge_committed(&mut self, desired: &Self, delete_missing: bool) {
        if delete_missing {
            *self = desired.clone();
            return;
        }
        self.config.version = desired.config.version.clone();
        self.config.policies = desired.config.policies.clone();
        self.config.topics = desired.config.topics.clone();
        self.config.engine = desired.config.engine.clone();
        for (id, desired_group) in &desired.config.groups {
            let group = self.config.groups.entry(id.clone()).or_default();
            group.policies = desired_group.policies.clone();
            group.topics = desired_group.topics.clone();
            group.pipelines.extend(desired_group.pipelines.clone());
        }
    }
}

impl std::ops::Deref for SafeConfigSnapshot {
    type Target = OtelDataflowSpec;

    fn deref(&self) -> &Self::Target {
        self.as_config()
    }
}

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
    pub const fn safe_config_snapshot(&self) -> &Value {
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
#[derive(Clone, Debug)]
pub struct ResolvedNodeConfig {
    safe_config_snapshot: Arc<NodeUserConfig>,
    envelope: NodeUserConfig,
    component: ResolvedComponentConfig,
}

impl PartialEq for ResolvedNodeConfig {
    fn eq(&self, other: &Self) -> bool {
        self.envelope == other.envelope && self.component == other.component
    }
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
        let mut safe_config_snapshot = source.clone();
        safe_config_snapshot.config = component.safe_config_snapshot().clone();
        let mut envelope = source.clone();
        envelope.config = Value::Null;
        Ok(Self {
            safe_config_snapshot: Arc::new(safe_config_snapshot),
            envelope,
            component,
        })
    }

    /// Returns the safe configuration snapshot node envelope used by engine metadata paths.
    #[must_use]
    pub fn safe_config_snapshot(&self) -> Arc<NodeUserConfig> {
        Arc::clone(&self.safe_config_snapshot)
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
        self.safe_config_snapshot.kind()
    }

    /// Returns the precomputed component snapshot value.
    #[must_use]
    pub const fn safe_component_snapshot(&self) -> &Value {
        self.component.safe_config_snapshot()
    }
}

/// Runtime extension envelope paired with its factory-resolved component config.
#[derive(Clone, Debug)]
pub struct ResolvedExtensionConfig {
    safe_config_snapshot: Arc<ExtensionUserConfig>,
    envelope: ExtensionUserConfig,
    component: ResolvedComponentConfig,
}

impl PartialEq for ResolvedExtensionConfig {
    fn eq(&self, other: &Self) -> bool {
        self.envelope == other.envelope && self.component == other.component
    }
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
        let mut safe_config_snapshot = source.clone();
        safe_config_snapshot.config = component.safe_config_snapshot().clone();
        let mut envelope = source.clone();
        envelope.config = Value::Null;
        Ok(Self {
            safe_config_snapshot: Arc::new(safe_config_snapshot),
            envelope,
            component,
        })
    }

    /// Returns the safe configuration snapshot extension envelope.
    #[must_use]
    pub fn safe_config_snapshot(&self) -> Arc<ExtensionUserConfig> {
        Arc::clone(&self.safe_config_snapshot)
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
    pub const fn safe_component_snapshot(&self) -> &Value {
        self.component.safe_config_snapshot()
    }
}

/// A pipeline whose component configs have all been resolved by their factories.
#[derive(Clone, Debug)]
pub struct ResolvedPipelineConfig {
    safe_config_snapshot: PipelineConfig,
    envelope: PipelineConfig,
    nodes: HashMap<NodeId, Arc<ResolvedNodeConfig>>,
    extensions: HashMap<ExtensionId, Arc<ResolvedExtensionConfig>>,
}

impl PartialEq for ResolvedPipelineConfig {
    fn eq(&self, other: &Self) -> bool {
        self.envelope == other.envelope
            && self.nodes == other.nodes
            && self.extensions == other.extensions
    }
}

impl ResolvedPipelineConfig {
    /// Creates a resolved pipeline and materializes its safe configuration snapshot representation.
    pub fn new(
        source: &PipelineConfig,
        nodes: HashMap<NodeId, Arc<ResolvedNodeConfig>>,
        extensions: HashMap<ExtensionId, Arc<ResolvedExtensionConfig>>,
    ) -> Result<Self, ConfigError> {
        let node_snapshots = nodes
            .iter()
            .map(|(id, resolved)| (id.clone(), resolved.safe_component_snapshot().clone()))
            .collect();
        let extension_snapshots = extensions
            .iter()
            .map(|(id, resolved)| (id.clone(), resolved.safe_component_snapshot().clone()))
            .collect();
        let mut safe_config_snapshot = source.clone();
        safe_config_snapshot
            .replace_component_config_snapshots(&node_snapshots, &extension_snapshots)?;
        let mut envelope = source.clone();
        envelope.replace_component_config_snapshots(
            &nodes.keys().map(|id| (id.clone(), Value::Null)).collect(),
            &extensions
                .keys()
                .map(|id| (id.clone(), Value::Null))
                .collect(),
        )?;
        Ok(Self {
            safe_config_snapshot,
            envelope,
            nodes,
            extensions,
        })
    }

    /// Returns the safe configuration snapshot pipeline representation.
    #[must_use]
    pub const fn safe_config_snapshot(&self) -> &PipelineConfig {
        &self.safe_config_snapshot
    }

    /// Returns one factory-resolved node configuration by id.
    #[must_use]
    pub fn node(&self, id: &NodeId) -> Option<&Arc<ResolvedNodeConfig>> {
        self.nodes.get(id)
    }

    /// Iterates over all factory-resolved node configurations.
    pub fn nodes(&self) -> impl Iterator<Item = (&NodeId, &Arc<ResolvedNodeConfig>)> {
        self.nodes.iter()
    }

    /// Compares runtime semantics while ignoring only the pipeline policy block.
    #[must_use]
    pub fn eq_ignoring_policies(&self, other: &Self) -> bool {
        self.envelope.eq_ignoring_policies(&other.envelope)
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
        let removed = self.safe_config_snapshot.remove_unconnected_nodes();
        let removed_ids: HashSet<&NodeId> = removed.iter().map(|(id, _)| id).collect();
        self.nodes.retain(|id, _| !removed_ids.contains(id));
        (
            self.safe_config_snapshot,
            self.nodes,
            self.extensions,
            removed,
        )
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

    /// Scenario: A named SafeConfigSnapshot contains precomputed reporting envelopes and markers.
    /// Guarantees: Serialization preserves the engine JSON shape without an added wrapper field.
    #[test]
    fn safe_config_snapshot_preserves_reporting_json_envelope() {
        let config: OtelDataflowSpec = serde_json::from_value(serde_json::json!({
            "version": "otel_dataflow/v1",
            "engine": {"custom": {"private": OMITTED_VALUE}},
            "groups": {"g1": {"pipelines": {"p1": {"nodes": {
                "exporter": {"type": "urn:test:exporter:example", "config": OMITTED_VALUE}
            }}}}}
        }))
        .unwrap();
        let snapshot = SafeConfigSnapshot::from_source_omitted(config);
        let config = snapshot.as_config().clone();
        let encoded = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(encoded, serde_json::to_value(&config).unwrap());
        assert!(encoded.get("config").is_none());
        assert_eq!(snapshot.as_config(), &config);
        assert_eq!(encoded["engine"]["custom"]["private"], OMITTED_VALUE);
        assert_eq!(
            encoded["groups"]["g1"]["pipelines"]["p1"]["nodes"]["exporter"]["config"],
            OMITTED_VALUE
        );
    }

    /// Scenario: A source envelope contains arbitrary secrets in regular, controller, and observability components.
    /// Guarantees: Safe snapshot construction omits every private subtree and rejects raw non-empty group insertion.
    #[test]
    fn snapshot_construction_cannot_wrap_raw_component_values() {
        let config: OtelDataflowSpec = serde_json::from_value(serde_json::json!({
            "version": "otel_dataflow/v1",
            "engine": {"custom": {"private": {"nested": "private-custom"}},
                "controller": {"extensions": {"private": {"type": "urn:test:extension:example", "config": {"value": "private-controller"}}}}},
            "groups": {"g1": {"pipelines": {"p1": {"nodes": {
                "exporter": {"type": "urn:test:exporter:example", "config": {"nested": ["private-node"]}}
            }, "extensions": {"private": {"type": "urn:test:extension:example", "config": {"value": "private-pipeline-extension"}}}}}}}
        })).unwrap();
        let mut snapshot = SafeConfigSnapshot::from_source_omitted(config.clone());
        let before = snapshot.clone();
        assert!(
            !serde_json::to_string(&snapshot)
                .unwrap()
                .contains("private-")
        );
        assert!(
            snapshot
                .insert_empty_group(
                    "new".into(),
                    config.groups[&PipelineGroupId::from("g1")].clone()
                )
                .is_err()
        );
        assert_eq!(snapshot, before);
        for (_, node) in snapshot.engine.observability.pipeline.nodes.iter() {
            assert_eq!(node.config, OMITTED_VALUE);
        }
    }

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
    fn typed_resolution_materializes_snapshot_defaults() {
        let resolved =
            resolve_typed_config::<TestConfig>(&serde_json::json!({"value": "x"})).unwrap();
        let explicit = resolve_typed_config::<TestConfig>(&serde_json::json!({
            "value": "x",
            "count": 3
        }))
        .unwrap();
        assert_eq!(
            resolved.safe_config_snapshot(),
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
        assert_eq!(first.safe_config_snapshot(), second.safe_config_snapshot());
        assert_eq!(first.safe_config_snapshot()["token"], REDACTED_VALUE);
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
            assert_eq!(config.safe_config_snapshot()["count"], 3);
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
        assert_eq!(first.safe_config_snapshot(), OMITTED_VALUE);
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

    /// Scenario: Equal runtime values have different component-owned safe snapshots.
    /// Guarantees: Node, extension, and pipeline equality ignore reporting output but retain metadata.
    #[test]
    fn runtime_equivalence_is_independent_of_snapshot_output() {
        let source: PipelineConfig = serde_json::from_value(serde_json::json!({
            "nodes": {"receiver": {"type": "urn:otel:receiver:test", "config": {}}},
            "extensions": {"auth": {"type": "urn:otel:extension:test", "config": {}}}
        }))
        .unwrap();
        let make = |label: &str| {
            let node = source.nodes().get("receiver").unwrap();
            let extension = source.extensions().get("auth").unwrap();
            ResolvedPipelineConfig::new(
                &source,
                HashMap::from([(
                    ("receiver".into()),
                    Arc::new(
                        ResolvedNodeConfig::new(
                            node,
                            ResolvedComponentConfig::custom_safe(
                                42usize,
                                Value::String(label.to_owned()),
                            ),
                            ConfigSnapshotPolicy::CustomSafe,
                        )
                        .unwrap(),
                    ),
                )]),
                HashMap::from([(
                    ("auth".into()),
                    Arc::new(
                        ResolvedExtensionConfig::new(
                            extension,
                            ResolvedComponentConfig::custom_safe(
                                42usize,
                                Value::String(label.to_owned()),
                            ),
                            ConfigSnapshotPolicy::CustomSafe,
                        )
                        .unwrap(),
                    ),
                )]),
            )
            .unwrap()
        };
        let first = make("first safe display");
        let second = make("second safe display");
        assert_ne!(first.safe_config_snapshot(), second.safe_config_snapshot());
        assert_eq!(first, second);
        assert!(first.eq_ignoring_policies(&second));
        let mut changed = second.clone();
        changed.envelope = serde_json::from_value(serde_json::json!({
            "nodes": {"receiver": {"type": "urn:otel:receiver:test", "outputs": ["changed"]}},
            "extensions": {"auth": {"type": "urn:otel:extension:test"}}
        }))
        .unwrap();
        assert!(!first.eq_ignoring_policies(&changed));
    }

    /// Scenario: A resolver returns a policy different from the factory registration.
    /// Guarantees: Admission rejects the mismatch without exposing the submitted secret.
    #[test]
    fn policy_mismatch_is_rejected_without_disclosing_source() {
        let source = NodeUserConfig::with_user_config(
            "urn:otel:receiver:test".into(),
            serde_json::json!({"token": "private-input"}),
        );
        let error = ResolvedNodeConfig::new(
            &source,
            ResolvedComponentConfig::omitted(42usize),
            ConfigSnapshotPolicy::TypedSafe,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("snapshot policy"));
        assert!(!error.contains("private-input"));
    }
}
