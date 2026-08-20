// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Contract tests for the Weaver-generated SDK surface.

use otap_df_telemetry::attributes::AttributeSetHandler;
use otap_df_telemetry::metrics::MetricSetHandler;
use otap_df_telemetry::registry::TelemetryRegistryHandle;
use otap_df_telemetry_sdk::entities::{
    NodeAttributeSet, NodeAttributeSetIdentity, ParentOf, PipelineAttributeSet, SemanticEntity,
};
use otap_df_telemetry_sdk::metrics::receiver_otlp_requests::{
    AssociatedEntity, OtlpRequestMetrics,
};

fn node_identity(node_id: &str) -> NodeAttributeSet {
    NodeAttributeSet::new(NodeAttributeSetIdentity {
        core_id: 3,
        deployment_generation: 7,
        node_id: node_id.to_owned(),
        node_type: "receiver".to_owned(),
        node_urn: "urn:otel:receiver:otlp".to_owned(),
        numa_node_id: 0,
        pipeline_group_id: "default".to_owned(),
        pipeline_id: "ingest".to_owned(),
        service_instance_id: "engine-01".to_owned(),
    })
}

/// Scenario: two generated entities are iterated after both identities have been constructed.
/// Guarantees: each entity owns stable values and iteration does not alias thread-local scratch storage.
#[test]
fn generated_entities_own_their_attribute_values() {
    let first = node_identity("first");
    let second = node_identity("second");

    let first_values = first.attribute_values();
    let second_values = second.attribute_values();
    assert_ne!(first_values, second_values);
    assert_eq!(first_values.len(), first.descriptor().fields.len());
    assert_eq!(second_values.len(), second.descriptor().fields.len());
}

/// Scenario: a node entity is checked against the parent relationship declared by the registry.
/// Guarantees: the generated type system recognizes a pipeline as a direct parent of a node.
#[test]
fn generated_entity_relationships_are_typed() {
    fn assert_parent<Parent, Child>()
    where
        Parent: ParentOf<Child>,
        Child: SemanticEntity,
    {
    }

    assert_parent::<PipelineAttributeSet, NodeAttributeSet>();
}

/// Scenario: the generated OTLP receiver metric set is registered with an associated node entity and records counters.
/// Guarantees: generated association markers compile, descriptors preserve registry metadata, and snapshots follow field order.
#[test]
fn generated_metric_set_registers_and_snapshots() {
    fn assert_associated<T: AssociatedEntity>() {}
    assert_associated::<NodeAttributeSet>();

    let registry = TelemetryRegistryHandle::new();
    let mut metrics = OtlpRequestMetrics::register(&registry, node_identity("otlp"));
    metrics.started.inc();
    metrics.payload_size.add(512);

    let snapshot = metrics.snapshot();
    assert_eq!(metrics.descriptor().name, "receiver.otlp.requests");
    assert_eq!(
        snapshot.get_metrics().len(),
        metrics.descriptor().metrics.len()
    );
    assert!(metrics.needs_flush());
}
