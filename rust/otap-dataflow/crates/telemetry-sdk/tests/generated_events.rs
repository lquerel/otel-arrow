// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Contract tests for Weaver-generated semantic events.

use otap_df_telemetry::attributes::{AttributeSetHandler, AttributeValue};
use otap_df_telemetry::descriptor::AttributeValueType;
use otap_df_telemetry_sdk::entities::{NodeAttributeSet, NodeAttributeSetIdentity};
use otap_df_telemetry_sdk::event::{
    EventAttributes, EventClient, EventDescriptor, EventLevel, EventSeverity, EventSink,
};
use otap_df_telemetry_sdk::events::contrib_nodes::RecordsetKqlProcessorQueryOutput;
use otap_df_telemetry_sdk::events::otap::{
    OtapSocketKeepaliveRetriesIgnored, OtlpHttpReceiverPipelineSendFailed,
};
use otap_df_telemetry_sdk::events::quiver::QuiverEngineInit;

#[derive(Debug)]
struct RecordedEvent {
    descriptor: &'static EventDescriptor,
    level: EventLevel,
    entity_schema: &'static str,
    attributes: Vec<(&'static str, AttributeValue)>,
}

#[derive(Debug, Default)]
struct RecordingSink {
    events: Vec<RecordedEvent>,
}

impl EventSink for RecordingSink {
    fn emit(
        &mut self,
        entity: &dyn AttributeSetHandler,
        descriptor: &'static EventDescriptor,
        level: EventLevel,
        attributes: &dyn EventAttributes,
    ) {
        let mut values = Vec::with_capacity(descriptor.attributes.len());
        attributes.visit_attributes(&mut |attribute, value| {
            values.push((attribute.wire_key, value.into_owned()));
        });
        self.events.push(RecordedEvent {
            descriptor,
            level,
            entity_schema: entity.schema_name(),
            attributes: values,
        });
    }
}

fn node_entity() -> NodeAttributeSet {
    NodeAttributeSet::new(NodeAttributeSetIdentity {
        core_id: 3,
        deployment_generation: 7,
        node_id: "otlp-input".to_owned(),
        node_type: "receiver".to_owned(),
        node_urn: "urn:otel:receiver:otlp".to_owned(),
        numa_node_id: 0,
        pipeline_group_id: "default".to_owned(),
        pipeline_id: "ingest".to_owned(),
        service_instance_id: "engine-01".to_owned(),
    })
}

/// Scenario: a generated event with a normalized convention name is emitted at one of its fixed call-site levels.
/// Guarantees: the sink receives canonical and wire identities, the selected level, the associated entity, and typed attributes.
#[test]
fn generated_event_preserves_registry_and_wire_metadata() {
    let event = OtapSocketKeepaliveRetriesIgnored {
        platform: "linux".to_owned(),
    };
    let mut client = EventClient::new(RecordingSink::default());

    event.emit_warn(&mut client, &node_entity());

    let recorded = &client.sink().events[0];
    assert_eq!(
        recorded.descriptor.name,
        "otap.socket.keepalive_retries_ignored"
    );
    assert_eq!(
        recorded.descriptor.wire_name,
        "Socket.KeepaliveRetriesIgnored"
    );
    assert_eq!(recorded.descriptor.scope_names, &["otap-df-otap"]);
    assert_eq!(recorded.descriptor.severity_levels, &[EventSeverity::Warn]);
    assert_eq!(recorded.level, EventLevel::Warn);
    assert_eq!(recorded.entity_schema, "node.attrs");
    assert_eq!(
        recorded.attributes,
        vec![("platform", AttributeValue::String("linux".to_owned()))]
    );
}

/// Scenario: an event attribute is backed by a semantic-convention string enum inferred from Rust.
/// Guarantees: generated payloads expose a concrete String and report the enum's string wire type instead of AttributeValue::Any.
#[test]
fn generated_event_uses_enum_wire_type() {
    let event = OtlpHttpReceiverPipelineSendFailed {
        path: "/v1/logs".to_owned(),
        signal: "logs".to_owned(),
    };
    let mut client = EventClient::new(RecordingSink::default());

    event.emit_warn(&mut client, &node_entity());

    assert_eq!(
        OtlpHttpReceiverPipelineSendFailed::DESCRIPTOR.attributes[1].value_type,
        AttributeValueType::String
    );
    assert_eq!(
        client.sink().events[0].attributes,
        vec![
            ("path", AttributeValue::String("/v1/logs".to_owned())),
            ("signal", AttributeValue::String("logs".to_owned())),
        ]
    );
}

/// Scenario: every recommended attribute is absent when a generated multi-level event is emitted.
/// Guarantees: optional fields are omitted from the zero-allocation visitor while all observed severity choices remain available.
#[test]
fn generated_event_omits_absent_optional_attributes() {
    let event = QuiverEngineInit {
        budget_cap: None,
        error: None,
        error_type: None,
        min_budget: None,
        path: None,
        reason: None,
        segment_size: None,
        set_permissions_supported: None,
    };
    let mut client = EventClient::new(RecordingSink::default());

    event.emit_error(&mut client, &node_entity());

    let recorded = &client.sink().events[0];
    assert_eq!(
        recorded.descriptor.severity_levels,
        &[
            EventSeverity::Debug,
            EventSeverity::Error,
            EventSeverity::Warn
        ]
    );
    assert_eq!(recorded.level, EventLevel::Error);
    assert!(recorded.attributes.is_empty());
}

/// Scenario: an event originating from a runtime-selectable call site is emitted with a concrete level.
/// Guarantees: the generated dynamic emission method forwards the selected occurrence level while retaining dynamic call-site metadata.
#[test]
fn generated_dynamic_event_accepts_a_concrete_level() {
    let event = RecordsetKqlProcessorQueryOutput {
        query_column_number: 8,
        query_line_number: 21,
    };
    let mut client = EventClient::new(RecordingSink::default());

    event.emit(&mut client, &node_entity(), EventLevel::Info);

    let recorded = &client.sink().events[0];
    assert_eq!(
        recorded.descriptor.severity_levels,
        &[EventSeverity::Dynamic]
    );
    assert_eq!(recorded.level, EventLevel::Info);
    assert_eq!(recorded.attributes.len(), 2);
}
