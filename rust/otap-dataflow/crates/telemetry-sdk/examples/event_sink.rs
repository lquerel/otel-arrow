// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Emits a Weaver-generated event to a backend-independent in-memory sink.

use otap_df_telemetry::attributes::{AttributeSetHandler, AttributeValue};
use otap_df_telemetry_sdk::entities::{NodeAttributeSet, NodeAttributeSetIdentity};
use otap_df_telemetry_sdk::event::{
    EventAttributes, EventClient, EventDescriptor, EventLevel, EventSink,
};
use otap_df_telemetry_sdk::events::otap::OtapSocketKeepaliveRetriesIgnored;

#[derive(Debug)]
struct RecordedEvent {
    canonical_name: &'static str,
    wire_name: &'static str,
    level: EventLevel,
    entity_schema: &'static str,
    attributes: Vec<(&'static str, AttributeValue)>,
}

#[derive(Debug, Default)]
struct InMemoryEventSink {
    events: Vec<RecordedEvent>,
}

impl EventSink for InMemoryEventSink {
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
            canonical_name: descriptor.name,
            wire_name: descriptor.wire_name,
            level,
            entity_schema: entity.schema_name(),
            attributes: values,
        });
    }
}

fn main() {
    let node = NodeAttributeSet::new(NodeAttributeSetIdentity {
        core_id: 3,
        deployment_generation: 7,
        node_id: "otlp-input".to_owned(),
        node_type: "receiver".to_owned(),
        node_urn: "urn:otel:receiver:otlp".to_owned(),
        numa_node_id: 0,
        pipeline_group_id: "default".to_owned(),
        pipeline_id: "ingest".to_owned(),
        service_instance_id: "engine-01".to_owned(),
    });
    let event = OtapSocketKeepaliveRetriesIgnored {
        platform: "linux".to_owned(),
    };
    let mut client = EventClient::new(InMemoryEventSink::default());

    event.emit_warn(&mut client, &node);

    let recorded = &client.sink().events[0];
    assert_eq!(
        recorded.canonical_name,
        "otap.socket.keepalive_retries_ignored"
    );
    assert_eq!(recorded.wire_name, "Socket.KeepaliveRetriesIgnored");
    assert_eq!(recorded.level, EventLevel::Warn);
    assert_eq!(recorded.entity_schema, "node.attrs");
    assert_eq!(
        recorded.attributes,
        vec![("platform", AttributeValue::String("linux".to_owned()))]
    );
}
