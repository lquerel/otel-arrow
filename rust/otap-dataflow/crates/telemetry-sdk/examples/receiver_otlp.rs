// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Registers and records a Weaver-generated OTLP receiver metric set.

use otap_df_telemetry::metrics::MetricSetHandler;
use otap_df_telemetry::registry::TelemetryRegistryHandle;
use otap_df_telemetry_sdk::entities::{NodeAttributeSet, NodeAttributeSetIdentity};
use otap_df_telemetry_sdk::metrics::receiver_otlp_requests::OtlpRequestMetrics;

fn main() {
    let registry = TelemetryRegistryHandle::new();
    let receiver = NodeAttributeSet::new(NodeAttributeSetIdentity {
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

    let mut metrics = OtlpRequestMetrics::register(&registry, receiver);
    metrics.started.inc();
    metrics.payload_size.add(2_048);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.get_metrics().len(), 3);
    assert_eq!(metrics.descriptor().name, "receiver.otlp.requests");
}
