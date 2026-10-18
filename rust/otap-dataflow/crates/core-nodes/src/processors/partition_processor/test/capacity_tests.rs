// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Production-loop regression checks for capacity refusal, completion, and slot reuse.

use super::*;
use crate::processors::split_capacity_tests::{CapacityScenario, Reply, assert_capacity_recovery};

fn input(count: usize) -> OtapArrowRecords {
    otlp_to_otap(&OtlpProtoMessage::Logs(LogsData {
        resource_logs: vec![ResourceLogs::new(
            Resource::default(),
            vec![ScopeLogs::new(
                InstrumentationScope::default(),
                (0..count)
                    .map(|id| {
                        LogRecord::build()
                            .attributes(vec![KeyValue::new(
                                "x",
                                AnyValue::new_string(id.to_string()),
                            )])
                            .finish()
                    })
                    .collect::<Vec<_>>(),
            )],
        )],
    }))
}

/// Scenario: the production loop refuses a third partition and another request at capacity.
/// Guarantees: Ack/Nack replies drain once, refusals preserve input, and slots support a later split.
#[test]
fn production_capacity_exhaustion_completes_and_recovers() {
    for inbound_limit in [1, 2] {
        for (replies, expected_permanent) in [
            ([Reply::Ack, Reply::Ack], true),
            ([Reply::TransientNack, Reply::TransientNack], false),
            ([Reply::PermanentNack, Reply::TransientNack], true),
        ] {
            let metadata = TestRuntime::<OtapPdata>::new();
            let processor = create_processor_with_config(
                serde_json::json!({
                    "partition_by": { "opl_expression": "attributes[\"x\"]" },
                    "partition_header_name": "partition-header",
                    "inbound_request_limit": inbound_limit,
                    "outbound_request_limit": 2,
                }),
                &metadata,
            )
            .unwrap();
            assert_capacity_recovery(
                processor,
                metadata.metrics_reporter(),
                CapacityScenario {
                    split_input: input(3),
                    recovery_input: input(2),
                    ports: &[],
                    refusal_reason: "insufficient outbound slots for partitions",
                    inbound_limit,
                    replies,
                    expected_permanent,
                },
            );
        }
    }
}
