// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Regression checks for routed transform capacity recovery in the real loop.

use super::*;
use crate::processors::split_capacity_tests::{CapacityScenario, Reply, assert_capacity_recovery};

fn check_recovery(query: &str) {
    for inbound_limit in [1, 2] {
        for (replies, expected_permanent) in [
            ([Reply::Ack, Reply::Ack], true),
            ([Reply::TransientNack, Reply::TransientNack], false),
            ([Reply::PermanentNack, Reply::TransientNack], true),
        ] {
            let metadata = TestRuntime::<OtapPdata>::new();
            let processor = try_create_with_config(
                json!({
                    "opl_query": query,
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
                    split_input: to_otap_logs(create_log_records(&["INFO", "ERROR", "WARN"])),
                    recovery_input: to_otap_logs(create_log_records(&["INFO", "ERROR"])),
                    ports: &["info_port", "error_port", "warn_port"],
                    refusal_reason: "outbound slots were not available",
                    inbound_limit,
                    replies,
                    expected_permanent,
                },
            );
        }
    }
}

/// Scenario: the real transform loop exhausts capacity after default and named output sends.
/// Guarantees: pending Ack/Nacks complete once, immediate refusals Nack, and a later split succeeds.
#[test]
fn production_capacity_exhaustion_completes_and_recovers() {
    check_recovery(
        r#"logs
        | if (severity_text == "ERROR") { route_to "error_port" }
        | if (severity_text == "WARN") { route_to "warn_port" }
    "#,
    );
}

/// Scenario: three named routes exhaust two slots while the default output stays empty.
/// Guarantees: draining, zero-send refusal, reply classification, and slot reuse work without default data.
#[test]
fn production_named_only_capacity_exhaustion_completes_and_recovers() {
    check_recovery(
        r#"logs
        | if (severity_text == "INFO") { route_to "info_port" }
        | if (severity_text == "ERROR") { route_to "error_port" }
        | if (severity_text == "WARN") { route_to "warn_port" }
    "#,
    );
}
