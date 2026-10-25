// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use serde_json::json;

/// Scenario: Both shared gRPC exporter factories resolve credential-bearing endpoint URLs.
/// Guarantees: Snapshots hide full endpoints, runtime values retain them, and secret changes compare unequal.
#[test]
fn grpc_factory_snapshots_hide_endpoints_and_compare_real_values() {
    use crate::exporters::{otap_exporter::OTAP_EXPORTER, otlp_grpc_exporter::OTLP_EXPORTER};
    for resolve in [OTLP_EXPORTER.resolve_config, OTAP_EXPORTER.resolve_config] {
        for endpoint in [
            "http://user:private-password@127.0.0.1:9",
            "http://127.0.0.1:9/private-path?token=private-query",
        ] {
            let raw = json!({"grpc_endpoint": endpoint});
            let resolved = resolve(&raw).unwrap();
            assert_eq!(
                resolved.safe_config_snapshot()["grpc_endpoint"],
                "[REDACTED]"
            );
            assert_eq!(resolved.safe_config_snapshot()["concurrency_limit"], 256);
            assert!(
                !resolved
                    .safe_config_snapshot()
                    .to_string()
                    .contains("private-")
            );
            let changed = resolve(&json!({"grpc_endpoint": "http://127.0.0.1:10"})).unwrap();
            assert_ne!(resolved, changed);
            assert_eq!(
                resolved.safe_config_snapshot(),
                changed.safe_config_snapshot()
            );
        }
        for marker in ["[REDACTED]", "[OMITTED]"] {
            assert!(resolve(&json!({"grpc_endpoint": marker})).is_err());
        }
    }
}
