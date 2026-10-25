// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_engine::component_config::{NormalizeConfigForEditFn, ResolveConfigFn};
use serde_json::{Value, json};

fn check_basis(
    resolve: ResolveConfigFn,
    normalize: NormalizeConfigForEditFn,
    mut raw: Value,
) -> Value {
    let before = resolve(&raw).unwrap();
    normalize(&mut raw).unwrap();
    assert_eq!(before, resolve(&raw).unwrap());
    let once = raw.clone();
    normalize(&mut raw).unwrap();
    assert_eq!(raw, once);
    raw
}

/// Scenario: HTTP and OTLP/gRPC sources use legacy compression keys and private credentials.
/// Guarantees: Canonical edits preserve secrets, are idempotent, and retain typed runtime meaning.
#[test]
fn compression_edit_bases_preserve_private_values() {
    use crate::exporters::{
        otlp_grpc_exporter::OTLP_EXPORTER, otlp_http_exporter::OTLP_HTTP_EXPORTER,
    };
    let mut http = check_basis(
        OTLP_HTTP_EXPORTER.resolve_config,
        OTLP_HTTP_EXPORTER.normalize_config_for_edit.unwrap(),
        json!({
            "endpoint": "http://user:private-url@127.0.0.1:9", "client_pool_size": 1,
            "http": {"compression_method": "gzip", "headers": {"authorization": "private-header"}}
        }),
    );
    assert_eq!(http["http"]["compression"], "gzip");
    assert!(http["http"].get("compression_method").is_none());
    assert_eq!(http["http"]["headers"]["authorization"], "private-header");
    http["http"]["compression"] = "deflate".into();
    assert!((OTLP_HTTP_EXPORTER.resolve_config)(&http).is_ok());
    _ = http["http"].as_object_mut().unwrap().remove("compression");
    assert!((OTLP_HTTP_EXPORTER.resolve_config)(&http).is_ok());
    let mut grpc = check_basis(
        OTLP_EXPORTER.resolve_config,
        OTLP_EXPORTER.normalize_config_for_edit.unwrap(),
        json!({
            "grpc_endpoint": "http://user:private-url@127.0.0.1:9", "compression_method": "gzip", "headers": {"authorization": "private-header"}
        }),
    );
    assert_eq!(grpc["compression"], "gzip");
    grpc["compression"] = "zstd".into();
    assert!((OTLP_EXPORTER.resolve_config)(&grpc).is_ok());
    assert_eq!(grpc["headers"]["authorization"], "private-header");
}

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

/// Scenario: OTAP source config sets its independent Arrow compression_method field.
/// Guarantees: Private edit normalization never reinterprets that field as gRPC compression.
#[test]
fn otap_arrow_compression_is_not_a_grpc_alias() {
    use crate::exporters::otap_exporter::OTAP_EXPORTER;
    let mut raw = json!({"grpc_endpoint": "http://127.0.0.1:9", "compression_method": "zstd"});
    let before = (OTAP_EXPORTER.resolve_config)(&raw).unwrap();
    if let Some(normalize) = OTAP_EXPORTER.normalize_config_for_edit {
        normalize(&mut raw).unwrap();
    }
    assert_eq!(before, (OTAP_EXPORTER.resolve_config)(&raw).unwrap());
    assert_eq!(raw["compression_method"], "zstd");
    assert!(raw.get("compression").is_none());
}

/// Scenario: Parquet source uses its legacy externally tagged partition strategy name.
/// Guarantees: Canonical edit normalization changes the key without changing runtime behavior.
#[test]
fn parquet_edit_basis_normalizes_partition_variant() {
    use crate::exporters::parquet_exporter::PARQUET_EXPORTER;
    let raw = check_basis(
        PARQUET_EXPORTER.resolve_config,
        PARQUET_EXPORTER.normalize_config_for_edit.unwrap(),
        json!({
            "storage": {"file": {"base_uri": "/tmp/config-edit-parquet"}},
            "partitioning_strategies": [{"schema_metadata": ["tenant"]}]
        }),
    );
    assert_eq!(
        raw["partitioning_strategies"][0]["SchemaMetadata"],
        json!(["tenant"])
    );
}

/// Scenario: Linux receiver sources use legacy host-root and journald priority shorthand.
/// Guarantees: Canonical fields can be edited or removed without retaining conflicting source fields.
#[cfg(target_os = "linux")]
#[test]
fn receiver_edit_bases_normalize_legacy_layouts() {
    use crate::receivers::{
        host_metrics_receiver::HOST_METRICS_RECEIVER, journald_receiver::JOURNALD_RECEIVER,
    };
    let mut host = check_basis(
        HOST_METRICS_RECEIVER.resolve_config,
        HOST_METRICS_RECEIVER.normalize_config_for_edit.unwrap(),
        json!({"root_path": "/host"}),
    );
    assert!(host.get("root_path").is_none());
    host["host_view"]["root_path"] = "/other".into();
    assert!((HOST_METRICS_RECEIVER.resolve_config)(&host).is_ok());
    let mut journal = check_basis(
        JOURNALD_RECEIVER.resolve_config,
        JOURNALD_RECEIVER.normalize_config_for_edit.unwrap(),
        json!({"max_priority": "warning"}),
    );
    assert!(journal.get("max_priority").is_none());
    assert_eq!(journal["priorities"], json!([0, 1, 2, 3, 4]));
    journal["priorities"] = json!([0, 1]);
    assert!((JOURNALD_RECEIVER.resolve_config)(&journal).is_ok());
    _ = journal.as_object_mut().unwrap().remove("priorities");
    assert!((JOURNALD_RECEIVER.resolve_config)(&journal).is_ok());
}
