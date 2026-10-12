// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tests that every registered pipeline component declares resolution metadata.
//!
//! The test iterates over all factories registered in `OTAP_PIPELINE_FACTORY`
//! so a newly linked factory cannot escape the explicit snapshot-policy audit.

use otel_arrow_dfe_engine::component_config::ConfigSnapshotPolicy;
use otel_arrow_dfe_otap::OTAP_PIPELINE_FACTORY;

// Keep these side-effect imports so their linkme registrations are visible.
use otel_arrow_dfe_contrib_nodes as _;
use otel_arrow_dfe_core_nodes as _;

fn assert_declared(name: &str, resolver: usize, policy: ConfigSnapshotPolicy) {
    assert_ne!(resolver, 0, "factory `{name}` has no resolver");
    match policy {
        ConfigSnapshotPolicy::TypedSafe
        | ConfigSnapshotPolicy::CustomSafe
        | ConfigSnapshotPolicy::Omit => {}
    }
}

/// Scenario: All linked receiver, processor, exporter, and extension factories are inspected.
/// Guarantees: Every registration exposes a resolver and one explicit snapshot policy.
#[test]
fn all_linked_factories_declare_resolution_metadata() {
    let receivers = OTAP_PIPELINE_FACTORY.get_receiver_factory_map();
    let processors = OTAP_PIPELINE_FACTORY.get_processor_factory_map();
    let exporters = OTAP_PIPELINE_FACTORY.get_exporter_factory_map();
    let extensions = OTAP_PIPELINE_FACTORY.get_extension_factory_map();
    assert!(!receivers.is_empty(), "no receiver factories registered");
    assert!(!processors.is_empty(), "no processor factories registered");
    assert!(!exporters.is_empty(), "no exporter factories registered");

    for (name, factory) in receivers {
        assert_declared(
            name,
            factory.resolve_config as usize,
            factory.snapshot_policy,
        );
    }
    for (name, factory) in processors {
        assert_declared(
            name,
            factory.resolve_config as usize,
            factory.snapshot_policy,
        );
    }
    for (name, factory) in exporters {
        assert_declared(
            name,
            factory.resolve_config as usize,
            factory.snapshot_policy,
        );
    }
    for (name, factory) in extensions {
        assert_declared(
            name,
            factory.resolve_config as usize,
            factory.snapshot_policy,
        );
    }
}
