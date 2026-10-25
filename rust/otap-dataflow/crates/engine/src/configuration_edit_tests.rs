// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use serde_json::json;

static FACTORY: PipelineFactory<()> = PipelineFactory::new(
    &[
        ReceiverFactory {
            name: "urn:otel:receiver:edit_basis",
            create: |_, _, _, _, _| unreachable!("editing must not create runtime state"),
            context_declarations: None,
            wiring_contract: wiring_contract::WiringContract::UNRESTRICTED,
            resolve_config: |_| unreachable!("editing must not repeat resolution"),
            snapshot_policy: ConfigSnapshotPolicy::Omit,
            normalize_config_for_edit: Some(|raw| {
                component_config::normalize_alias(raw, "legacy", "canonical")
            }),
        },
        ReceiverFactory {
            name: "urn:otel:receiver:identity",
            create: |_, _, _, _, _| unreachable!("editing must not create runtime state"),
            context_declarations: None,
            wiring_contract: wiring_contract::WiringContract::UNRESTRICTED,
            resolve_config: |_| unreachable!("editing must not repeat resolution"),
            snapshot_policy: ConfigSnapshotPolicy::Omit,
            normalize_config_for_edit: None,
        },
    ],
    &[],
    &[],
    &[ExtensionFactory {
        name: "urn:otel:extension:edit_basis",
        description: "Edit basis test",
        documentation_url: "",
        capabilities: None,
        create: |_, _, _, _| unreachable!("editing must not create extensions"),
        resolve_config: |_| unreachable!("editing must not resolve extensions"),
        snapshot_policy: ConfigSnapshotPolicy::Omit,
        normalize_config_for_edit: Some(|raw| {
            component_config::normalize_alias(raw, "legacy", "canonical")
        }),
    }],
);

fn path(scope: &str) -> Vec<String> {
    [scope, "changed", "config", "canonical"]
        .map(str::to_owned)
        .to_vec()
}

/// Scenario: Existing or newly added components have legacy fields and private raw values.
/// Guarantees: Only touched edit bases normalize, replacements stay untouched, and no resolver or runtime runs.
#[test]
fn edit_normalization_uses_current_candidate_type_and_preserves_private_source() {
    for scope in ["nodes", "extensions"] {
        let urn = if scope == "nodes" {
            "receiver:edit_basis"
        } else {
            "extension:edit_basis"
        };
        let component = json!({"type": urn, "config": {"legacy": 7, "secret": "private-value"}});
        let mut pipeline = json!({(scope): {"changed": component, "untouched": component}});
        let before = pipeline.clone();
        FACTORY
            .normalize_source_for_edit(&mut pipeline, &path(scope)[..3])
            .unwrap();
        assert_eq!(pipeline, before);
        FACTORY
            .normalize_source_for_edit(&mut pipeline, &path(scope))
            .unwrap();
        assert_eq!(
            pipeline[scope]["changed"]["config"],
            json!({"canonical": 7, "secret": "private-value"})
        );
        assert_eq!(pipeline[scope]["untouched"], before[scope]["untouched"]);
        let once = pipeline.clone();
        FACTORY
            .normalize_source_for_edit(&mut pipeline, &path(scope))
            .unwrap();
        assert_eq!(pipeline, once);
        if scope == "nodes" {
            pipeline[scope]["changed"] =
                json!({"type": "receiver:identity", "config": {"legacy": 9}});
            FACTORY
                .normalize_source_for_edit(&mut pipeline, &path(scope))
                .unwrap();
            assert_eq!(pipeline[scope]["changed"]["config"], json!({"legacy": 9}));
        }
    }
}

/// Scenario: Management clients edit an authored alias instead of its canonical snapshot name.
/// Guarantees: Node and extension edit bases keep the addressed source key and all private values.
#[test]
fn edit_normalization_preserves_authored_alias_paths() {
    for scope in ["nodes", "extensions"] {
        let urn = if scope == "nodes" {
            "receiver:edit_basis"
        } else {
            "extension:edit_basis"
        };
        let mut pipeline = json!({(scope): {"changed": {
            "type": urn, "config": {"legacy": {"nested": 7}, "secret": "private-value"}
        }}});
        let before = pipeline.clone();
        for field in ["nested", "new_field"] {
            let mut path = path(scope);
            path[3] = "legacy".into();
            path.push(field.into());
            FACTORY
                .normalize_source_for_edit(&mut pipeline, &path)
                .unwrap();
            assert_eq!(pipeline, before);
        }
    }
}

/// Scenario: A source edit basis contains both an alias and its canonical key.
/// Guarantees: Normalization rejects ambiguity without dropping private values or choosing a winner.
#[test]
fn edit_normalization_rejects_conflicting_aliases() {
    let mut raw = json!({"legacy": "private-one", "canonical": "private-two"});
    let before = raw.clone();
    let error = component_config::normalize_alias(&mut raw, "legacy", "canonical").unwrap_err();
    assert_eq!(raw, before);
    assert!(!format!("{error:?}").contains("private-"));
}
