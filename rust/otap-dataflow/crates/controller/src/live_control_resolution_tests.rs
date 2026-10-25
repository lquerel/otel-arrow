// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use otel_arrow_dfe_config::secret::RedactedString;
use otel_arrow_dfe_engine::component_config::resolve_typed_config;
use serde::Serialize;

const TYPED_RECEIVER: &str = "urn:test:receiver:semantic";
const TYPED_CONTROLLER: &str = "urn:test:extension:semantic";

#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct SemanticConfig {
    token: RedactedString,
    #[serde(default = "default_attempts", alias = "retry_count")]
    attempts: usize,
    #[serde(default)]
    fail_creation: bool,
}

const fn default_attempts() -> usize {
    3
}

fn create_semantic_receiver(
    context: PipelineContext,
    node: otel_arrow_dfe_engine::node::NodeId,
    config: Arc<ResolvedNodeConfig>,
    receiver: &ReceiverConfig,
    capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities,
) -> Result<ReceiverWrapper<()>, otel_arrow_dfe_config::error::Error> {
    if config.component_config::<SemanticConfig>()?.fail_creation {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: format!(
                "intentional creation failure with {}",
                config.component_config::<SemanticConfig>()?.token.expose()
            ),
        });
    }
    recovery_test_receiver_create(context, node, config, receiver, capabilities)
}

static FACTORY: PipelineFactory<()> = PipelineFactory::new(
    &[
        ReceiverFactory {
            name: TYPED_RECEIVER,
            create: create_semantic_receiver,
            context_declarations: None,
            wiring_contract: WiringContract::UNRESTRICTED,
            resolve_config: resolve_typed_config::<SemanticConfig>,
            snapshot_policy: ConfigSnapshotPolicy::TypedSafe,
        },
        ReceiverFactory {
            name: "urn:otel:receiver:internal_telemetry",
            create: test_receiver_create,
            context_declarations: None,
            wiring_contract: WiringContract::UNRESTRICTED,
            resolve_config: test_resolve_config,
            snapshot_policy: ConfigSnapshotPolicy::Omit,
        },
    ],
    TEST_PROCESSOR_FACTORIES,
    RECOVERY_TEST_EXPORTER_FACTORIES,
    &[],
);

fn source() -> OtelDataflowSpec {
    engine_config_with_pipeline(
        r#"
        policies:
          resources:
            core_allocation: {type: core_count, count: 1}
        nodes:
          receiver:
            type: urn:test:receiver:semantic
            config: {token: first-secret}
          exporter:
            type: urn:test:exporter:example
        connections:
          - {from: receiver, to: exporter}
"#,
    )
}

fn pipeline_with(config: &OtelDataflowSpec, raw: serde_json::Value) -> PipelineConfig {
    let mut value = serde_json::to_value(
        &config.groups[&PipelineGroupId::from("g1")].pipelines[&PipelineId::from("p1")],
    )
    .unwrap();
    value["nodes"]["receiver"]["config"] = raw;
    serde_json::from_value(value).unwrap()
}

fn request(pipeline: PipelineConfig) -> ReconfigureRequest {
    ReconfigureRequest {
        pipeline,
        step_timeout_secs: 2,
        drain_timeout_secs: 2,
    }
}

fn committed(
    runtime: &ControllerRuntime<()>,
) -> (
    OtelDataflowSpec,
    SafeConfigSnapshot,
    HashMap<PipelineKey, otel_arrow_dfe_engine::component_config::ResolvedPipelineConfig>,
) {
    let state = runtime.state.lock().unwrap();
    (
        state.live_config.clone(),
        state.safe_config_snapshot.clone(),
        state.resolved_pipelines.clone(),
    )
}

fn assert_management_reports_committed_source(runtime: &Arc<ControllerRuntime<()>>) {
    let (source, _, _) = committed(runtime);
    let control_plane = runtime.control_plane();
    assert_eq!(control_plane.engine_config_snapshot().unwrap(), source);
    for (group_id, group) in &source.groups {
        assert_eq!(
            control_plane
                .group_details(group_id.as_ref())
                .unwrap()
                .as_ref(),
            Some(group)
        );
        for (pipeline_id, pipeline) in &group.pipelines {
            assert_eq!(
                &control_plane
                    .pipeline_details(group_id.as_ref(), pipeline_id.as_ref())
                    .unwrap()
                    .unwrap()
                    .pipeline,
                pipeline
            );
        }
    }
}

fn wait_for_terminal(runtime: &ControllerRuntime<()>, rollout_id: &str) -> RolloutStatus {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status = runtime.rollout_status_snapshot(rollout_id).unwrap();
        if matches!(
            status.state,
            ApiPipelineRolloutState::Succeeded
                | ApiPipelineRolloutState::Failed
                | ApiPipelineRolloutState::RollbackFailed
        ) {
            return status;
        }
        assert!(Instant::now() < deadline, "rollout must settle: {status:?}");
        thread::sleep(Duration::from_millis(10));
    }
}

/// Scenario: A captured worker panic contains an arbitrary inline credential in its payload.
/// Guarantees: Debug, rollout summaries, and observed-state diagnostic sources retain no payload values.
#[test]
fn panic_reports_do_not_retain_private_payload_values() {
    let report = PanicReport::capture(
        "rollout worker",
        Box::new("private-panic-credential".to_owned()),
        Some("public-worker".to_owned()),
        Some(1),
        Some(0),
    );
    for output in [
        format!("{report:?}"),
        report.summary_message(),
        report.detail_message(),
        serde_json::to_string(&report.error_summary()).unwrap(),
    ] {
        assert!(!output.contains("private-panic-credential"));
    }
    assert_eq!(report.summary_message(), "rollout worker panicked");
    assert!(
        report
            .detail_message()
            .contains("thread_name=public-worker")
    );
    assert!(report.detail_message().contains("backtrace:"));
}

fn assert_limiter_binding_rejected(bindings: serde_json::Value, expected: &str) {
    for existing in [false, true] {
        let mut config = source();
        let mut pipeline = serde_json::to_value(
            &config.groups[&PipelineGroupId::from("g1")].pipelines[&PipelineId::from("p1")],
        )
        .unwrap();
        pipeline["nodes"]["receiver"]["rate_limiters"] = bindings.clone();
        if !existing {
            config
                .groups
                .get_mut(&PipelineGroupId::from("g1"))
                .unwrap()
                .pipelines
                .clear();
        }
        let runtime = test_runtime_with_factory(&config, &FACTORY);
        if existing {
            register_existing_pipeline(&runtime, &config);
        }
        let _messages = existing.then(|| {
            register_runtime_instance(&runtime, "g1", "p1", 0, 0, RuntimeInstanceLifecycle::Active)
        });
        let before = committed(&runtime);
        let (revision, next_thread_id, generations, instances) = {
            let state = runtime.state.lock().unwrap();
            (
                state.config_revision,
                state.next_thread_id,
                state.generation_counters.clone(),
                state
                    .runtime_instances
                    .keys()
                    .cloned()
                    .collect::<HashSet<_>>(),
            )
        };
        let error = runtime
            .control_plane()
            .reconfigure_pipeline(
                "g1",
                "p1",
                request(serde_json::from_value(pipeline).unwrap()),
            )
            .unwrap_err();
        assert!(
            matches!(error, ControlPlaneError::InvalidRequest { ref message }
            if message.contains(expected)),
            "{error:?}"
        );
        assert_eq!(committed(&runtime), before);
        let state = runtime.state.lock().unwrap();
        assert_eq!(state.config_revision, revision);
        assert_eq!(state.next_thread_id, next_thread_id);
        assert_eq!(state.generation_counters, generations);
        assert_eq!(
            state
                .runtime_instances
                .keys()
                .cloned()
                .collect::<HashSet<_>>(),
            instances
        );
        assert!(state.rollouts.is_empty());
        assert!(state.active_rollouts.is_empty());
        assert!(state.pipeline_operation_reservations.is_empty());
    }
}

/// Scenario: A single-pipeline create or replacement request binds a receiver to an unknown limiter.
/// Guarantees: Admission rejects it without launching cores or changing any committed representation.
#[test]
fn single_pipeline_admission_rejects_unknown_limiter_bindings() {
    assert_limiter_binding_rejected(
        serde_json::json!(["missing"]),
        "does not name an effective limiter",
    );
}

/// Scenario: A single-pipeline create or replacement request specifies multiple node limiter bindings.
/// Guarantees: Cardinality validation rejects it before rollout, creation, or committed-state changes.
#[test]
fn single_pipeline_admission_rejects_multiple_limiter_bindings() {
    assert_limiter_binding_rejected(
        serde_json::json!(["first", "second"]),
        "at most one rate limiter binding",
    );
}

/// Scenario: Alias/default-equivalent input and secret-only changes are admitted for an active pipeline.
/// Guarantees: Equivalent input commits as a no-op; a hidden credential change requires replacement.
#[test]
fn typed_reconciliation_preserves_noops_and_detects_secret_changes() {
    let config = source();
    let runtime = test_runtime_with_factory(&config, &FACTORY);
    register_existing_pipeline(&runtime, &config);
    let _messages =
        register_runtime_instance(&runtime, "g1", "p1", 0, 0, RuntimeInstanceLifecycle::Active);
    let original_safe_config_snapshot = runtime.safe_config_snapshot();
    let alias = pipeline_with(
        &config,
        serde_json::json!({"token": "first-secret", "retry_count": 3}),
    );
    let plan = runtime
        .prepare_rollout_plan("g1", "p1", &request(alias.clone()))
        .unwrap();
    assert_eq!(plan.action, RolloutAction::NoOp);
    let status = runtime.spawn_rollout(plan).unwrap();
    assert_eq!(status.state, ApiPipelineRolloutState::Succeeded);
    let (source, safe_config_snapshot, resolved) = committed(&runtime);
    let key = PipelineKey::new("g1".into(), "p1".into());
    assert_eq!(
        source.groups[&PipelineGroupId::from("g1")].pipelines[&PipelineId::from("p1")],
        alias
    );
    assert_eq!(safe_config_snapshot, original_safe_config_snapshot);
    assert_management_reports_committed_source(&runtime);
    assert_eq!(
        resolved[&key].safe_config_snapshot(),
        &safe_config_snapshot.groups[&PipelineGroupId::from("g1")].pipelines
            [&PipelineId::from("p1")]
    );
    let changed = pipeline_with(&config, serde_json::json!({"token": "second-secret"}));
    let plan = runtime
        .prepare_rollout_plan("g1", "p1", &request(changed))
        .unwrap();
    assert_eq!(plan.action, RolloutAction::Replace);
    assert_eq!(
        plan.target_deployment
            .runtime_resolved
            .safe_config_snapshot(),
        resolved[&key].safe_config_snapshot()
    );
    assert_ne!(plan.target_deployment.runtime_resolved, resolved[&key]);
}

/// Scenario: A candidate fails static admission or rolls back before replacing the old generation.
/// Guarantees: Source, safe snapshot, and resolved state all retain the previous committed values.
#[test]
fn failed_admission_and_rollback_preserve_all_representations() {
    let config = source();
    let runtime = test_runtime_with_factory(&config, &FACTORY);
    let _runner = ObservedStateRunner::start(&runtime);
    register_existing_pipeline(&runtime, &config);
    let _messages =
        register_runtime_instance(&runtime, "g1", "p1", 0, 0, RuntimeInstanceLifecycle::Active);
    let before = committed(&runtime);
    let invalid = pipeline_with(&config, serde_json::json!({"token": "[REDACTED]"}));
    assert!(
        runtime
            .prepare_rollout_plan("g1", "p1", &request(invalid))
            .is_err()
    );
    assert_eq!(committed(&runtime), before);
    assert_management_reports_committed_source(&runtime);
    let replacement = pipeline_with(
        &config,
        serde_json::json!({"token": "second-secret", "fail_creation": true}),
    );
    let plan = runtime
        .prepare_rollout_plan("g1", "p1", &request(replacement))
        .unwrap();
    assert_eq!(plan.action, RolloutAction::Replace);
    let status = runtime.spawn_rollout(plan).unwrap();
    let status = wait_for_terminal(&runtime, &status.rollout_id);
    assert_eq!(status.state, ApiPipelineRolloutState::Failed, "{status:?}");
    assert!(
        !status
            .failure_reason
            .as_ref()
            .unwrap()
            .contains("timed out"),
        "creation must fail before the readiness deadline: {status:?}"
    );
    assert_eq!(committed(&runtime), before);
    assert_management_reports_committed_source(&runtime);
}

/// Scenario: A newly admitted pipeline is launched successfully and committed by the rollout worker.
/// Guarantees: All three representations publish the same generation after runtime readiness.
#[test]
fn successful_rollout_commits_all_representations() {
    let mut config = source();
    config
        .groups
        .get_mut(&PipelineGroupId::from("g1"))
        .unwrap()
        .pipelines
        .clear();
    let runtime = test_runtime_with_factory(&config, &FACTORY);
    let _runner = ObservedStateRunner::start(&runtime);
    let desired = source();
    let pipeline = pipeline_with(&desired, serde_json::json!({"token": "new-secret"}));
    let status = runtime
        .request_reconfigure_pipeline("g1", "p1", &request(pipeline.clone()))
        .unwrap();
    let status = wait_for_terminal(&runtime, &status.rollout_id);
    assert_eq!(
        status.state,
        ApiPipelineRolloutState::Succeeded,
        "{status:?}"
    );
    let (source, safe_config_snapshot, resolved) = committed(&runtime);
    let key = PipelineKey::new("g1".into(), "p1".into());
    assert_eq!(
        source.groups[&PipelineGroupId::from("g1")].pipelines[&PipelineId::from("p1")],
        pipeline
    );
    assert_eq!(
        resolved[&key].safe_config_snapshot(),
        &safe_config_snapshot.groups[&PipelineGroupId::from("g1")].pipelines
            [&PipelineId::from("p1")]
    );
    assert_eq!(
        resolved[&key]
            .node(&"receiver".into())
            .unwrap()
            .component_config::<SemanticConfig>()
            .unwrap()
            .token
            .expose(),
        "new-secret"
    );
    assert!(
        !serde_json::to_string(&safe_config_snapshot)
            .unwrap()
            .contains("new-secret")
    );
    assert_management_reports_committed_source(&runtime);
    runtime.request_shutdown_all(2).unwrap();
}

/// Scenario: Full reconciliation changes an immutable controller extension or observability pipeline.
/// Guarantees: Unsupported changes fail before rollout while default-equivalent settings remain no-ops.
#[test]
fn engine_scoped_components_require_restart_before_snapshot_commit() {
    let mut config = source();
    config.engine.controller.extensions = serde_json::from_value(serde_json::json!({
        "private": {"type": TYPED_CONTROLLER, "config": {"token": "controller-secret"}}
    }))
    .unwrap();
    let mut extensions = ControllerExtensionRegistry::empty();
    extensions.register(
        TYPED_CONTROLLER.into(),
        |_| panic!("planning must not start extensions"),
        resolve_typed_config::<SemanticConfig>,
        ConfigSnapshotPolicy::TypedSafe,
    );
    let (filter, handle) = RuntimeLogFilter::new(config.engine.telemetry.logs.level.as_ref());
    let runtime = test_runtime_with_supplied_log_filter_and_topology(
        &config,
        &FACTORY,
        NumaTopology::unknown(),
        filter,
        handle,
        extensions,
    )
    .0;
    register_existing_pipeline(&runtime, &config);
    let _messages =
        register_runtime_instance(&runtime, "g1", "p1", 0, 0, RuntimeInstanceLifecycle::Active);
    let before = committed(&runtime);
    for change in [
        "controller",
        "observability",
        "remove_controller",
        "add_controller",
    ] {
        let mut desired = serde_json::to_value(&config).unwrap();
        match change {
            "controller" => {
                desired["engine"]["controller"]["extensions"]["private"]["config"]["token"] =
                    "replacement-secret".into()
            }
            "remove_controller" => {
                desired["engine"]["controller"]["extensions"] = serde_json::json!({})
            }
            "add_controller" => {
                desired["engine"]["controller"]["extensions"]["second"] =
                    desired["engine"]["controller"]["extensions"]["private"].clone()
            }
            _ => {
                desired["engine"]["observability"]["pipeline"]["nodes"]["logs_console"]["config"] =
                    serde_json::json!({"changed": true})
            }
        }
        let desired = serde_json::from_value(desired).unwrap();
        let error = runtime
            .reconcile_engine_config(reconcile_request(desired, true))
            .unwrap_err();
        assert!(
            matches!(error, ControlPlaneError::InvalidRequest { message } if message.contains("restart"))
        );
        assert_eq!(committed(&runtime), before);
    }
    let mut equivalent = serde_json::to_value(&config).unwrap();
    equivalent["engine"]["controller"]["extensions"]["private"]["config"]["retry_count"] = 3.into();
    // Explicit empty observability policies have the same inherited runtime defaults.
    equivalent["engine"]["observability"]["pipeline"]["policies"] = serde_json::json!({});
    let status = runtime
        .reconcile_engine_config(reconcile_request(
            serde_json::from_value(equivalent).unwrap(),
            true,
        ))
        .unwrap();
    assert_eq!(status.state, EngineConfigReconcileState::Succeeded);
    assert!(
        status
            .changes
            .iter()
            .all(|change| change.action == ConfigChangeAction::Noop)
    );
    assert_eq!(
        committed(&runtime).1.engine.controller.extensions,
        before.1.engine.controller.extensions
    );
}
