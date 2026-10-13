// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::tests::{
    TEST_OBSERVABILITY_EXPORTERS, TEST_OBSERVABILITY_PROCESSORS,
    create_test_observability_receiver, run_console_reporting_child,
};
use otel_arrow_dfe_config::secret::RedactedString;
use otel_arrow_dfe_engine::component_config::{ResolvedComponentConfig, ResolvedNodeConfig};
use otel_arrow_dfe_engine::config::{ExtensionConfig, ReceiverConfig};
use otel_arrow_dfe_engine::context::ExtensionContext;
use otel_arrow_dfe_engine::extension::wrapper::{ExtensionBundle, ExtensionWrapper};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_engine::wiring_contract::WiringContract;
use otel_arrow_dfe_engine::{ExtensionFactory, ReceiverFactory};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

const RECEIVER: &str = "urn:test:receiver:resolved";
const EXTENSION: &str = "urn:test:extension:resolved";
const SECRET: &str = "configuration-resolution-private-token";
static RESOLUTIONS: AtomicUsize = AtomicUsize::new(0);
static CREATIONS: AtomicUsize = AtomicUsize::new(0);
static SHARED_CONFIG: Mutex<Option<Arc<TestConfig>>> = Mutex::new(None);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct TestConfig {
    token: RedactedString,
    #[serde(default = "default_retry", alias = "retry_count")]
    retry: usize,
    #[serde(default)]
    fail_creation: bool,
}

const fn default_retry() -> usize {
    3
}

fn resolve(
    raw: &serde_json::Value,
) -> Result<ResolvedComponentConfig, otel_arrow_dfe_config::error::Error> {
    let _ = RESOLUTIONS.fetch_add(1, Ordering::SeqCst);
    otel_arrow_dfe_engine::component_config::resolve_typed_config::<TestConfig>(raw)
}

fn resolve_omitted(
    raw: &serde_json::Value,
) -> Result<ResolvedComponentConfig, otel_arrow_dfe_config::error::Error> {
    let _ = RESOLUTIONS.fetch_add(1, Ordering::SeqCst);
    otel_arrow_dfe_engine::component_config::resolve_omitted_config::<TestConfig>(raw)
}

fn create_receiver(
    context: PipelineContext,
    node: NodeId,
    config: Arc<ResolvedNodeConfig>,
    runtime: &ReceiverConfig,
    capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities,
) -> Result<ReceiverWrapper<()>, otel_arrow_dfe_config::error::Error> {
    let typed = config.component_config::<TestConfig>()?;
    if typed.fail_creation {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: "intentional candidate creation failure".to_owned(),
        });
    }
    assert_eq!(typed.token.expose(), SECRET);
    assert_eq!(typed.retry, 3);
    let mut shared = SHARED_CONFIG.lock().unwrap();
    if let Some(first) = shared.as_ref() {
        assert!(Arc::ptr_eq(first, &typed));
    }
    *shared = Some(typed);
    let _ = CREATIONS.fetch_add(1, Ordering::SeqCst);
    drop(shared);
    create_test_observability_receiver(context, node, config, runtime, capabilities)
}

fn create_extension(
    _context: &ExtensionContext,
    id: ExtensionId,
    config: Arc<ResolvedExtensionConfig>,
    runtime: &ExtensionConfig,
) -> Result<ExtensionBundle, otel_arrow_dfe_config::error::Error> {
    let typed = config.component_config::<TestConfig>()?;
    assert_eq!(typed.token.expose(), SECRET);
    ExtensionWrapper::builder(id, config.effective(), runtime)
        .passive()
        .cloned()
        .shared(typed)
        .build()
        .map_err(
            |error| otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error: error.to_string(),
            },
        )
}

fn factory() -> &'static PipelineFactory<()> {
    Box::leak(Box::new(PipelineFactory::new(
        Box::leak(Box::new([
            ReceiverFactory {
                name: RECEIVER,
                create: create_receiver,
                context_declarations: None,
                wiring_contract: WiringContract::UNRESTRICTED,
                resolve_config: resolve,
                snapshot_policy: ConfigSnapshotPolicy::TypedSafe,
            },
            ReceiverFactory {
                name: "urn:otel:receiver:internal_telemetry",
                create: create_test_observability_receiver,
                context_declarations: None,
                wiring_contract: WiringContract::UNRESTRICTED,
                resolve_config: otel_arrow_dfe_engine::component_config::resolve_no_config,
                snapshot_policy: ConfigSnapshotPolicy::TypedSafe,
            },
        ])),
        TEST_OBSERVABILITY_PROCESSORS,
        TEST_OBSERVABILITY_EXPORTERS,
        Box::leak(Box::new([ExtensionFactory {
            name: EXTENSION,
            description: "Resolution test extension",
            documentation_url: "",
            capabilities: None,
            create: create_extension,
            resolve_config: resolve_omitted,
            snapshot_policy: ConfigSnapshotPolicy::Omit,
        }])),
    )))
}

fn source(address: SocketAddr) -> OtelDataflowSpec {
    OtelDataflowSpec::from_json(&serde_json::json!({
        "version": "otel_dataflow/v1",
        "engine": {
            "http_admin": {"bind_address": address.to_string()},
            "custom": {"private": {"token": SECRET}},
            "telemetry": {"logs": {"providers": {"global": "noop", "engine": "noop", "admin": "noop", "internal": "noop"}}},
            "controller": {"extensions": {"observe": {"type": EXTENSION, "config": {"token": SECRET}}}}
        },
        "groups": {"g1": {"pipelines": {"p1": {
            "policies": {"resources": {"core_allocation": {"type": "core_count", "count": 2}}},
            "nodes": {"receiver": {"type": RECEIVER, "config": {"token": SECRET}},
                "exporter": {"type": "urn:test:exporter:example"}},
            "extensions": {"private": {"type": EXTENSION, "config": {"token": SECRET}}},
            "connections": [{"from": "receiver", "to": "exporter"}]
        }}}}
    }).to_string()).unwrap()
}

fn get_json(address: SocketAddr, path: &str) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stream = loop {
        match TcpStream::connect(address) {
            Ok(stream) => break stream,
            Err(error) if Instant::now() >= deadline => panic!("admin startup failed: {error}"),
            Err(_) => thread::sleep(Duration::from_millis(10)),
        }
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response).unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(
        headers.starts_with("HTTP/1.0 200") || headers.starts_with("HTTP/1.1 200"),
        "{headers}"
    );
    assert!(!body.contains(SECRET));
    serde_json::from_str(body).unwrap()
}

/// Scenario: An observability node names a rate limiter absent from its effective policy catalog.
/// Guarantees: Preparation rejects the complete candidate before starting any runtime services.
#[test]
fn prepare_rejects_invalid_observability_limiter_bindings() {
    let mut value = serde_json::to_value(source("127.0.0.1:0".parse().unwrap())).unwrap();
    value["engine"]["controller"]["extensions"] = serde_json::json!({});
    value["engine"]["observability"]["pipeline"]["nodes"]["internal_telemetry"]["rate_limiters"] =
        serde_json::json!(["missing"]);
    let source = serde_json::from_value(value).unwrap();
    let error = Controller::new(factory())
        .prepare(source, ControllerRunOptions::default())
        .unwrap_err()
        .to_string();
    assert!(error.contains("rate limiter binding"), "{error}");
}

/// Scenario: One submitted candidate starts two cores and is reported through real Admin and OpAMP paths.
/// Guarantees: Nodes and extensions resolve once, cores share typed values, and reporting never discloses source secrets.
#[test]
fn startup_and_reporting_reuse_one_resolved_candidate() {
    if std::env::var_os("OTAP_CONSOLE_REPORTING_CHILD").is_none() {
        run_console_reporting_child(
            "configuration_resolution_tests::startup_and_reporting_reuse_one_resolved_candidate",
        );
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let (sender, contexts) = std::sync::mpsc::sync_channel(1);
    let mut registry = ControllerExtensionRegistry::empty();
    registry.register(
        EXTENSION.into(),
        move |context| {
            assert_eq!(
                context
                    .extension
                    .component_config::<TestConfig>()?
                    .token
                    .expose(),
                SECRET
            );
            sender.send(context).unwrap();
            Ok(Box::new(|cancellation| {
                Box::pin(async move {
                    cancellation.cancelled().await;
                    Ok(())
                })
            }))
        },
        resolve_omitted,
        ConfigSnapshotPolicy::Omit,
    );
    let factory = factory();
    let mut source = source(address);
    source.engine.observability.pipeline.extensions = serde_json::from_value(serde_json::json!({
        "private": {"type": EXTENSION, "config": {"token": SECRET}}
    }))
    .unwrap();
    let replay_registry = registry.clone();
    let submitted = source.clone();
    let prepared = Controller::new(factory)
        .prepare(
            source,
            ControllerRunOptions {
                extensions: registry,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(RESOLUTIONS.load(Ordering::SeqCst), 4);
    let expected = serde_json::to_value(prepared.effective_config()).unwrap();
    assert_eq!(
        expected["engine"]["observability"]["pipeline"]["extensions"]["private"]["config"],
        OMITTED_VALUE
    );
    assert_eq!(
        expected["engine"]["controller"]["extensions"]["observe"]["config"],
        OMITTED_VALUE
    );
    assert_eq!(
        expected["groups"]["g1"]["pipelines"]["p1"]["nodes"]["receiver"]["config"]["retry"],
        3
    );
    assert!(!expected.to_string().contains(SECRET));
    let controller = thread::spawn(move || {
        prepared
            .run_till_shutdown()
            .map_err(|error| error.to_string())
    });
    let context = contexts.recv_timeout(Duration::from_secs(5)).unwrap();
    for _ in 0..3 {
        assert_eq!(get_json(address, "/api/v1/config"), expected);
        assert_eq!(
            get_json(address, "/api/v1/groups/g1"),
            expected["groups"]["g1"]
        );
        assert_eq!(
            get_json(address, "/api/v1/groups/g1/pipelines/p1")["pipeline"],
            expected["groups"]["g1"]["pipelines"]["p1"]
        );
        let report = extension::opamp::effective_config(&context);
        let body = &report.config_map.unwrap().config_map["state"].body;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(body).unwrap(),
            expected
        );
    }
    assert_eq!(RESOLUTIONS.load(Ordering::SeqCst), 4);
    let deadline = Instant::now() + Duration::from_secs(5);
    while CREATIONS.load(Ordering::SeqCst) < 2 {
        assert!(
            Instant::now() < deadline,
            "both cores must create the resolved receiver"
        );
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(CREATIONS.load(Ordering::SeqCst), 2);
    let mut equivalent = serde_json::to_value(&submitted).unwrap();
    equivalent["groups"]["g1"]["pipelines"]["p1"]["nodes"]["receiver"]["config"]["retry_count"] =
        3.into();
    let status = context
        .control_plane
        .reconcile_engine_config(otel_arrow_dfe_admin::EngineConfigReconcileRequest {
            config: serde_json::from_value(equivalent.clone()).unwrap(),
            step_timeout_secs: 2,
            drain_timeout_secs: 2,
            delete_timeout_secs: 2,
            delete_missing: true,
        })
        .unwrap();
    assert_eq!(
        status.state,
        otel_arrow_dfe_admin::EngineConfigReconcileState::Succeeded
    );
    assert!(
        status
            .changes
            .iter()
            .all(|change| change.action == otel_arrow_dfe_admin::ConfigChangeAction::Noop)
    );
    assert_eq!(RESOLUTIONS.load(Ordering::SeqCst), 8);
    assert_eq!(CREATIONS.load(Ordering::SeqCst), 2);
    equivalent["groups"]["g1"]["pipelines"]["p1"]["nodes"]["receiver"]["config"]["fail_creation"] =
        true.into();
    equivalent["groups"]["g1"]["pipelines"]["p1"]["nodes"]["receiver"]["config"]["token"] =
        "failed-private-candidate".into();
    let status = context
        .control_plane
        .reconcile_engine_config(otel_arrow_dfe_admin::EngineConfigReconcileRequest {
            config: serde_json::from_value(equivalent).unwrap(),
            step_timeout_secs: 2,
            drain_timeout_secs: 2,
            delete_timeout_secs: 2,
            delete_missing: true,
        })
        .unwrap();
    assert_eq!(
        status.state,
        otel_arrow_dfe_admin::EngineConfigReconcileState::Failed
    );
    assert_eq!(RESOLUTIONS.load(Ordering::SeqCst), 12);
    assert_eq!(get_json(address, "/api/v1/config"), expected);
    let report = extension::opamp::effective_config(&context);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &report.config_map.unwrap().config_map["state"].body
        )
        .unwrap(),
        expected
    );
    assert_eq!(RESOLUTIONS.load(Ordering::SeqCst), 12);
    context.control_plane.shutdown_all(5).unwrap();
    controller.join().unwrap().unwrap();
    let replay: OtelDataflowSpec = serde_json::from_value(expected).unwrap();
    assert!(
        Controller::new(factory)
            .prepare(
                replay,
                ControllerRunOptions {
                    extensions: replay_registry,
                    ..Default::default()
                }
            )
            .is_err()
    );
    std::process::exit(0);
}
