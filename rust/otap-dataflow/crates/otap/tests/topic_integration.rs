// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#![allow(missing_docs)]

//! Integration smoke test for in-memory topics (balanced mode).
//!
//! Covers:
//! - Topic registry construction and wiring through ControllerContext
//! - OTAP topic exporter -> receiver end-to-end data flow
//! - Balanced subscription path (single consumer)
//!
//! Does NOT cover:
//! - Load balancing with multiple consumers in the same group
//! - Broadcast mode or mixed subscription modes
//! - Topic scope resolution (global vs group vs pipeline)
//! - Overflow / backpressure / error policies
//! - Negative cases (missing topic, wrong subscription config)

use otap_df_config::engine::{EngineConfig, EngineSettings};
use otap_df_config::observed_state::{ObservedStateSettings, SendPolicy};
use otap_df_config::pipeline::{PipelineConfig, PipelineConfigBuilder, PipelineType};
use otap_df_config::pipeline_group::PipelineGroupConfig;
use otap_df_config::topic::TopicConfig;
use otap_df_config::{DeployedPipelineKey, PipelineGroupId, PipelineId};
use otap_df_engine::context::ControllerContext;
use otap_df_engine::control::{
    AckMsg, NodeControlMsg, PipelineControlMsg, pipeline_ctrl_msg_channel,
};
use otap_df_engine::distributed_slice;
use otap_df_engine::entity_context::set_pipeline_entity_key;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::exporter::ExporterWrapper;
use otap_df_engine::local::exporter::{EffectHandler, Exporter};
use otap_df_engine::message::{Message, MessageChannel};
use otap_df_engine::topic::{TopicRegistry, TopicRegistryHandle};
use otap_df_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otap_df_otap::OTAP_PIPELINE_FACTORY;
use otap_df_otap::fake_data_generator::OTAP_FAKE_DATA_GENERATOR_URN;
use otap_df_otap::fake_data_generator::config::{
    Config as FakeDataGeneratorConfig, DataSource, TrafficConfig,
};
use otap_df_otap::pdata::OtapPdata;
use otap_df_otap::topic_exporter::TOPIC_EXPORTER_URN;
use otap_df_otap::topic_receiver::TOPIC_RECEIVER_URN;
use otap_df_state::store::ObservedStateStore;
use otap_df_telemetry::InternalTelemetrySystem;
use serde_json::{Value, json, to_value};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use weaver_common::vdir::VirtualDirectoryPath;

static MESSAGE_COUNT: AtomicUsize = AtomicUsize::new(0);
const COUNT_EXPORTER_URN: &str = "urn:otel:test:exporter";
static MESSAGE_COUNT_A: AtomicUsize = AtomicUsize::new(0);
static MESSAGE_COUNT_B: AtomicUsize = AtomicUsize::new(0);
const COUNT_EXPORTER_A_URN: &str = "urn:otel:test:exporter:a:exporter";
const COUNT_EXPORTER_B_URN: &str = "urn:otel:test:exporter:b:exporter";

struct CountingExporter;
struct CountingExporterA;
struct CountingExporterB;

#[allow(unsafe_code)]
#[distributed_slice(otap_df_otap::OTAP_EXPORTER_FACTORIES)]
static COUNT_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: COUNT_EXPORTER_URN,
    create: |_pipeline, node, node_config, exporter_config| {
        Ok(ExporterWrapper::local(
            CountingExporter,
            node,
            node_config,
            exporter_config,
        ))
    },
};

#[async_trait::async_trait(?Send)]
impl Exporter<OtapPdata> for CountingExporter {
    async fn start(
        self: Box<Self>,
        mut msg_chan: MessageChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<otap_df_engine::terminal_state::TerminalState, EngineError> {
        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    return Ok(otap_df_engine::terminal_state::TerminalState::new(
                        deadline,
                        Vec::<otap_df_telemetry::metrics::MetricSetSnapshot>::new(),
                    ));
                }
                Message::PData(data) => {
                    let _ = MESSAGE_COUNT.fetch_add(1, Ordering::AcqRel);
                    effect_handler.notify_ack(AckMsg::new(data)).await?;
                }
                _ => {}
            }
        }
    }
}

#[allow(unsafe_code)]
#[distributed_slice(otap_df_otap::OTAP_EXPORTER_FACTORIES)]
static COUNT_EXPORTER_A: ExporterFactory<OtapPdata> = ExporterFactory {
    name: COUNT_EXPORTER_A_URN,
    create: |_pipeline, node, node_config, exporter_config| {
        Ok(ExporterWrapper::local(
            CountingExporterA,
            node,
            node_config,
            exporter_config,
        ))
    },
};

#[allow(unsafe_code)]
#[distributed_slice(otap_df_otap::OTAP_EXPORTER_FACTORIES)]
static COUNT_EXPORTER_B: ExporterFactory<OtapPdata> = ExporterFactory {
    name: COUNT_EXPORTER_B_URN,
    create: |_pipeline, node, node_config, exporter_config| {
        Ok(ExporterWrapper::local(
            CountingExporterB,
            node,
            node_config,
            exporter_config,
        ))
    },
};

#[async_trait::async_trait(?Send)]
impl Exporter<OtapPdata> for CountingExporterA {
    async fn start(
        self: Box<Self>,
        mut msg_chan: MessageChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<otap_df_engine::terminal_state::TerminalState, EngineError> {
        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    return Ok(otap_df_engine::terminal_state::TerminalState::new(
                        deadline,
                        Vec::<otap_df_telemetry::metrics::MetricSetSnapshot>::new(),
                    ));
                }
                Message::PData(data) => {
                    let _ = MESSAGE_COUNT_A.fetch_add(1, Ordering::AcqRel);
                    effect_handler.notify_ack(AckMsg::new(data)).await?;
                }
                _ => {}
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Exporter<OtapPdata> for CountingExporterB {
    async fn start(
        self: Box<Self>,
        mut msg_chan: MessageChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<otap_df_engine::terminal_state::TerminalState, EngineError> {
        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    return Ok(otap_df_engine::terminal_state::TerminalState::new(
                        deadline,
                        Vec::<otap_df_telemetry::metrics::MetricSetSnapshot>::new(),
                    ));
                }
                Message::PData(data) => {
                    let _ = MESSAGE_COUNT_B.fetch_add(1, Ordering::AcqRel);
                    effect_handler.notify_ack(AckMsg::new(data)).await?;
                }
                _ => {}
            }
        }
    }
}

#[test]
fn test_topic_balanced_integration() {
    MESSAGE_COUNT.store(0, Ordering::Release);

    let pipeline_group_id: PipelineGroupId = "topic-group".into();
    let producer_id: PipelineId = "producer".into();
    let consumer_id: PipelineId = "consumer".into();

    // 1) Build two pipelines connected via topic "shared".
    let producer_config = build_producer_pipeline_with_max(&pipeline_group_id, &producer_id, 5);
    let consumer_config =
        build_consumer_pipeline_with_exporter(&pipeline_group_id, &consumer_id, COUNT_EXPORTER_URN);

    let mut pipeline_group = PipelineGroupConfig::new();
    pipeline_group
        .add_pipeline(producer_id.clone(), producer_config.clone())
        .expect("producer pipeline config");
    pipeline_group
        .add_pipeline(consumer_id.clone(), consumer_config.clone())
        .expect("consumer pipeline config");

    let mut engine_config = EngineConfig {
        settings: EngineSettings::default(),
        topics: HashMap::new(),
        pipeline_groups: HashMap::new(),
    };
    let _ = engine_config.topics.insert(
        "shared".into(),
        TopicConfig {
            description: None,
            policy: Default::default(),
        },
    );
    let _ = engine_config
        .pipeline_groups
        .insert(pipeline_group_id.clone(), pipeline_group);
    engine_config.validate().expect("engine config validation");

    // 2) Create topic registry and inject into controller context.
    let topic_registry = Arc::new(
        TopicRegistry::<OtapPdata>::from_engine_config(&engine_config).expect("topic registry"),
    );

    let telemetry_system = InternalTelemetrySystem::default();
    let registry = telemetry_system.registry();
    let controller_ctx = ControllerContext::new(registry.clone())
        .with_topic_registry(TopicRegistryHandle::new(topic_registry));
    let observed_state_store = ObservedStateStore::new(&ObservedStateSettings::default());
    let event_reporter = observed_state_store.reporter(SendPolicy::default());
    let metrics_reporter = telemetry_system.reporter();

    let (consumer_ctrl_tx, consumer_ctrl_rx) = pipeline_ctrl_msg_channel(
        consumer_config
            .pipeline_settings()
            .default_pipeline_ctrl_msg_channel_size,
    );
    let (producer_ctrl_tx, producer_ctrl_rx) = pipeline_ctrl_msg_channel(
        producer_config
            .pipeline_settings()
            .default_pipeline_ctrl_msg_channel_size,
    );

    let (ready_tx, ready_rx) = mpsc::channel();
    // 3) Start consumer first so its subscription is ready.
    let consumer_ctx =
        controller_ctx.pipeline_context_with(pipeline_group_id.clone(), consumer_id.clone(), 0, 0);
    let consumer_key = DeployedPipelineKey {
        pipeline_group_id: pipeline_group_id.clone(),
        pipeline_id: consumer_id.clone(),
        core_id: 0,
    };
    let consumer_handle = spawn_pipeline(
        consumer_key,
        consumer_ctx,
        consumer_config.clone(),
        consumer_ctrl_tx.clone(),
        consumer_ctrl_rx,
        event_reporter.clone(),
        metrics_reporter.clone(),
        Some(ready_tx),
    );

    ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("consumer pipeline ready");

    // 4) Start producer to publish at least one message.
    let producer_ctx =
        controller_ctx.pipeline_context_with(pipeline_group_id.clone(), producer_id.clone(), 1, 1);
    let producer_key = DeployedPipelineKey {
        pipeline_group_id: pipeline_group_id.clone(),
        pipeline_id: producer_id.clone(),
        core_id: 1,
    };
    let producer_handle = spawn_pipeline(
        producer_key,
        producer_ctx,
        producer_config.clone(),
        producer_ctrl_tx.clone(),
        producer_ctrl_rx,
        event_reporter.clone(),
        metrics_reporter.clone(),
        None,
    );

    // 5) Assert message observed, then shutdown both pipelines.
    let received = wait_for_count(1, Duration::from_secs(2));

    let shutdown_deadline = Instant::now() + Duration::from_secs(1);
    producer_ctrl_tx
        .try_send(PipelineControlMsg::Shutdown {
            deadline: shutdown_deadline,
            reason: "topic test shutdown".to_owned(),
        })
        .expect("producer shutdown");
    consumer_ctrl_tx
        .try_send(PipelineControlMsg::Shutdown {
            deadline: shutdown_deadline,
            reason: "topic test shutdown".to_owned(),
        })
        .expect("consumer shutdown");

    let producer_result = producer_handle.join().expect("producer thread");
    let consumer_result = consumer_handle.join().expect("consumer thread");

    assert!(received, "no message received through the topic");
    assert!(producer_result.is_ok(), "producer pipeline failed");
    assert!(consumer_result.is_ok(), "consumer pipeline failed");
}

#[test]
fn test_topic_balanced_integration_two_consumers() {
    MESSAGE_COUNT_A.store(0, Ordering::Release);
    MESSAGE_COUNT_B.store(0, Ordering::Release);

    let expected_messages = 10usize;
    let pipeline_group_id: PipelineGroupId = "topic-group".into();
    let producer_id: PipelineId = "producer".into();
    let consumer_a_id: PipelineId = "consumer-a".into();
    let consumer_b_id: PipelineId = "consumer-b".into();

    let producer_config = build_producer_pipeline_with_max(
        &pipeline_group_id,
        &producer_id,
        expected_messages as u64,
    );
    let consumer_a_config = build_consumer_pipeline_with_exporter(
        &pipeline_group_id,
        &consumer_a_id,
        COUNT_EXPORTER_A_URN,
    );
    let consumer_b_config = build_consumer_pipeline_with_exporter(
        &pipeline_group_id,
        &consumer_b_id,
        COUNT_EXPORTER_B_URN,
    );

    let mut pipeline_group = PipelineGroupConfig::new();
    pipeline_group
        .add_pipeline(producer_id.clone(), producer_config.clone())
        .expect("producer pipeline config");
    pipeline_group
        .add_pipeline(consumer_a_id.clone(), consumer_a_config.clone())
        .expect("consumer-a pipeline config");
    pipeline_group
        .add_pipeline(consumer_b_id.clone(), consumer_b_config.clone())
        .expect("consumer-b pipeline config");

    let mut engine_config = EngineConfig {
        settings: EngineSettings::default(),
        topics: HashMap::new(),
        pipeline_groups: HashMap::new(),
    };
    let _ = engine_config.topics.insert(
        "shared".into(),
        TopicConfig {
            description: None,
            policy: Default::default(),
        },
    );
    let _ = engine_config
        .pipeline_groups
        .insert(pipeline_group_id.clone(), pipeline_group);
    engine_config.validate().expect("engine config validation");

    let topic_registry = Arc::new(
        TopicRegistry::<OtapPdata>::from_engine_config(&engine_config).expect("topic registry"),
    );

    let telemetry_system = InternalTelemetrySystem::default();
    let registry = telemetry_system.registry();
    let controller_ctx = ControllerContext::new(registry.clone())
        .with_topic_registry(TopicRegistryHandle::new(topic_registry));
    let observed_state_store = ObservedStateStore::new(&ObservedStateSettings::default());
    let event_reporter = observed_state_store.reporter(SendPolicy::default());
    let metrics_reporter = telemetry_system.reporter();

    let (consumer_a_ctrl_tx, consumer_a_ctrl_rx) = pipeline_ctrl_msg_channel(
        consumer_a_config
            .pipeline_settings()
            .default_pipeline_ctrl_msg_channel_size,
    );
    let (consumer_b_ctrl_tx, consumer_b_ctrl_rx) = pipeline_ctrl_msg_channel(
        consumer_b_config
            .pipeline_settings()
            .default_pipeline_ctrl_msg_channel_size,
    );
    let (producer_ctrl_tx, producer_ctrl_rx) = pipeline_ctrl_msg_channel(
        producer_config
            .pipeline_settings()
            .default_pipeline_ctrl_msg_channel_size,
    );

    let (ready_tx, ready_rx) = mpsc::channel();
    let consumer_a_ctx = controller_ctx.pipeline_context_with(
        pipeline_group_id.clone(),
        consumer_a_id.clone(),
        0,
        0,
    );
    let consumer_a_key = DeployedPipelineKey {
        pipeline_group_id: pipeline_group_id.clone(),
        pipeline_id: consumer_a_id.clone(),
        core_id: 0,
    };
    let consumer_a_handle = spawn_pipeline(
        consumer_a_key,
        consumer_a_ctx,
        consumer_a_config.clone(),
        consumer_a_ctrl_tx.clone(),
        consumer_a_ctrl_rx,
        event_reporter.clone(),
        metrics_reporter.clone(),
        Some(ready_tx.clone()),
    );

    let consumer_b_ctx = controller_ctx.pipeline_context_with(
        pipeline_group_id.clone(),
        consumer_b_id.clone(),
        1,
        1,
    );
    let consumer_b_key = DeployedPipelineKey {
        pipeline_group_id: pipeline_group_id.clone(),
        pipeline_id: consumer_b_id.clone(),
        core_id: 1,
    };
    let consumer_b_handle = spawn_pipeline(
        consumer_b_key,
        consumer_b_ctx,
        consumer_b_config.clone(),
        consumer_b_ctrl_tx.clone(),
        consumer_b_ctrl_rx,
        event_reporter.clone(),
        metrics_reporter.clone(),
        Some(ready_tx),
    );

    for _ in 0..2 {
        ready_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("consumer pipeline ready");
    }

    let producer_ctx =
        controller_ctx.pipeline_context_with(pipeline_group_id.clone(), producer_id.clone(), 2, 2);
    let producer_key = DeployedPipelineKey {
        pipeline_group_id: pipeline_group_id.clone(),
        pipeline_id: producer_id.clone(),
        core_id: 2,
    };
    let producer_handle = spawn_pipeline(
        producer_key,
        producer_ctx,
        producer_config.clone(),
        producer_ctrl_tx.clone(),
        producer_ctrl_rx,
        event_reporter.clone(),
        metrics_reporter.clone(),
        None,
    );

    let received = wait_for_total_count(expected_messages, Duration::from_secs(3));

    let shutdown_deadline = Instant::now() + Duration::from_secs(1);
    producer_ctrl_tx
        .try_send(PipelineControlMsg::Shutdown {
            deadline: shutdown_deadline,
            reason: "topic test shutdown".to_owned(),
        })
        .expect("producer shutdown");
    consumer_a_ctrl_tx
        .try_send(PipelineControlMsg::Shutdown {
            deadline: shutdown_deadline,
            reason: "topic test shutdown".to_owned(),
        })
        .expect("consumer-a shutdown");
    consumer_b_ctrl_tx
        .try_send(PipelineControlMsg::Shutdown {
            deadline: shutdown_deadline,
            reason: "topic test shutdown".to_owned(),
        })
        .expect("consumer-b shutdown");

    let producer_result = producer_handle.join().expect("producer thread");
    let consumer_a_result = consumer_a_handle.join().expect("consumer-a thread");
    let consumer_b_result = consumer_b_handle.join().expect("consumer-b thread");

    let total_received =
        MESSAGE_COUNT_A.load(Ordering::Acquire) + MESSAGE_COUNT_B.load(Ordering::Acquire);
    assert!(received, "expected messages not received through the topic");
    assert_eq!(
        total_received, expected_messages,
        "unexpected total message count"
    );
    assert!(producer_result.is_ok(), "producer pipeline failed");
    assert!(consumer_a_result.is_ok(), "consumer-a pipeline failed");
    assert!(consumer_b_result.is_ok(), "consumer-b pipeline failed");
}

fn spawn_pipeline(
    pipeline_key: DeployedPipelineKey,
    pipeline_ctx: otap_df_engine::context::PipelineContext,
    pipeline_config: PipelineConfig,
    pipeline_ctrl_tx: otap_df_engine::control::PipelineCtrlMsgSender<OtapPdata>,
    pipeline_ctrl_rx: otap_df_engine::control::PipelineCtrlMsgReceiver<OtapPdata>,
    event_reporter: otap_df_telemetry::event::ObservedEventReporter,
    metrics_reporter: otap_df_telemetry::reporter::MetricsReporter,
    ready_tx: Option<mpsc::Sender<()>>,
) -> thread::JoinHandle<Result<Vec<()>, EngineError>> {
    thread::spawn(move || {
        let pipeline_entity_key = pipeline_ctx.register_pipeline_entity();
        let runtime_pipeline = OTAP_PIPELINE_FACTORY
            .build(pipeline_ctx.clone(), pipeline_config.clone(), None)
            .expect("build pipeline");

        if let Some(tx) = ready_tx {
            let _ = tx.send(());
        }

        let _pipeline_entity_guard =
            set_pipeline_entity_key(pipeline_ctx.metrics_registry(), pipeline_entity_key);

        runtime_pipeline.run_forever(
            pipeline_key,
            pipeline_ctx,
            event_reporter,
            metrics_reporter,
            pipeline_ctrl_tx,
            pipeline_ctrl_rx,
        )
    })
}

fn build_producer_pipeline_with_max(
    pipeline_group_id: &PipelineGroupId,
    pipeline_id: &PipelineId,
    max_signal_count: u64,
) -> PipelineConfig {
    let receiver_config_value = fake_receiver_config_value_with_max(max_signal_count);
    let topic_exporter_config = json!({ "topic": "shared" });

    PipelineConfigBuilder::new()
        .add_receiver(
            "generator",
            OTAP_FAKE_DATA_GENERATOR_URN,
            Some(receiver_config_value),
        )
        .add_exporter("topic_out", TOPIC_EXPORTER_URN, Some(topic_exporter_config))
        .round_robin("generator", "out", ["topic_out"])
        .build(
            PipelineType::Otap,
            pipeline_group_id.clone(),
            pipeline_id.clone(),
        )
        .expect("producer pipeline config")
}

fn build_consumer_pipeline_with_exporter(
    pipeline_group_id: &PipelineGroupId,
    pipeline_id: &PipelineId,
    exporter_urn: &'static str,
) -> PipelineConfig {
    let topic_receiver_config: Value = json!({
        "topic": "shared",
        "subscription": {
            "mode": "balanced",
            "group": "workers"
        }
    });

    PipelineConfigBuilder::new()
        .add_receiver("topic_in", TOPIC_RECEIVER_URN, Some(topic_receiver_config))
        .add_exporter("counter", exporter_urn, None)
        .round_robin("topic_in", "out", ["counter"])
        .build(
            PipelineType::Otap,
            pipeline_group_id.clone(),
            pipeline_id.clone(),
        )
        .expect("consumer pipeline config")
}

fn fake_receiver_config_value_with_max(max_signal_count: u64) -> Value {
    let traffic_config = TrafficConfig::new(None, Some(max_signal_count), 1, 0, 0, 1);
    let registry_path = VirtualDirectoryPath::GitRepo {
        url: "https://github.com/open-telemetry/semantic-conventions.git".to_owned(),
        sub_folder: Some("model".to_owned()),
        refspec: None,
    };
    let receiver_config = FakeDataGeneratorConfig::new(traffic_config, registry_path)
        .with_data_source(DataSource::Static);
    to_value(receiver_config).expect("fake data generator config")
}

fn wait_for_count(expected: usize, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if MESSAGE_COUNT.load(Ordering::Acquire) >= expected {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

fn wait_for_total_count(expected: usize, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let total =
            MESSAGE_COUNT_A.load(Ordering::Acquire) + MESSAGE_COUNT_B.load(Ordering::Acquire);
        if total >= expected {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}
