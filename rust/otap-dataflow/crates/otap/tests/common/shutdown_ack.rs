// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Test nodes that emit exporter acknowledgements only during shutdown.

use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::{ExporterConfig, ProcessorConfig};
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{AckMsg, CallData, NodeControlMsg};
use otap_df_engine::error::Error;
use otap_df_engine::exporter::ExporterWrapper;
use otap_df_engine::local::exporter::{EffectHandler as ExporterEffectHandler, Exporter};
use otap_df_engine::local::processor::{EffectHandler as ProcessorEffectHandler, Processor};
use otap_df_engine::message::{ExporterInbox, Message};
use otap_df_engine::node::NodeId;
use otap_df_engine::processor::ProcessorWrapper;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_engine::{
    ConsumerEffectHandlerExtension, ExporterFactory, Interests,
    MessageSourceLocalEffectHandlerExtension, ProcessorFactory, ProducerEffectHandlerExtension,
};
use otap_df_otap::pdata::OtapPdata;
use otap_df_otap::{OTAP_EXPORTER_FACTORIES, OTAP_PROCESSOR_FACTORIES};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

pub const SHUTDOWN_ACK_PROCESSOR_URN: &str = "urn:otel:processor:shutdown_ack_test";
pub const SHUTDOWN_ACK_EXPORTER_URN: &str = "urn:otel:exporter:shutdown_ack_test";

#[derive(Clone)]
struct ShutdownAckState {
    exported: Arc<AtomicU64>,
    observed: Arc<AtomicU64>,
}

static STATE_REGISTRY: LazyLock<Mutex<HashMap<String, ShutdownAckState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Handles used to coordinate and validate one shutdown acknowledgement test.
pub struct ShutdownAckHandles {
    exported: Arc<AtomicU64>,
    observed: Arc<AtomicU64>,
}

impl ShutdownAckHandles {
    #[must_use]
    pub fn exported(&self) -> u64 {
        self.exported.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn observed(&self) -> u64 {
        self.observed.load(Ordering::Acquire)
    }
}

/// Registers isolated state for a test pipeline.
#[must_use]
pub fn register_state(id: impl Into<String>) -> ShutdownAckHandles {
    let exported = Arc::new(AtomicU64::new(0));
    let observed = Arc::new(AtomicU64::new(0));
    let state = ShutdownAckState {
        exported: exported.clone(),
        observed: observed.clone(),
    };
    let _ = STATE_REGISTRY.lock().insert(id.into(), state);
    ShutdownAckHandles { exported, observed }
}

/// Removes state after a test pipeline exits.
pub fn unregister_state(id: &str) {
    let _ = STATE_REGISTRY.lock().remove(id);
}

fn state_from_config(config: &NodeUserConfig) -> Option<ShutdownAckState> {
    config
        .config
        .get("test_id")
        .and_then(|value| value.as_str())
        .and_then(|id| STATE_REGISTRY.lock().get(id).cloned())
}

struct ShutdownAckProcessor {
    state: Option<ShutdownAckState>,
}

#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static SHUTDOWN_ACK_PROCESSOR: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: SHUTDOWN_ACK_PROCESSOR_URN,
    create: |_pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             processor_config: &ProcessorConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
        Ok(ProcessorWrapper::local(
            ShutdownAckProcessor {
                state: state_from_config(&node_config),
            },
            node,
            node_config,
            processor_config,
        ))
    },
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: |_| Ok(()),
};

#[async_trait(?Send)]
impl Processor<OtapPdata> for ShutdownAckProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut ProcessorEffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        match msg {
            Message::PData(mut data) => {
                effect_handler.subscribe_to(Interests::ACKS, CallData::default(), &mut data);
                effect_handler.send_message_with_source_node(data).await?;
            }
            Message::Control(NodeControlMsg::Ack(ack)) => {
                if let Some(state) = &self.state {
                    let _ = state.observed.fetch_add(1, Ordering::Release);
                }
                effect_handler.notify_ack(ack).await?;
            }
            Message::Control(_) => {}
        }
        Ok(())
    }
}

struct ShutdownAckExporter {
    state: Option<ShutdownAckState>,
}

#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
static SHUTDOWN_ACK_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: SHUTDOWN_ACK_EXPORTER_URN,
    create: |_pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
        Ok(ExporterWrapper::local(
            ShutdownAckExporter {
                state: state_from_config(&node_config),
            },
            node,
            node_config,
            exporter_config,
        ))
    },
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: |_| Ok(()),
};

#[async_trait(?Send)]
impl Exporter<OtapPdata> for ShutdownAckExporter {
    async fn start(
        self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        effect_handler: ExporterEffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let mut pending = Vec::new();
        loop {
            match inbox.recv().await? {
                Message::PData(data) => {
                    if let Some(state) = &self.state {
                        let _ = state.exported.fetch_add(1, Ordering::Release);
                    }
                    pending.push(data);
                }
                Message::Control(NodeControlMsg::Shutdown { .. }) => {
                    for data in pending.drain(..) {
                        effect_handler.notify_ack(AckMsg::new(data)).await?;
                    }
                    break;
                }
                Message::Control(_) => {}
            }
        }
        Ok(TerminalState::default())
    }
}
