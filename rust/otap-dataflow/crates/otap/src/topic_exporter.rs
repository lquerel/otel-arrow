// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Topic exporter implementation.

use crate::OTAP_EXPORTER_FACTORIES;
use crate::pdata::OtapPdata;
use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::ExporterConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{AckMsg, NackMsg, NodeControlMsg};
use otap_df_engine::error::Error;
use otap_df_engine::exporter::ExporterWrapper;
use otap_df_engine::local::exporter::{EffectHandler, Exporter};
use otap_df_engine::message::{Message, MessageChannel};
use otap_df_engine::terminal_state::TerminalState;
use otap_df_engine::topic::{TopicKey, TopicRegistry};
use otap_df_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// The URN for the topic exporter.
pub const TOPIC_EXPORTER_URN: &str = "urn:otel:topic:exporter";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    topic: String,
}

struct TopicExporter {
    registry: Arc<TopicRegistry<OtapPdata>>,
    topic_key: TopicKey,
}

/// Declares the topic exporter as a local exporter factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static TOPIC_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: TOPIC_EXPORTER_URN,
    create: |pipeline: PipelineContext,
             node: otap_df_engine::node::NodeId,
             node_config: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig| {
        Ok(ExporterWrapper::local(
            TopicExporter::from_config(pipeline, &node_config.config)?,
            node,
            node_config,
            exporter_config,
        ))
    },
};

impl TopicExporter {
    fn from_config(
        pipeline_ctx: PipelineContext,
        config: &Value,
    ) -> Result<Self, otap_df_config::error::Error> {
        let config: Config = serde_json::from_value(config.clone()).map_err(|e| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: e.to_string(),
            }
        })?;

        let topic = config.topic.trim();
        if topic.is_empty() {
            return Err(otap_df_config::error::Error::InvalidUserConfig {
                error: "topic name cannot be empty".to_string(),
            });
        }

        let registry = pipeline_ctx
            .topic_registry::<OtapPdata>()
            .ok_or_else(|| otap_df_config::error::Error::InvalidUserConfig {
                error: "topic registry is not configured".to_string(),
            })?;

        let topic_key = registry.resolve(
            &pipeline_ctx.pipeline_group_id(),
            &pipeline_ctx.pipeline_id(),
            topic,
        )?;

        Ok(Self { registry, topic_key })
    }
}

#[async_trait(?Send)]
impl Exporter<OtapPdata> for TopicExporter {
    async fn start(
        self: Box<Self>,
        mut msg_chan: MessageChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    return Ok(TerminalState::new(
                        deadline,
                        Vec::<otap_df_telemetry::metrics::MetricSetSnapshot>::new(),
                    ));
                }
                Message::Control(NodeControlMsg::CollectTelemetry { .. }) => {}
                Message::PData(data) => {
                    let publish_data = data.clone();
                    let publish_result = self.registry.publish(&self.topic_key, publish_data).await;
                    match publish_result {
                        Ok(()) => {
                            effect_handler.notify_ack(AckMsg::new(data)).await?;
                        }
                        Err(err) => {
                            let reason = err.to_string();
                            effect_handler
                                .notify_nack(NackMsg::new(&reason, data))
                                .await?;
                        }
                    }
                }
                _ => {}
            }
        }
    }
}
