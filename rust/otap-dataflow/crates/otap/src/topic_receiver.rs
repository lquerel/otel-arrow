// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Topic receiver implementation (balanced subscription).

use crate::OTAP_RECEIVER_FACTORIES;
use crate::pdata::OtapPdata;
use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::ReceiverFactory;
use otap_df_engine::config::ReceiverConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::error::Error;
use otap_df_engine::local::receiver as local;
use otap_df_engine::receiver::ReceiverWrapper;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_engine::topic::TopicSubscription;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// The URN for the topic receiver.
pub const TOPIC_RECEIVER_URN: &str = "urn:otel:topic:receiver";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    topic: String,
    subscription: SubscriptionConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionConfig {
    mode: SubscriptionMode,
    group: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SubscriptionMode {
    Balanced,
    Broadcast,
}

struct TopicReceiver {
    subscription: TopicSubscription<OtapPdata>,
}

/// Declares the topic receiver as a local receiver factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static TOPIC_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: TOPIC_RECEIVER_URN,
    create: |pipeline: PipelineContext,
             node: otap_df_engine::node::NodeId,
             node_config: Arc<NodeUserConfig>,
             receiver_config: &ReceiverConfig| {
        Ok(ReceiverWrapper::local(
            TopicReceiver::from_config(pipeline, &node_config.config)?,
            node,
            node_config,
            receiver_config,
        ))
    },
};

impl TopicReceiver {
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

        let group = match config.subscription.mode {
            SubscriptionMode::Balanced => {
                let group = config.subscription.group.as_deref().ok_or_else(|| {
                    otap_df_config::error::Error::InvalidUserConfig {
                        error: "subscription group is required for balanced mode".to_string(),
                    }
                })?;
                let group = group.trim();
                if group.is_empty() {
                    return Err(otap_df_config::error::Error::InvalidUserConfig {
                        error: "subscription group cannot be empty".to_string(),
                    });
                }
                group.to_string()
            }
            SubscriptionMode::Broadcast => {
                return Err(otap_df_config::error::Error::InvalidUserConfig {
                    error: "broadcast subscription mode is not supported yet".to_string(),
                });
            }
        };

        let registry = pipeline_ctx.topic_registry::<OtapPdata>().ok_or_else(|| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: "topic registry is not configured".to_string(),
            }
        })?;

        let topic_key = registry.resolve(
            &pipeline_ctx.pipeline_group_id(),
            &pipeline_ctx.pipeline_id(),
            topic,
        )?;

        let subscription = registry.subscribe_balanced(&topic_key, &group)?;

        let _ = topic_key;

        Ok(Self { subscription })
    }
}

#[async_trait(?Send)]
impl local::Receiver<OtapPdata> for TopicReceiver {
    async fn start(
        mut self: Box<Self>,
        mut ctrl_chan: local::ControlChannel<OtapPdata>,
        effect_handler: local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        loop {
            tokio::select! {
                biased;
                ctrl_msg = ctrl_chan.recv() => {
                    match ctrl_msg {
                        Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                            return Ok(TerminalState::new(
                                deadline,
                                Vec::<otap_df_telemetry::metrics::MetricSetSnapshot>::new(),
                            ));
                        }
                        Ok(NodeControlMsg::CollectTelemetry { .. }) => {}
                        Ok(_) => {}
                        Err(e) => return Err(Error::ChannelRecvError(e)),
                    }
                }
                recv = self.subscription.recv() => {
                    match recv {
                        Ok(data) => {
                            effect_handler.send_message(data).await?;
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
            }
        }

        Ok(TerminalState::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_engine::context::ControllerContext;
    use otap_df_telemetry::registry::TelemetryRegistryHandle;
    use serde_json::json;

    fn test_pipeline_context() -> PipelineContext {
        let telemetry_registry = TelemetryRegistryHandle::new();
        let controller_ctx = ControllerContext::new(telemetry_registry);
        controller_ctx.pipeline_context_with("grp".into(), "pipe".into(), 0, 0)
    }

    #[test]
    fn topic_receiver_rejects_empty_topic() {
        // Topic name is required and cannot be blank.
        let pipeline_ctx = test_pipeline_context();
        let config = json!({
            "topic": "   ",
            "subscription": {
                "mode": "balanced",
                "group": "workers"
            }
        });

        let err = TopicReceiver::from_config(pipeline_ctx, &config)
            .err()
            .expect("expected empty topic error");
        assert!(err.to_string().contains("topic name cannot be empty"));
    }

    #[test]
    fn topic_receiver_requires_group_for_balanced() {
        // Balanced mode requires an explicit consumer group.
        let pipeline_ctx = test_pipeline_context();
        let config = json!({
            "topic": "shared",
            "subscription": {
                "mode": "balanced"
            }
        });

        let err = TopicReceiver::from_config(pipeline_ctx, &config)
            .err()
            .expect("expected missing group error");
        assert!(
            err.to_string()
                .contains("subscription group is required for balanced mode")
        );
    }

    #[test]
    fn topic_receiver_rejects_broadcast_mode() {
        // Broadcast is parsed but not supported yet.
        let pipeline_ctx = test_pipeline_context();
        let config = json!({
            "topic": "shared",
            "subscription": {
                "mode": "broadcast"
            }
        });

        let err = TopicReceiver::from_config(pipeline_ctx, &config)
            .err()
            .expect("expected broadcast mode error");
        assert!(
            err.to_string()
                .contains("broadcast subscription mode is not supported yet")
        );
    }

    #[test]
    fn topic_receiver_requires_registry() {
        // A registry must be injected into the pipeline context.
        let pipeline_ctx = test_pipeline_context();
        let config = json!({
            "topic": "shared",
            "subscription": {
                "mode": "balanced",
                "group": "workers"
            }
        });

        let err = TopicReceiver::from_config(pipeline_ctx, &config)
            .err()
            .expect("expected missing registry error");
        assert!(err.to_string().contains("topic registry is not configured"));
    }
}
