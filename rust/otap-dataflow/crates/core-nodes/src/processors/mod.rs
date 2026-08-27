// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

/// Delay processor.
pub mod delay_processor;

/// Debug processor.
pub mod debug_processor;

/// Shared selected-route admission machinery for exclusive routers.
pub mod exclusive_router_admission;

/// Batch processor.
pub mod batch_processor;

/// Attributes processor.
pub mod attributes_processor;

/// Content router processor.
pub mod content_router;

/// Durable buffer processor.
pub mod durable_buffer_processor;

/// Partition processor.
pub mod partition_processor;

/// Retry processor.
pub mod retry_processor;

/// Transform processor.
pub mod transform_processor;

/// Fan-out processor.
pub mod fanout_processor;

/// Filter processor.
pub mod filter_processor;

/// Signal type router processor.
pub mod signal_type_router;

/// Log sampling processor.
pub mod log_sampling_processor;

/// Temporal reaggregation processor.
pub mod temporal_reaggregation_processor;

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use otel_arrow_dfe_config::{SignalType, node::NodeUserConfig};
    use otel_arrow_dfe_engine::{
        Interests,
        capability::registry::Capabilities,
        context::ControllerContext,
        control::{PipelineCompletionMsg, pipeline_completion_msg_channel},
        message::Message,
        testing::{processor::TestRuntime, test_node},
    };
    use otel_arrow_dfe_otap::{OTAP_PROCESSOR_FACTORIES, pdata::OtapPdata, testing::TestCallData};
    use otel_arrow_dfe_pdata::codec::{EncodedPdata, PdataEncoding};
    use serde_json::json;
    use std::sync::Arc;

    /// Scenario: record processors receive an admitted batch containing malformed encoded data.
    /// Guarantees: attributes, filter, partition and transform Nack the original bytes without exiting.
    #[test]
    fn record_processors_nack_malformed_codec_input() {
        let configs = [
            (
                super::attributes_processor::ATTRIBUTES_PROCESSOR_URN,
                json!({"actions": [{"action": "delete", "key": "secret"}]}),
            ),
            (super::filter_processor::FILTER_PROCESSOR_URN, json!({})),
            (
                super::partition_processor::PARTITION_PROCESSOR_URN,
                json!({
                    "partition_by": {"opl_expression": "attributes[\"x\"]"},
                    "partition_header_name": "partition"
                }),
            ),
            (
                super::transform_processor::TRANSFORM_PROCESSOR_URN,
                json!({"kql_query": "logs | where severity_text == \"ERROR\""}),
            ),
        ];
        for (urn, config) in configs {
            let runtime = TestRuntime::<OtapPdata>::new();
            let controller = ControllerContext::new(runtime.metrics_registry());
            let pipeline =
                controller.pipeline_context_with("group".into(), "pipeline".into(), 0, 1, 0);
            let mut node_config = NodeUserConfig::new_processor_config(urn);
            node_config.config = config;
            node_config.default_output = Some("default".into());
            let factory = OTAP_PROCESSOR_FACTORIES
                .iter()
                .find(|f| f.name == urn)
                .unwrap();
            let processor = (factory.create)(
                pipeline,
                test_node("codec-test"),
                Arc::new(node_config),
                runtime.config(),
                &Capabilities::empty(),
            )
            .unwrap();
            runtime
                .set_processor(processor)
                .run_test(move |mut ctx| async move {
                    let (completion_tx, mut completion_rx) = pipeline_completion_msg_channel(1);
                    ctx.set_pipeline_completion_sender(completion_tx);
                    let encoding = PdataEncoding::new("test-otlp-codec");
                    let bytes = Bytes::from(vec![1, 2, 3]);
                    let pdata = OtapPdata::new_default(
                        EncodedPdata::new(encoding.clone(), SignalType::Logs, bytes.clone())
                            .expect("registered test codec")
                            .into(),
                    )
                    .test_subscribe_to(
                        Interests::NACKS | Interests::RETURN_DATA,
                        TestCallData::new_with(0, 0).into(),
                        11,
                    );
                    ctx.process(Message::PData(pdata)).await.unwrap();
                    assert!(ctx.drain_pdata().await.is_empty(), "{urn}");
                    match completion_rx.recv().await.unwrap() {
                        PipelineCompletionMsg::DeliverNack { nack } => {
                            assert!(nack.permanent, "{urn}");
                            assert!(nack.reason.contains(encoding.as_str()), "{urn}");
                            let output = nack
                                .refused
                                .into_parts()
                                .1
                                .into_encoded(encoding, Default::default())
                                .unwrap();
                            assert_eq!(output.bytes().as_ptr(), bytes.as_ptr(), "{urn}");
                            assert_eq!(output.bytes(), &bytes, "{urn}");
                        }
                        other => panic!("{urn}: expected codec Nack, got {other:?}"),
                    }
                })
                .validate(|_| async {});
        }
    }
}
