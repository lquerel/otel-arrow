// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Capacity recovery checks through the real processor loop with explicit expected outcomes.

use otel_arrow_dfe_engine::{
    Interests,
    control::{
        AckMsg, Controllable, NackCause, NackMsg, NodeControlMsg, PipelineCompletionMsg,
        pipeline_completion_msg_channel, runtime_ctrl_msg_channel,
    },
    message::{Receiver, Sender},
    node::{Node, NodeWithPDataReceiver, NodeWithPDataSender},
    processor::ProcessorWrapper,
    shared::message::{SharedReceiver, SharedSender},
    testing::{processor::run_processor_loop, setup_test_runtime},
};
use otel_arrow_dfe_otap::{
    pdata::OtapPdata,
    testing::{TestCallData, next_ack, next_nack},
};
use otel_arrow_dfe_pdata::OtapArrowRecords;
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;

#[derive(Clone, Copy)]
pub(super) enum Reply {
    Ack,
    TransientNack,
    PermanentNack,
}

pub(super) struct CapacityScenario {
    pub split_input: OtapArrowRecords,
    pub recovery_input: OtapArrowRecords,
    pub ports: &'static [&'static str],
    pub refusal_reason: &'static str,
    pub inbound_limit: usize,
    pub replies: [Reply; 2],
    pub expected_permanent: bool,
}

fn request(batch: OtapArrowRecords, id: u64) -> OtapPdata {
    OtapPdata::new_default(batch.into()).test_subscribe_to(
        Interests::ACKS_OR_NACKS | Interests::RETURN_DATA,
        TestCallData::new_with(id, 0).into(),
        999,
    )
}

fn reply(output: OtapPdata, result: Reply) -> NodeControlMsg<OtapPdata> {
    match result {
        Reply::Ack => NodeControlMsg::Ack(
            next_ack(AckMsg::new(output))
                .expect("split Ack subscription")
                .1,
        ),
        Reply::TransientNack | Reply::PermanentNack => {
            let nack = if matches!(result, Reply::PermanentNack) {
                NackMsg::new_permanent_with_cause("downstream failure", output, NackCause::Refused)
            } else {
                NackMsg::new_with_cause("downstream failure", output, NackCause::Refused)
            };
            NodeControlMsg::Nack(next_nack(nack).expect("split Nack subscription").1)
        }
    }
}

fn assert_nack(message: PipelineCompletionMsg<OtapPdata>, id: u64, reason: &str, permanent: bool) {
    let PipelineCompletionMsg::DeliverNack { nack } = message else {
        panic!("expected Nack for request {id}, got {message:?}");
    };
    let (node, mut nack) = next_nack(nack).expect("original Nack subscriber retained");
    assert_eq!(node, 999);
    assert_eq!(
        TestCallData::try_from(nack.unwind.route.calldata)
            .expect("original request correlation data"),
        TestCallData::new_with(id, 0)
    );
    assert_eq!(nack.reason, reason);
    assert_eq!(nack.cause, NackCause::RouteFull);
    assert_eq!(nack.permanent, permanent);
    assert_eq!(
        nack.refused.num_items(),
        3,
        "return the original refused input"
    );
}

pub(super) fn assert_capacity_recovery(
    mut processor: ProcessorWrapper<OtapPdata>,
    reporter: MetricsReporter,
    scenario: CapacityScenario,
) {
    let node = processor.node_id().clone();
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(8);
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(8);
    processor
        .set_pdata_receiver(
            node.clone(),
            Receiver::Shared(SharedReceiver::mpsc(input_rx)),
        )
        .expect("configure processor input");
    // Merge actual ports only for observation; every configured send still uses the real handler.
    for port in std::iter::once("default").chain(scenario.ports.iter().copied()) {
        processor
            .set_pdata_sender(
                node.clone(),
                port.into(),
                Sender::Shared(SharedSender::mpsc(output_tx.clone())),
            )
            .expect("configure processor output");
    }
    drop(output_tx);
    let control = processor.control_sender();
    let (rt, tasks) = setup_test_runtime();
    tasks.block_on(&rt, async move {
        tokio::time::timeout(std::time::Duration::from_secs(5), async move {
            let (runtime_tx, _runtime_rx) = runtime_ctrl_msg_channel(16);
            let (completion_tx, mut completion_rx) = pipeline_completion_msg_channel(16);
            let task = tokio::task::spawn_local(run_processor_loop(
                processor,
                runtime_tx,
                completion_tx,
                reporter,
            ));
            input_tx
                .send(request(scenario.split_input.clone(), 1))
                .await
                .expect("submit initial split");
            let mut outputs = Vec::new();
            for _ in 0..2 {
                let mut output = output_rx
                    .recv()
                    .await
                    .expect("output before capacity refusal");
                assert!(
                    output.num_items() > 0,
                    "empty outputs must not consume capacity"
                );
                outputs.push(output);
            }
            // A second request cannot emit anything while the first holds all output slots.
            input_tx
                .send(request(scenario.split_input, 2))
                .await
                .expect("submit request while capacity is occupied");
            let zero_send_reason = if scenario.inbound_limit == 1 {
                "inbound slots not available"
            } else {
                scenario.refusal_reason
            };
            assert_nack(
                completion_rx
                    .recv()
                    .await
                    .expect("immediate refusal from the live processor"),
                2,
                zero_send_reason,
                false,
            );
            assert!(output_rx.try_recv().is_err());
            assert!(
                !task.is_finished(),
                "capacity refusal must keep the loop alive"
            );
            let stale = reply(outputs[0].clone(), Reply::PermanentNack);
            let duplicate = reply(
                outputs[0].clone(),
                if matches!(scenario.replies[0], Reply::Ack) {
                    Reply::PermanentNack
                } else {
                    Reply::Ack
                },
            );
            control
                .send(reply(outputs.remove(0), scenario.replies[0]))
                .await
                .expect("deliver first fresh reply");
            control
                .send(duplicate)
                .await
                .expect("deliver contradictory duplicate");
            // FIFO control-message barrier: observe telemetry only after the first reply
            // and its contradictory duplicate were consumed, without sleep-based assertions.
            let (snapshots, barrier_reporter) = MetricsReporter::create_new_and_receiver(16);
            control
                .send(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: barrier_reporter,
                })
                .await
                .expect("request control-message barrier");
            let _ = snapshots
                .recv_async()
                .await
                .expect("control-message barrier");
            assert!(
                completion_rx.try_recv().is_err(),
                "completion must wait for the last live output"
            );
            control
                .send(reply(outputs.remove(0), scenario.replies[1]))
                .await
                .expect("deliver final fresh reply");
            assert_nack(
                completion_rx
                    .recv()
                    .await
                    .expect("aggregate Nack after draining"),
                1,
                scenario.refusal_reason,
                scenario.expected_permanent,
            );
            // A new split must be able to reuse both inbound and outbound slots.
            input_tx
                .send(request(scenario.recovery_input, 3))
                .await
                .expect("submit recovery split");
            let a = output_rx.recv().await.expect("first recovered output");
            let b = output_rx.recv().await.expect("second recovered output");
            control
                .send(stale)
                .await
                .expect("replay stale result after slot reuse");
            control
                .send(reply(a, Reply::Ack))
                .await
                .expect("Ack first recovered output");
            control
                .send(reply(b, Reply::Ack))
                .await
                .expect("Ack second recovered output");
            let message = completion_rx
                .recv()
                .await
                .expect("completion of recovered request");
            let PipelineCompletionMsg::DeliverAck { ack } = message else {
                panic!("recovered request must Ack, got {message:?}");
            };
            let (node, ack) = next_ack(ack).expect("original Ack subscriber retained");
            assert_eq!(node, 999);
            assert_eq!(
                TestCallData::try_from(ack.unwind.route.calldata)
                    .expect("recovered request correlation data"),
                TestCallData::new_with(3, 0)
            );
            control
                .send(NodeControlMsg::Shutdown {
                    deadline: std::time::Instant::now(),
                    reason: "capacity regression complete".into(),
                })
                .await
                .expect("request clean shutdown");
            task.await
                .expect("processor task")
                .expect("capacity recovery must not be fatal");
            assert!(
                completion_rx.try_recv().is_err(),
                "one terminal decision per request"
            );
            assert!(
                output_rx.try_recv().is_err(),
                "unregistered outputs must stay unsent"
            );
        })
        .await
        .expect("capacity recovery must complete through the production loop");
    });
}
