// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Real inbox/router shutdown regressions using the engine's controlled clock.
use super::*;
use crate::processors::exclusive_router_admission::{OnFullPolicy, PROBE_INTERVAL};
use otel_arrow_dfe_channel::{error::RecvError, mpsc};
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_engine::Interests;
use otel_arrow_dfe_engine::clock::SimClock;
use otel_arrow_dfe_engine::control::{
    AckMsg, Controllable, PipelineCompletionMsg, PipelineCompletionMsgReceiver,
    pipeline_completion_msg_channel,
};
use otel_arrow_dfe_engine::local::message::{LocalReceiver, LocalSender};
use otel_arrow_dfe_engine::message::{ProcessorInbox, Receiver, Sender};
use otel_arrow_dfe_engine::node::{NodeWithPDataReceiver, NodeWithPDataSender};
use otel_arrow_dfe_engine::processor::ProcessorWrapperRuntime;
use otel_arrow_dfe_engine::testing::{test_node, test_pipeline_runtime_services};
use otel_arrow_dfe_otap::testing::{TestCallData, next_ack, next_nack};
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_telemetry::InternalTelemetrySystem;
use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

const SIGNALS: [(SignalType, &str); 3] = [
    (SignalType::Logs, PORT_LOGS),
    (SignalType::Metrics, PORT_METRICS),
    (SignalType::Traces, PORT_TRACES),
];

fn ready<F: Future>(future: F) -> F::Output {
    // Exclude the test task's cooperative budget, while keeping real channel
    // capacity and scheduler readiness authoritative.
    let mut future = std::pin::pin!(tokio::task::unconstrained(future));
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("operation stayed pending at a controlled ready boundary"),
    }
}

fn data(signal: SignalType, caller: Option<usize>) -> OtapPdata {
    let payload = match signal {
        SignalType::Logs => OtapArrowRecords::Logs(Default::default()),
        SignalType::Metrics => OtapArrowRecords::Metrics(Default::default()),
        SignalType::Traces => OtapArrowRecords::Traces(Default::default()),
    };
    let data = OtapPdata::new_default(payload.into());
    match caller {
        Some(caller) => data.test_subscribe_to(
            Interests::ACKS | Interests::NACKS | Interests::RETURN_DATA,
            TestCallData::new_with(caller as u64, 99).into(),
            caller,
        ),
        None => data,
    }
}

struct Fixture {
    processor: Box<dyn local::Processor<OtapPdata>>,
    inbox: ProcessorInbox<OtapPdata>,
    handler: local::EffectHandler<OtapPdata>,
    input: Option<mpsc::Sender<OtapPdata>>,
    control: Sender<NodeControlMsg<OtapPdata>>,
    output: mpsc::Receiver<OtapPdata>,
    completion: PipelineCompletionMsgReceiver<OtapPdata>,
    _healthy: mpsc::Receiver<OtapPdata>,
    _telemetry: InternalTelemetrySystem,
}

impl Fixture {
    async fn new(signal: SignalType, named_port: &'static str, default: bool) -> Self {
        let telemetry = InternalTelemetrySystem::default();
        let config = ProcessorConfig::new("signal_router_shutdown");
        let node = test_node(config.name.clone());
        let mut user = NodeUserConfig::new_processor_config(SIGNAL_TYPE_ROUTER_URN);
        let port = if default { "fallback" } else { named_port };
        if default {
            user.default_output = Some(port.into());
        }
        let router = SignalTypeRouter::new(SignalTypeRouterConfig {
            admission_policy: SelectedRouteAdmissionPolicy {
                on_full: OnFullPolicy::Backpressure,
            },
        });
        let mut wrapper = ProcessorWrapper::local(router, node.clone(), Arc::new(user), &config);
        let (input, receiver) = mpsc::Channel::new(2);
        wrapper
            .set_pdata_receiver(node.clone(), Receiver::Local(LocalReceiver::mpsc(receiver)))
            .unwrap();
        let (sender, output) = mpsc::Channel::new(1);
        sender.send(data(signal, None)).unwrap();
        wrapper
            .set_pdata_sender(
                node.clone(),
                port.into(),
                Sender::Local(LocalSender::mpsc(sender)),
            )
            .unwrap();
        let (sender, healthy) = mpsc::Channel::new(1);
        let healthy_port = if named_port == PORT_LOGS {
            PORT_METRICS
        } else {
            PORT_LOGS
        };
        wrapper
            .set_pdata_sender(
                node,
                healthy_port.into(),
                Sender::Local(LocalSender::mpsc(sender)),
            )
            .unwrap();
        let control = wrapper.control_sender();
        let ProcessorWrapperRuntime::Local {
            processor,
            inbox,
            mut effect_handler,
        } = wrapper
            .prepare_runtime(
                telemetry.reporter(),
                Interests::empty(),
                test_pipeline_runtime_services(),
            )
            .await
            .unwrap()
        else {
            unreachable!()
        };
        let (sender, completion) = pipeline_completion_msg_channel(8);
        effect_handler.set_pipeline_completion_msg_sender(sender);
        Self {
            processor,
            inbox,
            handler: effect_handler,
            input: Some(input),
            control,
            output,
            completion,
            _healthy: healthy,
            _telemetry: telemetry,
        }
    }

    fn process(&mut self, message: Message<OtapPdata>) {
        ready(self.processor.process(message, &mut self.handler)).unwrap();
    }

    fn receive(&mut self) -> Message<OtapPdata> {
        let allow = self.processor.accept_pdata();
        let next = {
            let mut receive =
                std::pin::pin!(tokio::task::unconstrained(self.inbox.recv_when(allow)));
            receive
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        };
        match next {
            Poll::Ready(value) => value.unwrap(),
            Poll::Pending => {
                assert!(
                    self.handler.pop_wakeup().is_some(),
                    "graceful shutdown cleared a router-owned retry before its parked work drained"
                );
                panic!("retry was not delivered at its controlled deadline");
            }
        }
    }

    fn latch_shutdown(&mut self, deadline: Instant) {
        self.control
            .try_send(NodeControlMsg::Shutdown {
                deadline,
                reason: "graceful regression".into(),
            })
            .unwrap();
        let mut receive = std::pin::pin!(tokio::task::unconstrained(self.inbox.recv_when(false)));
        assert!(matches!(
            receive
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
    }

    fn assert_forwarded(&self, signal: SignalType, expected: usize) {
        let message = self
            .output
            .try_recv()
            .expect("selected route must receive its retained message");
        assert_eq!(message.signal_type(), signal);
        let (caller, ack) = next_ack(AckMsg::new(message)).unwrap();
        assert_eq!(caller, expected);
        assert_eq!(
            TestCallData::try_from(ack.unwind.route.calldata.clone()).unwrap(),
            TestCallData::new_with(expected as u64, 99)
        );
        assert!(next_ack(ack).is_none());
    }
}

/// Scenario: Graceful shutdown latches with a full named/default route and buffered input, after which capacity returns before the drain deadline.
/// Guarantees: All three signal types retry and rearm while draining, deliver both caller identities exactly once, and finish without shutdown NACKs or a deadline wait.
#[tokio::test(flavor = "current_thread")]
async fn graceful_shutdown_drains_full_routes_after_capacity_returns() {
    for (signal, port) in SIGNALS {
        for default in [false, true] {
            for full_retries in [0, 1, 3] {
                let clock = SimClock::new();
                let _clock_guard = clock.install();
                let mut fixture = Fixture::new(signal, port, default).await;
                fixture
                    .input
                    .as_ref()
                    .unwrap()
                    .send(data(signal, Some(11)))
                    .unwrap();
                fixture
                    .input
                    .as_ref()
                    .unwrap()
                    .send(data(signal, Some(12)))
                    .unwrap();
                let message = fixture.receive();
                fixture.process(message);
                assert!(!fixture.processor.accept_pdata());
                let deadline = clock.now() + Duration::from_secs(10);
                fixture.latch_shutdown(deadline);
                for _ in 0..full_retries {
                    clock.advance(PROBE_INTERVAL);
                    let message = fixture.receive();
                    assert!(matches!(
                        message,
                        Message::Control(NodeControlMsg::Wakeup { .. })
                    ));
                    fixture.process(message);
                    assert!(!fixture.processor.accept_pdata());
                    assert!(matches!(
                        fixture.completion.try_recv(),
                        Err(RecvError::Empty)
                    ));
                }
                let prefill = fixture.output.try_recv().unwrap();
                assert!(!prefill.has_context_frames());
                clock.advance(PROBE_INTERVAL);
                let message = fixture.receive();
                assert!(matches!(
                    message,
                    Message::Control(NodeControlMsg::Wakeup { .. })
                ));
                fixture.process(message);
                assert!(fixture.processor.accept_pdata());
                // Keep message 11 in the output so buffered message 12 must
                // park and schedule a fresh retry after shutdown has latched.
                let message = fixture.receive();
                assert!(matches!(message, Message::PData(_)));
                fixture.process(message);
                assert!(!fixture.processor.accept_pdata());
                // Upstream is done, but the locally parked second message
                // still has time to recover before the graceful deadline.
                drop(fixture.input.take());
                fixture.assert_forwarded(signal, 11);
                clock.advance(PROBE_INTERVAL);
                let message = fixture.receive();
                assert!(matches!(
                    message,
                    Message::Control(NodeControlMsg::Wakeup { .. })
                ));
                fixture.process(message);
                fixture.assert_forwarded(signal, 12);
                assert!(fixture.processor.accept_pdata());
                let shutdown = fixture.receive();
                assert!(
                    matches!(&shutdown, Message::Control(NodeControlMsg::Shutdown { reason, .. }) if reason == "graceful regression")
                );
                fixture.process(shutdown);
                assert!(clock.now() < deadline);
                assert!(matches!(
                    fixture.completion.try_recv(),
                    Err(RecvError::Empty)
                ));
                assert!(fixture.handler.pop_wakeup().is_none());
                assert_eq!(
                    fixture.handler.set_wakeup(WakeupSlot(0), clock.now()),
                    Err(WakeupError::ShuttingDown)
                );
                assert!(matches!(
                    ready(fixture.inbox.recv_when(false)),
                    Err(RecvError::Closed)
                ));
            }
        }
    }
}

/// Scenario: A named/default route stays full until the graceful deadline while a wakeup and buffered input are pending.
/// Guarantees: The deadline wins over the due wakeup, parked work receives exactly one retryable NodeShutdown NACK, and no new wakeups are admitted after termination.
#[tokio::test(flavor = "current_thread")]
async fn graceful_shutdown_deadline_nacks_parked_work_once() {
    for (signal, port) in SIGNALS {
        for default in [false, true] {
            let clock = SimClock::new();
            let _clock_guard = clock.install();
            let mut fixture = Fixture::new(signal, port, default).await;
            fixture
                .input
                .as_ref()
                .unwrap()
                .send(data(signal, Some(21)))
                .unwrap();
            fixture
                .input
                .as_ref()
                .unwrap()
                .send(data(signal, Some(22)))
                .unwrap();
            let message = fixture.receive();
            fixture.process(message);
            let deadline = clock.now() + Duration::from_secs(10);
            fixture.latch_shutdown(deadline);
            clock.advance_to(deadline);
            let shutdown = fixture.receive();
            assert!(
                shutdown.is_shutdown(),
                "shutdown deadline must take priority over a due retry"
            );
            fixture.process(shutdown);
            let PipelineCompletionMsg::DeliverNack { nack } =
                fixture.completion.try_recv().unwrap()
            else {
                panic!("parked work must produce a NACK")
            };
            assert_eq!(nack.cause, NackCause::NodeShutdown);
            assert!(!nack.permanent);
            let (caller, nack) = next_nack(nack).unwrap();
            assert_eq!(caller, 21);
            assert_eq!(nack.refused.signal_type(), signal);
            assert!(next_nack(nack).is_none());
            assert!(matches!(
                fixture.completion.try_recv(),
                Err(RecvError::Empty)
            ));
            assert!(fixture.processor.accept_pdata());
            assert!(fixture.handler.pop_wakeup().is_none());
            assert_eq!(
                fixture.handler.set_wakeup(WakeupSlot(0), clock.now()),
                Err(WakeupError::ShuttingDown)
            );
            assert!(matches!(
                ready(fixture.inbox.recv_when(false)),
                Err(RecvError::Closed)
            ));
        }
    }
}
