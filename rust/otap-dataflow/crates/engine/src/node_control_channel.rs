// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Engine adapters for the standalone control-channel crate.

use crate::channel_metrics::{
    CHANNEL_IMPL_INTERNAL, CHANNEL_KIND_CONTROL, CHANNEL_MODE_LOCAL, CHANNEL_MODE_SHARED,
    CHANNEL_TYPE_MPSC, control_channel_id,
};
use crate::context::PipelineContext;
use crate::control::{AckMsg, NackMsg, NodeControlMsg, UnwindData};
use crate::entity_context::current_node_telemetry_handle;
use crate::node::NodeId;
use otap_df_channel::error::SendError;
use otap_df_control_channel::local::{
    LocalNodeControlReceiver as RawLocalNodeControlReceiver,
    LocalNodeControlSender as RawLocalNodeControlSender,
    LocalReceiverControlReceiver as RawLocalReceiverControlReceiver,
    LocalReceiverControlSender as RawLocalReceiverControlSender,
};
use otap_df_control_channel::shared::{
    SharedNodeControlReceiver as RawSharedNodeControlReceiver,
    SharedNodeControlSender as RawSharedNodeControlSender,
    SharedReceiverControlReceiver as RawSharedReceiverControlReceiver,
    SharedReceiverControlSender as RawSharedReceiverControlSender,
};
use otap_df_control_channel::{
    AckMsg as ChannelAckMsg, CompletionMsg as ChannelCompletionMsg, ControlChannelConfig,
    ControlChannelStats, ControlCmd, LifecycleSendResult, NackMsg as ChannelNackMsg,
    NodeControlEvent as ChannelNodeControlEvent, Phase,
    ReceiverControlEvent as ChannelReceiverControlEvent, local, shared,
};
use otap_df_telemetry::error::Error as TelemetryError;
use otap_df_telemetry::instrument::{Counter, Gauge};
use otap_df_telemetry::metrics::MetricSet;
use otap_df_telemetry::reporter::MetricsReporter;
use otap_df_telemetry_macros::metric_set;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

fn control_channel_config(capacity: usize) -> ControlChannelConfig {
    ControlChannelConfig {
        completion_msg_capacity: capacity,
        ..ControlChannelConfig::default()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CompletionPayload<PData> {
    pdata: PData,
    unwind: UnwindData,
}

fn ack_to_channel<PData>(ack: AckMsg<PData>) -> ChannelAckMsg<CompletionPayload<PData>> {
    let AckMsg { accepted, unwind } = ack;
    ChannelAckMsg::new(CompletionPayload {
        pdata: *accepted,
        unwind,
    })
}

fn nack_to_channel<PData>(nack: NackMsg<PData>) -> ChannelNackMsg<CompletionPayload<PData>> {
    let NackMsg {
        reason,
        refused,
        unwind,
        permanent,
    } = nack;

    let payload = CompletionPayload {
        pdata: *refused,
        unwind,
    };

    if permanent {
        ChannelNackMsg::new_permanent(reason, payload)
    } else {
        ChannelNackMsg::new(reason, payload)
    }
}

fn completion_to_node_control<PData>(
    completion: ChannelCompletionMsg<CompletionPayload<PData>>,
) -> NodeControlMsg<PData> {
    match completion {
        ChannelCompletionMsg::Ack(ack) => {
            let CompletionPayload { pdata, unwind } = *ack.accepted;
            NodeControlMsg::Ack(AckMsg {
                accepted: Box::new(pdata),
                unwind,
            })
        }
        ChannelCompletionMsg::Nack(nack) => {
            let CompletionPayload { pdata, unwind } = *nack.refused;
            NodeControlMsg::Nack(NackMsg {
                reason: nack.reason,
                refused: Box::new(pdata),
                unwind,
                permanent: nack.permanent,
            })
        }
    }
}

pub(crate) fn control_stats_is_empty(stats: &ControlChannelStats) -> bool {
    !stats.has_pending_drain_ingress
        && !stats.has_pending_shutdown
        && stats.completion_len == 0
        && !stats.has_pending_config
        && !stats.has_pending_timer_tick
        && !stats.has_pending_collect_telemetry
}

pub(crate) fn push_receiver_event<PData>(
    event: ChannelReceiverControlEvent<CompletionPayload<PData>>,
    pending: &mut VecDeque<NodeControlMsg<PData>>,
    metrics_reporter: &MetricsReporter,
) {
    match event {
        ChannelReceiverControlEvent::DrainIngress(msg) => {
            pending.push_back(NodeControlMsg::DrainIngress {
                deadline: msg.deadline,
                reason: msg.reason,
            });
        }
        ChannelReceiverControlEvent::CompletionBatch(batch) => {
            pending.extend(batch.into_iter().map(completion_to_node_control));
        }
        ChannelReceiverControlEvent::Config { config } => {
            pending.push_back(NodeControlMsg::Config { config });
        }
        ChannelReceiverControlEvent::TimerTick => {
            pending.push_back(NodeControlMsg::TimerTick {});
        }
        ChannelReceiverControlEvent::CollectTelemetry => {
            pending.push_back(NodeControlMsg::CollectTelemetry {
                metrics_reporter: metrics_reporter.clone(),
            });
        }
        ChannelReceiverControlEvent::Shutdown(msg) => {
            pending.push_back(NodeControlMsg::Shutdown {
                deadline: msg.deadline,
                reason: msg.reason,
            });
        }
    }
}

pub(crate) fn push_node_event<PData>(
    event: ChannelNodeControlEvent<CompletionPayload<PData>>,
    pending: &mut VecDeque<NodeControlMsg<PData>>,
    metrics_reporter: &MetricsReporter,
) {
    match event {
        ChannelNodeControlEvent::CompletionBatch(batch) => {
            pending.extend(batch.into_iter().map(completion_to_node_control));
        }
        ChannelNodeControlEvent::Config { config } => {
            pending.push_back(NodeControlMsg::Config { config });
        }
        ChannelNodeControlEvent::TimerTick => {
            pending.push_back(NodeControlMsg::TimerTick {});
        }
        ChannelNodeControlEvent::CollectTelemetry => {
            pending.push_back(NodeControlMsg::CollectTelemetry {
                metrics_reporter: metrics_reporter.clone(),
            });
        }
        ChannelNodeControlEvent::Shutdown(msg) => {
            pending.push_back(NodeControlMsg::Shutdown {
                deadline: msg.deadline,
                reason: msg.reason,
            });
        }
    }
}

#[metric_set(name = "channel.control")]
#[derive(Debug, Default, Clone)]
pub(crate) struct ControlChannelMetrics {
    #[metric(name = "phase", unit = "{state}")]
    phase: Gauge<u64>,
    #[metric(name = "drain_ingress.recorded", unit = "{1}")]
    drain_ingress_recorded: Gauge<u64>,
    #[metric(name = "shutdown.recorded", unit = "{1}")]
    shutdown_recorded: Gauge<u64>,
    #[metric(name = "drain_ingress.pending", unit = "{1}")]
    pending_drain_ingress: Gauge<u64>,
    #[metric(name = "shutdown.pending", unit = "{1}")]
    pending_shutdown: Gauge<u64>,
    #[metric(name = "completion.len", unit = "{message}")]
    completion_len: Gauge<u64>,
    #[metric(name = "config.pending", unit = "{1}")]
    pending_config: Gauge<u64>,
    #[metric(name = "timer_tick.pending", unit = "{1}")]
    pending_timer_tick: Gauge<u64>,
    #[metric(name = "collect_telemetry.pending", unit = "{1}")]
    pending_collect_telemetry: Gauge<u64>,
    #[metric(name = "completion_burst.len", unit = "{message}")]
    completion_burst_len: Gauge<u64>,
    #[metric(name = "completion_batch.emitted", unit = "{batch}")]
    completion_batch_emitted: Counter<u64>,
    #[metric(name = "completion_message.emitted", unit = "{message}")]
    completion_message_emitted: Counter<u64>,
    #[metric(name = "config.replaced", unit = "{message}")]
    config_replaced: Counter<u64>,
    #[metric(name = "timer_tick.coalesced", unit = "{message}")]
    timer_tick_coalesced: Counter<u64>,
    #[metric(name = "collect_telemetry.coalesced", unit = "{message}")]
    collect_telemetry_coalesced: Counter<u64>,
    #[metric(name = "normal_event.dropped_during_drain", unit = "{message}")]
    normal_event_dropped_during_drain: Counter<u64>,
    #[metric(name = "completion.abandoned_on_forced_shutdown", unit = "{message}")]
    completion_abandoned_on_forced_shutdown: Counter<u64>,
    #[metric(name = "shutdown.forced", unit = "{1}")]
    shutdown_forced: Gauge<u64>,
    #[metric(name = "closed", unit = "{1}")]
    closed: Gauge<u64>,
}

struct ControlChannelMetricsState {
    metrics: MetricSet<ControlChannelMetrics>,
    last_stats: Option<ControlChannelStats>,
}

impl ControlChannelMetricsState {
    fn new(metrics: MetricSet<ControlChannelMetrics>) -> Self {
        Self {
            metrics,
            last_stats: None,
        }
    }

    fn report(
        &mut self,
        stats: ControlChannelStats,
        metrics_reporter: &mut MetricsReporter,
    ) -> Result<(), TelemetryError> {
        self.metrics.phase.set(match stats.phase {
            Phase::Normal => 0,
            Phase::IngressDrainRecorded => 1,
            Phase::ShutdownRecorded => 2,
        });
        self.metrics
            .drain_ingress_recorded
            .set(u64::from(stats.drain_ingress_recorded));
        self.metrics
            .shutdown_recorded
            .set(u64::from(stats.shutdown_recorded));
        self.metrics
            .pending_drain_ingress
            .set(u64::from(stats.has_pending_drain_ingress));
        self.metrics
            .pending_shutdown
            .set(u64::from(stats.has_pending_shutdown));
        self.metrics.completion_len.set(stats.completion_len as u64);
        self.metrics
            .pending_config
            .set(u64::from(stats.has_pending_config));
        self.metrics
            .pending_timer_tick
            .set(u64::from(stats.has_pending_timer_tick));
        self.metrics
            .pending_collect_telemetry
            .set(u64::from(stats.has_pending_collect_telemetry));
        self.metrics
            .completion_burst_len
            .set(stats.completion_burst_len as u64);
        self.metrics
            .shutdown_forced
            .set(u64::from(stats.shutdown_forced));
        self.metrics.closed.set(u64::from(stats.closed));

        let previous = self.last_stats.as_ref();
        self.metrics.completion_batch_emitted.add(
            stats
                .completion_batch_emitted_total
                .saturating_sub(previous.map_or(0, |prev| prev.completion_batch_emitted_total)),
        );
        self.metrics.completion_message_emitted.add(
            stats
                .completion_message_emitted_total
                .saturating_sub(previous.map_or(0, |prev| prev.completion_message_emitted_total)),
        );
        self.metrics.config_replaced.add(
            stats
                .config_replaced_total
                .saturating_sub(previous.map_or(0, |prev| prev.config_replaced_total)),
        );
        self.metrics.timer_tick_coalesced.add(
            stats
                .timer_tick_coalesced_total
                .saturating_sub(previous.map_or(0, |prev| prev.timer_tick_coalesced_total)),
        );
        self.metrics.collect_telemetry_coalesced.add(
            stats.collect_telemetry_coalesced_total.saturating_sub(
                previous.map_or(0, |prev| prev.collect_telemetry_coalesced_total),
            ),
        );
        self.metrics.normal_event_dropped_during_drain.add(
            stats.normal_event_dropped_during_drain_total.saturating_sub(
                previous.map_or(0, |prev| prev.normal_event_dropped_during_drain_total),
            ),
        );
        self.metrics.completion_abandoned_on_forced_shutdown.add(
            stats.completion_abandoned_on_forced_shutdown_total.saturating_sub(
                previous.map_or(0, |prev| prev.completion_abandoned_on_forced_shutdown_total),
            ),
        );

        self.last_stats = Some(stats);
        metrics_reporter.report(&mut self.metrics)
    }
}

type ControlChannelMetricsHandle = Arc<Mutex<ControlChannelMetricsState>>;

fn register_control_channel_metrics(
    node_id: &NodeId,
    pipeline_ctx: &PipelineContext,
    channel_mode: &'static str,
    enabled: bool,
) -> Option<ControlChannelMetricsHandle> {
    let channel_entity_key = pipeline_ctx.register_channel_entity(
        control_channel_id(node_id),
        "input".into(),
        CHANNEL_KIND_CONTROL,
        channel_mode,
        CHANNEL_TYPE_MPSC,
        CHANNEL_IMPL_INTERNAL,
    );

    if let Some(telemetry) = current_node_telemetry_handle() {
        telemetry.set_control_channel_key(channel_entity_key);
        if enabled {
            let metrics =
                pipeline_ctx.register_metric_set_for_entity::<ControlChannelMetrics>(channel_entity_key);
            telemetry.track_metric_set(metrics.metric_set_key());
            return Some(Arc::new(Mutex::new(ControlChannelMetricsState::new(metrics))));
        }
    }

    None
}

fn unsupported_message_error<PData>(msg: NodeControlMsg<PData>) -> SendError<NodeControlMsg<PData>> {
    SendError::Closed(msg)
}

fn control_cmd_to_node_control<PData>(
    cmd: ControlCmd<CompletionPayload<PData>>,
    collect_telemetry_reporter: Option<MetricsReporter>,
) -> NodeControlMsg<PData> {
    match cmd {
        ControlCmd::Ack(ack) => completion_to_node_control(ChannelCompletionMsg::Ack(ack)),
        ControlCmd::Nack(nack) => completion_to_node_control(ChannelCompletionMsg::Nack(nack)),
        ControlCmd::Config { config } => NodeControlMsg::Config { config },
        ControlCmd::TimerTick => NodeControlMsg::TimerTick {},
        ControlCmd::CollectTelemetry => NodeControlMsg::CollectTelemetry {
            metrics_reporter: collect_telemetry_reporter
                .expect("collect telemetry conversion requires a metrics reporter"),
        },
    }
}

fn map_try_send_error<PData>(
    err: otap_df_control_channel::TrySendError<CompletionPayload<PData>>,
    collect_telemetry_reporter: Option<MetricsReporter>,
) -> SendError<NodeControlMsg<PData>> {
    match err {
        otap_df_control_channel::TrySendError::Closed(cmd) => {
            SendError::Closed(control_cmd_to_node_control(cmd, collect_telemetry_reporter))
        }
        otap_df_control_channel::TrySendError::Full { cmd, .. } => {
            SendError::Full(control_cmd_to_node_control(cmd, collect_telemetry_reporter))
        }
    }
}

fn map_send_error<PData>(
    err: otap_df_control_channel::SendError<CompletionPayload<PData>>,
    collect_telemetry_reporter: Option<MetricsReporter>,
) -> SendError<NodeControlMsg<PData>> {
    match err {
        otap_df_control_channel::SendError::Closed(cmd) => {
            SendError::Closed(control_cmd_to_node_control(cmd, collect_telemetry_reporter))
        }
    }
}

pub struct LocalReceiverControlSender<PData> {
    inner: RawLocalReceiverControlSender<CompletionPayload<PData>>,
    metrics: Option<ControlChannelMetricsHandle>,
}

pub struct SharedReceiverControlSender<PData> {
    inner: RawSharedReceiverControlSender<CompletionPayload<PData>>,
    metrics: Option<ControlChannelMetricsHandle>,
}

pub struct LocalNodeControlSender<PData> {
    inner: RawLocalNodeControlSender<CompletionPayload<PData>>,
    metrics: Option<ControlChannelMetricsHandle>,
}

pub struct SharedNodeControlSender<PData> {
    inner: RawSharedNodeControlSender<CompletionPayload<PData>>,
    metrics: Option<ControlChannelMetricsHandle>,
}

impl<PData> Clone for LocalReceiverControlSender<PData> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<PData> Clone for SharedReceiverControlSender<PData> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<PData> Clone for LocalNodeControlSender<PData> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<PData> Clone for SharedNodeControlSender<PData> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

pub(crate) struct LocalReceiverControlReceiver<PData> {
    inner: RawLocalReceiverControlReceiver<CompletionPayload<PData>>,
}

pub(crate) struct SharedReceiverControlReceiver<PData> {
    inner: RawSharedReceiverControlReceiver<CompletionPayload<PData>>,
}

pub(crate) struct LocalNodeControlReceiver<PData> {
    inner: RawLocalNodeControlReceiver<CompletionPayload<PData>>,
}

pub(crate) struct SharedNodeControlReceiver<PData> {
    inner: RawSharedNodeControlReceiver<CompletionPayload<PData>>,
}

macro_rules! impl_receiver_sender {
    ($ty:ident) => {
        impl<PData> $ty<PData> {
            fn with_metrics(
                mut self,
                metrics: Option<ControlChannelMetricsHandle>,
            ) -> Self {
                self.metrics = metrics;
                self
            }

            #[must_use]
            pub fn accept_drain_ingress(
                &self,
                deadline: Instant,
                reason: String,
            ) -> LifecycleSendResult {
                self.inner
                    .accept_drain_ingress(otap_df_control_channel::DrainIngressMsg { deadline, reason })
            }

            #[must_use]
            pub fn accept_shutdown(&self, deadline: Instant, reason: String) -> LifecycleSendResult {
                self.inner
                    .accept_shutdown(otap_df_control_channel::ShutdownMsg { deadline, reason })
            }

            pub async fn send(
                &self,
                msg: NodeControlMsg<PData>,
            ) -> Result<(), SendError<NodeControlMsg<PData>>> {
                match msg {
                    NodeControlMsg::Ack(ack) => self
                        .inner
                        .send(ControlCmd::Ack(ack_to_channel(ack)))
                        .await
                        .map(|_| ())
                        .map_err(|err| map_send_error(err, None)),
                    NodeControlMsg::Nack(nack) => self
                        .inner
                        .send(ControlCmd::Nack(nack_to_channel(nack)))
                        .await
                        .map(|_| ())
                        .map_err(|err| map_send_error(err, None)),
                    NodeControlMsg::Config { config } => self
                        .inner
                        .send(ControlCmd::Config { config })
                        .await
                        .map(|_| ())
                        .map_err(|err| map_send_error(err, None)),
                    NodeControlMsg::TimerTick {} => self
                        .inner
                        .send(ControlCmd::TimerTick)
                        .await
                        .map(|_| ())
                        .map_err(|err| map_send_error(err, None)),
                    NodeControlMsg::CollectTelemetry { metrics_reporter } => self
                        .inner
                        .send(ControlCmd::CollectTelemetry)
                        .await
                        .map(|_| ())
                        .map_err(|err| map_send_error(err, Some(metrics_reporter))),
                    NodeControlMsg::DrainIngress { deadline, reason } => {
                        match self.accept_drain_ingress(deadline, reason.clone()) {
                            LifecycleSendResult::Accepted | LifecycleSendResult::AlreadyAccepted => Ok(()),
                            LifecycleSendResult::Closed => {
                                Err(SendError::Closed(NodeControlMsg::DrainIngress { deadline, reason }))
                            }
                        }
                    }
                    NodeControlMsg::Shutdown { deadline, reason } => {
                        match self.accept_shutdown(deadline, reason.clone()) {
                            LifecycleSendResult::Accepted | LifecycleSendResult::AlreadyAccepted => Ok(()),
                            LifecycleSendResult::Closed => {
                                Err(SendError::Closed(NodeControlMsg::Shutdown { deadline, reason }))
                            }
                        }
                    }
                    other => Err(unsupported_message_error(other)),
                }
            }

            pub fn try_send(
                &self,
                msg: NodeControlMsg<PData>,
            ) -> Result<(), SendError<NodeControlMsg<PData>>> {
                match msg {
                    NodeControlMsg::Ack(ack) => self
                        .inner
                        .try_send(ControlCmd::Ack(ack_to_channel(ack)))
                        .map(|_| ())
                        .map_err(|err| map_try_send_error(err, None)),
                    NodeControlMsg::Nack(nack) => self
                        .inner
                        .try_send(ControlCmd::Nack(nack_to_channel(nack)))
                        .map(|_| ())
                        .map_err(|err| map_try_send_error(err, None)),
                    NodeControlMsg::Config { config } => self
                        .inner
                        .try_send(ControlCmd::Config { config })
                        .map(|_| ())
                        .map_err(|err| map_try_send_error(err, None)),
                    NodeControlMsg::TimerTick {} => self
                        .inner
                        .try_send(ControlCmd::TimerTick)
                        .map(|_| ())
                        .map_err(|err| map_try_send_error(err, None)),
                    NodeControlMsg::CollectTelemetry { metrics_reporter } => self
                        .inner
                        .try_send(ControlCmd::CollectTelemetry)
                        .map(|_| ())
                        .map_err(|err| map_try_send_error(err, Some(metrics_reporter))),
                    NodeControlMsg::DrainIngress { deadline, reason } => {
                        match self.accept_drain_ingress(deadline, reason.clone()) {
                            LifecycleSendResult::Accepted | LifecycleSendResult::AlreadyAccepted => Ok(()),
                            LifecycleSendResult::Closed => {
                                Err(SendError::Closed(NodeControlMsg::DrainIngress { deadline, reason }))
                            }
                        }
                    }
                    NodeControlMsg::Shutdown { deadline, reason } => {
                        match self.accept_shutdown(deadline, reason.clone()) {
                            LifecycleSendResult::Accepted | LifecycleSendResult::AlreadyAccepted => Ok(()),
                            LifecycleSendResult::Closed => {
                                Err(SendError::Closed(NodeControlMsg::Shutdown { deadline, reason }))
                            }
                        }
                    }
                    other => Err(unsupported_message_error(other)),
                }
            }

            pub(crate) fn stats(&self) -> ControlChannelStats {
                self.inner.stats()
            }

            pub(crate) fn report_metrics(
                &self,
                metrics_reporter: &mut MetricsReporter,
            ) -> Result<(), TelemetryError> {
                if let Some(metrics) = &self.metrics {
                    metrics.lock().report(self.stats(), metrics_reporter)?;
                }
                Ok(())
            }
        }
    };
}

macro_rules! impl_node_sender {
    ($ty:ident) => {
        impl<PData> $ty<PData> {
            fn with_metrics(
                mut self,
                metrics: Option<ControlChannelMetricsHandle>,
            ) -> Self {
                self.metrics = metrics;
                self
            }

            #[must_use]
            pub fn accept_shutdown(&self, deadline: Instant, reason: String) -> LifecycleSendResult {
                self.inner
                    .accept_shutdown(otap_df_control_channel::ShutdownMsg { deadline, reason })
            }

            pub async fn send(
                &self,
                msg: NodeControlMsg<PData>,
            ) -> Result<(), SendError<NodeControlMsg<PData>>> {
                match msg {
                    NodeControlMsg::Ack(ack) => self
                        .inner
                        .send(ControlCmd::Ack(ack_to_channel(ack)))
                        .await
                        .map(|_| ())
                        .map_err(|err| map_send_error(err, None)),
                    NodeControlMsg::Nack(nack) => self
                        .inner
                        .send(ControlCmd::Nack(nack_to_channel(nack)))
                        .await
                        .map(|_| ())
                        .map_err(|err| map_send_error(err, None)),
                    NodeControlMsg::Config { config } => self
                        .inner
                        .send(ControlCmd::Config { config })
                        .await
                        .map(|_| ())
                        .map_err(|err| map_send_error(err, None)),
                    NodeControlMsg::TimerTick {} => self
                        .inner
                        .send(ControlCmd::TimerTick)
                        .await
                        .map(|_| ())
                        .map_err(|err| map_send_error(err, None)),
                    NodeControlMsg::CollectTelemetry { metrics_reporter } => self
                        .inner
                        .send(ControlCmd::CollectTelemetry)
                        .await
                        .map(|_| ())
                        .map_err(|err| map_send_error(err, Some(metrics_reporter))),
                    NodeControlMsg::Shutdown { deadline, reason } => {
                        match self.accept_shutdown(deadline, reason.clone()) {
                            LifecycleSendResult::Accepted | LifecycleSendResult::AlreadyAccepted => Ok(()),
                            LifecycleSendResult::Closed => {
                                Err(SendError::Closed(NodeControlMsg::Shutdown { deadline, reason }))
                            }
                        }
                    }
                    other => Err(unsupported_message_error(other)),
                }
            }

            pub fn try_send(
                &self,
                msg: NodeControlMsg<PData>,
            ) -> Result<(), SendError<NodeControlMsg<PData>>> {
                match msg {
                    NodeControlMsg::Ack(ack) => self
                        .inner
                        .try_send(ControlCmd::Ack(ack_to_channel(ack)))
                        .map(|_| ())
                        .map_err(|err| map_try_send_error(err, None)),
                    NodeControlMsg::Nack(nack) => self
                        .inner
                        .try_send(ControlCmd::Nack(nack_to_channel(nack)))
                        .map(|_| ())
                        .map_err(|err| map_try_send_error(err, None)),
                    NodeControlMsg::Config { config } => self
                        .inner
                        .try_send(ControlCmd::Config { config })
                        .map(|_| ())
                        .map_err(|err| map_try_send_error(err, None)),
                    NodeControlMsg::TimerTick {} => self
                        .inner
                        .try_send(ControlCmd::TimerTick)
                        .map(|_| ())
                        .map_err(|err| map_try_send_error(err, None)),
                    NodeControlMsg::CollectTelemetry { metrics_reporter } => self
                        .inner
                        .try_send(ControlCmd::CollectTelemetry)
                        .map(|_| ())
                        .map_err(|err| map_try_send_error(err, Some(metrics_reporter))),
                    NodeControlMsg::Shutdown { deadline, reason } => {
                        match self.accept_shutdown(deadline, reason.clone()) {
                            LifecycleSendResult::Accepted | LifecycleSendResult::AlreadyAccepted => Ok(()),
                            LifecycleSendResult::Closed => {
                                Err(SendError::Closed(NodeControlMsg::Shutdown { deadline, reason }))
                            }
                        }
                    }
                    other => Err(unsupported_message_error(other)),
                }
            }

            pub(crate) fn stats(&self) -> ControlChannelStats {
                self.inner.stats()
            }

            pub(crate) fn report_metrics(
                &self,
                metrics_reporter: &mut MetricsReporter,
            ) -> Result<(), TelemetryError> {
                if let Some(metrics) = &self.metrics {
                    metrics.lock().report(self.stats(), metrics_reporter)?;
                }
                Ok(())
            }
        }
    };
}

impl_receiver_sender!(LocalReceiverControlSender);
impl_receiver_sender!(SharedReceiverControlSender);
impl_node_sender!(LocalNodeControlSender);
impl_node_sender!(SharedNodeControlSender);

impl<PData> LocalReceiverControlReceiver<PData> {
    pub(crate) async fn recv_event(
        &mut self,
    ) -> Option<ChannelReceiverControlEvent<CompletionPayload<PData>>> {
        self.inner.recv().await
    }
}

impl<PData> SharedReceiverControlReceiver<PData> {
    pub(crate) async fn recv_event(
        &mut self,
    ) -> Option<ChannelReceiverControlEvent<CompletionPayload<PData>>> {
        self.inner.recv().await
    }
}

impl<PData> LocalNodeControlReceiver<PData> {
    pub(crate) async fn recv_event(
        &mut self,
    ) -> Option<ChannelNodeControlEvent<CompletionPayload<PData>>> {
        self.inner.recv().await
    }

    pub(crate) fn try_recv_event(
        &mut self,
    ) -> Option<ChannelNodeControlEvent<CompletionPayload<PData>>> {
        self.inner.try_recv()
    }

    pub(crate) fn stats(&self) -> ControlChannelStats {
        self.inner.stats()
    }
}

impl<PData> SharedNodeControlReceiver<PData> {
    pub(crate) async fn recv_event(
        &mut self,
    ) -> Option<ChannelNodeControlEvent<CompletionPayload<PData>>> {
        self.inner.recv().await
    }

    pub(crate) fn try_recv_event(
        &mut self,
    ) -> Option<ChannelNodeControlEvent<CompletionPayload<PData>>> {
        self.inner.try_recv()
    }

    pub(crate) fn stats(&self) -> ControlChannelStats {
        self.inner.stats()
    }
}

/// Role-specific control sender for receivers.
///
/// Receivers accept both ordinary node control traffic and the receiver-only
/// `DrainIngress` lifecycle token.
pub enum ReceiverControlSender<PData> {
    /// Local receiver control sender.
    Local(LocalReceiverControlSender<PData>),
    /// Shared receiver control sender.
    Shared(SharedReceiverControlSender<PData>),
}

/// Role-specific control sender for processors and exporters.
///
/// Non-receiver nodes accept ordinary node control traffic plus `Shutdown`, but
/// not receiver-only `DrainIngress`.
pub enum NodeControlSender<PData> {
    /// Local node control sender.
    Local(LocalNodeControlSender<PData>),
    /// Shared node control sender.
    Shared(SharedNodeControlSender<PData>),
}

impl<PData> Clone for ReceiverControlSender<PData> {
    fn clone(&self) -> Self {
        match self {
            Self::Local(sender) => Self::Local(sender.clone()),
            Self::Shared(sender) => Self::Shared(sender.clone()),
        }
    }
}

impl<PData> Clone for NodeControlSender<PData> {
    fn clone(&self) -> Self {
        match self {
            Self::Local(sender) => Self::Local(sender.clone()),
            Self::Shared(sender) => Self::Shared(sender.clone()),
        }
    }
}

impl<PData> ReceiverControlSender<PData> {
    /// Accepts the receiver-only ingress-drain lifecycle token.
    #[must_use]
    pub fn accept_drain_ingress(&self, deadline: Instant, reason: String) -> LifecycleSendResult {
        match self {
            Self::Local(sender) => sender.accept_drain_ingress(deadline, reason),
            Self::Shared(sender) => sender.accept_drain_ingress(deadline, reason),
        }
    }

    /// Accepts the shutdown lifecycle token.
    #[must_use]
    pub fn accept_shutdown(&self, deadline: Instant, reason: String) -> LifecycleSendResult {
        match self {
            Self::Local(sender) => sender.accept_shutdown(deadline, reason),
            Self::Shared(sender) => sender.accept_shutdown(deadline, reason),
        }
    }

    /// Sends a control message using receiver-role semantics.
    pub async fn send(
        &self,
        msg: NodeControlMsg<PData>,
    ) -> Result<(), SendError<NodeControlMsg<PData>>> {
        match self {
            Self::Local(sender) => sender.send(msg).await,
            Self::Shared(sender) => sender.send(msg).await,
        }
    }

    /// Attempts to send a control message without awaiting bounded capacity.
    pub fn try_send(
        &self,
        msg: NodeControlMsg<PData>,
    ) -> Result<(), SendError<NodeControlMsg<PData>>> {
        match self {
            Self::Local(sender) => sender.try_send(msg),
            Self::Shared(sender) => sender.try_send(msg),
        }
    }

    pub(crate) fn report_metrics(
        &self,
        metrics_reporter: &mut MetricsReporter,
    ) -> Result<(), TelemetryError> {
        match self {
            Self::Local(sender) => sender.report_metrics(metrics_reporter),
            Self::Shared(sender) => sender.report_metrics(metrics_reporter),
        }
    }
}

impl<PData> NodeControlSender<PData> {
    /// Accepts the shutdown lifecycle token.
    #[must_use]
    pub fn accept_shutdown(&self, deadline: Instant, reason: String) -> LifecycleSendResult {
        match self {
            Self::Local(sender) => sender.accept_shutdown(deadline, reason),
            Self::Shared(sender) => sender.accept_shutdown(deadline, reason),
        }
    }

    /// Sends a control message using non-receiver node semantics.
    pub async fn send(
        &self,
        msg: NodeControlMsg<PData>,
    ) -> Result<(), SendError<NodeControlMsg<PData>>> {
        match self {
            Self::Local(sender) => sender.send(msg).await,
            Self::Shared(sender) => sender.send(msg).await,
        }
    }

    /// Attempts to send a control message without awaiting bounded capacity.
    pub fn try_send(
        &self,
        msg: NodeControlMsg<PData>,
    ) -> Result<(), SendError<NodeControlMsg<PData>>> {
        match self {
            Self::Local(sender) => sender.try_send(msg),
            Self::Shared(sender) => sender.try_send(msg),
        }
    }

    pub(crate) fn report_metrics(
        &self,
        metrics_reporter: &mut MetricsReporter,
    ) -> Result<(), TelemetryError> {
        match self {
            Self::Local(sender) => sender.report_metrics(metrics_reporter),
            Self::Shared(sender) => sender.report_metrics(metrics_reporter),
        }
    }
}

pub(crate) fn local_receiver_channel<PData>(
    capacity: usize,
) -> (
    LocalReceiverControlSender<PData>,
    LocalReceiverControlReceiver<PData>,
) {
    let (sender, receiver) =
        local::receiver_channel(control_channel_config(capacity)).expect("control config must be valid");
    (
        LocalReceiverControlSender {
            inner: sender,
            metrics: None,
        },
        LocalReceiverControlReceiver { inner: receiver },
    )
}

pub(crate) fn shared_receiver_channel<PData>(
    capacity: usize,
) -> (
    SharedReceiverControlSender<PData>,
    SharedReceiverControlReceiver<PData>,
) {
    let (sender, receiver) =
        shared::receiver_channel(control_channel_config(capacity)).expect("control config must be valid");
    (
        SharedReceiverControlSender {
            inner: sender,
            metrics: None,
        },
        SharedReceiverControlReceiver { inner: receiver },
    )
}

pub(crate) fn local_node_channel<PData>(
    capacity: usize,
) -> (LocalNodeControlSender<PData>, LocalNodeControlReceiver<PData>) {
    let (sender, receiver) =
        local::node_channel(control_channel_config(capacity)).expect("control config must be valid");
    (
        LocalNodeControlSender {
            inner: sender,
            metrics: None,
        },
        LocalNodeControlReceiver { inner: receiver },
    )
}

pub(crate) fn shared_node_channel<PData>(
    capacity: usize,
) -> (SharedNodeControlSender<PData>, SharedNodeControlReceiver<PData>) {
    let (sender, receiver) =
        shared::node_channel(control_channel_config(capacity)).expect("control config must be valid");
    (
        SharedNodeControlSender {
            inner: sender,
            metrics: None,
        },
        SharedNodeControlReceiver { inner: receiver },
    )
}

pub(crate) fn attach_local_receiver_metrics<PData>(
    sender: LocalReceiverControlSender<PData>,
    node_id: &NodeId,
    pipeline_ctx: &PipelineContext,
    enabled: bool,
) -> LocalReceiverControlSender<PData> {
    sender.with_metrics(register_control_channel_metrics(
        node_id,
        pipeline_ctx,
        CHANNEL_MODE_LOCAL,
        enabled,
    ))
}

pub(crate) fn attach_shared_receiver_metrics<PData>(
    sender: SharedReceiverControlSender<PData>,
    node_id: &NodeId,
    pipeline_ctx: &PipelineContext,
    enabled: bool,
) -> SharedReceiverControlSender<PData> {
    sender.with_metrics(register_control_channel_metrics(
        node_id,
        pipeline_ctx,
        CHANNEL_MODE_SHARED,
        enabled,
    ))
}

pub(crate) fn attach_local_node_metrics<PData>(
    sender: LocalNodeControlSender<PData>,
    node_id: &NodeId,
    pipeline_ctx: &PipelineContext,
    enabled: bool,
) -> LocalNodeControlSender<PData> {
    sender.with_metrics(register_control_channel_metrics(
        node_id,
        pipeline_ctx,
        CHANNEL_MODE_LOCAL,
        enabled,
    ))
}

pub(crate) fn attach_shared_node_metrics<PData>(
    sender: SharedNodeControlSender<PData>,
    node_id: &NodeId,
    pipeline_ctx: &PipelineContext,
    enabled: bool,
) -> SharedNodeControlSender<PData> {
    sender.with_metrics(register_control_channel_metrics(
        node_id,
        pipeline_ctx,
        CHANNEL_MODE_SHARED,
        enabled,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::AckMsg;
    use crate::context::ControllerContext;
    use crate::entity_context::{
        NodeTelemetryGuard, NodeTelemetryHandle, set_pipeline_entity_key,
        with_node_telemetry_handle,
    };
    use crate::testing::test_nodes;
    use otap_df_config::node::NodeKind;
    use otap_df_control_channel::LifecycleSendResult;
    use otap_df_telemetry::metrics::MetricValue;
    use std::collections::HashMap;
    use std::time::Duration;

    fn assert_u64(value: &MetricValue, expected: u64, msg: &str) {
        match value {
            MetricValue::U64(actual) => assert_eq!(*actual, expected, "{msg}"),
            other => panic!("{msg}: expected U64, got {other:?}"),
        }
    }

    #[test]
    fn control_stats_empty_only_when_no_pending_work() {
        let stats = ControlChannelStats {
            phase: Phase::Normal,
            drain_ingress_recorded: false,
            shutdown_recorded: false,
            has_pending_drain_ingress: false,
            has_pending_shutdown: false,
            completion_len: 0,
            has_pending_config: false,
            has_pending_timer_tick: false,
            has_pending_collect_telemetry: false,
            completion_burst_len: 0,
            completion_batch_emitted_total: 0,
            completion_message_emitted_total: 0,
            config_replaced_total: 0,
            timer_tick_coalesced_total: 0,
            collect_telemetry_coalesced_total: 0,
            normal_event_dropped_during_drain_total: 0,
            completion_abandoned_on_forced_shutdown_total: 0,
            shutdown_forced: false,
            closed: true,
        };
        assert!(control_stats_is_empty(&stats));
    }

    #[test]
    fn receiver_collect_telemetry_event_rehydrates_metrics_reporter() {
        let (_rx, reporter) = MetricsReporter::create_new_and_receiver(1);
        let mut pending: VecDeque<NodeControlMsg<()>> = VecDeque::new();
        push_receiver_event(
            ChannelReceiverControlEvent::CollectTelemetry,
            &mut pending,
            &reporter,
        );
        assert!(matches!(
            pending.pop_front(),
            Some(NodeControlMsg::CollectTelemetry { .. })
        ));
    }

    #[test]
    fn completion_conversion_preserves_unwind() {
        let mut ack = AckMsg::new("hello".to_owned());
        ack.unwind = UnwindData::default();
        let msg = completion_to_node_control(ChannelCompletionMsg::Ack(ack_to_channel(ack)));
        assert!(matches!(msg, NodeControlMsg::Ack(_)));
    }

    #[test]
    fn report_metrics_emits_channel_control_snapshot() {
        const CONTROL_PHASE: usize = 0;
        const CONTROL_SHUTDOWN_RECORDED: usize = 2;
        const CONTROL_PENDING_SHUTDOWN: usize = 4;
        const CONTROL_COMPLETION_LEN: usize = 5;

        let registry = otap_df_telemetry::registry::TelemetryRegistryHandle::new();
        let controller = ControllerContext::new(registry);
        let pipeline_ctx =
            controller.pipeline_context_with("test_grp".into(), "test_pipeline".into(), 0, 1, 0);
        let pipeline_entity_key = pipeline_ctx.register_pipeline_entity();
        let _pipeline_guard =
            set_pipeline_entity_key(pipeline_ctx.metrics_registry(), pipeline_entity_key);

        let node_ctx = pipeline_ctx.with_node_context(
            "processor".into(),
            "urn:test:processor:example".into(),
            NodeKind::Processor,
            HashMap::new(),
        );
        let node_entity_key = node_ctx.register_node_entity();
        let node_telemetry = NodeTelemetryHandle::new(node_ctx.metrics_registry(), node_entity_key);
        let _node_guard = NodeTelemetryGuard::new(node_telemetry.clone());
        let node_id = test_nodes(vec!["processor"])
            .into_iter()
            .next()
            .expect("test node should exist");

        let (snapshot_rx, mut metrics_reporter) = MetricsReporter::create_new_and_receiver(4);
        let (sender, _receiver) = shared_node_channel::<String>(4);
        let sender = with_node_telemetry_handle(node_telemetry, || {
            attach_shared_node_metrics(sender, &node_id, &node_ctx, true)
        });

        sender
            .try_send(NodeControlMsg::Ack(AckMsg::new("ack-1".to_owned())))
            .expect("first ack should be buffered");
        sender
            .try_send(NodeControlMsg::Ack(AckMsg::new("ack-2".to_owned())))
            .expect("second ack should be buffered");
        assert!(matches!(
            sender.accept_shutdown(
                Instant::now() + Duration::from_secs(1),
                "test shutdown".to_owned()
            ),
            LifecycleSendResult::Accepted
        ));

        sender
            .report_metrics(&mut metrics_reporter)
            .expect("control-channel metrics should report");

        let snapshot = snapshot_rx
            .try_recv()
            .expect("channel.control snapshot should be emitted");
        let metrics = snapshot.get_metrics();
        assert_u64(
            &metrics[CONTROL_PHASE],
            2,
            "phase should report shutdown-recorded state",
        );
        assert_u64(
            &metrics[CONTROL_SHUTDOWN_RECORDED],
            1,
            "shutdown.recorded should latch once accepted",
        );
        assert_u64(
            &metrics[CONTROL_PENDING_SHUTDOWN],
            1,
            "shutdown.pending should remain visible until consumed",
        );
        assert_u64(
            &metrics[CONTROL_COMPLETION_LEN],
            2,
            "completion.len should reflect the buffered ack backlog",
        );
    }
}
