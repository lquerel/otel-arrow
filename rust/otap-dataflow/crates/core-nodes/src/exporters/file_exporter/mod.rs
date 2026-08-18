// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Multi-signal OTLP JSON Lines and length-prefixed protobuf file exporter.
//!
//! The exporter encodes OTLP-byte or OTAP Arrow pdata, then writes one bounded frame to a lazily
//! opened signal-specific file. The local run loop owns active files, crash-recoverable rotation,
//! bounded retention, and completion. Optional file-level compression uses one background task per
//! signal writer while later lifecycle work restores backpressure and the engine ACK/NACK contract.

mod compression;
mod config;
mod encoding;
mod metrics;
mod rotation;
mod writer;

pub use config::{
    Durability, FileCompression, FileExporterConfig, FileFormat, OpenMode, RetentionConfig,
    RotationConfig, TailRecovery,
};

use async_trait::async_trait;
use config::RenderedPaths;
use encoding::{
    FrameEncodeError, encode_logs, encode_metrics, encode_proto_records, encode_traces,
    frame_proto_bytes,
};
use linkme::distributed_slice;
use metrics::{
    FileExporterExportMetrics, FileFailureMetrics, FileOperation, FileSignalMetrics,
    SignalOperationAttributes,
};
use otap_df_config::SignalType;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::ExporterConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{AckMsg, NackMsg, NodeControlMsg};
use otap_df_engine::error::{Error, ExporterErrorKind};
use otap_df_engine::exporter::ExporterWrapper;
use otap_df_engine::local::exporter::{EffectHandler, Exporter};
use otap_df_engine::message::{ExporterInbox, Message};
use otap_df_engine::node::NodeId;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otap_df_otap::OTAP_EXPORTER_FACTORIES;
use otap_df_otap::pdata::OtapPdata;
use otap_df_pdata::otlp::ProtoBuffer;
use otap_df_pdata::otlp::logs::LogsProtoBytesEncoder;
use otap_df_pdata::otlp::metrics::MetricsProtoBytesEncoder;
use otap_df_pdata::otlp::traces::TracesProtoBytesEncoder;
use otap_df_pdata::views::otap::{OtapLogsView, OtapMetricsView, OtapTracesView};
use otap_df_pdata::views::otlp::bytes::logs::RawLogsData;
use otap_df_pdata::views::otlp::bytes::metrics::RawMetricsData;
use otap_df_pdata::views::otlp::bytes::traces::RawTraceData;
use otap_df_pdata::{OtapPayload, OtapPayloadHelpers, OtlpProtoBytes};
use otap_df_telemetry::attributes::AttributeEnum as _;
use otap_df_telemetry::common_attributes::{Outcome, SignalAttributes, SignalOutcomeAttributes};
use otap_df_telemetry::metrics::MeasurementMetricSet;
use otap_df_telemetry::{otel_error, otel_info, otel_warn};
use std::sync::Arc;
use tokio::time::Instant;
use writer::{SignalWriter, WriterFailure};

/// Component URN for the file exporter.
pub const FILE_EXPORTER_URN: &str = "urn:otel:exporter:file";

/// OTLP file exporter with one lazily opened writer per signal.
pub struct FileExporter {
    config: FileExporterConfig,
    paths: RenderedPaths,
    writers: [Option<SignalWriter>; 3],
    encoder: PayloadEncoder,
    failure_active: [bool; 3],
    export_metrics: MeasurementMetricSet<FileExporterExportMetrics>,
    signal_metrics: MeasurementMetricSet<FileSignalMetrics>,
    failure_metrics: MeasurementMetricSet<FileFailureMetrics>,
}

/// Declares the file exporter as a local exporter factory.
#[allow(unsafe_code)]
#[otap_df_engine::component_inventory(category = Exporter)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static FILE_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: FILE_EXPORTER_URN,
    create: |pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
        let config = FileExporterConfig::parse(&node_config.config)?;
        let paths = config.render_paths(pipeline.core_id(), pipeline.deployment_generation())?;
        let exporter = FileExporter {
            encoder: PayloadEncoder::new(config.max_frame_bytes),
            config,
            paths,
            writers: std::array::from_fn(|_| None),
            failure_active: [false; 3],
            export_metrics: FileExporterExportMetrics::register(&pipeline),
            signal_metrics: FileSignalMetrics::register(&pipeline),
            failure_metrics: FileFailureMetrics::register(&pipeline),
        };
        Ok(ExporterWrapper::local(
            exporter,
            node,
            node_config,
            exporter_config,
        ))
    },
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: |value| FileExporterConfig::parse(value).map(|_| ()),
};

#[async_trait(?Send)]
impl Exporter<OtapPdata> for FileExporter {
    async fn start(
        mut self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        otel_info!(
            "otelcol.node.file.start",
            format = self.config.format.as_str(),
            create_directories = self.config.create_directories,
            open_mode = self.config.open_mode.as_str(),
            durability = self.config.durability.as_str(),
            compression = self
                .config
                .compression
                .map_or("none", |compression| compression.as_str()),
            tail_recovery = self
                .config
                .effective_tail_recovery()
                .map_or("disabled", |recovery| recovery.as_str()),
            max_frame_bytes = self.config.max_frame_bytes,
        );
        loop {
            let rotation_delay = match self.next_lifecycle_delay() {
                Ok(delay) => delay,
                Err((signal, failure)) => {
                    self.record_io_failure(signal, &failure);
                    self.log_write_failure(signal, &failure);
                    return Err(exporter_error(
                        &effect_handler,
                        ExporterErrorKind::Transport,
                        "file lifecycle deadline evaluation failed",
                    ));
                }
            };
            let message = if let Some(delay) = rotation_delay {
                tokio::select! {
                    message = inbox.recv() => message?,
                    () = tokio::time::sleep(delay) => {
                        if let Err((signal, failure)) = self.maintain_writers().await {
                            self.record_io_failure(signal, &failure);
                            self.log_write_failure(signal, &failure);
                            return Err(exporter_error(
                                &effect_handler,
                                ExporterErrorKind::Transport,
                                "file lifecycle maintenance failed",
                            ));
                        }
                        continue;
                    }
                }
            } else {
                inbox.recv().await?
            };
            match message {
                Message::Control(NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                }) => {
                    _ = metrics_reporter.report_measurement(&mut self.export_metrics);
                    _ = metrics_reporter.report_measurement(&mut self.signal_metrics);
                    _ = metrics_reporter.report_measurement(&mut self.failure_metrics);
                }
                Message::Control(NodeControlMsg::Config { .. }) => {}
                Message::Control(NodeControlMsg::Shutdown { deadline, reason }) => {
                    self.finalize(deadline, &effect_handler).await?;
                    otel_info!("otelcol.node.file.stop", reason = reason.as_str(),);
                    let mut snapshots = self.export_metrics.terminal_snapshots();
                    snapshots.extend(self.signal_metrics.terminal_snapshots());
                    snapshots.extend(self.failure_metrics.terminal_snapshots());
                    return Ok(TerminalState::new(deadline, snapshots));
                }
                Message::PData(pdata) => {
                    self.export_pdata(pdata, &effect_handler).await?;
                }
                _ => {}
            }
        }
    }
}

impl FileExporter {
    async fn export_pdata(
        &mut self,
        pdata: OtapPdata,
        effect_handler: &EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let signal = pdata.signal_type();
        if pdata.is_empty() {
            effect_handler.notify_ack(AckMsg::new(pdata)).await?;
            self.record_export_outcome(signal, Outcome::Success);
            return Ok(());
        }
        let item_count = pdata.payload_ref().num_items() as u64;
        let (context, mut payload) = pdata.into_parts();
        let encode_result = self.encoder.encode(
            self.config.format,
            &mut payload,
            self.config.max_frame_bytes,
        );
        let pdata = OtapPdata::new(context, payload);
        if let Err(error) = encode_result {
            self.record_export_outcome(signal, Outcome::Failure);
            let reason = match error {
                EncodeFailure::Frame(FrameEncodeError::FrameTooLarge { .. }) => {
                    "file exporter frame exceeds max_frame_bytes; split the batch upstream"
                        .to_owned()
                }
                error => format!("file exporter rejected invalid pdata: {error}"),
            };
            effect_handler
                .notify_nack(NackMsg::new_permanent(&reason, pdata))
                .await?;
            return Ok(());
        }
        if let Err(failure) = self.ensure_writer(signal).await {
            self.record_io_failure(signal, &failure);
            self.record_export_outcome(signal, Outcome::Failure);
            self.log_write_failure(signal, &failure);
            effect_handler
                .notify_nack(NackMsg::new("file writer is not ready", pdata))
                .await?;
            return Ok(());
        }
        let index = signal_index(signal);
        let Some(writer) = self.writers[index].as_mut() else {
            self.record_export_outcome(signal, Outcome::Failure);
            effect_handler
                .notify_nack(NackMsg::new("file writer unavailable after open", pdata))
                .await?;
            return Err(exporter_error(
                effect_handler,
                ExporterErrorKind::Other,
                "file writer missing after successful open",
            ));
        };
        let progress = match writer.write_frame(self.encoder.frame()).await {
            Ok(progress) => progress,
            Err(failure) => {
                self.record_io_failure(signal, &failure);
                self.record_export_outcome(signal, Outcome::Failure);
                self.log_write_failure(signal, &failure);
                let fatal = failure.is_fatal();
                effect_handler
                    .notify_nack(NackMsg::new(
                        if failure.rollback_error.is_some() {
                            "file write failed and rollback left file state indeterminate"
                        } else if fatal {
                            "file lifecycle failed and requires exporter restart"
                        } else {
                            "file write failed and was rolled back"
                        },
                        pdata,
                    ))
                    .await?;
                if fatal {
                    return Err(exporter_error(
                        effect_handler,
                        ExporterErrorKind::Transport,
                        if failure.rollback_error.is_some() {
                            "file write rollback failed"
                        } else {
                            "file lifecycle recovery requires exporter restart"
                        },
                    ));
                }
                return Ok(());
            }
        };
        self.record_writer_progress(signal, progress);
        self.failure_active[index] = false;
        self.signal_metrics
            .with(SignalAttributes { signal })
            .items
            .add(item_count);
        self.signal_metrics
            .with(SignalAttributes { signal })
            .bytes
            .add(self.encoder.frame().len() as u64);
        self.record_export_outcome(signal, Outcome::Success);
        effect_handler.notify_ack(AckMsg::new(pdata)).await?;
        Ok(())
    }

    async fn ensure_writer(&mut self, signal: SignalType) -> Result<(), WriterFailure> {
        let index = signal_index(signal);
        if self.writers[index].is_some() {
            return Ok(());
        }
        let (writer, recovery) = SignalWriter::open(self.paths.get(signal), &self.config).await?;
        if recovery.recovered_bytes != 0 {
            self.signal_metrics
                .with(SignalAttributes { signal })
                .tail_recoveries
                .inc();
            self.signal_metrics
                .with(SignalAttributes { signal })
                .tail_recovered_bytes
                .add(recovery.recovered_bytes);
            otel_warn!(
                "otelcol.node.file.tail.recover",
                signal = signal.as_str(),
                recovered_bytes = recovery.recovered_bytes,
                message = "Removed an incomplete final file frame"
            );
        }
        self.writers[index] = Some(writer);
        self.failure_active[index] = false;
        otel_info!("otelcol.node.file.writer.start", signal = signal.as_str(),);
        Ok(())
    }

    fn record_export_outcome(&mut self, signal: SignalType, outcome: Outcome) {
        self.export_metrics
            .with(SignalOutcomeAttributes { signal, outcome })
            .messages
            .inc();
    }

    fn record_io_failure(&mut self, signal: SignalType, failure: &WriterFailure) {
        self.failure_metrics
            .with(SignalOperationAttributes {
                signal,
                operation: failure.operation,
            })
            .failures
            .inc();
        if failure.rollback_error.is_some() {
            self.failure_metrics
                .with(SignalOperationAttributes {
                    signal,
                    operation: FileOperation::Rollback,
                })
                .failures
                .inc();
        }
    }

    fn log_write_failure(&mut self, signal: SignalType, failure: &WriterFailure) {
        let index = signal_index(signal);
        if let Some(rollback_error) = failure.rollback_error.as_deref() {
            otel_error!(
                "otelcol.node.file.rollback.fail",
                signal = signal.as_str(),
                operation = failure.operation.as_str(),
                error = failure.error.as_str(),
                rollback_error = rollback_error,
                message = "File write rollback failed"
            );
        } else if let Some(fatal_error) = failure.fatal_error.as_deref() {
            otel_error!(
                "otelcol.node.file.lifecycle.fail",
                signal = signal.as_str(),
                operation = failure.operation.as_str(),
                error = failure.error.as_str(),
                fatal_error = fatal_error,
                message = "File lifecycle state requires exporter restart"
            );
        } else if !self.failure_active[index] {
            otel_warn!(
                "otelcol.node.file.operation.fail",
                signal = signal.as_str(),
                operation = failure.operation.as_str(),
                error = failure.error.as_str(),
                message = "File writer entered a failure state"
            );
        }
        self.failure_active[index] = true;
    }

    fn next_lifecycle_delay(
        &self,
    ) -> Result<Option<std::time::Duration>, (SignalType, WriterFailure)> {
        let mut next = None;
        for signal in [SignalType::Logs, SignalType::Metrics, SignalType::Traces] {
            if let Some(writer) = &self.writers[signal_index(signal)]
                && let Some(delay) = writer
                    .time_until_lifecycle()
                    .map_err(|failure| (signal, failure))?
            {
                next = Some(next.map_or(delay, |current: std::time::Duration| current.min(delay)));
            }
        }
        Ok(next)
    }

    async fn maintain_writers(&mut self) -> Result<(), (SignalType, WriterFailure)> {
        for signal in [SignalType::Logs, SignalType::Metrics, SignalType::Traces] {
            if let Some(writer) = &mut self.writers[signal_index(signal)] {
                let progress = writer
                    .maintain_if_due()
                    .await
                    .map_err(|failure| (signal, failure))?;
                self.record_writer_progress(signal, progress);
            }
        }
        Ok(())
    }

    fn record_rotation(&mut self, signal: SignalType) {
        self.signal_metrics
            .with(SignalAttributes { signal })
            .rotations
            .inc();
        otel_info!("otelcol.node.file.rotate", signal = signal.as_str(),);
    }

    fn record_writer_progress(&mut self, signal: SignalType, progress: writer::WriterProgress) {
        if progress.rotated {
            self.record_rotation(signal);
        }
        if progress.compressions != 0 {
            self.signal_metrics
                .with(SignalAttributes { signal })
                .compressions
                .add(progress.compressions);
            otel_info!(
                "otelcol.node.file.compress",
                signal = signal.as_str(),
                files = progress.compressions,
            );
        }
    }

    async fn finalize(
        &mut self,
        deadline: std::time::Instant,
        effect_handler: &EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let result = tokio::time::timeout_at(Instant::from_std(deadline), async {
            let mut failures = Vec::new();
            let mut compressions = Vec::new();
            for signal in [SignalType::Logs, SignalType::Metrics, SignalType::Traces] {
                let writer = &mut self.writers[signal_index(signal)];
                if let Some(writer) = writer {
                    match writer.finalize().await {
                        Ok(count) if count != 0 => compressions.push((signal, count)),
                        Ok(_) => {}
                        Err(failure) => failures.push((signal, failure)),
                    }
                }
            }
            (failures, compressions)
        })
        .await;
        match result {
            Ok((failures, compressions)) => {
                for (signal, count) in compressions {
                    self.record_writer_progress(
                        signal,
                        writer::WriterProgress {
                            rotated: false,
                            compressions: count,
                        },
                    );
                }
                if failures.is_empty() {
                    return Ok(());
                }
                for (signal, failure) in failures {
                    self.record_io_failure(signal, &failure);
                    self.log_write_failure(signal, &failure);
                }
                Err(exporter_error(
                    effect_handler,
                    ExporterErrorKind::Shutdown,
                    "file synchronization or background compression failed during shutdown",
                ))
            }
            Err(_) => Err(exporter_error(
                effect_handler,
                ExporterErrorKind::Shutdown,
                "file synchronization exceeded the shutdown deadline",
            )),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum EncodeFailure {
    #[error("{0}")]
    View(String),
    #[error(transparent)]
    Frame(#[from] FrameEncodeError),
}

struct PayloadEncoder {
    frame: Vec<u8>,
    proto_buffer: ProtoBuffer,
    logs_proto: LogsProtoBytesEncoder,
    metrics_proto: MetricsProtoBytesEncoder,
    traces_proto: TracesProtoBytesEncoder,
}

impl PayloadEncoder {
    fn new(max_frame_bytes: usize) -> Self {
        let initial_capacity = max_frame_bytes.min(8 * 1024);
        Self {
            frame: Vec::with_capacity(initial_capacity),
            proto_buffer: ProtoBuffer::with_capacity_and_limit(
                initial_capacity,
                max_frame_bytes.saturating_sub(size_of::<u32>()),
            ),
            logs_proto: LogsProtoBytesEncoder::new(),
            metrics_proto: MetricsProtoBytesEncoder::new(),
            traces_proto: TracesProtoBytesEncoder::new(),
        }
    }

    fn frame(&self) -> &[u8] {
        &self.frame
    }

    fn encode(
        &mut self,
        format: FileFormat,
        payload: &mut OtapPayload,
        max_frame_bytes: usize,
    ) -> Result<(), EncodeFailure> {
        self.frame.clear();
        match format {
            FileFormat::OtlpJson => encode_json_payload(payload, &mut self.frame, max_frame_bytes),
            FileFormat::OtlpProto => self.encode_proto_payload(payload, max_frame_bytes),
        }
    }

    fn encode_proto_payload(
        &mut self,
        payload: &mut OtapPayload,
        max_frame_bytes: usize,
    ) -> Result<(), EncodeFailure> {
        match payload {
            OtapPayload::OtlpBytes(bytes) => {
                validate_otlp_bytes(bytes)?;
                frame_proto_bytes(bytes.as_bytes(), &mut self.frame, max_frame_bytes)?;
            }
            OtapPayload::OtapArrowRecords(records) => match records.signal_type() {
                SignalType::Logs => encode_proto_records(
                    &mut self.logs_proto,
                    records,
                    &mut self.proto_buffer,
                    &mut self.frame,
                    max_frame_bytes,
                )?,
                SignalType::Metrics => encode_proto_records(
                    &mut self.metrics_proto,
                    records,
                    &mut self.proto_buffer,
                    &mut self.frame,
                    max_frame_bytes,
                )?,
                SignalType::Traces => encode_proto_records(
                    &mut self.traces_proto,
                    records,
                    &mut self.proto_buffer,
                    &mut self.frame,
                    max_frame_bytes,
                )?,
            },
        }
        Ok(())
    }
}

fn validate_otlp_bytes(bytes: &OtlpProtoBytes) -> Result<(), EncodeFailure> {
    match bytes {
        OtlpProtoBytes::ExportLogsRequest(_) => {
            _ = RawLogsData::try_from(bytes)
                .map_err(|error| EncodeFailure::View(error.to_string()))?;
        }
        OtlpProtoBytes::ExportMetricsRequest(bytes) => {
            _ = RawMetricsData::try_new(bytes)
                .map_err(|error| EncodeFailure::View(error.to_string()))?;
        }
        OtlpProtoBytes::ExportTracesRequest(bytes) => {
            _ = RawTraceData::try_new(bytes)
                .map_err(|error| EncodeFailure::View(error.to_string()))?;
        }
    }
    Ok(())
}

fn encode_json_payload(
    payload: &OtapPayload,
    frame: &mut Vec<u8>,
    max_frame_bytes: usize,
) -> Result<(), EncodeFailure> {
    match payload {
        OtapPayload::OtlpBytes(bytes) => match bytes {
            OtlpProtoBytes::ExportLogsRequest(_) => {
                let view = RawLogsData::try_from(bytes)
                    .map_err(|error| EncodeFailure::View(error.to_string()))?;
                encode_logs(&view, frame, max_frame_bytes)?;
            }
            OtlpProtoBytes::ExportMetricsRequest(bytes) => {
                let view = RawMetricsData::try_new(bytes)
                    .map_err(|error| EncodeFailure::View(error.to_string()))?;
                encode_metrics(&view, frame, max_frame_bytes)?;
            }
            OtlpProtoBytes::ExportTracesRequest(bytes) => {
                let view = RawTraceData::try_new(bytes)
                    .map_err(|error| EncodeFailure::View(error.to_string()))?;
                encode_traces(&view, frame, max_frame_bytes)?;
            }
        },
        OtapPayload::OtapArrowRecords(records) => match records.signal_type() {
            SignalType::Logs => {
                let view = OtapLogsView::try_from(records)
                    .map_err(|error| EncodeFailure::View(error.to_string()))?;
                encode_logs(&view, frame, max_frame_bytes)?;
            }
            SignalType::Metrics => {
                let view = OtapMetricsView::try_from(records)
                    .map_err(|error| EncodeFailure::View(error.to_string()))?;
                encode_metrics(&view, frame, max_frame_bytes)?;
            }
            SignalType::Traces => {
                let view = OtapTracesView::try_from(records)
                    .map_err(|error| EncodeFailure::View(error.to_string()))?;
                encode_traces(&view, frame, max_frame_bytes)?;
            }
        },
    }
    Ok(())
}

const fn signal_index(signal: SignalType) -> usize {
    match signal {
        SignalType::Logs => 0,
        SignalType::Metrics => 1,
        SignalType::Traces => 2,
    }
}

fn exporter_error(
    effect_handler: &EffectHandler<OtapPdata>,
    kind: ExporterErrorKind,
    error: impl Into<String>,
) -> Error {
    Error::ExporterError {
        exporter: effect_handler.exporter_id(),
        kind,
        error: error.into(),
        source_detail: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_engine::Interests;
    use otap_df_engine::control::PipelineCompletionMsg;
    use otap_df_engine::testing::exporter::{
        TestContext, TestRuntime, create_exporter_from_factory,
    };
    use otap_df_otap::testing::{TestCallData, create_empty_test_pdata};
    use otap_df_pdata::OtlpProtoBytes;
    use otap_df_pdata::encode::{
        encode_logs_otap_batch, encode_metrics_otap_batch, encode_spans_otap_batch,
    };
    use otap_df_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use otap_df_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use otap_df_pdata::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;
    use otap_df_pdata::proto::opentelemetry::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use otap_df_pdata::proto::opentelemetry::metrics::v1::{Metric, ResourceMetrics, ScopeMetrics};
    use otap_df_pdata::proto::opentelemetry::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use serde_json::json;
    use std::time::{Duration, Instant as StdInstant};
    use tempfile::tempdir;

    fn encoded<M: prost::Message>(message: &M) -> Vec<u8> {
        let mut bytes = Vec::new();
        message.encode(&mut bytes).unwrap();
        bytes
    }

    fn pdata<M: prost::Message>(signal: SignalType, message: M) -> OtapPdata {
        let bytes = encoded(&message);
        OtapPdata::new_default(OtlpProtoBytes::new_from_bytes(signal, bytes).into())
    }

    fn log_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        event_name: "ready".to_owned(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn log_pdata() -> OtapPdata {
        pdata(SignalType::Logs, log_request())
    }

    fn metric_request() -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "requests".to_owned(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn metric_pdata() -> OtapPdata {
        pdata(SignalType::Metrics, metric_request())
    }

    fn trace_request() -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        name: "GET /ready".to_owned(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn trace_pdata() -> OtapPdata {
        pdata(SignalType::Traces, trace_request())
    }

    async fn assert_permanent_nack(ctx: &mut TestContext<OtapPdata>, expected_reason: &str) {
        let mut completion_receiver = ctx.take_pipeline_completion_receiver().unwrap();
        let completion = tokio::time::timeout(Duration::from_secs(3), completion_receiver.recv())
            .await
            .expect("timed out waiting for file exporter NACK")
            .expect("completion channel closed before file exporter NACK");
        match completion {
            PipelineCompletionMsg::DeliverNack { nack } => {
                assert!(nack.permanent);
                assert!(nack.reason.contains(expected_reason), "{}", nack.reason);
            }
            PipelineCompletionMsg::DeliverAck { .. } => panic!("expected a permanent NACK"),
        }
    }

    /// Scenario: OTAP Arrow batches for logs, metrics, and traces enter the common payload path.
    /// Guarantees: Every OTAP signal is framed as one JSON object with the matching top-level key.
    #[test]
    fn encode_payload_accepts_all_otap_signal_views() {
        let logs_bytes = encoded(&log_request());
        let metrics_bytes = encoded(&metric_request());
        let traces_bytes = encoded(&trace_request());
        let logs = RawLogsData::try_new(&logs_bytes).unwrap();
        let metrics = RawMetricsData::try_new(&metrics_bytes).unwrap();
        let traces = RawTraceData::try_new(&traces_bytes).unwrap();
        let payloads = [
            OtapPayload::OtapArrowRecords(encode_logs_otap_batch(&logs).unwrap()),
            OtapPayload::OtapArrowRecords(encode_metrics_otap_batch(&metrics).unwrap()),
            OtapPayload::OtapArrowRecords(encode_spans_otap_batch(&traces).unwrap()),
        ];
        let expected_fields = ["resourceLogs", "resourceMetrics", "resourceSpans"];
        let mut encoder = PayloadEncoder::new(4096);
        for (mut payload, expected_field) in payloads.into_iter().zip(expected_fields) {
            encoder
                .encode(FileFormat::OtlpJson, &mut payload, 4096)
                .unwrap();
            let value: serde_json::Value = serde_json::from_slice(encoder.frame()).unwrap();
            assert!(value.get(expected_field).is_some());
        }
    }

    /// Scenario: A malformed non-empty OTLP protobuf payload reaches view validation.
    /// Guarantees: Validation fails without retaining bytes from an earlier encoded frame.
    #[test]
    fn malformed_otlp_payload_clears_the_reusable_frame() {
        let mut payload =
            OtapPayload::OtlpBytes(OtlpProtoBytes::new_from_bytes(SignalType::Logs, vec![0x80]));
        let mut encoder = PayloadEncoder::new(4096);
        encoder.frame.extend_from_slice(b"previous telemetry\n");
        assert!(
            encoder
                .encode(FileFormat::OtlpJson, &mut payload, 4096)
                .is_err()
        );
        assert!(encoder.frame().is_empty());
    }

    /// Scenario: OTAP Arrow batches for all three signals use protobuf file encoding.
    /// Guarantees: Every frame has a correct length prefix and a decodable OTLP request.
    #[test]
    fn protobuf_encoding_accepts_all_otap_signals() {
        let logs_bytes = encoded(&log_request());
        let metrics_bytes = encoded(&metric_request());
        let traces_bytes = encoded(&trace_request());
        let logs = RawLogsData::try_new(&logs_bytes).unwrap();
        let metrics = RawMetricsData::try_new(&metrics_bytes).unwrap();
        let traces = RawTraceData::try_new(&traces_bytes).unwrap();
        let payloads = [
            OtapPayload::OtapArrowRecords(encode_logs_otap_batch(&logs).unwrap()),
            OtapPayload::OtapArrowRecords(encode_metrics_otap_batch(&metrics).unwrap()),
            OtapPayload::OtapArrowRecords(encode_spans_otap_batch(&traces).unwrap()),
        ];
        let mut encoder = PayloadEncoder::new(4096);
        for mut payload in payloads {
            encoder
                .encode(FileFormat::OtlpProto, &mut payload, 4096)
                .unwrap();
            let frame = encoder.frame();
            let payload_len = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
            assert_eq!(payload_len, frame.len() - 4);
            match payload.signal_type() {
                SignalType::Logs => {
                    _ = <ExportLogsServiceRequest as prost::Message>::decode(&frame[4..]).unwrap();
                }
                SignalType::Metrics => {
                    _ = <ExportMetricsServiceRequest as prost::Message>::decode(&frame[4..])
                        .unwrap();
                }
                SignalType::Traces => {
                    _ = <ExportTraceServiceRequest as prost::Message>::decode(&frame[4..]).unwrap();
                }
            }
        }
    }

    /// Scenario: One exporter instance receives non-empty logs, metrics, and traces in sequence.
    /// Guarantees: Each signal produces exactly one replayable JSON line in its exclusive file.
    #[test]
    fn exporter_writes_one_otlp_json_line_per_signal_batch() {
        let dir = tempdir().unwrap();
        let template = dir
            .path()
            .join("capture-{signal}-{core_id}-{generation}.jsonl");
        let exporter = create_exporter_from_factory(
            &FILE_EXPORTER,
            json!({"path": template.to_string_lossy()}),
        )
        .unwrap();
        let paths = [
            dir.path().join("capture-logs-0-0.jsonl"),
            dir.path().join("capture-metrics-0-0.jsonl"),
            dir.path().join("capture-traces-0-0.jsonl"),
        ];
        TestRuntime::new()
            .set_exporter(exporter)
            .run_test(|ctx| async move {
                for pdata in [log_pdata(), metric_pdata(), trace_pdata()] {
                    ctx.send_pdata(pdata).await.unwrap();
                }
                ctx.send_shutdown(StdInstant::now() + Duration::from_secs(10), "test complete")
                    .await
                    .unwrap();
            })
            .run_validation(move |_, result| async move {
                result.unwrap();
                let expected_fields = ["resourceLogs", "resourceMetrics", "resourceSpans"];
                for (path, expected_field) in paths.iter().zip(expected_fields) {
                    let content = tokio::fs::read_to_string(path).await.unwrap();
                    assert_eq!(content.lines().count(), 1);
                    let value: serde_json::Value = serde_json::from_str(&content).unwrap();
                    assert!(value.get(expected_field).is_some());
                }
            });
    }

    /// Scenario: The exporter receives one raw OTLP protobuf batch for every supported signal.
    /// Guarantees: The `proto` alias writes one replayable length-prefixed request per signal file.
    #[test]
    fn exporter_writes_one_otlp_protobuf_frame_per_signal_batch() {
        let dir = tempdir().unwrap();
        let template = dir
            .path()
            .join("capture-{signal}-{core_id}-{generation}.bin");
        let exporter = create_exporter_from_factory(
            &FILE_EXPORTER,
            json!({"path": template.to_string_lossy(), "format": "proto"}),
        )
        .unwrap();
        let paths = [
            dir.path().join("capture-logs-0-0.bin"),
            dir.path().join("capture-metrics-0-0.bin"),
            dir.path().join("capture-traces-0-0.bin"),
        ];
        TestRuntime::new()
            .set_exporter(exporter)
            .run_test(|ctx| async move {
                for pdata in [log_pdata(), metric_pdata(), trace_pdata()] {
                    ctx.send_pdata(pdata).await.unwrap();
                }
                ctx.send_shutdown(StdInstant::now() + Duration::from_secs(10), "test complete")
                    .await
                    .unwrap();
            })
            .run_validation(move |_, result| async move {
                result.unwrap();
                for path in paths {
                    let frame = tokio::fs::read(path).await.unwrap();
                    let payload_len = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
                    assert_eq!(payload_len, frame.len() - 4);
                }
            });
    }

    /// Scenario: A time deadline expires after one batch while gzip compression is configured.
    /// Guarantees: The idle run loop rotates, commits a standard compressed file, and removes its
    /// uncompressed finalized source without waiting for another pdata message.
    #[test]
    fn exporter_rotates_and_compresses_on_the_idle_timer() {
        let dir = tempdir().unwrap();
        let template = dir
            .path()
            .join("capture-{signal}-{core_id}-{generation}.jsonl");
        let exporter = create_exporter_from_factory(
            &FILE_EXPORTER,
            json!({
                "path": template.to_string_lossy(),
                "max_frame_bytes": 4096,
                "compression": "gzip",
                "rotation": {
                    "max_duration": "5ms",
                    "retention": {"max_backups": 2}
                }
            }),
        )
        .unwrap();
        let active = dir.path().join("capture-logs-0-0.jsonl");
        let source = rotation::segment_path(&active, 0);
        let finalized = rotation::compressed_segment_path(&active, 0, FileCompression::Gzip);
        TestRuntime::new()
            .set_exporter(exporter)
            .run_test(|ctx| async move {
                ctx.send_pdata(log_pdata()).await.unwrap();
                tokio::time::sleep(Duration::from_millis(250)).await;
                ctx.send_shutdown(StdInstant::now() + Duration::from_secs(10), "test complete")
                    .await
                    .unwrap();
            })
            .run_validation(move |_, result| async move {
                result.unwrap();
                assert!(tokio::fs::read(active).await.unwrap().is_empty());
                assert!(!source.exists());
                let mut decoder =
                    flate2::read::GzDecoder::new(std::fs::File::open(finalized).unwrap());
                let mut content = String::new();
                _ = std::io::Read::read_to_string(&mut decoder, &mut content).unwrap();
                assert_eq!(content.lines().count(), 1);
            });
    }

    /// Scenario: An empty pdata batch is delivered before any signal writer has opened.
    /// Guarantees: The exporter ACKs and shuts down without creating an unused signal file.
    #[test]
    fn empty_pdata_does_not_create_a_file() {
        let dir = tempdir().unwrap();
        let template = dir
            .path()
            .join("capture-{signal}-{core_id}-{generation}.jsonl");
        let exporter = create_exporter_from_factory(
            &FILE_EXPORTER,
            json!({"path": template.to_string_lossy()}),
        )
        .unwrap();
        let logs_path = dir.path().join("capture-logs-0-0.jsonl");
        TestRuntime::new()
            .set_exporter(exporter)
            .run_test(|ctx| async move {
                ctx.send_pdata(create_empty_test_pdata()).await.unwrap();
                ctx.send_shutdown(StdInstant::now() + Duration::from_secs(10), "test complete")
                    .await
                    .unwrap();
            })
            .run_validation(move |_, result| async move {
                result.unwrap();
                assert!(!logs_path.exists());
            });
    }

    /// Scenario: A malformed non-empty protobuf batch is delivered before a writer opens.
    /// Guarantees: The exporter permanently rejects the input without creating a signal file.
    #[test]
    fn invalid_pdata_does_not_create_a_file() {
        let dir = tempdir().unwrap();
        let template = dir
            .path()
            .join("capture-{signal}-{core_id}-{generation}.jsonl");
        let exporter = create_exporter_from_factory(
            &FILE_EXPORTER,
            json!({"path": template.to_string_lossy()}),
        )
        .unwrap();
        let logs_path = dir.path().join("capture-logs-0-0.jsonl");
        TestRuntime::new()
            .set_exporter(exporter)
            .run_test(|ctx| async move {
                let pdata = OtapPdata::new_default(
                    OtlpProtoBytes::new_from_bytes(SignalType::Logs, vec![0x80]).into(),
                )
                .test_subscribe_to(
                    Interests::NACKS,
                    TestCallData::default().into(),
                    123,
                );
                ctx.send_pdata(pdata).await.unwrap();
                ctx.send_shutdown(StdInstant::now() + Duration::from_secs(10), "test complete")
                    .await
                    .unwrap();
            })
            .run_validation(move |mut ctx, result| async move {
                result.unwrap();
                assert_permanent_nack(&mut ctx, "invalid pdata").await;
                assert!(!logs_path.exists());
            });
    }

    /// Scenario: A valid batch exceeds the configured complete-frame byte limit.
    /// Guarantees: The exporter permanently rejects the frame without creating a signal file.
    #[test]
    fn oversized_pdata_does_not_create_a_file() {
        let dir = tempdir().unwrap();
        let template = dir
            .path()
            .join("capture-{signal}-{core_id}-{generation}.jsonl");
        let exporter = create_exporter_from_factory(
            &FILE_EXPORTER,
            json!({"path": template.to_string_lossy(), "max_frame_bytes": 1}),
        )
        .unwrap();
        let logs_path = dir.path().join("capture-logs-0-0.jsonl");
        TestRuntime::new()
            .set_exporter(exporter)
            .run_test(|ctx| async move {
                let pdata = log_pdata().test_subscribe_to(
                    Interests::NACKS,
                    TestCallData::default().into(),
                    123,
                );
                ctx.send_pdata(pdata).await.unwrap();
                ctx.send_shutdown(StdInstant::now() + Duration::from_secs(10), "test complete")
                    .await
                    .unwrap();
            })
            .run_validation(move |mut ctx, result| async move {
                result.unwrap();
                assert_permanent_nack(&mut ctx, "exceeds max_frame_bytes").await;
                assert!(!logs_path.exists());
            });
    }
}
