// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Weaver live-check processor.
//!
//! This processor forwards incoming telemetry unchanged on the default output and
//! emits OTLP log findings on the fixed `findings` output port.

mod config;
mod mapper;
mod metrics;

pub use config::Config;
pub use metrics::WeaverLiveCheckMetrics;

use async_trait::async_trait;
use config::validate_node_outputs;
use linkme::distributed_slice;
use mapper::{CorrelationContext, map_otlp_message};
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::MessageSourceLocalEffectHandlerExtension;
use otap_df_engine::config::ProcessorConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::error::{Error, ProcessorErrorKind, TypedError};
use otap_df_engine::local::processor as local;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::processor::ProcessorWrapper;
use otap_df_engine::wiring_contract::WiringContract;
use otap_df_otap::OTAP_PROCESSOR_FACTORIES;
use otap_df_otap::pdata::OtapPdata;
use otap_df_pdata::OtapPayload;
use otap_df_pdata::otlp::OtlpProtoBytes;
use otap_df_pdata::proto::OtlpProtoMessage;
use otap_df_pdata::proto::opentelemetry::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use otap_df_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs, SeverityNumber,
};
use prost::Message as ProstMessage;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use weaver_checker::{FindingLevel, PolicyFinding};
use weaver_forge::registry::ResolvedRegistry;
use weaver_live_check::advice::{
    Advisor, DeprecatedAdvisor, EnumAdvisor, RegoAdvisor, StabilityAdvisor, TypeAdvisor,
};
use weaver_live_check::sample_attribute::SampleAttribute;
use weaver_live_check::sample_log::SampleLog;
use weaver_live_check::sample_metric::{
    DataPoints, SampleExemplar, SampleExponentialHistogramDataPoint, SampleHistogramDataPoint,
    SampleMetric, SampleNumberDataPoint,
};
use weaver_live_check::sample_resource::SampleResource;
use weaver_live_check::sample_span::{SampleSpan, SampleSpanEvent, SampleSpanLink};
use weaver_live_check::{
    DisabledStatistics, LiveCheckResult, LiveCheckRunner, LiveCheckStatistics, Sample,
    VersionedRegistry, live_checker::LiveChecker,
};

/// URN identifier for the Weaver live-check processor.
pub const WEAVER_LIVE_CHECK_PROCESSOR_URN: &str = "urn:otel:processor:weaver_live_check";

/// Fixed named output used for finding events.
pub const FINDINGS_PORT: &str = "findings";

const FINDING_EVENT_NAME: &str = "weaver.live_check.finding";

/// Local processor that runs `weaver_live_check` against incoming telemetry.
pub struct WeaverLiveCheckProcessor {
    live_checker: LiveChecker,
    registry_url: String,
    metrics: otap_df_telemetry::metrics::MetricSet<WeaverLiveCheckMetrics>,
    seen_findings: HashSet<FindingFingerprint>,
}

struct PreparedFindings {
    pdata: OtapPdata,
    total: u64,
    information: u64,
    improvement: u64,
    violation: u64,
    fingerprints: Vec<FindingFingerprint>,
}

#[derive(Debug, Clone)]
struct EmittedFinding {
    finding: PolicyFinding,
    context: CorrelationContext,
}

#[derive(Debug, Clone, Default)]
struct FindingLevelCounts {
    information: u64,
    improvement: u64,
    violation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CorrelationKey {
    resource: Vec<u8>,
    resource_schema_url: String,
    scope: Vec<u8>,
    scope_schema_url: String,
}

struct GroupedFindings {
    resource: Option<otap_df_pdata::proto::opentelemetry::resource::v1::Resource>,
    resource_schema_url: String,
    scope: Option<InstrumentationScope>,
    scope_schema_url: String,
    log_records: Vec<LogRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FindingFingerprint {
    id: String,
    level: FindingLevel,
    message: String,
    signal_type: Option<String>,
    signal_name: Option<String>,
    context_json: String,
}

/// Factory function to create the processor.
pub fn create_weaver_live_check_processor(
    pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError> {
    validate_node_outputs(&node_config)?;

    Ok(ProcessorWrapper::local(
        WeaverLiveCheckProcessor::from_config(pipeline_ctx, &node_config.config)?,
        node,
        node_config,
        processor_config,
    ))
}

/// Register the processor as an OTAP processor factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
pub static WEAVER_LIVE_CHECK_PROCESSOR_FACTORY: otap_df_engine::ProcessorFactory<OtapPdata> =
    otap_df_engine::ProcessorFactory {
        name: WEAVER_LIVE_CHECK_PROCESSOR_URN,
        create: |pipeline_ctx: PipelineContext,
                 node: NodeId,
                 node_config: Arc<NodeUserConfig>,
                 processor_config: &ProcessorConfig| {
            create_weaver_live_check_processor(pipeline_ctx, node, node_config, processor_config)
        },
        wiring_contract: WiringContract::UNRESTRICTED,
        validate_config: otap_df_config::validation::validate_typed_config::<Config>,
    };

impl WeaverLiveCheckProcessor {
    /// Build a processor from user config and eagerly load the registry.
    pub fn from_config(pipeline_ctx: PipelineContext, config: &Value) -> Result<Self, ConfigError> {
        let config: Config = serde_json::from_value(config.clone()).map_err(|error| {
            ConfigError::InvalidUserConfig {
                error: error.to_string(),
            }
        })?;

        let loaded_registry = config.load_registry()?;
        let registry_url = loaded_registry.registry_url.clone();
        let live_checker = build_live_checker(loaded_registry.resolved_registry)?;
        let metrics = pipeline_ctx.register_metrics::<WeaverLiveCheckMetrics>();

        Ok(Self {
            live_checker,
            registry_url,
            metrics,
            seen_findings: HashSet::new(),
        })
    }

    fn update_attribute_coverage(&mut self, attributes: &[SampleAttribute]) {
        for attribute in attributes {
            self.metrics.attribute_names_seen.add(1);
            if self.live_checker.find_attribute(&attribute.name).is_some()
                || self.live_checker.find_template(&attribute.name).is_some()
            {
                self.metrics.attribute_names_matched.add(1);
            }
        }
    }

    fn update_coverage_for_sample(&mut self, sample: &Sample) {
        match sample {
            Sample::Attribute(attribute) => {
                self.update_attribute_coverage(std::slice::from_ref(attribute))
            }
            Sample::Resource(resource) => self.update_attribute_coverage(&resource.attributes),
            Sample::Log(log) => {
                if !log.event_name.is_empty() {
                    self.metrics.event_names_seen.add(1);
                    if self.live_checker.find_event(&log.event_name).is_some() {
                        self.metrics.event_names_matched.add(1);
                    }
                }
                self.update_attribute_coverage(&log.attributes);
            }
            Sample::Metric(metric) => {
                self.metrics.metric_names_seen.add(1);
                if self.live_checker.find_metric(&metric.name).is_some() {
                    self.metrics.metric_names_matched.add(1);
                }

                if let Some(data_points) = &metric.data_points {
                    match data_points {
                        DataPoints::Number(points) => {
                            for point in points {
                                self.update_attribute_coverage(&point.attributes);
                                for exemplar in &point.exemplars {
                                    self.update_attribute_coverage(&exemplar.filtered_attributes);
                                }
                            }
                        }
                        DataPoints::Histogram(points) => {
                            for point in points {
                                self.update_attribute_coverage(&point.attributes);
                                for exemplar in &point.exemplars {
                                    self.update_attribute_coverage(&exemplar.filtered_attributes);
                                }
                            }
                        }
                        DataPoints::ExponentialHistogram(points) => {
                            for point in points {
                                self.update_attribute_coverage(&point.attributes);
                                for exemplar in &point.exemplars {
                                    self.update_attribute_coverage(&exemplar.filtered_attributes);
                                }
                            }
                        }
                    }
                }
            }
            Sample::Span(span) => {
                self.update_attribute_coverage(&span.attributes);
                for event in &span.span_events {
                    self.update_attribute_coverage(&event.attributes);
                }
                for link in &span.span_links {
                    self.update_attribute_coverage(&link.attributes);
                }
            }
            Sample::SpanEvent(event) => self.update_attribute_coverage(&event.attributes),
            Sample::SpanLink(link) => self.update_attribute_coverage(&link.attributes),
        }

        self.refresh_coverage_gauges();
    }

    fn refresh_coverage_gauges(&mut self) {
        let seen = self.metrics.attribute_names_seen.get()
            + self.metrics.metric_names_seen.get()
            + self.metrics.event_names_seen.get();
        let matched = self.metrics.attribute_names_matched.get()
            + self.metrics.metric_names_matched.get()
            + self.metrics.event_names_matched.get();

        self.metrics.coverage_items_seen.set(seen);
        self.metrics.coverage_items_matched.set(matched);
        self.metrics.coverage_ratio.set(if seen == 0 {
            0.0
        } else {
            matched as f64 / seen as f64
        });
    }

    fn record_processing_error(&mut self, failure_kind: &'static str, error: String) {
        self.metrics.processing_errors.add(1);
        otap_df_telemetry::otel_error!(
            "weaver_live_check_processor.failure",
            failure_kind = failure_kind,
            error = %error
        );
    }

    fn build_findings(&mut self, pdata: OtapPdata) -> Result<Option<PreparedFindings>, String> {
        let (context, payload) = pdata.into_parts();
        let payload: OtlpProtoBytes = payload
            .try_into()
            .map_err(|error| format!("failed to normalize payload to OTLP bytes: {error}"))?;

        let message: OtlpProtoMessage = payload
            .try_into()
            .map_err(|error| format!("failed to decode normalized OTLP payload: {error}"))?;

        let mut mapped_samples = map_otlp_message(message);
        let mut findings = Vec::new();
        let mut stats = LiveCheckStatistics::Disabled(DisabledStatistics);

        for mapped_sample in &mut mapped_samples {
            self.update_coverage_for_sample(&mapped_sample.sample);

            let parent_signal = mapped_sample.sample.clone();
            if let Err(error) = mapped_sample.sample.run_live_check(
                &mut self.live_checker,
                &mut stats,
                None,
                &parent_signal,
            ) {
                self.record_processing_error(
                    "weaver_live_check_processor.live_check_failed",
                    error.to_string(),
                );
                continue;
            }

            collect_findings_from_sample(
                &mapped_sample.sample,
                &mapped_sample.context,
                &mut findings,
            );
        }

        let (findings, fingerprints) = self.filter_findings_for_emission(findings);

        if findings.is_empty() {
            return Ok(None);
        }

        let (logs, counts) = build_findings_logs(findings, &self.registry_url);
        let mut buffer = Vec::new();
        OtlpProtoMessage::Logs(logs)
            .encode(&mut buffer)
            .map_err(|error| format!("failed to encode finding logs: {error}"))?;

        Ok(Some(PreparedFindings {
            pdata: OtapPdata::new(
                context,
                OtapPayload::OtlpBytes(OtlpProtoBytes::ExportLogsRequest(buffer.into())),
            ),
            total: counts.information + counts.improvement + counts.violation,
            information: counts.information,
            improvement: counts.improvement,
            violation: counts.violation,
            fingerprints,
        }))
    }

    fn emit_findings_best_effort(
        &mut self,
        effect_handler: &local::EffectHandler<OtapPdata>,
        prepared: PreparedFindings,
    ) {
        match effect_handler.try_send_message_with_source_node_to(FINDINGS_PORT, prepared.pdata) {
            Ok(()) => {
                self.mark_findings_as_seen(&prepared.fingerprints);
                self.metrics.findings_emitted.add(prepared.total);
                self.metrics.findings_information.add(prepared.information);
                self.metrics.findings_improvement.add(prepared.improvement);
                self.metrics.findings_violation.add(prepared.violation);
            }
            Err(error) => {
                let error_text = error.to_string();
                self.metrics.findings_dropped.add(prepared.total);
                match error {
                    TypedError::Error(Error::UnknownOutputPort { .. }) => {
                        otap_df_telemetry::otel_warn!(
                            "weaver_live_check_processor.findings_port_missing",
                            error = %error_text
                        );
                    }
                    TypedError::ChannelSendError(otap_df_channel::error::SendError::Full(_)) => {
                        otap_df_telemetry::otel_warn!(
                            "weaver_live_check_processor.findings_backpressure",
                            error = %error_text
                        );
                    }
                    TypedError::ChannelSendError(otap_df_channel::error::SendError::Closed(_)) => {
                        otap_df_telemetry::otel_warn!(
                            "weaver_live_check_processor.findings_port_closed",
                            error = %error_text
                        );
                    }
                    other => {
                        otap_df_telemetry::otel_warn!(
                            "weaver_live_check_processor.findings_send_failed",
                            error = %other
                        );
                    }
                }
            }
        }
    }

    fn filter_findings_for_emission(
        &self,
        findings: Vec<EmittedFinding>,
    ) -> (Vec<EmittedFinding>, Vec<FindingFingerprint>) {
        let mut batch_seen = HashSet::new();
        let mut filtered = Vec::new();
        let mut fingerprints = Vec::new();

        for finding in findings {
            let fingerprint = FindingFingerprint::from_policy_finding(&finding.finding);
            if self.seen_findings.contains(&fingerprint) || !batch_seen.insert(fingerprint.clone())
            {
                continue;
            }

            fingerprints.push(fingerprint);
            filtered.push(finding);
        }

        (filtered, fingerprints)
    }

    fn mark_findings_as_seen(&mut self, fingerprints: &[FindingFingerprint]) {
        self.seen_findings.extend(fingerprints.iter().cloned());
    }
}

impl FindingFingerprint {
    fn from_policy_finding(finding: &PolicyFinding) -> Self {
        Self {
            id: finding.id.clone(),
            level: finding.level.clone(),
            message: finding.message.clone(),
            signal_type: finding.signal_type.clone(),
            signal_name: finding.signal_name.clone(),
            context_json: serde_json::to_string(&finding.context)
                .unwrap_or_else(|_| finding.context.to_string()),
        }
    }
}

fn build_live_checker(resolved_registry: ResolvedRegistry) -> Result<LiveChecker, ConfigError> {
    let registry = Arc::new(VersionedRegistry::V1(resolved_registry));
    let advisors: Vec<Box<dyn Advisor>> = vec![
        Box::new(DeprecatedAdvisor),
        Box::new(EnumAdvisor),
        Box::new(StabilityAdvisor),
        Box::new(TypeAdvisor),
    ];

    let mut live_checker = LiveChecker::new(registry, advisors);
    let none_path = None::<std::path::PathBuf>;
    let rego_advisor =
        RegoAdvisor::new(&live_checker, &none_path, &none_path).map_err(|error| {
            ConfigError::InvalidUserConfig {
                error: format!("failed to initialize default Rego advisor: {error}"),
            }
        })?;
    live_checker.add_advisor(Box::new(rego_advisor));

    Ok(live_checker)
}

fn collect_findings_from_sample(
    sample: &Sample,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    match sample {
        Sample::Attribute(attribute) => collect_attribute_findings(attribute, context, findings),
        Sample::Resource(resource) => collect_resource_findings(resource, context, findings),
        Sample::Log(log) => collect_log_findings(log, context, findings),
        Sample::Metric(metric) => collect_metric_findings(metric, context, findings),
        Sample::Span(span) => collect_span_findings(span, context, findings),
        Sample::SpanEvent(event) => collect_span_event_findings(event, context, findings),
        Sample::SpanLink(link) => collect_span_link_findings(link, context, findings),
    }
}

fn push_result_findings(
    result: Option<&LiveCheckResult>,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    if let Some(result) = result {
        findings.extend(
            result
                .all_advice
                .iter()
                .cloned()
                .map(|finding| EmittedFinding {
                    finding,
                    context: context.clone(),
                }),
        );
    }
}

fn collect_attribute_findings(
    attribute: &SampleAttribute,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    push_result_findings(attribute.live_check_result.as_ref(), context, findings);
}

fn collect_resource_findings(
    resource: &SampleResource,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    push_result_findings(resource.live_check_result.as_ref(), context, findings);
    for attribute in &resource.attributes {
        collect_attribute_findings(attribute, context, findings);
    }
}

fn collect_log_findings(
    log: &SampleLog,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    push_result_findings(log.live_check_result.as_ref(), context, findings);
    for attribute in &log.attributes {
        collect_attribute_findings(attribute, context, findings);
    }
}

fn collect_metric_findings(
    metric: &SampleMetric,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    push_result_findings(metric.live_check_result.as_ref(), context, findings);
    if let Some(data_points) = &metric.data_points {
        match data_points {
            DataPoints::Number(points) => {
                for point in points {
                    collect_number_point_findings(point, context, findings);
                }
            }
            DataPoints::Histogram(points) => {
                for point in points {
                    collect_histogram_point_findings(point, context, findings);
                }
            }
            DataPoints::ExponentialHistogram(points) => {
                for point in points {
                    collect_exp_histogram_point_findings(point, context, findings);
                }
            }
        }
    }
}

fn collect_number_point_findings(
    point: &SampleNumberDataPoint,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    push_result_findings(point.live_check_result.as_ref(), context, findings);
    for attribute in &point.attributes {
        collect_attribute_findings(attribute, context, findings);
    }
    for exemplar in &point.exemplars {
        collect_exemplar_findings(exemplar, context, findings);
    }
}

fn collect_histogram_point_findings(
    point: &SampleHistogramDataPoint,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    push_result_findings(point.live_check_result.as_ref(), context, findings);
    for attribute in &point.attributes {
        collect_attribute_findings(attribute, context, findings);
    }
    for exemplar in &point.exemplars {
        collect_exemplar_findings(exemplar, context, findings);
    }
}

fn collect_exp_histogram_point_findings(
    point: &SampleExponentialHistogramDataPoint,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    push_result_findings(point.live_check_result.as_ref(), context, findings);
    for attribute in &point.attributes {
        collect_attribute_findings(attribute, context, findings);
    }
    for exemplar in &point.exemplars {
        collect_exemplar_findings(exemplar, context, findings);
    }
}

fn collect_exemplar_findings(
    exemplar: &SampleExemplar,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    push_result_findings(exemplar.live_check_result.as_ref(), context, findings);
    for attribute in &exemplar.filtered_attributes {
        collect_attribute_findings(attribute, context, findings);
    }
}

fn collect_span_findings(
    span: &SampleSpan,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    push_result_findings(span.live_check_result.as_ref(), context, findings);
    for attribute in &span.attributes {
        collect_attribute_findings(attribute, context, findings);
    }
    for event in &span.span_events {
        collect_span_event_findings(event, context, findings);
    }
    for link in &span.span_links {
        collect_span_link_findings(link, context, findings);
    }
}

fn collect_span_event_findings(
    event: &SampleSpanEvent,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    push_result_findings(event.live_check_result.as_ref(), context, findings);
    for attribute in &event.attributes {
        collect_attribute_findings(attribute, context, findings);
    }
}

fn collect_span_link_findings(
    link: &SampleSpanLink,
    context: &CorrelationContext,
    findings: &mut Vec<EmittedFinding>,
) {
    push_result_findings(link.live_check_result.as_ref(), context, findings);
    for attribute in &link.attributes {
        collect_attribute_findings(attribute, context, findings);
    }
}

fn build_findings_logs(
    findings: Vec<EmittedFinding>,
    registry_url: &str,
) -> (LogsData, FindingLevelCounts) {
    let mut groups: HashMap<CorrelationKey, GroupedFindings> = HashMap::new();
    let now_unix_nano = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
        .min(u128::from(u64::MAX)) as u64;
    let mut counts = FindingLevelCounts::default();

    for finding in findings {
        match finding.finding.level {
            FindingLevel::Information => counts.information += 1,
            FindingLevel::Improvement => counts.improvement += 1,
            FindingLevel::Violation => counts.violation += 1,
        }

        let key = CorrelationKey {
            resource: finding
                .context
                .resource
                .as_ref()
                .map_or_else(Vec::new, ProstMessage::encode_to_vec),
            resource_schema_url: finding.context.resource_schema_url.clone(),
            scope: finding
                .context
                .scope
                .as_ref()
                .map_or_else(Vec::new, ProstMessage::encode_to_vec),
            scope_schema_url: finding.context.scope_schema_url.clone(),
        };

        let group = groups.entry(key).or_insert_with(|| GroupedFindings {
            resource: finding.context.resource.clone(),
            resource_schema_url: finding.context.resource_schema_url.clone(),
            scope: finding.context.scope.clone(),
            scope_schema_url: finding.context.scope_schema_url.clone(),
            log_records: Vec::new(),
        });
        group
            .log_records
            .push(finding_to_log_record(&finding, registry_url, now_unix_nano));
    }

    let logs = LogsData {
        resource_logs: groups
            .into_values()
            .map(|group| ResourceLogs {
                resource: group.resource,
                scope_logs: vec![ScopeLogs {
                    scope: group.scope,
                    log_records: group.log_records,
                    schema_url: group.scope_schema_url,
                }],
                schema_url: group.resource_schema_url,
            })
            .collect(),
    };

    (logs, counts)
}

fn finding_to_log_record(
    finding: &EmittedFinding,
    registry_url: &str,
    now_unix_nano: u64,
) -> LogRecord {
    let (severity_number, severity_text) = match finding.finding.level {
        FindingLevel::Violation => (SeverityNumber::Error as i32, "ERROR"),
        FindingLevel::Improvement => (SeverityNumber::Warn as i32, "WARN"),
        FindingLevel::Information => (SeverityNumber::Info as i32, "INFO"),
    };

    let mut attributes = vec![
        string_key_value("weaver.finding.id", &finding.finding.id),
        string_key_value("weaver.finding.level", &finding.finding.level.to_string()),
        string_key_value("weaver.registry.url", registry_url),
        string_key_value(
            "weaver.finding.context_json",
            &serde_json::to_string(&finding.finding.context)
                .unwrap_or_else(|_| finding.finding.context.to_string()),
        ),
    ];

    if let Some(signal_type) = &finding.finding.signal_type {
        attributes.push(string_key_value("weaver.signal.type", signal_type));
    }

    if let Some(signal_name) = &finding.finding.signal_name {
        attributes.push(string_key_value("weaver.signal.name", signal_name));
    }

    LogRecord {
        time_unix_nano: now_unix_nano,
        observed_time_unix_nano: now_unix_nano,
        severity_number,
        severity_text: severity_text.to_owned(),
        body: Some(string_any_value(&finding.finding.message)),
        attributes,
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: finding.context.trace_id.clone(),
        span_id: finding.context.span_id.clone(),
        event_name: FINDING_EVENT_NAME.to_owned(),
    }
}

fn string_any_value(value: &str) -> AnyValue {
    AnyValue {
        value: Some(
            otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value::StringValue(
                value.to_owned(),
            ),
        ),
    }
}

fn string_key_value(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(string_any_value(value)),
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for WeaverLiveCheckProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        match msg {
            Message::Control(control) => {
                if let NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                } = control
                {
                    let _ = metrics_reporter.report(&mut self.metrics);
                }
                Ok(())
            }
            Message::PData(pdata) => {
                self.metrics.batches_processed.add(1);
                self.metrics.items_processed.add(pdata.num_items() as u64);

                let findings_input = pdata.clone_without_context();

                effect_handler
                    .send_message_with_source_node(pdata)
                    .await
                    .map_err(|error| Error::ProcessorError {
                        processor: effect_handler.processor_id(),
                        kind: ProcessorErrorKind::Transport,
                        error: error.to_string(),
                        source_detail: String::new(),
                    })?;

                match self.build_findings(findings_input) {
                    Ok(Some(prepared)) => self.emit_findings_best_effort(effect_handler, prepared),
                    Ok(None) => {}
                    Err(error) => {
                        self.record_processing_error(
                            "weaver_live_check_processor.normalization_failed",
                            error,
                        );
                    }
                }

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_channel::mpsc;
    use otap_df_engine::effect_handler::SourceTagging;
    use otap_df_engine::local::message::LocalSender;
    use otap_df_engine::local::processor::Processor;
    use otap_df_engine::message::Sender;
    use otap_df_engine::testing::test_node;
    use otap_df_engine::testing::test_pipeline_ctx;
    use otap_df_pdata::proto::opentelemetry::common::v1::InstrumentationScope;
    use otap_df_pdata::proto::opentelemetry::logs::v1::{LogsData, ResourceLogs, ScopeLogs};
    use otap_df_pdata::proto::opentelemetry::metrics::v1::{
        Histogram, HistogramDataPoint, Metric, MetricsData, ResourceMetrics, ScopeMetrics,
        metric::Data,
    };
    use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
    use otap_df_pdata::testing::equiv::assert_equivalent;
    use otap_df_pdata::testing::round_trip::{otlp_message_to_bytes, otlp_to_otap};

    struct Harness {
        processor: WeaverLiveCheckProcessor,
        effect_handler: local::EffectHandler<OtapPdata>,
        default_rx: mpsc::Receiver<OtapPdata>,
        findings_rx: Option<mpsc::Receiver<OtapPdata>>,
    }

    fn cargo_home() -> std::path::PathBuf {
        std::env::var_os("CARGO_HOME")
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(std::path::PathBuf::from)
                    .map(|home| home.join(".cargo"))
            })
            .expect("CARGO_HOME or HOME must be set for tests")
    }

    fn find_weaver_registry_fixture() -> std::path::PathBuf {
        let checkouts = cargo_home().join("git").join("checkouts");
        let entries = std::fs::read_dir(&checkouts).expect("cargo git checkouts should exist");

        for entry in entries.flatten() {
            let Ok(revisions) = std::fs::read_dir(entry.path()) else {
                continue;
            };
            for revision in revisions.flatten() {
                let candidate = revision
                    .path()
                    .join("crates/weaver_codegen_test/semconv_registry");
                if candidate.is_dir() {
                    return candidate;
                }
            }
        }

        panic!("failed to locate a local weaver semconv test fixture under {checkouts:?}");
    }

    fn processor_config_value() -> Value {
        serde_json::json!({
            "registry_path": find_weaver_registry_fixture().display().to_string()
        })
    }

    fn build_harness(include_findings: bool, findings_capacity: usize) -> Harness {
        let (pipeline_ctx, _registry) = test_pipeline_ctx();
        let processor =
            WeaverLiveCheckProcessor::from_config(pipeline_ctx, &processor_config_value())
                .expect("processor config should load");

        let node = test_node("weaver-live-check");
        let (metrics_rx, metrics_reporter) =
            otap_df_telemetry::reporter::MetricsReporter::create_new_and_receiver(16);
        drop(metrics_rx);

        let (default_tx, default_rx) = mpsc::Channel::new(16);
        let mut msg_senders = HashMap::new();
        let _ = msg_senders.insert(
            "default".into(),
            Sender::Local(LocalSender::mpsc(default_tx)),
        );

        let findings_rx = if include_findings {
            let (findings_tx, findings_rx) = mpsc::Channel::new(findings_capacity);
            let _ = msg_senders.insert(
                FINDINGS_PORT.into(),
                Sender::Local(LocalSender::mpsc(findings_tx)),
            );
            Some(findings_rx)
        } else {
            None
        };

        let mut effect_handler =
            local::EffectHandler::new(node, msg_senders, Some("default".into()), metrics_reporter);
        effect_handler.set_source_tagging(SourceTagging::Disabled);

        Harness {
            processor,
            effect_handler,
            default_rx,
            findings_rx,
        }
    }

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(string_any_value(value)),
        }
    }

    fn missing_log_message() -> OtlpProtoMessage {
        missing_log_message_with_signal("missing.event", "missing.attr")
    }

    fn missing_log_message_with_signal(event_name: &str, attribute_name: &str) -> OtlpProtoMessage {
        OtlpProtoMessage::Logs(LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "checkout")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "scope-a".to_owned(),
                        version: "1.0.0".to_owned(),
                        attributes: vec![kv("scope.attr", "x")],
                        dropped_attributes_count: 0,
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 0,
                        observed_time_unix_nano: 0,
                        severity_number: SeverityNumber::Info as i32,
                        severity_text: "INFO".to_owned(),
                        body: Some(string_any_value("hello")),
                        attributes: vec![kv(attribute_name, "x")],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0x11; 16],
                        span_id: vec![0x22; 8],
                        event_name: event_name.to_owned(),
                    }],
                    schema_url: "scope-schema".to_owned(),
                }],
                schema_url: "resource-schema".to_owned(),
            }],
        })
    }

    fn matching_metric_message() -> OtlpProtoMessage {
        OtlpProtoMessage::Metrics(MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "checkout")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: "http.server.request.duration".to_owned(),
                        description: String::new(),
                        unit: "s".to_owned(),
                        metadata: Vec::new(),
                        data: Some(Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes: vec![kv("http.request.method", "GET")],
                                start_time_unix_nano: 0,
                                time_unix_nano: 0,
                                count: 1,
                                sum: Some(0.25),
                                bucket_counts: vec![1],
                                explicit_bounds: vec![1.0],
                                exemplars: Vec::new(),
                                flags: 0,
                                min: Some(0.25),
                                max: Some(0.25),
                            }],
                            aggregation_temporality: 2,
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        })
    }

    fn decode_otlp_message(pdata: OtapPdata) -> (otap_df_otap::pdata::Context, OtlpProtoMessage) {
        let (context, payload) = pdata.into_parts();
        let bytes: OtlpProtoBytes = payload.try_into().expect("payload should convert to OTLP");
        let message: OtlpProtoMessage = bytes.try_into().expect("OTLP bytes should decode");
        (context, message)
    }

    fn only_findings_message(receiver: &mut mpsc::Receiver<OtapPdata>) -> OtlpProtoMessage {
        let pdata = receiver.try_recv().expect("findings should be available");
        let (_, message) = decode_otlp_message(pdata);
        message
    }

    fn extract_finding_records(message: OtlpProtoMessage) -> Vec<LogRecord> {
        let OtlpProtoMessage::Logs(logs) = message else {
            panic!("expected logs output");
        };

        logs.resource_logs
            .into_iter()
            .flat_map(|resource_logs| resource_logs.scope_logs)
            .flat_map(|scope_logs| scope_logs.log_records)
            .collect()
    }

    fn finding_attr_value<'a>(record: &'a LogRecord, key: &str) -> Option<&'a str> {
        record.attributes.iter().find_map(|attribute| {
            if attribute.key != key {
                return None;
            }

            match attribute.value.as_ref()?.value.as_ref()? {
                otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value::StringValue(
                    value,
                ) => Some(value.as_str()),
                _ => None,
            }
        })
    }

    fn finding_record_signatures(records: &[LogRecord]) -> Vec<String> {
        let mut signatures = records
            .iter()
            .map(|record| {
                format!(
                    "{:?}|{}|{:?}|{:?}|{:?}|{:?}",
                    record.severity_number,
                    record.event_name,
                    record
                        .body
                        .as_ref()
                        .and_then(|body| body.value.as_ref())
                        .cloned(),
                    record.attributes,
                    record.trace_id,
                    record.span_id
                )
            })
            .collect::<Vec<_>>();
        signatures.sort();
        signatures
    }

    fn synthetic_context() -> CorrelationContext {
        CorrelationContext {
            resource: None,
            resource_schema_url: String::new(),
            scope: None,
            scope_schema_url: String::new(),
            trace_id: Vec::new(),
            span_id: Vec::new(),
        }
    }

    fn synthetic_finding(
        level: FindingLevel,
        id: &str,
        signal_name: Option<&str>,
    ) -> EmittedFinding {
        EmittedFinding {
            finding: PolicyFinding {
                id: id.to_owned(),
                context: serde_json::json!({
                    "finding_id": id,
                    "signal_name": signal_name,
                }),
                message: format!("{level}:{id}:{}", signal_name.unwrap_or_default()),
                level,
                signal_type: Some("log".to_owned()),
                signal_name: signal_name.map(str::to_owned),
            },
            context: synthetic_context(),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn processor_passes_through_and_emits_findings() {
        let mut harness = build_harness(true, 16);
        let input = missing_log_message();

        harness
            .processor
            .process(
                Message::PData(OtapPdata::new_default(otlp_message_to_bytes(&input).into())),
                &mut harness.effect_handler,
            )
            .await
            .expect("processor should succeed");

        let (_, passthrough) =
            decode_otlp_message(harness.default_rx.try_recv().expect("default output"));
        assert_equivalent(
            std::slice::from_ref(&passthrough),
            std::slice::from_ref(&input),
        );

        let findings = extract_finding_records(only_findings_message(
            harness.findings_rx.as_mut().expect("findings receiver"),
        ));
        assert_eq!(findings.len(), 4);
        assert!(
            findings
                .iter()
                .all(|record| record.event_name == FINDING_EVENT_NAME)
        );
        assert!(
            findings
                .iter()
                .any(|record| record.severity_number == SeverityNumber::Error as i32)
        );
        assert!(
            findings
                .iter()
                .filter(|record| finding_attr_value(record, "weaver.signal.type") == Some("log"))
                .all(|record| record.trace_id == vec![0x11; 16])
        );
        assert!(
            findings
                .iter()
                .filter(|record| finding_attr_value(record, "weaver.signal.type") == Some("log"))
                .all(|record| record.span_id == vec![0x22; 8])
        );

        let first = &findings[0];
        assert!(
            first
                .attributes
                .iter()
                .any(|attr| attr.key == "weaver.finding.id"),
            "finding id attribute must be present"
        );
        assert!(
            first
                .attributes
                .iter()
                .any(|attr| attr.key == "weaver.registry.url"),
            "registry url attribute must be present"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn processor_emits_identical_findings_for_otlp_and_otap_inputs() {
        let message = missing_log_message();

        let mut otlp_harness = build_harness(true, 16);
        otlp_harness
            .processor
            .process(
                Message::PData(OtapPdata::new_default(
                    otlp_message_to_bytes(&message).into(),
                )),
                &mut otlp_harness.effect_handler,
            )
            .await
            .expect("otlp input should process");

        let mut otap_harness = build_harness(true, 16);
        otap_harness
            .processor
            .process(
                Message::PData(OtapPdata::new_default(otlp_to_otap(&message).into())),
                &mut otap_harness.effect_handler,
            )
            .await
            .expect("otap input should process");

        let otlp_records = extract_finding_records(only_findings_message(
            otlp_harness
                .findings_rx
                .as_mut()
                .expect("findings receiver"),
        ));
        let otap_records = extract_finding_records(only_findings_message(
            otap_harness
                .findings_rx
                .as_mut()
                .expect("findings receiver"),
        ));

        assert_eq!(otlp_records.len(), otap_records.len());
        assert_eq!(
            finding_record_signatures(&otlp_records),
            finding_record_signatures(&otap_records)
        );
    }

    #[test]
    fn repeated_non_violations_are_emitted_only_once() {
        let mut processor = build_harness(false, 0).processor;

        let (first_batch, first_fingerprints) = processor.filter_findings_for_emission(vec![
            synthetic_finding(
                FindingLevel::Improvement,
                "duplicate_improvement",
                Some("event-a"),
            ),
            synthetic_finding(
                FindingLevel::Improvement,
                "duplicate_improvement",
                Some("event-a"),
            ),
        ]);
        assert_eq!(first_batch.len(), 1);
        processor.mark_findings_as_seen(&first_fingerprints);

        let (second_batch, _) = processor.filter_findings_for_emission(vec![synthetic_finding(
            FindingLevel::Improvement,
            "duplicate_improvement",
            Some("event-a"),
        )]);
        assert!(second_batch.is_empty());

        let (third_batch, _) = processor.filter_findings_for_emission(vec![synthetic_finding(
            FindingLevel::Information,
            "duplicate_improvement",
            Some("event-a"),
        )]);
        assert_eq!(third_batch.len(), 1);
    }

    #[test]
    fn repeated_violations_are_emitted_only_once() {
        let mut processor = build_harness(false, 0).processor;

        let (first_batch, first_fingerprints) =
            processor.filter_findings_for_emission(vec![synthetic_finding(
                FindingLevel::Violation,
                "missing_attribute",
                Some("event-a"),
            )]);
        assert_eq!(first_batch.len(), 1);
        processor.mark_findings_as_seen(&first_fingerprints);

        let (second_batch, _) = processor.filter_findings_for_emission(vec![synthetic_finding(
            FindingLevel::Violation,
            "missing_attribute",
            Some("event-a"),
        )]);
        assert!(second_batch.is_empty());
    }

    #[test]
    fn dropped_findings_are_retried_until_emitted() {
        let mut processor = build_harness(false, 0).processor;
        let finding = synthetic_finding(
            FindingLevel::Violation,
            "missing_attribute",
            Some("event-a"),
        );

        let (first_attempt, _) = processor.filter_findings_for_emission(vec![finding.clone()]);
        assert_eq!(first_attempt.len(), 1);

        let (second_attempt, second_fingerprints) =
            processor.filter_findings_for_emission(vec![finding.clone()]);
        assert_eq!(second_attempt.len(), 1);
        processor.mark_findings_as_seen(&second_fingerprints);

        let (third_attempt, _) = processor.filter_findings_for_emission(vec![finding]);
        assert!(third_attempt.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn findings_port_missing_is_best_effort() {
        let mut harness = build_harness(false, 0);

        harness
            .processor
            .process(
                Message::PData(OtapPdata::new_default(
                    otlp_message_to_bytes(&missing_log_message()).into(),
                )),
                &mut harness.effect_handler,
            )
            .await
            .expect("processor should still succeed");

        assert!(harness.default_rx.try_recv().is_ok());
        assert_eq!(harness.processor.metrics.findings_dropped.get(), 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closed_findings_port_is_best_effort() {
        let (pipeline_ctx, _registry) = test_pipeline_ctx();
        let processor =
            WeaverLiveCheckProcessor::from_config(pipeline_ctx, &processor_config_value())
                .expect("processor config should load");
        let node = test_node("weaver-live-check");
        let (metrics_rx, metrics_reporter) =
            otap_df_telemetry::reporter::MetricsReporter::create_new_and_receiver(16);
        drop(metrics_rx);
        let (default_tx, default_rx) = mpsc::Channel::new(16);
        let (findings_tx, findings_rx) = mpsc::Channel::new(16);
        drop(findings_rx);

        let mut senders = HashMap::new();
        let _ = senders.insert(
            "default".into(),
            Sender::Local(LocalSender::mpsc(default_tx)),
        );
        let _ = senders.insert(
            FINDINGS_PORT.into(),
            Sender::Local(LocalSender::mpsc(findings_tx)),
        );
        let mut effect_handler =
            local::EffectHandler::new(node, senders, Some("default".into()), metrics_reporter);
        effect_handler.set_source_tagging(SourceTagging::Disabled);

        let mut processor = processor;
        processor
            .process(
                Message::PData(OtapPdata::new_default(
                    otlp_message_to_bytes(&missing_log_message()).into(),
                )),
                &mut effect_handler,
            )
            .await
            .expect("processor should still succeed");

        assert!(default_rx.try_recv().is_ok());
        assert_eq!(processor.metrics.findings_dropped.get(), 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_findings_port_drops_side_stream_without_blocking_main_flow() {
        let mut harness = build_harness(true, 1);
        let first_input =
            OtapPdata::new_default(otlp_message_to_bytes(&missing_log_message()).into());
        let second_input = OtapPdata::new_default(
            otlp_message_to_bytes(&missing_log_message_with_signal(
                "missing.event.variant",
                "missing.attr.variant",
            ))
            .into(),
        );

        harness
            .processor
            .process(Message::PData(first_input), &mut harness.effect_handler)
            .await
            .expect("first batch should process");
        harness
            .processor
            .process(Message::PData(second_input), &mut harness.effect_handler)
            .await
            .expect("second batch should process");

        assert_eq!(harness.processor.metrics.findings_emitted.get(), 4);
        assert!(harness.processor.metrics.findings_dropped.get() > 0);
        assert!(harness.default_rx.try_recv().is_ok());
        assert!(harness.default_rx.try_recv().is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn findings_context_is_reset_before_side_stream_emission() {
        let mut harness = build_harness(true, 16);
        let mut input =
            OtapPdata::new_default(otlp_message_to_bytes(&missing_log_message()).into());
        input.context_mut().set_source_node(42);

        harness
            .processor
            .process(Message::PData(input), &mut harness.effect_handler)
            .await
            .expect("processor should succeed");

        let findings_pdata = harness
            .findings_rx
            .as_mut()
            .expect("findings receiver")
            .try_recv()
            .expect("findings output");
        assert!(
            !findings_pdata.has_context_frames(),
            "findings side-stream must not inherit upstream ack/nack frames"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coverage_metrics_track_registry_hits_and_misses() {
        let mut harness = build_harness(true, 16);

        harness
            .processor
            .process(
                Message::PData(OtapPdata::new_default(
                    otlp_message_to_bytes(&matching_metric_message()).into(),
                )),
                &mut harness.effect_handler,
            )
            .await
            .expect("matching metric should process");

        harness
            .processor
            .process(
                Message::PData(OtapPdata::new_default(
                    otlp_message_to_bytes(&missing_log_message()).into(),
                )),
                &mut harness.effect_handler,
            )
            .await
            .expect("missing log should process");

        assert_eq!(harness.processor.metrics.metric_names_seen.get(), 1);
        assert_eq!(harness.processor.metrics.metric_names_matched.get(), 1);
        assert_eq!(harness.processor.metrics.attribute_names_seen.get(), 5);
        assert_eq!(harness.processor.metrics.attribute_names_matched.get(), 1);
        assert_eq!(harness.processor.metrics.event_names_seen.get(), 1);
        assert_eq!(harness.processor.metrics.event_names_matched.get(), 0);
        assert_eq!(harness.processor.metrics.coverage_items_seen.get(), 7);
        assert_eq!(harness.processor.metrics.coverage_items_matched.get(), 2);
        assert!(
            (harness.processor.metrics.coverage_ratio.get() - (2.0 / 7.0)).abs() < f64::EPSILON
        );
    }
}
