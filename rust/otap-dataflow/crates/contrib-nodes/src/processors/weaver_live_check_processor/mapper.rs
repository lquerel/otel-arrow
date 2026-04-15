// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTLP-to-Weaver sample mapping for the Weaver live-check processor.

use chrono::{TimeZone, Utc};
use otap_df_pdata::proto::OtlpProtoMessage;
use otap_df_pdata::proto::opentelemetry::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use otap_df_pdata::proto::opentelemetry::logs::v1::{LogRecord, LogsData};
use otap_df_pdata::proto::opentelemetry::metrics::v1::{
    Exemplar, ExponentialHistogramDataPoint, HistogramDataPoint, Metric, MetricsData,
    NumberDataPoint, metric::Data,
};
use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
use otap_df_pdata::proto::opentelemetry::trace::v1::{
    Span, Status, TracesData,
    span::{Link, SpanKind},
    status::StatusCode as OtlpStatusCode,
};
use serde_json::{Value as JsonValue, json};
use weaver_live_check::Sample;
use weaver_live_check::sample_attribute::SampleAttribute;
use weaver_live_check::sample_log::SampleLog;
use weaver_live_check::sample_metric::{
    DataPoints, SampleExemplar, SampleExponentialHistogramBuckets,
    SampleExponentialHistogramDataPoint, SampleHistogramDataPoint, SampleInstrument, SampleMetric,
    SampleNumberDataPoint,
};
use weaver_live_check::sample_resource::SampleResource;
use weaver_live_check::sample_span::{
    SampleSpan, SampleSpanEvent, SampleSpanLink, Status as SampleStatus,
    StatusCode as SampleStatusCode,
};
use weaver_semconv::group::{InstrumentSpec, SpanKindSpec};

/// Correlated OTLP context for an emitted finding.
#[derive(Debug, Clone, PartialEq)]
pub struct CorrelationContext {
    /// Original resource, if present.
    pub resource: Option<Resource>,
    /// Original resource schema URL.
    pub resource_schema_url: String,
    /// Original scope, if present.
    pub scope: Option<InstrumentationScope>,
    /// Original scope schema URL.
    pub scope_schema_url: String,
    /// Source trace id from the originating signal, if present.
    pub trace_id: Vec<u8>,
    /// Source span id from the originating signal, if present.
    pub span_id: Vec<u8>,
}

impl CorrelationContext {
    fn new(
        resource: Option<Resource>,
        resource_schema_url: String,
        scope: Option<InstrumentationScope>,
        scope_schema_url: String,
        trace_id: Vec<u8>,
        span_id: Vec<u8>,
    ) -> Self {
        Self {
            resource,
            resource_schema_url,
            scope,
            scope_schema_url,
            trace_id,
            span_id,
        }
    }

    fn for_resource(resource: Option<Resource>, resource_schema_url: String) -> Self {
        Self::new(
            resource,
            resource_schema_url,
            None,
            String::new(),
            Vec::new(),
            Vec::new(),
        )
    }

    fn with_scope(&self, scope: Option<InstrumentationScope>, scope_schema_url: String) -> Self {
        Self::new(
            self.resource.clone(),
            self.resource_schema_url.clone(),
            scope,
            scope_schema_url,
            Vec::new(),
            Vec::new(),
        )
    }

    fn with_trace_span(&self, trace_id: &[u8], span_id: &[u8]) -> Self {
        Self::new(
            self.resource.clone(),
            self.resource_schema_url.clone(),
            self.scope.clone(),
            self.scope_schema_url.clone(),
            trace_id.to_vec(),
            span_id.to_vec(),
        )
    }
}

/// Weaver sample plus the OTLP context it came from.
#[derive(Debug, Clone, PartialEq)]
pub struct MappedSample {
    /// Root sample to pass into `weaver_live_check`.
    pub sample: Sample,
    /// Correlated OTLP context for findings emitted from this sample subtree.
    pub context: CorrelationContext,
}

/// Map an OTLP message to the root Weaver samples used by the live checker.
#[must_use]
pub fn map_otlp_message(message: OtlpProtoMessage) -> Vec<MappedSample> {
    match message {
        OtlpProtoMessage::Logs(logs) => map_logs(logs),
        OtlpProtoMessage::Metrics(metrics) => map_metrics(metrics),
        OtlpProtoMessage::Traces(traces) => map_traces(traces),
    }
}

fn map_logs(logs: LogsData) -> Vec<MappedSample> {
    let mut samples = Vec::new();

    for resource_logs in logs.resource_logs {
        let resource_context = CorrelationContext::for_resource(
            resource_logs.resource.clone(),
            resource_logs.schema_url.clone(),
        );

        if let Some(resource) = resource_logs.resource {
            samples.push(MappedSample {
                sample: Sample::Resource(SampleResource {
                    attributes: resource
                        .attributes
                        .iter()
                        .map(sample_attribute_from_key_value)
                        .collect(),
                    live_check_result: None,
                }),
                context: resource_context.clone(),
            });
        }

        for scope_logs in resource_logs.scope_logs {
            let scoped_context = resource_context
                .with_scope(scope_logs.scope.clone(), scope_logs.schema_url.clone());

            if let Some(scope) = &scope_logs.scope {
                for attribute in &scope.attributes {
                    samples.push(MappedSample {
                        sample: Sample::Attribute(sample_attribute_from_key_value(attribute)),
                        context: scoped_context.clone(),
                    });
                }
            }

            for log_record in &scope_logs.log_records {
                samples.push(MappedSample {
                    sample: Sample::Log(otlp_log_record_to_sample_log(log_record)),
                    context: scoped_context
                        .with_trace_span(&log_record.trace_id, &log_record.span_id),
                });
            }
        }
    }

    samples
}

fn map_metrics(metrics: MetricsData) -> Vec<MappedSample> {
    let mut samples = Vec::new();

    for resource_metrics in metrics.resource_metrics {
        let resource_context = CorrelationContext::for_resource(
            resource_metrics.resource.clone(),
            resource_metrics.schema_url.clone(),
        );

        if let Some(resource) = resource_metrics.resource {
            samples.push(MappedSample {
                sample: Sample::Resource(SampleResource {
                    attributes: resource
                        .attributes
                        .iter()
                        .map(sample_attribute_from_key_value)
                        .collect(),
                    live_check_result: None,
                }),
                context: resource_context.clone(),
            });
        }

        for scope_metrics in resource_metrics.scope_metrics {
            let scoped_context = resource_context.with_scope(
                scope_metrics.scope.clone(),
                scope_metrics.schema_url.clone(),
            );

            if let Some(scope) = &scope_metrics.scope {
                for attribute in &scope.attributes {
                    samples.push(MappedSample {
                        sample: Sample::Attribute(sample_attribute_from_key_value(attribute)),
                        context: scoped_context.clone(),
                    });
                }
            }

            for metric in &scope_metrics.metrics {
                samples.push(MappedSample {
                    sample: Sample::Metric(otlp_metric_to_sample(metric)),
                    context: scoped_context.clone(),
                });
            }
        }
    }

    samples
}

fn map_traces(traces: TracesData) -> Vec<MappedSample> {
    let mut samples = Vec::new();

    for resource_spans in traces.resource_spans {
        let resource_context = CorrelationContext::for_resource(
            resource_spans.resource.clone(),
            resource_spans.schema_url.clone(),
        );

        if let Some(resource) = resource_spans.resource {
            samples.push(MappedSample {
                sample: Sample::Resource(SampleResource {
                    attributes: resource
                        .attributes
                        .iter()
                        .map(sample_attribute_from_key_value)
                        .collect(),
                    live_check_result: None,
                }),
                context: resource_context.clone(),
            });
        }

        for scope_spans in resource_spans.scope_spans {
            let scoped_context = resource_context
                .with_scope(scope_spans.scope.clone(), scope_spans.schema_url.clone());

            if let Some(scope) = &scope_spans.scope {
                for attribute in &scope.attributes {
                    samples.push(MappedSample {
                        sample: Sample::Attribute(sample_attribute_from_key_value(attribute)),
                        context: scoped_context.clone(),
                    });
                }
            }

            for span in &scope_spans.spans {
                samples.push(MappedSample {
                    sample: Sample::Span(otlp_span_to_sample(span)),
                    context: scoped_context.with_trace_span(&span.trace_id, &span.span_id),
                });
            }
        }
    }

    samples
}

fn maybe_to_json(value: Option<AnyValue>) -> Option<JsonValue> {
    let value = value?;
    let value = value.value?;

    match value {
        otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value::StringValue(value) => {
            Some(JsonValue::String(value))
        }
        otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value::BoolValue(value) => {
            Some(JsonValue::Bool(value))
        }
        otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value::IntValue(value) => {
            Some(JsonValue::Number(value.into()))
        }
        otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value::DoubleValue(value) => {
            Some(json!(value))
        }
        otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value::ArrayValue(values) => {
            Some(JsonValue::Array(
                values
                    .values
                    .into_iter()
                    .filter_map(|value| maybe_to_json(Some(value)))
                    .collect(),
            ))
        }
        otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value::KvlistValue(_)
        | otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value::BytesValue(_) => None,
    }
}

/// Convert an OTLP `KeyValue` to a `SampleAttribute`.
#[must_use]
pub fn sample_attribute_from_key_value(key_value: &KeyValue) -> SampleAttribute {
    let value = maybe_to_json(key_value.value.clone());
    let r#type = value.as_ref().and_then(SampleAttribute::infer_type);

    SampleAttribute {
        name: key_value.key.clone(),
        value,
        r#type,
        live_check_result: None,
    }
}

fn span_kind_from_otlp_kind(kind: SpanKind) -> SpanKindSpec {
    match kind {
        SpanKind::Server => SpanKindSpec::Server,
        SpanKind::Client => SpanKindSpec::Client,
        SpanKind::Producer => SpanKindSpec::Producer,
        SpanKind::Consumer => SpanKindSpec::Consumer,
        SpanKind::Internal | SpanKind::Unspecified => SpanKindSpec::Internal,
    }
}

fn status_from_otlp_status(status: Option<Status>) -> Option<SampleStatus> {
    let status = status?;
    let code = match OtlpStatusCode::try_from(status.code).unwrap_or(OtlpStatusCode::Unset) {
        OtlpStatusCode::Unset => SampleStatusCode::Unset,
        OtlpStatusCode::Ok => SampleStatusCode::Ok,
        OtlpStatusCode::Error => SampleStatusCode::Error,
    };

    Some(SampleStatus {
        code,
        message: status.message,
    })
}

fn otlp_span_to_sample(span: &Span) -> SampleSpan {
    SampleSpan {
        name: span.name.clone(),
        kind: span_kind_from_otlp_kind(SpanKind::try_from(span.kind).unwrap_or(SpanKind::Internal)),
        status: status_from_otlp_status(span.status.clone()),
        attributes: span
            .attributes
            .iter()
            .map(sample_attribute_from_key_value)
            .collect(),
        span_events: span
            .events
            .iter()
            .map(|event| SampleSpanEvent {
                name: event.name.clone(),
                attributes: event
                    .attributes
                    .iter()
                    .map(sample_attribute_from_key_value)
                    .collect(),
                live_check_result: None,
            })
            .collect(),
        span_links: span.links.iter().map(otlp_span_link_to_sample).collect(),
        live_check_result: None,
    }
}

fn otlp_span_link_to_sample(link: &Link) -> SampleSpanLink {
    SampleSpanLink {
        attributes: link
            .attributes
            .iter()
            .map(sample_attribute_from_key_value)
            .collect(),
        live_check_result: None,
    }
}

fn otlp_metric_to_sample(metric: &Metric) -> SampleMetric {
    SampleMetric {
        name: metric.name.clone(),
        instrument: otlp_data_to_instrument(&metric.data),
        unit: metric.unit.clone(),
        data_points: otlp_data_to_data_points(&metric.data),
        live_check_result: None,
    }
}

fn otlp_data_to_instrument(data: &Option<Data>) -> SampleInstrument {
    match data {
        Some(Data::Sum(sum)) => {
            if sum.is_monotonic {
                SampleInstrument::Supported(InstrumentSpec::Counter)
            } else {
                SampleInstrument::Supported(InstrumentSpec::UpDownCounter)
            }
        }
        Some(Data::Gauge(_)) => SampleInstrument::Supported(InstrumentSpec::Gauge),
        Some(Data::Histogram(_)) | Some(Data::ExponentialHistogram(_)) => {
            SampleInstrument::Supported(InstrumentSpec::Histogram)
        }
        Some(Data::Summary(_)) => SampleInstrument::Unsupported("Summary".to_owned()),
        None => SampleInstrument::Unsupported("Unspecified".to_owned()),
    }
}

fn otlp_data_to_data_points(data: &Option<Data>) -> Option<DataPoints> {
    match data {
        Some(Data::Sum(sum)) => Some(number_data_points_to_sample(&sum.data_points)),
        Some(Data::Gauge(gauge)) => Some(number_data_points_to_sample(&gauge.data_points)),
        Some(Data::Histogram(histogram)) => {
            Some(histogram_data_points_to_sample(&histogram.data_points))
        }
        Some(Data::ExponentialHistogram(histogram)) => {
            Some(exp_histogram_data_points_to_sample(&histogram.data_points))
        }
        Some(Data::Summary(_)) | None => None,
    }
}

fn number_data_points_to_sample(points: &[NumberDataPoint]) -> DataPoints {
    DataPoints::Number(
        points
            .iter()
            .map(|point| SampleNumberDataPoint {
                attributes: point
                    .attributes
                    .iter()
                    .map(sample_attribute_from_key_value)
                    .collect(),
                value: match point.value {
                    Some(otap_df_pdata::proto::opentelemetry::metrics::v1::number_data_point::Value::AsDouble(
                        value,
                    )) => json!(value),
                    Some(otap_df_pdata::proto::opentelemetry::metrics::v1::number_data_point::Value::AsInt(
                        value,
                    )) => JsonValue::Number(value.into()),
                    None => JsonValue::Null,
                },
                flags: point.flags,
                exemplars: point
                    .exemplars
                    .iter()
                    .map(otlp_exemplar_to_sample)
                    .collect(),
                live_check_result: None,
            })
            .collect(),
    )
}

fn histogram_data_points_to_sample(points: &[HistogramDataPoint]) -> DataPoints {
    DataPoints::Histogram(
        points
            .iter()
            .map(|point| SampleHistogramDataPoint {
                attributes: point
                    .attributes
                    .iter()
                    .map(sample_attribute_from_key_value)
                    .collect(),
                count: point.count,
                sum: point.sum,
                bucket_counts: point.bucket_counts.clone(),
                explicit_bounds: point.explicit_bounds.clone(),
                min: point.min,
                max: point.max,
                flags: point.flags,
                exemplars: point
                    .exemplars
                    .iter()
                    .map(otlp_exemplar_to_sample)
                    .collect(),
                live_check_result: None,
            })
            .collect(),
    )
}

fn exp_histogram_data_points_to_sample(points: &[ExponentialHistogramDataPoint]) -> DataPoints {
    DataPoints::ExponentialHistogram(
        points
            .iter()
            .map(|point| SampleExponentialHistogramDataPoint {
                attributes: point
                    .attributes
                    .iter()
                    .map(sample_attribute_from_key_value)
                    .collect(),
                count: point.count,
                sum: point.sum,
                scale: point.scale,
                zero_count: point.zero_count,
                positive: point.positive.as_ref().map(|buckets| {
                    SampleExponentialHistogramBuckets {
                        offset: buckets.offset,
                        bucket_counts: buckets.bucket_counts.clone(),
                    }
                }),
                negative: point.negative.as_ref().map(|buckets| {
                    SampleExponentialHistogramBuckets {
                        offset: buckets.offset,
                        bucket_counts: buckets.bucket_counts.clone(),
                    }
                }),
                flags: point.flags,
                min: point.min,
                max: point.max,
                zero_threshold: point.zero_threshold,
                exemplars: point
                    .exemplars
                    .iter()
                    .map(otlp_exemplar_to_sample)
                    .collect(),
                live_check_result: None,
            })
            .collect(),
    )
}

fn otlp_exemplar_to_sample(exemplar: &Exemplar) -> SampleExemplar {
    SampleExemplar {
        filtered_attributes: exemplar
            .filtered_attributes
            .iter()
            .map(sample_attribute_from_key_value)
            .collect(),
        value: match exemplar.value {
            Some(otap_df_pdata::proto::opentelemetry::metrics::v1::exemplar::Value::AsDouble(
                value,
            )) => json!(value),
            Some(otap_df_pdata::proto::opentelemetry::metrics::v1::exemplar::Value::AsInt(
                value,
            )) => JsonValue::Number(value.into()),
            None => JsonValue::Null,
        },
        timestamp: unix_nanos_to_utc(exemplar.time_unix_nano),
        span_id: span_id_hex(&exemplar.span_id),
        trace_id: trace_id_hex(&exemplar.trace_id),
        live_check_result: None,
    }
}

fn unix_nanos_to_utc(time_unix_nano: u64) -> String {
    let Ok(nanos) = i64::try_from(time_unix_nano) else {
        return String::new();
    };

    Utc.timestamp_nanos(nanos).to_rfc3339()
}

fn span_id_hex(span_id: &[u8]) -> String {
    if span_id.len() != 8 {
        return String::new();
    }

    let Ok(bytes) = <[u8; 8]>::try_from(span_id) else {
        return String::new();
    };

    format!("{:016x}", u64::from_be_bytes(bytes))
}

fn trace_id_hex(trace_id: &[u8]) -> String {
    if trace_id.len() != 16 {
        return String::new();
    }

    let Ok(bytes) = <[u8; 16]>::try_from(trace_id) else {
        return String::new();
    };

    format!("{:032x}", u128::from_be_bytes(bytes))
}

fn otlp_log_record_to_sample_log(log_record: &LogRecord) -> SampleLog {
    SampleLog {
        event_name: log_record.event_name.clone(),
        severity_number: Some(log_record.severity_number),
        severity_text: Some(log_record.severity_text.clone()),
        body: log_record
            .body
            .as_ref()
            .and_then(|value| value.value.as_ref().map(|value| format!("{value:?}"))),
        attributes: log_record
            .attributes
            .iter()
            .map(sample_attribute_from_key_value)
            .collect(),
        trace_id: {
            let trace_id = trace_id_hex(&log_record.trace_id);
            if trace_id.is_empty() {
                None
            } else {
                Some(trace_id)
            }
        },
        span_id: {
            let span_id = span_id_hex(&log_record.span_id);
            if span_id.is_empty() {
                None
            } else {
                Some(span_id)
            }
        },
        live_check_result: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_pdata::proto::opentelemetry::common::v1::{AnyValue, InstrumentationScope};
    use otap_df_pdata::proto::opentelemetry::logs::v1::{LogsData, ResourceLogs, ScopeLogs};
    use otap_df_pdata::proto::opentelemetry::metrics::v1::{
        Histogram, Metric, MetricsData, ResourceMetrics, ScopeMetrics, Summary, metric::Data,
    };
    use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
    use otap_df_pdata::proto::opentelemetry::trace::v1::{
        ResourceSpans, ScopeSpans, Span, Status, TracesData, span, status,
    };

    fn string_value(value: &str) -> AnyValue {
        AnyValue {
            value: Some(
                otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value::StringValue(
                    value.to_owned(),
                ),
            ),
        }
    }

    fn key_value(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(string_value(value)),
        }
    }

    #[test]
    fn maps_logs_with_resource_scope_and_trace_context() {
        let message = OtlpProtoMessage::Logs(LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![key_value("service.name", "checkout")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "scope-a".to_owned(),
                        version: "1.2.3".to_owned(),
                        attributes: vec![key_value("scope.attr", "x")],
                        dropped_attributes_count: 0,
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 0,
                        observed_time_unix_nano: 0,
                        severity_number: 9,
                        severity_text: "INFO".to_owned(),
                        body: Some(string_value("hello")),
                        attributes: vec![key_value("http.request.method", "GET")],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0x11; 16],
                        span_id: vec![0x22; 8],
                        event_name: "missing.event".to_owned(),
                    }],
                    schema_url: "scope-schema".to_owned(),
                }],
                schema_url: "resource-schema".to_owned(),
            }],
        });

        let mapped = map_otlp_message(message);
        assert_eq!(mapped.len(), 3);

        assert!(matches!(mapped[0].sample, Sample::Resource(_)));
        assert!(matches!(mapped[1].sample, Sample::Attribute(_)));

        let Sample::Log(log) = &mapped[2].sample else {
            panic!("expected log sample");
        };
        assert_eq!(log.event_name, "missing.event");
        assert_eq!(
            log.trace_id.as_deref(),
            Some("11111111111111111111111111111111")
        );
        assert_eq!(log.span_id.as_deref(), Some("2222222222222222"));
        assert_eq!(mapped[2].context.resource_schema_url, "resource-schema");
        assert_eq!(mapped[2].context.scope_schema_url, "scope-schema");
        assert_eq!(
            mapped[2].context.scope.as_ref().expect("scope").name,
            "scope-a"
        );
    }

    #[test]
    fn maps_metric_instruments_and_unsupported_forms() {
        let message = OtlpProtoMessage::Metrics(MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![
                        Metric {
                            name: "http.server.request.duration".to_owned(),
                            description: String::new(),
                            unit: "s".to_owned(),
                            metadata: Vec::new(),
                            data: Some(Data::Histogram(Histogram {
                                data_points: vec![HistogramDataPoint {
                                    attributes: vec![key_value("http.request.method", "GET")],
                                    start_time_unix_nano: 0,
                                    time_unix_nano: 0,
                                    count: 1,
                                    sum: Some(1.5),
                                    bucket_counts: vec![1],
                                    explicit_bounds: vec![1.0],
                                    exemplars: vec![Exemplar {
                                        filtered_attributes: vec![key_value("http.request.method", "GET")],
                                        time_unix_nano: 5,
                                        span_id: vec![0x01; 8],
                                        trace_id: vec![0x02; 16],
                                        value: Some(
                                            otap_df_pdata::proto::opentelemetry::metrics::v1::exemplar::Value::AsDouble(
                                                2.5,
                                            ),
                                        ),
                                    }],
                                    flags: 7,
                                    min: Some(1.0),
                                    max: Some(2.0),
                                }],
                                aggregation_temporality: 2,
                            })),
                        },
                        Metric {
                            name: "summary.metric".to_owned(),
                            description: String::new(),
                            unit: String::new(),
                            metadata: Vec::new(),
                            data: Some(Data::Summary(Summary { data_points: Vec::new() })),
                        },
                        Metric {
                            name: "unspecified.metric".to_owned(),
                            description: String::new(),
                            unit: String::new(),
                            metadata: Vec::new(),
                            data: None,
                        },
                    ],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        });

        let mapped = map_otlp_message(message);
        assert_eq!(mapped.len(), 3);

        let Sample::Metric(histogram_metric) = &mapped[0].sample else {
            panic!("expected metric");
        };
        assert_eq!(
            histogram_metric.instrument,
            SampleInstrument::Supported(InstrumentSpec::Histogram)
        );
        let Some(DataPoints::Histogram(points)) = &histogram_metric.data_points else {
            panic!("expected histogram data points");
        };
        assert_eq!(points.len(), 1);
        assert_eq!(
            points[0].exemplars[0].trace_id,
            "02020202020202020202020202020202"
        );

        let Sample::Metric(summary_metric) = &mapped[1].sample else {
            panic!("expected metric");
        };
        assert_eq!(
            summary_metric.instrument,
            SampleInstrument::Unsupported("Summary".to_owned())
        );
        assert!(summary_metric.data_points.is_none());

        let Sample::Metric(unspecified_metric) = &mapped[2].sample else {
            panic!("expected metric");
        };
        assert_eq!(
            unspecified_metric.instrument,
            SampleInstrument::Unsupported("Unspecified".to_owned())
        );
    }

    #[test]
    fn maps_spans_with_events_and_links() {
        let message = OtlpProtoMessage::Traces(TracesData {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![0xaa; 16],
                        span_id: vec![0xbb; 8],
                        trace_state: String::new(),
                        parent_span_id: Vec::new(),
                        flags: 0,
                        name: "GET /checkout".to_owned(),
                        kind: SpanKind::Server as i32,
                        start_time_unix_nano: 0,
                        end_time_unix_nano: 1,
                        attributes: vec![key_value("http.request.method", "GET")],
                        dropped_attributes_count: 0,
                        events: vec![span::Event {
                            time_unix_nano: 0,
                            name: "exception".to_owned(),
                            attributes: vec![key_value("exception.type", "io")],
                            dropped_attributes_count: 0,
                        }],
                        dropped_events_count: 0,
                        links: vec![Link {
                            trace_id: vec![0xcc; 16],
                            span_id: vec![0xdd; 8],
                            trace_state: String::new(),
                            attributes: vec![key_value("link.attr", "y")],
                            dropped_attributes_count: 0,
                            flags: 0,
                        }],
                        dropped_links_count: 0,
                        status: Some(Status {
                            message: "boom".to_owned(),
                            code: status::StatusCode::Error as i32,
                        }),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        });

        let mapped = map_otlp_message(message);
        assert_eq!(mapped.len(), 1);

        let Sample::Span(span) = &mapped[0].sample else {
            panic!("expected span");
        };
        assert_eq!(span.kind, SpanKindSpec::Server);
        assert_eq!(
            span.status.as_ref().expect("status").code,
            SampleStatusCode::Error
        );
        assert_eq!(span.span_events.len(), 1);
        assert_eq!(span.span_events[0].name, "exception");
        assert_eq!(span.span_links.len(), 1);
        assert_eq!(span.span_links[0].attributes[0].name, "link.attr");
        assert_eq!(mapped[0].context.trace_id, vec![0xaa; 16]);
        assert_eq!(mapped[0].context.span_id, vec![0xbb; 8]);
    }

    #[test]
    fn unsupported_anyvalue_becomes_untyped_attribute() {
        let attribute = sample_attribute_from_key_value(&KeyValue {
            key: "complex".to_owned(),
            value: Some(AnyValue {
                value: Some(
                    otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value::KvlistValue(
                        otap_df_pdata::proto::opentelemetry::common::v1::KeyValueList {
                            values: vec![key_value("nested", "x")],
                        },
                    ),
                ),
            }),
        });

        assert_eq!(attribute.name, "complex");
        assert!(attribute.value.is_none());
        assert!(attribute.r#type.is_none());
    }
}
