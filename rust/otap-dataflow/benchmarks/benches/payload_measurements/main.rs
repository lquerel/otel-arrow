// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#![allow(missing_docs)]

//! This crate benchmarks cached PData measurements for OTLP and OTAP payloads.

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_otap::pdata::{Context, OtapPdata};
use otel_arrow_dfe_pdata::OtapPayload;
use otel_arrow_dfe_pdata::batching::{BatchPlan, PdataFormat};
use otel_arrow_dfe_pdata::codec::{CodecState, EncodingPlan};
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::otlp::OtlpProtoBytes;
use otel_arrow_dfe_pdata::proto::OtlpProtoMessage;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::*;
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::*;
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::*;
use otel_arrow_dfe_pdata::testing::round_trip::{otlp_message_to_bytes, otlp_to_otap};

#[cfg(not(windows))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(windows))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn create_logs_data(record_count: usize) -> LogsData {
    let kvs = vec![
        KeyValue::new("k1", AnyValue::new_string("v1")),
        KeyValue::new("k2", AnyValue::new_string("v2")),
    ];
    let resource = Resource::build().attributes(kvs.clone()).finish();
    let scope = InstrumentationScope::build().name("library").finish();
    let record = LogRecord::build()
        .time_unix_nano(2_000_000_000u64)
        .severity_number(SeverityNumber::Info)
        .event_name("event1")
        .attributes(kvs)
        .finish();
    let scope_logs = ScopeLogs::new(scope, vec![record; record_count])
        .set_schema_url("http://schema.opentelemetry.io");

    LogsData::new(vec![ResourceLogs::new(resource, vec![scope_logs])])
}

fn count_logs(c: &mut Criterion) {
    let mut group = c.benchmark_group("OTLP Logs counting");
    let logs = create_logs_data(1_000);

    _ = group.bench_function("Manual", |b| {
        b.iter(|| {
            let mut count = 0;
            for rl in &logs.resource_logs {
                for sl in &rl.scope_logs {
                    count += sl.log_records.len();
                }
            }
            black_box(count)
        })
    });

    _ = group.bench_function("FlatMap", |b| {
        b.iter(|| {
            logs.resource_logs
                .iter()
                .flat_map(|rl| &rl.scope_logs)
                .flat_map(|sl| &sl.log_records)
                .count()
        })
    });

    group.finish();
}

fn count_payload_items(c: &mut Criterion) {
    let mut group = c.benchmark_group("PData item-count overhead");

    for record_count in [10, 100, 1_000] {
        let message = OtlpProtoMessage::Logs(create_logs_data(record_count));
        let otlp_bytes: OtlpProtoBytes = otlp_message_to_bytes(&message);
        let otap_records: OtapArrowRecords = otlp_to_otap(&message);

        for format in ["OTLP", "OTAP"] {
            let fresh_payload = |format: &str| -> OtapPayload {
                match format {
                    "OTLP" => otlp_bytes.clone().into(),
                    _ => otap_records.clone().into(),
                }
            };

            let disabled = OtapPdata::new(Context::default(), fresh_payload(format));
            _ = group.bench_with_input(
                BenchmarkId::new(format!("{format}/disabled"), record_count),
                &disabled,
                |b, pdata| b.iter(|| black_box(pdata.signal_type())),
            );

            _ = group.bench_function(
                BenchmarkId::new(format!("{format}/clone/uncached"), record_count),
                |b| {
                    let pdata =
                        OtapPdata::new(Context::default(), black_box(fresh_payload(format)));
                    b.iter(|| black_box(pdata.clone()))
                },
            );

            let mut cached = OtapPdata::new(Context::default(), fresh_payload(format));
            _ = black_box(cached.num_items());
            _ = group.bench_with_input(
                BenchmarkId::new(format!("{format}/clone/cached"), record_count),
                &cached,
                |b, pdata| b.iter(|| black_box(pdata.clone())),
            );

            if format == "OTLP" {
                _ = group.bench_function(
                    BenchmarkId::new(format!("{format}/count/uncached"), record_count),
                    |b| {
                        b.iter_batched_ref(
                            || OtapPdata::new(Context::default(), black_box(fresh_payload(format))),
                            |pdata| black_box(pdata.num_items()),
                            BatchSize::SmallInput,
                        )
                    },
                );

                _ = group.bench_function(
                    BenchmarkId::new(format!("{format}/count/cached"), record_count),
                    |b| {
                        let mut pdata = cached.clone();
                        b.iter(|| black_box(pdata.num_items()))
                    },
                );
            } else {
                _ = group.bench_function(
                    BenchmarkId::new(format!("{format}/count/direct"), record_count),
                    |b| {
                        let mut pdata = cached.clone();
                        b.iter(|| black_box(pdata.num_items()))
                    },
                );
            }
        }
    }

    group.finish();
}

fn measure_payload_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("PData size overhead");

    for record_count in [10, 100, 1_000] {
        let message = OtlpProtoMessage::Logs(create_logs_data(record_count));
        let otlp_bytes: OtlpProtoBytes = otlp_message_to_bytes(&message);
        let otap_records: OtapArrowRecords = otlp_to_otap(&message);

        _ = group.bench_function(BenchmarkId::new("OTLP/size/direct", record_count), |b| {
            let mut pdata =
                OtapPdata::new(Context::default(), black_box(otlp_bytes.clone().into()));
            b.iter(|| black_box(pdata.num_bytes()))
        });

        _ = group.bench_function(BenchmarkId::new("OTAP/size/uncached", record_count), |b| {
            b.iter_batched_ref(
                || OtapPdata::new(Context::default(), black_box(otap_records.clone().into())),
                |pdata| black_box(pdata.num_bytes()),
                BatchSize::SmallInput,
            )
        });

        let mut cached = OtapPdata::new(Context::default(), black_box(otap_records.clone().into()));
        _ = black_box(cached.num_bytes());
        _ = group.bench_function(BenchmarkId::new("OTAP/size/cached", record_count), |b| {
            b.iter(|| black_box(cached.num_bytes()))
        });
    }

    group.finish();
}

fn convert_native_payload(c: &mut Criterion) {
    let mut group = c.benchmark_group("PData native conversion");

    for record_count in [10, 100, 1_000] {
        let message = OtlpProtoMessage::Logs(create_logs_data(record_count));
        let otap_records: OtapArrowRecords = otlp_to_otap(&message);

        let mut direct_codecs = CodecState::default();
        _ = group.bench_function(BenchmarkId::new("payload/direct", record_count), |b| {
            b.iter_batched(
                || OtapPayload::from(otap_records.clone()),
                |payload| {
                    black_box(
                        payload
                            .try_into_otap(&mut direct_codecs)
                            .expect("native payload conversion"),
                    )
                },
                BatchSize::SmallInput,
            )
        });

        let mut split_codecs = CodecState::default();
        _ = group.bench_function(BenchmarkId::new("pdata/split_direct", record_count), |b| {
            b.iter_batched(
                || OtapPdata::new(Context::default(), OtapPayload::from(otap_records.clone())),
                |pdata| {
                    let (context, payload) = pdata.into_parts();
                    let records = payload
                        .try_into_otap(&mut split_codecs)
                        .expect("native payload conversion");
                    black_box((context, records))
                },
                BatchSize::SmallInput,
            )
        });

        let mut capability_codecs = CodecState::default();
        _ = group.bench_function(BenchmarkId::new("pdata/capability", record_count), |b| {
            b.iter_batched(
                || OtapPdata::new(Context::default(), OtapPayload::from(otap_records.clone())),
                |pdata| {
                    black_box(
                        pdata
                            .try_into_otap(&mut capability_codecs)
                            .expect("native pdata conversion"),
                    )
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn exercise_codec_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("PData codec paths");

    for record_count in [10, 100, 1_000] {
        let message = OtlpProtoMessage::Logs(create_logs_data(record_count));
        let otlp_bytes: OtlpProtoBytes = otlp_message_to_bytes(&message);
        let otap_records: OtapArrowRecords = otlp_to_otap(&message);

        let mut forward_codecs = CodecState::default();
        _ = group.bench_function(BenchmarkId::new("OTLP/forward", record_count), |b| {
            b.iter_batched(
                || OtapPayload::from(otlp_bytes.clone()),
                |payload| {
                    black_box(
                        payload
                            .into_encoded(&mut forward_codecs, &EncodingPlan::OTLP)
                            .expect("matching-codec forwarding"),
                    )
                },
                BatchSize::SmallInput,
            )
        });

        let mut decode_codecs = CodecState::default();
        _ = group.bench_function(BenchmarkId::new("OTLP/decode", record_count), |b| {
            b.iter_batched(
                || OtapPayload::from(otlp_bytes.clone()),
                |payload| {
                    black_box(
                        payload
                            .try_into_otap(&mut decode_codecs)
                            .expect("OTLP decode"),
                    )
                },
                BatchSize::SmallInput,
            )
        });

        let mut encode_codecs = CodecState::default();
        _ = group.bench_function(BenchmarkId::new("OTAP/encode_otlp", record_count), |b| {
            b.iter_batched(
                || OtapPayload::from(otap_records.clone()),
                |payload| {
                    black_box(
                        payload
                            .into_encoded(&mut encode_codecs, &EncodingPlan::OTLP)
                            .expect("OTLP encode"),
                    )
                },
                BatchSize::SmallInput,
            )
        });

        let native_plan =
            BatchPlan::new(PdataFormat::OTAP, PdataFormat::OTAP.default_profile(), true)
                .expect("native batching plan");
        let mut native_batch_codecs = CodecState::default();
        _ = group.bench_function(BenchmarkId::new("OTAP/batch", record_count), |b| {
            b.iter_batched(
                || {
                    vec![
                        OtapPayload::from(otap_records.clone()),
                        OtapPayload::from(otap_records.clone()),
                    ]
                },
                |payloads| {
                    black_box(
                        native_plan
                            .batch(SignalType::Logs, payloads, &mut native_batch_codecs)
                            .expect("native OTAP batch"),
                    )
                },
                BatchSize::SmallInput,
            )
        });

        let encoded_plan =
            BatchPlan::new(PdataFormat::OTLP, PdataFormat::OTLP.default_profile(), true)
                .expect("encoded batching plan");
        let mut encoded_batch_codecs = CodecState::default();
        _ = group.bench_function(BenchmarkId::new("OTLP/batch", record_count), |b| {
            b.iter_batched(
                || {
                    vec![
                        OtapPayload::from(otlp_bytes.clone()),
                        OtapPayload::from(otlp_bytes.clone()),
                    ]
                },
                |payloads| {
                    black_box(
                        encoded_plan
                            .batch(SignalType::Logs, payloads, &mut encoded_batch_codecs)
                            .expect("encoded OTLP batch"),
                    )
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group!(
    payload_measurements,
    count_logs,
    count_payload_items,
    measure_payload_size,
    convert_native_payload,
    exercise_codec_paths
);
criterion_main!(payload_measurements);
