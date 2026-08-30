// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = receiver::KAFKA_RECEIVER_URN,
    target = "otel.receiver.kafka",
);

/// Implementation of the config settings for the kafka receiver
pub mod config;
/// Error types for the Kafka Receiver.
pub mod error;
/// Kafka header extraction and injection into telemetry payloads.
pub mod headers;
/// Implementation of the metrics to collect from the kafka receiver
pub mod metrics;
/// Per-offset tracking for Kafka consumer offset management.
pub mod offset_tracker;
/// Consumer-group rebalance handling (partition assign/revoke callbacks).
pub mod rebalance;
/// Implementation of the main kafka receiver
pub mod receiver;

/// Checks received OTLP storage without converting or reconstructing its envelope.
#[cfg(test)]
fn encoded_otlp(
    pdata: &otel_arrow_dfe_otap::pdata::OtapPdata,
    signal: otel_arrow_dfe_config::SignalType,
) -> &[u8] {
    use otel_arrow_dfe_pdata::batching::PdataFormat;

    assert_eq!(
        pdata.payload_ref().format(),
        PdataFormat::otlp().expect("selected OTLP format")
    );
    assert_eq!(pdata.signal_type(), signal);
    pdata
        .payload_ref()
        .encoded_bytes()
        .expect("OTLP receiver output must remain encoded")
        .as_ref()
}
