// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! crate containing GRPC Server implementations for the OTLP services that
//! convert the received OTLP signals into OTAP

pub mod client;

pub mod server;
pub mod server_new;

const LOGS_SERVICE_NAME: &str = "opentelemetry.proto.collector.logs.v1.LogsService";
const LOGS_SERVICE_EXPORT_PATH: &str = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";
const METRICS_SERVICE_NAME: &str = "opentelemetry.proto.collector.metrics.v1.MetricsService";
const METRICS_SERVICE_EXPORT_PATH: &str =
    "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export";
const TRACE_SERVICE_NAME: &str = "opentelemetry.proto.collector.trace.v1.TraceService";
const TRACE_SERVICE_EXPORT_PATH: &str =
    "/opentelemetry.proto.collector.trace.v1.TraceService/Export";

#[cfg(test)]
async fn assert_encoded_decoder<D>(decoder: D, signal: otel_arrow_dfe_config::SignalType)
where
    D: tonic::codec::Decoder<Item = crate::pdata::OtapPdata, Error = tonic::Status>
        + Send
        + 'static,
{
    use bytes::Bytes;
    use otel_arrow_dfe_pdata::PayloadData;
    use otel_arrow_dfe_pdata::codec::ResolvedCodec;

    // Valid gRPC framing around deliberately malformed protobuf proves that
    // admission leaves protobuf interpretation to the eventual consumer.
    let body = http_body_util::Full::new(Bytes::from_static(&[0, 0, 0, 0, 2, 0xff, 0x80]));
    let mut stream = tonic::Streaming::new_request(decoder, body, None, None);
    let pdata = stream.message().await.unwrap().unwrap();
    let PayloadData::Encoded(encoded) = pdata.payload_ref().data() else {
        panic!("OTLP decoder must admit encoded pdata");
    };
    assert_eq!(encoded.codec(), ResolvedCodec::OTLP);
    assert_eq!(encoded.signal_type(), signal);
    assert_eq!(encoded.bytes().as_ref(), &[0xff, 0x80]);
    assert_eq!(encoded.item_count(), None);
    assert!(stream.message().await.unwrap().is_none());
}
