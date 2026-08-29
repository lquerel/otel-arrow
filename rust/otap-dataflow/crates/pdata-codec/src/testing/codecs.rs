// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Registered OTLP-compatible test codecs for generic pdata paths.

use bytes::Bytes;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::OtapArrowRecords;
use otel_arrow_dfe_pdata::views::otlp::bytes::logs::RawLogsData;
use otel_arrow_dfe_pdata::views::otlp::bytes::metrics::RawMetricsData;
use otel_arrow_dfe_pdata::views::otlp::bytes::traces::RawTraceData;

use crate::codecs::otlp::{OtlpBatcher, OtlpDecoder, OtlpEncoder};
use crate::{
    BatchProfile, BatchSizer, BatchingSupport, CodecBatcherRegistration, CodecError, CodecMetadata,
    CodecOperation, CodecRegistration, PdataDecoder, PdataEncoding,
};

struct TestDecoder(OtlpDecoder);

impl PdataDecoder for TestDecoder {
    fn decode(
        &mut self,
        signal: SignalType,
        bytes: &Bytes,
    ) -> Result<OtapArrowRecords, CodecError> {
        let validation = match signal {
            SignalType::Logs => RawLogsData::try_new(bytes).map(|_| ()),
            SignalType::Metrics => RawMetricsData::try_new(bytes).map(|_| ()),
            SignalType::Traces => RawTraceData::try_new(bytes).map(|_| ()),
        };
        validation.map_err(|source| {
            CodecError::operation(&TEST_ENCODING, CodecOperation::Decode, source)
        })?;
        self.0.decode(signal, bytes)
    }
}

/// Registered encoding with OTAP fallback batching.
pub const TEST_ENCODING: PdataEncoding = PdataEncoding::new("test-otlp-codec");
/// Registered input encoding without an output encoder.
pub const DECODE_ONLY_ENCODING: PdataEncoding = PdataEncoding::new("test-otlp-decode-only");
/// Codec with native byte batching, independent of the built-in identity.
pub const NATIVE_ENCODING: PdataEncoding = PdataEncoding::new("test-native-otlp");
/// Output-only codec that must never be admitted into the pipeline.
pub const ENCODE_ONLY_ENCODING: PdataEncoding = PdataEncoding::new("test-otlp-encode-only");

static SIGNALS: &[SignalType] = &[SignalType::Logs, SignalType::Metrics, SignalType::Traces];

static FALLBACK_METADATA: CodecMetadata = CodecMetadata::new(TEST_ENCODING, SIGNALS);
static DECODE_METADATA: CodecMetadata = CodecMetadata::new(DECODE_ONLY_ENCODING, SIGNALS);
static NATIVE_METADATA: CodecMetadata = CodecMetadata::new(NATIVE_ENCODING, SIGNALS);
static ENCODE_METADATA: CodecMetadata =
    CodecMetadata::new(ENCODE_ONLY_ENCODING, &[SignalType::Logs]);
static NATIVE_BATCHING: BatchingSupport = BatchingSupport {
    sizers: &[BatchSizer::Bytes],
    default_profile: BatchProfile::otlp(),
};

crate::register_pdata_codec!(
    FALLBACK,
    CodecRegistration::new(&FALLBACK_METADATA)
        .with_decoder(|| Box::new(TestDecoder(OtlpDecoder)))
        .with_encoder(|policy| Ok(Box::new(OtlpEncoder::new(policy)))),
);

crate::register_pdata_codec!(
    DECODE,
    CodecRegistration::new(&DECODE_METADATA).with_decoder(|| Box::new(TestDecoder(OtlpDecoder))),
);

crate::register_pdata_codec!(
    NATIVE,
    CodecRegistration::new(&NATIVE_METADATA)
        .with_decoder(|| Box::new(TestDecoder(OtlpDecoder)))
        .with_encoder(|policy| Ok(Box::new(OtlpEncoder::new(policy))))
        .with_batcher(CodecBatcherRegistration::new(&NATIVE_BATCHING, || {
            Box::new(OtlpBatcher)
        })),
);

crate::register_pdata_codec!(
    ENCODE,
    CodecRegistration::new(&ENCODE_METADATA)
        .with_encoder(|policy| Ok(Box::new(OtlpEncoder::new(policy)))),
);
