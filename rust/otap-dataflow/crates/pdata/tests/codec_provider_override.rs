// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Verifies final-binary provider selection through the linked codec catalog.

use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use otel_arrow_dfe_config::{EncodeOptions, SignalType};
use otel_arrow_dfe_pdata::OtapPayload;
use otel_arrow_dfe_pdata::codec::{
    CodecProviderId, CodecRegistryOptions, CodecState, EncodingPlan, PDATA_CODEC_FACTORIES,
    PdataCodec, PdataCodecMetadata, PdataCodecRegistration, PdataEncoding,
    configure_codec_registry, find,
};
use otel_arrow_dfe_pdata::error::Error;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;

const TEST_ENCODING: PdataEncoding = PdataEncoding::new("example-framed-v1");
const REFERENCE_PROVIDER: CodecProviderId =
    CodecProviderId::new("org.opentelemetry.test.reference");
const OPTIMIZED_PROVIDER: CodecProviderId = CodecProviderId::new("com.example.telemetry.optimized");

static CREATES: AtomicUsize = AtomicUsize::new(0);
static METADATA: PdataCodecMetadata = PdataCodecMetadata {
    encoding: TEST_ENCODING,
    signals: &[SignalType::Logs],
    format_version: Some("1"),
    compression: None,
    can_decode: true,
    can_encode: true,
    batching: None,
};

struct TestCodec;

impl PdataCodec for TestCodec {
    fn decode(
        &mut self,
        _signal: SignalType,
        _bytes: &Bytes,
    ) -> Result<OtapArrowRecords, otel_arrow_dfe_pdata::encode::Error> {
        unreachable!("matching-format forwarding must not decode")
    }

    fn encode(
        &mut self,
        _records: OtapArrowRecords,
        _options: EncodeOptions,
    ) -> Result<Bytes, Error> {
        unreachable!("matching-format forwarding must not encode")
    }
}

fn create_codec() -> Box<dyn PdataCodec> {
    _ = CREATES.fetch_add(1, Ordering::Relaxed);
    Box::new(TestCodec)
}

#[allow(unsafe_code)]
#[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]
static REFERENCE_CODEC: PdataCodecRegistration = PdataCodecRegistration {
    provider: REFERENCE_PROVIDER,
    metadata: &METADATA,
    create: create_codec,
    count_items: None,
};

#[allow(unsafe_code)]
#[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]
static OPTIMIZED_CODEC: PdataCodecRegistration = PdataCodecRegistration {
    provider: OPTIMIZED_PROVIDER,
    metadata: &METADATA,
    create: create_codec,
    count_items: None,
};

/// Scenario: a final binary links a reference codec and a proprietary replacement under one name.
/// Guarantees: explicit startup selection chooses the proprietary provider and forwarding stays zero-copy and instance-free.
#[test]
fn final_binary_selects_explicit_provider() {
    configure_codec_registry(
        CodecRegistryOptions::default().select(TEST_ENCODING, OPTIMIZED_PROVIDER),
    )
    .expect("final binary registry configuration");

    let codec = find(&TEST_ENCODING).expect("selected test codec");
    assert_eq!(codec.provider(), OPTIMIZED_PROVIDER);

    let bytes = Bytes::from_static(b"complete independent batch");
    let pointer = bytes.as_ptr();
    let payload = OtapPayload::from(
        codec
            .admit(SignalType::Logs, bytes)
            .expect("admitted test batch"),
    );
    let plan = EncodingPlan::new(codec, EncodeOptions::default()).expect("test encoding plan");
    let forwarded = payload
        .into_encoded(&mut CodecState::default(), &plan)
        .expect("matching-format forwarding");

    assert_eq!(forwarded.bytes().as_ptr(), pointer);
    assert_eq!(CREATES.load(Ordering::Relaxed), 0);
}
