// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Pluggable codecs between independent encoded batches and native OTAP.
//!
//! Codec extensions register immutable factories at link time. A factory creates
//! private codec state on the calling core; implementations need not be `Send`
//! or `Sync`. Payloads contain identity, signal, bytes, and optional item counts,
//! never codec state. Passing or cloning a payload does not consult the registry
//! or materialize telemetry records.

use std::borrow::Cow;
use std::fmt;

use bytes::Bytes;
use otel_arrow_dfe_config::{ConversionOptions, SignalType};

use crate::error::Error;
use crate::otap::OtapArrowRecords;
use crate::{OtapPayloadHelpers, OtlpProtoBytes, TryIntoWithOptions};

/// Stable identity of an independently decodable byte representation.
///
/// Names include any version or compression distinction needed to interpret the
/// bytes. Built-in names are reserved; other authors should use a vendor prefix.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PdataEncoding(Cow<'static, str>);

impl PdataEncoding {
    /// OTLP protobuf service-request bytes, without transport compression.
    pub const OTLP: Self = Self::new("otlp-bytes");

    /// Declares a compile-time encoding identity.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self(Cow::Borrowed(name))
    }

    /// Returns the stable name, also usable in configuration and diagnostics.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for PdataEncoding {
    fn from(name: String) -> Self {
        Self(Cow::Owned(name))
    }
}

impl fmt::Display for PdataEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Encoded batch envelope. Signal and identity remain available without decoding.
#[derive(Clone, Debug)]
pub struct EncodedPdata {
    encoding: PdataEncoding,
    signal: SignalType,
    bytes: Bytes,
    item_count: Option<usize>,
}

impl EncodedPdata {
    /// Wraps bytes without validating or decoding them, including unknown formats.
    #[must_use]
    pub fn new(encoding: PdataEncoding, signal: SignalType, bytes: Bytes) -> Self {
        Self {
            encoding,
            signal,
            bytes,
            item_count: None,
        }
    }

    /// Supplies a count already known by the receiver, without decoding for metrics.
    #[must_use]
    pub fn with_item_count(mut self, item_count: usize) -> Self {
        self.item_count = Some(item_count);
        self
    }

    /// Stable encoding identity.
    #[must_use]
    pub fn encoding(&self) -> &PdataEncoding {
        &self.encoding
    }

    /// Signal carried outside the encoded bytes.
    #[must_use]
    pub const fn signal_type(&self) -> SignalType {
        self.signal
    }

    /// Borrows the original encoded buffer.
    #[must_use]
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Takes ownership of the encoded buffer without copying it.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    /// Known item count, if supplied by the producer of this envelope.
    #[must_use]
    pub const fn item_count(&self) -> Option<usize> {
        self.item_count
    }
}

/// Immutable representation metadata advertised by a codec extension.
#[derive(Debug)]
pub struct PdataCodecMetadata {
    /// Globally stable name; duplicate registrations are rejected.
    pub encoding: PdataEncoding,
    /// Signals understood by this codec.
    pub signals: &'static [SignalType],
    /// Informational version; incompatible versions need different identities.
    pub format_version: Option<&'static str>,
    /// Compression intrinsic to the format, not HTTP/gRPC transport compression.
    pub compression: Option<&'static str>,
    /// Whether encoded bytes can be converted to OTAP.
    pub can_decode: bool,
    /// Whether OTAP can be converted to this encoding.
    pub can_encode: bool,
}

/// Per-consumer codec state. No cross-core sharing or locks are required.
///
/// Each call must consume/produce a complete independent batch: stream-relative
/// dictionary deltas are not an independent encoded representation. Implementors
/// must validate input, respect conversion options, and preserve the signal.
pub trait PdataCodec {
    /// Converts a complete encoded batch to native OTAP.
    fn decode(
        &mut self,
        signal: SignalType,
        bytes: Bytes,
        options: ConversionOptions,
    ) -> Result<OtapArrowRecords, crate::encode::Error>;

    /// Converts native OTAP to a complete independently decodable encoded batch.
    fn encode(
        &mut self,
        records: OtapArrowRecords,
        options: ConversionOptions,
    ) -> Result<Bytes, Error>;
}

/// Link-time codec extension registration. Only factories, not mutable state, are shared.
pub struct PdataCodecRegistration {
    /// Representation identity and capabilities.
    pub metadata: &'static PdataCodecMetadata,
    /// Creates independent state on the calling core.
    pub create: fn() -> Box<dyn PdataCodec>,
}

/// Trusted codec extensions compiled into this binary.
///
/// Register with `#[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]`.
/// This is separate from service extensions with background lifecycles: codecs
/// are synchronous data conversions and have no engine task or control channel.
#[allow(unsafe_code)]
#[linkme::distributed_slice]
pub static PDATA_CODEC_FACTORIES: [PdataCodecRegistration];

pub(crate) fn codec_error(encoding: &PdataEncoding, reason: impl Into<String>) -> Error {
    Error::PdataCodec {
        encoding: encoding.clone(),
        reason: reason.into(),
    }
}

/// Validates registration names and capabilities before a pipeline starts.
pub fn validate_registrations() -> Result<(), Error> {
    validate_factories(&PDATA_CODEC_FACTORIES)
}

fn validate_factories(factories: &[PdataCodecRegistration]) -> Result<(), Error> {
    for (index, factory) in factories.iter().enumerate() {
        let metadata = factory.metadata;
        let name = metadata.encoding.as_str();
        if name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-:".contains(&b))
        {
            return Err(codec_error(
                &metadata.encoding,
                "identity must use lowercase ASCII letters, digits, '.', '_', '-', or ':'",
            ));
        }
        if metadata.signals.is_empty() || (!metadata.can_decode && !metadata.can_encode) {
            return Err(codec_error(
                &metadata.encoding,
                "must advertise a signal and an encoder or decoder",
            ));
        }
        if factories[..index]
            .iter()
            .any(|other| other.metadata.encoding == metadata.encoding)
        {
            return Err(codec_error(
                &metadata.encoding,
                "duplicate encoding registration",
            ));
        }
    }
    Ok(())
}

/// The conversion a component requires from a codec.
#[derive(Clone, Copy, Debug)]
pub enum CodecDirection {
    /// Encoded bytes to native OTAP.
    Decode,
    /// Native OTAP to encoded bytes.
    Encode,
}

/// Finds a codec and validates a node's required signal and conversion direction.
///
/// Factories can call this at startup when their input/output representation is
/// known. Unknown runtime representations are checked when conversion is needed.
pub fn resolve(
    encoding: &PdataEncoding,
    signal: SignalType,
    direction: CodecDirection,
) -> Result<&'static PdataCodecRegistration, Error> {
    let mut matches = PDATA_CODEC_FACTORIES
        .iter()
        .filter(|f| &f.metadata.encoding == encoding);
    let factory = matches
        .next()
        .ok_or_else(|| codec_error(encoding, "no codec registered"))?;
    if matches.next().is_some() {
        return Err(codec_error(encoding, "duplicate encoding registration"));
    }
    let metadata = factory.metadata;
    if !metadata.signals.contains(&signal) {
        return Err(codec_error(
            encoding,
            format!("unsupported signal {signal:?}"),
        ));
    }
    match direction {
        CodecDirection::Encode if !metadata.can_encode => {
            return Err(codec_error(encoding, "encoder unavailable"));
        }
        CodecDirection::Decode if !metadata.can_decode => {
            return Err(codec_error(encoding, "decoder unavailable"));
        }
        _ => {}
    }
    Ok(factory)
}

pub(crate) fn decode(
    encoded: EncodedPdata,
    options: ConversionOptions,
) -> Result<OtapArrowRecords, crate::encode::Error> {
    let signal = encoded.signal;
    let factory = resolve(&encoded.encoding, signal, CodecDirection::Decode)?;
    let records = (factory.create)().decode(signal, encoded.bytes, options)?;
    if records.signal_type() != signal {
        return Err(codec_error(&encoded.encoding, "decoder changed the signal type").into());
    }
    Ok(records)
}

/// Built-in codec for the existing OTLP protobuf representation.
#[derive(Default)]
pub struct OtlpCodec;

impl PdataCodec for OtlpCodec {
    fn decode(
        &mut self,
        signal: SignalType,
        bytes: Bytes,
        options: ConversionOptions,
    ) -> Result<OtapArrowRecords, crate::encode::Error> {
        OtlpProtoBytes::new_from_bytes(signal, bytes).try_into_with_options(options)
    }

    fn encode(
        &mut self,
        records: OtapArrowRecords,
        options: ConversionOptions,
    ) -> Result<Bytes, Error> {
        let mut bytes: OtlpProtoBytes = records.try_into_with_options(options)?;
        Ok(bytes.replace_bytes(Bytes::new()))
    }
}

/// Metadata of the built-in OTLP codec.
pub static OTLP_METADATA: PdataCodecMetadata = PdataCodecMetadata {
    encoding: PdataEncoding::OTLP,
    signals: &[SignalType::Logs, SignalType::Metrics, SignalType::Traces],
    format_version: None,
    compression: None,
    can_decode: true,
    can_encode: true,
};

#[allow(unsafe_code)]
#[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]
static OTLP_CODEC: PdataCodecRegistration = PdataCodecRegistration {
    metadata: &OTLP_METADATA,
    create: || Box::new(OtlpCodec),
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::fixtures::logs_with_full_resource_and_scope;
    use crate::testing::round_trip::otlp_bytes_to_message;
    use crate::{OtapPayload, PayloadData};
    use prost::Message;
    use std::cell::Cell;
    use std::rc::Rc;

    thread_local! {
        static DECODES: Cell<usize> = const { Cell::new(0) };
    }

    const TEST_ENCODING: PdataEncoding = PdataEncoding::new("test-framed-otlp");
    static TEST_METADATA: PdataCodecMetadata = PdataCodecMetadata {
        encoding: TEST_ENCODING,
        signals: &[SignalType::Logs],
        format_version: Some("1"),
        compression: None,
        can_decode: true,
        can_encode: true,
    };

    // The Rc deliberately makes this codec !Send and !Sync. Only its factory
    // is shared; no implementation state is attached to a payload.
    #[derive(Default)]
    struct TestCodec {
        calls: Rc<Cell<usize>>,
    }

    impl PdataCodec for TestCodec {
        fn decode(
            &mut self,
            signal: SignalType,
            bytes: Bytes,
            options: ConversionOptions,
        ) -> Result<OtapArrowRecords, crate::encode::Error> {
            DECODES.with(|count| count.set(count.get() + 1));
            self.calls.set(self.calls.get() + 1);
            if bytes.first() == Some(&2) {
                return Ok(OtapArrowRecords::Metrics(Default::default()));
            }
            if bytes.first() != Some(&1) {
                return Err(codec_error(&TEST_ENCODING, "invalid test frame").into());
            }
            OtlpCodec.decode(signal, bytes.slice(1..), options)
        }

        fn encode(
            &mut self,
            records: OtapArrowRecords,
            options: ConversionOptions,
        ) -> Result<Bytes, Error> {
            let bytes = OtlpCodec.encode(records, options)?;
            let mut frame = Vec::with_capacity(bytes.len() + 1);
            frame.push(1);
            frame.extend_from_slice(&bytes);
            Ok(frame.into())
        }
    }

    #[allow(unsafe_code)]
    #[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]
    static TEST_CODEC: PdataCodecRegistration = PdataCodecRegistration {
        metadata: &TEST_METADATA,
        create: || Box::<TestCodec>::default(),
    };

    static DECODE_ONLY_METADATA: PdataCodecMetadata = PdataCodecMetadata {
        encoding: PdataEncoding::new("test-decode-only"),
        signals: &[SignalType::Logs],
        format_version: None,
        compression: None,
        can_decode: true,
        can_encode: false,
    };

    #[allow(unsafe_code)]
    #[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]
    static DECODE_ONLY: PdataCodecRegistration = PdataCodecRegistration {
        metadata: &DECODE_ONLY_METADATA,
        create: || Box::<TestCodec>::default(),
    };

    fn logs_payload() -> OtapPayload {
        OtlpProtoBytes::new_from_bytes(
            SignalType::Logs,
            logs_with_full_resource_and_scope().encode_to_vec(),
        )
        .into()
    }

    /// Scenario: OTLP payloads pass through the generalized encoded API.
    /// Guarantees: payload layout stays compact and signal/bytes are unchanged without a decode.
    #[test]
    fn otlp_passthrough_keeps_original_buffer() {
        // Compare against the pre-extension layout rather than a platform-specific size.
        #[allow(dead_code)]
        enum BuiltinPayload {
            Otlp(OtlpProtoBytes),
            Otap(OtapArrowRecords),
        }
        assert_eq!(
            size_of::<PayloadData>(),
            size_of::<BuiltinPayload>(),
            "extension envelopes must not enlarge queued built-in payloads",
        );
        for signal in [SignalType::Logs, SignalType::Metrics, SignalType::Traces] {
            let bytes = Bytes::from(vec![0xff, 0x80]); // Deliberately not decodable.
            let shared = bytes.clone();
            let pointer = shared.as_ptr();
            let payload =
                OtapPayload::from_encoded(EncodedPdata::new(PdataEncoding::OTLP, signal, bytes));
            assert_eq!(payload.encoding(), Some(&PdataEncoding::OTLP));
            assert!(matches!(payload.data(), PayloadData::OtlpBytes(_)));
            let output = payload
                .into_encoded(PdataEncoding::OTLP, Default::default())
                .unwrap();
            assert_eq!(output.signal_type(), signal);
            assert_eq!(output.bytes().as_ptr(), pointer);
            assert_eq!(output.bytes().as_ref(), &[0xff, 0x80]);
        }
    }

    /// Scenario: an unregistered encoding is measured, cloned and exported unchanged.
    /// Guarantees: passthrough needs no codec and shares bytes; empty remainders reset counts.
    #[test]
    fn unknown_encoding_passthrough_and_measurements() {
        let encoding = PdataEncoding::from("example-unknown-v1".to_owned());
        let bytes = Bytes::from(vec![1, 2, 3]);
        let pointer = bytes.as_ptr();
        let mut payload = OtapPayload::from_encoded(
            EncodedPdata::new(encoding.clone(), SignalType::Logs, bytes).with_item_count(7),
        );
        assert_eq!(payload.num_items(), 7);
        assert_eq!(payload.num_bytes(), Some(3));
        assert_eq!(payload.retained_memory_bytes(), 3);
        assert!(!payload.is_empty());
        let mut clone = payload.clone();
        match (payload.data(), clone.data()) {
            (PayloadData::Encoded(original), PayloadData::Encoded(cloned)) => {
                assert!(std::sync::Arc::ptr_eq(original, cloned));
            }
            _ => panic!("expected shared encoded envelopes"),
        }
        let output = clone
            .take_payload()
            .into_encoded(encoding.clone(), Default::default())
            .unwrap();
        assert_eq!(output.bytes().as_ptr(), pointer);
        assert_eq!(output.item_count(), Some(7));
        assert_eq!(clone.encoding(), Some(&encoding));
        assert_eq!(clone.num_items(), 0);
        assert_eq!(clone.num_bytes(), Some(0));
        assert!(clone.is_empty());
        let output = payload.into_encoded(encoding, Default::default()).unwrap();
        assert_eq!(output.bytes().as_ptr(), pointer);
    }

    /// Scenario: a codec with core-local state converts OTLP through OTAP and back.
    /// Guarantees: registered conversions preserve log contents and materialize once per branch.
    #[test]
    fn registered_codec_roundtrip_and_lazy_fanout() {
        DECODES.with(|count| count.set(0));
        let original = logs_payload();
        let encoded = original
            .clone()
            .into_encoded(TEST_ENCODING, Default::default())
            .unwrap();
        let mut decoded = OtapPayload::from_encoded(encoded);
        let passthrough = decoded.clone();
        assert_eq!(decoded.num_items(), original.clone().num_items());
        assert_eq!(DECODES.with(Cell::get), 0);
        decoded.materialize_otap(Default::default()).unwrap();
        decoded.materialize_otap(Default::default()).unwrap();
        assert_eq!(DECODES.with(Cell::get), 1);
        assert_eq!(decoded.encoding(), None);
        assert_eq!(passthrough.encoding(), Some(&TEST_ENCODING));
        let actual: OtlpProtoBytes = decoded.try_into_with_default().unwrap();
        let expected: OtlpProtoBytes = original.try_into_with_default().unwrap();
        crate::testing::equiv::assert_equivalent(
            &[otlp_bytes_to_message(actual)],
            &[otlp_bytes_to_message(expected)],
        );
        let output: OtlpProtoBytes = passthrough.try_into_with_default().unwrap();
        assert_eq!(output.signal_type(), SignalType::Logs);
        assert_eq!(DECODES.with(Cell::get), 2);
    }

    /// Scenario: bytes cannot be decoded, or the requested output codec is absent.
    /// Guarantees: failed conversion keeps identity, signal, original bytes and cached counts.
    #[test]
    fn failed_conversion_preserves_original_payload() {
        for encoding in [TEST_ENCODING, PdataEncoding::new("missing-codec")] {
            let bytes = Bytes::from(vec![0]);
            let pointer = bytes.as_ptr();
            let mut payload = OtapPayload::from_encoded(
                EncodedPdata::new(encoding.clone(), SignalType::Logs, bytes).with_item_count(5),
            );
            assert!(payload.materialize_otap(Default::default()).is_err());
            let error = payload
                .convert_encoding(PdataEncoding::new("missing-output"), Default::default())
                .unwrap_err();
            assert!(error.to_string().contains("missing-output"));
            assert_eq!(payload.encoding(), Some(&encoding));
            assert_eq!(payload.num_items(), 5);
            assert_eq!(payload.signal_type(), SignalType::Logs);
            let output = payload.into_encoded(encoding, Default::default()).unwrap();
            assert_eq!(output.bytes().as_ptr(), pointer);
        }
    }

    /// Scenario: factories declare duplicate identities or a consumer requests absent capabilities.
    /// Guarantees: ambiguity, missing codecs, unsupported signals and encode-only requests fail clearly.
    #[test]
    fn registration_and_capabilities_are_validated() {
        validate_registrations().unwrap();
        let factories = [
            PdataCodecRegistration {
                metadata: &TEST_METADATA,
                create: || Box::<TestCodec>::default(),
            },
            PdataCodecRegistration {
                metadata: &TEST_METADATA,
                create: || Box::<TestCodec>::default(),
            },
        ];
        assert!(
            validate_factories(&factories)
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
        assert!(
            resolve(
                &PdataEncoding::new("missing"),
                SignalType::Logs,
                CodecDirection::Decode
            )
            .err()
            .unwrap()
            .to_string()
            .contains("no codec registered")
        );
        assert!(
            resolve(&TEST_ENCODING, SignalType::Metrics, CodecDirection::Decode)
                .err()
                .unwrap()
                .to_string()
                .contains("unsupported signal")
        );
        assert!(
            resolve(
                &DECODE_ONLY_METADATA.encoding,
                SignalType::Logs,
                CodecDirection::Encode
            )
            .err()
            .unwrap()
            .to_string()
            .contains("encoder unavailable")
        );
    }

    /// Scenario: an extension encoder encounters the configured OTLP output limit.
    /// Guarantees: conversion options reach the codec and a failed encode retains native input.
    #[test]
    fn conversion_options_reach_extension_encoder() {
        let mut payload = logs_payload();
        payload.materialize_otap(Default::default()).unwrap();
        let original = payload.clone().into_otap().unwrap();
        let options = ConversionOptions {
            otlp_size_limit: std::num::NonZeroUsize::new(1),
        };
        assert!(payload.convert_encoding(TEST_ENCODING, options).is_err());
        assert_eq!(payload.encoding(), None);
        assert_eq!(payload.into_otap().unwrap(), original);
    }

    /// Scenario: a defective codec returns a different signal from the envelope.
    /// Guarantees: the framework rejects the conversion and preserves the original signal.
    #[test]
    fn decoder_cannot_change_signal() {
        let mut payload = OtapPayload::from_encoded(EncodedPdata::new(
            TEST_ENCODING,
            SignalType::Logs,
            Bytes::from_static(&[2]),
        ));
        assert!(
            payload
                .materialize_otap(Default::default())
                .unwrap_err()
                .to_string()
                .contains("decoder changed the signal type")
        );
        assert_eq!(payload.signal_type(), SignalType::Logs);
        assert_eq!(payload.encoding(), Some(&TEST_ENCODING));
    }
}
