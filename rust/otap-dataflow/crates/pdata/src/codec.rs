// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Pluggable codecs between independent encoded batches and native OTAP.
//!
//! Codec extensions register immutable factories at link time. A factory creates
//! private codec state owned by a pipeline runtime. Payloads contain identity,
//! signal, bytes, and optional item counts, never codec state. Passing or cloning
//! a payload does not consult the registry or materialize telemetry records.

use std::borrow::Cow;
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use otel_arrow_dfe_config::{ConversionOptions, SignalType};

use crate::batching::{BatchProfile, BatchSizer, BatchingSupport, CodecBatches};
use crate::error::Error;
use crate::otap::OtapArrowRecords;
use crate::otlp::logs::LogsProtoBytesEncoder;
use crate::otlp::metrics::MetricsProtoBytesEncoder;
use crate::otlp::traces::TracesProtoBytesEncoder;
use crate::otlp::{BoundedBuf, ProtoBuffer, ProtoBytesEncoder};
use crate::{
    OtapPayload, OtapPayloadDecodeError, OtapPayloadHelpers, OtlpProtoBytes, PayloadView,
    TryIntoWithOptions,
};

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
    codec: ResolvedCodec,
    signal: SignalType,
    bytes: Bytes,
    item_count: Option<usize>,
}

impl EncodedPdata {
    /// Resolves and admits a decodable representation without decoding its bytes.
    pub fn new(encoding: PdataEncoding, signal: SignalType, bytes: Bytes) -> Result<Self, Error> {
        resolve(&encoding, signal, CodecDirection::Decode)?.admit(signal, bytes)
    }

    pub(crate) const fn from_resolved(
        codec: ResolvedCodec,
        signal: SignalType,
        bytes: Bytes,
    ) -> Self {
        Self {
            codec,
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
        &self.codec.metadata().encoding
    }

    /// Validated immutable codec identity; no mutable codec state travels with data.
    #[must_use]
    pub const fn codec(&self) -> ResolvedCodec {
        self.codec
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

    pub(crate) fn set_item_count(&mut self, count: Option<usize>) {
        self.item_count = count;
    }

    pub(crate) fn num_items(&mut self) -> usize {
        if self.item_count.is_none() {
            self.item_count = self
                .codec
                .0
                .count_items
                .and_then(|count| count(self.signal, &self.bytes));
        }
        self.item_count.unwrap_or(0)
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
    /// Optional in-line native batching, including supported sizing modes and defaults.
    pub batching: Option<BatchingSupport>,
}

/// Reusable synchronous codec implementation owned by a pipeline runtime.
///
/// Each call must process/produce a complete independent batch: stream-relative
/// dictionary deltas are not an independent encoded representation. Implementors
/// must validate input, respect conversion options, and preserve the signal.
pub trait PdataCodec: Send {
    /// Converts a borrowed complete encoded batch to native OTAP. Borrowing lets
    /// the caller retain the exact input for recovery without cloning its buffer.
    fn decode(
        &mut self,
        signal: SignalType,
        bytes: &Bytes,
        options: ConversionOptions,
    ) -> Result<OtapArrowRecords, crate::encode::Error>;

    /// Converts native OTAP to a complete independently decodable encoded batch.
    fn encode(
        &mut self,
        records: OtapArrowRecords,
        options: ConversionOptions,
    ) -> Result<Bytes, Error>;

    /// Prepares output that may borrow reusable encoder storage. The default
    /// supports codecs returning owned bytes; codecs with scratch can override it.
    fn prepare_encode<'a>(
        &'a mut self,
        records: &mut OtapArrowRecords,
        options: ConversionOptions,
    ) -> Result<EncodedOutput<'a>, Error> {
        self.encode(records.clone(), options)
            .map(EncodedOutput::bytes)
    }

    /// Borrows a supported native view or decodes to owned OTAP records. Returned
    /// views borrow the input, never codec state, so they may outlive this call.
    fn view<'a>(
        &mut self,
        signal: SignalType,
        bytes: &'a Bytes,
        options: ConversionOptions,
    ) -> Result<PayloadView<'a>, crate::encode::Error> {
        self.decode(signal, bytes, options)
            .map(|records| PayloadView::OtapArrowRecords(Cow::Owned(records)))
    }

    /// Measures a native encoded batch for batching. Unknown counts are never zero.
    /// Codecs may override item counting with a cheaper scan than decoding.
    fn measure(
        &mut self,
        signal: SignalType,
        bytes: Bytes,
        sizer: BatchSizer,
    ) -> Result<usize, Error> {
        match sizer {
            BatchSizer::Bytes => Ok(bytes.len()),
            BatchSizer::Items => {
                let records = self
                    .decode(signal, &bytes, ConversionOptions::default())
                    .map_err(|error| Error::Format {
                        error: error.to_string(),
                    })?;
                if records.signal_type() != signal {
                    return Err(Error::Format {
                        error: "decoder changed the signal type".into(),
                    });
                }
                Ok(records.num_items())
            }
            BatchSizer::Requests => Err(Error::Format {
                error: "request sizing is unsupported".into(),
            }),
        }
    }

    /// Re-batches in input order. Ownership weights must partition the input
    /// units exactly, including when splitting duplicates encoding wrappers.
    /// Implementations must enforce the profile's split amplification budgets.
    fn batch(
        &mut self,
        _signal: SignalType,
        _profile: &BatchProfile,
        _inputs: Vec<Bytes>,
    ) -> Result<CodecBatches, Error> {
        Err(Error::Format {
            error: "native batching is unavailable".into(),
        })
    }
}

/// Stateless optional item scan, used without allocating a codec instance.
pub type ItemCounter = fn(SignalType, &[u8]) -> Option<usize>;

/// Link-time codec extension registration. Only factories, not mutable state, are shared.
#[derive(Debug)]
pub struct PdataCodecRegistration {
    /// Representation identity and capabilities.
    pub metadata: &'static PdataCodecMetadata,
    /// Creates independent state in the runtime codec service.
    pub create: fn() -> Box<dyn PdataCodec>,
    /// Optional stateless item scan for flow metrics. Returning None means the
    /// count is unknown. Admission and forwarding never invoke this hook.
    pub count_items: Option<ItemCounter>,
}

/// Trusted codec extensions compiled into this binary.
///
/// Register with `#[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]`.
/// This is separate from service extensions with background lifecycles. Current
/// codecs are synchronous data conversions; effect handlers provide the async
/// boundary needed by future blocking or asynchronous codec executors.
#[allow(unsafe_code)]
#[linkme::distributed_slice]
pub static PDATA_CODEC_FACTORIES: [PdataCodecRegistration];

/// An immutable, resolved codec. Construction is confined to the registry.
#[derive(Clone, Copy, Debug)]
pub struct ResolvedCodec(&'static PdataCodecRegistration);

impl PartialEq for ResolvedCodec {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.0, other.0)
    }
}

impl Eq for ResolvedCodec {}

impl ResolvedCodec {
    /// The built-in OTLP representation, known without a registry lookup.
    pub const OTLP: Self = Self(&OTLP_CODEC);

    /// Immutable capabilities and canonical encoding name.
    #[must_use]
    pub const fn metadata(self) -> &'static PdataCodecMetadata {
        self.0.metadata
    }

    /// Checks a signal and operation without re-resolving the codec name.
    pub fn require(self, signal: SignalType, direction: CodecDirection) -> Result<(), Error> {
        let metadata = self.metadata();
        if !metadata.signals.contains(&signal) {
            return Err(codec_error(
                &metadata.encoding,
                format!("unsupported signal {signal:?}"),
            ));
        }
        match direction {
            CodecDirection::Encode if !metadata.can_encode => {
                Err(codec_error(&metadata.encoding, "encoder unavailable"))
            }
            CodecDirection::Decode if !metadata.can_decode => {
                Err(codec_error(&metadata.encoding, "decoder unavailable"))
            }
            _ => Ok(()),
        }
    }

    /// Admits independently encoded input without parsing it. Receivers map
    /// admission failures to their protocol's producer-facing error response.
    pub fn admit(self, signal: SignalType, bytes: Bytes) -> Result<EncodedPdata, Error> {
        self.require(signal, CodecDirection::Decode)?;
        Ok(EncodedPdata::from_resolved(self, signal, bytes))
    }
}

/// Finds a unique codec without selecting a signal or conversion direction.
pub fn find(encoding: &PdataEncoding) -> Result<ResolvedCodec, Error> {
    let mut matches = PDATA_CODEC_FACTORIES
        .iter()
        .filter(|f| &f.metadata.encoding == encoding);
    let factory = matches
        .next()
        .ok_or_else(|| codec_error(encoding, "no codec registered"))?;
    if matches.next().is_some() {
        return Err(codec_error(encoding, "duplicate encoding registration"));
    }
    Ok(ResolvedCodec(factory))
}

/// Iterates the finite set of compiled-in codec identities, without instantiating them.
pub fn registered_codecs() -> impl Iterator<Item = ResolvedCodec> {
    PDATA_CODEC_FACTORIES.iter().map(ResolvedCodec)
}

/// Reusable synchronous codec state owned by a pipeline runtime.
/// Neither payloads nor the immutable registry own these instances.
#[derive(Default)]
pub struct CodecContext {
    codecs: Vec<(ResolvedCodec, Box<dyn PdataCodec>)>,
}

/// Lock-free handle to codec state confined to one pipeline runtime thread.
///
/// Cloning this value only clones an `Rc`. Codec operations are deliberately
/// scoped: mutable codec state cannot escape the call or remain borrowed across
/// an async suspension point.
#[derive(Clone, Default)]
pub struct LocalCodecExecutor {
    context: Rc<RefCell<CodecContext>>,
}

/// Scoped access implemented by runtime-local and sendable codec handles.
#[doc(hidden)]
pub trait CodecExecutor {
    /// Runs one synchronous operation without allowing codec state to escape.
    fn execute<R>(&self, operation: impl FnOnce(&mut CodecContext) -> R) -> R;
}

impl LocalCodecExecutor {
    /// Runs one synchronous codec operation on the runtime-local state.
    ///
    /// This is an implementation hook for pdata-aware effect handlers. Node
    /// implementations should use those higher-level capabilities instead.
    #[doc(hidden)]
    pub fn execute<R>(&self, operation: impl FnOnce(&mut CodecContext) -> R) -> R {
        operation(&mut self.context.borrow_mut())
    }

    /// Extracts or decodes native records while retaining failed input.
    pub fn try_into_otap(
        &self,
        payload: OtapPayload,
        options: ConversionOptions,
    ) -> Result<OtapArrowRecords, OtapPayloadDecodeError> {
        self.execute(|context| payload.try_into_otap_with(context, options))
    }

    /// Borrows a representation-independent view of a payload.
    pub fn view<'a>(
        &self,
        payload: &'a OtapPayload,
        options: ConversionOptions,
    ) -> Result<PayloadView<'a>, crate::encode::Error> {
        self.execute(|context| payload.view_with(context, options))
    }

    /// Encodes inside a scope that cannot outlive reusable codec storage.
    pub fn with_encoded<R>(
        &self,
        payload: &mut OtapPayload,
        codec: ResolvedCodec,
        options: ConversionOptions,
        consume: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, Error> {
        self.execute(|context| {
            let output = payload.prepare_encoded(context, codec, options)?;
            Ok(consume(output.as_ref()))
        })
    }

    /// Encodes to owned bytes suitable for an asynchronous send.
    pub fn encode_owned(
        &self,
        payload: &mut OtapPayload,
        codec: ResolvedCodec,
        options: ConversionOptions,
    ) -> Result<Bytes, Error> {
        self.execute(|context| {
            payload
                .prepare_encoded(context, codec, options)
                .map(EncodedOutput::into_bytes)
        })
    }

    /// Returns whether two handles address the same runtime-owned state.
    #[must_use]
    pub fn shares_state_with(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.context, &other.context)
    }
}

impl CodecExecutor for LocalCodecExecutor {
    fn execute<R>(&self, operation: impl FnOnce(&mut CodecContext) -> R) -> R {
        Self::execute(self, operation)
    }
}

/// Sendable handle to codec state used by shared pipeline nodes.
///
/// Shared nodes may execute on worker threads, so this variant serializes the
/// current synchronous codec implementations. Future async and blocking codec
/// pools can replace the implementation without changing node-facing APIs.
#[derive(Clone, Default)]
pub struct SharedCodecExecutor {
    context: Arc<Mutex<CodecContext>>,
}

impl SharedCodecExecutor {
    fn lock(&self) -> MutexGuard<'_, CodecContext> {
        self.context
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Runs one synchronous codec operation on shared runtime state.
    ///
    /// This is an implementation hook for pdata-aware effect handlers. Node
    /// implementations should use those higher-level capabilities instead.
    #[doc(hidden)]
    pub fn execute<R>(&self, operation: impl FnOnce(&mut CodecContext) -> R) -> R {
        operation(&mut self.lock())
    }

    /// Extracts or decodes native records while retaining failed input.
    pub fn try_into_otap(
        &self,
        payload: OtapPayload,
        options: ConversionOptions,
    ) -> Result<OtapArrowRecords, OtapPayloadDecodeError> {
        self.execute(|context| payload.try_into_otap_with(context, options))
    }

    /// Borrows a representation-independent view of a payload.
    pub fn view<'a>(
        &self,
        payload: &'a OtapPayload,
        options: ConversionOptions,
    ) -> Result<PayloadView<'a>, crate::encode::Error> {
        self.execute(|context| payload.view_with(context, options))
    }

    /// Encodes inside a scope that cannot outlive reusable codec storage.
    pub fn with_encoded<R>(
        &self,
        payload: &mut OtapPayload,
        codec: ResolvedCodec,
        options: ConversionOptions,
        consume: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, Error> {
        self.execute(|context| {
            let output = payload.prepare_encoded(context, codec, options)?;
            Ok(consume(output.as_ref()))
        })
    }

    /// Encodes to owned bytes suitable for an asynchronous send.
    pub fn encode_owned(
        &self,
        payload: &mut OtapPayload,
        codec: ResolvedCodec,
        options: ConversionOptions,
    ) -> Result<Bytes, Error> {
        self.execute(|context| {
            payload
                .prepare_encoded(context, codec, options)
                .map(EncodedOutput::into_bytes)
        })
    }

    /// Returns whether two handles address the same runtime-owned state.
    #[must_use]
    pub fn shares_state_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.context, &other.context)
    }
}

impl CodecExecutor for SharedCodecExecutor {
    fn execute<R>(&self, operation: impl FnOnce(&mut CodecContext) -> R) -> R {
        Self::execute(self, operation)
    }
}

/// Codec handles created once for a pipeline runtime and injected into effect handlers.
#[derive(Clone, Default)]
pub struct CodecExecutors {
    /// Direct, lock-free executor used by local nodes on the pipeline thread.
    pub local: LocalCodecExecutor,
    /// Sendable executor used by shared nodes running on worker threads.
    pub shared: SharedCodecExecutor,
}

impl CodecContext {
    pub(crate) fn instance(&mut self, codec: ResolvedCodec) -> &mut dyn PdataCodec {
        let index = match self.codecs.iter().position(|(key, _)| *key == codec) {
            Some(index) => index,
            None => {
                let index = self.codecs.len();
                self.codecs.push((codec, (codec.0.create)()));
                index
            }
        };
        self.codecs[index].1.as_mut()
    }

    /// Decodes using reusable runtime-owned state and verifies the codec's signal contract.
    pub fn decode(
        &mut self,
        encoded: &EncodedPdata,
        options: ConversionOptions,
    ) -> Result<OtapArrowRecords, crate::encode::Error> {
        let codec = encoded.codec;
        let signal = encoded.signal;
        let records = self
            .instance(codec)
            .decode(signal, &encoded.bytes, options)
            .map_err(|error| codec_error(&codec.metadata().encoding, error.to_string()))?;
        if records.signal_type() != signal {
            return Err(codec_error(
                &codec.metadata().encoding,
                "decoder changed the signal type",
            )
            .into());
        }
        Ok(records)
    }

    /// Creates a view through the same codec instance used for conversion.
    pub fn view<'a>(
        &mut self,
        encoded: &'a EncodedPdata,
        options: ConversionOptions,
    ) -> Result<PayloadView<'a>, crate::encode::Error> {
        let codec = encoded.codec;
        let view = self
            .instance(codec)
            .view(encoded.signal, &encoded.bytes, options)
            .map_err(|error| codec_error(&codec.metadata().encoding, error.to_string()))?;
        if view.signal_type() != encoded.signal {
            return Err(
                codec_error(&codec.metadata().encoding, "view changed the signal type").into(),
            );
        }
        Ok(view)
    }

    pub(crate) fn encode_records<'a>(
        &'a mut self,
        records: &mut OtapArrowRecords,
        codec: ResolvedCodec,
        options: ConversionOptions,
    ) -> Result<EncodedOutput<'a>, Error> {
        codec.require(records.signal_type(), CodecDirection::Encode)?;
        self.instance(codec).prepare_encode(records, options)
    }
}

struct OtlpEncoderState {
    logs: LogsProtoBytesEncoder,
    metrics: MetricsProtoBytesEncoder,
    traces: TracesProtoBytesEncoder,
    logs_buffer: ProtoBuffer,
    metrics_buffer: ProtoBuffer,
    traces_buffer: ProtoBuffer,
}

impl Default for OtlpEncoderState {
    fn default() -> Self {
        Self {
            logs: LogsProtoBytesEncoder::default(),
            metrics: MetricsProtoBytesEncoder::default(),
            traces: TracesProtoBytesEncoder::default(),
            // Preserve gRPC buffer growth and keep large batches in one signal
            // from inflating detached allocations for the other signals.
            logs_buffer: ProtoBuffer::with_capacity(8 * 1024),
            metrics_buffer: ProtoBuffer::with_capacity(8 * 1024),
            traces_buffer: ProtoBuffer::with_capacity(8 * 1024),
        }
    }
}

enum OutputStorage<'a> {
    Bytes(Bytes),
    Buffer(&'a mut ProtoBuffer),
}

/// Prepared output, either original shared bytes or reusable encoder storage.
/// Borrow it for compression, or take ownership before an asynchronous send.
pub struct EncodedOutput<'a>(OutputStorage<'a>);

impl<'a> EncodedOutput<'a> {
    /// Returns independently owned encoded bytes.
    pub fn bytes(bytes: Bytes) -> Self {
        Self(OutputStorage::Bytes(bytes))
    }

    /// Borrows a bounded encoder buffer until the output is consumed or dropped.
    pub fn buffer(buffer: &'a mut ProtoBuffer) -> Self {
        Self(OutputStorage::Buffer(buffer))
    }

    /// Detaches an encoder buffer without copying and replenishes its capacity.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        match self.0 {
            OutputStorage::Bytes(bytes) => bytes,
            OutputStorage::Buffer(buffer) => {
                let (bytes, capacity) = buffer.take_into_bytes();
                buffer.ensure_capacity(capacity);
                bytes
            }
        }
    }

    /// Keeps scratch capacity and copies only when the output uses that scratch.
    #[must_use]
    pub fn copy_into_bytes(self) -> Bytes {
        match self.0 {
            OutputStorage::Bytes(bytes) => bytes,
            OutputStorage::Buffer(buffer) => Bytes::copy_from_slice(buffer.as_ref()),
        }
    }
}

impl AsRef<[u8]> for EncodedOutput<'_> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            OutputStorage::Bytes(bytes) => bytes.as_ref(),
            OutputStorage::Buffer(buffer) => buffer.as_ref(),
        }
    }
}

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
        if ["otap", "otlp", "preserve"].contains(&name) {
            return Err(codec_error(&metadata.encoding, "reserved format name"));
        }
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
        if let Some(batching) = &metadata.batching {
            batching.default_profile.validate()?;
            if !metadata.can_decode || !batching.sizers.contains(&batching.default_profile.sizer) {
                return Err(codec_error(
                    &metadata.encoding,
                    "native batching requires a decoder and a supported default sizer",
                ));
            }
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
/// known. Receivers resolve message-specific codec names before admitting input.
pub fn resolve(
    encoding: &PdataEncoding,
    signal: SignalType,
    direction: CodecDirection,
) -> Result<ResolvedCodec, Error> {
    let codec = find(encoding)?;
    codec.require(signal, direction)?;
    Ok(codec)
}

/// Built-in codec for the existing OTLP protobuf representation.
#[derive(Default)]
pub struct OtlpCodec {
    encoder: Option<Box<OtlpEncoderState>>,
}

impl PdataCodec for OtlpCodec {
    fn decode(
        &mut self,
        signal: SignalType,
        bytes: &Bytes,
        options: ConversionOptions,
    ) -> Result<OtapArrowRecords, crate::encode::Error> {
        OtlpProtoBytes::new_from_bytes(signal, bytes.clone()).try_into_with_options(options)
    }

    fn encode(
        &mut self,
        mut records: OtapArrowRecords,
        options: ConversionOptions,
    ) -> Result<Bytes, Error> {
        Ok(self.prepare_encode(&mut records, options)?.into_bytes())
    }

    fn prepare_encode<'a>(
        &'a mut self,
        records: &mut OtapArrowRecords,
        options: ConversionOptions,
    ) -> Result<EncodedOutput<'a>, Error> {
        let state = self
            .encoder
            .get_or_insert_with(|| Box::new(OtlpEncoderState::default()));
        let signal = records.signal_type();
        let buffer = match signal {
            SignalType::Logs => &mut state.logs_buffer,
            SignalType::Metrics => &mut state.metrics_buffer,
            SignalType::Traces => &mut state.traces_buffer,
        };
        buffer.clear();
        buffer.set_limit(
            options
                .otlp_size_limit
                .map_or(crate::otlp::common::MAX_OTLP_SIZE_LIMIT, |limit| {
                    limit.get().min(crate::otlp::common::MAX_OTLP_SIZE_LIMIT)
                }),
        );
        match signal {
            SignalType::Logs => state.logs.encode(records, buffer)?,
            SignalType::Metrics => state.metrics.encode(records, buffer)?,
            SignalType::Traces => state.traces.encode(records, buffer)?,
        }
        Ok(EncodedOutput::buffer(buffer))
    }

    fn view<'a>(
        &mut self,
        signal: SignalType,
        bytes: &'a Bytes,
        _options: ConversionOptions,
    ) -> Result<PayloadView<'a>, crate::encode::Error> {
        Ok(PayloadView::OtlpBytes { signal, bytes })
    }

    fn batch(
        &mut self,
        signal: SignalType,
        profile: &BatchProfile,
        inputs: Vec<Bytes>,
    ) -> Result<CodecBatches, Error> {
        let limit = |value: Option<std::num::NonZeroUsize>| {
            value.map(|value| std::num::NonZeroU64::new(value.get() as u64).expect("nonzero"))
        };
        let result = crate::otlp::batching::make_bytes_batches_owned(
            signal,
            limit(profile.max_size),
            limit(profile.max_split_fragments),
            limit(profile.max_split_overhead_bytes),
            limit(profile.max_split_fragments_per_flush),
            inputs
                .into_iter()
                .map(|bytes| OtlpProtoBytes::new_from_bytes(signal, bytes))
                .collect(),
        )?;
        Ok(CodecBatches {
            batches: result
                .batches
                .into_iter()
                .map(|(mut bytes, weight)| (bytes.replace_bytes(Bytes::new()), weight))
                .collect(),
            budget_fallbacks: result.budget_fallbacks,
        })
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
    batching: Some(BatchingSupport {
        sizers: &[BatchSizer::Bytes],
        default_profile: BatchProfile::otlp(),
    }),
};

#[allow(unsafe_code)]
#[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]
static OTLP_CODEC: PdataCodecRegistration = PdataCodecRegistration {
    metadata: &OTLP_METADATA,
    create: || Box::new(OtlpCodec::default()),
    count_items: Some(|signal, bytes| Some(crate::payload::count_otlp_items(signal, bytes))),
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OtapPayload;
    use crate::testing::fixtures::logs_with_full_resource_and_scope;
    use crate::testing::round_trip::otlp_bytes_to_message;
    use prost::Message;
    use std::cell::Cell;
    use std::mem::size_of;
    use std::sync::Arc;

    thread_local! {
        static DECODES: Cell<usize> = const { Cell::new(0) };
        static CREATES: Cell<usize> = const { Cell::new(0) };
    }

    const TEST_ENCODING: PdataEncoding = PdataEncoding::new("test-framed-otlp");
    static TEST_METADATA: PdataCodecMetadata = PdataCodecMetadata {
        encoding: TEST_ENCODING,
        signals: &[SignalType::Logs],
        format_version: Some("1"),
        compression: None,
        can_decode: true,
        can_encode: true,
        batching: None,
    };

    // The codec is Send but not Sync. Mutable instances remain behind one
    // runtime executor and are never attached to a payload.
    #[derive(Default)]
    struct TestCodec {
        calls: Cell<usize>,
        otlp: OtlpCodec,
    }

    impl PdataCodec for TestCodec {
        fn decode(
            &mut self,
            signal: SignalType,
            bytes: &Bytes,
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
            self.otlp.decode(signal, &bytes.slice(1..), options)
        }

        fn encode(
            &mut self,
            records: OtapArrowRecords,
            options: ConversionOptions,
        ) -> Result<Bytes, Error> {
            let bytes = self.otlp.encode(records, options)?;
            let mut frame = Vec::with_capacity(bytes.len() + 1);
            frame.push(1);
            frame.extend_from_slice(&bytes);
            Ok(frame.into())
        }
    }

    #[allow(unsafe_code)]
    #[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]
    static TEST_CODEC: PdataCodecRegistration = PdataCodecRegistration {
        count_items: None,
        metadata: &TEST_METADATA,
        create: || {
            CREATES.with(|count| count.set(count.get() + 1));
            Box::<TestCodec>::default()
        },
    };

    static DECODE_ONLY_METADATA: PdataCodecMetadata = PdataCodecMetadata {
        encoding: PdataEncoding::new("test-decode-only"),
        signals: &[SignalType::Logs],
        format_version: None,
        compression: None,
        can_decode: true,
        can_encode: false,
        batching: None,
    };

    #[allow(unsafe_code)]
    #[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]
    static DECODE_ONLY: PdataCodecRegistration = PdataCodecRegistration {
        count_items: None,
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

    fn framed_logs_payload() -> OtapPayload {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&logs_with_full_resource_and_scope().encode_to_vec());
        EncodedPdata::new(TEST_ENCODING, SignalType::Logs, bytes.into())
            .expect("registered test codec")
            .into()
    }

    /// Scenario: cloned local executor handles perform repeated codec work.
    /// Guarantees: handles share one lazily created codec instance without a lock or per-node state.
    #[test]
    fn local_executor_clones_share_lazy_codec_state() {
        CREATES.with(|count| count.set(0));
        let first = LocalCodecExecutor::default();
        let second = first.clone();
        assert!(first.shares_state_with(&second));

        _ = first
            .try_into_otap(framed_logs_payload(), Default::default())
            .unwrap();
        _ = second
            .try_into_otap(framed_logs_payload(), Default::default())
            .unwrap();

        CREATES.with(|count| assert_eq!(count.get(), 1));
    }

    /// Scenario: cloned shared executor handles perform repeated codec work.
    /// Guarantees: the sendable handles share one lazily created codec instance and are Send + Sync.
    #[test]
    fn shared_executor_clones_share_lazy_codec_state() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SharedCodecExecutor>();

        CREATES.with(|count| count.set(0));
        let first = SharedCodecExecutor::default();
        let second = first.clone();
        assert!(first.shares_state_with(&second));

        _ = first
            .try_into_otap(framed_logs_payload(), Default::default())
            .unwrap();
        _ = second
            .try_into_otap(framed_logs_payload(), Default::default())
            .unwrap();

        CREATES.with(|count| assert_eq!(count.get(), 1));
    }

    /// Scenario: OTLP payloads pass through the generalized encoded API.
    /// Guarantees: payload layout stays compact and signal/bytes are unchanged without a decode.
    #[test]
    fn otlp_passthrough_keeps_original_buffer() {
        assert_eq!(
            size_of::<OtapPayloadDecodeError>(),
            size_of::<usize>(),
            "recoverable failures stay off the successful conversion path"
        );
        for signal in [SignalType::Logs, SignalType::Metrics, SignalType::Traces] {
            let bytes = Bytes::from(vec![0xff, 0x80]); // Deliberately not decodable.
            let shared = bytes.clone();
            let pointer = shared.as_ptr();
            let payload = OtapPayload::from_encoded(
                EncodedPdata::new(PdataEncoding::OTLP, signal, bytes)
                    .expect("registered test codec"),
            );
            assert_eq!(payload.encoding(), Some(&PdataEncoding::OTLP));
            assert!(payload.encoded_bytes().is_some());
            let output = payload
                .into_encoded(PdataEncoding::OTLP, Default::default())
                .unwrap();
            assert_eq!(output.signal_type(), signal);
            assert_eq!(output.bytes().as_ptr(), pointer);
            assert_eq!(output.bytes().as_ref(), &[0xff, 0x80]);
        }
    }

    /// Scenario: a consumer takes ownership of an already-native payload.
    /// Guarantees: records move without cloning their Arrow arrays and no codec is instantiated.
    #[test]
    fn native_recoverable_conversion_moves_without_codec() {
        let records = logs_payload()
            .try_into_otap_with(&mut CodecContext::default(), Default::default())
            .unwrap();
        let column = records
            .root_record_batch()
            .expect("logs root batch")
            .column(0)
            .clone();
        let pointer = Arc::as_ptr(&column) as *const ();
        let mut context = CodecContext::default();
        let output = OtapPayload::from(records)
            .try_into_otap_with(&mut context, Default::default())
            .unwrap();
        let output_pointer = Arc::as_ptr(
            output
                .root_record_batch()
                .expect("logs root batch")
                .column(0),
        ) as *const ();
        assert_eq!(output_pointer, pointer);
        assert!(context.codecs.is_empty());
    }

    /// Scenario: an admitted encoding is measured, cloned and exported unchanged.
    /// Guarantees: passthrough does not instantiate a codec and shares bytes; empty remainders reset counts.
    #[test]
    fn admitted_encoding_passthrough_and_measurements() {
        let encoding = TEST_ENCODING;
        let bytes = Bytes::from(vec![1, 2, 3]);
        let pointer = bytes.as_ptr();
        let mut payload = OtapPayload::from_encoded(
            EncodedPdata::new(encoding.clone(), SignalType::Logs, bytes)
                .expect("registered test codec")
                .with_item_count(7),
        );
        assert_eq!(payload.num_items(), 7);
        assert_eq!(payload.num_bytes(), Some(3));
        assert_eq!(payload.retained_memory_bytes(), 3);
        assert!(!payload.is_empty());
        let mut clone = payload.clone();
        assert_eq!(
            payload.encoded_bytes().expect("encoded input").as_ptr(),
            clone.encoded_bytes().expect("encoded clone").as_ptr()
        );
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
        decoded
            .materialize_otap_with(&mut CodecContext::default(), Default::default())
            .unwrap();
        decoded
            .materialize_otap_with(&mut CodecContext::default(), Default::default())
            .unwrap();
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
        {
            let encoding = TEST_ENCODING;
            let bytes = Bytes::from(vec![0]);
            let pointer = bytes.as_ptr();
            let payload = OtapPayload::from_encoded(
                EncodedPdata::new(encoding.clone(), SignalType::Logs, bytes)
                    .expect("registered test codec")
                    .with_item_count(5),
            );
            let error = payload
                .try_into_otap_with(&mut CodecContext::default(), Default::default())
                .unwrap_err();
            assert!(error.error().to_string().contains(encoding.as_str()));
            let (_error, mut payload) = error.into_parts();
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
                count_items: None,
                metadata: &TEST_METADATA,
                create: || Box::<TestCodec>::default(),
            },
            PdataCodecRegistration {
                count_items: None,
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
        payload
            .materialize_otap_with(&mut CodecContext::default(), Default::default())
            .unwrap();
        let original = payload
            .clone()
            .try_into_otap_with(&mut CodecContext::default(), Default::default())
            .unwrap();
        let options = ConversionOptions {
            otlp_size_limit: std::num::NonZeroUsize::new(1),
        };
        assert!(payload.convert_encoding(TEST_ENCODING, options).is_err());
        assert_eq!(payload.encoding(), None);
        assert_eq!(
            payload
                .try_into_otap_with(&mut CodecContext::default(), Default::default())
                .unwrap(),
            original
        );

        let encoded = logs_payload()
            .into_encoded(TEST_ENCODING, Default::default())
            .unwrap();
        let mut payload: OtapPayload =
            EncodedPdata::new(TEST_ENCODING, SignalType::Logs, encoded.bytes().clone())
                .unwrap()
                .into();
        let mut context = CodecContext::default();
        assert!(payload.known_item_count().is_none());
        assert!(
            payload
                .prepare_encoded(
                    &mut context,
                    ResolvedCodec::OTLP,
                    ConversionOptions {
                        otlp_size_limit: std::num::NonZeroUsize::new(1),
                    }
                )
                .is_err()
        );
        assert!(payload.known_item_count().is_none());
        assert_eq!(
            payload.encoded_bytes().unwrap().as_ptr(),
            encoded.bytes().as_ptr()
        );
    }

    /// Scenario: a defective codec returns a different signal from the envelope.
    /// Guarantees: the framework rejects the conversion and preserves the original signal.
    #[test]
    fn decoder_cannot_change_signal() {
        let mut payload = OtapPayload::from_encoded(
            EncodedPdata::new(TEST_ENCODING, SignalType::Logs, Bytes::from_static(&[2]))
                .expect("registered test codec"),
        );
        assert!(
            payload
                .materialize_otap_with(&mut CodecContext::default(), Default::default())
                .unwrap_err()
                .to_string()
                .contains("decoder changed the signal type")
        );
        assert_eq!(payload.signal_type(), SignalType::Logs);
        assert_eq!(payload.encoding(), Some(&TEST_ENCODING));
        assert!(
            TestCodec::default()
                .measure(
                    SignalType::Logs,
                    Bytes::from_static(&[2]),
                    BatchSizer::Items
                )
                .is_err()
        );
    }

    /// Scenario: a receiver resolves an input before admitting its bytes.
    /// Guarantees: unknown codecs, unavailable decoders and unsupported signals
    /// fail admission; valid handles admit malformed bytes without eager decoding.
    #[test]
    fn receiver_admission_requires_a_decoder_but_is_lazy() {
        DECODES.with(|count| count.set(0));
        CREATES.with(|count| count.set(0));
        for (encoding, signal, expected) in [
            (
                PdataEncoding::new("missing"),
                SignalType::Logs,
                "no codec registered",
            ),
            (
                crate::testing::codec::ENCODE_ONLY_ENCODING,
                SignalType::Logs,
                "decoder unavailable",
            ),
            (TEST_ENCODING, SignalType::Metrics, "unsupported signal"),
        ] {
            let error = EncodedPdata::new(encoding, signal, Bytes::new()).unwrap_err();
            assert!(error.to_string().contains(expected));
        }
        let codec = resolve(&TEST_ENCODING, SignalType::Logs, CodecDirection::Decode).unwrap();
        let input = Bytes::from_static(&[0xff]);
        let admitted = codec.admit(SignalType::Logs, input.clone()).unwrap();
        assert_eq!(admitted.bytes().as_ptr(), input.as_ptr());
        assert_eq!(DECODES.with(Cell::get), 0);
        assert_eq!(CREATES.with(Cell::get), 0);
    }

    /// Scenario: one node processes multiple encoded messages and a second node
    /// receives a fan-out clone, including a malformed message between valid ones.
    /// Guarantees: codec instances are reused within a node, isolated across nodes,
    /// and remain usable after errors without attaching mutable state to messages.
    #[test]
    fn consumer_context_reuses_and_isolates_codec_instances() {
        let input = logs_payload()
            .into_encoded(TEST_ENCODING, Default::default())
            .unwrap();
        CREATES.with(|count| count.set(0));
        let mut first = CodecContext::default();
        let mut second = CodecContext::default();
        _ = first.decode(&input, Default::default()).unwrap();
        _ = first.decode(&input, Default::default()).unwrap();
        let bad = input
            .codec()
            .admit(SignalType::Logs, Bytes::from_static(&[0]))
            .unwrap();
        assert!(first.decode(&bad, Default::default()).is_err());
        _ = first.decode(&input, Default::default()).unwrap();
        assert_eq!(CREATES.with(Cell::get), 1);
        _ = second.decode(&input, Default::default()).unwrap();
        assert_eq!(CREATES.with(Cell::get), 2);
    }

    /// Scenario: each OTLP operation is the first use of a consumer's codec context.
    /// Guarantees: decode, encode, views and native batching all instantiate the
    /// registered codec once and reuse it on subsequent calls for every signal.
    #[test]
    fn otlp_operations_use_registered_consumer_state() {
        use crate::batching::{BatchPlan, PdataFormat};
        for (signal, bytes) in [
            (
                SignalType::Logs,
                logs_with_full_resource_and_scope().encode_to_vec(),
            ),
            (
                SignalType::Metrics,
                crate::testing::fixtures::metrics_sum_with_full_resource_and_scope()
                    .encode_to_vec(),
            ),
            (
                SignalType::Traces,
                crate::testing::fixtures::traces_with_full_resource_and_scope().encode_to_vec(),
            ),
        ] {
            let encoded = ResolvedCodec::OTLP.admit(signal, bytes.into()).unwrap();
            let original = OtapPayload::from(encoded.clone());
            let records = original
                .clone()
                .try_into_otap_with(&mut CodecContext::default(), Default::default())
                .unwrap();
            for operation in 0..4 {
                let mut context = CodecContext::default();
                for _ in 0..2 {
                    match operation {
                        0 => {
                            let result = original
                                .clone()
                                .try_into_otap_with(&mut context, Default::default())
                                .unwrap();
                            assert_eq!(result.signal_type(), signal);
                        }
                        1 => {
                            let mut payload = OtapPayload::from(records.clone());
                            let output = payload
                                .prepare_encoded(
                                    &mut context,
                                    ResolvedCodec::OTLP,
                                    Default::default(),
                                )
                                .unwrap();
                            assert!(!output.as_ref().is_empty());
                        }
                        2 => {
                            let PayloadView::OtlpBytes {
                                signal: viewed_signal,
                                bytes: viewed_bytes,
                            } = original
                                .view_with(&mut context, Default::default())
                                .unwrap()
                            else {
                                panic!("OTLP codec must supply a borrowed protobuf view");
                            };
                            assert_eq!(viewed_signal, signal);
                            assert_eq!(viewed_bytes.as_ptr(), encoded.bytes().as_ptr());
                        }
                        _ => {
                            let plan =
                                BatchPlan::new(PdataFormat::OTLP, BatchProfile::otlp(), true)
                                    .unwrap();
                            let output = plan
                                .batch(signal, vec![original.clone()], &mut context)
                                .unwrap();
                            assert!(!output.batches.is_empty());
                            assert!(
                                output
                                    .batches
                                    .iter()
                                    .all(|(batch, _)| batch.format() == PdataFormat::OTLP)
                            );
                        }
                    }
                    assert_eq!(context.codecs.len(), 1);
                    assert_eq!(context.codecs[0].0, ResolvedCodec::OTLP);
                }
            }
        }
    }

    /// Scenario: an encoded format supplies a stateless item counter and is cloned
    /// before and after metrics request a count, then forwarded without conversion.
    /// Guarantees: counts stay lazy and branch-local, and neither measurement nor
    /// passthrough instantiates a codec or copies the encoded buffer.
    #[test]
    fn registered_item_counter_is_lazy_cached_and_instance_free() {
        thread_local! {
            static COUNTS: Cell<usize> = const { Cell::new(0) };
        }
        static REGISTRATION: PdataCodecRegistration = PdataCodecRegistration {
            metadata: &TEST_METADATA,
            create: || panic!("counting and passthrough must not instantiate a codec"),
            count_items: Some(|_, bytes| {
                COUNTS.with(|count| count.set(count.get() + 1));
                Some(bytes.len())
            }),
        };
        let codec = ResolvedCodec(&REGISTRATION);
        let mut payload: OtapPayload = codec
            .admit(SignalType::Logs, Bytes::from_static(b"data"))
            .unwrap()
            .into();
        let mut before = payload.clone();
        assert_eq!(payload.known_item_count(), None);
        assert_eq!(COUNTS.with(Cell::get), 0);
        assert_eq!(payload.num_items(), 4);
        assert_eq!(payload.num_items(), 4);
        assert_eq!(COUNTS.with(Cell::get), 1);
        assert_eq!(before.known_item_count(), None);
        assert_eq!(before.num_items(), 4);
        assert_eq!(COUNTS.with(Cell::get), 2);
        let mut after = payload.clone();
        assert_eq!(after.num_items(), 4);
        assert_eq!(COUNTS.with(Cell::get), 2);
        let mut context = CodecContext::default();
        let output = payload
            .prepare_encoded(&mut context, codec, Default::default())
            .unwrap();
        assert_eq!(
            output.as_ref().as_ptr(),
            before.encoded_bytes().unwrap().as_ptr()
        );
        assert!(context.codecs.is_empty());
    }

    /// Scenario: an exporter borrows OTLP scratch for compression, interleaves
    /// signals, detaches it for gRPC, and encounters a temporary size limit.
    /// Guarantees: each signal reuses separate scratch, zero-copy detachment
    /// survives errors, and conversion options reset for the next request.
    #[test]
    fn prepared_output_reuses_scratch_and_detaches_without_copying() {
        let mut payload = logs_payload();
        payload
            .materialize_otap_with(&mut CodecContext::default(), Default::default())
            .unwrap();
        let mut context = CodecContext::default();
        let output = payload
            .prepare_encoded(&mut context, ResolvedCodec::OTLP, Default::default())
            .unwrap();
        let pointer = output.as_ref().as_ptr();
        let expected = output.copy_into_bytes();
        let mut pointers = vec![pointer];
        for (signal, bytes) in [
            (
                SignalType::Metrics,
                crate::testing::fixtures::metrics_sum_with_full_resource_and_scope()
                    .encode_to_vec(),
            ),
            (
                SignalType::Traces,
                crate::testing::fixtures::traces_with_full_resource_and_scope().encode_to_vec(),
            ),
        ] {
            let mut other = OtapPayload::from(OtlpProtoBytes::new_from_bytes(signal, bytes));
            other
                .materialize_otap_with(&mut CodecContext::default(), Default::default())
                .unwrap();
            let output = other
                .prepare_encoded(&mut context, ResolvedCodec::OTLP, Default::default())
                .unwrap();
            let other_pointer = output.as_ref().as_ptr();
            assert!(!pointers.contains(&other_pointer));
            pointers.push(other_pointer);
            let expected_other = output.copy_into_bytes();
            let output = other
                .prepare_encoded(&mut context, ResolvedCodec::OTLP, Default::default())
                .unwrap();
            assert_eq!(output.as_ref().as_ptr(), other_pointer);
            assert_eq!(output.as_ref(), expected_other.as_ref());
        }
        let output = payload
            .prepare_encoded(&mut context, ResolvedCodec::OTLP, Default::default())
            .unwrap();
        assert_eq!(output.as_ref().as_ptr(), pointer);
        let detached = output.into_bytes();
        assert_eq!(detached.as_ptr(), pointer);
        assert_eq!(detached, expected);
        let options = ConversionOptions {
            otlp_size_limit: std::num::NonZeroUsize::new(1),
        };
        assert!(
            payload
                .prepare_encoded(&mut context, ResolvedCodec::OTLP, options)
                .is_err()
        );
        let output = payload
            .prepare_encoded(&mut context, ResolvedCodec::OTLP, Default::default())
            .unwrap();
        assert_eq!(output.as_ref(), expected.as_ref());
    }

    /// Scenario: registered codecs batch natively, use decoder-only OTAP fallback,
    /// or require explicit re-encoding of an item-based fallback result.
    /// Guarantees: output policy, ownership totals, item limits and signal contents
    /// are preserved without adding codec-specific branches to the processor.
    #[test]
    fn codec_batching_native_fallback_and_explicit_output() {
        use crate::batching::{BatchPlan, PdataFormat};
        use crate::testing::codec::{
            DECODE_ONLY_ENCODING, NATIVE_ENCODING, TEST_ENCODING as FALLBACK_ENCODING,
        };
        let mut original = logs_payload();
        let count = original.num_items();
        let bytes = original.encoded_bytes().unwrap().clone();
        for (encoding, preserve, sizer) in [
            (NATIVE_ENCODING, true, BatchSizer::Bytes),
            (DECODE_ONLY_ENCODING, true, BatchSizer::Items),
            (FALLBACK_ENCODING, false, BatchSizer::Items),
        ] {
            let codec = resolve(&encoding, SignalType::Logs, CodecDirection::Decode).unwrap();
            let format = PdataFormat::encoded(codec);
            let mut profile = format.default_profile();
            profile.min_size = std::num::NonZeroUsize::new(1);
            profile.max_size = if sizer == BatchSizer::Items {
                std::num::NonZeroUsize::new(1)
            } else {
                None
            };
            let plan = BatchPlan::new(format, profile, preserve).unwrap();
            let mut context = CodecContext::default();
            let mut inputs: Vec<OtapPayload> = (0..2)
                .map(|_| codec.admit(SignalType::Logs, bytes.clone()).unwrap().into())
                .collect();
            for input in &mut inputs {
                plan.prepare(input, &mut context).unwrap();
            }
            let result = plan.batch(SignalType::Logs, inputs, &mut context).unwrap();
            let expected_weight = if sizer == BatchSizer::Bytes {
                bytes.len() * 2
            } else {
                count * 2
            };
            assert_eq!(
                result
                    .batches
                    .iter()
                    .map(|(_, weight)| *weight)
                    .sum::<usize>(),
                expected_weight
            );
            let expected_format = if encoding == DECODE_ONLY_ENCODING {
                PdataFormat::OTAP
            } else {
                format
            };
            let mut messages = Vec::new();
            for (mut output, _) in result.batches {
                plan.finish(&mut output, &mut context).unwrap();
                assert_eq!(output.format(), expected_format);
                if sizer == BatchSizer::Items {
                    assert!(output.num_items() <= 1);
                }
                messages.push(otlp_bytes_to_message(
                    output.try_into_with_default().unwrap(),
                ));
            }
            let expected = otlp_bytes_to_message(original.clone().try_into_with_default().unwrap());
            crate::testing::equiv::assert_equivalent(&messages, &[expected.clone(), expected]);
        }
        let decode_only = PdataFormat::resolve(DECODE_ONLY_ENCODING.as_str()).unwrap();
        assert!(BatchPlan::new(decode_only, BatchProfile::otap(), false).is_err());
        assert!(BatchPlan::new(decode_only, BatchProfile::otlp(), true).is_err());
    }

    /// Scenario: an extension batcher drops input ownership, emits zero weights,
    /// or overflows the ownership sum.
    /// Guarantees: invalid output is rejected before any delivery context can
    /// be assigned to a fragment or acknowledged against the wrong input.
    #[test]
    fn native_batcher_must_partition_input_ownership() {
        use crate::batching::{BatchPlan, PdataFormat};
        struct InvalidBatcher;
        impl PdataCodec for InvalidBatcher {
            fn decode(
                &mut self,
                signal: SignalType,
                bytes: &Bytes,
                options: ConversionOptions,
            ) -> Result<OtapArrowRecords, crate::encode::Error> {
                OtlpCodec::default().decode(signal, bytes, options)
            }
            fn encode(
                &mut self,
                records: OtapArrowRecords,
                options: ConversionOptions,
            ) -> Result<Bytes, Error> {
                OtlpCodec::default().encode(records, options)
            }
            fn batch(
                &mut self,
                _: SignalType,
                _: &BatchProfile,
                inputs: Vec<Bytes>,
            ) -> Result<CodecBatches, Error> {
                let bytes = &inputs[0];
                let batches = match bytes[0] {
                    0 => Vec::new(),
                    1 => vec![(bytes.clone(), 0), (bytes.clone(), 1)],
                    _ => vec![(bytes.clone(), usize::MAX), (bytes.clone(), 1)],
                };
                Ok(CodecBatches {
                    batches,
                    budget_fallbacks: 0,
                })
            }
        }
        static METADATA: PdataCodecMetadata = PdataCodecMetadata {
            encoding: PdataEncoding::new("test-invalid-batcher"),
            signals: &[SignalType::Logs],
            format_version: None,
            compression: None,
            can_decode: true,
            can_encode: true,
            batching: Some(BatchingSupport {
                sizers: &[BatchSizer::Bytes],
                default_profile: BatchProfile::otlp(),
            }),
        };
        static REGISTRATION: PdataCodecRegistration = PdataCodecRegistration {
            count_items: None,
            metadata: &METADATA,
            create: || Box::new(InvalidBatcher),
        };
        let codec = ResolvedCodec(&REGISTRATION);
        let plan = BatchPlan::new(PdataFormat::encoded(codec), BatchProfile::otlp(), true).unwrap();
        let mut context = CodecContext::default();
        for bytes in [b"\0", b"\x01", b"\x02"] {
            let payload = codec
                .admit(SignalType::Logs, Bytes::from_static(bytes))
                .unwrap();
            let error = plan
                .batch(SignalType::Logs, vec![payload.into()], &mut context)
                .err()
                .unwrap();
            assert!(error.to_string().contains("partition input ownership"));
        }
    }
}
