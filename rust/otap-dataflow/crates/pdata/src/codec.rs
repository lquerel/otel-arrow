// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Pluggable codecs between independent encoded batches and native OTAP.
//!
//! Codec extensions register immutable factories at link time. A factory creates
//! private codec state owned by a pipeline runtime. Payloads contain identity,
//! signal, bytes, and optional item counts, never codec state. Passing or cloning
//! a payload does not consult the registry or materialize telemetry records.
//!
//! The distributed slice is a candidate catalog, not a conflict-resolution
//! mechanism. The final binary resolves that catalog once into an immutable
//! registry. Duplicate encoding names fail by default; a binary that links a
//! replacement must explicitly select its namespaced provider ID before building
//! any pipeline. Link order never participates in selection.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use bytes::Bytes;
use otel_arrow_dfe_config::{EncodeOptions, SignalType};

#[cfg(test)]
use crate::TryIntoWithOptions;
use crate::batching::{BatchProfile, BatchSizer, BatchingSupport, CodecBatches};
use crate::error::Error;
use crate::otap::OtapArrowRecords;
use crate::otlp::logs::LogsProtoBytesEncoder;
use crate::otlp::metrics::MetricsProtoBytesEncoder;
use crate::otlp::traces::TracesProtoBytesEncoder;
use crate::otlp::{BoundedBuf, ProtoBuffer, ProtoBytesEncoder};
use crate::{OtapPayload, OtapPayloadDecodeError, OtapPayloadHelpers, OtlpProtoBytes, PayloadView};

/// Stable identity of an independently decodable byte representation.
///
/// Names include any version or compression distinction needed to interpret the
/// bytes. Built-in names are reserved; other authors should use a vendor prefix.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
    /// Globally stable wire name; duplicate providers require explicit selection.
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
/// must validate input and preserve the signal.
pub trait PdataCodec: Send {
    /// Converts a borrowed complete encoded batch to native OTAP. Borrowing lets
    /// the caller retain the exact input for recovery without cloning its buffer.
    fn decode(
        &mut self,
        signal: SignalType,
        bytes: &Bytes,
    ) -> Result<OtapArrowRecords, crate::encode::Error>;

    /// Converts native OTAP to a complete independently decodable encoded batch.
    fn encode(&mut self, records: OtapArrowRecords, options: EncodeOptions)
    -> Result<Bytes, Error>;

    /// Prepares output that may borrow reusable encoder storage. The default
    /// supports codecs returning owned bytes; codecs with scratch can override it.
    fn prepare_encode<'a>(
        &'a mut self,
        records: &mut OtapArrowRecords,
        options: EncodeOptions,
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
    ) -> Result<PayloadView<'a>, crate::encode::Error> {
        self.decode(signal, bytes)
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
                let records = self.decode(signal, &bytes).map_err(|error| Error::Format {
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

/// Stable, namespaced identity of one codec implementation.
///
/// This differs from [`PdataEncoding`]: several providers may implement the
/// same wire representation, while a provider ID identifies one implementation
/// that the final binary can select explicitly.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CodecProviderId(&'static str);

impl CodecProviderId {
    /// Declares a compile-time provider identity.
    #[must_use]
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }

    /// Returns the namespaced provider identity used in configuration and diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for CodecProviderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl From<CodecProviderId> for String {
    fn from(provider: CodecProviderId) -> Self {
        provider.as_str().to_owned()
    }
}

/// Link-time codec extension registration. Only factories, not mutable state, are shared.
#[derive(Debug)]
pub struct PdataCodecRegistration {
    /// Unique implementation identity. Replacements share an encoding but not a provider ID.
    pub provider: CodecProviderId,
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
    /// Resolves the provider selected by the final binary for OTLP.
    pub fn otlp() -> Result<Self, Error> {
        codec_registry()?.otlp()
    }

    /// Stable identity of the selected implementation.
    #[must_use]
    pub const fn provider(self) -> CodecProviderId {
        self.0.provider
    }

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

/// Startup-resolved output encoding and its output-specific options.
///
/// Nodes construct this once and reuse it for every payload. Signal support is
/// checked when a concrete payload is encoded because pipelines may carry more
/// than one signal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodingPlan {
    codec: ResolvedCodec,
    options: EncodeOptions,
}

impl EncodingPlan {
    /// Resolves the provider selected by the final binary for default OTLP output.
    pub fn otlp() -> Result<Self, Error> {
        Self::new(ResolvedCodec::otlp()?, EncodeOptions::default())
    }

    /// Builds a plan from an already-resolved codec.
    pub fn new(codec: ResolvedCodec, options: EncodeOptions) -> Result<Self, Error> {
        if !codec.metadata().can_encode {
            return Err(codec_error(
                &codec.metadata().encoding,
                "encoder unavailable",
            ));
        }
        Ok(Self { codec, options })
    }

    /// Resolves an encoding name once while constructing a node.
    pub fn resolve(encoding: &PdataEncoding, options: EncodeOptions) -> Result<Self, Error> {
        Self::new(find(encoding)?, options)
    }

    /// Resolved codec used by this plan.
    #[must_use]
    pub const fn codec(self) -> ResolvedCodec {
        self.codec
    }

    /// Output-specific options used by this plan.
    #[must_use]
    pub const fn options(self) -> EncodeOptions {
        self.options
    }

    pub(crate) fn require(self, signal: SignalType) -> Result<(), Error> {
        self.codec.require(signal, CodecDirection::Encode)
    }
}

/// Explicit provider choice made by the final binary for one wire encoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodecSelection {
    encoding: PdataEncoding,
    provider: String,
}

impl CodecSelection {
    /// Selects one provider for an encoding that has multiple linked implementations.
    #[must_use]
    pub fn new(encoding: PdataEncoding, provider: impl Into<String>) -> Self {
        Self {
            encoding,
            provider: provider.into(),
        }
    }

    /// Encoding whose implementation is selected.
    #[must_use]
    pub fn encoding(&self) -> &PdataEncoding {
        &self.encoding
    }

    /// Namespaced provider identity requested by the final binary.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }
}

/// Startup configuration for the immutable process codec registry.
///
/// The default is deliberately strict: every encoding must have exactly one
/// linked provider. A final binary may explicitly select a provider for an
/// encoding with multiple implementations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CodecRegistryOptions {
    selections: Vec<CodecSelection>,
}

impl CodecRegistryOptions {
    /// Builds startup options from explicit selections, typically loaded by the final binary.
    #[must_use]
    pub fn new(selections: impl IntoIterator<Item = CodecSelection>) -> Self {
        Self {
            selections: selections.into_iter().collect(),
        }
    }

    /// Adds an explicit provider selection.
    #[must_use]
    pub fn select(mut self, encoding: PdataEncoding, provider: impl Into<String>) -> Self {
        self.selections
            .push(CodecSelection::new(encoding, provider));
        self
    }

    /// Returns the requested selections before registry validation.
    #[must_use]
    pub fn selections(&self) -> &[CodecSelection] {
        &self.selections
    }
}

/// Why a provider was selected for an encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CodecSelectionReason {
    /// The provider was the only linked implementation.
    Unique,
    /// The final binary explicitly selected this provider.
    Explicit,
}

/// Diagnostic view of one resolved encoding and all linked candidates.
#[derive(Clone, Debug)]
pub struct CodecRegistryEntry {
    encoding: PdataEncoding,
    selected: ResolvedCodec,
    candidates: Vec<CodecProviderId>,
    reason: CodecSelectionReason,
}

impl CodecRegistryEntry {
    /// Wire encoding represented by this entry.
    #[must_use]
    pub fn encoding(&self) -> &PdataEncoding {
        &self.encoding
    }

    /// Provider selected for routine codec operations.
    #[must_use]
    pub const fn selected(&self) -> ResolvedCodec {
        self.selected
    }

    /// All linked provider identities, sorted deterministically.
    #[must_use]
    pub fn candidates(&self) -> &[CodecProviderId] {
        &self.candidates
    }

    /// Whether selection was implicit because the candidate was unique or explicit.
    #[must_use]
    pub const fn reason(&self) -> CodecSelectionReason {
        self.reason
    }
}

/// Immutable codec choices resolved once before pipeline construction.
#[derive(Debug)]
pub struct CodecRegistry {
    entries: Vec<CodecRegistryEntry>,
    otlp: Option<ResolvedCodec>,
}

impl CodecRegistry {
    fn from_linked(options: CodecRegistryOptions) -> Result<Self, Error> {
        Self::from_registrations(&PDATA_CODEC_FACTORIES, options)
    }

    fn from_registrations(
        registrations: &'static [PdataCodecRegistration],
        options: CodecRegistryOptions,
    ) -> Result<Self, Error> {
        validate_factories(registrations)?;

        let mut requested = BTreeMap::new();
        for selection in options.selections {
            if requested
                .insert(selection.encoding.clone(), selection.provider)
                .is_some()
            {
                return Err(codec_error(
                    &selection.encoding,
                    "provider was selected more than once",
                ));
            }
        }

        let mut groups: BTreeMap<PdataEncoding, Vec<ResolvedCodec>> = BTreeMap::new();
        for registration in registrations {
            groups
                .entry(registration.metadata.encoding.clone())
                .or_default()
                .push(ResolvedCodec(registration));
        }

        let mut entries = Vec::with_capacity(groups.len());
        for (encoding, mut candidates) in groups {
            candidates.sort_unstable_by_key(|codec| codec.provider());
            if candidates
                .windows(2)
                .any(|pair| pair[0].provider() == pair[1].provider())
            {
                return Err(codec_error(
                    &encoding,
                    "the same provider registered this encoding more than once",
                ));
            }

            let requested_provider = requested.remove(&encoding);
            let (selected, reason) = match (candidates.as_slice(), requested_provider) {
                ([only], None) => (*only, CodecSelectionReason::Unique),
                (_, Some(provider)) => {
                    let selected = candidates
                        .iter()
                        .copied()
                        .find(|codec| codec.provider().as_str() == provider)
                        .ok_or_else(|| {
                            codec_error(
                                &encoding,
                                format!(
                                    "selected provider '{provider}' is not linked; available providers: {}",
                                    provider_list(&candidates)
                                ),
                            )
                        })?;
                    (selected, CodecSelectionReason::Explicit)
                }
                (_, None) => {
                    return Err(codec_error(
                        &encoding,
                        format!(
                            "multiple providers are linked; explicitly select one of: {}",
                            provider_list(&candidates)
                        ),
                    ));
                }
            };
            validate_replacement_compatibility(selected, &candidates)?;
            let candidate_providers = candidates
                .iter()
                .map(|candidate| candidate.provider())
                .collect();
            entries.push(CodecRegistryEntry {
                encoding,
                selected,
                candidates: candidate_providers,
                reason,
            });
        }

        if let Some((encoding, provider)) = requested.into_iter().next() {
            return Err(codec_error(
                &encoding,
                format!("provider '{provider}' was selected for an encoding that is not linked"),
            ));
        }
        let otlp = entries
            .binary_search_by(|entry| entry.encoding.cmp(&PdataEncoding::OTLP))
            .ok()
            .map(|index| entries[index].selected);
        Ok(Self { entries, otlp })
    }

    /// Finds the selected provider for a wire encoding.
    pub fn find(&self, encoding: &PdataEncoding) -> Result<ResolvedCodec, Error> {
        self.entries
            .binary_search_by(|entry| entry.encoding.cmp(encoding))
            .map(|index| self.entries[index].selected)
            .map_err(|_| codec_error(encoding, "no codec registered"))
    }

    fn otlp(&self) -> Result<ResolvedCodec, Error> {
        self.otlp
            .ok_or_else(|| codec_error(&PdataEncoding::OTLP, "no codec registered"))
    }

    /// Iterates selected codecs in deterministic encoding-name order.
    #[must_use]
    pub fn codecs(&self) -> impl ExactSizeIterator<Item = ResolvedCodec> + '_ {
        self.entries.iter().map(|entry| entry.selected)
    }

    /// Returns deterministic selection diagnostics, including shadowed providers.
    #[must_use]
    pub fn entries(&self) -> &[CodecRegistryEntry] {
        &self.entries
    }
}

static CODEC_REGISTRY: OnceLock<CodecRegistry> = OnceLock::new();

/// Configures codec overrides exactly once, before any codec lookup or pipeline build.
///
/// Open-source binaries do not call this function and therefore retain strict
/// duplicate rejection. A proprietary final binary calls it during startup with
/// explicit encoding-to-provider selections.
///
/// ```no_run
/// use otel_arrow_dfe_pdata::codec::{
///     CodecRegistryOptions, PdataEncoding, configure_codec_registry,
/// };
///
/// configure_codec_registry(
///     CodecRegistryOptions::default()
///         .select(PdataEncoding::OTLP, "com.example.telemetry.otlp-optimized"),
/// )?;
/// # Ok::<(), otel_arrow_dfe_pdata::error::Error>(())
/// ```
pub fn configure_codec_registry(options: CodecRegistryOptions) -> Result<(), Error> {
    if CODEC_REGISTRY.get().is_some() {
        return Err(registry_error("registry is already initialized"));
    }
    let registry = CodecRegistry::from_linked(options)?;
    CODEC_REGISTRY
        .set(registry)
        .map_err(|_| registry_error("registry was initialized concurrently"))
}

/// Returns the immutable selected registry, applying the strict default on first use.
pub fn codec_registry() -> Result<&'static CodecRegistry, Error> {
    if let Some(registry) = CODEC_REGISTRY.get() {
        return Ok(registry);
    }
    let registry = CodecRegistry::from_linked(CodecRegistryOptions::default())?;
    let _ = CODEC_REGISTRY.set(registry);
    CODEC_REGISTRY
        .get()
        .ok_or_else(|| registry_error("registry initialization failed"))
}

/// Finds the provider selected by the final binary for an encoding.
pub fn find(encoding: &PdataEncoding) -> Result<ResolvedCodec, Error> {
    codec_registry()?.find(encoding)
}

/// Iterates selected codecs without instantiating mutable codec state.
pub fn registered_codecs() -> Result<impl ExactSizeIterator<Item = ResolvedCodec>, Error> {
    Ok(codec_registry()?.codecs())
}

/// Reusable synchronous codec state owned by a pipeline runtime.
/// Neither payloads nor the immutable registry own these instances.
#[derive(Default)]
pub struct CodecState {
    codecs: Vec<(ResolvedCodec, Box<dyn PdataCodec>)>,
}

/// Lock-free handle to codec state confined to one pipeline runtime thread.
///
/// Cloning this value only clones an `Rc`. Codec operations are deliberately
/// scoped: mutable codec state cannot escape the call or remain borrowed across
/// an async suspension point.
#[derive(Clone, Default)]
pub struct LocalCodecExecutor {
    state: Rc<RefCell<CodecState>>,
}

/// Scoped access implemented by runtime-local and sendable codec handles.
#[doc(hidden)]
pub trait CodecExecutor {
    /// Runs one synchronous operation without allowing codec state to escape.
    fn execute<R>(&self, operation: impl FnOnce(&mut CodecState) -> R) -> R;
}

impl LocalCodecExecutor {
    /// Runs one synchronous codec operation on the runtime-local state.
    ///
    /// This is an implementation hook for pdata-aware effect handlers. Node
    /// implementations should use those higher-level capabilities instead.
    #[doc(hidden)]
    pub fn execute<R>(&self, operation: impl FnOnce(&mut CodecState) -> R) -> R {
        operation(&mut self.state.borrow_mut())
    }

    /// Extracts or decodes native records while retaining failed input.
    pub fn try_into_otap(
        &self,
        payload: OtapPayload,
    ) -> Result<OtapArrowRecords, OtapPayloadDecodeError> {
        self.execute(|state| payload.try_into_otap(state))
    }

    /// Borrows a representation-independent view of a payload.
    pub fn view<'a>(
        &self,
        payload: &'a OtapPayload,
    ) -> Result<PayloadView<'a>, crate::encode::Error> {
        self.execute(|state| payload.view(state))
    }

    /// Encodes inside a scope that cannot outlive reusable codec storage.
    pub fn with_encoded<R>(
        &self,
        payload: &mut OtapPayload,
        plan: &EncodingPlan,
        consume: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, Error> {
        self.execute(|state| {
            let output = payload.prepare_encoded(state, plan)?;
            Ok(consume(output.as_ref()))
        })
    }

    /// Encodes to owned bytes suitable for an asynchronous send.
    pub fn encode_owned(
        &self,
        payload: &mut OtapPayload,
        plan: &EncodingPlan,
    ) -> Result<Bytes, Error> {
        self.execute(|state| {
            payload
                .prepare_encoded(state, plan)
                .map(EncodedOutput::into_bytes)
        })
    }

    /// Returns whether two handles address the same runtime-owned state.
    #[must_use]
    pub fn shares_state_with(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.state, &other.state)
    }
}

impl CodecExecutor for LocalCodecExecutor {
    fn execute<R>(&self, operation: impl FnOnce(&mut CodecState) -> R) -> R {
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
    state: Arc<Mutex<CodecState>>,
}

impl SharedCodecExecutor {
    fn lock(&self) -> MutexGuard<'_, CodecState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Runs one synchronous codec operation on shared runtime state.
    ///
    /// This is an implementation hook for pdata-aware effect handlers. Node
    /// implementations should use those higher-level capabilities instead.
    #[doc(hidden)]
    pub fn execute<R>(&self, operation: impl FnOnce(&mut CodecState) -> R) -> R {
        operation(&mut self.lock())
    }

    /// Extracts or decodes native records while retaining failed input.
    pub fn try_into_otap(
        &self,
        payload: OtapPayload,
    ) -> Result<OtapArrowRecords, OtapPayloadDecodeError> {
        self.execute(|state| payload.try_into_otap(state))
    }

    /// Borrows a representation-independent view of a payload.
    pub fn view<'a>(
        &self,
        payload: &'a OtapPayload,
    ) -> Result<PayloadView<'a>, crate::encode::Error> {
        self.execute(|state| payload.view(state))
    }

    /// Encodes inside a scope that cannot outlive reusable codec storage.
    pub fn with_encoded<R>(
        &self,
        payload: &mut OtapPayload,
        plan: &EncodingPlan,
        consume: impl FnOnce(&[u8]) -> R,
    ) -> Result<R, Error> {
        self.execute(|state| {
            let output = payload.prepare_encoded(state, plan)?;
            Ok(consume(output.as_ref()))
        })
    }

    /// Encodes to owned bytes suitable for an asynchronous send.
    pub fn encode_owned(
        &self,
        payload: &mut OtapPayload,
        plan: &EncodingPlan,
    ) -> Result<Bytes, Error> {
        self.execute(|state| {
            payload
                .prepare_encoded(state, plan)
                .map(EncodedOutput::into_bytes)
        })
    }

    /// Returns whether two handles address the same runtime-owned state.
    #[must_use]
    pub fn shares_state_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl CodecExecutor for SharedCodecExecutor {
    fn execute<R>(&self, operation: impl FnOnce(&mut CodecState) -> R) -> R {
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

impl CodecState {
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
    ) -> Result<OtapArrowRecords, crate::encode::Error> {
        let codec = encoded.codec;
        let signal = encoded.signal;
        let records = self
            .instance(codec)
            .decode(signal, &encoded.bytes)
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
    ) -> Result<PayloadView<'a>, crate::encode::Error> {
        let codec = encoded.codec;
        let view = self
            .instance(codec)
            .view(encoded.signal, &encoded.bytes)
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
        plan: &EncodingPlan,
    ) -> Result<EncodedOutput<'a>, Error> {
        plan.require(records.signal_type())?;
        self.instance(plan.codec())
            .prepare_encode(records, plan.options())
    }
}

const OTLP_INITIAL_BUFFER_CAPACITY: usize = 8 * 1024;
const OTLP_MAX_RETAINED_BUFFER_CAPACITY: usize = 256 * 1024;

struct OtlpSignalEncoder<E> {
    encoder: E,
    buffer: ProtoBuffer,
}

impl<E: Default> Default for OtlpSignalEncoder<E> {
    fn default() -> Self {
        Self {
            encoder: E::default(),
            buffer: ProtoBuffer::with_capacity(OTLP_INITIAL_BUFFER_CAPACITY),
        }
    }
}

#[derive(Default)]
struct OtlpEncoderState {
    logs: Option<Box<OtlpSignalEncoder<LogsProtoBytesEncoder>>>,
    metrics: Option<Box<OtlpSignalEncoder<MetricsProtoBytesEncoder>>>,
    traces: Option<Box<OtlpSignalEncoder<TracesProtoBytesEncoder>>>,
}

struct BufferOutput<'a> {
    buffer: &'a mut ProtoBuffer,
    max_retained_capacity: usize,
}

impl Drop for BufferOutput<'_> {
    fn drop(&mut self) {
        self.buffer.retain_capacity(self.max_retained_capacity);
    }
}

enum OutputStorage<'a> {
    Bytes(Bytes),
    Buffer(BufferOutput<'a>),
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
    pub fn buffer(buffer: &'a mut ProtoBuffer, max_retained_capacity: usize) -> Self {
        Self(OutputStorage::Buffer(BufferOutput {
            buffer,
            max_retained_capacity,
        }))
    }

    /// Detaches an encoder buffer without copying and replenishes its capacity.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        match self.0 {
            OutputStorage::Bytes(bytes) => bytes,
            OutputStorage::Buffer(output) => {
                let (bytes, capacity) = output.buffer.take_into_bytes();
                output
                    .buffer
                    .ensure_capacity(capacity.min(output.max_retained_capacity));
                bytes
            }
        }
    }

    /// Keeps scratch capacity and copies only when the output uses that scratch.
    #[must_use]
    pub fn copy_into_bytes(self) -> Bytes {
        match self.0 {
            OutputStorage::Bytes(bytes) => bytes,
            OutputStorage::Buffer(output) => Bytes::copy_from_slice(output.buffer.as_ref()),
        }
    }
}

impl AsRef<[u8]> for EncodedOutput<'_> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            OutputStorage::Bytes(bytes) => bytes.as_ref(),
            OutputStorage::Buffer(output) => output.buffer.as_ref(),
        }
    }
}

pub(crate) fn codec_error(encoding: &PdataEncoding, reason: impl Into<String>) -> Error {
    Error::PdataCodec {
        encoding: encoding.clone(),
        reason: reason.into(),
    }
}

fn registry_error(reason: impl Into<String>) -> Error {
    Error::PdataCodecRegistry {
        reason: reason.into(),
    }
}

fn provider_list(candidates: &[ResolvedCodec]) -> String {
    candidates
        .iter()
        .map(|codec| codec.provider().as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn validate_replacement_compatibility(
    selected: ResolvedCodec,
    candidates: &[ResolvedCodec],
) -> Result<(), Error> {
    let selected_metadata = selected.metadata();
    for candidate in candidates {
        let candidate_metadata = candidate.metadata();
        if selected_metadata.format_version != candidate_metadata.format_version
            || selected_metadata.compression != candidate_metadata.compression
        {
            return Err(codec_error(
                &selected_metadata.encoding,
                format!(
                    "selected provider '{}' has a different wire contract than provider '{}'",
                    selected.provider(),
                    candidate.provider()
                ),
            ));
        }
        if (candidate_metadata.can_decode && !selected_metadata.can_decode)
            || (candidate_metadata.can_encode && !selected_metadata.can_encode)
            || candidate_metadata
                .signals
                .iter()
                .any(|signal| !selected_metadata.signals.contains(signal))
        {
            return Err(codec_error(
                &selected_metadata.encoding,
                format!(
                    "selected provider '{}' does not preserve the capabilities of provider '{}'",
                    selected.provider(),
                    candidate.provider()
                ),
            ));
        }
        if let Some(candidate_batching) = candidate_metadata.batching.as_ref() {
            let Some(selected_batching) = selected_metadata.batching.as_ref() else {
                return Err(codec_error(
                    &selected_metadata.encoding,
                    format!(
                        "selected provider '{}' does not preserve batching from provider '{}'",
                        selected.provider(),
                        candidate.provider()
                    ),
                ));
            };
            if candidate_batching
                .sizers
                .iter()
                .any(|sizer| !selected_batching.sizers.contains(sizer))
                || selected_batching.default_profile != candidate_batching.default_profile
            {
                return Err(codec_error(
                    &selected_metadata.encoding,
                    format!(
                        "selected provider '{}' has incompatible batching metadata with provider '{}'",
                        selected.provider(),
                        candidate.provider()
                    ),
                ));
            }
        }
        if candidate.0.count_items.is_some() && selected.0.count_items.is_none() {
            return Err(codec_error(
                &selected_metadata.encoding,
                format!(
                    "selected provider '{}' does not preserve item counting from provider '{}'",
                    selected.provider(),
                    candidate.provider()
                ),
            ));
        }
    }
    Ok(())
}

/// Initializes and validates the selected registry before a pipeline starts.
pub fn validate_registrations() -> Result<(), Error> {
    codec_registry().map(|_| ())
}

fn validate_factories(factories: &[PdataCodecRegistration]) -> Result<(), Error> {
    for factory in factories {
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
        let provider = factory.provider.as_str();
        if provider.is_empty()
            || !provider.contains('.')
            || !provider
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-:".contains(&b))
        {
            return Err(codec_error(
                &metadata.encoding,
                format!(
                    "provider identity '{provider}' must be namespaced and use lowercase ASCII letters, digits, '.', '_', '-', or ':'"
                ),
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

impl OtlpCodec {
    fn output_limit(options: EncodeOptions) -> usize {
        options
            .otlp_size_limit
            .map_or(crate::otlp::common::MAX_OTLP_SIZE_LIMIT, |limit| {
                limit.get().min(crate::otlp::common::MAX_OTLP_SIZE_LIMIT)
            })
    }

    #[inline(never)]
    fn prepare_logs<'a>(
        state: &'a mut OtlpEncoderState,
        records: &mut OtapArrowRecords,
        options: EncodeOptions,
    ) -> Result<EncodedOutput<'a>, Error> {
        let state = state
            .logs
            .get_or_insert_with(|| Box::new(OtlpSignalEncoder::default()));
        state.buffer.clear();
        state.buffer.set_limit(Self::output_limit(options));
        if let Err(error) = state.encoder.encode(records, &mut state.buffer) {
            state
                .buffer
                .retain_capacity(OTLP_MAX_RETAINED_BUFFER_CAPACITY);
            return Err(error);
        }
        Ok(EncodedOutput::buffer(
            &mut state.buffer,
            OTLP_MAX_RETAINED_BUFFER_CAPACITY,
        ))
    }

    #[inline(never)]
    fn prepare_metrics<'a>(
        state: &'a mut OtlpEncoderState,
        records: &mut OtapArrowRecords,
        options: EncodeOptions,
    ) -> Result<EncodedOutput<'a>, Error> {
        let state = state
            .metrics
            .get_or_insert_with(|| Box::new(OtlpSignalEncoder::default()));
        state.buffer.clear();
        state.buffer.set_limit(Self::output_limit(options));
        if let Err(error) = state.encoder.encode(records, &mut state.buffer) {
            state
                .buffer
                .retain_capacity(OTLP_MAX_RETAINED_BUFFER_CAPACITY);
            return Err(error);
        }
        Ok(EncodedOutput::buffer(
            &mut state.buffer,
            OTLP_MAX_RETAINED_BUFFER_CAPACITY,
        ))
    }

    #[inline(never)]
    fn prepare_traces<'a>(
        state: &'a mut OtlpEncoderState,
        records: &mut OtapArrowRecords,
        options: EncodeOptions,
    ) -> Result<EncodedOutput<'a>, Error> {
        let state = state
            .traces
            .get_or_insert_with(|| Box::new(OtlpSignalEncoder::default()));
        state.buffer.clear();
        state.buffer.set_limit(Self::output_limit(options));
        if let Err(error) = state.encoder.encode(records, &mut state.buffer) {
            state
                .buffer
                .retain_capacity(OTLP_MAX_RETAINED_BUFFER_CAPACITY);
            return Err(error);
        }
        Ok(EncodedOutput::buffer(
            &mut state.buffer,
            OTLP_MAX_RETAINED_BUFFER_CAPACITY,
        ))
    }
}

impl PdataCodec for OtlpCodec {
    fn decode(
        &mut self,
        signal: SignalType,
        bytes: &Bytes,
    ) -> Result<OtapArrowRecords, crate::encode::Error> {
        OtapArrowRecords::try_from(OtlpProtoBytes::new_from_bytes(signal, bytes.clone()))
    }

    fn encode(
        &mut self,
        mut records: OtapArrowRecords,
        options: EncodeOptions,
    ) -> Result<Bytes, Error> {
        Ok(self.prepare_encode(&mut records, options)?.into_bytes())
    }

    fn prepare_encode<'a>(
        &'a mut self,
        records: &mut OtapArrowRecords,
        options: EncodeOptions,
    ) -> Result<EncodedOutput<'a>, Error> {
        let state = self
            .encoder
            .get_or_insert_with(|| Box::new(OtlpEncoderState::default()));
        match records.signal_type() {
            SignalType::Logs => Self::prepare_logs(state, records, options),
            SignalType::Metrics => Self::prepare_metrics(state, records, options),
            SignalType::Traces => Self::prepare_traces(state, records, options),
        }
    }

    fn view<'a>(
        &mut self,
        signal: SignalType,
        bytes: &'a Bytes,
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

/// Provider identity of the open-source OTLP implementation.
pub const OTLP_PROVIDER: CodecProviderId = CodecProviderId::new("org.opentelemetry.otlp.reference");

#[allow(unsafe_code)]
#[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]
static OTLP_CODEC: PdataCodecRegistration = PdataCodecRegistration {
    provider: OTLP_PROVIDER,
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
        ) -> Result<OtapArrowRecords, crate::encode::Error> {
            DECODES.with(|count| count.set(count.get() + 1));
            self.calls.set(self.calls.get() + 1);
            if bytes.first() == Some(&2) {
                return Ok(OtapArrowRecords::Metrics(Default::default()));
            }
            if bytes.first() != Some(&1) {
                return Err(codec_error(&TEST_ENCODING, "invalid test frame").into());
            }
            self.otlp.decode(signal, &bytes.slice(1..))
        }

        fn encode(
            &mut self,
            records: OtapArrowRecords,
            options: EncodeOptions,
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
        provider: CodecProviderId::new("org.opentelemetry.test.framed"),
        count_items: None,
        metadata: &TEST_METADATA,
        create: || {
            CREATES.with(|count| count.set(count.get() + 1));
            Box::<TestCodec>::default()
        },
    };

    const REFERENCE_PROVIDER: CodecProviderId =
        CodecProviderId::new("org.opentelemetry.test.reference");
    const OPTIMIZED_PROVIDER: CodecProviderId = CodecProviderId::new("com.example.test.optimized");
    static DUPLICATE_REGISTRATIONS: [PdataCodecRegistration; 2] = [
        PdataCodecRegistration {
            provider: REFERENCE_PROVIDER,
            count_items: None,
            metadata: &TEST_METADATA,
            create: || Box::<TestCodec>::default(),
        },
        PdataCodecRegistration {
            provider: OPTIMIZED_PROVIDER,
            count_items: None,
            metadata: &TEST_METADATA,
            create: || Box::<TestCodec>::default(),
        },
    ];
    static REVERSED_DUPLICATE_REGISTRATIONS: [PdataCodecRegistration; 2] = [
        PdataCodecRegistration {
            provider: OPTIMIZED_PROVIDER,
            count_items: None,
            metadata: &TEST_METADATA,
            create: || Box::<TestCodec>::default(),
        },
        PdataCodecRegistration {
            provider: REFERENCE_PROVIDER,
            count_items: None,
            metadata: &TEST_METADATA,
            create: || Box::<TestCodec>::default(),
        },
    ];
    static INCOMPATIBLE_METADATA: PdataCodecMetadata = PdataCodecMetadata {
        encoding: TEST_ENCODING,
        signals: &[SignalType::Logs],
        format_version: Some("2"),
        compression: None,
        can_decode: true,
        can_encode: true,
        batching: None,
    };
    static INCOMPATIBLE_REGISTRATIONS: [PdataCodecRegistration; 2] = [
        PdataCodecRegistration {
            provider: REFERENCE_PROVIDER,
            count_items: None,
            metadata: &TEST_METADATA,
            create: || Box::<TestCodec>::default(),
        },
        PdataCodecRegistration {
            provider: OPTIMIZED_PROVIDER,
            count_items: None,
            metadata: &INCOMPATIBLE_METADATA,
            create: || Box::<TestCodec>::default(),
        },
    ];

    /// Scenario: the built-in OTLP codec is checked by the reusable conformance harness.
    /// Guarantees: OTLP preserves signals and counts and forwards matching encoded bytes
    /// without copying them while retaining its permissive protobuf compatibility behavior.
    #[test]
    fn otlp_codec_conforms_to_registered_codec_contract() {
        crate::testing::codec_conformance::assert_decode_conformance(
            crate::testing::codec_conformance::DecodeConformanceCase {
                codec: ResolvedCodec::otlp().expect("selected OTLP codec"),
                signal: SignalType::Logs,
                valid: logs_with_full_resource_and_scope().encode_to_vec().into(),
                malformed: None,
                expected_items: 4,
            },
        );
    }

    /// Scenario: a runtime exports only one telemetry signal through OTLP.
    /// Guarantees: encoder state and its initial scratch allocation are created only for
    /// the signal that is actually encoded.
    #[test]
    fn otlp_encoder_state_is_lazy_per_signal() {
        let mut records = logs_payload()
            .try_into_otap(&mut CodecState::default())
            .unwrap();
        let mut codec = OtlpCodec::default();
        assert!(codec.encoder.is_none());

        let output = codec
            .prepare_encode(&mut records, EncodeOptions::default())
            .unwrap();
        drop(output);

        let state = codec.encoder.as_deref().expect("encoder state");
        assert!(state.logs.is_some());
        assert!(state.metrics.is_none());
        assert!(state.traces.is_none());
    }

    /// Scenario: one encoded batch grows the OTLP scratch allocation far beyond normal use.
    /// Guarantees: detaching the bytes stays zero-copy while replenished scratch capacity is
    /// bounded by the retained-buffer policy.
    #[test]
    fn prepared_output_caps_retained_scratch_after_outlier() {
        let mut buffer = ProtoBuffer::with_capacity(OTLP_MAX_RETAINED_BUFFER_CAPACITY * 2);
        buffer.try_extend(b"payload").unwrap();
        let pointer = buffer.as_ref().as_ptr();

        let bytes =
            EncodedOutput::buffer(&mut buffer, OTLP_MAX_RETAINED_BUFFER_CAPACITY).into_bytes();

        assert_eq!(bytes.as_ptr(), pointer);
        assert_eq!(bytes.as_ref(), b"payload");
        assert!(buffer.capacity() <= OTLP_MAX_RETAINED_BUFFER_CAPACITY);
    }

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
        provider: CodecProviderId::new("org.opentelemetry.test.decode-only"),
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

        _ = first.try_into_otap(framed_logs_payload()).unwrap();
        _ = second.try_into_otap(framed_logs_payload()).unwrap();

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

        _ = first.try_into_otap(framed_logs_payload()).unwrap();
        _ = second.try_into_otap(framed_logs_payload()).unwrap();

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
                .into_encoded_for_test(PdataEncoding::OTLP, Default::default())
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
            .try_into_otap(&mut CodecState::default())
            .unwrap();
        let column = records
            .root_record_batch()
            .expect("logs root batch")
            .column(0)
            .clone();
        let pointer = Arc::as_ptr(&column) as *const ();
        let mut context = CodecState::default();
        let output = OtapPayload::from(records)
            .try_into_otap(&mut context)
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
            .into_encoded_for_test(encoding.clone(), Default::default())
            .unwrap();
        assert_eq!(output.bytes().as_ptr(), pointer);
        assert_eq!(output.item_count(), Some(7));
        assert_eq!(clone.encoding(), Some(&encoding));
        assert_eq!(clone.num_items(), 0);
        assert_eq!(clone.num_bytes(), Some(0));
        assert!(clone.is_empty());
        let output = payload
            .into_encoded_for_test(encoding, Default::default())
            .unwrap();
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
            .into_encoded_for_test(TEST_ENCODING, Default::default())
            .unwrap();
        let mut decoded = OtapPayload::from_encoded(encoded);
        let passthrough = decoded.clone();
        assert_eq!(decoded.num_items(), original.clone().num_items());
        assert_eq!(DECODES.with(Cell::get), 0);
        decoded
            .materialize_otap(&mut CodecState::default())
            .unwrap();
        decoded
            .materialize_otap(&mut CodecState::default())
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
                .try_into_otap(&mut CodecState::default())
                .unwrap_err();
            assert!(error.error().to_string().contains(encoding.as_str()));
            let (_error, mut payload) = error.into_parts();
            let error = payload
                .convert_encoding_for_test(PdataEncoding::new("missing-output"), Default::default())
                .unwrap_err();
            assert!(error.to_string().contains("missing-output"));
            assert_eq!(payload.encoding(), Some(&encoding));
            assert_eq!(payload.num_items(), 5);
            assert_eq!(payload.signal_type(), SignalType::Logs);
            let output = payload
                .into_encoded_for_test(encoding, Default::default())
                .unwrap();
            assert_eq!(output.bytes().as_ptr(), pointer);
        }
    }

    /// Scenario: two linked providers implement the same encoding without an explicit choice.
    /// Guarantees: the safe default rejects ambiguity and reports candidates in stable provider order.
    #[test]
    fn duplicate_providers_require_explicit_selection() {
        let error = CodecRegistry::from_registrations(
            &REVERSED_DUPLICATE_REGISTRATIONS,
            CodecRegistryOptions::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("multiple providers"));
        let optimized = error.find(OPTIMIZED_PROVIDER.as_str()).unwrap();
        let reference = error.find(REFERENCE_PROVIDER.as_str()).unwrap();
        assert!(optimized < reference);
    }

    /// Scenario: a final binary explicitly selects its replacement from duplicate providers.
    /// Guarantees: resolution and diagnostics select the same provider regardless of link order.
    #[test]
    fn explicit_provider_selection_is_deterministic() {
        for registrations in [&DUPLICATE_REGISTRATIONS, &REVERSED_DUPLICATE_REGISTRATIONS] {
            let registry = CodecRegistry::from_registrations(
                registrations,
                CodecRegistryOptions::default().select(TEST_ENCODING, OPTIMIZED_PROVIDER),
            )
            .unwrap();
            let selected = registry.find(&TEST_ENCODING).unwrap();
            assert_eq!(selected.provider(), OPTIMIZED_PROVIDER);
            let [entry] = registry.entries() else {
                panic!("one encoding expected");
            };
            assert_eq!(entry.selected().provider(), OPTIMIZED_PROVIDER);
            assert_eq!(entry.reason(), CodecSelectionReason::Explicit);
            assert_eq!(
                entry.candidates().to_vec(),
                vec![OPTIMIZED_PROVIDER, REFERENCE_PROVIDER]
            );
        }
    }

    /// Scenario: admission and matching-format forwarding use an explicitly selected provider.
    /// Guarantees: provider resolution stays at startup and the payload hot path creates no codec or byte copy.
    #[test]
    fn selected_provider_has_no_forwarding_overhead() {
        let registry = CodecRegistry::from_registrations(
            &DUPLICATE_REGISTRATIONS,
            CodecRegistryOptions::default().select(TEST_ENCODING, OPTIMIZED_PROVIDER),
        )
        .unwrap();
        let codec = registry.find(&TEST_ENCODING).unwrap();
        let bytes = Bytes::from_static(&[1, 2, 3]);
        let pointer = bytes.as_ptr();
        let payload = OtapPayload::from(codec.admit(SignalType::Logs, bytes).unwrap());
        let plan = EncodingPlan::new(codec, EncodeOptions::default()).unwrap();
        let mut state = CodecState::default();

        let forwarded = payload.into_encoded(&mut state, &plan).unwrap();

        assert_eq!(forwarded.bytes().as_ptr(), pointer);
        assert!(state.codecs.is_empty());
    }

    /// Scenario: an override names an absent provider or changes the wire contract.
    /// Guarantees: startup fails before a pipeline can use an unintended or incompatible codec.
    #[test]
    fn invalid_provider_selection_fails_at_startup() {
        let unknown = CodecRegistry::from_registrations(
            &DUPLICATE_REGISTRATIONS,
            CodecRegistryOptions::default().select(TEST_ENCODING, "com.example.test.not-linked"),
        )
        .unwrap_err()
        .to_string();
        assert!(unknown.contains("not linked"));

        let incompatible = CodecRegistry::from_registrations(
            &INCOMPATIBLE_REGISTRATIONS,
            CodecRegistryOptions::default().select(TEST_ENCODING, OPTIMIZED_PROVIDER),
        )
        .unwrap_err()
        .to_string();
        assert!(incompatible.contains("different wire contract"));
    }

    /// Scenario: linked registrations and requested operations are validated before use.
    /// Guarantees: the built-in registry, missing codecs, unsupported signals and directions fail clearly.
    #[test]
    fn registration_and_capabilities_are_validated() {
        validate_registrations().unwrap();
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
    /// Guarantees: encode options reach the codec and a failed encode retains native input.
    #[test]
    fn encode_options_reach_extension_encoder() {
        let mut payload = logs_payload();
        payload
            .materialize_otap(&mut CodecState::default())
            .unwrap();
        let original = payload
            .clone()
            .try_into_otap(&mut CodecState::default())
            .unwrap();
        let options = EncodeOptions {
            otlp_size_limit: std::num::NonZeroUsize::new(1),
        };
        assert!(
            payload
                .convert_encoding_for_test(TEST_ENCODING, options)
                .is_err()
        );
        assert_eq!(payload.encoding(), None);
        assert_eq!(
            payload.try_into_otap(&mut CodecState::default()).unwrap(),
            original
        );

        let encoded = logs_payload()
            .into_encoded_for_test(TEST_ENCODING, Default::default())
            .unwrap();
        let mut payload: OtapPayload =
            EncodedPdata::new(TEST_ENCODING, SignalType::Logs, encoded.bytes().clone())
                .unwrap()
                .into();
        let mut context = CodecState::default();
        let plan = EncodingPlan::new(
            ResolvedCodec::otlp().expect("selected OTLP codec"),
            EncodeOptions {
                otlp_size_limit: std::num::NonZeroUsize::new(1),
            },
        )
        .unwrap();
        assert!(payload.known_item_count().is_none());
        assert!(payload.prepare_encoded(&mut context, &plan).is_err());
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
                .materialize_otap(&mut CodecState::default())
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
            .into_encoded_for_test(TEST_ENCODING, Default::default())
            .unwrap();
        CREATES.with(|count| count.set(0));
        let mut first = CodecState::default();
        let mut second = CodecState::default();
        _ = first.decode(&input).unwrap();
        _ = first.decode(&input).unwrap();
        let bad = input
            .codec()
            .admit(SignalType::Logs, Bytes::from_static(&[0]))
            .unwrap();
        assert!(first.decode(&bad).is_err());
        _ = first.decode(&input).unwrap();
        assert_eq!(CREATES.with(Cell::get), 1);
        _ = second.decode(&input).unwrap();
        assert_eq!(CREATES.with(Cell::get), 2);
    }

    /// Scenario: each OTLP operation is the first use of a consumer's codec state.
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
            let encoded = ResolvedCodec::otlp()
                .unwrap()
                .admit(signal, bytes.into())
                .unwrap();
            let original = OtapPayload::from(encoded.clone());
            let records = original
                .clone()
                .try_into_otap(&mut CodecState::default())
                .unwrap();
            for operation in 0..4 {
                let mut context = CodecState::default();
                for _ in 0..2 {
                    match operation {
                        0 => {
                            let result = original.clone().try_into_otap(&mut context).unwrap();
                            assert_eq!(result.signal_type(), signal);
                        }
                        1 => {
                            let mut payload = OtapPayload::from(records.clone());
                            let output = payload
                                .prepare_encoded(
                                    &mut context,
                                    &EncodingPlan::otlp().expect("selected OTLP encoding plan"),
                                )
                                .unwrap();
                            assert!(!output.as_ref().is_empty());
                        }
                        2 => {
                            let PayloadView::OtlpBytes {
                                signal: viewed_signal,
                                bytes: viewed_bytes,
                            } = original.view(&mut context).unwrap()
                            else {
                                panic!("OTLP codec must supply a borrowed protobuf view");
                            };
                            assert_eq!(viewed_signal, signal);
                            assert_eq!(viewed_bytes.as_ptr(), encoded.bytes().as_ptr());
                        }
                        _ => {
                            let plan = BatchPlan::new(
                                PdataFormat::otlp().expect("selected OTLP format"),
                                BatchProfile::otlp(),
                                true,
                            )
                            .unwrap();
                            let output = plan
                                .batch(signal, vec![original.clone()], &mut context)
                                .unwrap();
                            assert!(!output.batches.is_empty());
                            assert!(output.batches.iter().all(|(batch, _)| batch.format()
                                == PdataFormat::otlp().expect("selected OTLP format")));
                        }
                    }
                    assert_eq!(context.codecs.len(), 1);
                    assert_eq!(
                        context.codecs[0].0,
                        ResolvedCodec::otlp().expect("selected OTLP codec")
                    );
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
            provider: CodecProviderId::new("org.opentelemetry.test.counter"),
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
        let mut context = CodecState::default();
        let plan = EncodingPlan::new(codec, Default::default()).unwrap();
        let output = payload.prepare_encoded(&mut context, &plan).unwrap();
        assert_eq!(
            output.as_ref().as_ptr(),
            before.encoded_bytes().unwrap().as_ptr()
        );
        drop(output);
        assert!(context.codecs.is_empty());
    }

    /// Scenario: an exporter borrows OTLP scratch for compression, interleaves
    /// signals, detaches it for gRPC, and encounters a temporary size limit.
    /// Guarantees: each signal reuses separate scratch, zero-copy detachment
    /// survives errors, and encode options reset for the next request.
    #[test]
    fn prepared_output_reuses_scratch_and_detaches_without_copying() {
        let mut payload = logs_payload();
        payload
            .materialize_otap(&mut CodecState::default())
            .unwrap();
        let mut context = CodecState::default();
        let output = payload
            .prepare_encoded(
                &mut context,
                &EncodingPlan::otlp().expect("selected OTLP encoding plan"),
            )
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
            other.materialize_otap(&mut CodecState::default()).unwrap();
            let output = other
                .prepare_encoded(
                    &mut context,
                    &EncodingPlan::otlp().expect("selected OTLP encoding plan"),
                )
                .unwrap();
            let other_pointer = output.as_ref().as_ptr();
            assert!(!pointers.contains(&other_pointer));
            pointers.push(other_pointer);
            let expected_other = output.copy_into_bytes();
            let output = other
                .prepare_encoded(
                    &mut context,
                    &EncodingPlan::otlp().expect("selected OTLP encoding plan"),
                )
                .unwrap();
            assert_eq!(output.as_ref().as_ptr(), other_pointer);
            assert_eq!(output.as_ref(), expected_other.as_ref());
        }
        let output = payload
            .prepare_encoded(
                &mut context,
                &EncodingPlan::otlp().expect("selected OTLP encoding plan"),
            )
            .unwrap();
        assert_eq!(output.as_ref().as_ptr(), pointer);
        let detached = output.into_bytes();
        assert_eq!(detached.as_ptr(), pointer);
        assert_eq!(detached, expected);
        let options = EncodeOptions {
            otlp_size_limit: std::num::NonZeroUsize::new(1),
        };
        let limited_plan =
            EncodingPlan::new(ResolvedCodec::otlp().expect("selected OTLP codec"), options)
                .unwrap();
        assert!(
            payload
                .prepare_encoded(&mut context, &limited_plan)
                .is_err()
        );
        let output = payload
            .prepare_encoded(
                &mut context,
                &EncodingPlan::otlp().expect("selected OTLP encoding plan"),
            )
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
            let mut context = CodecState::default();
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
            ) -> Result<OtapArrowRecords, crate::encode::Error> {
                OtlpCodec::default().decode(signal, bytes)
            }
            fn encode(
                &mut self,
                records: OtapArrowRecords,
                options: EncodeOptions,
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
            provider: CodecProviderId::new("org.opentelemetry.test.invalid-batcher"),
            count_items: None,
            metadata: &METADATA,
            create: || Box::new(InvalidBatcher),
        };
        let codec = ResolvedCodec(&REGISTRATION);
        let plan = BatchPlan::new(PdataFormat::encoded(codec), BatchProfile::otlp(), true).unwrap();
        let mut context = CodecState::default();
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
