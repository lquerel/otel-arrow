// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the pipeline data that is passed between pipeline components.
//!
//! Internally, data is native OTAP Arrow records or independently encoded bytes
//! identified by a pdata codec. OTLP protobuf uses the same encoded storage and
//! lazy conversion interface as every other byte representation; see
//! [crate::codec] for the extension contract.
//!
//! OTAP stream-relative Arrow IPC dictionary deltas are transport state, not
//! independently encoded pdata. Native OTAP remains the common intermediate
//! representation for codec conversions.
//!
//! This module also contains conversions between the various types using the `From`
//! and `TryFrom` traits. For example:
//! ```
//! # use std::sync::Arc;
//! # use arrow::array::{RecordBatch, UInt16Array};
//! # use arrow::datatypes::{DataType, Field, Schema};
//! # use otel_arrow_dfe_pdata::otap::{OtapArrowRecords, Logs};
//! # use otel_arrow_dfe_pdata::proto::opentelemetry::{
//!     arrow::v1::ArrowPayloadType,
//!     collector::logs::v1::ExportLogsServiceRequest,
//!     common::v1::{AnyValue, InstrumentationScope, KeyValue},
//!     logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
//!     resource::v1::Resource
//! };
//! # use otel_arrow_dfe_pdata::OtapPayload;
//! # use otel_arrow_dfe_pdata::codec::{CodecContext, ResolvedCodec};
//! # use otel_arrow_dfe_config::SignalType;
//! # use prost::Message;
//! # use bytes::Bytes;
//! let otlp_service_req = ExportLogsServiceRequest::new(vec![
//!    ResourceLogs::new(
//!        Resource::default(),
//!        vec![
//!            ScopeLogs::new(
//!                InstrumentationScope::default(),
//!                vec![
//!                    LogRecord::build()
//!                        .time_unix_nano(2u64)
//!                        .severity_number(SeverityNumber::Info)
//!                        .event_name("event")
//!                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
//!                        .finish(),
//!                ],
//!            ),
//!        ],
//!    ),
//!  ]);
//! let mut buf = Vec::new();
//! otlp_service_req.encode(&mut buf).unwrap();
//!
//! // Create a new OtapPayload from OTLP bytes
//! let payload: OtapPayload = ResolvedCodec::OTLP
//!     .admit(SignalType::Logs, Bytes::from(buf)).unwrap().into();
//!
//! // Convert to OTAP records
//! let otap_arrow_records: OtapArrowRecords = payload
//!     .try_into_otap_with(&mut CodecContext::default(), Default::default())
//!     .unwrap();
//! ```
//!
//! Internally, conversions are happening using various utility functions:
//! ```text
//!                                      +-----------------------+
//!                                      |                       |
//!                                      |      OTLP Bytes       |
//!                                      |                       |
//!                                      +---+-------------------+
//!                                          |                 ^
//!                                          |                 |
//!                                          |                 |
//!                                          v                 |
//!    otel_arrow_dfe_otap::encoder::encode_<signal>_otap_batch    otel_arrow_dfe_pdata::otlp::<signal>::<signal_>_from()
//!                                          |                 ^
//!                                          |                 |
//!                                          |                 |
//!                                          v                 |
//!                                      +---------------------+---+
//!                                      |                         |
//!                                      |    OTAP Arrow Records   |
//!                                      |                         |
//!                                      +-------------------------+
//! ```
// ^^ TODO we're currently in the process of reworking conversion between OTLP & OTAP to go
// directly from OTAP -> OTLP bytes. The utility functions we use might change as part of
// this diagram may need to be updated (https://github.com/open-telemetry/otel-arrow/issues/1095)

use crate::TryFromWithOptions;
#[cfg(test)]
use crate::TryIntoWithOptions;
use crate::batching::PdataFormat;
use crate::codec::{
    self, CodecContext, CodecDirection, EncodedOutput, EncodedPdata, PdataEncoding, ResolvedCodec,
};
use crate::encode::{encode_logs_otap_batch, encode_metrics_otap_batch, encode_spans_otap_batch};
use crate::error::Error;
use crate::otap::{OtapArrowRecords, OtapBatchStore};
use crate::otlp::logs::LogsProtoBytesEncoder;
use crate::otlp::metrics::MetricsProtoBytesEncoder;
use crate::otlp::traces::TracesProtoBytesEncoder;
use crate::otlp::{OtlpProtoBytes, ProtoBuffer, ProtoBytesEncoder};
use crate::proto::OtlpProtoMessage;
use crate::views::otlp::bytes::logs::RawLogsData;
use crate::views::otlp::bytes::metrics::RawMetricsData;
use crate::views::otlp::bytes::traces::RawTraceData;
use bytes::{Bytes, BytesMut};
use otel_arrow_dfe_config::{ConversionOptions, SignalFormat, SignalType};
use prost::{EncodeError, Message};
use std::borrow::Cow;
use std::sync::Arc;

/// Concrete storage representation backing an [`OtapPayload`].
///
/// Storage introspection is exported only for tests. Production consumers use
/// the wrapper operations, so extending encoded formats requires no new matches
/// in processors or exporters.
#[derive(Clone, Debug)]
pub enum PayloadData {
    /// Independently encoded bytes, including OTLP, with an immutable codec identity.
    Encoded(EncodedPdata),
    /// Native OTAP records and their cached logical byte size.
    OtapArrowRecords {
        /// Arrow records for the primary signal.
        records: OtapArrowRecords,
        /// Measurement scoped to these exact records; cleared before mutation.
        size: Option<usize>,
    },
}

/// Readable representations supplied by codecs or borrowed from native OTAP.
/// Views borrow input bytes, never mutable codec state.
pub enum PayloadView<'a> {
    /// Borrowed OTLP protobuf bytes for direct views without an owned envelope.
    OtlpBytes {
        /// Signal carried by the protobuf service request.
        signal: SignalType,
        /// Uncompressed service-request bytes.
        bytes: &'a [u8],
    },
    /// Borrowed native OTAP or records decoded from encoded bytes.
    OtapArrowRecords(Cow<'a, OtapArrowRecords>),
}

impl PayloadView<'_> {
    /// Signal exposed by this view.
    #[must_use]
    pub fn signal_type(&self) -> SignalType {
        match self {
            Self::OtlpBytes { signal, .. } => *signal,
            Self::OtapArrowRecords(records) => records.signal_type(),
        }
    }
}

impl PayloadData {
    fn signal_type(&self) -> SignalType {
        match self {
            Self::Encoded(value) => value.signal_type(),
            Self::OtapArrowRecords { records, .. } => records.signal_type(),
        }
    }

    const fn signal_format(&self) -> SignalFormat {
        match self {
            Self::OtapArrowRecords { .. } => SignalFormat::OtapRecords,
            Self::Encoded(_) => SignalFormat::Encoded,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Encoded(value) => value.bytes().is_empty(),
            Self::OtapArrowRecords { records, .. } => records.is_empty(),
        }
    }

    /// Empties this representation in place, returning the old contents and caches.
    fn take_payload(&mut self) -> Self {
        match self {
            Self::Encoded(value) => {
                let empty =
                    EncodedPdata::from_resolved(value.codec(), value.signal_type(), Bytes::new());
                Self::Encoded(std::mem::replace(value, empty))
            }
            Self::OtapArrowRecords { records, size } => Self::OtapArrowRecords {
                records: records.take_payload(),
                size: size.take(),
            },
        }
    }

    fn num_bytes(&self) -> Option<usize> {
        match self {
            Self::Encoded(value) => Some(value.bytes().len()),
            Self::OtapArrowRecords { records, size } => size.or_else(|| records.num_bytes()),
        }
    }

    fn retained_memory_bytes(&self) -> usize {
        match self {
            Self::Encoded(value) => value.bytes().len(),
            Self::OtapArrowRecords { records, .. } => records.retained_memory_bytes(),
        }
    }
}

/// Container for the various representations of the telemetry data.
///
/// `OtapPayload` owns both the concrete [`PayloadData`] and cached expensive
/// measurements. OTLP item counts and OTAP logical byte sizes are cached when
/// first requested. The cache is scoped to the exact logical payload version it
/// was created for:
///
/// - Wrapping native records or OTLP compatibility bytes starts a fresh cache.
///   Admitted encoded envelopes preserve any supplied counts. [`Self::empty`]
///   and [`Self::take_payload`]'s emptied remainder start with fresh caches.
/// - Ordinary clones copy any measurements already computed for the source
///   payload.
/// - [`Self::take_payload`]'s returned value preserves the cache of the
///   payload version it contains.
///
/// Test-only storage introspection exposes a shared reference. Accessing the underlying
/// representation for mutation requires consuming the wrapper through
/// [`Self::into_data`] or [`Self::try_into_otap_with`], and wrapping the mutated
/// representation creates a fresh cache. No manual invalidation is required.
#[derive(Clone, Debug)]
pub struct OtapPayload {
    data: PayloadData,
}

/// A failed native OTAP conversion together with the recoverable input payload.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct OtapPayloadDecodeError(Box<OtapPayloadDecodeErrorInner>);

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
struct OtapPayloadDecodeErrorInner {
    #[source]
    source: crate::encode::Error,
    payload: OtapPayload,
}

impl OtapPayloadDecodeError {
    /// Returns the conversion error.
    #[must_use]
    pub const fn error(&self) -> &crate::encode::Error {
        &self.0.source
    }

    /// Returns the payload retained for Nack or retry.
    #[must_use]
    pub fn payload(&self) -> &OtapPayload {
        &self.0.payload
    }

    /// Splits the conversion error and recoverable payload.
    #[must_use]
    pub fn into_parts(self) -> (crate::encode::Error, OtapPayload) {
        let inner = *self.0;
        (inner.source, inner.payload)
    }

    fn into_error(self) -> crate::encode::Error {
        let inner = *self.0;
        inner.source
    }
}

impl OtapPayload {
    /// Wraps payload data in a fresh `OtapPayload` with an uninitialized
    /// measurement cache.
    fn from_data(data: PayloadData) -> Self {
        Self { data }
    }

    /// Constructs a fresh payload from OTLP protobuf bytes.
    #[must_use]
    pub fn from_otlp(mut payload: OtlpProtoBytes) -> Self {
        Self::from_encoded(EncodedPdata::from_resolved(
            ResolvedCodec::OTLP,
            payload.signal_type(),
            payload.replace_bytes(Bytes::new()),
        ))
    }

    /// Constructs a fresh payload from OTAP Arrow records.
    #[must_use]
    pub fn from_otap(payload: OtapArrowRecords) -> Self {
        Self::from_data(PayloadData::OtapArrowRecords {
            records: payload,
            size: None,
        })
    }

    /// Wraps any admitted encoded batch without allocating, copying, or decoding it.
    #[must_use]
    pub fn from_encoded(encoded: EncodedPdata) -> Self {
        Self::from_data(PayloadData::Encoded(encoded))
    }

    /// Encoding of byte-oriented payloads, or None for native OTAP.
    #[must_use]
    pub fn encoding(&self) -> Option<&PdataEncoding> {
        match &self.data {
            PayloadData::Encoded(encoded) => Some(encoded.encoding()),
            PayloadData::OtapArrowRecords { .. } => None,
        }
    }

    /// Resolved logical representation, independent of its storage layout.
    #[must_use]
    pub fn format(&self) -> PdataFormat {
        match &self.data {
            PayloadData::OtapArrowRecords { .. } => PdataFormat::OTAP,
            PayloadData::Encoded(encoded) => PdataFormat::encoded(encoded.codec()),
        }
    }

    /// Existing encoded bytes, without decoding or converting native records.
    #[must_use]
    pub fn encoded_bytes(&self) -> Option<&Bytes> {
        match &self.data {
            PayloadData::Encoded(encoded) => Some(encoded.bytes()),
            PayloadData::OtapArrowRecords { .. } => None,
        }
    }

    /// Takes existing encoded bytes, returning the original payload if native.
    pub fn into_encoded_bytes(self) -> Result<Bytes, Self> {
        match self.data {
            PayloadData::Encoded(encoded) => Ok(encoded.into_bytes()),
            _ => Err(self),
        }
    }

    /// Borrows already materialized native records without conversion.
    #[must_use]
    pub fn otap_ref(&self) -> Option<&OtapArrowRecords> {
        match &self.data {
            PayloadData::OtapArrowRecords { records, .. } => Some(records),
            _ => None,
        }
    }

    /// Known item count. An absent count is distinct from an empty batch.
    #[must_use]
    pub fn known_item_count(&self) -> Option<usize> {
        match &self.data {
            PayloadData::Encoded(encoded) => encoded.item_count(),
            PayloadData::OtapArrowRecords { records, .. } => Some(records.num_items()),
        }
    }

    /// Returns a cached byte measurement or measures the current data read-only.
    #[must_use]
    pub fn measured_bytes(&self) -> Option<usize> {
        self.data.num_bytes()
    }

    pub(crate) fn set_item_count(&mut self, count: usize) {
        if let PayloadData::Encoded(encoded) = &mut self.data {
            encoded.set_item_count(Some(count));
        }
    }

    /// Extracts or decodes native records using reusable consumer-local state.
    ///
    /// Native records move directly. Encoded input is retained with shared bytes
    /// until decoding succeeds so callers can recover the original payload on error.
    pub fn try_into_otap_with(
        self,
        context: &mut CodecContext,
        options: ConversionOptions,
    ) -> Result<OtapArrowRecords, OtapPayloadDecodeError> {
        match self.data {
            PayloadData::OtapArrowRecords { records, .. } => Ok(records),
            PayloadData::Encoded(encoded) => {
                let payload = Self::from_encoded(encoded.clone());
                context.decode(encoded, options).map_err(|source| {
                    OtapPayloadDecodeError(Box::new(OtapPayloadDecodeErrorInner {
                        source,
                        payload,
                    }))
                })
            }
        }
    }

    /// Materializes once on success, retaining the original payload on failure.
    pub(crate) fn materialize_otap_with(
        &mut self,
        context: &mut CodecContext,
        options: ConversionOptions,
    ) -> Result<(), crate::encode::Error> {
        if !matches!(self.data, PayloadData::OtapArrowRecords { .. }) {
            let records = self
                .clone()
                .try_into_otap_with(context, options)
                .map_err(OtapPayloadDecodeError::into_error)?;
            *self = records.into();
        }
        Ok(())
    }

    /// Converts to a target encoding through OTAP, or returns compatible bytes
    /// directly. Admission has already validated the source codec.
    pub fn into_encoded(
        self,
        encoding: PdataEncoding,
        options: ConversionOptions,
    ) -> Result<EncodedPdata, Error> {
        let codec = codec::resolve(&encoding, self.signal_type(), CodecDirection::Decode)?;
        self.into_encoded_with(&mut CodecContext::default(), codec, options)
    }

    /// Converts to an admitted encoded representation using reusable codec state.
    pub fn into_encoded_with(
        mut self,
        context: &mut CodecContext,
        codec: ResolvedCodec,
        options: ConversionOptions,
    ) -> Result<EncodedPdata, Error> {
        let signal = self.signal_type();
        codec.require(signal, CodecDirection::Decode)?;
        if self.format() == PdataFormat::encoded(codec) {
            return Ok(match self.data {
                PayloadData::Encoded(encoded) => encoded,
                PayloadData::OtapArrowRecords { .. } => unreachable!("native OTAP has no encoding"),
            });
        }
        let bytes = self.prepare_encoded(context, codec, options)?.into_bytes();
        let mut encoded = EncodedPdata::from_resolved(codec, signal, bytes);
        if let Some(count) = self.known_item_count() {
            encoded = encoded.with_item_count(count);
        }
        Ok(encoded)
    }

    /// Prepares output without changing an encoded source's representation.
    /// Native encoding reuses scratch buffers; unchanged bytes remain shared.
    pub fn prepare_encoded<'a>(
        &mut self,
        context: &'a mut CodecContext,
        codec: ResolvedCodec,
        options: ConversionOptions,
    ) -> Result<EncodedOutput<'a>, Error> {
        if self.format() == PdataFormat::encoded(codec) {
            return Ok(EncodedOutput::bytes(
                self.encoded_bytes()
                    .expect("encoded representation")
                    .clone(),
            ));
        }
        codec.require(self.signal_type(), CodecDirection::Encode)?;
        if let PayloadData::OtapArrowRecords { records, size } = &mut self.data {
            *size = None;
            return context.encode_records(records, codec, options);
        }
        let mut records = self
            .clone()
            .try_into_otap_with(context, options.clone())
            .map_err(|error| {
                codec::codec_error(
                    &codec.metadata().encoding,
                    format!("source decode failed: {error}"),
                )
            })?;
        let count = records.num_items();
        let output = context.encode_records(&mut records, codec, options)?;
        self.set_item_count(count);
        Ok(output)
    }

    /// Atomically replaces the stored representation after successful encoding.
    pub fn convert_encoding_with(
        &mut self,
        context: &mut CodecContext,
        codec: ResolvedCodec,
        options: ConversionOptions,
    ) -> Result<(), Error> {
        if self.format() != PdataFormat::encoded(codec) {
            *self = Self::from_encoded(self.clone().into_encoded_with(context, codec, options)?);
        }
        Ok(())
    }

    /// Changes the byte representation only after a successful conversion.
    ///
    /// The original payload is retained on error so callers can Nack or retry it.
    pub fn convert_encoding(
        &mut self,
        encoding: PdataEncoding,
        options: ConversionOptions,
    ) -> Result<(), Error> {
        if self.encoding() != Some(&encoding) {
            *self = Self::from_encoded(self.clone().into_encoded(encoding, options)?);
        }
        Ok(())
    }

    /// Borrows the concrete payload data for pattern matching.
    #[must_use]
    #[cfg(any(test, feature = "test-internals"))]
    pub fn data(&self) -> &PayloadData {
        &self.data
    }

    /// Borrows an existing representation or decodes an extension for record views.
    pub fn view(
        &self,
        options: ConversionOptions,
    ) -> Result<PayloadView<'_>, crate::encode::Error> {
        self.view_with(&mut CodecContext::default(), options)
    }

    /// Borrows native views or decodes once using consumer-local codec state.
    pub fn view_with(
        &self,
        context: &mut CodecContext,
        options: ConversionOptions,
    ) -> Result<PayloadView<'_>, crate::encode::Error> {
        match &self.data {
            PayloadData::OtapArrowRecords { records, .. } => {
                Ok(PayloadView::OtapArrowRecords(Cow::Borrowed(records)))
            }
            PayloadData::Encoded(encoded) => context.view(encoded, options),
        }
    }

    /// Consumes this payload, returning its concrete payload data.
    ///
    /// The cached measurements are dropped; construct a new `OtapPayload`
    /// (for example via `From`) to wrap the representation again with a
    /// fresh cache.
    #[must_use]
    #[cfg(any(test, feature = "test-internals"))]
    pub fn into_data(self) -> PayloadData {
        self.into_uncached_data()
    }

    #[cfg(not(any(test, feature = "test-internals")))]
    pub(crate) fn into_data(self) -> PayloadData {
        self.into_uncached_data()
    }

    fn into_uncached_data(mut self) -> PayloadData {
        match &mut self.data {
            PayloadData::Encoded(encoded) => encoded.set_item_count(None),
            PayloadData::OtapArrowRecords { size, .. } => *size = None,
        }
        self.data
    }

    /// Returns the type of signal represented by this `OtapPdata` instance.
    #[must_use]
    pub fn signal_type(&self) -> SignalType {
        self.data.signal_type()
    }

    /// Returns the signal format.
    #[must_use]
    pub const fn signal_format(&self) -> SignalFormat {
        self.data.signal_format()
    }

    /// True if the payload is empty. By definition, we can skip sending an
    /// empty request.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Removes the payload from this request, leaving an empty request.
    ///
    /// The returned `OtapPayload` retains this payload's cached measurements.
    /// `self` is left holding an emptied representation with a fresh cache, so
    /// it cannot reuse stale measurements.
    #[must_use]
    pub fn take_payload(&mut self) -> Self {
        Self {
            data: self.data.take_payload(),
        }
    }

    /// Returns the primary signal count, scanning encoded bytes only if the
    /// codec provides a stateless counter. Unknown counts report zero for metrics.
    #[must_use]
    pub fn num_items(&mut self) -> usize {
        match &mut self.data {
            PayloadData::Encoded(encoded) => encoded.num_items(),
            PayloadData::OtapArrowRecords { records, .. } => records.num_items(),
        }
    }

    /// Returns encoded byte length or a cached logical Arrow byte estimate.
    #[must_use]
    pub fn num_bytes(&mut self) -> Option<usize> {
        match &mut self.data {
            PayloadData::Encoded(encoded) => Some(encoded.bytes().len()),
            PayloadData::OtapArrowRecords { records, size } => {
                if size.is_none() {
                    *size = records.num_bytes();
                }
                *size
            }
        }
    }

    /// Returns the best available retained-memory byte estimate.
    ///
    /// For OTLP bytes this is the encoded byte length because the payload uses
    /// `bytes::Bytes`, which does not expose backing allocation capacity. A
    /// `Bytes` slice may pin a larger shared allocation, but that larger
    /// capacity is not measurable here.
    #[must_use]
    pub fn retained_memory_bytes(&self) -> usize {
        self.data.retained_memory_bytes()
    }

    /// Return an empty payload of a certain type.
    #[must_use]
    pub const fn empty(signal: SignalType) -> Self {
        Self {
            data: PayloadData::Encoded(EncodedPdata::from_resolved(
                ResolvedCodec::OTLP,
                signal,
                Bytes::new(),
            )),
        }
    }

    /// Test-only introspection: true if the OTLP item-count cache has been
    /// computed.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn test_has_cached_item_count(&self) -> bool {
        matches!(&self.data, PayloadData::Encoded(encoded) if encoded.item_count().is_some())
    }

    /// Test-only introspection: true if the OTAP size cache has been computed.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn test_has_cached_size(&self) -> bool {
        matches!(
            &self.data,
            PayloadData::OtapArrowRecords { size: Some(_), .. }
        )
    }
}

/* -------- Trait implementations -------- */

/// Helper methods that internal representations of OTAP PData should implement
pub trait OtapPayloadHelpers: Into<OtapPayload> {
    /// Returns the type of signal represented by this `OtapPdata` instance.
    fn signal_type(&self) -> SignalType;

    /// Number of items.
    fn num_items(&self) -> usize;

    /// Logical byte size of the current representation, if measurable.
    fn num_bytes(&self) -> Option<usize>;

    /// Best available retained-memory byte estimate.
    fn retained_memory_bytes(&self) -> usize;

    /// Return true if there is no data.
    fn is_empty(&self) -> bool;

    /// Takes the payload, leaving an empty payload behind.
    fn take_payload(&mut self) -> Self;
}

impl OtapPayloadHelpers for OtapArrowRecords {
    fn signal_type(&self) -> SignalType {
        match self {
            Self::Logs(_) => SignalType::Logs,
            Self::Metrics(_) => SignalType::Metrics,
            Self::Traces(_) => SignalType::Traces,
        }
    }

    fn num_bytes(&self) -> Option<usize> {
        self.logical_arrow_bytes().ok()
    }

    fn retained_memory_bytes(&self) -> usize {
        self.retained_memory_bytes()
    }

    fn take_payload(&mut self) -> Self {
        match self {
            Self::Logs(value) => Self::Logs(std::mem::take(value)),
            Self::Metrics(value) => Self::Metrics(std::mem::take(value)),
            Self::Traces(value) => Self::Traces(std::mem::take(value)),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Logs(_) => self
                .get(crate::proto::opentelemetry::arrow::v1::ArrowPayloadType::Logs)
                .is_none_or(|batch| batch.num_rows() == 0),
            Self::Traces(_) => self
                .get(crate::proto::opentelemetry::arrow::v1::ArrowPayloadType::Spans)
                .is_none_or(|batch| batch.num_rows() == 0),
            Self::Metrics(_) => self
                .get(crate::proto::opentelemetry::arrow::v1::ArrowPayloadType::UnivariateMetrics)
                .is_none_or(|batch| batch.num_rows() == 0),
        }
    }

    fn num_items(&self) -> usize {
        // Arrow batches store row counts, so this does not scan individual items.
        match self {
            Self::Logs(records) => records.num_items(),
            Self::Traces(records) => records.num_items(),
            Self::Metrics(records) => records.num_items(),
        }
    }
}

impl OtapPayloadHelpers for OtlpProtoBytes {
    fn signal_type(&self) -> SignalType {
        match self {
            Self::ExportLogsRequest(_) => SignalType::Logs,
            Self::ExportMetricsRequest(_) => SignalType::Metrics,
            Self::ExportTracesRequest(_) => SignalType::Traces,
        }
    }

    fn num_bytes(&self) -> Option<usize> {
        Some(self.num_bytes())
    }

    fn retained_memory_bytes(&self) -> usize {
        self.as_bytes().len()
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::ExportLogsRequest(bytes) => bytes.is_empty(),
            Self::ExportMetricsRequest(bytes) => bytes.is_empty(),
            Self::ExportTracesRequest(bytes) => bytes.is_empty(),
        }
    }

    fn take_payload(&mut self) -> Self {
        match self {
            Self::ExportLogsRequest(value) => Self::ExportLogsRequest(std::mem::take(value)),
            Self::ExportMetricsRequest(value) => Self::ExportMetricsRequest(std::mem::take(value)),
            Self::ExportTracesRequest(value) => Self::ExportTracesRequest(std::mem::take(value)),
        }
    }

    fn num_items(&self) -> usize {
        count_otlp_items(self.signal_type(), self.as_bytes())
    }
}

/// Stateless OTLP item scan shared by the codec and low-level compatibility helper.
pub(crate) fn count_otlp_items(signal: SignalType, bytes: &[u8]) -> usize {
    // Counting requires traversing the encoded protobuf record hierarchy.
    match signal {
        SignalType::Logs => {
            let logs_data_view = RawLogsData::new(bytes);
            use otel_arrow_dfe_pdata_views::views::logs::{
                LogsDataView, ResourceLogsView, ScopeLogsView,
            };
            logs_data_view
                .resources()
                .map(|rl| {
                    rl.scopes()
                        .map(|sl| sl.log_records().count())
                        .sum::<usize>()
                })
                .sum()
        }
        SignalType::Traces => {
            let traces_data_view = RawTraceData::new(bytes);
            use otel_arrow_dfe_pdata_views::views::trace::{
                ResourceSpansView, ScopeSpansView, TracesView,
            };
            traces_data_view
                .resources()
                .map(|rs| rs.scopes().map(|ss| ss.spans().count()).sum::<usize>())
                .sum()
        }
        SignalType::Metrics => {
            let metrics_data_view = RawMetricsData::new(bytes);
            use otel_arrow_dfe_pdata_views::views::metrics::{
                DataView, ExponentialHistogramView, GaugeView, HistogramView, MetricView,
                MetricsView, ResourceMetricsView, ScopeMetricsView, SumView, SummaryView,
            };
            metrics_data_view
                .resources()
                .map(|rm| {
                    rm.scopes()
                        .map(|sm| {
                            sm.metrics()
                                .map(|metric| {
                                    metric
                                        .data()
                                        .map(|data| {
                                            let mut count = 0;
                                            if let Some(gauge) = data.as_gauge() {
                                                count += gauge.data_points().count();
                                            } else if let Some(sum) = data.as_sum() {
                                                count += sum.data_points().count();
                                            } else if let Some(histogram) = data.as_histogram() {
                                                count += histogram.data_points().count();
                                            } else if let Some(exp_histogram) =
                                                data.as_exponential_histogram()
                                            {
                                                count += exp_histogram.data_points().count();
                                            } else if let Some(summary) = data.as_summary() {
                                                count += summary.data_points().count();
                                            }
                                            count
                                        })
                                        .unwrap_or(0)
                                })
                                .sum::<usize>()
                        })
                        .sum::<usize>()
                })
                .sum()
        }
    }
}

/* -------- Conversion implementations -------- */

impl From<OtapArrowRecords> for OtapPayload {
    fn from(value: OtapArrowRecords) -> Self {
        Self::from_otap(value)
    }
}

impl From<OtlpProtoBytes> for OtapPayload {
    fn from(value: OtlpProtoBytes) -> Self {
        Self::from_otlp(value)
    }
}

impl From<EncodedPdata> for OtapPayload {
    fn from(value: EncodedPdata) -> Self {
        Self::from_encoded(value)
    }
}

impl From<Arc<EncodedPdata>> for OtapPayload {
    fn from(value: Arc<EncodedPdata>) -> Self {
        Self::from_encoded(Arc::unwrap_or_clone(value))
    }
}

impl From<PayloadData> for OtapPayload {
    fn from(value: PayloadData) -> Self {
        Self::from_data(value)
    }
}

impl TryFromWithOptions<OtapPayload> for OtlpProtoBytes {
    type Error = Error;

    fn try_from_with_options(
        value: OtapPayload,
        opts: ConversionOptions,
    ) -> Result<Self, Self::Error> {
        let signal = value.signal_type();
        let encoded = value.into_encoded(PdataEncoding::OTLP, opts)?;
        Ok(OtlpProtoBytes::new_from_bytes(signal, encoded.into_bytes()))
    }
}

impl TryFromWithOptions<OtapArrowRecords> for OtlpProtoBytes {
    type Error = Error;

    fn try_from_with_options(
        mut value: OtapArrowRecords,
        opts: ConversionOptions,
    ) -> Result<Self, Self::Error> {
        match value {
            OtapArrowRecords::Logs(_) => {
                // TODO it'd be nice to expose a better API where we can make it easier to pass the encoder
                // and the buffer, a these structures can be used between requests
                let mut logs_encoder = LogsProtoBytesEncoder::new();
                let mut buffer = ProtoBuffer::new(opts);

                logs_encoder.encode(&mut value, &mut buffer)?;
                Ok(Self::ExportLogsRequest(buffer.into_bytes()))
            }
            OtapArrowRecords::Metrics(_) => {
                let mut metrics_encoder = MetricsProtoBytesEncoder::new();
                let mut buffer = ProtoBuffer::new(opts);
                metrics_encoder.encode(&mut value, &mut buffer)?;

                Ok(Self::ExportMetricsRequest(buffer.into_bytes()))
            }
            OtapArrowRecords::Traces(_) => {
                let mut traces_encoder = TracesProtoBytesEncoder::new();
                let mut buffer = ProtoBuffer::new(opts);
                traces_encoder.encode(&mut value, &mut buffer)?;
                Ok(Self::ExportTracesRequest(buffer.into_bytes()))
            }
        }
    }
}

impl TryFromWithOptions<OtlpProtoBytes> for OtapArrowRecords {
    type Error = crate::encode::Error;

    fn try_from_with_options(
        value: OtlpProtoBytes,
        _opts: ConversionOptions,
    ) -> Result<Self, Self::Error> {
        match value {
            OtlpProtoBytes::ExportLogsRequest(bytes) => {
                let logs_data_view = RawLogsData::new(bytes.as_ref());
                let otap_batch = encode_logs_otap_batch(&logs_data_view)?;

                Ok(otap_batch)
            }
            OtlpProtoBytes::ExportTracesRequest(bytes) => {
                let trace_data_view = RawTraceData::new(bytes.as_ref());
                let otap_batch = encode_spans_otap_batch(&trace_data_view)?;

                Ok(otap_batch)
            }
            OtlpProtoBytes::ExportMetricsRequest(bytes) => {
                let metrics_data_view = RawMetricsData::new(bytes.as_ref());
                let otap_batch = encode_metrics_otap_batch(&metrics_data_view)?;

                Ok(otap_batch)
            }
        }
    }
}

impl TryFrom<OtlpProtoMessage> for OtapPayload {
    type Error = EncodeError;

    fn try_from(value: OtlpProtoMessage) -> Result<Self, Self::Error> {
        let mut bytes = BytesMut::new();
        Ok(match value {
            OtlpProtoMessage::Logs(logs_data) => {
                logs_data.encode(&mut bytes)?;
                OtlpProtoBytes::ExportLogsRequest(bytes.freeze()).into()
            }
            OtlpProtoMessage::Metrics(metrics_data) => {
                metrics_data.encode(&mut bytes)?;
                OtlpProtoBytes::ExportMetricsRequest(bytes.freeze()).into()
            }
            OtlpProtoMessage::Traces(trace_data) => {
                trace_data.encode(&mut bytes)?;
                OtlpProtoBytes::ExportTracesRequest(bytes.freeze()).into()
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::testing::fixtures::logs_with_full_resource_and_scope;
    use crate::{
        otap::OtapArrowRecords,
        proto::opentelemetry::{
            collector::{
                logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
                trace::v1::ExportTraceServiceRequest,
            },
            common::v1::{AnyValue, InstrumentationScope, KeyValue},
            logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
            metrics::v1::{
                AggregationTemporality, Exemplar, ExponentialHistogram,
                ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint, Metric,
                NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
                exemplar, exponential_histogram_data_point::Buckets, metric::Data,
                number_data_point::Value, summary_data_point::ValueAtQuantile,
            },
            resource::v1::Resource,
            trace::v1::{
                ResourceSpans, ScopeSpans, Span, SpanFlags, Status,
                span::{Event, Link},
                status::StatusCode,
            },
        },
    };
    use bytes::Bytes;
    use pretty_assertions::assert_eq;
    use prost::Message;

    fn into_otap(payload: OtapPayload) -> OtapArrowRecords {
        payload
            .try_into_otap_with(&mut CodecContext::default(), Default::default())
            .unwrap()
    }

    #[test]
    fn test_conversion_logs() {
        let mut otlp_bytes = vec![];
        let otlp_service_req = logs_with_full_resource_and_scope();
        otlp_service_req.encode(&mut otlp_bytes).unwrap();

        let pdata: OtapPayload = OtlpProtoBytes::ExportLogsRequest(otlp_bytes.into()).into();

        // test can go OtlpProtoBytes -> OtapBatch & back
        let otap_batch = into_otap(pdata);
        assert!(matches!(otap_batch, OtapArrowRecords::Logs(_)));
        let pdata: OtapPayload = otap_batch.into();

        let otlp_bytes: OtlpProtoBytes = pdata.try_into_with_default().unwrap();
        assert!(matches!(otlp_bytes, OtlpProtoBytes::ExportLogsRequest(_)));
        let pdata: OtapPayload = otlp_bytes.into();

        let otlp_bytes: OtlpProtoBytes = pdata.try_into_with_default().unwrap();
        assert!(matches!(otlp_bytes, OtlpProtoBytes::ExportLogsRequest(_)));
        let pdata: OtapPayload = otlp_bytes.into();

        let otap_batch = into_otap(pdata);
        assert!(matches!(otap_batch, OtapArrowRecords::Logs(_)));
    }

    // TODO add additional tests for converting between metrics once we have the ability to convert
    //  between OTLP bytes -> OTAP for this signal types
    // https://github.com/open-telemetry/otel-arrow/issues/768

    fn roundtrip_otlp_otap_logs(otlp_service_req: ExportLogsServiceRequest) {
        let mut otlp_bytes = vec![];
        otlp_service_req.encode(&mut otlp_bytes).unwrap();
        let pdata: OtapPayload = OtlpProtoBytes::ExportLogsRequest(otlp_bytes.into()).into();

        // test can go OtlpProtoBytes (written by prost) -> OtapBatch & back (using prost)
        let otap_batch = into_otap(pdata);
        assert!(matches!(otap_batch, OtapArrowRecords::Logs(_)));
        let pdata: OtapPayload = otap_batch.clone().into();

        let otlp_bytes: OtlpProtoBytes = pdata.try_into_with_default().unwrap();
        let bytes = match &otlp_bytes {
            OtlpProtoBytes::ExportLogsRequest(bytes) => bytes.clone(),
            _ => panic!("unexpected otlp bytes pdata variant"),
        };

        let result = ExportLogsServiceRequest::decode(bytes.as_ref()).unwrap();
        assert_eq!(otlp_service_req, result);

        // check that we can also re-decode the OTLP proto bytes that we encode directly
        // from OTAP, that we get the same result
        let pdata: OtapPayload = otlp_bytes.into();
        let otap_batch2 = into_otap(pdata);
        assert_eq!(otap_batch, otap_batch2);
    }

    fn roundtrip_otlp_otap_traces(otlp_service_req: ExportTraceServiceRequest) {
        let mut otlp_bytes = vec![];
        otlp_service_req.encode(&mut otlp_bytes).unwrap();
        let pdata: OtapPayload = OtlpProtoBytes::ExportTracesRequest(otlp_bytes.into()).into();

        // test can go OtlpBytes (written by prost) -> OtapBatch & back (using prost)
        let otap_batch = into_otap(pdata);
        assert!(matches!(otap_batch, OtapArrowRecords::Traces(_)));
        let pdata: OtapPayload = otap_batch.clone().into();

        let otlp_bytes: OtlpProtoBytes = pdata.try_into_with_default().unwrap();
        let bytes = match &otlp_bytes {
            OtlpProtoBytes::ExportTracesRequest(bytes) => bytes.clone(),
            _ => panic!("unexpected otlp bytes pdata variant"),
        };

        let result = ExportTraceServiceRequest::decode(bytes.as_ref()).unwrap();
        assert_eq!(otlp_service_req, result);

        // check that we can also re-decode the OTLP proto bytes that we encode directly
        // from OTAP, that we get the same result
        let pdata: OtapPayload = otlp_bytes.into();
        let otap_batch2 = into_otap(pdata);
        assert_eq!(otap_batch, otap_batch2);
    }

    fn roundtrip_otlp_otap_metrics(otlp_service_request: ExportMetricsServiceRequest) {
        let mut otlp_bytes = vec![];
        otlp_service_request.encode(&mut otlp_bytes).unwrap();
        let pdata: OtapPayload = OtlpProtoBytes::ExportMetricsRequest(otlp_bytes.into()).into();

        // test can go OtlpBytes (written by prost) to OTAP & back (using prost)
        let otap_batch = into_otap(pdata);
        assert!(matches!(otap_batch, OtapArrowRecords::Metrics(_)));
        let pdata: OtapPayload = otap_batch.clone().into();

        let otlp_bytes: OtlpProtoBytes = pdata.try_into_with_default().unwrap();
        let bytes = match &otlp_bytes {
            OtlpProtoBytes::ExportMetricsRequest(bytes) => bytes.clone(),
            _ => panic!("unexpected otlp bytes pdata variant"),
        };

        let result = ExportMetricsServiceRequest::decode(bytes.as_ref()).unwrap();
        assert_eq!(otlp_service_request, result);

        // check that we can also re-decode the OTLP proto bytes that we encode directly
        // from OTAP, that we get the same result
        let pdata: OtapPayload = otlp_bytes.into();
        let otap_batch2 = into_otap(pdata);
        assert_eq!(otap_batch, otap_batch2);
    }

    #[test]
    fn test_otlp_otap_logs_roundtrip() {
        // test to ensure the correct attributes are assigned to the correct log message after
        // roundtrip encoding/decoding

        let otlp_service_req = ExportLogsServiceRequest::new(vec![
            ResourceLogs::new(
                Resource {
                    attributes: vec![KeyValue::new("res_key", AnyValue::new_string("val1"))],
                    ..Default::default()
                },
                vec![ScopeLogs::new(
                    InstrumentationScope {
                        attributes: vec![KeyValue::new("scope_key", AnyValue::new_string("val1"))],
                        ..Default::default()
                    },
                    vec![
                        LogRecord::build()
                            .time_unix_nano(1u64)
                            .severity_number(SeverityNumber::Info)
                            .event_name("event1")
                            .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                            .finish(),
                        LogRecord::build()
                            .time_unix_nano(2u64)
                            .severity_number(SeverityNumber::Info)
                            .event_name("event1")
                            .attributes(vec![KeyValue::new("key", AnyValue::new_string("val2"))])
                            .finish(),
                        LogRecord::build()
                            .time_unix_nano(3u64)
                            .severity_number(SeverityNumber::Info)
                            .event_name("event2")
                            .attributes(vec![KeyValue::new("key", AnyValue::new_string("val3"))])
                            .finish(),
                    ],
                )],
            ),
            ResourceLogs::new(
                Resource {
                    attributes: vec![KeyValue::new("res_key", AnyValue::new_string("val2"))],
                    ..Default::default()
                },
                vec![
                    ScopeLogs::new(
                        InstrumentationScope {
                            name: "Scope2".into(),
                            attributes: vec![KeyValue::new(
                                "scope_key",
                                AnyValue::new_string("val2"),
                            )],
                            ..Default::default()
                        },
                        vec![
                            LogRecord::build()
                                .time_unix_nano(4u64)
                                .severity_number(SeverityNumber::Info)
                                .event_name("event3")
                                .attributes(vec![KeyValue::new(
                                    "key",
                                    AnyValue::new_string("val4"),
                                )])
                                .finish(),
                            LogRecord::build()
                                .time_unix_nano(5u64)
                                .severity_number(SeverityNumber::Info)
                                .event_name("event1")
                                .attributes(vec![KeyValue::new(
                                    "key",
                                    AnyValue::new_string("val5"),
                                )])
                                .finish(),
                        ],
                    ),
                    ScopeLogs::new(
                        InstrumentationScope {
                            attributes: vec![KeyValue::new(
                                "scope_key",
                                AnyValue::new_string("val3"),
                            )],
                            ..Default::default()
                        },
                        vec![
                            LogRecord::build()
                                .time_unix_nano(6u64)
                                .severity_number(SeverityNumber::Info)
                                .event_name("event1")
                                .attributes(vec![KeyValue::new(
                                    "key",
                                    AnyValue::new_string("val6"),
                                )])
                                .finish(),
                            LogRecord::build()
                                .time_unix_nano(7u64)
                                .severity_number(SeverityNumber::Info)
                                .event_name("")
                                .attributes(vec![KeyValue::new(
                                    "key",
                                    AnyValue::new_string("val7"),
                                )])
                                .finish(),
                        ],
                    ),
                ],
            ),
        ]);

        roundtrip_otlp_otap_logs(otlp_service_req);
    }

    #[test]
    fn test_otlp_otap_logs_repeated_attributes() {
        // check to ensure attributes that are repeated are correctly encoded and decoding when
        // doing round-trip between OTLP and OTAP. This test is needed because OTAP attributes'
        // parent IDs can be in multiple formats: plain encoded, and quasi-delta encoded (where
        // delta encoding is used for sequential runs of some key-value pairs).

        let otlp_service_req = ExportLogsServiceRequest::new(vec![
            ResourceLogs::new(
                Resource {
                    attributes: vec![KeyValue::new("res_key", AnyValue::new_string("val"))],
                    ..Default::default()
                },
                vec![ScopeLogs::new(
                    InstrumentationScope::build()
                        .name("scope1")
                        .attributes(vec![KeyValue::new(
                            "scope_key",
                            AnyValue::new_string("val"),
                        )])
                        .finish(),
                    vec![
                        // Add some logs with repeated attributes
                        LogRecord::build()
                            .time_unix_nano(1u64)
                            .severity_number(SeverityNumber::Info)
                            .event_name("")
                            .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                            .finish(),
                        LogRecord::build()
                            .time_unix_nano(2u64)
                            .severity_number(SeverityNumber::Info)
                            .event_name("")
                            .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                            .finish(),
                        LogRecord::build()
                            .time_unix_nano(3u64)
                            .severity_number(SeverityNumber::Info)
                            .event_name("")
                            .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                            .finish(),
                    ],
                )],
            ),
            // also add some scopes and resources where the attributes repeat ...
            ResourceLogs::new(
                Resource {
                    attributes: vec![KeyValue::new("res_key", AnyValue::new_string("val"))],
                    ..Default::default()
                },
                vec![ScopeLogs::new(
                    InstrumentationScope::build()
                        .name("scope2")
                        .attributes(vec![KeyValue::new(
                            "scope_key",
                            AnyValue::new_string("val"),
                        )])
                        .finish(),
                    vec![
                        LogRecord::build()
                            .time_unix_nano(4u64)
                            .severity_number(SeverityNumber::Info)
                            .event_name("")
                            .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                            .finish(),
                    ],
                )],
            ),
            ResourceLogs::new(
                Resource {
                    attributes: vec![KeyValue::new("res_key", AnyValue::new_string("val"))],
                    ..Default::default()
                },
                vec![ScopeLogs::new(
                    InstrumentationScope::build()
                        .name("scope2")
                        .attributes(vec![KeyValue::new(
                            "scope_key",
                            AnyValue::new_string("val"),
                        )])
                        .finish(),
                    vec![
                        LogRecord::build()
                            .time_unix_nano(7u64)
                            .severity_number(SeverityNumber::Info)
                            .event_name("")
                            .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                            .finish(),
                    ],
                )],
            ),
        ]);

        roundtrip_otlp_otap_logs(otlp_service_req);
    }

    #[test]
    fn test_otlp_otap_traces_roundtrip() {
        let otlp_service_req = ExportTraceServiceRequest::new(vec![
            ResourceSpans::new(
                Resource::build()
                    .attributes(vec![KeyValue::new("res_key", AnyValue::new_string("val1"))])
                    .finish(),
                vec![ScopeSpans::new(
                    InstrumentationScope::build()
                        .attributes(vec![KeyValue::new(
                            "scope_key",
                            AnyValue::new_string("val1"),
                        )])
                        .finish(),
                    vec![
                        Span::build()
                            .trace_id(u128::to_be_bytes(1).to_vec())
                            .span_id(u64::to_be_bytes(1).to_vec())
                            .name("albert")
                            .start_time_unix_nano(1u64)
                            .end_time_unix_nano(4u64)
                            .status(Status::new(StatusCode::Ok, "status1"))
                            .attributes(vec![KeyValue::new("key", AnyValue::new_string("val1"))])
                            .links(vec![
                                Link::build()
                                    .trace_id(u128::to_be_bytes(10))
                                    .span_id(u64::to_be_bytes(10))
                                    .finish(),
                                // this Link's trace_id repeats with the next one. doing this to ensure
                                // their parent IDs don't get interpreted as delta encoded
                                Link::build()
                                    .trace_id(u128::to_be_bytes(11))
                                    .span_id(u64::to_be_bytes(11))
                                    .attributes(vec![
                                        KeyValue::new("link_key", AnyValue::new_string("val0")),
                                        // repeating the attr here with the next one for same reason
                                        // as repeating link with trace ID
                                        KeyValue::new("link_key_r", AnyValue::new_string("val1")),
                                    ])
                                    .finish(),
                            ])
                            .events(vec![
                                Event::build().name("event0").time_unix_nano(0u64).finish(),
                                // this event has the repeating name with the next one. doing this to
                                // ensure their parent IDs don't get interpreted as delta encoded
                                Event::build()
                                    .name("event1")
                                    .time_unix_nano(1u64)
                                    .attributes(vec![
                                        KeyValue::new("evt_key", AnyValue::new_string("val0")),
                                        // repeating the attr here with the next one for same reason
                                        // as repeating link with trace ID
                                        KeyValue::new("evt_key_r", AnyValue::new_string("val1")),
                                    ])
                                    .finish(),
                            ])
                            .finish(),
                        Span::build()
                            .trace_id(u128::to_be_bytes(2))
                            .span_id(u64::to_be_bytes(2))
                            .name("terry")
                            .start_time_unix_nano(2u64)
                            .flags(SpanFlags::TraceFlagsMask)
                            .end_time_unix_nano(3u64)
                            .status(Status::new(StatusCode::Ok, "status1"))
                            .attributes(vec![KeyValue::new("key", AnyValue::new_string("val2"))])
                            .links(vec![
                                // this Link's trace_id repeats with the previous, and  next one.
                                // doing this to ensure their parent IDs don't get interpreted as
                                // delta encoded
                                Link::build()
                                    .trace_id(u128::to_be_bytes(11))
                                    .span_id(u64::to_be_bytes(20))
                                    .attributes(vec![
                                        KeyValue::new("link_key_r", AnyValue::new_string("val1")),
                                        KeyValue::new("link_key", AnyValue::new_string("val2")),
                                    ])
                                    .flags(255u32)
                                    .finish(),
                            ])
                            .events(vec![
                                // this event has the repeating name with the next one and previous one
                                // doing this to ensure their parent IDs don't get interpreted as
                                // delta encoded
                                Event::build()
                                    .name("event1")
                                    .time_unix_nano(1u64)
                                    .attributes(vec![
                                        KeyValue::new("evt_key_r", AnyValue::new_string("val1")),
                                        KeyValue::new("evt_key", AnyValue::new_string("val2")),
                                    ])
                                    .finish(),
                            ])
                            .finish(),
                    ],
                )],
            ),
            ResourceSpans::new(
                Resource::build()
                    .attributes(vec![KeyValue::new("res_key", AnyValue::new_string("val2"))])
                    .finish(),
                vec![ScopeSpans::new(
                    InstrumentationScope::build()
                        .attributes(vec![KeyValue::new(
                            "scope_key",
                            AnyValue::new_string("val3"),
                        )])
                        .finish(),
                    vec![
                        Span::build()
                            .trace_id(u128::to_be_bytes(3))
                            .span_id(u64::to_be_bytes(3))
                            .name("albert")
                            .start_time_unix_nano(3u64)
                            .end_time_unix_nano(4u64)
                            .status(Status::new(StatusCode::Ok, "status1"))
                            .attributes(vec![KeyValue::new("key", AnyValue::new_string("val1"))])
                            .links(vec![
                                // this Link's trace_id repeats with the previous one. do this to ensure they
                                // don't get interpreted as delta encoded
                                Link::build()
                                    .trace_id(u128::to_be_bytes(11))
                                    .span_id(u64::to_be_bytes(30))
                                    .finish(),
                                Link::build()
                                    .trace_id(u128::to_be_bytes(31))
                                    .span_id(u64::to_be_bytes(31))
                                    .finish(),
                            ])
                            .events(vec![
                                // this event has the repeating name with the previous one. doing this
                                // to ensure their parent IDs don't get interpreted as delta encoded
                                Event::build().name("event1").time_unix_nano(2u64).finish(),
                            ])
                            .finish(),
                        Span::build()
                            .trace_id(u128::to_be_bytes(4))
                            .span_id(u64::to_be_bytes(4))
                            .name("terry")
                            .start_time_unix_nano(4u64)
                            .end_time_unix_nano(5u64)
                            .status(Status::new(StatusCode::Ok, "status1"))
                            .attributes(vec![KeyValue::new("key", AnyValue::new_string("val4"))])
                            .links(vec![
                                Link::build()
                                    .trace_id(u128::to_be_bytes(40))
                                    .span_id(u64::to_be_bytes(40))
                                    .finish(),
                            ])
                            .finish(),
                    ],
                )],
            ),
        ]);

        roundtrip_otlp_otap_traces(otlp_service_req);
    }

    #[test]
    fn test_otlp_otap_metrics_roundtrip() {
        let otlp_service_req = ExportMetricsServiceRequest::new(vec![ResourceMetrics {
            schema_url: "resource1 schema url".into(),
            resource: Some(Resource {
                dropped_attributes_count: 1,
                attributes: vec![KeyValue::new("res_attr1", AnyValue::new_string("res_val1"))],
                // TODO support entity refs
                entity_refs: Default::default(),
            }),

            scope_metrics: vec![ScopeMetrics {
                schema_url: "scope1 schema url".into(),
                scope: Some(InstrumentationScope {
                    name: "scope1 name".into(),
                    version: "scope1 version".into(),
                    attributes: vec![KeyValue::new("scp_attr1", AnyValue::new_string("scp_val1"))],
                    dropped_attributes_count: 2,
                }),
                metrics: vec![
                    Metric {
                        name: "metric1".into(),
                        description: "metric1 desc".into(),
                        unit: "m1 unit".into(),
                        // Test empty data
                        data: None,
                        metadata: vec![KeyValue::new(
                            "met_attr1",
                            AnyValue::new_string("met_val1"),
                        )],
                    },
                    Metric {
                        name: "metric2".into(),
                        description: "metric2 desc".into(),
                        unit: "m2 unit".into(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![
                                NumberDataPoint {
                                    attributes: vec![KeyValue::new(
                                        "attr1",
                                        AnyValue::new_string("val1"),
                                    )],
                                    start_time_unix_nano: 5,
                                    time_unix_nano: 6,
                                    exemplars: vec![
                                        Exemplar {
                                            time_unix_nano: 56,
                                            span_id: 4u64.to_le_bytes().to_vec(),
                                            trace_id: 999u128.to_le_bytes().to_vec(),
                                            value: Some(exemplar::Value::AsDouble(-3.0)),
                                            filtered_attributes: vec![
                                                KeyValue::new(
                                                    "attr1",
                                                    AnyValue::new_string("val1"),
                                                ),
                                                KeyValue::new(
                                                    "attr2",
                                                    AnyValue::new_string("val1"),
                                                ),
                                            ],
                                        },
                                        Exemplar {
                                            time_unix_nano: 56,
                                            span_id: 2u64.to_le_bytes().to_vec(),
                                            trace_id: 9393939u128.to_le_bytes().to_vec(),
                                            value: Some(exemplar::Value::AsInt(-10)),
                                            filtered_attributes: vec![KeyValue::new(
                                                "attr3",
                                                AnyValue::new_string("val3"),
                                            )],
                                        },
                                    ],
                                    flags: 8,
                                    value: None, // Test None Value
                                },
                                NumberDataPoint {
                                    attributes: vec![
                                        KeyValue::new("attr1", AnyValue::new_string("val1")),
                                        KeyValue::new("attr2", AnyValue::new_string("val1")),
                                    ],
                                    start_time_unix_nano: 6,
                                    time_unix_nano: 7,
                                    exemplars: vec![Exemplar {
                                        time_unix_nano: 156,
                                        span_id: 12u64.to_le_bytes().to_vec(),
                                        trace_id: 932339u128.to_le_bytes().to_vec(),
                                        value: Some(exemplar::Value::AsInt(10)),
                                        filtered_attributes: vec![KeyValue::new(
                                            "attr4",
                                            AnyValue::new_string("val40"),
                                        )],
                                    }],
                                    flags: 9,
                                    value: Some(Value::AsDouble(11.0)), // Test double value
                                },
                                NumberDataPoint {
                                    attributes: vec![KeyValue::new(
                                        "attr2",
                                        AnyValue::new_string("val1"),
                                    )],
                                    start_time_unix_nano: 6,
                                    time_unix_nano: 8,
                                    exemplars: vec![],
                                    flags: 9,
                                    value: Some(Value::AsInt(14)), // Test int value
                                },
                            ],
                        })),
                        metadata: vec![
                            KeyValue::new("met_attr1", AnyValue::new_string("met_val2")),
                            KeyValue::new("met_attr2", AnyValue::new_string("met_val2")),
                        ],
                    },
                    Metric {
                        name: "metric3".into(),
                        description: "metric3 desc".into(),
                        unit: "m3 unit".into(),
                        metadata: vec![KeyValue::new(
                            "met_attr2",
                            AnyValue::new_string("met_val1"),
                        )],
                        data: Some(Data::Sum(Sum {
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            is_monotonic: true,
                            data_points: vec![
                                NumberDataPoint {
                                    attributes: vec![
                                        KeyValue::new("attr2", AnyValue::new_string("val1")),
                                        KeyValue::new("attr4", AnyValue::new_string("val1")),
                                    ],
                                    start_time_unix_nano: 16,
                                    time_unix_nano: 18,
                                    exemplars: vec![],
                                    flags: 19,
                                    value: Some(Value::AsInt(14)),
                                },
                                NumberDataPoint {
                                    attributes: vec![KeyValue::new(
                                        "attr",
                                        AnyValue::new_string("val1"),
                                    )],
                                    start_time_unix_nano: 17,
                                    time_unix_nano: 18,
                                    exemplars: vec![Exemplar {
                                        time_unix_nano: 1,
                                        span_id: 2u64.to_le_bytes().to_vec(),
                                        trace_id: 3u128.to_le_bytes().to_vec(),
                                        value: Some(exemplar::Value::AsDouble(-4.0)),
                                        filtered_attributes: vec![KeyValue::new(
                                            "attr5",
                                            AnyValue::new_string("val6"),
                                        )],
                                    }],
                                    flags: 0,
                                    value: Some(Value::AsInt(14)),
                                },
                            ],
                        })),
                    },
                    Metric {
                        name: "metric4".into(),
                        description: "metric4 desc".into(),
                        unit: "m4 unit".into(),
                        metadata: vec![
                            KeyValue::new("met_attr1", AnyValue::new_string("met_val2")),
                            KeyValue::new("met_attr2", AnyValue::new_string("met_val2")),
                            KeyValue::new("met_attr3", AnyValue::new_string("met_val1")),
                        ],
                        data: Some(Data::Histogram(Histogram {
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                            data_points: vec![
                                HistogramDataPoint {
                                    time_unix_nano: 1,
                                    start_time_unix_nano: 2,
                                    attributes: vec![
                                        KeyValue::new("attr1", AnyValue::new_string("val1")),
                                        KeyValue::new("attr2", AnyValue::new_string("val2")),
                                    ],
                                    count: 3,
                                    sum: Some(4.0),
                                    bucket_counts: vec![1, 2],
                                    exemplars: vec![Exemplar {
                                        time_unix_nano: 56,
                                        span_id: 78u64.to_le_bytes().to_vec(),
                                        trace_id: 1011u128.to_le_bytes().to_vec(),
                                        value: Some(exemplar::Value::AsInt(-10)),
                                        filtered_attributes: vec![KeyValue::new(
                                            "attr4",
                                            AnyValue::new_string("terry"),
                                        )],
                                    }],
                                    explicit_bounds: vec![3.0, 4.0, 5.0],
                                    flags: 6,
                                    min: Some(7.0),
                                    max: Some(8.0),
                                },
                                HistogramDataPoint {
                                    time_unix_nano: 3,
                                    start_time_unix_nano: 4,
                                    attributes: vec![KeyValue::new(
                                        "attr1",
                                        AnyValue::new_string("val1"),
                                    )],
                                    count: 2,
                                    sum: Some(5.0),
                                    bucket_counts: vec![6, 7, 8],
                                    exemplars: vec![],
                                    explicit_bounds: vec![9.0, 10.0],
                                    flags: 16,
                                    min: Some(17.0),
                                    max: Some(18.0),
                                },
                            ],
                        })),
                    },
                    Metric {
                        name: "metric5".into(),
                        description: "metric5 desc".into(),
                        unit: "m5 unit".into(),
                        metadata: vec![
                            KeyValue::new("attr1", AnyValue::new_string("val6")),
                            KeyValue::new("attr2", AnyValue::new_string("val7")),
                        ],
                        data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            data_points: vec![
                                ExponentialHistogramDataPoint {
                                    start_time_unix_nano: 8,
                                    time_unix_nano: 3,
                                    count: 99,
                                    sum: Some(94.4),
                                    scale: 76,
                                    zero_count: 324,
                                    positive: Some(Buckets {
                                        offset: -3,
                                        bucket_counts: vec![1, 2, 2345435235, 2, 443434],
                                    }),
                                    negative: Some(Buckets {
                                        offset: 5,
                                        bucket_counts: vec![1, 2, 4, 0, 1, 3, 9999, 3],
                                    }),
                                    flags: 48,
                                    min: Some(9.4),
                                    max: Some(99.5),
                                    zero_threshold: 4.9,
                                    attributes: vec![
                                        KeyValue::new("attr1", AnyValue::new_string("val6")),
                                        KeyValue::new("attr2", AnyValue::new_string("val7")),
                                    ],
                                    exemplars: vec![Exemplar {
                                        time_unix_nano: 9,
                                        span_id: 78u64.to_le_bytes().to_vec(),
                                        trace_id: 1011u128.to_le_bytes().to_vec(),
                                        value: Some(exemplar::Value::AsInt(-999)),
                                        filtered_attributes: vec![KeyValue::new(
                                            "attr4",
                                            AnyValue::new_string("lance"),
                                        )],
                                    }],
                                },
                                ExponentialHistogramDataPoint {
                                    positive: Some(Buckets {
                                        offset: -3,
                                        bucket_counts: vec![4, 4, 5, 3],
                                    }),
                                    negative: Some(Buckets {
                                        offset: 5,
                                        bucket_counts: vec![1, 2, 3],
                                    }),
                                    attributes: vec![KeyValue::new(
                                        "attr1",
                                        AnyValue::new_string("val6"),
                                    )],
                                    exemplars: vec![], // TODO
                                    ..Default::default()
                                },
                            ],
                        })),
                    },
                    Metric {
                        name: "metric6".into(),
                        description: "metric 6 desc".into(),
                        unit: "metric6 unit".into(),
                        metadata: vec![
                            KeyValue::new("attr1", AnyValue::new_string("val99")),
                            KeyValue::new("attr2", AnyValue::new_string("val007")),
                        ],
                        data: Some(Data::Summary(Summary {
                            data_points: vec![
                                SummaryDataPoint {
                                    count: 1,
                                    sum: 2.0,
                                    attributes: vec![
                                        KeyValue::new("dp_attr1", AnyValue::new_string("val99")),
                                        KeyValue::new("dp_attr2", AnyValue::new_string("val007")),
                                    ],
                                    start_time_unix_nano: 8383,
                                    time_unix_nano: 9873,
                                    quantile_values: vec![
                                        ValueAtQuantile {
                                            quantile: 1.0,
                                            value: 2.0,
                                        },
                                        ValueAtQuantile {
                                            quantile: 8.0,
                                            value: 4.0,
                                        },
                                        ValueAtQuantile {
                                            quantile: 9.0,
                                            value: 5.0,
                                        },
                                    ],
                                    flags: 256,
                                },
                                SummaryDataPoint {
                                    count: 11,
                                    sum: 21.0,
                                    attributes: vec![KeyValue::new(
                                        "dp_attr11",
                                        AnyValue::new_string("val99"),
                                    )],
                                    start_time_unix_nano: 333,
                                    time_unix_nano: 444,
                                    quantile_values: vec![
                                        ValueAtQuantile {
                                            quantile: 11.0,
                                            value: 20.0,
                                        },
                                        ValueAtQuantile {
                                            quantile: 81.0,
                                            value: 40.0,
                                        },
                                        ValueAtQuantile {
                                            quantile: 91.0,
                                            value: 59.0,
                                        },
                                    ],
                                    flags: 200,
                                },
                            ],
                        })),
                    },
                ],
            }],
        }]);

        roundtrip_otlp_otap_metrics(otlp_service_req);
    }

    #[test]
    fn test_signal_type() {
        // Test signal_type for OtlpProtoBytes variants
        let logs_bytes = OtlpProtoBytes::ExportLogsRequest(Bytes::new());
        let metrics_bytes = OtlpProtoBytes::ExportMetricsRequest(Bytes::new());
        let traces_bytes = OtlpProtoBytes::ExportTracesRequest(Bytes::new());

        assert_eq!(logs_bytes.signal_type(), SignalType::Logs);
        assert_eq!(metrics_bytes.signal_type(), SignalType::Metrics);
        assert_eq!(traces_bytes.signal_type(), SignalType::Traces);

        // Test signal_type for OtapArrowRecords variants
        let logs_records = OtapArrowRecords::Logs(Default::default());
        let metrics_records = OtapArrowRecords::Metrics(Default::default());
        let traces_records = OtapArrowRecords::Traces(Default::default());

        assert_eq!(logs_records.signal_type(), SignalType::Logs);
        assert_eq!(metrics_records.signal_type(), SignalType::Metrics);
        assert_eq!(traces_records.signal_type(), SignalType::Traces);

        // Test signal_type for OtapPdata variants
        let pdata_logs: OtapPayload = OtlpProtoBytes::ExportLogsRequest(Bytes::new()).into();
        let pdata_metrics: OtapPayload = OtapArrowRecords::Metrics(Default::default()).into();
        assert_eq!(pdata_logs.signal_type(), SignalType::Logs);
        assert_eq!(pdata_metrics.signal_type(), SignalType::Metrics);
    }

    #[test]
    fn test_otlp_proto_bytes_metrics_num_items() {
        use crate::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
        use crate::proto::opentelemetry::common::v1::InstrumentationScope;
        use crate::proto::opentelemetry::metrics::v1::exponential_histogram_data_point::Buckets;
        use crate::proto::opentelemetry::metrics::v1::number_data_point::Value;
        use crate::proto::opentelemetry::metrics::v1::summary_data_point::ValueAtQuantile;
        use crate::proto::opentelemetry::metrics::v1::{
            AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge,
            Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
            Sum, Summary, SummaryDataPoint, metric::Data,
        };
        use crate::proto::opentelemetry::resource::v1::Resource;
        use prost::Message;

        let metrics = ExportMetricsServiceRequest {
            resource_metrics: vec![
                ResourceMetrics {
                    resource: Some(Resource::default()),
                    scope_metrics: vec![ScopeMetrics {
                        scope: Some(InstrumentationScope::default()),
                        metrics: vec![
                            Metric {
                                name: "gauge_metric".into(),
                                data: Some(Data::Gauge(Gauge {
                                    data_points: vec![
                                        NumberDataPoint {
                                            value: Some(Value::AsDouble(1.0)),
                                            ..Default::default()
                                        },
                                        NumberDataPoint {
                                            value: Some(Value::AsDouble(2.0)),
                                            ..Default::default()
                                        },
                                    ],
                                })),
                                ..Default::default()
                            },
                            Metric {
                                name: "sum_metric".into(),
                                data: Some(Data::Sum(Sum {
                                    data_points: vec![
                                        NumberDataPoint {
                                            value: Some(Value::AsInt(100)),
                                            ..Default::default()
                                        },
                                        NumberDataPoint {
                                            value: Some(Value::AsInt(200)),
                                            ..Default::default()
                                        },
                                        NumberDataPoint {
                                            value: Some(Value::AsInt(300)),
                                            ..Default::default()
                                        },
                                    ],
                                    aggregation_temporality: AggregationTemporality::Cumulative
                                        .into(),
                                    is_monotonic: true,
                                })),
                                ..Default::default()
                            },
                            Metric {
                                name: "histogram_metric".into(),
                                data: Some(Data::Histogram(Histogram {
                                    data_points: vec![
                                        HistogramDataPoint {
                                            count: 10,
                                            sum: Some(100.0),
                                            bucket_counts: vec![2, 5, 3],
                                            explicit_bounds: vec![10.0, 50.0],
                                            ..Default::default()
                                        },
                                        HistogramDataPoint {
                                            count: 20,
                                            sum: Some(200.0),
                                            bucket_counts: vec![5, 10, 5],
                                            explicit_bounds: vec![10.0, 50.0],
                                            ..Default::default()
                                        },
                                    ],
                                    aggregation_temporality: AggregationTemporality::Cumulative
                                        .into(),
                                })),
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ResourceMetrics {
                    resource: Some(Resource::default()),
                    scope_metrics: vec![ScopeMetrics {
                        scope: Some(InstrumentationScope::default()),
                        metrics: vec![
                            Metric {
                                name: "exp_histogram_metric".into(),
                                data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                                    data_points: vec![
                                        ExponentialHistogramDataPoint {
                                            count: 15,
                                            sum: Some(150.0),
                                            scale: 1,
                                            zero_count: 1,
                                            positive: Some(Buckets {
                                                offset: 0,
                                                bucket_counts: vec![3, 5, 7],
                                            }),
                                            negative: Some(Buckets {
                                                offset: 0,
                                                bucket_counts: vec![1, 2],
                                            }),
                                            ..Default::default()
                                        },
                                        ExponentialHistogramDataPoint {
                                            count: 25,
                                            sum: Some(250.0),
                                            scale: 1,
                                            zero_count: 2,
                                            positive: Some(Buckets {
                                                offset: 0,
                                                bucket_counts: vec![5, 10, 8],
                                            }),
                                            ..Default::default()
                                        },
                                    ],
                                    aggregation_temporality: AggregationTemporality::Cumulative
                                        .into(),
                                })),
                                ..Default::default()
                            },
                            Metric {
                                name: "summary_metric".into(),
                                data: Some(Data::Summary(Summary {
                                    data_points: vec![
                                        SummaryDataPoint {
                                            count: 100,
                                            sum: 1000.0,
                                            quantile_values: vec![
                                                ValueAtQuantile {
                                                    quantile: 0.5,
                                                    value: 10.0,
                                                },
                                                ValueAtQuantile {
                                                    quantile: 0.95,
                                                    value: 50.0,
                                                },
                                            ],
                                            ..Default::default()
                                        },
                                        SummaryDataPoint {
                                            count: 200,
                                            sum: 2000.0,
                                            quantile_values: vec![ValueAtQuantile {
                                                quantile: 0.5,
                                                value: 20.0,
                                            }],
                                            ..Default::default()
                                        },
                                    ],
                                })),
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
        };

        let mut buf = Vec::new();
        metrics.encode(&mut buf).unwrap();

        let otlp_bytes = OtlpProtoBytes::ExportMetricsRequest(Bytes::from(buf));

        assert_eq!(otlp_bytes.num_items(), 11);
    }
}
