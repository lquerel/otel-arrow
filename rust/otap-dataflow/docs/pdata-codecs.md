# Pdata codec extensions

Pipelines carry native OTAP records or independently encoded batches. Codecs
convert encoded batches to and from OTAP; OTAP remains the common intermediate
representation. This implements the extension model in
[issue #3452](https://github.com/open-telemetry/otel-arrow/issues/3452).

The built-in `otlp-bytes` codec supports logs, metrics, and traces using the
existing OTLP conversion routines. This change does not implement syslog batch,
Parquet, or an independent OTAP byte encoding. OTAP gRPC stream dictionaries
remain transport state owned by the existing receiver/exporter.

## Representation and passthrough

`EncodedPdata` carries a `PdataEncoding`, a signal, a `bytes::Bytes` buffer,
and an optional item count. `OtapPdata` keeps transport headers, peer address,
and delivery context outside that envelope.

Use `OtapPayload::from_encoded` to wrap an encoded batch. OTLP is stored in the
existing `PayloadData::OtlpBytes` variant so current protobuf views and byte
batching keep their fast paths. All other encodings use
`PayloadData::Encoded`. Both expose their identity through `encoding()`.
Native `OtapArrowRecords` have no byte encoding identity. Extension envelopes
are shared through `Arc`, keeping the payload enum as compact as its built-in
representations and avoiding envelope allocation when forwarding or cloning.

Routing, retry, fan-out, and topic delivery can forward or clone the payload
without looking up a codec, decoding, or copying its byte buffer. Exporting to
the current encoding also needs no codec, including for an unknown encoding.

## Registration and ownership

Codec extensions are compiled into the binary and register immutable factories
in `PDATA_CODEC_FACTORIES` using `linkme`, like the engine's component factories.
Registration is process-wide; mutable codec state is not. There is no dynamic
loader, mutable global registry, pipeline YAML entry, background task, or control
channel. Codec extensions are distinct from the capability-based service
extensions described in [Extension System Architecture](extension-system-architecture.md).

Each registration advertises:

- A stable encoding name and supported signals.
- Encoder and decoder availability.
- Optional informational format version and intrinsic compression.

Built-in names are reserved. Other authors should use a vendor-prefixed name.
Names use lowercase ASCII letters, digits, periods, underscores, hyphens, or
colons. Incompatible versions or compression variants require distinct encoding
identities. Transport compression, such as HTTP gzip, is outside the identity.

A factory creates a `Box<dyn PdataCodec>`. Codec implementations may contain
`Rc`, reusable buffers, or other state that is neither `Send` nor `Sync`.
Instances belong to the consumer on the calling core, never to a payload.
The convenience conversion methods create an instance only when converting;
components that repeatedly convert can resolve a factory once and retain their
own codec. The built-in OTLP conversion path calls its concrete codec directly.

Codecs must produce independent batches. A decoder cannot require dictionaries
or frames from a previous message because batches can be retried, reordered,
fanned out, or sent across pipeline boundaries. Codecs must preserve signal type,
honor applicable conversion options, validate input, and bound format-specific
allocation and decompression. The interface adds no new worker tasks or queues.

## Component contracts

| Component behavior | Interface and responsibility |
| --- | --- |
| Encoded receiver | Wrap bytes with the correct identity and signal. Supply the item count when known. |
| Passthrough processor | Forward or clone pdata unchanged; no codec is required. |
| Record processor | Use `materialize_otap(options)` or the existing `TryIntoWithOptions<OtapArrowRecords>` conversion. |
| Read-only record consumer | Use `view(options)` to borrow OTLP/native OTAP or decode another encoding into an owned OTAP view. |
| Encoded exporter | Request its target with `into_encoded(encoding, options)`; matching input returns original bytes directly. |
| Factory with known codec requirements | Call `codec::resolve(encoding, signal, CodecDirection)` during construction. |

Output representation is chosen explicitly by the consumer. There is no
automatic negotiation or inference across arbitrary pipeline graphs. Existing
OTLP exporters request OTLP; existing OTAP exporters request native OTAP.

Batching preserves its OTLP byte-batching path. Other encodings decode to OTAP
and use the configured native or OTLP batcher. Durable buffering currently
preserves opaque OTLP bytes only; other encodings decode to its Arrow storage
format. Generalized opaque disk storage is not part of this change.

## Errors, fan-out, and accounting

Pipeline construction rejects invalid or duplicate registrations. Resolution
rejects missing codecs, unsupported signals, and unavailable encoder/decoder
directions. When a format is known only at runtime, conversion reports the
failure then; opaque passthrough remains valid without a decoder.

`materialize_otap` and `convert_encoding` change the payload only after
success. Failure leaves the original payload and measurement cache intact.
Their `OtapPdata` counterparts also leave headers and the Ack/Nack stack
untouched. Consuming conversion methods still require the caller to retain the
original when needed for Nack/retry, as with the existing conversion traits.

Clones initially share byte buffers. Materializing one branch does not modify
another branch. Successful in-place materialization retains OTAP for later use
on that branch; there is no cross-branch decoded cache. Read-only `view` calls
do not replace the source representation.

Encoded logical and retained size estimates use the byte length. As with OTLP,
a `Bytes` slice may pin a larger allocation whose capacity is not exposed.
Unknown item counts report zero to existing item metrics, never force a decode,
and do not make a nonempty byte buffer empty. Receivers should supply accurate
counts when available. Native OTAP keeps its existing logical/retained memory
accounting and lazy measurement caches. The transient input plus decoded output
during a conversion must be included in a component's resource planning.

## Future flat Parquet example

A future implementation can provide a codec with metadata such as
`example-flat-parquet-v1-zstd` and register its factory:

```rust,ignore
use otel_arrow_dfe_pdata::codec::{
    PdataCodecMetadata, PdataCodecRegistration, PdataEncoding, PDATA_CODEC_FACTORIES,
};
use otel_arrow_dfe_config::SignalType;

// FlatParquetCodec is a future implementation of PdataCodec.
static METADATA: PdataCodecMetadata = PdataCodecMetadata {
    encoding: PdataEncoding::new("example-flat-parquet-v1-zstd"),
    signals: &[SignalType::Logs],
    format_version: Some("1"),
    compression: Some("zstd"),
    can_decode: true,
    can_encode: true,
};

#[allow(unsafe_code)]
#[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]
static FACTORY: PdataCodecRegistration = PdataCodecRegistration {
    metadata: &METADATA,
    create: || Box::new(FlatParquetCodec::new()),
};
```

The deployment can then be:

```text
OTLP/OTAP -> record processing -> flat Parquet encoder -> transport
transport -> encoded receiver -> route/retry/topic -> compatible encoded exporter
```

The first pipeline needs the encoder. The downstream receiver must preserve
the encoding identity, signal, and relevant request metadata in its transport
envelope. Its routing and retry path can run without that codec and export the
same bytes through a compatible exporter. Adding record processing downstream
would require the decoder. Such a transport, exporter, and Parquet schema are
future work, not capabilities of the existing OTLP/OTAP network protocols.
