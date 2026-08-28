# Pdata codec extensions

Receivers identify the incoming codec; pdata owns representation-specific
conversion, measurement, and batching. Processors own scheduling and delivery
tracking, and exporters request their required output representation. This
implements the extension model in
[issue #3452](https://github.com/open-telemetry/otel-arrow/issues/3452).

The built-in `otlp-bytes` codec supports logs, metrics, and traces. Native OTAP
is the common intermediate representation, not an artificial byte encoding.
OTAP gRPC dictionaries remain stream-local transport state. Syslog batches,
Parquet codecs, and new wire protocols are future work.

## Receiver admission and lazy decoding

`codec::resolve(encoding, signal, CodecDirection::Decode)` returns an immutable
`ResolvedCodec`. A receiver with a fixed input codec can resolve it at startup
and reuse the handle. A receiver with message-specific codec names must resolve
them before enqueueing a message and translate failures into producer-facing
protocol errors when supported.

`ResolvedCodec::admit(signal, bytes)` creates an `EncodedPdata` only if the codec
supports that signal and has a decoder. `EncodedPdata::new` provides the same
checks when starting from a name. Unknown and encode-only codecs cannot enter
the pipeline. Admission does not parse the bytes: malformed data can still fail
later when a consumer requires decoding. Existing OTLP receivers use the known
built-in handle; they do not add a codec-name header to OTLP.

The envelope carries the resolved handle, signal, shared `Bytes`, and an
optional known item count. Headers, peer address, and Ack/Nack context remain
outside it in `OtapPdata`. Storage variants are private to pdata; test builds
expose them for introspection. All byte representations, including OTLP, use
inline encoded envelopes. Only
native OTAP uses record storage. Encoded item counts live in the envelope;
logical Arrow size caches live beside the records, so caches do not enlarge
every queued representation. Cloning shares the bytes without allocating an
additional envelope.

Forwarding, routing, retry, and fan-out neither resolve nor instantiate codecs.
Matching encoded output shares the original bytes, even when the input codec
has no encoder: no conversion is needed in that case. Unknown codecs cannot
reach this path because admission has already failed.

## Registration and consumer-local state

Extensions register immutable `PdataCodecRegistration` factories in the
`linkme` distributed slice `PDATA_CODEC_FACTORIES`. Registration is process-wide;
mutable state is not. There is no dynamic loader, mutable global registry,
background task, or codec control channel. Codecs are distinct from the service
extensions in [Extension System Architecture](extension-system-architecture.md).

Each registration advertises its canonical encoding name, supported signals,
encoder/decoder availability, optional format version and intrinsic compression,
and optional native batching capabilities and default profile.

Names use lowercase ASCII letters, digits, periods, underscores, hyphens, or
colons. `otap`, `otlp`, and `preserve` are reserved configuration names;
`otlp-bytes` identifies the built-in codec. Use vendor prefixes for extensions.
Incompatible versions and intrinsic compression variants need distinct names.
HTTP/gRPC transport compression is separate from codec identity.

Each pipeline runtime owns a `CodecState` for its lifetime. It lazily creates
and reuses at most one instance per used codec, including OTLP. Conversion,
prepared output, views, and native batching all use that same instance.
Codec implementations can own scratch buffers and other mutable state. They must
be `Send` so the shared executor can own them, but they do not need to be `Sync`.
The pipeline runtime injects codec state through effect handlers; nodes do not
store it or construct one for each input. Runtime-local nodes use lock-free
state, while shared nodes use the sendable executor selected by the runtime.

Codecs consume and produce complete independent batches. They cannot rely on a
previous message's dictionary or framing state. They must preserve signal type,
honor output encoding options, bound decompression and scratch allocation, and remain
usable after a failed operation.

## Consumer operations

| Consumer | Pdata operation |
| --- | --- |
| Record processor | Effect-handler `try_into_otap` returns native pdata. |
| Native record owner | `OtapPayload::try_into_otap` consumes it with `CodecState`. |
| Read-only consumer | Effect-handler `view` borrows or asks the codec for a view. |
| Encoded exporter | An `EncodingPlan` and effect-handler encoding share bytes or reuse scratch. |
| Representation conversion | `convert_encoding` replaces data after success. |
| Batch processor | `BatchPlan` prepares, measures, batches, and finishes. |

The concrete encoded/native storage enum is private. Components use
`PdataFormat` for identity and the operations above for access, so adding a byte
codec cannot introduce new representation matches in receivers, processors, or
exporters. A failed typed native conversion owns the original pdata and its
delivery context for Nack or retry.

`EncodedOutput::as_ref()` lets HTTP compression consume encoder scratch directly.
`copy_into_bytes()` retains scratch capacity, copying only scratch-backed output.
`into_bytes()` detaches encoded storage for an asynchronous send without copying.
The borrow prevents reuse while an output still references scratch storage.
`PdataCodec::prepare_encode` defaults to owned output; codecs may override it
to return `EncodedOutput::buffer` borrowing their own bounded scratch storage.
OTLP owns its encoder state inside the registered codec, initialized only when
encoding is requested. Its scratch buffers grow independently for logs, metrics,
and traces, so a large
batch of one signal does not inflate detached allocations for the others.

Existing exporters retain their wire protocols: OTLP exporters request OTLP
bytes, while the OTAP exporter requests native records for its stream encoder.
There is no automatic graph-wide format negotiation. Durable storage retains
its existing opaque OTLP and native Arrow formats; arbitrary opaque disk storage
is not introduced here.

## Batching contract

`BatchPlan` resolves capabilities before buffering. OTLP uses its existing
protobuf byte-batching implementation, and OTAP uses native item batching.
Extensions may implement `PdataCodec::batch` and advertise supported sizing
modes through `BatchingSupport`. `measure` can be overridden for an efficient
native item count; its default item implementation decodes.

Without a suitable native batcher, item-based batching materializes OTAP.
Under the default `preserve` policy this fallback emits OTAP, allowing later
processors to use those records directly. An explicit output codec requires an
encoder and re-encodes only emitted batches. Retained tails stay in the working
representation. Byte sizing requires native byte-batching support; the framework
does not approximate byte limits by counting items. Request sizing is reserved
and currently unsupported.

Native batching must preserve input order and return output ownership weights
that partition the input units exactly. Byte splitting can duplicate wrapper
bytes, so output length and ownership weight can differ. The framework checks
the total weight, and the processor uses ownership to track each input across
all its output fragments. Codecs must honor fragment, wrapper-overhead, and
per-flush split budgets; indivisible or over-budget entries remain whole and
contribute to the split-budget fallback count.

The processor owns timers, bounded inbound/outbound completion slots, and
Ack/Nack delivery. Buffer and timer identities come from a finite set of
resolved plans, not arbitrary names received from producers. Completion tokens
identify the owning buffer even if downstream changes payload representation.
Equivalent fallback plans share a buffer.

See [Batch Processor](../crates/core-nodes/src/processors/batch_processor/README.md)
for compatible aliases and the codec-name-based configuration.

## Failure, fan-out, and measurements

Registration validation rejects invalid names, duplicate identities, and
inconsistent capabilities. Admission rejects missing decoders and unsupported
signals. Conversion validates the requested target before decoding its source.
Data failures use the component's existing error/Nack path rather than silently
turning a failed conversion into an empty batch.

In-place materialization and representation conversion retain the original
payload and measurement cache on failure, with delivery context untouched.
Consuming conversions require the caller to retain input when needed for retry.
Clones share bytes; materializing one branch does not modify another. Read-only
views do not replace the source representation or create a cross-branch cache.

`known_item_count()` reads existing metadata without parsing and distinguishes
an absent count from zero. Registrations can supply a stateless `count_items`
hook for metrics; OTLP uses its existing protobuf scan. The result is cached
locally to that payload branch. This neither instantiates a codec nor decodes
to OTAP. Codecs without a counter retain unknown counts until supplied by the
receiver or measured during processing. Batching prepares and measures its input
before testing for emptiness. Existing optional item
metrics still report zero for unknown counts without forcing a decode; receivers
should supply accurate counts when available. Encoded memory estimates use byte
length; a `Bytes` slice can pin a larger allocation whose capacity is not exposed.
Account for both input and decoded output during conversion, as well as retained
consumer-local codec buffers.

## Registering a future codec

A future independently decodable format can opt into OTAP fallback batching:

```rust,ignore
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::codec::{
    PdataCodecMetadata, PdataCodecRegistration, PdataEncoding, PDATA_CODEC_FACTORIES,
};

static METADATA: PdataCodecMetadata = PdataCodecMetadata {
    encoding: PdataEncoding::new("example-format-v1"),
    signals: &[SignalType::Logs],
    format_version: Some("1"),
    compression: None,
    can_decode: true,
    can_encode: true,
    batching: None,
};

#[allow(unsafe_code)]
#[linkme::distributed_slice(PDATA_CODEC_FACTORIES)]
static FACTORY: PdataCodecRegistration = PdataCodecRegistration {
    metadata: &METADATA,
    create: || Box::new(ExampleCodec::new()),
    count_items: None,
};
```

The corresponding receiver must recognize that format and admit it with the
resolved decoder. Existing processors require no new representation branches.
A compatible exporter can forward matching bytes or request encoding from OTAP.
`PdataCodec::view` defaults to decoding OTAP. OTLP overrides it with a borrowed
signal-and-byte view; existing read-only consumers can retain their direct
protobuf paths without inspecting payload storage. Views borrow the input, not
codec state, and do not replace the original representation.

Such receivers and exporters are not capabilities of the current OTLP/OTAP
network protocols.
