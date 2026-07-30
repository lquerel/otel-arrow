---
Proposal Name: file-exporter
Start Date: 2026-07-28
RFC PR: open-telemetry/otel-arrow#0000
Tracking Issue: open-telemetry/otel-arrow#0000
---

# RFC NNNN: OTAP-Native File Exporter

## Summary

Add an experimental file exporter to `otap-dataflow` with component URN
`urn:otel:exporter:file`. The exporter supports logs, metrics, and traces
(spans), accepts both OTLP protobuf bytes and OTAP Arrow records, writes
newline-delimited OTLP JSON to local files, and reports completion through the
engine's existing ACK/NACK path.

Each output file contains exactly one signal type, as required by the
OpenTelemetry Protocol File Exporter specification. A required `{signal}` path
token resolves to `logs`, `metrics`, or `traces`; required `{core_id}` and
`{generation}` tokens keep each writer exclusive to one pipeline runtime. The
exporter writes one `LogsData`, `MetricsData`, or `TracesData` JSON object per
input batch.

The first release deliberately provides a bounded capture-and-replay
capability: ordered writes per signal and runtime, bounded in-memory encoding,
explicit write durability, crash-tail recovery, and deterministic file
ownership. It does not include profiles, file rotation, compression, dynamic
attribute-based paths, arbitrary encodings, or an internal retry queue.

The OpenTelemetry Collector contrib `fileexporter` informs the compatibility
contract. This proposal follows the
[reference-informed capability design guide][design-guide] by preserving the
reference's multi-signal OTLP JSON behavior, composing retry and buffering with
existing pipeline components, improving readiness and file ownership, and
excluding behaviors that would add unbounded or security-sensitive state.

## Motivation

`otap-dataflow` can send telemetry to network services, print a diagnostic
rendering to the console, and write analytics-oriented Parquet output. It does
not have a small local-file sink that preserves logs, metrics, and traces in an
interoperable OTLP representation.

A file exporter supports three initial scenarios:

- capture any supported signal locally while developing or diagnosing a
  pipeline;
- transfer telemetry through an offline or air-gapped workflow; and
- replay captured telemetry with tools that read newline-delimited OTLP JSON,
  including the Collector's OTLP JSON File receiver.

The capability is not a durable queue. A successful local write does not make
the pipeline exactly-once, and the first release does not manage long-term disk
retention. Operators using it for more than bounded capture must provision a
filesystem quota or another external disk limit.

### Why one multi-signal component

The user outcome is the same for logs, metrics, and traces: write a lossless
OTLP representation to a local file for capture or replay. The format and
lifecycle options are also shared. A single `file` component therefore gives
operators one configuration model and matches the Go reference.

The component remains signal-aware internally. `OtapPdata::signal_type()`
selects the view, encoder, output path, item counter, and diagnostics. The
required `{signal}` token ensures that different top-level OTLP JSON types are
never mixed in one file. Supporting all three signals does not require a
signal-erased payload or a generic opaque encoder.

The public name is `file`, not `filelog`: logs are only one supported signal,
and spans are represented by the OTLP traces signal.

### Why OTLP JSON first

OTLP JSON preserves resources, instrumentation scopes, and signal records
needed for lossless replay. It is the format currently described by the
OpenTelemetry Protocol File Exporter specification and provides a direct
compatibility path with the Go reference's default format.

Plain text is useful for operational logs, but selecting a body or rendering a
template is a different and potentially lossy contract. Protobuf and compressed
output require explicit framing and recovery rules. Those formats should not be
introduced through an opaque encoding hook in the first release.

## Guide-level explanation

### User model

Each file exporter instance receives `OtapPdata`, identifies its signal,
converts the payload to canonical OTLP JSON, appends a line-feed byte, and
writes the entire frame to the file for that signal. It ACKs the original pdata
only after the configured durability point succeeds.

```text
                            +-> logs path
OtapPdata -> signal -> JSON +-> metrics path -> async write -> ACK
                            +-> traces path
                                  |
                                  +-> failure -> rollback -> NACK
```

There is no exporter-owned queue. At most one pdata message and one encoded
frame are retained by an instance. When the filesystem is slow, the exporter
awaits the write and the bounded input channel applies backpressure upstream.

### First useful configuration

The path is an absolute template. `{signal}`, `{core_id}`, and `{generation}`
are required, even when a pipeline currently carries one signal on one core.
They keep each physical file single-signal and unique to one pipeline runtime,
including during live replacement.

```yaml
type: exporter:file
config:
  path: "/var/log/otel/telemetry-{signal}-{core_id}-{generation}.jsonl"
  create_directories: false
  format: otlp_json
  open_mode: append
  durability: write
  max_frame_bytes: 67108864
  tail_recovery: truncate_partial
```

For logs on core `3` in deployment generation `7`, this resolves to:

```text
/var/log/otel/telemetry-logs-3-7.jsonl
```

Metrics and traces resolve the same template with `metrics` and `traces`.
Unused signal paths are not created; a signal writer is opened and probed
before its first payload is accepted.

The parent directory must exist unless `create_directories` is true. On Unix,
new files are created with mode `0600` and new directories with mode `0700`,
both still subject to the process umask. Existing permissions are never
changed. Windows files inherit access control from their parent directory.

### Output contract

`format: otlp_json` writes one compact JSON object per non-empty incoming pdata
message. The top-level object is the OTLP ProtoJSON representation associated
with the payload signal:

<!-- markdownlint-disable MD013 -->

| Signal | `{signal}` value | Top-level OTLP data | Repeated field |
| --- | --- | --- | --- |
| Logs | `logs` | `LogsData` | `resourceLogs` |
| Metrics | `metrics` | `MetricsData` | `resourceMetrics` |
| Traces (spans) | `traces` | `TracesData` | `resourceSpans` |

<!-- markdownlint-enable MD013 -->

Each frame ends with the single byte `\n` on every platform. JSON string
escaping ensures embedded newlines do not create extra physical records. Each
file contains exactly one signal type and conforms to the OpenTelemetry
Protocol File Exporter JSON Lines contract.

Small frames have these shapes:

```json
{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"ready"}}]}]}]}
```

```json
{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"requests"}]}]}]}
```

```json
{"resourceSpans":[{"scopeSpans":[{"spans":[{"name":"GET /ready"}]}]}]}
```

Field ordering and insignificant whitespace are not part of the contract.
Field names, enum representation, 64-bit integer representation, byte encoding,
and omission of default values follow OTLP ProtoJSON. Compatibility tests
compare decoded JSON values and replay behavior rather than byte order.

An empty payload is ACKed without writing a line. A single input batch remains
a single line; the exporter does not split, flatten, or reorder log records,
metric data points, or spans.

### File modes

`open_mode` has three values:

- `append` is the default. Existing complete frames are retained and new frames
  are appended.
- `truncate` explicitly discards existing contents when a signal path is first
  opened.
- `create_new` fails the first payload for a signal if its resolved path already
  exists.

`append` is intentionally safer than the Go reference's default truncation. The
generation token allows a new live configuration to start new files, while a
process restart of the same generation can resume prior files.

### Tail recovery

A process can stop after only part of a JSON frame reaches a file. In `append`
mode, `tail_recovery: truncate_partial` scans backward by at most
`max_frame_bytes` from the end:

1. If the file is empty or ends in `\n`, no repair is needed.
2. Otherwise, the exporter truncates to the byte after the last `\n`.
3. If no boundary is found within the bounded scan, opening that signal writer
   fails instead of guessing where valid data ends.

`tail_recovery: fail` rejects any file whose final byte is not `\n`.
`tail_recovery` is rejected with `truncate` and `create_new`, where it has no
meaning.

Tail repair can remove only the incomplete final frame. A complete frame that
reached disk immediately before a crash is retained even if its ACK was not
observed upstream, so replay can contain duplicates. That is the intended
at-least-once tradeoff.

### Durability and acknowledgement

`durability` defines what a successful ACK means:

- `write` ACKs after the operating system accepts the complete frame. It does
  not claim survival across a kernel or power failure.
- `sync_data` also completes `sync_data` for the signal file before ACKing. This
  is slower and still cannot make the whole pipeline exactly-once.

On graceful shutdown, the exporter drains pdata already admitted by the
exporter inbox, flushes every open signal file, and attempts `sync_data` before
the shutdown deadline. The final shutdown sync improves graceful-stop behavior
but does not retroactively strengthen ACKs previously issued with
`durability: write`.

### Unsupported configurations

The first release rejects or does not expose:

- profiles;
- protobuf output and custom encoding extensions;
- plain text, templates, and per-record flattening;
- zstd or other compression;
- rotation, age retention, and backup deletion;
- paths derived from resource or record attributes;
- multiple writer instances for one resolved signal path;
- relative paths;
- an internal sending queue or retry loop; and
- exactly-once or write-ahead-log semantics.

Static routing to several destinations can be composed from a bounded routing
processor and multiple file exporter nodes with operator-declared paths. Retry
and persistent buffering belong in the existing retry and durable-buffer
processors rather than in the file writer.

## Reference-level explanation

### Evidence reviewed

The reference review covers OpenTelemetry Collector contrib release
`v0.157.0` and `main` at commit
[`21196c805ba7091d0928434ec9ca145ed0386cab`][go-pin], both reviewed on
2026-07-28. Relevant modules include:

- `README.md`, `config.go`, and `factory.go` for the public contract and
  lifecycle;
- `file_exporter.go` and `grouping_file_exporter.go` for signal and grouping
  behavior;
- `marshaller.go`, `codec.go`, and `file_writer.go` for encoding and framing;
- `buffered_writer.go` and `compression_writer.go` for flushing and
  compression; and
- component tests and metadata for defaults and stability.

The OpenTelemetry Protocol [File Exporter specification][file-exporter-spec]
was also reviewed. It requires UTF-8 JSON Lines, `\n` delimiters, OTLP JSON
encoding, and exactly one of traces, metrics, or logs in each file.

At the pinned Collector snapshot, logs, metrics, and traces are alpha, profiles
are in development, and the [beta promotion issue][go-beta-issue] remains open.

Operational reports were also treated as evidence:

- startup can complete before the destination has proven writable
  ([#49192][go-startup-issue]);
- shutdown has a reported writer race
  ([#46871][go-shutdown-issue]);
- grouping has reports covering path containment, writer eviction, and
  unbounded cardinality
  ([#49233][go-path-issue], [#49228][go-race-issue],
  [#49226][go-cardinality-issue]);
- rotation can consume substantial CPU with many directories and backups
  ([#49899][go-rotation-issue]); and
- native zstd output still has an unresolved text-framing request
  ([#49328][go-compression-issue]).

These reports do not imply that every deployment is affected. They identify
design boundaries that the first OTAP implementation should avoid or make
explicit.

### Finding classification

<!-- markdownlint-disable MD013 -->

| Reference finding | Classification | OTAP decision |
| --- | --- | --- |
| Default OTLP JSON output | Preserve | `otlp_json` is the only first-release format. |
| One JSON object per exported batch | Preserve | One non-empty pdata message becomes one newline-delimited top-level data object. |
| Logs, metrics, traces, and profiles share one component | Simplify | One `file` exporter supports OTAP's three current signals; profiles remain out of scope. |
| A standards-conforming file contains one signal | Preserve | Require `{signal}` in every path and never mix top-level types. |
| Truncate is the default when append is false | Improve | Default to explicit `append`; keep `truncate` opt-in. |
| Buffered writer and periodic flush goroutine | Simplify | Keep one frame in flight and await async file I/O. |
| Collector exporter queue/retry composition | Compose | Use engine channels, ACK/NACK, retry, and durable-buffer processors. |
| Protobuf uses a four-byte length prefix | Investigate | Defer binary framing until a reader and format contract are designed together. |
| Per-message zstd and feature-gated native zstd | Reject for v1 | Any future compression must be standard file-level compression. |
| Arbitrary encoding extensions | Avoid | Add typed formats or a separate encoding capability only when framing is known. |
| Dynamic `group_by` resource path | Reject for v1 | Telemetry values never affect filesystem paths. |
| LRU of open grouped writers | Avoid | At most three fixed signal writers exist per runtime instance. |
| Optional directory creation | Preserve | Support it, disabled by default and using restrictive creation modes. |
| Rotation and backup retention | Investigate | Defer until cleanup cost, crash recovery, and multi-generation ownership are bounded. |
| Startup opens the writer | Improve | Probe each signal destination before accepting its first payload. |
| Shutdown closes shared writer state | Improve | The run loop exclusively owns all signal writers and has no flush goroutine. |

<!-- markdownlint-enable MD013 -->

### Component identity and placement

The public and internal names follow the component naming conventions:

- module: `file_exporter`;
- URN: `urn:otel:exporter:file`; and
- primary component metric set: `exporter.file`.

The component belongs in `crates/core-nodes/src/exporters/file_exporter/`
because OTLP file capture is a protocol-level local sink without a vendor
dependency. The module contains `config.rs`, `writer.rs`, `metrics.rs`,
`README.md`, and the run loop in `mod.rs`.

The required reference-informed development note lives at
`crates/core-nodes/src/exporters/file_exporter/DEVELOPMENT.md`. It records the
pinned reference version, classifications above, unsupported behavior,
validation status, and any later divergence from this RFC.

The component registers an `ExporterFactory<OtapPdata>` in
`OTAP_EXPORTER_FACTORIES` and returns an `ExporterWrapper::local`. No new engine
trait, node category, or channel type is required.

### Configuration contract

The implementation uses a typed configuration with
`#[serde(deny_unknown_fields)]`. Its logical schema is:

<!-- markdownlint-disable MD013 -->

| Field | Type | Default | Validation and meaning |
| --- | --- | --- | --- |
| `path` | string | required | Absolute path containing `{signal}`, `{core_id}`, and `{generation}` exactly once. |
| `create_directories` | boolean | `false` | Create missing parents with restrictive permissions. |
| `format` | enum | `otlp_json` | Only `otlp_json` is accepted in v1. |
| `open_mode` | enum | `append` | `append`, `truncate`, or `create_new`. |
| `durability` | enum | `write` | `write` or `sync_data`. |
| `max_frame_bytes` | positive integer | `67108864` | Maximum encoded JSON frame size, including `\n`; maximum accepted value is 256 MiB. |
| `tail_recovery` | enum | `truncate_partial` | In append mode, `truncate_partial` or `fail`. |

<!-- markdownlint-enable MD013 -->

Configuration-load validation checks syntax and value relationships without
touching the filesystem. The factory then performs context-dependent validation
and path rendering with `PipelineContext`.

Errors name the exact field and condition. Examples include:

```text
file.path must be absolute
file.path must contain {signal} exactly once
file.path must contain {core_id} exactly once
file.path must contain {generation} exactly once
file.max_frame_bytes must be in the range 1..=268435456
file.tail_recovery is only valid with open_mode=append
```

### Path rendering and ownership

Only three template tokens exist:

- `{signal}` is replaced with `logs`, `metrics`, or `traces`;
- `{core_id}` is replaced with `PipelineContext::core_id()`; and
- `{generation}` is replaced with
  `PipelineContext::deployment_generation()`.

Core and generation values are decimal integers generated by the engine.
Unknown, repeated, or missing tokens are rejected. Environment expansion
remains the responsibility of the repository's normal configuration provider
and occurs before component validation.

Requiring all three tokens provides:

- exactly one OTLP signal type in each file;
- one writer per signal and core in the thread-per-core model;
- distinct files for old and candidate generations during rolling replacement;
- safe scale-up without changing a previously one-core path; and
- paths that cannot be influenced by telemetry attributes.

A process-local path lease registry rejects two live file exporter writers that
resolve to the same normalized path. The registry is used only when opening and
closing a signal writer; it is not on the data path. The exporter does not
promise exclusion against a different process. Operators must not point
multiple engine processes at the same physical file.

Path normalization is used only to detect duplicate ownership. It does not
rewrite the configured destination or use string-prefix checks as a security
boundary.

### OTLP JSON encoders

The canonical encoders belong in `otap_df_pdata`, not in the exporter. Add
bounded encoders under `crates/pdata/src/otlp/json/` for the existing
`LogsDataView`, `MetricsView`, and `TracesView` abstractions. Each encoder
serializes into a caller-provided buffer.

The exporter selects an existing zero-copy view from representation and signal:

<!-- markdownlint-disable MD013 -->

| Signal | `OtapPayload::OtlpBytes` | `OtapPayload::OtapArrowRecords` |
| --- | --- | --- |
| Logs | `RawLogsData` | `OtapLogsView` |
| Metrics | `RawMetricsData` | `OtapMetricsView` |
| Traces | `RawTraceData` | `OtapTracesView` |

<!-- markdownlint-enable MD013 -->

The view-based encoders avoid materializing intermediate protobuf objects or
converting OTAP records to protobuf bytes before producing JSON. They write
into one reusable buffer with a hard limit and return a typed limit error
before any file bytes are changed.

The encoders follow ProtoJSON special cases, including hexadecimal trace and
span IDs, base64 byte values, quoted 64-bit integers where required, enum
representation, lower-camel-case field names, and omission of default-valued
fields. They always emit valid UTF-8 and compact JSON.

These are shared pdata utilities so a future OTLP JSON receiver or another
exporter can use the same contract. The implementation adds all three encoders
before the exporter is advertised as supporting all OTAP signals.

### Execution and allocation model

Encoding happens on the local pipeline runtime. File operations use an async
file API backed by the runtime's blocking-I/O facility so a slow filesystem does
not synchronously block every task pinned to that core.

The run loop preserves arrival order and allows only one file operation in
flight across signals. It retains:

- the current `OtapPdata`;
- one reusable JSON buffer capped by `max_frame_bytes`; and
- up to three file handles with constant-size signal-indexed bookkeeping.

There is no writer task, flush ticker, LRU, or component-owned retry buffer.
The reusable buffer may retain up to `max_frame_bytes` after a large batch but
cannot grow beyond that configured limit.

Ordering is preserved within each resolved file. No ordering is promised
between signal files, cores, or deployment generations.

### Lazy writer startup and readiness

The component renders and validates all three paths before the node reports
ready, including proving that the normalized paths are distinct from one
another. It neither acquires their process-local leases nor creates unused
files. For each signal, the first payload opens and probes that signal's writer
before encoding or accepting a subsequent payload:

1. Acquire the process-local path lease.
2. Create parent directories when configured.
3. Open the target with the requested mode and restrictive creation
   permissions.
4. In append mode, validate or recover the final frame boundary.
5. Record the initial length.
6. Append `{}` plus `\n`, apply the requested durability, and truncate back to
   the initial length.
7. Mark the signal writer ready only after the probe and rollback succeed.

The probe is a valid empty top-level OTLP JSON object for every signal. If the
process stops between the probe write and rollback, later replay sees an empty
request rather than malformed bytes. A failed probe or rollback produces a
retryable NACK for the triggering pdata and closes the writer; the node remains
available for other signals.

Eagerly proving every signal destination at node startup would create empty
files for signals a pipeline never sends. Lazy proof preserves a truthful
per-signal readiness boundary without that filesystem side effect.

### Write transaction and failure handling

Before a write, the exporter records the signal file's starting length. It
encodes the complete frame before modifying the file, writes it, and optionally
calls `sync_data`.

If an I/O operation fails before ACK:

1. Attempt to truncate the signal file back to the recorded length.
2. If rollback succeeds, send a retryable NACK for the original pdata.
3. If rollback fails, send a retryable NACK that states file state is
   indeterminate, emit a failure event, and terminate the node.

The retryable NACK favors at-least-once delivery. If the operating system
completed a write but returned a later durability error, a retry can duplicate
the frame. The exporter never claims exactly-once semantics.

Permanent input errors do not touch a file and receive a permanent NACK. Empty
pdata receives an ACK without opening or writing its signal file.

<!-- markdownlint-disable MD013 -->

| Condition | Outcome |
| --- | --- |
| Signal other than logs, metrics, or traces | Permanent NACK; exporter continues. |
| Malformed OTLP protobuf bytes | Permanent NACK; exporter continues. |
| OTAP view or signal-specific JSON encoding failure | Permanent NACK; exporter continues. |
| Encoded frame exceeds `max_frame_bytes` | Permanent NACK with guidance to split upstream. |
| Signal writer open or probe fails | Retryable NACK; exporter continues and may retry opening on later pdata. |
| File write or `sync_data` fails and rollback succeeds | Retryable NACK; exporter continues without an internal retry. |
| File rollback fails | Retryable NACK, failure event, and fatal exporter error. |
| ACK routing fails after a completed write | Fatal exporter error; a later retry may duplicate the completed frame. |

<!-- markdownlint-enable MD013 -->

### Backpressure and retry

The exporter calls `ExporterInbox::recv()` only when it is ready to encode and
write another message. The bounded pdata channel therefore limits queued work
and propagates slow-disk pressure upstream.

The exporter sends retryable NACKs for recoverable I/O failures but does not
sleep or retry internally. Pipelines that need retry policy compose the retry
processor. Pipelines that need crash-persistent pending work compose the
durable-buffer processor.

This separation keeps the file node's memory bound independent of retry
duration and prevents a second, hidden queue from competing with engine
backpressure.

### Shutdown

The exporter inbox latches shutdown while it force-drains pdata already
buffered for an exporter. The file exporter loop continues normal writes and
ACK/NACK delivery until the inbox returns the latched shutdown message.

On shutdown, the exporter:

1. stops accepting new work through normal engine shutdown;
2. flushes every open signal file;
3. attempts `sync_data` for every open file within the supplied deadline;
4. closes the files and releases their path leases; and
5. returns terminal metric snapshots.

If the deadline is already expired, or final flush/sync cannot finish before
it, the node returns a timeout error. Previously delivered ACKs are not
reversed.

The file handles and buffer remain exclusively owned by the run loop, so no
background flush operation can race with close.

### Live reconfiguration

The component does not mutate writer configuration in response to
`NodeControlMsg::Config`. In-place changes to paths, format, mode, durability,
or limits would make output boundaries ambiguous.

Pipeline replacement uses the controller's normal serial rolling cutover:

1. the candidate generation resolves different files because its generation
   token changed;
2. the candidate validates all paths before reporting ready;
3. each signal writer is probed when that signal first arrives;
4. the old generation drains its own files; and
5. the old generation closes independently.

Files from both generations can overlap in wall-clock coverage. No ordering is
promised across signals, cores, or generations. A no-op configuration update
does not create a generation or file.

Changing only core allocation keeps the current deployment generation.
`{core_id}` gives added cores new signal files and lets removed cores drain
their files without colliding with unchanged cores.

### Security and privacy

The exporter writes full log, metric, and trace contents, including attributes,
resources, scopes, log bodies, metric values, span events, and links. Operators
must treat every destination as sensitive telemetry storage.

The first release applies these controls:

- telemetry-controlled values never participate in path construction;
- paths are absolute and contain only trusted configuration, a closed signal
  value, and numeric engine identifiers;
- only one writer is leased for a resolved path in the process;
- new Unix files and directories use restrictive modes;
- unknown configuration fields and path tokens are rejected;
- paths and raw error strings are not metric attributes;
- normal internal events do not include signal content or serialized frames;
  and
- the encoder has a hard memory bound.

The component does not claim a sandbox boundary. A trusted operator can
configure any absolute destination writable by the process, and filesystem
mounts, ACLs, quotas, encryption, and cross-process exclusion remain deployment
responsibilities.

### Telemetry and diagnostics

The component reuses `exporter.pdata.exports.messages` with the existing
bounded `signal=logs|metrics|traces` and `outcome=success|failure` attributes.

Its primary metric set is `exporter.file`:

<!-- markdownlint-disable MD013 -->

| Metric | Unit | Attributes | Description |
| --- | --- | --- | --- |
| `exporter.file.items` | `{item}` | `signal` | Log records, metric data points, or spans in frames successfully written before ACK routing. |
| `exporter.file.bytes` | `By` | `signal` | Frame bytes successfully written, including delimiters. |
| `exporter.file.write_failures` | `{failure}` | `signal`, `operation` | Open, write, sync, or rollback failures; `operation` is a closed enum. |
| `exporter.file.tail_recoveries` | `{recovery}` | `signal` | Startup repairs that truncated an incomplete final frame. |
| `exporter.file.tail_recovered_bytes` | `By` | `signal` | Bytes removed by successful tail recovery. |

<!-- markdownlint-enable MD013 -->

The metric set has no path attribute. Engine node, pipeline, core, and
generation identity comes from the registered entity context. `signal` and
`operation` are closed enums and do not create unbounded cardinality.

Events follow the repository's event conventions and use `otel_*` macros:

<!-- markdownlint-disable MD013 -->

| Event | Severity | When |
| --- | --- | --- |
| `otelcol.node.file.start` | info | Node startup completed; includes bounded mode and durability fields, not paths. |
| `otelcol.node.file.writer.start` | info | A signal writer passed its lazy readiness probe. |
| `otelcol.node.file.tail.recover` | warn | A partial final frame was truncated for a signal. |
| `otelcol.node.file.write.fail` | warn | A signal writer enters a failure state; repeated failures are consolidated. |
| `otelcol.node.file.rollback.fail` | error | A failed write could not be rolled back and the node will terminate. |
| `otelcol.node.file.stop` | info | Graceful shutdown completed. |

<!-- markdownlint-enable MD013 -->

Events may attach the bounded signal and operation enums. Exceptions use the
standard exception attributes. Pdata and JSON frames are never attached to
diagnostic events.

### Validation plan

Validation is scenario-based rather than a byte-for-byte reimplementation of
Go internals.

Configuration and startup coverage includes:

- defaults, unknown fields, enum values, and cross-field validation;
- absolute paths and exact validation of all three required tokens;
- deterministic `logs`, `metrics`, and `traces` path rendering;
- one-core, multi-core, generation replacement, and core resize rendering;
- duplicate resolved-path leases, including cross-signal collisions;
- missing and auto-created directories;
- read-only, full, and blocked destinations through fault injection; and
- append, truncate, and create-new behavior on Linux and Windows.

Format coverage includes:

- representative resources, scopes, attributes, timestamps, IDs, flags, and
  dropped counts for all three signals;
- all log `AnyValue` variants, every metric data point kind and temporality,
  and span events, links, status, and trace state;
- semantic equivalence for OTLP bytes and OTAP Arrow inputs;
- golden semantic comparison with the pinned Go file exporter;
- replay of each signal file through the Collector OTLP JSON File receiver;
- proof that no file contains more than one top-level signal type;
- empty pdata, malformed protobuf, and maximum-size boundaries; and
- fuzzing of all view-based JSON encoders and the tail-boundary scanner.

Runtime coverage includes:

- ACK only after the selected durability point;
- permanent and retryable NACK classification;
- lazy open and readiness independently for every signal;
- alternating logs, metrics, and traces through one exporter instance;
- short and partial writes with successful rollback;
- rollback, sync, ACK-routing, and close failures;
- bounded memory with a stalled writer;
- upstream backpressure while a write is pending;
- forced drain and shutdown deadline behavior; and
- overlapping old and candidate generations writing distinct files.

Performance validation measures items and bytes per second, allocation count,
maximum event-loop stall, and scratch-buffer retention for each signal and both
pdata representations. The Go exporter is a useful compatibility and throughput
reference, not a required performance target.

### Implementation sequence

The implementation should remain reviewable as separate pull requests:

1. Add typed config, factory registration, signal-aware path rendering, a no-op
   run loop, README, and `DEVELOPMENT.md`.
2. Add bounded logs, metrics, and traces ProtoJSON encoders to `otap_df_pdata`
   with semantic compatibility fixtures.
3. Add lazy signal writers, readiness probes, tail recovery, async writing, and
   ACK/NACK behavior.
4. Add component telemetry, fault-injection integration tests, Collector replay
   validation, and performance coverage.

The first implementation PR that changes user-visible behavior adds a Rust
changelog entry. This RFC itself is documentation-only and does not require
one.

## Drawbacks

OTLP JSON is larger and more CPU-intensive than protobuf or OTAP Arrow. The
bounded scratch buffer temporarily coexists with the input pdata, increasing
per-instance peak memory by up to `max_frame_bytes`.

Required signal, core, and generation tokens create several files instead of
the stable single path familiar to some Go file exporter users. Consumers must
read globs and must tolerate unordered files across signals, cores, and
generations.

Lazy writer readiness means the node can be globally ready before a destination
has been proven writable for a signal it has never received. The first pdata
for that signal receives a retryable NACK if the probe fails. Eagerly opening
all destinations would prove more at node startup but would create unused files.

The first release is not a complete telemetry archival solution. Without
integrated rotation, compression, or retention, operators must bound disk usage
outside the component.

`sync_data` can materially reduce throughput. Even with it, the exporter cannot
atomically combine a file write with an upstream ACK, so duplicates remain
possible.

## Rationale and alternatives

### Port the Go file exporter option-for-option

This would offer the easiest configuration migration, but it would also import
dynamic path cardinality, background flush state, legacy compression framing,
arbitrary encoders, and rotation behavior before their OTAP ownership and
failure semantics were settled. The larger initial surface would be harder to
validate and reverse.

This RFC instead preserves the Go component's name, three stable signals,
default interoperable format, and one-object-per-batch contract.

### Use separate filelog, filemetric, and filetrace components

Separate public components would duplicate an identical user outcome,
configuration, lifecycle, and framing policy. They would also diverge from the
reference and force users to learn three names. Signal-specific encoders remain
separate internally, while one `file` factory dispatches on `SignalType`.

### Allow all signals in one physical file

The JSON objects are distinguishable by their top-level fields, but mixed files
violate the OpenTelemetry Protocol File Exporter requirement that a file contain
exactly one data type. A required `{signal}` token makes the contract
unambiguous and replayable.

### Write one JSON object per record or data point

Per-record JSON is convenient for text tools, but it must duplicate resource
and scope data or invent a new envelope. It is not the Collector file
exporter's OTLP JSON contract and cannot be replayed as the same batch without
reassembly. Keeping one top-level data object per line preserves hierarchy and
compatibility.

### Write plain log bodies and human-readable metrics or spans

Signal-specific text renderings are useful for humans but discard information
or invent templates and aggregation rules. They should be explicit typed
formats with documented loss and framing, not the default meaning of `file`.

### Use protobuf first

For `OtlpProtoBytes`, protobuf could sometimes be written with little work, but
OTAP Arrow input still needs encoding and every frame needs a length boundary.
The Go format's four-byte prefix is prior art, but adopting it should be paired
with a Rust reader and explicit corruption recovery. JSON offers the more useful
first end-to-end scenario.

### Let all cores append to one file per signal

This avoids file globs but introduces shared synchronization on the hot path,
cross-platform append atomicity questions, partial-write rollback races, and
live-generation overlap. Per-runtime paths preserve the share-nothing model and
make recovery local.

### Open all three signal files at node startup

Eager opening proves every destination before global readiness, but most
pipelines carry one signal and would create two unused files per core and
generation. Validating all paths at startup and probing each writer before its
first payload keeps failures retryable and avoids unused filesystem artifacts.

### Use synchronous standard-library writes

The implementation would be small, but a slow filesystem operation could block
the pinned runtime and delay unrelated nodes on that core. Awaiting the
runtime's blocking-I/O facility preserves backpressure without synchronously
stalling the core.

### Put retry, buffering, and disk spooling in the exporter

That would duplicate pipeline processors and create hidden memory and disk
queues. Engine ACK/NACK routing lets operators choose retry and durability
policies once and observe them consistently.

### Do not add the component

Users can run a Go Collector file exporter downstream or use the Parquet
exporter. That adds another process or changes the output from replayable OTLP
JSON to an analytics schema. It does not provide a small native local capture
sink for all three stable OTAP signals.

## Prior art

The primary prior art is the OpenTelemetry Collector contrib
[File Exporter][go-fileexporter]. It establishes demand for local telemetry
files, supports logs, metrics, and traces, uses OTLP JSON as its default
interoperable format, writes one JSON data object per line, and composes its
consume functions with Collector exporter helpers.

The OpenTelemetry Protocol [File Exporter specification][file-exporter-spec]
defines the single-signal JSON Lines contract used by this RFC.

The Collector [OTLP JSON File receiver][otlp-json-file-receiver] demonstrates
the corresponding replay workflow for traces, metrics, and logs. Compatibility
with that receiver is an end-to-end acceptance criterion.

JSON Lines provides the recovery property used here: a complete line is an
independently parseable data object, embedded newlines are escaped, and an
incomplete crash tail can be detected at the final delimiter.

The existing OTAP Parquet exporter is useful prior art for filesystem-backed
exporter lifecycle and telemetry, but its partitioned analytical schema and
flush behavior serve a different user outcome.

## Unresolved questions

- Is `64 MiB` the right default for `max_frame_bytes`, or should the field be
  required until representative batches for all three signals establish a
  default?
- Should `{core_id}` and `{generation}` remain visibly required, or should the
  exporter append equivalent suffixes automatically while exposing the
  resolved path in startup diagnostics?
- Should `{signal}` remain a required visible token, or should the exporter
  insert the signal before the final extension when it is absent?
- Does compatibility require only semantic ProtoJSON equivalence and replay,
  as proposed, or is matching the Go writer's field order valuable enough to
  constrain the Rust encoders?
- Must bounded size rotation be part of the first experimental release, or is a
  filesystem quota sufficient for the deliberately bounded capture-and-replay
  scope?

The following topics are explicitly out of scope for resolution by this RFC:

- profile support;
- a generic encoding extension API;
- a durable file-backed queue or write-ahead log;
- file input discovery and checkpoints;
- dynamic attribute-to-path routing; and
- retention across hosts or object storage.

## Future possibilities

Later typed format contracts could add plain text logs, human-readable metric
or trace renderings, protobuf frames, or a structured per-record envelope.
Each format must define supported signals, framing, information loss, bounds,
and a matching reader before it is exposed.

Size and time rotation can be added after defining bounded directory scanning,
backup ownership across generations, crash recovery, and retention-failure
semantics. Native file-level zstd compression could then finalize a standard
stream at rotation and shutdown; legacy per-message compressed frames should
not be introduced.

A bounded partitioning processor could route declared attribute values to a
fixed set of file nodes. If dynamic paths are ever needed, the design should
include canonical containment, an allowlist or cardinality budget, total file
creation limits, writer lifetime synchronization, and telemetry for churn and
rejection.

A matching OTAP-native OTLP JSON file receiver would enable fully native
capture/replay tests and could share framing and tail-recovery utilities for
all three signals.

Profiles can be added when OTAP has a stable profile signal representation,
views, engine registration, and a top-level file format covered by the
OpenTelemetry specification.

Rotation and a receiver do not make a durable queue by themselves. Durable
handoff would need a separate segment manifest, checksums, fsync policy,
consumer checkpoints, and deletion only after downstream acknowledgement.

[design-guide]: ../docs/ai/reference-informed-otap-native-capability-design.md
[file-exporter-spec]: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/file-exporter.md
[go-beta-issue]: https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/41669
[go-cardinality-issue]: https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/49226
[go-compression-issue]: https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/49328
[go-fileexporter]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/fileexporter
[go-path-issue]: https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/49233
[go-pin]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/21196c805ba7091d0928434ec9ca145ed0386cab/exporter/fileexporter
[go-race-issue]: https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/49228
[go-rotation-issue]: https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/49899
[go-shutdown-issue]: https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/46871
[go-startup-issue]: https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/49192
[otlp-json-file-receiver]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/otlpjsonfilereceiver
