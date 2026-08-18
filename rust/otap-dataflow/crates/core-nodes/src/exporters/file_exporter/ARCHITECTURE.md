# File Exporter Architecture

<!-- markdownlint-disable MD013 -->

## Purpose and Audience

This document explains how the OTAP Dataflow file exporter is built, why its
boundaries differ from the Go Collector file exporter, and which invariants
future changes must preserve. It is intended for maintainers and contributors.
For configuration and operational guidance, see the [file exporter README](README.md).

The component is experimental and registered as `urn:otel:exporter:file`. It
writes logs, metrics, and traces as newline-delimited OTLP JSON or
length-prefixed OTLP protobuf for local capture, offline transfer, and replay.
It accepts both OTLP protobuf bytes and OTAP Arrow records. Optional rotation,
retention, and file-level compression provide a bounded lifecycle without
turning the exporter into a durable queue.

## Compatibility Contract

The [OpenTelemetry Protocol File Exporter specification][file-exporter-spec]
is the `otlp_json` serialization contract. Its status is Development, so
maintainers must recheck it before extending the on-disk format. The relevant
requirements are:

| Specification requirement | File exporter implementation |
| --- | --- |
| UTF-8 JSON Lines | The shared OTLP JSON serializer emits UTF-8 JSON. |
| Every line is a valid JSON value | A complete top-level OTLP data object is encoded before any file write. |
| Lines end with the byte `\n` | The framing adapter reserves and appends exactly one line-feed byte. |
| Data follows OTLP JSON encoding | Serialization is delegated to the shared view-based OTLP JSON module. |
| A file contains exactly one of `LogsData`, `MetricsData`, or `TracesData` | `{signal}` is mandatory and resolves to a different path for each signal. |
| No ordering guarantee | This implementation preserves arrival order within one resolved file but makes no cross-file ordering promise. |

The specification also discusses SDK environment variables, stdout, and
programmatic exporter configuration. Those provisions do not map directly to
an OTAP Dataflow node and are not part of this component's contract.

`otlp_proto` is an explicit extension to that specification. It follows the
Go Collector file exporter's framing: a four-byte unsigned big-endian payload
length followed by one serialized OTLP `Export*ServiceRequest`. The length does
not include the prefix. The aliases `json` and `proto` affect configuration
spelling only; telemetry emits the canonical `otlp_json` and `otlp_proto`
values.

## Goals and Non-goals

The current design optimizes for a small, bounded capture-and-replay sink:

- preserve the complete OTLP hierarchy for logs, metrics, and traces;
- write one independently parseable frame per non-empty input batch;
- bound encoding memory and writer count;
- preserve the engine's backpressure and ACK/NACK semantics;
- make file ownership deterministic across signals, cores, and deployment
  generations;
- define the durability point and crash-tail behavior explicitly; and
- keep paths, telemetry attributes, failure state, manifest size, retained
  files, cleanup work, and compression concurrency bounded.

It is intentionally not a durable queue, write-ahead log, telemetry archive,
general-purpose templating engine, or exactly-once sink. Retry, persistent
buffering, batching, and static routing are composed with pipeline components.
Filesystem quotas, encryption, archival handoff, and cross-process exclusion
remain operator responsibilities.

## Architecture Overview

```text
                         +--------------------------+
OtapPdata                | representation + signal  |
  OTLP bytes ----------->| JSON view or raw protobuf|
  OTAP Arrow records --->| JSON view or proto encoder|
                         +------------+-------------+
                                      |
                           bounded physical frame
                                      |
                         +------------+------------+
                         | local exporter run loop |
                         +------------+------------+
                                      |
                  +-------------------+-------------------+
                  v                   v                   v
             logs writer         metrics writer      traces writer
                  |                   |                   |
                  +--- active write / rollback / ACK-NACK+
                                      |
                           rotation manifest slots
                                      |
                        finalized file -> gzip/zstd
```

One local exporter instance owns the complete mutable state. Its run loop
receives one message, encodes it, performs any required active-file operation,
and routes completion before receiving the next message. Time rotation, age
retention, and completed compression jobs enter the same loop as bounded timer
work. Compression is the only background task and never owns pdata or ACK
state.

### Module Responsibilities

| Module | Responsibility |
| --- | --- |
| [`mod.rs`](mod.rs) | Factory registration, local run loop, representation dispatch, ACK/NACK routing, shutdown, and event emission. |
| [`config.rs`](config.rs) | Typed configuration, cross-field validation, path-token substitution, ownership-token isolation, and path collision checks. |
| [`encoding.rs`](encoding.rs) | Exporter-local size bounds, JSON Lines framing, and protobuf length-prefix framing. |
| [`writer.rs`](writer.rs) | Path leases, active-file lifecycle, format-aware tail recovery, transactional writes, lifecycle scheduling, and final synchronization. |
| [`rotation.rs`](rotation.rs) | Alternating manifests, rotation recovery, deterministic segment naming, count/age retention, and compression-state commits. |
| [`compression.rs`](compression.rs) | One streaming blocking gzip or zstd job with standard whole-file output. |
| [`metrics.rs`](metrics.rs) | Component-specific counters with closed signal, outcome, and operation attributes. |
| [`otap_df_pdata::otlp::json`](../../../../pdata/src/otlp/json/README.md) | Canonical view-based OTLP JSON serialization shared with other producers and consumers. |

Keep protocol serialization in `otap_df_pdata`. The exporter should contain
only file-specific framing, limits, lifecycle, and completion behavior. The
shared protobuf encoders remain in `otap_df_pdata`; gzip and zstd are file
container concerns and therefore remain local to this exporter.

## Construction and Runtime State

The factory performs these steps before the run loop starts:

1. Parse `FileExporterConfig` with unknown-field rejection.
2. Validate configuration syntax and field relationships without filesystem
   access.
3. Substitute the pipeline core ID and deployment generation into the three
   signal paths.
4. Verify that every ownership token changes the normalized destination and
   reject cross-signal collisions.
5. Register metric sets and allocate a small reusable frame buffer.

The instance retains:

- three `Option<SignalWriter>` slots indexed by the closed `SignalType` enum;
- one reusable physical-frame buffer, one bounded protobuf buffer, and three
  reusable signal-specific protobuf encoders;
- three booleans used to consolidate repeated failure events; and
- bounded metric sets keyed by closed enums.

At most one input pdata and one encoded frame are being processed by an
instance. The encoded length is limited by `max_frame_bytes`; allocators may
round reusable capacities above logical lengths. At most three active file
handles and three process-local path leases exist per instance. With rotation,
each writer has two manifest slots, at most `max_backups + 1` tracked finalized
segments during a transition, and at most one compression task. There is no
compression request queue.

## Data and Encoding Path

`OtapPdata` can carry either OTLP request bytes or OTAP Arrow records. JSON
selects a view without converting between those representations:

| Signal | OTLP bytes view | OTAP Arrow view |
| --- | --- | --- |
| Logs | `RawLogsData` | `OtapLogsView` |
| Metrics | `RawMetricsData` | `OtapMetricsView` |
| Traces | `RawTraceData` | `OtapTracesView` |

Each view implements the signal-specific pdata view trait consumed by the
shared JSON serializer. This keeps resource, scope, and record semantics in one
serializer and avoids a protobuf allocation on the OTAP-to-JSON path.

The framing adapter clears the reusable buffer, allows the serializer to write
at most `max_frame_bytes - 1` document bytes, and appends `\n` only after
successful serialization. A serialization or limit error clears every partial
byte. Consequently, invalid input and oversized frames are rejected before a
destination is opened or modified.

For `otlp_proto`, existing OTLP bytes are validated and framed without
re-encoding. OTAP Arrow records use the shared signal-specific protobuf
encoders and a bounded `ProtoBuffer`. The exporter prepends the four-byte
big-endian length only after complete encoding. Encoder mutations needed to
normalize transport-optimized IDs remain in the pdata returned through the
ACK/NACK path.

One non-empty input batch remains one top-level OTLP object and one physical
frame in either format. Do not flatten records or split a batch inside a file:
doing so would change hierarchy, replay behavior, and ACK granularity. Empty
pdata is ACKed without opening a signal file or writing a frame.

Field order and insignificant whitespace are not contractual. ProtoJSON
semantics such as lower-camel-case field names, quoted 64-bit integers, base64
bytes, hexadecimal trace and span IDs, numeric enums, and omitted default
values are contractual and belong in shared serializer tests.

For protobuf, the prefix and payload together count against
`max_frame_bytes`. Length-prefix byte order, request type per signal, and exact
preservation of valid input OTLP bytes are contractual.

## Paths and File Ownership

Every path template must be absolute and contain each of these tokens exactly
once:

- `{signal}` becomes `logs`, `metrics`, or `traces` and enforces the
  specification's single-data-type rule;
- `{core_id}` prevents several share-nothing pipeline runtimes from writing
  the same file; and
- `{generation}` isolates old and candidate deployments during rolling
  replacement.

These tokens are required even in a one-signal, one-core deployment. Optional
tokens would make a configuration unsafe after scaling or reconfiguration and
would require hidden suffix rules. Unknown or repeated tokens are rejected.
Telemetry values never participate in path construction.

Configuration-time lexical normalization verifies that changing each required
token changes the destination and catches collisions among the three rendered
signal paths. On first use, `PathLease` resolves the original filesystem path
or its closest existing ancestor before appending any missing suffix. This
preserves symlink and parent-component traversal order. The resulting
process-local lease prevents two writers in this process from owning the same
path and is released with the writer.

The lease is not a filesystem lock or security boundary. It does not exclude a
different process, and changes to symlinks after acquisition are outside its
contract. Cross-process coordination requires a separate design.

Rotation derives every owned lifecycle path from the leased active path. A
20-digit sequence suffix names finalized files; `.gz` or `.zst` follows that
suffix after successful compression. Two `.manifest.N.json` slots and one
codec-specific `.tmp` path are also deterministic. The manifest stores only
sequence numbers, timestamps, and closed compression states, never configured
paths or telemetry values.

The active path itself may be a symbolic link when rotation is disabled, as in
the original writer contract. Rotation rejects that final-component symlink:
renaming it would move the link rather than the opened target. Symlinked parent
directories still pass through canonical lease resolution.

When enabled, directory creation runs off the local runtime's synchronous hot
path. On Unix, new directories request mode `0700` and new files request mode
`0600`, subject to umask. Existing permissions are not changed.

## Writer Lifecycle

### Lazy Open and First Write

Writers are created independently on the first valid, non-empty batch for each
signal. Lazy open avoids creating two unused files in a single-signal pipeline.
It also means node readiness does not prove that an unused signal destination
can be opened or written.

Opening a writer follows this order:

1. Acquire the process-local path lease.
2. Optionally create parent directories.
3. Open with `append`, `truncate`, or `create_new` semantics.
4. In append mode, validate or repair the final frame boundary.
5. When rotation is enabled, load the newest valid manifest slot, reconcile a
   pending rename or compression commit, apply retention, and resume at most
   one pending compression job.

The writer is installed in the signal slot only when every step succeeds. The
exporter does not write a synthetic readiness frame into the destination. The
first actual frame exercises write, flush, and configured durability behavior
through the normal transactional path. A failed open receives a retryable
NACK, and a later batch may retry the lazy open.

### Append-tail Recovery

For `otlp_json`, append mode first checks whether an existing non-empty file
ends in `\n`. If not, `tail_recovery: fail` rejects it. `truncate_partial`
scans backward by at most `max_frame_bytes`, finds the last complete line
boundary, truncates the incomplete suffix, and synchronizes the repair.

For `otlp_proto`, recovery walks four-byte length prefixes from the beginning,
rejects any physical frame above `max_frame_bytes`, and tracks the last
complete boundary. A partial final prefix or payload can be truncated only
after at least one complete frame. Size rotation normally bounds this scan by
`max_bytes`; without rotation, protobuf append recovery is proportional to the
existing active file size.

If no boundary exists within that bound, opening fails without guessing. In
particular, the exporter does not silently erase a non-empty file containing
only an incomplete first frame. Recovery repairs only an incomplete tail; it
does not semantically validate older complete JSON or protobuf frames.

### Frame Write Transaction

Before writing, `SignalWriter` records the current file length. It then seeks
to the end, writes the complete frame, flushes pending file operations, and, for
`durability: sync_data`, synchronizes file data before success.

If write, flush, or synchronization fails, the writer attempts a compensating
rollback: truncate to the recorded length, seek to the new end, and
synchronize. This is not an atomic filesystem transaction. A successful write
followed by an unobserved ACK can still be replayed twice, and some filesystem
or device failures can make the final state unknowable.

### Rotation State Machine

Size rotation occurs before a frame would make a non-empty active file exceed
`max_bytes`. Configuration requires `max_bytes >= max_frame_bytes`, so one
valid frame always fits in an empty active file. Time rotation uses a monotonic
deadline during the process lifetime and is also serviced while input is idle.
The manifest persists the corresponding wall-clock start so a restart does not
silently reset the file age.

Rotation is a crash-recoverable state transition:

1. Reserve the next sequence and persist a pending segment in the manifest.
2. Flush and synchronize the active file, close it, and rename it to the
   reserved finalized path.
3. Create a new active file at the configured path.
4. Move the pending segment into the committed segment list and persist the
   manifest.
5. Apply retention and, when configured, start compression.

On restart, a pending record whose finalized path exists is committed. A
pending record whose path does not exist is cleared. The exporter never
guesses ownership from filename patterns. Any failure after the pending state
is persisted is fatal to the current writer; restart recovery must reconcile
it before more frames are accepted.

### Alternating Manifest Slots

Each signal writer owns two JSON manifest slots. Every update increments a
revision, includes a checksum of the state, writes and synchronizes the older
slot, and leaves the other valid slot as a fallback against torn writes.
Startup validates both slots and selects the highest valid revision.

The manifest is bounded to 1 MiB and 1001 decoded segment records. Runtime
configuration permits at most 1000 retained backups; the extra record covers
an in-progress transition. The state contains sequence numbers, timestamps,
and closed compression states, not arbitrary paths. A missing manifest starts
new ownership state; invalid existing slots cause open to fail rather than
triggering directory reconstruction.

### Retention

Count retention is mandatory whenever rotation is enabled and defaults to 10
finalized files. A value of zero removes a segment after it is safely
finalized. Optional age retention is serviced by the same idle run-loop timer
as time rotation. Both policies remain bounded by the manifest record limit
and perform no directory scan.

Pending compression temporarily protects its source from retention. Cleanup
deletes only paths derived from records in the active path's manifest, and a
cleanup error makes the lifecycle state fatal. Never broaden retention to a
glob or directory prefix: it could delete files owned by another core,
generation, exporter, or operator.

### Background File Compression

Compression is paired with rotation because only a finalized file can become
a portable whole-file gzip or zstd stream. Before work starts, the manifest
marks the segment pending. A blocking task streams the uncompressed source to
a same-directory temporary path, synchronizes and closes the output, then
renames it to the codec-specific final path. The run loop commits the complete
state to the manifest before deleting the source fallback.

Restart reconciliation removes only manifest-owned temporary output and
handles these durable states:

- pending plus final output commits the compressed representation;
- complete plus a remaining source retries source deletion; and
- complete without its output but with a source returns to pending.

Each signal writer owns at most one compression task and has no job queue.
Normal active-file writes may continue while that task runs, but the next
rotation, retention cleanup, or shutdown drains all manifest-tracked work.
This provides bounded concurrency and eventual backpressure. A worker or
manifest failure retains the source when possible and terminates the exporter
so startup recovery can resume from durable state.

### Shutdown

The engine drains pdata admitted to the exporter before delivering the latched
shutdown control message. The exporter then flushes and calls `sync_data` on
each open writer, drains any manifest-tracked compression, emits terminal
metric snapshots, and drops the writers and leases. All work shares the
supplied shutdown deadline.

There is no concurrent flush worker to race with close. If the shared deadline
expires, remaining writers may not be synchronized or fully compressed and
the exporter returns a shutdown error. The uncompressed finalized source is
the recovery fallback until a compressed representation is committed.
Previously issued ACKs are not revoked.

## Completion and Failure Semantics

| Condition | Completion and node behavior |
| --- | --- |
| Empty pdata | ACK; no encoding, open, or write. |
| Malformed input, serialization, or protobuf validation failure | Permanent NACK; no file modification; continue. |
| Frame exceeds `max_frame_bytes` | Permanent NACK with upstream-splitting guidance; continue. |
| Open or tail validation fails | Retryable NACK; leave writer unopened; continue. |
| Write or `sync_data` fails and rollback succeeds | Retryable NACK; continue. |
| Rollback fails | Retryable NACK, error event, and fatal exporter error because file state is indeterminate. |
| Rotation, retention, or manifest update fails | NACK the associated pdata when present, retain recoverable files, and terminate for startup recovery. |
| Background compression fails | Retain the uncompressed source and terminate for startup recovery. |
| ACK routing fails after a successful write | Fatal exporter error; a later replay may duplicate the frame. |
| Final synchronization or compression drain fails or times out | Shutdown error; prior ACKs remain valid at their configured durability level. |

The exporter does not retry or retain failed pdata. Retry policy belongs in the
retry processor; crash-persistent pending work belongs in the durable-buffer
processor. This avoids a hidden queue and keeps retained memory independent of
retry duration.

## Backpressure, Concurrency, and Ordering

The run loop does not receive another pdata while encoding, opening, writing,
performing required lifecycle work, or routing completion for the current one.
A slow filesystem therefore fills the bounded exporter inbox and propagates
pressure upstream.

Within one exporter instance, writes across all signals are serialized. Arrival
order is preserved within each resolved signal file. Separate cores and
deployment generations own separate files and run independently, so the
exporter promises no ordering across files or by telemetry timestamp.

Compression is the one bounded exception: it can overlap active writes, has
one task per signal writer, never owns pdata, and is drained by later lifecycle
work. Do not introduce a background active-file writer merely to increase
throughput. Any parallel design must retain bounded queues, define ordering
and shutdown ownership, propagate pressure, and preserve per-message ACK
durability. Prefer scaling through the engine's share-nothing cores and
upstream batching first.

## Security and Observability Boundaries

Files contain full telemetry bodies, attributes, resources, scopes, metric
values, events, and links. They must be treated as sensitive storage. The
component does not sandbox configured paths; an operator may target any
absolute location writable by the process.

Metrics use closed `signal`, `outcome`, and `operation` attributes. Paths,
serialized data, telemetry-derived strings, and raw payloads must not become
metric attributes. Events may report closed configuration values and bounded
error text but must not include destination paths or telemetry content. See the
[README telemetry tables](README.md#telemetry) for the public names.

Any new dynamic dimension must have a demonstrably finite value set. Any new
event emitted on the data path needs an explicit consolidation or sampling
policy to avoid an I/O failure becoming an event storm.

## Intentional Differences from the Go Exporter

The table compares this implementation with the pinned Go baseline. The Go
features are implementation choices unless the specification column above says
otherwise.

| Area | Go Collector file exporter | OTAP Dataflow file exporter | Rationale |
| --- | --- | --- | --- |
| Signal isolation | A configured exporter path is normally tied to a signal pipeline. | `{signal}` always creates distinct paths for logs, metrics, and traces. | Enforce the specification in a multi-signal OTAP node. |
| Runtime ownership | A path can be relative and does not encode the OTAP runtime identity. | Paths are absolute and require `{core_id}` and `{generation}`. | Preserve share-nothing cores and rolling-generation isolation. |
| Default open behavior | `append: false` truncates by default. | `append` is the default; `truncate` and `create_new` are explicit. | Avoid silently discarding a previous capture. |
| Output formats | JSON, length-prefixed protobuf, and encoding extensions are available. | Typed `otlp_json` and `otlp_proto` are available, with `json` and `proto` aliases. | Keep canonical OTAP names while accepting familiar Go configuration names. |
| Buffering | A buffered writer and `flush_interval` can defer writes. | One frame is awaited directly; there is no flush ticker. | Preserve direct backpressure and simple completion ownership. |
| Durability | Flush timing is configurable but ACK durability is not exposed as the same explicit contract. | `write` and `sync_data` define the pre-ACK durability point. | Make completion semantics reviewable and testable. |
| Write recovery | No equivalent bounded append-tail and per-frame rollback contract is exposed. | Append-tail repair is bounded; failed writes attempt rollback to the prior length. | Keep JSON Lines replayable after common interruption and I/O failures. |
| Writer readiness | Writer lifecycle follows the Go component startup model. | Each valid signal destination is opened on first use; the first actual frame exercises the write path. | Avoid unused and synthetic frames while failing the triggering batch if the destination is not writable. |
| Rotation and retention | Optional rotation and backup cleanup use the Go component's lifecycle model. | Size/time rotation and count/age retention use an alternating ownership manifest. | Avoid directory discovery and make interrupted lifecycle transitions recoverable. |
| Compression | zstd support includes historical per-message framing and native file-level behavior. | Rotation can produce standard whole-file gzip or zstd streams in a bounded background task. | Keep active writes uncompressed and preserve a manifest-tracked source until compression commits. |
| Dynamic grouping | Resource attributes can select paths, with an LRU bounded by `max_open_files`. | Telemetry cannot influence paths; each instance has at most three writers. | Avoid path injection, cardinality growth, churn, and hot-path synchronization. |
| Directory permissions | Directory mode is configurable and defaults to `0755`. | New Unix directories request `0700`; files request `0600`. | Default to private storage for full telemetry. |
| Profiles | Profiles are under development in the Go component. | Not yet supported. | Wait for stable OTAP profile views and a specified top-level file representation. |
| Retry and persistent buffering | Collector exporter helpers compose queue and retry behavior. | Engine ACK/NACK, retry, and durable-buffer components are composed explicitly. | Keep policy and retained work visible at the pipeline level. |

These differences are deliberate, not a backlog to reach option parity. A
proposal may change one, but it must justify the new behavior against protocol
compatibility, bounded resources, ownership, backpressure, and failure
semantics.

## Design Principles for Future Contributions

Every extension should satisfy these principles before configuration or code
is added:

1. Define the on-disk contract first. State framing, signal coverage,
   information loss, versioning, maximum frame size, crash-tail detection, and
   the reader that proves interoperability.
2. Keep all resources bounded. Specify memory, open files, queued frames,
   directories, retained backups, scan work, and attribute cardinality.
3. Preserve one clear owner. Identify which core and generation creates,
   writes, rotates, repairs, finalizes, and deletes every file.
4. Define completion precisely. State when ACK occurs, which failures are
   permanent or retryable, what rollback can restore, and when the node must
   terminate.
5. Preserve engine backpressure. Do not add hidden queues, sleeps, or retry
   loops that compete with pipeline policies.
6. Keep untrusted telemetry out of paths and instrumentation by default.
7. Prefer typed formats and options over arbitrary extension hooks.
8. Keep shared protocol logic out of the exporter. Serializers and parsers that
   are useful to both readers and writers belong in pdata.
9. Treat live configuration changes as generation replacement. Do not mutate
   the format, path, or lifecycle policy of an open writer in place.
10. Test semantic compatibility and failure boundaries, not JSON field order or
    incidental implementation details.

## Future Development

### Lifecycle Evolution

The rotation manifest, deterministic filenames, and active-file lease form one
ownership protocol. New lifecycle features such as archival handoff, upload,
or deletion acknowledgements must extend that state machine; they must not
infer ownership through directory scans. Preserve the two-slot recovery
property, fixed manifest bounds, complete-frame rotation boundary, and the
rule that only manifest-listed paths may be deleted.

Increasing retention beyond the current hard limit requires a new bound and a
review of manifest write amplification. Calendar schedules or wall-clock
alignment must define behavior for clock jumps while retaining monotonic
deadlines in-process. External filesystem quotas remain necessary even with
bounded backup counts.

Configurable compression levels must remain typed and bounded. Adding another
codec requires a standard whole-file stream, a conventional suffix, portable
rename behavior, restart reconciliation for each durable state, and reader
interoperability. Do not expose streaming active-file compression: it makes
frame rollback, rotation, and crash-tail recovery substantially weaker. Do not
claim JSON Lines or protobuf compatibility until a compressed file is
decompressed.

### Additional Formats

The existing protobuf format's big-endian length prefix, maximum length,
signal-specific request types, and append recovery are part of its on-disk
contract. Any incompatible framing change needs a new typed format name and a
matching reader; it must not silently change `otlp_proto`.

Human-readable logs, metrics, traces, templates, or per-record envelopes must
also be explicit typed formats. Their documentation must state which OTLP
fields are lost, how resource and scope context is represented, and whether
replay is possible. They must not silently change the meaning of `otlp_json`.

### Profiles

Add profiles only after OTAP has a stable signal representation, view traits,
engine registration, shared serialization, and a top-level file contract. The
signal must receive its own path value and preserve the one-data-type-per-file
invariant.

### Partitioning and Dynamic Paths

Prefer a bounded routing processor plus explicitly declared file exporter
nodes. If exporter-owned partitioning is still necessary, require a finite
allowlist or hard cardinality budget, canonical containment, total creation and
open-file limits, safe eviction with in-flight writes, bounded directory
creation, and rejection/churn telemetry. Resource values must not become raw
path segments. An LRU limit alone does not bound the number of files created or
the security surface.

### Receiver and Replay Support

An OTAP-native OTLP JSON file receiver would strengthen end-to-end validation.
Share parsing and framing utilities where contracts truly match, but keep
reader checkpoints and discovery separate from exporter write state. A reader
that globs files across signals, cores, and generations must not infer a global
order that the exporter does not promise.

### Cross-process Ownership

If multiple processes must coordinate, replace the process-local assumption
with an explicit portable lock or manifest protocol. Define stale-owner
recovery, host and process identity, atomic acquisition, filesystem support,
and behavior on network filesystems. Do not weaken required path tokens as a
substitute for real cross-process exclusion.

### Performance Work

Optimize only after preserving the completion and ownership model. Benchmarks
should cover logs, metrics, and traces; OTLP bytes and OTAP Arrow views;
representative small and large batches; `write` and `sync_data`; and 1, 2, and
4 cores. Track items and bytes per second, CPU, allocations, retained frame
capacity, and event-loop stalls. Throughput gains that add unbounded buffering
or weaken ACK semantics are not acceptable.

## Validation Expectations

Current unit and component coverage includes configuration defaults and
relationships, path rendering and collisions, exact frame bounds,
representative JSON and protobuf for all signals and input representations,
open modes, format-aware append repair, directory creation, process-local
leases, empty and invalid pdata, multi-signal output, size and idle time
rotation, manifest recovery and bounds, count and age retention, standard gzip
and zstd streams, idle compression completion, and metric schema.

Before promotion beyond experimental stability, add or maintain:

- replay fixtures through compatible JSON and protobuf readers;
- semantic golden coverage against the pinned or deliberately updated Go
  reference;
- fault-injected short writes, synchronization failures, rollback failures,
  and shutdown deadlines;
- fuzzing for view-based serialization, protobuf framing, manifest decoding,
  and tail-boundary recovery;
- bounded-memory and upstream-backpressure tests with a stalled writer;
- overlapping generation and core-resize scenarios; and
- repeatable allocation and throughput benchmarks.

When changing the format or lifecycle, tests must cover both input
representations and all supported signals. When changing failure handling,
tests must assert the ACK/NACK classification, file bytes, node continuation or
termination, and emitted telemetry.

## Maintainer Checklist

Before accepting a material change, verify:

- Does every resulting file still obey its documented framing and contain one
  signal type?
- Are memory, file descriptors, path cardinality, retained files, and cleanup
  work bounded?
- Is file ownership unambiguous for every signal, core, and generation?
- Is the ACK durability point unchanged or explicitly documented as a user
  contract change?
- Can every partial failure leave only a documented and recoverable state?
- Does a slow filesystem still propagate backpressure upstream?
- Are retry, buffering, and routing composed instead of duplicated?
- Can telemetry or error data expose sensitive paths or create unbounded
  metric/event cardinality?
- Are serializer changes made and tested in pdata rather than duplicated here?
- Are README, architecture, configuration examples, telemetry schema, tests,
  and changelog updated together when user-facing behavior changes?

## References

- [Original OTAP-native file exporter RFC][file-exporter-rfc]
- [OpenTelemetry Protocol File Exporter specification][file-exporter-spec]
- [OTLP JSON protobuf encoding][otlp-json]
- [Pinned Go Collector file exporter baseline][go-pin]
- [Current Go Collector file exporter][go-fileexporter]
- [Collector OTLP JSON File receiver][otlp-json-file-receiver]

[file-exporter-rfc]: https://github.com/lquerel/otel-arrow/blob/rfc-filelog-exporter/rust/otap-dataflow/rfcs/0000-file-exporter.md
[file-exporter-spec]: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/file-exporter.md
[go-fileexporter]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/fileexporter
[go-pin]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/21196c805ba7091d0928434ec9ca145ed0386cab/exporter/fileexporter
[otlp-json]: https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding
[otlp-json-file-receiver]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/otlpjsonfilereceiver

<!-- markdownlint-enable MD013 -->
