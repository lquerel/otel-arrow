# File Exporter

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `exporter:file` (`urn:otel:exporter:file`)
- Feature gate: Default
- Stability: Experimental

## Overview

The file exporter captures logs, metrics, and traces in replayable OTLP files.
It accepts OTLP protobuf bytes and OTAP Arrow records and supports two output
formats:

- `otlp_json` writes one compact OTLP ProtoJSON object plus `\n` per input
  batch. `json` is an alias.
- `otlp_proto` writes a four-byte unsigned big-endian length followed by one
  OTLP protobuf export request per input batch. `proto` is an alias.

Every physical file contains one signal type. Required path tokens keep files
exclusive to one signal, core, and deployment generation.

## Getting Started

```yaml
type: exporter:file
config:
  path: "/var/log/otel/telemetry-{signal}-{core_id}-{generation}.jsonl"
```

The parent directory must already exist unless `create_directories` is true.
For logs on core 3 in deployment generation 7, the example resolves to
`/var/log/otel/telemetry-logs-3-7.jsonl`.

This example adds bounded rotation, retention, and gzip compression:

```yaml
type: exporter:file
config:
  path: "/var/log/otel/telemetry-{signal}-{core_id}-{generation}.jsonl"
  max_frame_bytes: 8388608
  compression: gzip
  rotation:
    max_bytes: 134217728
    max_duration: 1h
    retention:
      max_backups: 24
      max_age: 168h
```

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `path` | Required | Absolute template containing `{signal}`, `{core_id}`, and `{generation}` exactly once. |
| `create_directories` | `false` | Create missing parent directories. |
| `format` | `otlp_json` | `otlp_json` (`json`) or `otlp_proto` (`proto`). |
| `open_mode` | `append` | First-open behavior: `append`, `truncate`, or `create_new`. |
| `durability` | `write` | ACK after `write`, or after `sync_data`. |
| `max_frame_bytes` | `67108864` | Maximum frame size including newline or length prefix; range 1 through 268435456. |
| `tail_recovery` | `truncate_partial` | Append-mode handling: `truncate_partial` or `fail`. |
| `rotation` | Disabled | Size/time triggers and bounded retention for finalized files. |
| `compression` | Disabled | `gzip` or `zstd` file-level compression; requires `rotation`. |

`rotation` has these fields:

| Field | Default | Description |
| --- | --- | --- |
| `max_bytes` | Disabled | Rotate before a frame would make a non-empty active file exceed this byte count. Must be at least `max_frame_bytes`. |
| `max_duration` | Disabled | Rotate a non-empty active file after this human-readable duration. |
| `retention.max_backups` | `10` | Finalized files retained per signal writer; range 0 through 1000. |
| `retention.max_age` | Disabled | Also delete finalized files older than this human-readable duration. |

Rotation requires `max_bytes`, `max_duration`, or both. Durations use values
such as `500ms`, `15m`, `24h`, or `168h`. Unknown fields and invalid field
relationships are rejected before filesystem access.

### Open modes and tail recovery

- `append` retains complete frames. `truncate_partial` removes only an
  incomplete final frame; `fail` rejects the file instead.
- `truncate` explicitly discards the active file contents on first open.
- `create_new` rejects an active path that already exists.

JSON recovery searches backward by at most `max_frame_bytes` for a newline.
Protobuf recovery walks validated length prefixes and truncates only after a
proven complete frame. Neither format erases a non-empty file when no complete
boundary can be established.

Unused signal files are not created. A signal writer and its lifecycle state
are opened when the first valid non-empty batch for that signal arrives.

### Rotation and retention

The configured path is always the active, uncompressed file. Rotation occurs
only between complete frames. Finalized files append a zero-padded sequence:

```text
telemetry-logs-3-7.jsonl.00000000000000000000
telemetry-logs-3-7.jsonl.00000000000000000001
```

Two checksum-protected manifest slots sit beside the active file:

```text
telemetry-logs-3-7.jsonl.manifest.0.json
telemetry-logs-3-7.jsonl.manifest.1.json
```

The alternating slots record sequence ownership, an interrupted rotation, and
compression state. Startup selects the newest valid slot and repairs only
manifest-owned state. Retention never discovers files with a directory scan
and never deletes an unlisted file. Count retention is always enabled when
rotation is enabled; age retention is serviced even while telemetry is idle.

Do not edit or copy manifests between active paths. If both slots are missing
or invalid, the exporter will not claim existing finalized files.

### Background compression

Compression applies to finalized files only. Completed gzip and zstd paths add
the conventional suffix:

```text
telemetry-logs-3-7.jsonl.00000000000000000000.gz
telemetry-logs-3-7.jsonl.00000000000000000001.zst
```

Each signal writer has at most one blocking compression job and no in-memory
job queue. Normal writes can continue while that job runs. A later rotation,
age cleanup, or graceful shutdown waits for outstanding compression and thus
propagates backpressure. The uncompressed source remains available until the
manifest durably records the completed compressed representation.

Outputs are ordinary whole-file gzip or zstd streams, not compressed
per-message frames. Decompress first, then consume the resulting JSON Lines or
length-prefixed protobuf file with a compatible reader.

### Durability and failures

`write` acknowledges after the complete active-file frame is accepted and
flushed by the operating system. `sync_data` additionally synchronizes active
file data before ACK. Rotation metadata and finalized files use their own
synchronization protocol regardless of ACK durability.

A failed frame write is truncated back to its previous length. A rollback or
lifecycle-state failure NACKs the current batch when applicable and terminates
the node because continuing could corrupt ownership. A background compression
failure keeps the uncompressed source and terminates the node for restart
recovery. Graceful shutdown synchronizes active files and drains compression
within the pipeline deadline.

The exporter has no telemetry retry queue. Compose the retry processor for
redelivery policy and the durable-buffer processor for crash-persistent
pending work.

## Output Contracts

| Signal | `{signal}` | OTLP JSON repeated field | OTLP protobuf message |
| --- | --- | --- | --- |
| Logs | `logs` | `resourceLogs` | `ExportLogsServiceRequest` |
| Metrics | `metrics` | `resourceMetrics` | `ExportMetricsServiceRequest` |
| Traces | `traces` | `resourceSpans` | `ExportTraceServiceRequest` |

OTLP JSON follows ProtoJSON rules, including quoted 64-bit integers,
hexadecimal trace and span IDs, base64 bytes, numeric enums, lower-camel-case
field names, and omission of default values. Field ordering and insignificant
whitespace are not contractual.

For `otlp_proto`, each physical frame is:

```text
+--------------------------+-------------------------------------+
| payload length (4B, BE)  | serialized OTLP export request      |
+--------------------------+-------------------------------------+
```

The length excludes the four-byte prefix. This matches the framing used by the
Go Collector file exporter.

## Security and Operations

Files contain full telemetry and must be treated as sensitive storage. On
Unix, newly created files request mode `0600` and directories request `0700`,
subject to umask. Existing permissions are unchanged.

Telemetry attributes never affect paths. A process-local lease prevents two
live exporter writers from owning the same normalized active path, but it does
not coordinate separate processes. With rotation enabled, the active path
itself cannot be a symbolic link; symlinked parent directories remain subject
to the normal ownership checks.

Do not externally rename, replace, rotate, compress, or delete active,
finalized, temporary, or manifest files while the exporter runs. Retention
bounds exporter-owned finalized files, but operators should still use a
filesystem quota for protection against other files, failed processes, or
manual copies. Encryption and cross-process exclusion remain operator
responsibilities.

## Telemetry

### Metric sets

| Metric | Unit | Attributes | Description |
| --- | --- | --- | --- |
| `exporter.file.exports.messages` | `{message}` | `signal`, `outcome` | Telemetry messages whose file export reached a terminal outcome. |
| `exporter.file.items` | `{item}` | `signal` | Signal items in successfully written frames. |
| `exporter.file.bytes` | `By` | `signal` | Successfully written active-file frame bytes including framing. |
| `exporter.file.rotations` | `{rotation}` | `signal` | Active files finalized by rotation. |
| `exporter.file.compressions` | `{file}` | `signal` | Compressed files committed to the manifest. |
| `exporter.file.failures` | `{failure}` | `signal`, `operation` | Failures for `open`, `write`, `sync`, `rollback`, `rotate`, or `compress`. |
| `exporter.file.tail_recoveries` | `{recovery}` | `signal` | Incomplete final frames repaired at open. |
| `exporter.file.tail_recovered_bytes` | `By` | `signal` | Bytes removed by successful tail repair. |

No metric contains a destination path.

### Events

| Event | Severity | Attributes | Description |
| --- | --- | --- | --- |
| `otelcol.node.file.start` | `info` | `format`, `compression`, `create_directories`, `open_mode`, `durability`, `tail_recovery`, `max_frame_bytes` | Exporter startup with bounded non-sensitive configuration. |
| `otelcol.node.file.writer.start` | `info` | `signal` | A signal writer opened successfully on first use. |
| `otelcol.node.file.rotate` | `info` | `signal` | An active file was finalized. |
| `otelcol.node.file.compress` | `info` | `signal`, `files` | Background compression results were committed. |
| `otelcol.node.file.tail.recover` | `warn` | `signal`, `recovered_bytes` | An incomplete final frame was removed. |
| `otelcol.node.file.operation.fail` | `warn` | `signal`, `operation`, `error` | A signal writer entered a recoverable I/O failure state. |
| `otelcol.node.file.rollback.fail` | `error` | `signal`, `operation`, `error`, `rollback_error` | Frame rollback failed and the node will terminate. |
| `otelcol.node.file.lifecycle.fail` | `error` | `signal`, `operation`, `error`, `fatal_error` | Lifecycle recovery requires an exporter restart. |
| `otelcol.node.file.stop` | `info` | `reason` | Graceful shutdown completed. |

## Limits

Profiles, plain-text templates, attribute-derived paths, configurable
compression levels, an internal retry queue, and exactly-once delivery are not
yet supported. A completed frame may be replayed twice if its write succeeded
but its ACK was not observed before a crash.

## Related Docs

- [Architecture and design decisions](ARCHITECTURE.md)
- [Example configuration](../../../../../configs/trafficgen-file.yaml)
- [Configuration model](../../../../../docs/configuration-model.md)
- [Core node catalog](../../../README.md)

<!-- markdownlint-enable MD013 -->
