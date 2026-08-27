# Batch Processor

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `processor:batch` (`urn:otel:processor:batch`)
- Feature gate: Default
- Stability: Experimental

## Overview

The batch processor combines admitted pdata before forwarding it downstream.
It uses a codec native batcher when available and otherwise batches OTAP records.
It tracks ACK/NACK-sensitive request state across batch flushes, independently
of downstream representation changes.

## Getting Started

Configure format-specific sizing and the maximum time to hold pending data:

```yaml
type: processor:batch
config:
  max_batch_duration: 500ms
  format: preserve
  otap:
    min_size: 8192
    max_size: null
    sizer: items
  otlp:
    min_size: 1048576
    max_size: null
    sizer: bytes
```

## Configuration

```yaml
type: processor:batch
config:
  # Batch sizing for OTAP records (defaults are format-specific).
  otap:
    min_size: 8192      # Flush threshold; null disables size flushing.
    max_size: null     # Optional upper bound.
    sizer: items       # "requests", "items", or "bytes".

  # Batch sizing for OTLP bytes (defaults are format-specific).
  otlp:
    min_size: 1048576
    max_size: null
    sizer: bytes
    max_split_fragments: 65536  # Cap on fragments per oversize entry (OTLP only).
    max_split_overhead_bytes: 8388608  # Cap on duplicated wrapper bytes per oversize entry (OTLP only).
    max_split_fragments_per_flush: 65536  # Greedy per-flush split threshold (OTLP only).

  # Maximum time before flushing pending data (default: 200ms).
  max_batch_duration: 500ms

  # Pending request tracking limits.
  inbound_request_limit: 1024
  outbound_request_limit: 512

  # Output format: "otap", "otlp", or "preserve" (default: preserve).
  format: preserve
```

Each format object contains:

- `min_size`: non-zero flush threshold, or `null` to disable size flushing.
- `max_size`: optional non-zero upper bound, or `null`.
- `sizer`: one of `requests`, `items`, or `bytes`.
- `max_split_fragments` (OTLP bytes only): non-zero cap on how many fragments a
  single oversize resource entry may split into, or `null` for unbounded
  (default 65536, a power-of-two backstop). Splitting an entry that exceeds
  `max_size` re-encodes the resource/scope
  headers around each fragment, so a tiny `max_size` relative to one indivisible
  input could fan out into very many fragments. When the projected fragment
  count exceeds this budget the entry is emitted whole (best-effort, possibly
  exceeding `max_size`) and counted by the `split.budget.fallbacks` metric.
- `max_split_overhead_bytes` (OTLP bytes only): non-zero cap on how many
  duplicated wrapper bytes a single oversize resource entry may amplify into, or
  `null` for unbounded (default 8 MiB). Because each fragment re-encodes the
  resource/scope headers, a large header split across many records can amplify
  output far beyond the input even when the fragment count stays under
  `max_split_fragments`. The amplification is measured from the *actual* greedy
  packing (the emitted fragments' total bytes minus one whole encoding of the
  entry), not a per-record worst case, so many small records under a large
  header are not falsely collapsed. When it exceeds this budget the entry is
  emitted whole (best-effort, possibly exceeding `max_size`) and counted by the
  same `split.budget.fallbacks` metric; emission also aborts early once the
  running amplification passes the budget, bounding transient memory. This is
  *measured* from the actual packing as the entry is split -- unlike
  `max_split_fragments`, it is not projected up front.
- `max_split_fragments_per_flush` (OTLP bytes only): non-zero greedy threshold on
  the number of output batches a single flush may build from splitting, or `null`
  for unbounded (default 65536). The two budgets above bound each entry
  individually, but a flush containing many large entries builds its entire
  output vector in memory before anything is sent, so their combined split
  fan-out could still amplify into a very large allocation. Once a flush has
  produced this many output batches, any further oversize entry is emitted whole
  (best-effort, counted by `split.budget.fallbacks`) instead of split. This is a
  simple greedy running threshold on *split fan-out*, not a strict total-output
  cap. The total output can exceed the threshold in two ways. First, the entry
  that crosses the threshold may still add its full per-entry fan-out, so split
  amplification is bounded by roughly this threshold plus one entry's
  `max_split_fragments`. Second, every remaining oversize or indivisible entry is
  still emitted whole, and each such entry contributes at least one output batch;
  the threshold bounds only the *additional* split fan-out, not this mandatory
  output floor of (at least) one batch per remaining top-level entry. It does not
  look ahead over later entries and is independent of Ack/Nack outbound-slot
  accounting (which governs *sending*, not up-front allocation).

## Codec profiles and output selection

Existing `otap` and `otlp` profiles remain supported. The equivalent canonical
configuration uses `codecs.otap` and `codecs.otlp-bytes`:

```yaml
type: processor:batch
config:
  format: preserve
  codecs:
    otap:
      min_size: 8192
      sizer: items
    otlp-bytes:
      min_size: 262144
      sizer: bytes
  max_batch_duration: 200ms
```

`format` accepts `preserve` (the default), `otap`, or a registered codec name.
`otlp` remains an alias for `otlp-bytes`. Configuring a profile through both a
legacy field and its canonical name is an error, even if the values agree.

With `max_batch_duration: 0s`, an unconfigured extension profile uses its native
threshold as a split limit and flushes immediately. Explicit profiles still
require `max_size` and no `min_size`, as do the legacy OTLP and OTAP profiles.

Under `preserve`, native batching retains the input representation. If the
codec lacks native support for the requested sizing mode, item batching falls
back to OTAP and emits OTAP; it does not immediately re-encode to the input
format. This also permits decoder-only input codecs. An explicit output codec
requires an encoder and produces that format after batching. Byte limits require
native byte-batching support; they are not approximated by OTAP item counts.
`requests` sizing remains unsupported.

Unconfigured extensions use their declared native default profile, or the
configured OTAP profile when they have no native batcher. Per-codec overrides
use the same profile fields and must request supported capabilities. Adding a
compiled codec does not require a new storage branch in the processor.

Timers and inbound/outbound tracking limits apply to each resolved batching
buffer and signal. Equivalent OTAP fallback profiles share a buffer. The number
of buffers is bounded by compiled codec registrations and configuration, not by
arbitrary incoming names.

## Examples

Flush every incoming message, splitting above 8192 items:

```yaml
type: processor:batch
config:
  format: otap
  otap:
    min_size: null
    max_size: 8192
    sizer: items
  max_batch_duration: 0s
```

## Telemetry

These tables list telemetry emitted directly by this node. Common engine
runtime metric sets may also be attached by the pipeline telemetry policy.

### Metric Sets

#### `otap.processor.batch`

| Metric | Unit | Description |
| --- | --- | --- |
| `otap.processor.batch.consumed_batches_logs` | `{item}` | Total batches consumed for logs signal. |
| `otap.processor.batch.consumed_batches_metrics` | `{item}` | Total batches consumed for metrics signal. |
| `otap.processor.batch.consumed_batches_traces` | `{item}` | Total batches consumed for traces signal. |
| `otap.processor.batch.produced_batches_logs` | `{item}` | Total batches produced for logs signal. |
| `otap.processor.batch.produced_batches_metrics` | `{item}` | Total batches produced for metrics signal. |
| `otap.processor.batch.produced_batches_traces` | `{item}` | Total batches produced for traces signal. |
| `otap.processor.batch.flushes_size` | `{flush}` | Number of flushes triggered by size threshold (all signals) |
| `otap.processor.batch.flushes_timer` | `{flush}` | Number of flushes triggered by timer (all signals) |
| `otap.processor.batch.flush_pending_requests` | `{request}` | Number of input requests pending at flush time. |
| `otap.processor.batch.flush_pending_bytes` | `By` | Number of bytes pending at flush time when byte size is known. |
| `otap.processor.batch.flush_age_duration` | `ns` | Time from first pending input arrival to actual flush start. |
| `otap.processor.batch.flush_timer_lateness_duration` | `ns` | Delay between scheduled timer wakeup and actual timer flush start. |
| `otap.processor.batch.flush_output_batches` | `{batch}` | Number of output batches emitted by each flush. |
| `otap.processor.batch.flush_output_bytes` | `By` | Number of bytes emitted by each flush when byte size is known. |
| `otap.processor.batch.dropped_conversion` | `{msg}` | Number of messages dropped due to conversion failures. |
| `otap.processor.batch.batching_errors` | `{error}` | Number of batches for which errors encountered. |
| `otap.processor.batch.nacked_inbound_slots` | `{msg}` | Number of requests nacked due to inbound slot exhaustion. |
| `otap.processor.batch.nacked_outbound_slots` | `{msg}` | Number of requests nacked due to outbound slot exhaustion. |
| `otap.processor.batch.split_budget_fallbacks` | `{entry}` | Number of oversize resource entries emitted whole because splitting would have exceeded `max_split_fragments`, `max_split_overhead_bytes`, or the per-flush `max_split_fragments_per_flush` threshold. |

### Events

| Event | Severity | Description |
| --- | --- | --- |
| *None* | N/A | No node-specific events are emitted. |

## Limits

- `min_size` and `max_size`, when set, must be non-zero.
- `bytes` sizing depends on payload formats that can report encoded size.
- `max_batch_duration: 0s` disables time-based accumulation and flushes
  immediately.

## Related Docs

- [Configuration model](../../../../../docs/configuration-model.md)
- [Processor taxonomy](../../../../../docs/processors.md)
- [Core node catalog](../../../README.md)
