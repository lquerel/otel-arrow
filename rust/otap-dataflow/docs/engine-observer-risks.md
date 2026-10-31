# Engine Observer Risk Analysis

## Assessment scope

This analysis describes the engine observer foundation.
Mitigations below are proposed work unless explicitly listed as current
safeguards. See the [architecture overview](engine-observer-architecture.md)
and the separate [feedback proposal](engine-observer-feedback.md).

The primary risk is that observing an overloaded engine makes its situation
worse. Native observers share the engine's address space, CPU resources,
allocator, logging delivery, and lifecycle. Read-only access and a bounded
input stream help, but they do not impose hard bounds on arbitrary extension
code.

## Current safeguards

- One shared overwrite ring bounds queued input retention, with bounded
  payload and metadata-work admission.
- Immutable `Arc` records share payloads across independent cursors.
- A slow consumer receives explicit lag; it does not make producers wait
  for consumption or remove a fast peer's unread records prematurely.
- Oversized inputs generate explicit rejection markers.
- Metric observation does not drain or reset normal export state.
- Native keys carry a registry lifetime identity, and metric/log records
  retain available entity definitions.
- Observer contexts expose no control-plane commands or mutable registry.
- The wrapper contains returned runtime errors and task unwinding panics.
- Observer output events are excluded from native observation inputs.

These safeguards do not make publication lock-free, make observations
lossless, or bound process RSS and observer-owned CPU/memory usage.

## Risks and mitigations

| Risk | Current exposure | Mitigation direction |
| --- | --- | --- |
| Extra latency and contention | Capture resolves metadata, performs bounded traversal/copying, allocates, and takes shared locks. State snapshots clone under a mutex. | Minimize synchronous capture, cache bounded definitions, use preallocated capture buffers and publication that drops rather than waits under contention, and expose immutable state views or byte/entry-bounded reads. |
| CPU and memory exhaustion | Observer-owned histories, retained `Arc` frames, and arbitrary allocations can exceed input-retention limits. A task can run without yielding. | Bound histories, retained bytes, work per turn, and output rates; supervise progress and CPU placement. Hard containment of arbitrary native allocations/work requires a separately resource-limited process or an enforced sandbox. |
| Startup and shutdown hangs | Synchronous preparation can block startup. Cancellation is cooperative and thread joins have no deadline. | Supervise optional initialization, make startup failure policy explicit, detect loss of progress, and bound engine waiting during shutdown. A deadline cannot safely terminate or reclaim a stuck native thread. |
| Process-wide failure | Runtime task error/panic handling does not contain aborts, allocation failure, memory corruption, or every initialization/destructor failure path. | Use trusted native extensions; place less-trusted analysis in an external host when stronger containment is required. |
| Telemetry amplification | Observer events use the normal logging/ITS delivery path, which can wait up to a configured logging timeout. Output can compete with essential telemetry. | Give observer output its own bounded nonblocking budget, rate-limit and coalesce repeated events, count suppression, and preserve capacity for essential engine telemetry. |
| Incorrect conclusions | Lag, rejection, filtered logs, missing definitions, and non-atomic metric/event/state views can create false findings or recovery reports. Logs can evict other inputs from the shared ring. | Track completeness and freshness, expose unknown/stale states, validate epochs/generations, and resynchronize where possible. Consider capacity reservations by signal class and preserve metric semantics when aggregating. |
| Sensitive data exposure | Engine-wide logs and entity attributes can contain sensitive or cross-pipeline information. A restricted Rust context is not an OS security boundary. | Scope subscriptions and redact before handoff. Native code remains trusted; external hosting also needs deliberate permissions and data scopes. |
| Future control instability | Automatic feedback could apply stale, conflicting, repeated, or oscillating changes. This actuation path is not implemented. | Keep actuation disabled initially; introduce controller-owned policy, expiry/generation checks, idempotency, bounded pending actions, rate limits, hysteresis, and cooldowns. Validate in shadow mode first. |

## Concrete implementation boundaries

### Capture work can affect producers

[`ObservedEventReporter::log`](../crates/telemetry/src/event.rs) calls the
observation hook synchronously before normal log delivery.
[`TelemetryRegistryHandle::observe_log`](../crates/telemetry/src/registry.rs)
resolves context under the registry mutex and copies the bounded encoded log
body. Metric admission performs metadata work under that mutex too.
Publication also takes the hub's sequence mutex before broadcast.

No observer callback executes in these paths, and publication never waits
for a reader to consume records. Mutex contention, allocation, copying, and
last-reference destruction can still take time. A nonblocking-consumer
contract is not an end-to-end latency guarantee.

Moving metadata work away from pipeline logging threads is a priority. Any
definition cache or capture-buffer pool added to do that must be bounded,
respect producer retirement and key generations, and report admission loss.

### Read-only state can still delay state updates

[`ObservedStateHandle::snapshot`](../crates/state/src/store.rs) clones the
current pipeline map while holding its mutex. Frequent or large reads cost
CPU/memory and can delay state application. Prefer immutable published views
or bounded read APIs, with explicit freshness, rather than unrestricted
polling of full snapshots.

### Queue limits do not bound extension-owned memory

The default 16 MiB sum of input-payload admission budgets covers queued
records. It does not cap observer histories, extension-owned allocations, thread
stacks, or received records retained after reading. Keeping an `Arc` extends
the payload and definition lifetimes beyond the ring's retention window.

Budget-aware collections and retention accounting help cooperative native
observers. They cannot prevent arbitrary native code from allocating through
other paths. An execution-time budget also cannot preempt a task that never
returns control to its executor.

### Cancellation does not guarantee termination

[`ThreadLocalTaskHandle`](../crates/controller/src/thread_task.rs) cancels its
token and joins the OS thread, including during drop. A non-cooperative task
can therefore stall teardown. The synchronous observer preparation function
also runs before engine threads start.

Startup/readiness deadlines, independent progress supervision, and bounded
waiting can protect engine lifecycle progress. Detaching a stuck thread does
not reclaim its CPU, memory, or references. Forceful reclamation needs a
terminable execution boundary, such as a separate process.

### Filtering direct echoes does not eliminate load-driven loops

Excluding the reserved observer-output target prevents direct event echo.
It does not prevent output load from congesting ITS, changing its metrics,
and causing more findings and output. Such a loop can amplify pressure even
when no observer consumes its own named events.

Output quotas, suppression/coalescing, and independent health reporting
should remain effective when ITS is stalled. Proposed control coordination
must use its own bounded contract rather than relying on successful log
export.

### Loss changes what can be concluded

With bounded memory and capture that never backpressures the engine,
arbitrary overload eventually requires dropping or aggregating observations.
This tradeoff must be explicit to consumers and controller policies.

Keeping the latest value is appropriate for some gauges. Counter deltas and
distributions need semantics-preserving accumulation or explicit loss
reporting; later delta snapshots cannot reconstruct missing contributions.
An observed-state snapshot also does not restore missing metric deltas.
Missing evidence must not be interpreted as no traffic, no error, or recovery.

## Stronger containment with an external host

A possible future architecture retains a small, trusted, bounded observation
bridge in DFE and runs analysis in a separate process with enforced CPU and
memory limits. A supervisor can terminate and restart a stuck external
observer without waiting for cooperative cancellation in the engine.

Compact batches and engine IDs can limit transport overhead. Native Rust
pointers and `Arc` ownership cannot simply be passed across a process
boundary: IPC needs a defined representation, or a carefully bounded shared
memory protocol with explicit ownership and recovery.

External hosting adds transport cost and another component to manage. It
does not remove capture overhead, missing evidence, or contention for host
resources by itself. CPU/memory isolation and any output/storage budgets must
be enforced independently. A sandboxed host would likewise need bounded host
calls as well as enforced memory/execution budgets.

## Evidence required before production adoption

Prioritize capture-path contention and state-read costs, supervised lifecycle
handling, independently bounded output, and measurable observer budgets.
Then compare no observers, one observer, and the configured maximum under:

- Normal traffic and bursts of logs/metric snapshots.
- Large topologies and rapid entity/metric-set creation and retirement.
- Slow, stalled, CPU-heavy, and non-cooperative observers.
- Attempts to retain excessive records and grow histories or output queues.
- Event storms, full logging delivery, missing definitions, lag, and restarts.
- The Kafka RouteFull/replay/recovery scenario while observers themselves
  stall or flood output.
- Startup failure and shutdown under pressure.

Measure application throughput and tail latency, collector freshness,
capture/analysis CPU, engine and observer peak memory, dropped/rejected input,
output/suppression rates, lock wait time, and startup/shutdown completion.
Compare against explicit acceptance budgets rather than assuming bounded
queues imply acceptable impact. Destructive resource-exhaustion and
non-cooperative termination tests should run in supervised child processes.

These measurements are acceptance criteria for evaluating the extension
design. They are not performance claims made by the current foundation.
