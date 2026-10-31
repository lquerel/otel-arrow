# Engine Observer Extensions

An engine observer is a trusted, process-local extension that consumes native
internal telemetry and reads observed engine state. It can emit named OTel
events through normal internal logging. Its context grants no control-plane
commands, registry mutation, or export-draining operations.

## Scope and configuration

Observers use the controller extension lifecycle and configuration namespace.
They are engine-scoped, not instantiated per pipeline or per core. There is one
observation hub per engine telemetry registry. Independently embedded controllers
have separate hubs and registry epochs.

```yaml
version: otel_dataflow/v1
engine:
  controller:
    observers:
      max_observers: 8
      channel_capacity: 256
      max_payload_bytes: 65536
    extensions:
      primary:
        type: "urn:example:extension:sample_observer"
      secondary:
        type: "urn:example:extension:sample_observer"
groups: {}
```

The example type must be linked into the collector; no concrete observer is
enabled by this foundation. Defaults allow eight instances, a shared ring of
256 observations and 64 KiB of logical retained payload per observation.
Hard limits are 32 instances, 1024 ring slots and 1 MiB per payload. Capacity
must be a power of two; the smallest payload limit is 256 bytes.
Settings and configured instance counts are validated before observer tasks
are prepared. Direct subscriptions also enforce the live instance cap.

## Native inputs and identity

`EngineObservationSubscription` receives shared `Arc<ObservationRecord>` values:

| Input | Contents and semantics |
| --- | --- |
| Metrics | Collected producer values, generational metric-set and entity keys, shared entity and registration attributes, static field and measurement schemas, bucket index. |
| Logs | Structured log record, callsite metadata, entity context keys and available shared entity definitions. |
| Lifecycle | An engine event published after successful handling by the observed-state store. |
| Rejected | Category of an observation rejected by payload or metadata-work limits. |

Metric values retain their native integers, floating point values and
distributions. They represent producer collection snapshots, not an OTLP export
transaction or a guaranteed sampling interval. Interpret each field using its
instrument and temporality. Collection still accumulates once into the normal
registry; subscribing does not drain exports or reset counters. Shared
definitions remain valid even after a producer unregisters.

Use `(registry_epoch, key)` for identity. An epoch is fresh for each registry
lifetime; keys must not be joined across restarts without it. Each frame has a
publication sequence and monotonic and wall-clock observation times. Sequence
order is publication order across sources, not causal order or an atomic
snapshot of the engine. Event payloads retain their original event times.

Structured log inputs come from the internal async logging reporters, after
normal log-level filtering and before their log-delivery queues. Console-direct
and disabled logging do not provide structured log inputs. Lifecycle and metric
inputs do not require log capture. Missing or retired log entity definitions
are absent from the resolved context; retain the original keys and treat this
as incomplete evidence.

## Multiple observers and bounded retention

Multiple observers share one overwrite ring. Payloads are retained once and
delivered by `Arc`, with an independent cursor for each observer. A slow reader
gets `RecvError::Lagged(n)` and resumes at the oldest retained record. Its loss
does not delay producers or remove a fast peer's observations. Loss counters
are cumulative per subscription. Treat lag and `Rejected` records as evidence
gaps; do not interpret them as an absence of activity.

The ring is allocated lazily at first subscription. With no subscribers,
producer snapshots are aggregated normally without allocating observation
frames. Publication does bounded admission work and briefly locks shared
engine metadata and publication sequencing; it never calls extension code or
waits for a consumer to make progress.

Payload admission limits metric values to 256 fields, log context to 32 keys,
and entity traversal to 512 attribute nodes and eight nested map levels. Large
vector capacities and boxed distributions count toward admission. Oversized
inputs generate small rejection markers instead of retaining oversized bodies.
These are logical payload limits, not a precise allocator or RSS budget.
At the defaults, queued payload admission is at most 16 MiB, plus bounded
envelopes, metadata/container overhead and in-progress publication. Shared
definitions may be counted more than once by admission.

Each configured observer adds one existing controller-extension thread and
task, a cursor, and its own application state. Observer authors must bound
their work per turn, retained frame references, histories and output rate.
Retaining an `Arc` after receipt extends its lifetime beyond the engine ring's
budget. `observed_state.snapshot()` clones the current state map and must also
be called and retained with care. It is read-only and not transactionally
aligned with an input frame. This foundation does not bound extension-owned
memory, CPU, or the engine's existing telemetry and state stores.

## Registering and writing an observer

A downstream observer crate depends on the controller, config and telemetry
crates, plus `linkme` and `tokio`. Link it into the collector with
`use my_observer_crate as _;`. The default `ControllerExtensionRegistry`
discovers `ENGINE_OBSERVER_FACTORIES` alongside controller extension factories.
Type URNs must be distinct across both kinds. Instance IDs identify configured
subscriptions and must be unique, nonempty and at most 128 bytes.

```rust,no_run
use otel_arrow_dfe_controller::{
    ControllerExtensionError, ControllerExtensionTaskFactory, distributed_slice,
    observer::{EngineObserverContext, EngineObserverFactory, ENGINE_OBSERVER_FACTORIES},
};
use otel_arrow_dfe_telemetry::observer_event;

#[allow(unsafe_code)]
#[distributed_slice(ENGINE_OBSERVER_FACTORIES)]
static SAMPLE_OBSERVER: EngineObserverFactory = EngineObserverFactory {
    name: "urn:example:extension:sample_observer",
    description: "Sample native engine observer.",
    documentation_url: "",
    validate_config: otel_arrow_dfe_config::validation::no_config,
    start,
};

fn start(mut context: EngineObserverContext)
    -> Result<ControllerExtensionTaskFactory, ControllerExtensionError>
{
    Ok(Box::new(move |cancel| Box::pin(async move {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                record = context.observations.recv() => match record {
                    Ok(record) => {
                        // Inspect a bounded amount of work; avoid retaining frames here.
                        let _ = record;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        observer_event!(context.events, WARN, "observer.input_gap", count);
                        // Reset calculations that require an uninterrupted input stream.
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
        observer_event!(context.events, INFO, "observer.stopped");
        Ok(())
    })))
}
```

Configuration validation and synchronous task preparation run before engine
threads start. The returned factory is `Send`; its future can use thread-local
`!Send` state. Respect the cancellation token and release inputs promptly at
shutdown. As with other controller extensions, shutdown is cooperative.
The hub remains alive through subscription handles; use cancellation rather
than waiting for the stream to close.

## Event output and failures

`observer_event!` emits a real named OTel event via `otel_event!`, with an
`observer_id` attribute and the reserved target `otel.observer.output`. Outputs
follow configured logging filters and normal console/ITS/export delivery;
emission does not acknowledge delivery. All observers' inputs exclude this
target, preventing both self-feedback and observer-to-observer feedback.
Use this macro for observer outputs rather than ordinary log macros. Existing
logging encode limits apply to event attributes.

Runtime errors and unwinding panics end only the failed observer task and emit
`observer.runtime_failed` or `observer.runtime_panicked`. They do not trigger
the shutdown policy of ordinary controller extensions. Startup validation and
preparation errors fail startup. There is no automatic task restart in this
foundation. In-process extensions remain trusted code: aborts, resource
exhaustion and non-cooperative tasks are not isolated by a process sandbox.
