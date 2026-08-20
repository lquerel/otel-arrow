# Weaver-generated telemetry SDK

This experimental crate is a reviewable prototype of a type-safe OTAP Dataflow
telemetry client generated from the project's semantic-convention v2 registry.
It is intentionally not integrated into existing instrumentation yet.

The generated surface covers all entity identity types, metric sets, and events
in the registry. Events target a small backend-independent `EventSink` contract
in this crate because the current telemetry runtime does not expose a generic
typed event-client contract.

## API shape

Each entity definition produces an owned identity type and an attribute-set
implementation. Each metric-set definition produces a cache-aligned struct of
concrete instruments, its static descriptor, and a registration method. The
registration method accepts only entity types listed by the metric's
`entity_associations`, so an invalid metric/entity pairing is rejected by the
Rust compiler. The custom entity hierarchy is also available through generated
`SemanticEntity` metadata and `ParentOf<Child>` implementations.

Each event definition produces a typed payload, static semantic and wire
metadata, explicit methods for its known call-site levels, and compile-time
event/entity association markers. Required attributes are required Rust fields;
recommended and opt-in attributes are `Option<T>`. Registry `any` attributes
retain the runtime's `AttributeValue` representation, while concrete registry
types become concrete Rust types. The payload visitor borrows values without
allocating; only a chosen sink decides whether to copy or encode them.

Compared with the current procedural macros, generated entity values are
materialized once in a fixed-size array. Attribute iteration does not allocate,
does not use a thread-local scratch vector, and does not extend lifetimes with
unsafe code. Metric recording remains direct struct-field access without a
hash-map or string lookup.

## Generate

Install Weaver v0.25.1, then run from `rust/otap-dataflow`:

```bash
weaver registry generate rust crates/telemetry-sdk/src/generated \
  --v2 \
  --registry semconv \
  --templates semconv-codegen/templates \
  --params semconv-codegen/metric-sets.yaml
cargo fmt --all
```

The checked-in files below `src/generated/` are generated artifacts. Edit the
registry, metric-set catalog, or templates instead of editing those files.

## Example

The `receiver_otlp` example constructs a generated node entity, registers the
generated OTLP receiver metric set, and records values through typed fields:

```bash
cargo run -p otap-df-telemetry-sdk --example receiver_otlp
```

The `event_sink` example emits a typed event to a standalone in-memory backend,
including its canonical and current wire identities:

```bash
cargo run -p otap-df-telemetry-sdk --example event_sink
```

This crate depends only on the generic telemetry runtime. It does not depend on
the component crates that currently declare metric sets and events, and it does
not adapt generated events to current instrumentation.
