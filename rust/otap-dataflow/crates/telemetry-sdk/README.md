# Weaver-generated telemetry SDK

This experimental crate is a reviewable prototype of a type-safe OTAP Dataflow
telemetry client generated from the project's semantic-convention v2 registry.
It is intentionally not integrated into existing instrumentation yet.

The generated surface covers all entity identity types and metric sets in the
registry.

## API shape

Each entity definition produces an owned identity type and an attribute-set
implementation. Each metric-set definition produces a cache-aligned struct of
concrete instruments, its static descriptor, and a registration method. The
registration method accepts only entity types listed by the metric's
`entity_associations`, so an invalid metric/entity pairing is rejected by the
Rust compiler. The custom entity hierarchy is also available through generated
`SemanticEntity` metadata and `ParentOf<Child>` implementations.

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

This crate depends only on the generic telemetry runtime and not on the
component crates that currently declare metric sets.
