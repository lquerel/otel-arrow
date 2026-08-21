# OTAP Dataflow semantic conventions

This directory contains the exhaustive semantic-convention contract for
production attributes, entities, metrics, and events emitted by OTAP Dataflow.
Definitions use Weaver's experimental
[`definition/2` syntax][definition-v2].

Tracking issue:
<https://github.com/open-telemetry/otel-arrow/issues/1613>

## Layout

```text
semconv/
  live-check.weaver.toml
  manifest.yaml
  registry/
    attributes.yaml
    entities.yaml
    metrics/*.yaml
    events/*.yaml
  triggers/*.sh
semconv-live-check/
  internal-events.yaml
semconv-codegen/
  metric-sets.yaml
  templates/registry/rust/
```

The manifest imports upstream OpenTelemetry semantic conventions when the
project can reuse an existing attribute. All project-owned definitions live
under `registry/`.

## Signal and entity model

Each Rust scope-level `#[attribute_set]` is represented by an entity. Item-level
attribute sets used while recording metrics are referenced by the metric's
standard `attributes` field instead. Composed scope attribute sets are
flattened into the entity identity, along with
`service.instance.id`. The hierarchy between scope entities is recorded in the
`otap_dataflow.parent_entities` annotation. Semantic-convention v2 supports
associating a metric or event with entities, but it does not define a native
entity-to-entity relationship expression.

Metrics use `<metric_set>.<instrument_name>` as their canonical convention name.
Each metric records only its numeric `code_generation.metric_value_type`, its
`otap_dataflow.metric_set` membership, and exceptional generation overrides.
The standard metric fields remain the source of truth for the instrument, unit,
and description.

Attributes use their standard `key` as the wire key. An exceptional
`otap_dataflow.wire.attribute_key` override is needed only when the emitted key
differs. Events similarly use their standard `name` as the wire name. An
invalid legacy wire name receives a normalized `otap.*` convention name, while
an exceptional `otap_dataflow.wire.event_name` override preserves the emitted
value. Event definitions include every statically declared attribute, scope,
severity, and source location.

The `semconv-live-check/` manifests describe the signals that a particular CI
runtime scenario must exercise. They are coverage expectations, not
semantic-convention definitions. Weaver still validates every telemetry sample
received during the scenario against the complete registry.

Metrics and events use `entity_associations` to identify the entity carrying
their scope attributes. When a signal can originate from several alternative
scope types, the association uses `one_of`.

The `otap_dataflow` annotations are project metadata. Weaver validates the
standard v2 fields and references.

## Observable entity architecture

The standalone interactive [entity and signal graph](entity-signal-graph.svg)
presents the entity hierarchy and signal counts. Open it directly in a browser
and select an entity to see its events and metrics, grouped by metric set. A
shared `one_of` association is included in every eligible entity's details.
Selecting another entity closes the previous details. Regenerate the checked-in
diagram from the registry with:

```bash
cargo xtask semconv-graph
```

## Validation

Run both checks from `rust/otap-dataflow`:

```bash
weaver registry check --v2 --registry semconv
cargo xtask semconv-check
```

The first command validates the v2 registry and imported references. The second
walks production Rust library and binary module graphs and compares discovered
telemetry declarations with the registry and metric-set catalog. It excludes
test-only modules and checks for missing, stale, or structurally different
attributes, entities, metrics, events, generated metric shapes, availability,
and entity associations.

`cargo xtask check` includes the semantic-convention drift check through its
structure-check step. CI pins Weaver to v0.25.1, runs the static check with
`--v2`, checks the Rust source inventory, and live-checks telemetry emitted by
an exercised engine scenario against the same registry.

The source inventory derives attribute types from their Rust declarations.
Primitive fields map to the corresponding OpenTelemetry scalar type. Unit
enums using `#[derive(AttributeEnum)]` map to v2 semantic-convention enums,
including `#[attribute_value = "..."]` overrides and variant documentation.
Manual `AttributeEnum::VARIANTS` implementations are recognized as well. When
multiple Rust enums use the same wire key, their known members are combined;
because semantic-convention enums are open, compatible string observations do
not discard the known member set. Truly dynamic values continue to use `any`.

## Client SDK generation

The custom Weaver templates under `semconv-codegen/templates/registry/rust`
generate the experimental `otap-df-telemetry-sdk` crate. The generated surface
contains owned entity identity types, cache-aligned metric-set structs, typed
event payloads, static descriptors, and compile-time signal/entity association
markers. Generated events target a backend-independent sink defined by the
experimental crate. The output is checked in for review but is not integrated
into existing instrumentation.

Run the generator from `rust/otap-dataflow` with Weaver v0.25.1:

```bash
weaver registry generate rust crates/telemetry-sdk/src/generated \
  --v2 \
  --registry semconv \
  --templates semconv-codegen/templates \
  --params semconv-codegen/metric-sets.yaml
cargo fmt --all
```

Event payloads use concrete Rust types where the registry has a concrete type
and retain `AttributeValue` for source values currently modeled as `any`.
Enum-backed attributes use `String`, their underlying OTLP wire type, while the
registry retains the known enum members for validation and future specialized
code generation.
Generated emission methods preserve the known call-site levels, canonical and
wire names, scope metadata, optional attributes, and entity associations.

For diagnostics or generator development, print the source inventory as JSON:

```bash
cargo xtask semconv-inventory
```

## Updating the contract

When a production telemetry declaration changes, update the corresponding v2
definition in the same change. Run the Weaver registry check before submitting
the change.

[definition-v2]: https://github.com/open-telemetry/weaver/blob/v0.25.1/schemas/semconv-syntax.v2.md
