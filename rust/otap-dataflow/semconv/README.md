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
  manifest.yaml
  registry/
    attributes.yaml
    entities.yaml
    metrics/*.yaml
    events/*.yaml
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

[`metric-sets.yaml`](../semconv-codegen/metric-sets.yaml) records package,
generated type, and set-wide availability once per metric set. It lives outside
the Weaver registry root because it is a project-owned code-generation input,
not a standard v2 definition. Metric field names, Rust value types,
temporalities, and wire identities are derived as follows:

- The Rust field is the metric name after removing the metric-set prefix and
  replacing dots with underscores. Existing fields that do not follow this
  rule use a sparse `otap_dataflow.rust.field` override.
- `int` maps to `u64`; `double` maps to `f64`.
- Counters are additive and delta by default. An observed cumulative counter
  uses `otap_dataflow.recording: observed`.
- Up/down counters are observed and cumulative by default. An additive
  up/down counter uses `otap_dataflow.recording: additive`.
- Gauges map to `Gauge`; histograms map to the pre-aggregated `Mmsc`
  implementation.
- Histograms backed by a distribution tier use the sparse
  `otap_dataflow.rust.instrument` override (`HistogramNormal` or
  `HistogramDetailed`) and export as exponential histograms.
- The wire scope is the metric-set identifier. The wire metric name is the
  canonical name after removing that prefix.

Conditional availability belongs to the metric-set catalog when it applies to
the entire set. A per-metric `otap_dataflow.availability` override is used only
for mixed sets such as `tokio.runtime`.

Events retain an existing wire name as their convention name when it is valid
v2 syntax. An invalid wire name receives a normalized `otap.*` convention name,
while `otap_dataflow.wire.event_name` preserves the emitted value. Event
definitions include every statically declared attribute, scope, severity, and
source location.

Metrics and events use `entity_associations` to identify the entity carrying
their scope attributes. When a signal can originate from several alternative
scope types, the association uses `one_of`.

The `otap_dataflow` annotations and metric-set catalog are project metadata.
Weaver validates the standard v2 fields; `cargo xtask semconv-check` validates
that the compact contract can derive the current Rust metric declarations and
makes the same inputs available to future code generators.

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
structure-check step. CI pins Weaver to v0.25.1 and always passes `--v2`.

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
Generated emission methods preserve the known call-site levels, canonical and
wire names, scope metadata, optional attributes, and entity associations.

For diagnostics or generator development, print the source inventory as JSON:

```bash
cargo xtask semconv-inventory
```

## Updating the contract

When a production telemetry declaration changes, update the corresponding v2
definition in the same change. The drift checker reports the canonical name and
the source-backed value expected for each mismatch. Validate with both commands
above before submitting the change.

[definition-v2]: https://github.com/open-telemetry/weaver/blob/v0.25.1/schemas/semconv-syntax.v2.md
