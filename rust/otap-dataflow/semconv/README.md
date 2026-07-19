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
```

The manifest imports upstream OpenTelemetry semantic conventions when the
project can reuse an existing attribute. All project-owned definitions live
under `registry/`.

## Signal and entity model

Each Rust `#[attribute_set]` scope is represented by an entity. Composed
attribute sets are flattened into the entity identity, along with
`service.instance.id`. The hierarchy between scope entities is recorded in the
`otap_dataflow.parent_entities` annotation. Semantic-convention v2 supports
associating a metric or event with entities, but it does not define a native
entity-to-entity relationship expression.

Metrics use `<metric_set>.<instrument_name>` as their canonical convention name.
The existing instrumentation scope and emitted instrument name remain in the
`otap_dataflow.wire` annotation. Rust type, field, instrument, value type,
temporality, and source information are also retained as generation metadata.

Events retain an existing wire name as their convention name when it is valid
v2 syntax. An invalid wire name receives a normalized `otap.*` convention name,
while `otap_dataflow.wire.event_name` preserves the emitted value. Event
definitions include every statically declared attribute, scope, severity, and
source location.

Metrics and events use `entity_associations` to identify the entity carrying
their scope attributes. When a signal can originate from several alternative
scope types, the association uses `one_of`.

The `otap_dataflow` annotations are project metadata. Weaver validates the
standard v2 fields; `cargo xtask semconv-check` validates these annotations
against the Rust source and makes them available to future code generators.

## Validation

Run both checks from `rust/otap-dataflow`:

```bash
weaver registry check --v2 --registry semconv
cargo xtask semconv-check
```

The first command validates the v2 registry and imported references. The second
walks production Rust library and binary module graphs and compares discovered
telemetry declarations with the registry. It excludes test-only modules and
checks for missing, stale, or structurally different attributes, entities,
metrics, events, wire metadata, and entity associations.

`cargo xtask check` includes the semantic-convention drift check through its
structure-check step. CI pins Weaver to v0.24.2 and always passes `--v2`.

For diagnostics or generator development, print the source inventory as JSON:

```bash
cargo xtask semconv-inventory
```

## Updating the contract

When a production telemetry declaration changes, update the corresponding v2
definition in the same change. The drift checker reports the canonical name and
the source-backed value expected for each mismatch. Validate with both commands
above before submitting the change.

[definition-v2]: https://github.com/open-telemetry/weaver/blob/v0.24.0/schemas/semconv-syntax.v2.md
