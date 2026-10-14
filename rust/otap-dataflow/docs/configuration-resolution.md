# Factory-Resolved Configuration

This document defines how the OTel Arrow Dataflow Engine turns submitted
configuration into runtime state and safe control-plane snapshots. It is the
architectural reference for component configuration resolution, live
reconciliation, and configuration privacy.

For the user-facing configuration format, see
[Configuration](configuration.md) and the
[configuration model reference](configuration-model.md). For rollout APIs and
operational behavior, see
[Live Pipeline Reconfiguration](admin/live-reconfiguration.md).

## Goals and Boundaries

The configuration lifecycle is designed to:

- parse, validate, default, and normalize component configuration before it is
  admitted to the runtime;
- give runtime components immutable values of their own concrete config types;
- compare runtime meaning rather than submitted JSON syntax or redacted output;
- provide Admin and OpAMP with a precomputed representation that is safe to
  disclose; and
- keep the submitted source model available only to the controller for later
  update construction and diagnostics.

The source artifact is the parsed `OtelDataflowSpec`. The engine does not retain
the original YAML or JSON bytes, comments, aliases, key ordering, or formatting.

Snapshot safety is a disclosure boundary, not encryption. The controller's
private source and resolved values can contain cleartext credentials needed by
the runtime. They must still be protected with process isolation, access
control, and appropriate operational handling.

## Configuration Representations

The controller keeps three distinct representations of an accepted
configuration.

| Representation | Contents | Consumers | Exposure |
| --- | --- | --- | --- |
| Source | Parsed submitted model, including operational inputs | Candidate construction and controller diagnostics | Controller-private |
| Resolved | Immutable, type-erased `Arc<T>` values and typed equivalence callbacks | Runtime creation and reconciliation | Never serialized by the control plane |
| SafeConfigSnapshot | Normalized envelopes with precomputed safe component snapshots | Admin responses and OpAMP `effective_config` | Safe control-plane reporting view |

`SafeConfigSnapshot` is a precomputed, privacy-preserving view of resolved
configuration intended for reporting. It may redact or omit values, is not a
runtime configuration or replayable source artifact, and is published only
after commit. "Safe" means safe for control-plane reporting under the declared
factory snapshot policies, not that the runtime configuration is secure or
that the snapshot replaces access control.

The `SafeConfigSnapshot` representation keeps the existing engine, group, and
pipeline envelopes. A component's `config` value, however, reflects the factory's
resolved defaults and normalization rather than necessarily preserving the
submitted syntax.

The source representation is not an API response. There is no Admin or OpAMP
endpoint for retrieving unredacted submitted configuration.

The snapshot API cannot wrap or mutably expose an arbitrary source envelope.
Construction first omits all component configs and `engine.custom` values.
Candidate assembly then installs only factory-resolved component snapshots.
Committed updates accept resolved pipelines or another `SafeConfigSnapshot`,
not submitted component values. Complete candidate resolution remains required
before admission; conservative assembly is not permission to skip a resolver.

## Resolution Lifecycle

```mermaid
flowchart TD
    A[Submitted YAML or JSON] --> B[Parsed source model]
    B --> C[Resolve complete candidate]
    C --> D{All factories and snapshots succeed?}
    D -- No --> E[Reject before runtime admission]
    D -- Yes --> F[Candidate source]
    D -- Yes --> G[Candidate resolved state]
    D -- Yes --> H[Candidate SafeConfigSnapshot]
    G --> I[Typed comparison and runtime creation]
    F --> K{Startup or rollout succeeds?}
    H --> K
    I --> K
    K -- No --> L[Keep previous committed generation]
    K -- Yes --> M[Commit source, resolved, and SafeConfigSnapshot]
    M --> N[Admin responses]
    M --> O[OpAMP effective_config]
```

The complete candidate is resolved before initial startup or live admission.
This includes regular pipelines, the engine observability pipeline, pipeline
extensions, and controller extensions. Resolution rejects the candidate when
it finds an unknown URN, a parsing or validation error, a snapshot-policy
mismatch, a reserved snapshot marker, or a safe-serialization failure.

Each component resolver is invoked once for a candidate. The resulting typed
value and safe snapshot are reused across pipeline cores, runtime creation,
Admin requests, and OpAMP heartbeats. Those consumers do not parse the
submitted JSON again.

Embedded hosts use `Controller::prepare(source, options)` to obtain a
`PreparedEngineConfig`. Validation-only execution may discard it; startup
consumes it through `run_forever()` or `run_till_shutdown()`. Host setup, such as
console stdout ownership, inspects `resolved_pipelines()` without re-resolving
the submitted configuration. Standalone validation helpers are intended for
tooling; calling them before preparing the same candidate would repeat
resolution.

## Factory Resolver Contract

Every receiver, processor, exporter, pipeline extension, and controller
extension factory declares both:

- a resolver that parses, validates, defaults, and normalizes its concrete
  configuration type; and
- one explicit `ConfigSnapshotPolicy` describing how the resolved value may be
  represented in the safe configuration snapshot.

The factory's creation callback receives a `ResolvedNodeConfig` or
`ResolvedExtensionConfig`. It obtains an immutable `Arc<T>` with the checked
`component_config::<T>()` accessor. A resolver/accessor type mismatch fails with
type information only; the error does not format or expose the submitted value.

Factories must not deserialize component JSON during creation. Multiple cores
share the same resolved value.

Closed schemas must reject unknown fields, including nested security settings.
For serde-backed schemas, prefer `deny_unknown_fields`. A schema using
`flatten` needs a strict flat wire decoder rather than combining incompatible
serde attributes. Intentionally open maps and plugin-defined schemas remain
open and use `Omit` unless their complete output has been audited.

### `TypedSafe`

Use `TypedSafe` when the concrete resolved type has an audited `Serialize`
implementation whose complete output is safe to disclose. Typed serialization
materializes defaults and normalized values in the safe configuration snapshot.

Secret fields in an otherwise safe schema must use types with safe
serialization, such as `RedactedString`. A plain `String` containing a secret
does not become safe merely because the surrounding schema is typed.

### `CustomSafe`

Use `CustomSafe` when the component can construct its own safe, normalized JSON
snapshot but ordinary serialization of the runtime type is unsuitable. The
resolver returns both the runtime value and the component-owned safe snapshot.

The custom serializer owns the complete safety of that component subtree. It
must not copy unknown or unaudited submitted fields into the snapshot.

### `Omit`

Use `Omit` when the configuration contains arbitrary properties, raw JSON,
plugin-defined schemas, or any field whose disclosure safety is uncertain. The
safe configuration snapshot replaces the component's complete `config` value
with the string `[OMITTED]`.

Examples that require conservative omission include Kafka arbitrary
properties, free-form plugin configuration, attribute-processor literals and
hash salts, traffic-generator resource attributes and registry locations, and
raw `serde_json::Value` payloads. Omission is preferred over partially
inspecting an uncertain schema.

There is no fallback to the submitted JSON for any policy. A factory policy is
mandatory, and the policy returned by its resolver must match the policy in the
factory registration.

## Runtime Equivalence and Live Reconciliation

Resolved component values define runtime equivalence. Types that implement
`PartialEq` use their real typed values. Types that cannot use `PartialEq` must
provide a component-owned semantic comparison callback.

Safe configuration snapshots are never used to decide whether a rollout is
needed. This has two important consequences:

- aliases, omitted defaults, or other inputs that resolve to the same runtime
  value can be classified as `noop`; and
- changing only a secret still changes runtime semantics even if both safe
  snapshots contain the same `[REDACTED]` marker.

Runtime equivalence excludes component snapshot output entirely. Changing the
safe display of an otherwise equivalent typed value does not itself require a
rollout. Graph metadata and normalized runtime policies are compared separately.

For a live update, the controller first builds and resolves all three candidate
forms. A pipeline rollout commits its source pipeline, pipeline snapshot, and
resolved runtime record together only after the rollout succeeds. A failed
admission or successful rollback leaves all three representations at the
previous committed generation.

This representation-level atomicity does not make full-engine reconciliation a
transaction across pipelines. Full-engine reconciliation performs a sequence of
pipeline operations. If an earlier pipeline rollout succeeds and a later one
fails, the earlier committed pipeline remains applied. Each committed pipeline
still has mutually consistent source, safe snapshot, and resolved state. Engine
and group metadata are committed only after the overall reconciliation succeeds.

Controller extensions and the engine observability pipeline do not currently
have a live replacement lifecycle. Full-engine reconciliation rejects additions,
removals, or runtime-significant changes to either before starting pipeline
operations; operators must restart the engine to apply them. Alias- or
default-equivalent changes remain acceptable because this boundary also uses
resolved typed values and normalized policies, not submitted syntax.

For rollout sequencing, failure handling, and the current consistency scope,
see [Live Pipeline Reconfiguration](admin/live-reconfiguration.md).

## Privacy and Security Model

### Secret Values

`RedactedString` is backed by `secrecy::SecretString` and has the following
contract:

- deserialization accepts the submitted cleartext value;
- runtime code must call `expose()` to access cleartext explicitly;
- serialization emits `[REDACTED]`;
- `Debug` output remains redacted; and
- equality compares the real secret, not the display marker.

Known inline secrets use type-owned safe serialization. These include
ClickHouse passwords, OAuth client secrets and signing keys, static HTTP and
gRPC headers, inline TLS private keys, OTLP HTTP endpoint URLs, and credentials
embedded in proxy URLs. The entire HTTP endpoint URL, including signal overrides,
is redacted because credentials may appear in its authority, path, or query.

`RedactedString` reduces accidental disclosure in snapshots and debug output.
It does not remove cleartext from the submitted source or from the runtime path
that needs the credential.

### Snapshot Markers and Replay

`[REDACTED]` and `[OMITTED]` are display-only markers, not valid replacements
for operational configuration.

- `RedactedString` rejects either marker when parsing a submitted secret.
- A complete component `config: "[OMITTED]"` value is rejected.
- Each top-level value under `engine.custom` is exported as `[OMITTED]`, and
  submitted marker values there are rejected.

These rules prevent a safe configuration snapshot from being silently replayed
as if it contained the original runtime credentials or omitted component data.

### Control-Plane Exposure

Admin engine, group, and pipeline reads serialize the committed
`SafeConfigSnapshot` or its group/pipeline views. OpAMP uses that same stored
representation for its protocol-defined `effective_config` field; the wire
field and `EffectiveConfig` message name are unchanged. This is a
privacy-preserving snapshot of the effective configuration, not the complete
runtime configuration. Neither path walks the source model looking for field
names such as `headers`, and neither performs late redaction.

### Editing Without Snapshot Replay

Full replacement requires an actual submitted source configuration, not an
exported snapshot. For edits based on an Admin view, the pipeline `PATCH` API
accepts explicit `set` and `remove` operations with object-key paths and an
`expectedRevision` from pipeline details. The controller applies those edits
to its private source under the normal pipeline operation reservation, then
resolves and admits the resulting candidate through the normal rollout path.

The TUI sends only a resource-policy edit for scaling. Its external editor
computes sparse edits relative to the original safe view and retains that
view's revision when a draft is reopened. Unchanged secret and omitted fields
are absent from the request; markers never mean "keep the old value". Any newly
supplied marker, including one nested in a map or array, is rejected. Arrays
are replaced as a whole, so editing an array containing markers or replacing
an omitted config requires supplying the complete real source value.

The revision is engine-wide: even a successful no-op update increments it.
Stale edits return a conflict before rollout; refresh and explicitly reapply
the edit. This guard can conservatively conflict after another pipeline
commits. Edits address submitted source fields, so normalized defaults are not
materialized into private source automatically. If an authored alias conflicts
with a canonical field being set, remove the alias explicitly or use a full
replacement from the original source.

### Diagnostics and References

Configuration URIs and provider contents are private too. CLI validation
confirmation identifies the provider kind without printing the URI; provider
debug output hides contents, and HTTP fetch errors remove URLs. Admin JSON
admission and factory resolution/creation boundaries do not forward arbitrary
parser or component error text, because it can echo submitted scalar values.
They retain status and component attribution rather than those private details.
Captured panic reports likewise discard arbitrary payload text, retaining only
worker context and a backtrace. This does not suppress process panic hooks.
Low-level parsing APIs and component-owned runtime logs are not safe snapshot
APIs; contributors must still audit diagnostics at explicit cleartext accesses.

Typed comparison observes configured inline values and reference identities,
not a fingerprint of every external credential source. For example, changing
a TLS file path changes configuration meaning, but rotating bytes at the same
path follows that component's reload behavior. A safe snapshot is not proof
that referenced material is unchanged, accessible, or secure. Provider-level
unexpanded views and an unredacted public source endpoint are not implemented.

Snapshot policies protect only configuration reporting. They do not replace
authorization on Admin endpoints, transport security for OpAMP, protection of
the process address space, or secure handling of the original submitted
configuration.

## Contributor Checklist

When adding or changing a component factory:

1. Define the concrete normalized runtime config type and its parsing,
   validation, and defaulting behavior. Reject unknown fields in closed schemas,
   especially nested security settings.
1. Register a resolver and an explicit snapshot policy for every factory,
   including test factories and no-config factories.
1. Select `TypedSafe` only after auditing every serialized field. Use
   `RedactedString` or another safe type for inline secrets.
1. Select `CustomSafe` only when the component constructs the complete safe
   snapshot itself.
1. Select `Omit` for free-form, plugin-defined, raw-value, or otherwise
   uncertain schemas.
1. Implement `PartialEq` for runtime semantics, or provide a component-owned
   semantic comparator.
1. In creation callbacks, retrieve `Arc<T>` through the checked typed accessor;
   do not deserialize the submitted JSON.
1. Test normalized/default-equivalent inputs, runtime-significant changes,
   secret redaction, omission, marker rejection, and typed accessor failures as
   applicable.
1. Test real factory serialization with public-field negative controls, nested
   secrets, maps/lists, aliases, and defaults. Audit newly added `String` fields;
   registry policy coverage alone cannot establish field-level safety.
1. Keep diagnostic text free of submitted private values, including malformed
   scalar input and creation failures. Treat explicit secret exposure as an
   audit boundary for logs and errors as well as runtime use.
1. Add the required `Scenario` and `Guarantees` comments to every new test.
