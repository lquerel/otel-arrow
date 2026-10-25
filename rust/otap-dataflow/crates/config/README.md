# `otel-arrow-dfe-config`

Configuration model and loading primitives for the OTel Arrow Dataflow Engine.
This crate is pre-1.0; its public API may evolve between minor releases.

## Documentation

- [Configuration Guide and Reference](../../docs/configuration.md): operator
  examples, field semantics, defaults, precedence, and validation.
- [Configuration Framework Design](../../docs/configuration-resolution.md):
  crate layering, factory resolution, runtime equivalence, and privacy.
- [Live Pipeline Reconfiguration](../../docs/admin/live-reconfiguration.md):
  Admin endpoints, partial updates, rollout, and recovery.

These guides are authoritative. This README describes only the crate boundary
and principal entry points; it does not duplicate configuration rules.

## Principal Types

- `engine::OtelDataflowSpec`: parsed root model.
- `engine::EngineConfig`: engine-wide declarations.
- `pipeline_group::PipelineGroupConfig` and `pipeline::PipelineConfig`:
  hierarchical pipeline envelopes and graph wiring.
- `node::NodeUserConfig` and `extension::ExtensionUserConfig`: ingestion
  envelopes containing component-owned, initially untyped payloads.
- `node_urn::NodeUrn` and `extension_urn::ExtensionUrn`: component identities.
- `policy::Policies` and `policy::ResolvedPolicies`: declarations and
  hierarchy-resolved policy values.
- `topic::TopicSpec`: topic declarations.
- `engine::ResolvedOtelDataflowSpec`: deterministic policy/topic resolution,
  including regular and observability pipeline roles.
- `secret::RedactedString`: explicit secret access with safe serialization.

## Loading and Parsing

`config_provider::resolve_config` loads source text and detects its format.
`config_provider::ResolvedConfig` holds private source URI/content, not a
public configuration snapshot. The CLI applies environment substitution
before model parsing.

Prefer `OtelDataflowSpec::from_yaml` or `from_json` for root parsing, and
`PipelineConfig::from_yaml` or `from_json` for pipeline updates and tooling.
These entry points canonicalize URNs and validate the model; direct serde
deserialization is not a substitute. `OtelDataflowSpec::from_file` is also
available, but source-aware hosts should use the provider API.

## Ownership Boundaries

`OtelDataflowSpec::resolve()` materializes policies and topic scopes. It does
not resolve concrete component types or produce safe reporting output.
The engine owns those factory contracts; the controller prepares and retains
the source, resolved runtime state, and committed `SafeConfigSnapshot` separately.
Admin HTTP and OpAMP report source, including sensitive values. Safe snapshots
remain available for future configuration events and are not management output.

Node and extension implementers define their normalized config types and
register mandatory resolvers with an explicit snapshot policy. Runtime
creation retrieves a checked `Arc<T>` instead of deserializing JSON again.
Use the [contributor checklist](../../docs/configuration-resolution.md#contributor-checklist)
when adding or changing a factory. Crate-level API details live in the Rust
documentation beside each public type and function.
