# Configuration Guide and Reference

This is the operator reference for the OTel Arrow Dataflow Engine. Start with
the quick start, then use the field and policy sections to look up supported
settings, defaults, precedence, and validation rules.

Configuration documentation:

- [Configuration Guide and Reference](configuration.md): author and validate
  configuration.
- [Configuration Framework Design](configuration-resolution.md): understand
  resolution and privacy.
- [Live Pipeline Reconfiguration](admin/live-reconfiguration.md): operate the
  running engine.

> [!WARNING]
> This project is experimental. The configuration format is not yet stable and
> can change, including incompatible changes between releases.

## Contents

- [Quick Start](#quick-start)
- [Configuration Location](#configuration-location)
- [Configuration Structure](#configuration-structure)
- [Receivers, Processors, and Exporters](#receivers-processors-and-exporters)
- [Pipeline Groups and Pipelines](#pipeline-groups-and-pipelines)
- [Policy Hierarchy](#policy-hierarchy)
- [Connections and Output Ports](#connections-and-output-ports)
- [Topic Declarations](#topic-declarations)
- [Engine Section](#engine-section)
- [Validate and Troubleshoot](#validate-and-troubleshoot)
- [Related Documentation](#related-documentation)

## Quick Start

Save the following complete configuration as `config.yaml`. All subsequent
YAML examples are configuration fragments unless they include a root `version`.

Minimal OTLP receive, batch, and export pipeline:

```yaml
version: otel_dataflow/v1
groups:
    default:
        pipelines:
            main:
                nodes:
                    otlp/ingest:
                        type: receiver:otlp
                        config:
                            protocols:
                                grpc:
                                    listening_addr: "127.0.0.1:4317"

                    batch:
                        type: processor:batch

                    otlp/export:
                        type: exporter:otlp_grpc
                        config:
                            grpc_endpoint: "http://192.0.2.10:4317"

                connections:
                    - from: otlp/ingest
                      to: batch
                    - from: batch
                      to: otlp/export
```

From the Rust workspace, validate it before starting the engine:

```bash
cargo run --bin df_engine -- --config config.yaml --validate-and-exit
cargo run --bin df_engine -- --config config.yaml
```

The example listens on loopback; replace its export endpoint with your backend.
For build prerequisites, see [Development Setup](../README.md#development-setup).

## Configuration Location

The `df_engine` binary reads one root configuration through `--config`.
If `--config` is omitted, the engine looks for `config.yaml` in the current
working directory.

Supported config sources:

<!-- markdownlint-disable MD013 -->

| Source                      | Description                                                                                |
| --------------------------- | ------------------------------------------------------------------------------------------ |
| `/path/to/config.yaml`      | Bare local path. Treated the same as `file:`.                                              |
| `file:/path/to/config.yaml` | Local file path. `.json` files parse as JSON; other files parse as YAML.                   |
| `env:MY_VAR`                | Environment variable containing the full configuration text.                               |
| `yaml:<content>`            | Inline YAML. `::` expands nested keys for small test fragments.                            |
| `http://host/path`          | Unauthenticated HTTP GET. JSON is detected from `Content-Type`; otherwise YAML is assumed. |

<!-- markdownlint-enable MD013 -->

The `http:` provider retries failed fetches with exponential backoff. `https:`,
authenticated config sources, and multi-file merge are not implemented.

Validation parses YAML or JSON, validates the root model, checks graph
references, checks that every component type is registered in the binary, and
requires every selected factory to resolve its component config into a typed
runtime value and a safe configuration snapshot. See
[Validate and Troubleshoot](#validate-and-troubleshoot) for the admission checks.
The [framework design](configuration-resolution.md) explains resolution.

Admin HTTP configuration reads and OpAMP `effective_config` return committed
source, including sensitive values and free-form settings. Protect these
management interfaces and saved responses accordingly. The engine retains a
separate `SafeConfigSnapshot` for future configuration events, not management
responses.

After loading, the CLI can override selected engine-level settings:

- `--num-cores`
- `--core-id-range`
- `--http-admin-bind`

Raw configuration text supports environment substitution before parsing:

- `${env:VAR}`: replace with `$VAR`; error if the variable is unset.
- `${env:VAR:-default}`: replace with `$VAR`, or `default` when unset.
- `${env:VAR:-}`: replace with `$VAR`, or the empty string when unset.
- `$$`: literal `$`; use it only when escaping a sequence that would otherwise
  be treated as environment substitution, such as `$${env:VAR}`.

## Configuration Structure

Every runtime file is a single root document that describes the engine process:

```yaml
version: otel_dataflow/v1
policies: {}
topics: {}
engine: {}
groups:
    default:
        topics: {}
        policies: {}
        pipelines:
            main:
                policies: {}
                extensions: {}
                nodes: {}
                connections: []
```

The only required root field is `version`, which must be `otel_dataflow/v1`.
Standalone pipeline files are not a startup root format.

Optional root fields:

- `groups`: pipeline groups keyed by group id; defaults to an empty map. A
  single engine can run many groups, and each group can run many pipelines.
- `policies`: top-level policy defaults.
- `topics`: global topic declarations.
- `engine`: engine-wide settings.

Most simple configurations only need `version` and `groups`.

## Receivers, Processors, and Exporters

Receivers, processors, and exporters are configured as pipeline `nodes`. Each
node has a pipeline-local id, such as `otlp/ingest`, `batch`, or `otlp/export`
in the example below, and a `type`:

```yaml
nodes:
    otlp/ingest:
        type: receiver:otlp
        config:
            protocols:
                grpc:
                    listening_addr: "127.0.0.1:4317"
    batch:
        type: processor:batch
    otlp/export:
        type: exporter:otlp_grpc
        config:
            grpc_endpoint: "http://192.0.2.10:4317"
```

The `type` can use either the OTel shortcut form or the full URN:

```yaml
type: receiver:otlp
```

```yaml
type: urn:otel:receiver:otlp
```

The node kind is inferred from the `type`. The engine does not use separate
Collector-style `receivers`, `processors`, and `exporters` maps.

Common node fields:

- `type`: required node implementation URN or shortcut.
- `description`: optional human-readable description.
- `config`: node-specific ingestion payload, owned and validated by the
  selected component; omitting it is equivalent to `config: {}`. Components
  with required fields still need a configuration block. An explicit
  `config: null` remains null and is accepted or rejected by the factory.
  Writing `config:` without a value is also YAML null, not an empty object.
- `outputs`: optional declared output ports for multi-output receivers or
  processors.
- `default_output`: optional default output port used by nodes that emit without
  selecting a port.
- `capabilities`: optional bindings from capability name to pipeline extension.
- `entity`: optional node entity enrichment metadata.
- `header_capture`: receiver-only transport header capture override.
- `header_propagation`: exporter-only transport header propagation override.
- `rate_limiters`: zero or one named admission limiter; omit it or use `[]` to
  leave the node unbound.
- `policies.telemetry`: node-honored message/duration instrumentation overrides;
  see [Node and Flow Metrics](node-and-flow-metrics.md).

Core node types are listed in the
[core-node catalog](../crates/core-nodes/README.md). Each node links to a
README beside its implementation with node-specific configuration examples,
limits, stability, and telemetry.

Optional contrib nodes are listed in the
[contrib-node catalog](../crates/contrib-nodes/README.md). They are registered
only when the corresponding crate features are enabled in the binary you run.

## Pipeline Groups and Pipelines

The engine is designed to run and manage many pipelines in parallel inside one
process. Groups are logical containers for related pipelines and can be mapped
to operational boundaries such as a team, project, tenant, environment, or
deployment slice.

```yaml
groups:
    ingest:
        policies: {}
        topics: {}
        pipelines:
            traces:
                nodes: {}
                connections: []
```

Group fields are `policies` (optional), `topics` (defaults to an empty map),
and `pipelines` (defaults to an empty map). Pipeline IDs are unique within
their group; the logical address is `(group_id, pipeline_id)`. The group ID
`system` is reserved for engine-managed pipelines.

A pipeline is an executable graph:

- `nodes`: receivers, processors, and exporters in the data path.
- `extensions`: long-lived components available to nodes through capabilities.
- `connections`: explicit graph wiring.
- `policies`: optional pipeline-level policy overrides.

`nodes`, `extensions`, and `connections` default to empty collections.
Each extension has a `type` and optional `description` and `config`; a node
binds extension capabilities through its `capabilities` map. As with nodes,
omitting an extension's `config` is equivalent to `config: {}` at every scope.
Extensions do not participate in data-path connections and use an ID namespace
separate from nodes.

Pipelines in `df_engine` are multi-signal by default. Logs, metrics, and traces
can move through the same pipeline graph, unlike the Collector model where
pipelines are usually split by signal type. You do not need a connector just to
move data from a traces path into a metrics path; model the routing or
conversion you need with nodes and explicit connections.

Each node and extension selects its component implementation through its `type`
field.

The engine does not infer pipeline order from node roles. If data should flow
between two nodes, add a `connections` entry.

After startup, updates are in-memory only; the startup file is not rewritten.
Per-pipeline updates target an existing group. Full-engine reconciliation and
group lifecycle operations can also change supported group/engine metadata.
For supported mutations, revision-guarded edits, and restart-only settings,
see [Live Pipeline Reconfiguration](admin/live-reconfiguration.md).

## Policy Hierarchy

Policies include channel capacity, health, runtime telemetry, resources
controls, runtime recovery, transport headers, authorized identity claim
capture, and composite context declarations:

```yaml
policies:
  channel_capacity:
      control:
        node: 256
        pipeline: 256
        completion: 512
      pdata: 128
  health:
    # optional overrides; defaults are applied when omitted
  telemetry:
    pipeline_metrics: true
    tokio_metrics: true
    runtime_metrics: basic
  resources:
    core_allocation:
      type: all_cores
    memory_limiter:
      mode: observe_only
      source: auto
      soft_limit: 7 GiB
      hard_limit: 8 GiB
    rate_limiters:
      ingress:
        enforcement: enforce
        aggregation: receiver_instance
        unit: request_bytes
        pressure: soft
        token_bucket:
          allow: 10485760
          interval: 1s
          burst: 10485760
  runtime_recovery:
    enabled: true
    max_restarts: 5
    initial_backoff: 250ms
    max_backoff: 30s
    startup_timeout: 30s
    reset_after: 60s
  transport_headers:
    header_capture:
      headers:
        - match_names: ["x-tenant-id"]
          store_as: tenant_id
    header_propagation:
      default:
        selector:
          type: all_captured
  authorized_identity:
    - claim: sub
      store_as: customer_id
    - claim: groups
      store_as: access_groups
```

For full transport header policy documentation, see
[transport-headers.md](transport-headers.md).

`authorized_identity` selects verified authorization claims for storage in
pdata context. It can be configured at top-level, group, or pipeline policy
scope. The nearest configured scope replaces the complete broader policy list;
entries are not merged across scopes. An empty list at a narrower scope
disables inherited identity capture.

Only OTLP HTTP and gRPC receivers currently produce authorized identity
entries, and only when the receiver binds a `bearer_token_authorizer`
capability. Configuring the policy on another receiver, or on an OTLP receiver
without an authorizer, captures no entries. Missing claims are omitted without
rejecting the request. Single- and multi-valued claims retain their original
cardinality.

Authorized identity entries remain distinct from transport headers. The
current policy does not propagate claims as outbound headers or provide
routing, predicates, composites, required-entry enforcement, or generic
context consumers. See the
[OTLP receiver documentation](../crates/core-nodes/src/receivers/otlp_receiver/README.md)
for the supported authorization path.

Resolution order:

- regular pipelines:
  `groups.<group>.pipelines.<pipeline>.policies` -> `groups.<group>.policies` ->
  top-level `policies`
- observability pipeline:
  `engine.observability.pipeline.policies` -> top-level `policies`

An omitted policy family inherits from the next broader scope; an empty
`policies: {}` does not reset inherited families. If no scope declares a
family, its built-in default applies. An explicitly selected family replaces
the broader family as a whole, with defaults filling its omitted fields;
nested fields are not deep-merged across scopes.

`resources` is the exception: `core_allocation`, `memory_limiter`, and
`rate_limiters` resolve independently. A selected `rate_limiters` map replaces
the broader map, and `{}` explicitly disables inherited limiters.

Composite `context.entries` declarations are collected across scopes and
compiled with scoped name resolution, rather than treated as a replacement
policy family. See [Transport Header Policies](transport-headers.md).

Built-in defaults:

- `channel_capacity.control.node = 256`
- `channel_capacity.control.pipeline = 256`
- `channel_capacity.control.completion = 512`
- `channel_capacity.pdata = 128`
- `telemetry.pipeline_metrics = true`
- `telemetry.tokio_metrics = true`
- `telemetry.runtime_metrics = basic`
- `resources.core_allocation = all_cores`
- `runtime_recovery.enabled = true`
- `runtime_recovery.max_restarts = 5`
- `runtime_recovery.initial_backoff = 250ms`
- `runtime_recovery.max_backoff = 30s`
- `runtime_recovery.startup_timeout = 30s`
- `runtime_recovery.reset_after = 60s`
- `transport_headers = not set` (opt-in; no headers captured or propagated)
- `authorized_identity = not set` (opt-in; no verified claims captured)

### Conditional Transport Header Propagation

Captured transport headers and authorized identity claims create distinct named
context entries that retain their source types. Their `store_as` fields name
those primitive entries. `policies.context.entries` groups typed members into
composites, with optional member renaming through `store_as`.

Composite context entries can conditionally select a captured transport header
for exporter propagation:

```yaml
policies:
    context:
        entries:
            tenant:
                - type: transport_header
                  name: workspace
                  store_as: workspace_id
                - type: transport_header_match
                  name: environment
                  value: production
    transport_headers:
        header_capture:
            headers:
                - match_names: [x-workspace]
                  store_as: workspace
                - match_names: [x-environment]
                  store_as: environment
        header_propagation:
            default:
                selector:
                    type: named
                    named: [tenant:workspace_id]
                name: stored_name
```

The selected `workspace` header is emitted as `workspace_id` only when it exists
and at least one captured `environment` value exactly matches `production`.
Every configured match condition must pass. Header names are ASCII
case-insensitive; values are exact byte matches. Other composite members are
not evaluated for this transport-header binding. The selected source and every
condition header must be captured; the example stores `x-workspace` as
`workspace` and `x-environment` as `environment`.

Qualified composite selectors cannot share a primitive source with another
qualified or unqualified entry. Repeated unqualified entries, including ASCII
case variants, are accepted as equivalent. Unknown composites, unknown members,
authorized-identity members, and conflicting source bindings are rejected
during startup. Exporter overrides retain precedence and can independently
select the primitive header. See
[Transport header policies](transport-headers.md#conditional-composite-members)
for complete matching, naming, and override semantics.

Composite declarations can contain authorized identity members, but transport
propagation selects only transport-header members. Other composite consumers
are not yet wired at runtime.

### Core Allocation

Use `policies.resources.core_allocation` to select the worker cores assigned to
each regular pipeline. The policy can be set at the top-level, group, or
pipeline scope using the policy precedence described above. The default is
`all_cores`.

Use every process-visible core:

```yaml
policies:
    resources:
        core_allocation:
            type: all_cores
```

Have the controller select a fixed number of cores:

```yaml
policies:
    resources:
        core_allocation:
            type: core_count
            count: 4
```

Or assign explicit inclusive core ranges:

```yaml
policies:
    resources:
        core_allocation:
            type: core_set
            set:
                - start: 0
                  end: 1
                - start: 4
                  end: 5
```

`core_count` requires `count` in YAML and rejects `set`; `all_cores` rejects
both `count` and `set`. `core_set` requires a non-empty `set` and rejects
`count`. Each range requires `start <= end`, and a count greater than the
number of process-visible cores is invalid.

The allocation strategies have different sharing behavior:

- `all_cores` uses every core visible to the process. It neither reserves cores
  from other pipelines nor observes their reservations.
- `core_count` is controller-selected and exclusive. The controller excludes
  cores assigned to `core_count` and `core_set` pipelines and rejects the
  configuration if the requested positive count cannot be satisfied. A count
  of `0`, or an omitted count accepted from programmatic configuration, means
  all currently unreserved visible cores and fails if none remain.
- `core_set` uses exactly the configured ranges. Core IDs must be visible to
  the process, ranges must be valid and non-overlapping within the allocation,
  and the set cannot be empty. Explicit `core_set` allocations may overlap one
  another, but their cores are unavailable to controller-selected
  `core_count` allocations.

On Linux, the controller discovers the CPUs available through process affinity
and cgroup restrictions and their NUMA topology. For `core_count`, it
deterministically prefers enough cores from one NUMA node, then falls back to
ascending visible core IDs when one node cannot satisfy the request. On
platforms where NUMA topology is unavailable, selection remains deterministic
by ascending visible core ID.

The controller plans explicit `core_set` allocations before `core_count`
allocations and validates all regular-pipeline placements before starting
workers. Live updates account for committed and accepted in-flight placements
and exclude the replaced pipeline from its own reservation calculation.
Resource policies on the system observability pipeline are rejected. Its worker
runs on one core that is not reserved and may overlap any regular-pipeline
allocation.

### Runtime Recovery

`runtime_recovery` inherits through regular-pipeline scopes and is unsupported
on the system observability pipeline. Its fields and defaults are shown above.
Only panic/error exits trigger recovery; clean exits are not restarted. For
per-core restart budgets, backoff, readiness, reset windows, and exhaustion
behavior, see [Runtime Recovery Flow](admin/live-reconfiguration.md#runtime-recovery-flow).

### Memory Limiter

- `policies.resources.memory_limiter` is optional and process-wide.
- If configured, `mode` must be explicitly set to `enforce` or `observe_only`.
- The policy is supported only at top-level `policies.resources`.
  Group, pipeline, and observability-pipeline resource placements are rejected.
- In Phase 1, `Soft` remains informational; `Hard` is the ingress-shedding
  threshold.
- Detailed runtime behavior and rollout guidance are documented in
  [memory-limiter-phase1.md](memory-limiter-phase1.md).

### Rate Limiters

- Named limiters are declared under `policies.resources.rate_limiters`.
- Any non-empty rate-limiter declaration requires a top-level
  `policies.resources.memory_limiter`, which supplies the process pressure
  signal that activates enforcement.
- A declaration may appear at top-level, group, or pipeline policy scope. The
  nearest declared map replaces the broader map; entries are not deep-merged.
- `rate_limiters: {}` at a narrower policy scope disables an inherited map.
- A node selects one effective limiter with `rate_limiters: [name]` and opts out
  with `rate_limiters: []`.
- Omitting the node field leaves the node unbound, regardless of how many
  limiters are effective.
- More than one bound limiter, an unknown name, or a dimension-incompatible
  selection fails admission at startup or during a live update.
- V1 creates one bucket per bound receiver instance. Reusing a declaration does
  not aggregate rate state across receivers or pipelines.
- OTLP supports `request_bytes`; Syslog / CEF supports `messages`.
- Detailed runtime behavior and limits are documented in
  [memory-limiter-phase1.md](memory-limiter-phase1.md).

### Channels and Telemetry

Control channel keys:

- `node`: per-node control inboxes
- `pipeline`: shared pipeline-runtime orchestration channel
- `completion`: shared Ack/Nack completion channel

Telemetry policy notes:

- `telemetry.runtime_metrics` accepts `none`, `basic`, `normal`, or `detailed`
- this level gates:
  - channel endpoint transport metrics
  - per-node input/output outcome metrics
  - shared pipeline control-plane metrics such as `pipeline.runtime_control`
    and `pipeline.completion`
- `basic` exports gauges and transport counters
- `normal` adds message and phase counters
- `detailed` adds latency/duration summaries and completion unwind-depth
  distribution
- node-local `policies.telemetry.duration_distribution` accepts `basic`,
  `normal`, or `detailed` and defaults to `normal`
- shared receiver and exporter local duration instruments use the selected
  node-local distribution tier when `duration: true`
- See [Node and Flow Metrics](node-and-flow-metrics.md) to configure and
  interpret node item metrics and processor flow metrics.

## Connections and Output Ports

Connections define the pipeline graph. `from` accepts a source or source
array, `to` accepts a destination or destination array, and optional
`policies.dispatch` controls dispatch:

```yaml
connections:
    - from: otlp/ingest
      to: batch
    - from: batch
      to: otlp/export
```

Connection sources must be receivers or processors. Exporters are sinks and
cannot be used as `from` endpoints.

Because connections are explicit, the configuration can describe topologies
beyond a simple receiver-processor-exporter chain, including fan-in, fan-out,
named output ports, competing consumers, topic bridges, and observability
pipelines.

Fan-in and fan-out are explicit:

```yaml
connections:
    - from: [ingest/a, ingest/b]
      to: batch
    - from: batch
      to: [worker/a, worker/b]
      policies:
          dispatch: one_of
```

`dispatch` defaults to `one_of`: each item goes to exactly one destination.
Multiple destinations act as competing consumers on one channel; selection
is runtime-driven and best-effort balanced, not deterministic round-robin.
`broadcast` is parsed but unsupported for multi-destination connections.
For a single destination, the dispatch policy has no runtime effect.

### Output Ports

Most nodes use the default output. Multi-output processors can expose named
ports:

```yaml
nodes:
    router:
        type: processor:type_router
        outputs: ["logs", "metrics", "traces"]
        config: {}

connections:
    - from: router["logs"]
      to: logs/export
    - from: router["metrics"]
      to: metrics/export
    - from: router["traces"]
      to: traces/export
```

If `from` omits a port selector, the engine selects the `default` output. When
a node declares `outputs`, any selected source port must be listed there.

This includes listing `default` when a connection omits a selector.
`default_output` controls implicit node emissions; it does not change the
connection selector. With multiple outputs and no default, implicit node
emissions are ambiguous and require explicit port selection at runtime.
Exporters do not expose output ports.

## Topics

Topics are named in-process communication points for inter-pipeline delivery.
They support balanced worker pools, broadcast fan-out/taps, and mixed
balanced/broadcast consumers without direct pipeline-to-pipeline wiring.

Declare global topics at `topics.<name>` or group-local topics at
`groups.<group>.topics.<name>`. A group-local declaration overrides the global
declaration of the same name for pipelines in that group.

Publish with `exporter:topic` and consume with `receiver:topic`:

```yaml
nodes:
    publish:
        type: exporter:topic
        config:
            topic: raw_signals
    consume:
        type: receiver:topic
        config:
            topic: raw_signals
```

These nodes illustrate opposite ends of a topic hop in different pipelines.

### Topic Declarations

Topic wiring must remain acyclic across topic hops. During startup, the
controller rejects feedback loops that involve declared topics, including:

- same-pipeline loops such as `receiver:topic -> ... -> exporter:topic`
- cross-pipeline loops where one pipeline eventually routes back into an
  earlier topic

Current topic declaration shape:

```yaml
topics:
  raw_signals:
    description: "raw ingest stream"
    backend: in_memory
    impl_selection: auto
    policies:
      balanced:
        queue_capacity: 1000
        on_full: drop_newest
      broadcast:
        queue_capacity: 1000
        on_lag: disconnect
        ack_mode: all
      ack_propagation:
        mode: auto
        max_in_flight: 1024
        timeout: 30s
```

Supported `backend` values:

- `in_memory` (default, currently implemented)
- `quiver` (reserved in the schema and rejected by the current runtime)

Unsupported backend or policy combinations are rejected during startup topic
declaration with explicit errors.

Supported `impl_selection` values:

- `auto`
- `force_mixed`

Supported `balanced.on_full` values:

- `block`
- `drop_newest`

Supported `broadcast.on_lag` values:

- `drop_oldest`
- `disconnect`

Supported `broadcast.ack_mode` values:

- `first` (default): the first subscriber Ack/Nack resolves upstream
- `all`: all subscribers eligible at publish time must Ack; requires
  `on_lag: disconnect` and `ack_propagation.mode: auto`

Supported `ack_propagation` fields:

- `mode`:
  - `disabled`
  - `auto`
- `max_in_flight` (default: `1024`, must be > 0)
- `timeout` (default: `30s`)

`balanced.on_full` applies to balanced delivery paths. `broadcast.on_lag`
applies to broadcast delivery paths. `ack_propagation.mode` applies to the
topic hop as a whole and controls whether Ack/Nack can be bridged across
pipelines. `ack_propagation.max_in_flight` and `ack_propagation.timeout`
govern tracked publish outcomes per publisher handle when Ack/Nack propagation
is enabled.

Processor-local NACKs follow the same rule. For example, when
`content_router` or `signal_type_router` emits a route-local NACK because a
selected route is full, closed, or being drained during shutdown, that
rejection stays local when `ack_propagation.mode: disabled` and is bridged
upstream when `ack_propagation.mode: auto`. See the
[`content_router`](../crates/core-nodes/src/processors/content_router/README.md)
and
[`signal_type_router`](../crates/core-nodes/src/processors/signal_type_router/README.md)
processor docs.

With `ack_propagation.mode: auto`, `broadcast.ack_mode: all` resolves upstream
as Ack only after every subscriber eligible at publish time Acks. Any required
Nack, or a required subscriber disappearing before Acking, resolves upstream
as Nack. Subscribers added after publish are not required. `all` requires
`on_lag: disconnect`, `ack_propagation.mode: auto`, and a broadcast-only
inferred topic mode; startup rejects other combinations.

If no subscribers are eligible at publish time, the empty consensus resolves
immediately as Nack. This prevents a producer from reporting successful
delivery before topic receivers subscribe during startup or live
reconfiguration. The upstream source must retry or durably buffer Nacked
messages for recovery.

`all + drop_oldest` is rejected: skipping a required branch delivery makes
recovery ambiguous, while replaying upstream can duplicate destinations that
already succeeded.

Choose the branch topology based on the required Ack boundary:

```text
export completion:  receiver -> retry          -> exporter
durable acceptance: receiver -> durable buffer -> exporter
```

Retry branches keep aggregate topic completion pending until every exporter
reaches a terminal outcome. Durable-buffer branches persist before Acking the
topic and retry exporters independently from branch-local storage. Configure
`ack_propagation.timeout` to exceed the retry budget or expected durable write
latency, respectively.

`ack_mode: all` provides aggregate outcome reporting, not atomic commit or
rollback. A destination that already succeeded keeps the data even if another
destination later fails. Once every branch durably accepts the message, export
completion is owned by the branch-local durable buffers.

Topic defaults:

- `backend = in_memory`
- `impl_selection = engine.topics.impl_selection` (whose default is `auto`)
- `policies.balanced.queue_capacity = 128`
- `policies.balanced.on_full = block`
- `policies.broadcast.queue_capacity = 128`
- `policies.broadcast.on_lag = drop_oldest`
- `policies.broadcast.ack_mode = first`
- `policies.ack_propagation.mode = disabled`
- `policies.ack_propagation.max_in_flight = 1024`
- `policies.ack_propagation.timeout = 30s`

`exporter:topic` may locally override full-queue behavior:

```yaml
nodes:
  publish/raw:
    type: exporter:topic
    config:
      topic: raw_signals
      queue_on_full: drop_newest
```

Exporter-local `queue_on_full` behavior:

- optional (`block` or `drop_newest`)
- precedence: exporter `config.queue_on_full` ->
  topic `policies.balanced.on_full` -> default `block`
- queue capacities remain topic-declaration-only (no exporter-local override)
- broadcast lag handling remains topic-declaration-only via
  `policies.broadcast.on_lag`
- broadcast Ack aggregation remains topic-declaration-only via
  `policies.broadcast.ack_mode`
- Ack/Nack tracking limits remain topic-declaration-only via
  `policies.ack_propagation`

For runnable `all + disconnect` scenarios contrasting retry and durable
acceptance, see
[topic_broadcast_ack_all.yaml](../configs/engine-conf/topic_broadcast_ack_all.yaml).
Compare the same fast-Ack/delayed-Nack branches under `first` and `all` in
[topic_broadcast_ack_modes.yaml](../configs/engine-conf/topic_broadcast_ack_modes.yaml),
or a lagging subscriber terminated by `disconnect` in
[topic_broadcast_disconnect.yaml](../configs/engine-conf/topic_broadcast_disconnect.yaml).

## Engine Section

The optional `engine` section contains engine-wide settings:

- `state_dir`
- `topics`
- `http_admin`
- `telemetry`
- `observed_state`
- `observability`
- `controller.extensions`
- `custom`

`http_admin` is disabled when absent. Its `bind_address` exposes the complete
Admin API, not only metrics; use access controls appropriate to your deployment.

```yaml
engine:
    http_admin:
        bind_address: "127.0.0.1:8080"
```

For `observed_state` liveness/readiness settings and the log configuration,
see [Admin](admin/README.md) and [Internal Telemetry](telemetry/README.md).

### Engine State Directory

`engine.state_dir` is an optional explicit absolute path, provisioned before
pipelines start on Linux. It has no default and cannot be added, removed, or
changed during live reconfiguration. Existing journald and scraper checkpoints
are not automatically migrated. See [Engine state directory](state-directory.md)
for permission, durability, platform, and consumer-adoption requirements.

### Engine Topic Settings

Engine-wide topic runtime defaults are declared at `engine.topics`.

- `engine.topics.impl_selection`:
  - `auto` (default): infer the most efficient runtime variant from topology
  - `force_mixed`: disable topology-based optimization and always use the
    mixed implementation

Per-topic `topics.*.impl_selection` overrides this engine-wide default when set.

### Engine Telemetry

Internal telemetry settings are declared at `engine.telemetry`. The
`reporting_interval` controls the metric-set snapshot cadence and supplies the
internal telemetry receiver's default registry drain and export interval.
`reporting_interval` defaults to `1s` and must be positive;
`reporting_channel_size` defaults to `100`. `logs` selects log providers.
`resource` supplies explicit resource attributes; `detectors` defaults to
`[service_instance, env, service_name]`. Explicit resource values win over
detectors, later detectors win over earlier ones, and an empty detector list
disables detection. The optional `process` detector can disclose command-line
arguments; do not enable it where they contain private values.

The internal telemetry receiver's `signals` field defaults to `[logs,
metrics]`. A custom receiver may select either `[logs]` or `[metrics]`; logs
must remain enabled whenever the global, engine, or admin log provider uses
`its`. When metrics are selected, the optional `metrics.interval` overrides the
receiver's cold-path interval, and `metrics.views` defines supported name and
description transformations.
When metrics are not selected, the receiver still drains their private ITS
export accumulator without converting or emitting OTLP data. This preserves
registry cleanup and does not consume the admin endpoint's metric view.

The Prometheus replacement is the admin endpoint at `/api/v1/metrics`; its bind
address comes from `engine.http_admin`. The path is fixed, receiver views are
not applied, and reading it does not reset or consume snapshots waiting for ITS
export.

The ITS bridge projects each multivariate metric set into standard univariate
OTLP metrics, which normal OTLP and OTAP exporters can consume. This projection
is transitional pending native multivariate metric-set support in OTAP.

### Observability Pipeline

Engine observability pipeline wiring is declared at
`engine.observability.pipeline`. The field cannot be null. When it is omitted,
the engine installs the following topology: metrics are continuously consumed
by the noop exporter, while logs explicitly configured to use ITS are rendered
by the console exporter. The default `console_async` log path bypasses this
pipeline.

```yaml
engine:
  observability:
    pipeline:
      nodes:
        internal:
          type: "urn:otel:receiver:internal_telemetry"
          config: {}
        router:
          type: "urn:otel:processor:type_router"
          outputs: [logs, metrics]
          config: {}
        logs_console:
          type: "urn:otel:exporter:console"
          config: {}
        metrics_noop:
          type: "urn:otel:exporter:noop"
          config: {}
      connections:
        - from: internal
          to: router
        - from: router["logs"]
          to: logs_console
        - from: router["metrics"]
          to: metrics_noop
```

A custom pipeline replaces this default and must retain exactly one connected
`receiver:internal_telemetry`. It uses the same node, connection, and
pipeline-scoped extension model as a regular pipeline. Its extensions can
provide capabilities to observability nodes through `capabilities` bindings.

Optional observability policies are supported at:
`engine.observability.pipeline.policies` for:

- `channel_capacity`
- `health`
- `telemetry`

`resources` is intentionally not supported for observability and is rejected.

### Migrating Internal Metrics from the Rust OpenTelemetry SDK

Move periodic export and views from `engine.telemetry.metrics` into the engine
observability pipeline. For example, this former SDK configuration exported
viewed metrics over OTLP/gRPC and exposed the unviewed metrics to Prometheus:

```yaml
engine:
    telemetry:
        metrics:
            readers:
                - periodic:
                    interval: 60s
                    exporter:
                        type: otlp
                        config:
                            protocol: grpc/protobuf
                            endpoint: http://localhost:50066
                            temporality: delta
                - pull:
                    exporter:
                        type: prometheus
                        config:
                            host: 0.0.0.0
                            port: 9091
                            path: /metrics
            views:
                - selector:
                    scope_name: engine
                    instrument_name: memory.rss
                  stream:
                    name: process_memory_usage
                    description: Process resident memory usage.
```

The equivalent native ITS configuration is:

```yaml
engine:
    http_admin:
        bind_address: "0.0.0.0:9091"
    observability:
        pipeline:
            nodes:
                internal:
                    type: receiver:internal_telemetry
                    config:
                        signals: [metrics]
                        metrics:
                            interval: 60s
                            views:
                                - selector:
                                    scope_name: engine
                                    instrument_name: memory.rss
                                  stream:
                                    name: process_memory_usage
                                    description: Process resident memory usage.
                otlp:
                    type: exporter:otlp_grpc
                    config:
                        grpc_endpoint: http://localhost:50066
            connections:
                - from: internal
                  to: otlp
```

Apply these mappings when migrating:

- Remove `metrics.provider`; ITS is now the only internal metrics path.
- Move `periodic.interval` to the receiver's `metrics.interval`. The receiver
  has one emission interval, so multiple periodic readers must share a cadence.
  Set it explicitly to `6s` if the former periodic-reader default was required.
- Move `metrics.views` unchanged to the receiver's `metrics.views` list.
- Replace a `grpc/protobuf` SDK exporter with `exporter:otlp_grpc`, changing
  `endpoint` to `grpc_endpoint`.
- Replace an `http/protobuf` SDK exporter with `exporter:otlp_http`. The native
  exporter accepts a base `endpoint` and optional full signal-specific endpoint
  overrides. The native exporter does not support the former `http/json` mode.
- Remove exporter `temporality`. Native ITS currently emits its canonical
  low-memory representation: synchronous counters and histograms are delta,
  while up-down and observed counters are cumulative. Instrument types describe
  how component code records measurements; they do not select a
  consumer-specific export preference. The pipeline cannot yet convert this
  representation to another temporality, and `processor:temporal_reaggregation`
  does not perform that conversion. Consequently, a former explicit
  `cumulative` or `delta` preference has no exact native equivalent yet. This
  limitation is tracked in
  [#3543](https://github.com/open-telemetry/otel-arrow/issues/3543).
- Replace a console reader with an `exporter:console` node. Fan out the internal
  receiver to multiple exporters when they share the same emission interval.
- Replace a Prometheus pull reader with `engine.http_admin.bind_address` and
  scrape `/api/v1/metrics`. The path is fixed, receiver views are not applied,
  and the bind address exposes the complete admin API rather than a dedicated
  metrics-only server.
- Use `signals: [metrics]` while log providers retain their defaults. Omit
  `signals`, or include `logs`, when a global, engine, or admin log provider
  uses `its`. To suppress metric conversion and export, select only `logs` and
  route that signal to a sink.

If `engine.telemetry.metrics` was omitted and no internal metrics were exported,
no migration is required: the built-in observability pipeline consumes metrics
with a noop exporter.

### Controller Extensions

Trusted in-process controller extensions can be declared at
`engine.controller.extensions`. Each entry uses a standard extension envelope
with a `type` URN and optional extension-specific `config`. These extensions
run with controller-level handles and are not exposed to pipeline nodes as
extension capabilities.

Available types depend on the factories linked into your distribution.
`--validate-and-exit` checks controller-extension types and their config
without starting them. For implementation guidance, see
[Developing Controller Extensions](../README.md#developing-controller-extensions).

The built-in read-only controller monitor is available in the stock
`df_engine` binary:

```yaml
engine:
  controller:
    extensions:
      controller_monitor:
        type: "urn:otel:extension:controller_monitor"
        config:
          interval: "30s"
          log_snapshots: true
```

The monitor periodically records controller-level metrics under
`controller.monitor` and can emit compact internal snapshot logs. It only reads
observed state and telemetry registry state; it does not reconfigure or shut
down pipelines.

### Custom Embedder Configuration

`engine.custom` is an optional map for applications that embed the dataflow
engine as a library. Embedding binaries can read namespaced keys for their
own concerns; the engine does not interpret their runtime meaning. Reporting
omits these values, so retain the original source for full replacements. See
the [privacy model](configuration-resolution.md#privacy-and-security-model).

```yaml
engine:
  custom:
    remote_management:
      server_url: "ws://mgmt.example.com/v1"
      heartbeat_interval_secs: 10
    custom_auth:
      provider: "oidc"
      token_endpoint: "https://auth.example.com/token"
```

## Validate and Troubleshoot

The engine envelope and closed component schemas reject unknown fields.
Intentionally free-form component payloads and `engine.custom` remain open.

Common validation checks include:

- `version` must be `otel_dataflow/v1`.
- Connections must reference existing nodes.
- Connection sources must be receivers or processors.
- Graphs must not contain cycles.
- Referenced output ports must exist when `outputs` is declared.
- Channel and topic capacities must be non-zero.
- `groups.system` is reserved for engine-managed pipelines.
- `backend: quiver` topics are rejected by the current runtime.
- Node `config` fields must match the selected node type.
- Node types must be registered in the `df_engine` binary.
- Node-level `header_capture` is receiver-only.
- Node-level `header_propagation` is exporter-only.
- Resource and runtime-recovery policies are unsupported on observability.
- Memory-limiter placement and rate-limiter bindings must be valid.
- Registered factories must parse and validate every component and construct
  its declared safe snapshot before admission.

Use `--validate-and-exit` while editing:

```bash
cargo run -- --config path/to/config.yaml --validate-and-exit
```

`--validate-and-exit` compiles context bindings and rejects qualified
propagation selectors that reference unknown composites, unknown or unsupported
members, or conflicting primitive transport-header sources.

If validation fails inside a node config, open that node's README from the
[core-node catalog](../crates/core-nodes/README.md) or
[contrib-node catalog](../crates/contrib-nodes/README.md). If validation fails
in the root model, use the field and policy sections above. Diagnostics do not
forward arbitrary submitted values; inspect your private source locally.

## Related Documentation

- [Runnable examples](../configs/README.md)
- [Core-node catalog](../crates/core-nodes/README.md)
- [Contrib-node catalog](../crates/contrib-nodes/README.md)
- [Development-node catalog](../crates/dev-nodes/README.md)
- [URN reference](urns.md)
- [Processor behavior taxonomy](processors.md)
- [Transport header policies](transport-headers.md)
- [Node and flow metrics](node-and-flow-metrics.md)
- [Memory limiter policy](memory-limiter-phase1.md)
- [Proxy support](proxy-support.md)

Component-specific fields, limits, telemetry, stability, and examples live
beside implementations. Start from the node catalogs rather than duplicating
those schemas here. Their READMEs use predictable headings such as `Metadata`,
`Configuration`, `Examples`, `Telemetry`, and `Limits` for people and agents.
