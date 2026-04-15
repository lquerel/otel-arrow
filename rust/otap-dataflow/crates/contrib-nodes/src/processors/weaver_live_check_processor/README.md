# Weaver Live-Check Processor

The Weaver live-check processor validates incoming telemetry against a
configurable semantic-conventions registry using
[`weaver_live_check`](https://github.com/open-telemetry/weaver/tree/main/crates/weaver_live_check).
It forwards the original telemetry on its default output and emits OTLP log
records describing findings on a fixed `findings` output port.

## Configuration

```yaml
processors:
  live_check:
    registry_path: "./semconv_registry"
```

`registry_path` accepts the same Weaver virtual-directory formats used by the
fake data generator, including local folders, archives, and Git URLs.

## Outputs

- Default output: original telemetry passthrough
- `findings`: OTLP log records for semantic-convention findings

When both outputs are used, declare an explicit `default_output` that is not
`findings`:

```yaml
processors:
  weaver_live_check:
    type: "urn:otel:processor:weaver_live_check"
    outputs: ["telemetry", "findings"]
    default_output: "telemetry"
    config:
      registry_path: "./semconv_registry"
```

The processor emits one log record per finding with:

- `event_name = "weaver.live_check.finding"`
- the finding message as the log body
- severity mapped from Weaver finding level
- `weaver.*` attributes carrying finding id, level, signal metadata, registry
  URL, and JSON context

## Behavior

- OTLP bytes and OTAP records are normalized through the same OTLP decode path
  before checking.
- Findings are best-effort: a missing, full, or closed `findings` port never
  blocks or NACKs the main telemetry path.
- Checker and conversion failures are fail-open: the processor logs an internal
  error, updates processor metrics, and still forwards the original telemetry.

## Building

Enable the `weaver-live-check-processor` feature flag:

```bash
cargo build --features weaver-live-check-processor
```
