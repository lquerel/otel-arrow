# Contributing to the OTAP Pipeline project

The OTAP Pipeline project is a part of the [OTEL Arrow
Project](https://github.com/open-telemetry/otel-arrow). See the
project-level [CONTRIBUTING][] document.

[CONTRIBUTING]: ../../CONTRIBUTING.md

## OTAP-Dataflow Development Process

Use the xtask commands below depending on the stage of development:

- Run `cargo xtask quick-check` for faster local iteration while working on
  Rust changes. This runs a narrower subset of the full checks and only
  compiles test targets instead of running the full workspace test suite.
- Run `cargo xtask check-benches` when bench targets or bench-only code
  changes.
- Run `cargo xtask check --diagnostics` when you need a timing-oriented summary
  of slow check phases, compile hotspots, or test binaries. See
  [docs/xtask-diagnostics.md](docs/xtask-diagnostics.md).
- Run `cargo xtask check` before sending changes. This is the required full
  validation suite: structure checks, formatting, clippy on `--all-targets`,
  and `cargo test --workspace`.

With [cargo-nextest](https://nexte.st/docs/installation/pre-built-binaries/)
0.9.127 or newer installed, use `cargo check-fast` for the full validation suite
with nextest for unit/integration tests and Cargo for doctests. This is an alias
for `cargo xtask check --nextest`; add `--diagnostics` to collect build timings.
Doctests run even if nextest reports a failure, and either failure makes the
full check fail. `cargo xtask check` remains available with Cargo as its runner.

For faster feedback while editing one crate, use `cargo check -p CRATE` and
`cargo nextest run -p CRATE`. To run all workspace tests without the formatting
and linting steps, use `cargo test-fast` followed by `cargo test-docs`. Nextest
does not run doctests, so `test-fast` alone is not the full test suite. Keep the
full check before sending changes; CI also covers the all-features scope.

The nextest configuration starts measured long-running tests first while
retaining retries, timeouts, and the default CPU-based concurrency. The default
profile's priorities are inherited by CI. Refresh the exact test-name list from
JUnit timings when runtimes change. Local warm-suite measurements motivated
this scheduling choice; performance on CI's smaller, partitioned workers should
be measured separately.

## Test build artifacts

The test profile uses `debug = "line-tables-only"` to retain backtrace file and
line information while reducing test binaries, linking work, and CI archive
I/O. Optimization levels, debug assertions, and overflow checks keep their
normal test-profile settings. For debugger sessions that need variables and
types, restore full debug information for that command:

```sh
CARGO_PROFILE_TEST_DEBUG=2 cargo test -p CRATE
```

Changing this setting requires a separate artifact rebuild. Keep the Rust
toolchain, profile, features, and compiler flags consistent between ordinary
runs to reuse cached artifacts. Compiler caching and linker selection remain
controlled by your Cargo configuration.

## Rust toolchain

Both Rust workspaces pin an exact toolchain version in `rust-toolchain.toml`
rather than tracking the `stable` channel, so local checks and CI build with
the same compiler and a new Rust release never breaks unrelated pull requests.
The pin is a directory override, so run `cargo` from inside the workspace; it
does not apply to commands invoked from elsewhere via `--manifest-path`.

To move to a new toolchain, open a dedicated pull request that bumps `channel`
in both `rust/otap-dataflow/rust-toolchain.toml` and
`rust/contrib/data_engine/rust-toolchain.toml`, and include any lint or
compile fixes the new version requires. `rustup` installs the pinned toolchain
automatically on the next `cargo` invocation.

## Test documentation

Document every test immediately above its declaration with Rust doc comments:

```rust
/// Scenario: <the behavior or condition under test>
/// Guarantees: <the observable invariant protected by the test>
```

Make both statements specific enough for a reviewer to understand the test's
intent and the behavior that must not regress without reading its implementation.

## Test and example server bind addresses

When a test or example starts a listening server, bind it to the loopback
interface (`127.0.0.1`, or `[::1]` for IPv6), not an all-interfaces address
(`0.0.0.0`, or `[::]`/`[::]:0` for IPv6). Prefer an ephemeral port
(`127.0.0.1:0`) and read the assigned port back from the listener.

Windows Defender Firewall exempts loopback binds from its allow/deny prompt, so
a loopback bind avoids the repeated firewall prompts that an all-interfaces bind
triggers on every `cargo test` / `cargo xtask check` rebuild (test binaries are
named by content hash, so a granted exception does not persist across rebuilds).
Production defaults that intentionally serve external traffic may still bind
`0.0.0.0` (or `[::]`).

## Changelog entries

User-facing Rust changes are recorded in
[`CHANGELOG.md`](./CHANGELOG.md). Changelog entries are added per PR as YAML
files under [`.chloggen/`](./.chloggen/) in this directory and collapsed into
the CHANGELOG at release time.

Copy `TEMPLATE.yaml` in the `.chloggen/` directory to a new `.yaml`
file (e.g. `otlp-exporter-fix-data-loss.yaml`) and fill in the fields.

See [`.chloggen/README.md`](./.chloggen/README.md) for the full guide,
including allowed `component:` values and skip conditions.

## Telemetry and logging

All internal logging MUST use the `otel_*` macros from `otel_arrow_dfe_telemetry`
(not `tracing::info!` or `println!`). See the
[Events Guide](docs/telemetry/events-guide.md) details.

TODO: Add metrics information

## Building a Docker image

Run

```bash
docker build \
  --build-context otel-arrow=../../ \
  -f Dockerfile \
  -t df_engine \
  .
