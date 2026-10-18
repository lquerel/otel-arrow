# Rate-bucket Creusot verification

This standalone target proves the arithmetic in the engine's
[`bucket/math.rs`](../../crates/engine/src/admission/bucket/math.rs).
Production and verification compile the same function bodies. Contracts and
logical imports are enabled only under `cfg(creusot)`. The normal engine build
keeps its existing compiler and dependencies and does not require Creusot.

## Run

The supported verifier environment is Linux x86_64. Install Docker with Buildx
0.26.1 or newer, Bash, Python 3.10 or newer, Git, curl, and sha256sum. Other
platforms can use a Linux x86_64 development machine; normal engine platform
support is unchanged. Run from the repository root:

```sh
python3 rust/otap-dataflow/tools/creusot-rate-bucket/verify.py
```

The first run builds a substantial compiler/OCaml toolchain image. Later local
runs reuse Docker's layer cache. Verification uses disposable input copies and
fresh translation, with proof replay and solver caches disabled. Containers run
with the invoking UID/GID. The checkout is never mounted into verification
containers, so negative controls cannot edit production source.

To build the image independently, then check without rebuilding it:

```sh
rust/otap-dataflow/tools/creusot-rate-bucket/build.sh
python3 rust/otap-dataflow/tools/creusot-rate-bucket/verify.py --skip-build
```

`--image NAME --skip-build` selects an explicitly supplied local image for
experiments; its actual image ID and version output are included in reports.
CI builds the versioned Dockerfile before using `--skip-build`.

Ordinary boundary tests can also run without Docker or Creusot installed:

```sh
cd rust/otap-dataflow
cargo +1.98.1 test --manifest-path tools/creusot-rate-bucket/Cargo.toml --locked
```

The proof target is outside the main Cargo workspace and has its own checked-in
lockfile. Its nightly toolchain pin does not change the production workspace.
Boundary tests alone do not constitute a successful proof run.

## Tooling pins

| Component | Pin |
| --- | --- |
| Creusot and its bundled prover definitions | 0.14.0, commit `65349025c67e69fcd7479f1f420fa5904f69bc27` |
| Verification Rust | `nightly-2026-08-03` |
| Ordinary Rust boundary tests | 1.98.1 |
| Why3 | commit `05bf4ddf55cb3042f34c3bc0ed3aff56a226978a` |
| Why3find | commit `0f054b93c86ac7ba20bca7df5d1563a9a4434a30` from Creusot's pinned `creusot-deps.opam` |
| OCaml | 5.3.0 |
| OCaml package repository | commit `eba24de6376079a3f9dfa4a9c9ca842475cd1bbc` |
| Selected solver | Alt-Ergo 2.6.4 |

Both Rust base images use immutable digests in the Dockerfile. Source archives
and the Why3 package metadata are checksum-verified.
`provers.sha256` fixes the Linux x86_64 prover binaries to the bytes used by
the tested pilot; the image build checks every bundled prover against it.
The pinned Creusot source
provides its Cargo lockfile and Why3/Why3find dependency pins. A narrow,
asserted installer patch makes its child Cargo commands honor that lockfile.
The standalone
Rust dependencies are locked in this directory's Cargo.lock.

The runtime image uses Debian Trixie for compatibility with the bundled solver
binaries. OS packages come from Debian's package repositories at build time;
this recipe does not promise a bit-for-bit reproducible image. Reports identify
the actual image used. Local and CI verification use the same recipe.

## Contracts and trusted boundary

`nanos_for_rate` proves these properties for all `u64` inputs:

- Zero units cost zero, including when the configuration is invalid.
- A nonzero charge with a zero rate or interval returns `u64::MAX`.
- Otherwise the result is the ceiling of `units * interval / allow`, saturated
  to `u64::MAX`.
- Intermediate arithmetic and conversion do not panic or overflow.

`next_theoretical_arrival` proves:

- The result never falls below the previously published arrival time.
- The result is at most `max(current, debt_limit)`.
- If `current >= debt_limit`, the existing arrival time is preserved.
- The result matches the specified saturating-add and clamp transition.

Neither function has a precondition excluding boundary or invalid inputs.
[`src/std_specs.rs`](src/std_specs.rs) supplies two external specifications
missing from Creusot 0.14.0: `u128::div_ceil` and `u64::try_from(u128)`. These are
trusted assumptions about Rust's standard library; their implementations are
not proved here. The toolchain also trusts Creusot's standard-library
specifications, compiler translation, Why3, and the selected solver. No rate
bucket implementation is marked `trusted`.

The atomic CAS loop, memory ordering, clock behavior, end-to-end admission,
scheduler fairness, allocation failure, and runtime performance are outside the
proof boundary. The existing engine bucket tests remain necessary. This is not
a proof of the entire rate limiter or of partition/transform completion.

## Evidence and negative controls

A complete run must:

1. Pass the shared arithmetic boundary tests with the production Rust version.
2. Prove both expected function files and find their named obligations in the
   newly generated proof reports.
3. Reject ceiling division replaced with floor division at `vc_nanos_for_rate`.
4. Reject removal of the final monotonicity clamp at
   `vc_next_theoretical_arrival`.
5. Prove the restored original source again.

A zero-file selection is an error. Compilation, Docker, download, or solver setup
failures do not count as a successful negative control. A rejected mutant shows
that these specific implementation defects are detected; it does not certify
that the specification captures every intended requirement.

`target/reports/` contains logs, generated baseline proof files, the Git revision,
dirty-checkout status, input hashes, and actual container image ID. `result.json`
is marked `proved` only after all checks complete; failures leave it incomplete.
CI uploads these reports for the tested revision. Generated proof files, build
output, temporary source copies, and caches are ignored by Git. Maintained
contracts, test sources, scripts, configuration, and dependency pins are tracked.

The Python evidence-handling tests can run independently:

```sh
python3 -m unittest discover -s rust/otap-dataflow/tools/creusot-rate-bucket -p 'test_*.py'
```

See the [project verification policy](../../docs/creusot-verification.md) for CI,
maintenance, and upgrade expectations.
