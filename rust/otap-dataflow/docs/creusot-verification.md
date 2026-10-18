# Creusot verification

Creusot is a development and CI verification tool for selected production Rust
kernels. Normal production builds and deployments do not require the verifier or
its nightly compiler. Proof targets are standalone Cargo workspaces under
`tools/`, with their own lockfiles and toolchain pins.

## Current coverage

| Target | Production source | Established properties | Important exclusions |
| --- | --- | --- | --- |
| [Rate bucket](../tools/creusot-rate-bucket/README.md) | `crates/engine/src/admission/bucket/math.rs` | Exact saturated rate cost; monotonic, bounded arrival-time transition; arithmetic panic safety | Atomic protocol, clocks, scheduling, end-to-end admission, and performance |
| [IdMask and bitmap](../tools/creusot-id-mask/README.md) | `crates/query-engine/src/pipeline/id_mask.rs`; `crates/pdata/src/otap/filter/id_bitmap.rs` and `words.rs` | Boolean set composition, bit-level membership and set operations, clearing, empty pool acquisition | Generation rollover; Arrow integration, iteration/popcount, exact eviction policy, termination, allocation behavior, and performance |

The production crate and proof target compile the same function bodies. Proof
annotations and logical definitions are conditional on `cfg(creusot)`. Keep
executable behavior identical between normal and verification builds; do not
hide an alternative algorithm behind that configuration.

A separate model proves properties of its own transitions. It requires an
explicit connection to production through shared implementation or conformance
checks. A passing model proof is not a claim that surrounding async code is
verified. Partition and transform lifecycle behavior is outside these targets.
Bitmap clearing and pool acquisition have an explicit generation-counter
precondition; unrestricted reuse across rollover is not verified.

## Local and CI workflow

From the repository root:

```sh
python3 rust/otap-dataflow/tools/creusot-rate-bucket/verify.py
python3 rust/otap-dataflow/tools/creusot-id-mask/verify.py --skip-build
```

The runner builds pinned tooling, tests the shared functions, proves them with
fresh verification output, rejects deliberate bugs, and rechecks the original
source. The [rate-bucket README](../tools/creusot-rate-bucket/README.md) documents
prerequisites and shared tooling pins. The
[IdMask README](../tools/creusot-id-mask/README.md) documents bitmap contracts,
preconditions, exclusions, and six additional negative controls. Both targets
record evidence separately and use the same toolchain image.

[Creusot CI](../../../.github/workflows/creusot.yml) runs on relevant pull
requests, relevant pushes to main, manual dispatch, and weekly upstream main
checks. PR paths cover admission callers, bitmap storage and filter integration,
query-engine pipeline code, proof sources, toolchain/manifests, and workflow.
Weekly runs cover the full current snapshot even if a path filter missed a
dependency. The workflow does not publish a container image or require registry
credentials.

The workflow is advisory during rollout: its failures remain failures, and it
is intentionally outside `rust-required-status-check`. This does not change
branch protection. Consider making it required for covered changes after
measuring cold-build cost, reliability, proof maintenance, and review ownership.

Only main runs write the dedicated Docker layer cache; PRs restore it. Max mode
retains intermediate compiler and OCaml build layers, which are needed to avoid
rebuilding the multistage toolchain. It shares the repository's Actions cache
quota, so monitor cache size and eviction before widening the proof suite.
Caches accelerate tool provisioning; cached solver results are not used as
proof evidence.

CI uploads source-associated reports and diagnostics even when verification
fails. Tool setup errors, timeouts, missing obligations, and an empty proof
selection cannot be treated as successful verification. Read `result.json`
status together with the job conclusion; the presence of an artifact alone is
not evidence of success.

## Maintaining the guarantee

For a change affecting covered code:

1. Review the intended behavior and receiver-visible examples independently of
   the implementation. Decide boundary and failure policies explicitly.
2. Update implementation, contracts, tests, and this coverage table together
   when behavior or proof scope changes.
3. Check that callers satisfy every precondition and that external
   specifications and other trusted assumptions remain justified.
4. Run ordinary production tests and verification. Proofs complement tests of
   concurrency, integration, overload, and platform behavior.
5. Run negative controls. Do not resolve a proof failure by weakening contracts,
   strengthening assumptions to exclude real inputs, or adding `trusted`
   without explicit semantic review.

Reviewers of covered admission, pdata, and query-engine code also own their
respective proof contracts. A contract proves conformance to the stated
requirements, not that those requirements are complete. Record assumptions and
unverified boundaries next to each target.

Tool upgrades belong in a dedicated maintenance PR. Update Creusot, its matching
`creusot-std`, Rust nightly, Why3/Why3find, solver selection, image digests, package
snapshot/checksums, and documentation coherently. Rebuild the container, run
fresh positive proofs and negative controls, and retain the resulting version
and source evidence. Keep the production compiler independent.

This infrastructure and behavior-preserving extraction are development changes;
they do not change runtime behavior or add production dependencies.
