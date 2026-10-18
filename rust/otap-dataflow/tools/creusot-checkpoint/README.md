# Checkpoint transition verification

This standalone target verifies the production Filelog progress and reset
kernels in [`transitions.rs`](../../crates/filelog-checkpoint/src/replay/transitions.rs),
using the actual durable types in
[`replay_values.rs`](../../crates/filelog-checkpoint/src/replay_values.rs).
The production replay paths call these same function bodies. There is no
verification-only replacement algorithm or simplified record type.

The branch builds on `chore/creusot-rate-bucket-verification`. It reuses that
branch's pinned toolchain image, build script, logging, and rejection checks.
Normal production builds retain their compiler and dependencies.

## Run

Use the Linux x86_64 prerequisites and
[toolchain pins](../creusot-rate-bucket/README.md#tooling-pins) from the shared
infrastructure. From the repository root:

```sh
python3 rust/otap-dataflow/tools/creusot-checkpoint/verify.py
```

After the shared verifier image has been built:

```sh
python3 rust/otap-dataflow/tools/creusot-checkpoint/verify.py --skip-build
```

`--image NAME --skip-build` permits an explicitly selected experimental image;
reports retain its actual image ID. CI builds the pinned image once and runs
both rate-bucket and checkpoint verification with it. This target has its own
Cargo.lock, matching `creusot-std = "=0.14.0"`, nightly pin, and solver
settings. It selects Alt-Ergo, CVC5, and Z3 from the same checksum-verified
bundle.

The executable tests also run with the ordinary compiler:

```sh
cd rust/otap-dataflow
cargo +1.98.1 test --manifest-path tools/creusot-checkpoint/Cargo.toml --locked
```

## Requirements and contracts

The existing [checkpoint format requirements][format] are the authority for
these contracts. The acceptance predicates describe valid transitions
independently of the executable branch ordering. Equality between acceptance
and the predicate checks both rejection of invalid requests and acceptance of
valid requests, preventing a validator that rejects everything from passing.

The shared kernels establish:

- Ordinary progress requires the expected active epoch and committed offset;
  accepted progress never moves backward within that unchanged epoch.
- The frontier window has the required length and canonical empty digest, and
  continuation coordinates fit the new offset. A zero-delta update preserves
  the guard and continuation exactly.
- Below a known record end, continuation preserves its origin/end and advances
  its fragment index. After that end, later continuation cannot overlap it.
- Progress application sets offset, guard, continuation, timestamp, and optional
  terminal lifecycle together on return, preserving identity and other evidence.
- Truncation requires exactly the next representable epoch and resets the cursor
  to zero with clean framing and the canonical empty guard. Epoch exhaustion
  rejects; it has no excluded-integer-boundary precondition.
- Quarantine resets follow their requested beginning/end policy, increment the
  epoch, clear quarantine evidence, and install replacement fingerprint evidence.
- `keep_failed` preserves every operational field, including last-seen time,
  even when the WAL carries a different audit-event timestamp.
- Rejected resets leave the complete record unchanged.

[`requirements.rs`](src/requirements.rs) contains separate consumer proofs:
accepted progress applies its cursor consistently, early closure is rejected,
reset invalidates old-epoch updates, finalization rejects later progress,
`keep_failed` is a full no-op, and rejected reset preserves the record.
These consumers use the kernel contracts rather than duplicating their bodies.

The ordinary tests exercise the byte-100/known-end-150 continuation example,
truncation followed by delayed old-epoch progress, the final epoch increment,
quarantine evidence mutations, zero-delta evidence, and terminal finalization.
The production checkpoint crate's codec, replay, and store tests remain needed.

## Proof boundary

These are proofs of local transition requirements for the actual typed values.
There are no application `trusted` functions, external specifications, or input
preconditions that exclude integer boundaries in the kernels. Runtime code and
proof code share their executable bodies; only contracts, logical definitions,
and verification trait implementations are conditional on `cfg(creusot)`.
The proof relies on Creusot's standard-library specifications, compiler
translation, Why3, and the selected prover.

The following remain caller obligations outside this first target:

- Selecting the record associated with an operation's file ID, namespace and
  administrative authorization, and operation ordering through the table.
- Complete-record validity and resource limits. Reset helpers operate on staged
  records; replay validates their resulting guard shape, fingerprint bounds,
  profile, and other structural invariants before publishing anything.
- Full transaction rollback, sequence advancement, locator uniqueness, and map
  staging/commit correspondence. Existing integration tests cover examples;
  this target does not formally prove those orchestration paths.
- The prepare/commit progress fast path's guarantee that the validated record
  stays unchanged before application. The scanner rejects duplicate progress
  IDs before that path; its codec and distinctness proof are outside this target.
- Source-derived facts: a scan-to-LF continuation's actual LF boundary, Ack
  authorization, reset-to-end evidence, fingerprint correspondence, and guard
  digest authenticity. Structural guard checks do not prove SHA-256 correctness
  or that a digest matches source bytes.
- Filesystem durability, crash recovery, scheduling, concurrency, allocation
  failure, termination, and performance.

In particular, "together on return" is a single-threaded function postcondition,
not a proof of crash atomicity or of a machine-atomic multi-field update.
For a scan-to-LF continuation, clean/later framing is structurally accepted
only under the caller's independent source-boundary obligation. A passing
replay proof cannot establish that missing source evidence.

## Evidence and negative controls

The runner uses disposable source copies; the checkout is never mounted into
verification containers. Each pass translates freshly and disables proof replay
and solver caches. Negative controls select their intended function and use
`why3find-negative.json` (one-second solver budget and depth two). Baseline,
weakened-producer, and restored-source positive proofs use the full budget.
Every unmodified negative-control target must first prove with the exact same
reduced budget; resource-induced failure cannot stand in for mutation detection.
`expected-proofs.json` records the complete reviewed file and obligation
inventory. Missing, unexpected, or empty selections fail.

Seven deliberate implementation defects must fail their intended goals:

| Mutation | Required rejection |
| --- | --- |
| Accept a stale expected epoch | `vc_check_progress` |
| Accept backward progress | `vc_check_progress` |
| Drop continuation before a known end | `vc_check_progress` |
| Replace a zero-delta frontier guard | `vc_check_progress` |
| Keep the old epoch after truncation | `vc_reset_after_truncate` |
| Update last-seen time under `keep_failed` | `vc_reset_quarantined` |
| Leave a finalized record active | `vc_apply_progress_fields` |

An eighth control weakens the progress acceptance contract. The producer must
still prove, while the independent `accepted_progress_applies_atomically`
consumer must fail. This checks that the contract supplies a guarantee its
consumer needs. Compilation or setup failures never count as bug detection.

The original source is restored between mutations and must prove again after
all controls. Only then does `target/reports/result.json` become `proved`.
Reports include versions, the actual image ID, source hashes, Git revision and
dirty status, baseline translations/certificates, and positive/negative logs.
CI retains separate revision-associated artifacts for 14 days. Maintained
sources, tests, contracts, inventory, and pins are tracked; generated output and
caches are ignored.

Evidence-handling regression tests run without Docker:

```sh
python3 -m unittest discover -s rust/otap-dataflow/tools/creusot-checkpoint -p 'test_*.py'
```

See the [project verification policy](../../docs/creusot-verification.md) for
advisory CI status, ownership, maintenance, and upgrade requirements.

[format]: ../../docs/filelog-checkpoint-format.md#replay-preconditions-idempotency-and-exact-transition-restrictions
