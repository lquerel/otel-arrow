# Checkpoint transition and transaction verification

This standalone target verifies the production Filelog progress and reset
kernels in [`transitions.rs`](../../crates/filelog-checkpoint/src/replay/transitions.rs),
using the actual durable types in
[`replay_values.rs`](../../crates/filelog-checkpoint/src/replay_values.rs).
The production replay paths call these same function bodies. It also verifies
shared ownership decisions in
[`ownership.rs`](../../crates/filelog-checkpoint/src/replay/ownership.rs).

Transaction-wide properties are established separately for the finite-map
protocol in [`transaction.rs`](src/transaction.rs). That model abstracts complete
record payloads and validation decisions. The concrete `HashMap` orchestration
is connected through shared decisions, source review, and runtime conformance
tests; its implementation is not fully verified by Creusot.

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

## Transaction rollback and ownership

The transaction model starts with consistent records and an ownership index.
Its invariant is bidirectional: every live record has exactly its claim, and
every claim points to a live record with that locator. This implies that two
live records cannot share a locator, and forbids stale index entries as well as
missing claims. The shared lifecycle selector treats quarantined records as
owners and finalized records as non-owners, including Windows locator bytes.

The model uses arbitrary mathematical integer labels for identities and
locators, finite maps, and an arbitrary integer payload token per record.
Tokens represent the whole opaque record; rollback preserves the maps exactly,
not only their membership or offsets. The sequence retains the runtime's full
`u64` range. There is no small table or transaction-length bound in the proof.

`stage_edit` checks the index while its acceptance contract independently checks
all records for conflicts. `stage_all` processes an arbitrary finite ordered
sequence, including repeated identities, against the evolving staged table.
A decreasing sequence-length variant proves termination of that model fold.
The transaction publishes only after all edits, final record-count admission,
and the final audit/replacement-validation flag succeed. Its contract proves:

- Acceptance if and only if the declarative ordered evaluation, sequence check,
  final admission, and final validation permit it; rejecting everything fails.
- Rejection leaves records, claims, payloads, and sequence exactly unchanged,
  including a failure after a successful prefix or after staging all operations.
- Success publishes the complete evaluated table, advances sequence exactly
  once, stays within the final count limit, and preserves two-way ownership.
- Sequence exhaustion rejects without publishing anything.

A pointwise overlay lemma covers absent entries, replacements, and explicit
`None` tombstones. The shared runtime selector proves that a tombstone masks a
base owner; it must not fall back to that owner. The runtime retains lazy base
lookups, touched-record overlays, and removal-before-insertion commit ordering.
The proof's functional map copies introduce no production copies or scans.

Independent consumers require complete rollback after late failure, collision
rejection, sequence-exhaustion rollback, unique ownership after replay, and
acceptance of an unchanged model transaction. The empty-sequence acceptance
consumer exercises the model fold's base case; the runtime scanner separately
requires nonempty transactions.

The production crate contains an index-free, record-scanning reference model.
It compares all 11,100 command sequences of lengths one through three across
empty, active, quarantined, finalized, and full initial tables with two capacity
settings. Its vocabulary covers registrations, administrative removals, and
non-administrative replacements. It checks complete record equality, rejection
rollback, sequence/generation, and the actual private index in both directions.
This is exhaustive for that bounded vocabulary, not for arbitrary WAL inputs.
A separate case checks late namespace rejection and `u64::MAX` exhaustion.
Existing replay tests cover progress-batch rollback and finalization release.

Run the concrete adapter tests separately from the standalone proof:

```sh
cd rust/otap-dataflow
cargo test --locked -p otel-arrow-dfe-filelog-checkpoint -- --test-threads=1
```

The command serializes the store tests. Parallel execution produced `Locked`
failures locally on both this branch and the unchanged checkpoint baseline;
those pre-existing storage-test failures are outside the transaction proof.

The advisory Creusot workflow now runs that command in a separate runtime job
with the pinned production compiler, in addition to the isolated proof job.
Both job conclusions are needed when assessing the model-to-runtime connection.

## Proof boundary

The local transition and ownership helpers use shared production bodies and
actual typed values. Transaction-wide guarantees apply to the explicit model.
There are no application `trusted` functions or custom external specifications,
and no input preconditions exclude integer boundaries in the production kernels.
Only contracts, logical definitions, and verification trait implementations are
conditional on `cfg(creusot)` in shared sources. The proof relies on Creusot's
standard-library specifications (including finite maps), compiler translation,
Why3, and the selected prover.

The pinned Creusot 0.14 library has no specifications for the `HashMap` mutations
used by replay. We have not filled that gap with assumed application contracts.
Consequently the adapter's refinement to the finite-map model is tested and
reviewed, rather than formally proved. A passing protocol proof alone cannot
establish runtime transaction correctness.

The following remain outside the formal transaction proof:

- Selecting the record associated with an operation's file ID, namespace and
  administrative authorization, and operation ordering through the table.
- Complete-record validity and resource limits. Reset helpers operate on staged
  records; replay validates their resulting guard shape, fingerprint bounds,
  profile, and other structural invariants before publishing anything.
- Concrete `HashMap` staging/commit correspondence, key hashing/equality,
  admission-counter arithmetic, and removal-before-insertion peak capacity.
  The protocol proves rollback, sequence, and ownership over finite maps;
  conformance tests exercise their connection to concrete replay.
- Per-operation namespace, transition, resulting-record validity, and final
  administrative/replacement validation. These are explicit Boolean inputs to
  the protocol. The shared fresh-replacement helper is verified, but the caller's
  membership lookups and the whole replacement policy are not formally proved.
- Establishing the initial two-way index invariant from decoded snapshots, and
  keeping a prepared transaction tied to its unchanged base until commit.
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

Fifteen deliberate defects must fail their intended goals (the last four target
the protocol model; the others mutate shared production code):

| Mutation | Required rejection |
| --- | --- |
| Accept a stale expected epoch | `vc_check_progress` |
| Accept backward progress | `vc_check_progress` |
| Drop continuation before a known end | `vc_check_progress` |
| Replace a zero-delta frontier guard | `vc_check_progress` |
| Keep the old epoch after truncation | `vc_reset_after_truncate` |
| Update last-seen time under `keep_failed` | `vc_reset_quarantined` |
| Leave a finalized record active | `vc_apply_progress_fields` |
| Fall back through a tombstone | `vc_effective_owner` |
| Permit a stolen live claim | `vc_claim_available` |
| Permit an existing replacement identity | `vc_replacement_allowed` |
| Release a quarantined locator | `vc_live_locator` |
| Leak a released model claim | `vc_stage_edit` |
| Publish a rejected model prefix | `vc_replay_transaction` |
| Advance model sequence before validation | `vc_replay_transaction` |
| Ignore final model admission | `vc_replay_transaction` |

Two more controls weaken the progress acceptance and transaction rollback
contracts separately. Each producer must still prove, while its independent
consumer (`accepted_progress_applies_atomically` or `rejected_tail_rolls_back`)
must fail. This checks that each contract supplies a guarantee its consumer
needs. Compilation or setup failures never count as bug detection.

The original source is restored between mutations and must prove again after
all controls. Only then does `target/reports/result.json` become `proved`.
Reports include versions, the actual image ID, source hashes, Git revision and
dirty status, baseline translations/certificates, and positive/negative logs.
Source hashes also identify the concrete adapter and conformance-test source;
the separate runtime job runs those tests, not the isolated proof container.
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
