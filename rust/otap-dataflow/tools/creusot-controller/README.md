# Controller lifecycle Creusot investigation

This target checks recovery ownership during rollout, reconciliation, and
shutdown handoffs. It shares the executable
[`lifecycle_kernels.rs`](../../crates/controller/src/live_control/lifecycle_kernels.rs)
with production. It also contains an explicitly separate two-store publication
model. Normal builds do not require Creusot or add production dependencies.
This branch preserves the existing runtime behavior, including the finding below.

## Run and tooling

Use the Linux x86_64 Docker environment and exact pins from the
[shared infrastructure](../creusot-rate-bucket/README.md#tooling-pins):
Creusot/creusot-std 0.14.0, nightly-2026-08-03, ordinary Rust 1.98.1, and the
checksum-verified Why3/Why3find and prover bundle. This target selects the bundled
Alt-Ergo, CVC5, and Z3 provers. It has its own locked standalone workspace and
nightly override; the production compiler stays independent.

From the repository root:

```sh
python3 rust/otap-dataflow/tools/creusot-controller/verify.py
python3 -m unittest discover -s rust/otap-dataflow/tools/creusot-controller -p 'test_*.py'
python3 rust/otap-dataflow/tools/creusot-controller/runtime_probes.py
```

After building the shared image once, use `verify.py --skip-build` to reuse it.
`--image NAME --skip-build` records the actual supplied image ID for experiments.
Run the real controller tests from `rust/otap-dataflow`:

```sh
cargo +1.98.1 test --locked -p otel-arrow-dfe-controller --lib
cargo +1.98.1 test --locked --manifest-path tools/creusot-controller/Cargo.toml
```

The two ignored diagnostic probes assert desired requirements and currently fail
on purpose. The runtime runner requires the exact executed test, nonzero exit,
and documented assertion text. Compilation failures, empty selections, and
unexpected success are rejected. These probes are evidence of unresolved
requirements, not ordinary passing regression tests.

## What the positive proofs establish

The reviewed inventory contains 44 translation files and 88 recorded goal entries
(including derived and dependency obligations). It covers:

- Exact ownership classification, including global shutdown, engine operations,
  pipeline rollouts, shutdowns, and planning reservations.
- Recovery launch authorization by worker token, cancellation, preemption,
  candidate generation, and exact next restart ordinal, including `usize::MAX`.
- Promotion requiring the current uncancelled, unpreempted worker and an active
  target, with exact field updates and rejected-call preservation.
- Cleanup that cannot clear a newer worker's candidate or worker ownership.
- Different failure ownership for rollout versus shutdown cancellation.
- Engine operation identity authorization and reservation admission as implemented.
- Completion accounting for cached activity, authoritative active records,
  operations, reservations, and phased shutdown coordinators.

Independent consumers state requirements such as stale-worker no-op behavior,
shutdown fencing, and preservation of active lifecycle work. A contract-weakening
control removes promotion's frame guarantee: its producer must still prove,
while the stale-worker consumer must fail. This checks that the consumer depends
on a meaningful contract rather than merely repeating a passing implementation.

Ten executable mutation controls must fail their intended goals: ignored worker
identity or preemption, wrong candidate or restart ordinal, dead-target promotion,
retained worker ownership, stale cleanup, incorrect shutdown reservation policy,
omitted active records, and foreign engine authorization. Each unmodified target
first proves with the same one-second, depth-two search budget. Uninterrupted
publication and current admission predicates also prove with that budget before
the corresponding desired requirement failures are checked.

## Connection to production and limits

The controller reads its real maps and instance lifecycle under its existing
mutex, calls shared scalar decisions, and applies successful cursor transitions
back to the same recovery record. The snapshot and cursor copies are fixed-size;
the extraction adds no collection allocation. Runtime conformance tests compare
these adapters with independent formulas over 48 ownership snapshots, 4,608 real
launch/promotion calls, and all 128 combinations of completion sources. They
check identity and unrelated field preservation, sibling-core isolation, and a
stale reservation guard retaining a newer shutdown owner. These are finite tests,
not formal refinement proofs or claims that every seeded snapshot is reachable.

Promotion does not independently verify readiness or compare its target with the
candidate field. The existing worker's ordered readiness/call path supplies
those relationships. Map-key binding, actual operation-string equality, token
issuance and counter exhaustion, and call ordering remain caller obligations.
No numeric identity allocator is proved fresh by this target.

The publication protocol is an abstraction of two real phases, not shared
production code. Its formal counterexample is connected to production by a
runtime probe using the actual locked acceptance, cancellation barrier, rollout
commit, override clearing, and delayed publication methods. The probe controls
the interleaving directly; it does not demonstrate its frequency in production.

This work does not prove whole-controller concurrency, scheduler fairness,
wall-clock deadlines, panic behavior, termination of pipeline threads, global
shutdown liveness, readiness observations, persistence, or resource bounds.
Full-config reconciliation applies pipeline changes sequentially; it does not
promise an atomic transaction across all pipelines, and rollback is best effort.
Creusot's standard specifications, translation, Why3, and solvers remain trusted.

## Findings and disposition

### CTRL-001: stale recovery status publication (confirmed bug)

A recovery worker promotes ready generation 1 under the controller mutex and
clears its worker token. Before it publishes the observed serving override,
an explicit rollout reserves ownership, crosses the recovery cancellation
barrier, and commits generation 2. The old worker then unconditionally publishes
generation 1. The real observed store again selects the stale override, despite
the logical deployment already being generation 2.

The desired `late_publication_preserves_new_owner` requirement fails in the
publication model. The runtime diagnostic reproduces the same stale status
selection after a newer rollout commits. This establishes an ownership race in
status publication; it does not establish that live traffic switches back or
that data is lost.

A separate runtime fix should make publication and ownership transfer coherent,
for example by retaining ownership through publication or applying a generation
or revision fence atomically in the observed store. Checking ownership, unlocking,
and then writing status would retain a race. Convert the diagnostic to an
ordinary regression test and move the requirement into the passing inventory
when the fix is implemented.

### CTRL-002: mutation after global shutdown (open policy requirement)

The proposed policy makes global shutdown an absorbing state for new mutation
authority. Current engine and pipeline-reservation admission do not consult the
global latch, although recovery preemption does. The formal desired rejection
requirement fails. The real reconciliation probe requests global shutdown on an
empty engine, then successfully adds an empty group to its configuration.

This is a policy gap requiring agreement about supported post-shutdown control
requests. It does not independently establish a data-loss bug. Decide whether to
reject mutation after shutdown, or explicitly support it and define convergence
and lifecycle handling. Update the contract, probe, and documentation together.
The investigation does not silently install the proposed policy.

## Evidence and CI

Verification runs in disposable copies, retranslates every pass, disables proof
replay and solver caches, and restores the positive sources for a final proof.
The exact reviewed inventory rejects empty, missing, unexpected, or changed
obligations. Compilation, prover setup, and unrelated failures cannot count as
detected mutations or known requirement failures.

`target/reports/result.json` becomes `verified_with_known_findings` only when the
positive proofs, 11 controls, and both requirement failures are reproduced. It
records Git revision/dirty state, actual image ID, input hashes, versions, and
separate confirmed-bug and open-policy lists. Logs and baseline translations and
certificates are retained. The runtime runner writes `target/runtime-probes` with
controller/observed-state source hashes and exact assertion diagnostics.
Generated evidence and caches are ignored; proof sources, tests, inventories,
contracts, findings, manifests, and pins are versioned.

CI reuses the shared pinned image, runs the controller suite and diagnostic
probes in a separate native job, and uploads both evidence sets for 14 days.
The workflow remains advisory, outside `rust-required-status-check`. A separate
safety gate intentionally fails while the confirmed bug or policy requirement
is unresolved; reproducing a known failure is not a safety success. Read the
report status and job conclusion together. No PR is created by this tooling.
