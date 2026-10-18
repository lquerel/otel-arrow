# Kafka state verification

This target extends the pinned rate-bucket infrastructure to Kafka offset and
transient-Nack replay bookkeeping. Its branch is based on
`chore/creusot-rate-bucket-verification`, independently of the checkpoint and
IdMask pilots. Production dependencies and runtime policies are unchanged.

**Two boundary findings remain unresolved.** The positive proofs do not
establish safe commit-offset or delivery-token exhaustion. The final report
status is `verified_with_known_findings`, rather than `proved`.

## Run

Use the Linux x86_64 prerequisites and
[toolchain pins](../creusot-rate-bucket/README.md#tooling-pins) from the shared
infrastructure. From the repository root:

```sh
python3 rust/otap-dataflow/tools/creusot-kafka/verify.py
```

After the shared image exists:

```sh
python3 rust/otap-dataflow/tools/creusot-kafka/verify.py --skip-build
```

An explicit experimental image requires `--image NAME --skip-build`. Reports
record its actual image ID. This target has its own lockfile and nightly pin,
uses `creusot-std = "=0.14.0"`, and selects the pinned Alt-Ergo, CVC5, and Z3
bundle. Ordinary production builds do not require Creusot or nightly.

Run the concrete adapter tests separately:

```sh
cd rust/otap-dataflow
cargo +1.98.1 test --locked -p otel-arrow-dfe-contrib-nodes --features kafka --lib receivers::kafka_receiver
python3 tools/creusot-kafka/runtime_probes.py
```

The `kafka` feature is required: a feature-disabled test binary can report zero
matching tests. The runtime-probe runner requires the exact executed case and
its diagnostic; empty selections, compilation failures, and unexpected passes
are errors. Its two probes are ignored in the ordinary suite and invoked
explicitly here and by advisory CI. They are diagnostic cases to resolve with
the findings, not promises to preserve unsafe behavior.

Evidence-handling tests need neither Docker nor a broker:

```sh
python3 -m unittest discover -s rust/otap-dataflow/tools/creusot-kafka -p 'test_*.py'
```

## Shared production kernels

The receiver calls the same bodies that the standalone target imports from
[`state_kernels.rs`](../../crates/contrib-nodes/src/receivers/kafka_receiver/state_kernels.rs).
Only contracts, logical definitions, and verification trait implementations
are conditional on `cfg(creusot)`. No alternate production algorithm is hidden
behind that configuration.

The kernels establish:

- Stale tracking leaves scalar state unchanged; a newer ownership period clears
  the previous minimum and acknowledgement evidence.
- Tracking computes the minimum; successful acknowledgement computes the
  maximum acknowledged offset and installs the adapter's new minimum together.
- Rewind preserves its pending barrier and caps old acknowledgement evidence
  below that barrier, including `i64::MIN` predecessor exhaustion.
- Revocation cannot purge a newer ownership period.
- Feedback older than either tracked or assigned ownership is stale. Consulting
  assignment state protects the revoke/reassign window before retracking.
- Only a matching delivery token and recorded deadline can promote Backoff to
  Due. Only a matching token in Due can enter Replaying; completion also needs
  the rewind offset and Replaying phase.
- Attempts and generation increments match their implemented saturation policy
  across the full integer domain. This is not a proof that a saturated token
  is fresh; KAFKA-002 demonstrates the opposite.

Time is an opaque generic value: production uses `Instant`. Equality checks and
phase transitions are shared; clocks, duration arithmetic, deadline ordering,
and the deadline index are outside this proof. Normal trait bounds accept the
production clock; verification adds its value model without a runtime dependency.
The receiver retains its single-thread design, existing collection operations,
constant-time cached count, and reused native partition list. The extraction
adds no allocations, synchronization, scans, or scheduling work.

## Pending-set protocol and runtime connection

[`model.rs`](src/model.rs) uses arbitrary finite sets of signed offsets and
mathematical counts. Its minimum is computed recursively and proved to be a
member no greater than any other member. Sparse offsets are allowed; it does
not assume every integer between two Kafka offsets is a message.

Model track, acknowledgement, and rewind preserve exact count cardinality.
Duplicate/stale tracks and unknown acknowledgements follow their respective
no-op policies. Replay keeps the earliest unresolved offset, drops obsolete
pending deliveries, and keeps the commit barrier. Its mathematical commit
boundary never passes a pending offset. Independent consumer proofs connect
actual cursor insertion/acknowledgement contracts to those finite-set minima.

The concrete adapter's `BTreeSet::first`, membership and mutation operations,
HashMaps, signed count deltas, and native `TopicPartitionList` are **not formally
refined to that model**. The pinned library does not provide their required
mutation specifications. No application `trusted` functions or custom external
specifications fill the gap.

A separate Vec-based runtime oracle compares all 22,620 traces of lengths one
through four over a 12-command vocabulary across two partitions. After every
event it checks pending membership, cached minima, high-water marks, ownership
generations, aggregate pending count, commit snapshots, and native partition-list
membership/offsets. Commands include sparse deliveries, duplicate/stale tracks,
known/unknown acknowledgements, replay, and stale/current revocation. This is
exhaustive for that bounded vocabulary, not for arbitrary receiver executions.
Existing receiver and retry tests exercise actual orchestration and broker mocks.

Remaining obligations include ordered broker delivery, mapping feedback to its
issued delivery/ownership identity, terminal versus transient Nack policy,
rebalance callbacks, index consistency, count representability, native commit
outcomes, network errors, durability, concurrency, resource admission, and
performance. Generation classification accepts nonstale ownership as a lower
bound; callers must establish that the feedback identity was actually issued.
The generation guards do not authorize arbitrary future tokens.

## Unresolved findings

These are real boundary failures in constructible runtime state, with formal
requirement failures and runtime reproductions. The probes force extreme values;
they do not demonstrate practical broker exposure or observed production loss.
Neither finding is silently excluded by a precondition on the shared kernels.

### KAFKA-001: commit successor overflow

[`offset_successor.rs`](../../crates/contrib-nodes/src/receivers/kafka_receiver/offset_successor.rs)
retains the original `high_water_mark + 1` calculation. Tracking and acknowledging
`i64::MAX` through the public `OffsetTracker` causes commit-snapshot construction
to panic in the checked test build. With overflow checks disabled the addition
can wrap to a negative offset. The proof of the desired safe successor contract
fails at `vc_committable_offset`.

The runner first proves a checked-add repair in its disposable copy under the
same solver budget, using a separate reviewed repair inventory. It then restores
the original and requires its proof to fail. This control demonstrates that the
requirement is provable with safe arithmetic; the repair is not applied to the
branch. A production fix should explicitly define exhaustion and review native
partition-list publication when no next offset is representable.

### KAFKA-002: saturated delivery token reuse

`DeliveryGeneration::saturating_next` retains `u64::MAX` after exhaustion.
`RetryManager` therefore allocates the same token for the next replay. Its
runtime probe seeds that allocator at the boundary and demonstrates that feedback
carrying the previous delivery's token still resolves to current ownership.
`exhausted_generation_is_fresh` fails the required freshness postcondition.

The implemented saturating arithmetic is proved without exclusions. A separate
consumer proves freshness before exhaustion; it does not establish freshness at
exhaustion. Fixing the policy requires a fail-closed allocation/receiver-lifetime
decision, rather than merely weakening the freshness requirement or allowing wrap.

When fixing either finding, move its safe production function into the passing
inventory, turn its diagnostic runtime probe into an ordinary regression test,
and remove/update the expected-failure disposition and control. Unexpected
success is deliberately rejected until that review happens.

## Proof evidence and negative controls

The runner mounts only disposable copies. Every pass retranslates, disables
proof replay/solver caches, and forces fresh Why3 proof search. The reviewed
positive and repair inventories reject missing, unexpected, changed, or empty
obligations. The repair inventory describes only the temporary checked-add
control; it is not evidence that the unchanged implementation satisfies it.

Twelve deliberate defects must fail their intended producer goals: stale/equal
generation confusion, retained old acknowledgement evidence, incorrect minima
or maxima, an unsafe rewind cap, stale revocation, ignored assignment fencing,
stale-token replay, wrong-offset completion, wrong-deadline promotion, duplicate
count increments, and leaked acknowledgement counts. Each unmodified target
first proves under the exact negative-control budget (one second, depth two).

A thirteenth control weakens replay phase preservation. Its producer must still
prove while `stale_replay_cannot_start` fails. Compilation, calibration, missing
selection, or unrelated proof failures never count as mutation detection.

The unchanged known findings are checked separately from these deliberate
mutations. After all checks, restored positive sources must prove again before
`target/reports/result.json` becomes `verified_with_known_findings`. Reports
contain source and adapter hashes, Git revision/dirty state, versions, actual
image ID, baseline certificates/translations, control logs, and the unresolved
finding list. The separate runtime runner writes `target/runtime-probes`.

CI is advisory, outside `rust-required-status-check`, and uploads both evidence
sets for 14 days. The proof runner succeeds when its positive scope, controls,
and recorded counterexamples are reproduced. A separate CI safety gate fails
while the report is not `proved` or still lists known findings. This keeps the
workflow red for the unresolved requirements, while retaining the positive proof
and runtime evidence; the findings are not accepted as safety successes.
Generated output and caches are ignored; sources, contracts, tests, inventories,
lockfiles, pins, and findings are versioned. The trusted base remains Creusot's
standard specifications, compiler translation, Why3, and the selected solvers.
