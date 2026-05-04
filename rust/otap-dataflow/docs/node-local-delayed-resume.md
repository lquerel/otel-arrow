# Node-Local Delayed Resume and Wakeup Design

## Status

Proposed design for exploration and prototype work.

## Summary

Replace the current runtime-managed `DelayData` control-plane path with
node-local delayed facilities owned by the processor inbox.

Use two primitives, not one:

- delayed resume for true payload retention and self-resume
- keyed wakeup for timer or callback style work that does not need a retained
  `PData` payload

This keeps delayed self-work conceptually aligned with "re-enqueue to myself
later", removes fake-empty-payload usage, and avoids pushing more node-local
work through the shared runtime-control manager.

## Current State

Today, delayed work is modeled as:

- node sends `RuntimeControlMsg::DelayData`
- runtime manager stores delayed payloads in a global heap
- runtime manager later sends `NodeControlMsg::DelayedData` back to the
  originating node

Relevant engine entry points:

- [`crates/engine/src/control.rs`](../crates/engine/src/control.rs)
- [`crates/engine/src/pipeline_ctrl.rs`](../crates/engine/src/pipeline_ctrl.rs)
- [`crates/engine/src/message.rs`](../crates/engine/src/message.rs)

Current users and actual semantics:

- Retry processor
  - real delayed payload retention
  - no replace or dedup requirement
  - scheduling failure must immediately surface as terminal failure
- Batch processor
  - no real payload retention
  - uses empty `PData` as a one-shot timer
  - logically wants replace or reschedule semantics
  - currently tolerates stale wakeups by ignoring early firings
- Durable buffer processor
  - no real delayed payload retention
  - uses empty `PData` plus calldata as a retry ticket
  - needs deferred retry bookkeeping and wakeup, not retained payload delivery

## Decision

Delayed resume belongs on the node inbox or message channel, not on normal
downstream `PData` senders and not as a runtime-manager-owned delayed-data
service.

Reasoning:

- delayed resume is self-work, not downstream delivery
- processor inbox delivery already owns admission, fairness, and shutdown-drain
  behavior
- downstream `PData` senders would incorrectly couple self-resume to forward-path
  admission and routing
- the runtime-control manager should not remain responsible for a growing class
  of node-local delayed work

Wakeup-only scheduling should be a separate primitive from delayed payload
resume.

Reasoning:

- batch and durable-buffer are currently using `DelayedData` as a callback
  surrogate
- wakeup-only scheduling can be keyed, replaceable, and much cheaper than
  storing full `PData`
- separating the two makes boundedness and shutdown semantics clearer

## Recommended Architecture

Add a node-local scheduler to `ProcessorMessageChannel`.

High-level structure:

- `ProcessorMessageChannel`
  - existing control receiver
  - existing `PData` receiver
  - new local delayed scheduler
- local delayed scheduler
  - bounded delayed-resume queue for `Box<PData>`
  - bounded keyed wakeup set for lightweight wakeups

The scheduler remains node-local and is polled from the same single-threaded
task that already drives processor message delivery.

This is compatible with the current thread-per-core and `LocalSet` execution
model because it does not introduce cross-node synchronization or background
tasks.

## Public API Shape

Prototype the processor-side API along these lines:

```rust
impl<PData> EffectHandler<PData> {
    pub async fn requeue_later(
        &self,
        when: Instant,
        data: Box<PData>,
    ) -> Result<(), Box<PData>>;

    pub async fn set_wakeup(
        &self,
        slot: WakeupSlot,
        when: Instant,
    ) -> Result<(), WakeupError>;

    pub async fn cancel_wakeup(&self, slot: WakeupSlot) -> bool;
}
```

Prototype receive-side delivery:

```rust
enum NodeControlMsg<PData> {
    DelayedData { when: Instant, data: Box<PData> },
    Wakeup { slot: WakeupSlot, when: Instant },
    // existing variants...
}
```

Notes:

- keep `DelayedData` as the receive-side variant in the prototype so retry can
  migrate with minimal churn
- add `Wakeup` as a distinct control message for wakeup-only scheduling
- `WakeupSlot` should be node-local and small, likely an internal enum or
  integer-like identifier rather than a stringly API

## Prototype Mechanics

The prototype needs one more piece beyond the public API: a processor-side way
to ask its own inbox to schedule work and receive an immediate accept or reject
result.

Recommended internal shape:

```rust
enum NodeLocalScheduleRequest<PData> {
    Resume {
        when: Instant,
        data: Box<PData>,
        reply: oneshot::Sender<Result<(), Box<PData>>>,
    },
    SetWakeup {
        slot: WakeupSlot,
        when: Instant,
        reply: oneshot::Sender<Result<(), WakeupError>>,
    },
    CancelWakeup {
        slot: WakeupSlot,
        reply: oneshot::Sender<bool>,
    },
}
```

Prototype wiring:

- `prepare_runtime()` creates a bounded per-processor schedule-request channel
- `EffectHandler` receives the sender side
- `ProcessorMessageChannel` receives the receiver side
- `EffectHandler::requeue_later()` and wakeup methods send a request and await
  the reply
- `MessageChannelCore` drains request-channel traffic, mutates the local
  scheduler, and decides whether the request is accepted or rejected

Why this is the right prototype mechanism:

- it keeps the scheduler owned by the inbox, not by the effect handler
- it preserves an immediate success or failure result to the caller
- it works for both local and shared processor wrappers
- it keeps shutdown rejection logic at the same boundary that already owns
  drain state

Suggested prototype policy for the request channel:

- bound it separately from stored delayed work
- initially size it from the node control-channel capacity to avoid inventing a
  second policy too early
- drain it before blocking in `recv_with_policy()` and after waking from any
  due-event sleep

## Internal Scheduling Model

Use two internal data structures:

- delayed resume queue
  - bounded min-heap of `(when, sequence, Box<PData>)`
  - append-only
  - no generic dedup or replace semantics
- wakeup set
  - bounded map from `WakeupSlot` to current generation and due time
  - heap entries may become stale
  - last-write-wins replace semantics
  - stale heap entries are discarded when popped

This split matches actual use:

- retry needs retained payloads
- batch needs a replaceable timer
- durable-buffer needs wakeup scheduling plus node-local retry state

## Boundedness Model

Both facilities must remain bounded.

Recommended model:

- delayed resume capacity
  - explicit per-node count bound
  - failure returns the original payload to the caller
- wakeup capacity
  - explicit per-node live-key bound
  - replacing an existing key does not consume extra capacity

Why count bounds are acceptable for the prototype:

- current engine control paths are already message-bounded rather than
  byte-accounted
- the key design question is ownership and semantics, not byte-precise memory
  control

Longer term, delayed resume may also need byte-aware instrumentation if retained
payloads become numerous or large.

## Delivery and Fairness Semantics

Due delayed work should be delivered through the processor inbox path, alongside
other control messages.

Recommended ordering model:

- `Ack` and `Nack` keep their current priority as ordinary control traffic
- due wakeups and delayed resumes are treated as control messages
- `PData` fairness remains governed by the existing control burst limit and
  `accept_pdata()` contract

This preserves the current engine model where the message channel, not each
processor, owns fairness and shutdown latching.

## Shutdown and Drain Semantics

This is the correctness-critical part of the design.

When shutdown is latched for a processor message channel:

- stop accepting new delayed resumes
  - `requeue_later()` returns the payload immediately
- stop accepting new wakeups
  - `set_wakeup()` returns an error immediately
- mark all pending delayed resumes due immediately
- mark all pending wakeups due immediately
- deliver pending delayed self-work before the final `Shutdown`
- release final `Shutdown` only after:
  - buffered input `PData` is drained under the current processor drain policy
  - pending local delayed resumes are delivered
  - pending local wakeups are delivered
  - or the shutdown deadline expires

This intentionally preserves the current manager-side invariant:

- queued delayed data is flushed immediately when drain begins
- newly requested delayed data is returned immediately once drain is active

The redesign changes ownership of the mechanism, not the shutdown contract.

## Migration by Current User

### Retry Processor

Use `requeue_later(when, Box<PData>)`.

Migration details:

- replace the old `effect_handler.delay_data(...)` API with `requeue_later(...)`
- keep handling `NodeControlMsg::DelayedData` for retained payload resume
- preserve current failure semantics where inability to schedule becomes a
  terminal nack

No wakeup primitive is needed here.

### Batch Processor

Use keyed wakeups, one key per active batch buffer.

Migration details:

- replace fake empty `PData` timer payloads
- replace stale-wakeup suppression logic with key replacement semantics
- map each format and signal buffer to a stable wakeup slot

This removes the current "old wakeup fires but should be ignored" behavior from
node logic and moves it into the scheduler.

### Durable Buffer Processor

Move retry state fully local and use wakeups instead of retry tickets.

Migration details:

- replace empty retry-ticket `PData` with local retry metadata
- keep `retry_scheduled` style bookkeeping, but store next due time and retry
  count locally
- use either:
  - one keyed wakeup per bundle, or
  - one aggregate wakeup slot that schedules the next earliest due retry

Recommendation:

- use one aggregate wakeup slot such as `RetryDue`
- keep per-bundle retry metadata in a local map
- on wakeup, process all due retries up to `max_in_flight` and reschedule the
  next earliest due retry

This avoids creating a wakeup entry per bundle while preserving bounded retry
state.

## Prototype Scope

Prototype this as an engine-internal module first.

Suggested shape:

- new module under `crates/engine/src/`
- owned by `ProcessorMessageChannel` or `MessageChannelCore`
- initially processor-only

Do not start with a standalone crate.

Reason:

- the main risk is integration with inbox delivery, fairness, and shutdown
- a standalone scheduler crate could prove heap behavior while hiding the actual
  engine risk
- the difficult logic lives at the engine boundary, not in the timer heap

If the engine-integrated prototype succeeds and the abstractions stay clean,
extraction can be considered later.

## Concrete File Touch Points

Expected first-wave engine changes:

- `crates/engine/src/control.rs`
  - add `NodeControlMsg::Wakeup`
  - keep `DelayedData` as the retained-payload resume delivery
- `crates/engine/src/effect_handler.rs`
  - add schedule-request sender to `EffectHandlerCore`
  - add `requeue_later()`, `set_wakeup()`, and `cancel_wakeup()`
- `crates/engine/src/local/processor.rs`
  - expose the new methods on the local effect handler
- `crates/engine/src/shared/processor.rs`
  - expose the new methods on the shared effect handler
- `crates/engine/src/message.rs`
  - add the node-local scheduler
  - add request-channel receive support
  - synthesize `DelayedData` and `Wakeup` deliveries from due local work
  - extend shutdown latching to include pending local delayed work
- `crates/engine/src/processor.rs`
  - create and wire the per-processor schedule-request channel in
    `prepare_runtime()`
- `crates/engine/src/testing/processor.rs`
  - wire the schedule-request channel for isolated processor tests

Expected migration changes:

- `crates/core-nodes/src/processors/retry_processor/mod.rs`
  - move from `delay_data()` to `requeue_later()`
- `crates/core-nodes/src/processors/batch_processor/mod.rs`
  - replace fake delayed empty payloads with keyed wakeups
- `crates/core-nodes/src/processors/durable_buffer_processor/mod.rs`
  - replace retry tickets with local retry metadata plus wakeups

Expected removal phase changes after migration:

- `crates/engine/src/pipeline_ctrl.rs`
  - remove delayed-data heap ownership
  - remove drain-time delayed-data flushing logic
  - keep timer and shutdown orchestration intact
- `crates/engine/src/control_plane_metrics.rs`
  - remove or replace runtime-manager delayed-data metrics
- `crates/engine/README.md`
  - update control-plane documentation to reflect node-local ownership

## Prototype Phases

### Phase 1: Engine Scaffolding

Goal:

- add the node-local request channel, scheduler storage, wakeup delivery, and
  shutdown integration without migrating any users yet

Exit criteria:

- message-channel tests can schedule synthetic delayed resumes and wakeups
- shutdown tests prove pending local delayed work drains before final
  `Shutdown`
- no existing processor behavior changes yet

### Phase 2: Retry Migration

Goal:

- migrate retry to `requeue_later()` while preserving current semantics

Why first:

- retry is the only user that truly needs retained payload resume
- it validates the delayed-resume half of the design without mixing in wakeup
  replacement semantics

Exit criteria:

- retry no longer depends on runtime-control `DelayData`
- current retry failure semantics still hold when scheduling is unavailable

### Phase 3: Wakeup Primitive and Batch Migration

Goal:

- migrate batch from fake delayed empty payloads to keyed wakeups

Why next:

- batch is the cleanest wakeup-only user
- its stale-wakeup behavior proves keyed replace semantics are working

Exit criteria:

- batch no longer uses empty delayed `PData`
- batch stale-wakeup suppression logic is removed or materially simplified

### Phase 4: Durable Buffer Migration

Goal:

- replace retry tickets with local retry metadata plus wakeups

Why after batch:

- durable-buffer is the more stateful migration
- it benefits from a wakeup primitive that is already proven in batch

Exit criteria:

- durable-buffer no longer encodes retry tickets into `PData` calldata
- retry backoff and `max_in_flight` behavior still hold

### Phase 5: Remove Runtime-Managed Delayed Data

Goal:

- delete the manager-side delayed-data path and related metrics once all users
  are migrated

Exit criteria:

- no production code calls `RuntimeControlMsg::DelayData`
- runtime manager no longer owns delayed-data heap state

## Prototype Exit Criteria

The prototype is successful if it demonstrates all of the following:

- retry, batch, and durable-buffer no longer need runtime-managed delayed data
- shutdown behavior remains explicit and equivalent in the ways that matter
- delayed self-work makes progress without reintroducing manager-side bottlenecks
- keyed wakeups remove fake payload usage and stale-timer logic from node code
- the resulting engine API is clearer than the current `DelayData` workaround

## Test Strategy

Add focused tests at the engine message-channel level and processor level.

Engine-level tests:

- delayed resume fires when due
- keyed wakeup fires when due
- keyed wakeup replace discards stale entries
- new schedules are rejected during drain
- pending resumes and wakeups are flushed immediately when shutdown latches
- final `Shutdown` is released only after local delayed work is drained or the
  deadline expires
- due local delayed work still makes progress under control bursts

Processor migration tests:

- retry still reschedules retained payloads and preserves terminal failure on
  schedule failure
- batch no longer needs stale-wakeup suppression in node logic
- durable-buffer retries still respect backoff and `max_in_flight`

## Tradeoffs Versus Current `DelayData` Path

Advantages:

- removes node-local delayed work from the runtime-control manager hot path
- eliminates fake empty-payload delayed messages for timer-like work
- aligns delayed self-work with the processor inbox, where shutdown and fairness
  already live
- reduces manager-side delayed-data heap ownership and related control-plane
  metrics complexity

Costs:

- adds per-node scheduler state to processor runtimes
- requires new message-channel integration work
- shifts some observability from one global manager metric set to node-local
  scheduler metrics

## Non-Goals

This design does not change:

- periodic timer ownership for normal repeating `TimerTick`
- receiver-first shutdown orchestration
- completion-lane `Ack` and `Nack` routing

## Final Recommendation

Proceed with a processor-inbox prototype based on two primitives:

- delayed resume primitive for retained `PData`
- keyed wakeup primitive for timer or callback work

Migrate retry to delayed resume.
Migrate batch and durable-buffer to wakeups.

Keep shutdown behavior equivalent to the current drain contract, but move the
ownership from the runtime manager into the node-local processor inbox.
