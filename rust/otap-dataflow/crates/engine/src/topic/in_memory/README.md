# In-Memory Topic Backend

## Goals

This backend provides a fast, process-local implementation of the topic runtime contract.

It is designed for:
- single-process deployments,
- low-overhead inter-pipeline communication,
- API compatibility with future durable/distributed backends.

## Design Overview

The backend mixes two queueing primitives:

- Balanced subscriptions: one bounded `flume` queue per `(topic, balanced_group)`.
  - Subscribers in the same group compete for one shared queue.
- Broadcast subscriptions: one Tokio `broadcast` ring per topic.
  - Each subscriber gets its own cursor on the shared ring.

Main files:
- `runtime.rs`: topic registry and runtime API implementation.
- `topic_state.rs`: publish fan-out orchestration and per-topic state.
- `balanced.rs`: balanced queue semantics and balanced ack handler.
- `broadcast.rs`: broadcast receive/lag semantics and broadcast settlement handling.
- `outcome_tracker.rs`: publisher outcome tracking and settlement resolution.

## Contracts and Semantics

### Topic creation

`create_topic(...)` validates:
- backend selector must be `in_memory`,
- balanced and broadcast capacities must be greater than zero,
- policy support through capability checks.

### Publish path

Each publish evaluates two destination families:

- balanced destinations: one send attempt per active balanced group,
- broadcast destinations: one ring send covering all current broadcast subscribers.

Returned report values are enqueue-time snapshots.

### Balanced mode

Queue full behavior (`balanced_on_full`):
- `block`: publisher waits for free capacity (`send_async`).
- `drop_newest`: publish does not block; newest message is dropped if queue is full (`try_send`).

Ack/nack behavior:
- subscriber `ack()` resolves publisher outcome as `Ack` when outcome tracking is enabled,
- subscriber `nack(...)` resolves publisher outcome as `Nack(...)`,
- no implicit requeue on nack in balanced mode.

### Broadcast mode

Lag behavior (`broadcast_on_lag`):
- `drop_oldest`: on lag, receiver skips stale messages and continues from newest available entry.
- `disconnect`: currently unsupported in this backend and rejected at create time.

Ack/nack behavior:
- `ack()` resolves this subscriber's settlement for the publish,
- `nack(...)` resolves this subscriber's settlement as Nack for the publish,
- no subscriber-local redelivery queue exists for broadcast.

## Guarantees

- Bounded memory usage for balanced queues and broadcast ring (from configured capacities).
- Backpressure semantics for balanced `block` are enforced.
- Fast-fail on unsupported policy/backend combinations at creation time.
- No hidden requeue in balanced mode on nack.
- Broadcast settlement tracking is bounded per subscriber and respects `drop_oldest`.

## Important Limitations

- Ephemeral backend: no persistence and no recovery across process restart.
- Delivery report is a snapshot at publish time, not an end-to-end delivery guarantee.
- Broadcast lag handling is receive-side behavior (Tokio `Lagged`) with settlement nacks for dropped entries.
