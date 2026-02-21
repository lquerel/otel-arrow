// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! In-memory runtime topic backend.
//!
//! Design notes:
//! - Balanced subscriptions use one shared queue per topic/group (`flume`).
//! - Broadcast subscriptions use one shared Tokio broadcast ring per topic.
//! - Delivery always returns an ack/nack handle so backend semantics can evolve
//!   to persistent/distributed implementations without changing node code.
//! - This backend intentionally fails fast on unsupported policies through the
//!   runtime capability contract (for example `broadcast_on_lag=disconnect`).
//!
//! Ack/Nack behavior:
//! - Transport contract:
//!   - Receiver-side `ack()/nack()` acts on the delivery handle returned by
//!     `TopicSubscriber::recv()`.
//!   - For balanced mode, when publisher outcome interest is enabled, downstream
//!     `ack()/nack()` resolves publisher-side `TopicPublishOutcome`.
//!   - For broadcast mode, `ack()/nack()` resolves publisher-side
//!     `TopicPublishOutcome` for subscribers included in the publish snapshot.
//! - Balanced mode:
//!   - `ack()` resolves publisher outcome when outcome interest is enabled.
//!   - `nack()` has no implicit requeue; it resolves publisher outcome as Nack
//!     when outcome interest is enabled.
//! - Broadcast mode:
//!   - `ack()` resolves that subscriber's settlement for the publish.
//!   - `nack()` resolves that subscriber's settlement as Nack for the publish.
//!   - There is no subscriber-local redelivery queue.
//!
//! Backpressure behavior:
//! - Balanced mode (per-group bounded queue):
//!   - `balanced_on_full=block`: publish awaits free space (`send_async`).
//!   - `balanced_on_full=drop_newest`: publish is non-blocking (`try_send`) and
//!     drops the new message when the queue is full.
//! - Broadcast mode (Tokio broadcast ring):
//!   - Publisher does not block on slow subscribers.
//!   - When subscribers lag past ring capacity, Tokio reports `Lagged` on recv;
//!     with `broadcast_on_lag=drop_oldest`, settlement entries are nacked for
//!     dropped messages and receiver continues with newer ones.
//!   - `broadcast_on_lag=disconnect` is currently unsupported and rejected at
//!     topic creation by the runtime capability contract.

mod balanced;
mod broadcast;
mod outcome_tracker;
mod runtime;
mod topic_state;

pub use runtime::InMemoryTopicRuntime;

#[cfg(test)]
mod tests;
