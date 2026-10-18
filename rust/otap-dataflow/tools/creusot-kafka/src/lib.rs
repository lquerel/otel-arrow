// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
//! Production Kafka scalar state transitions and explicit protocol models.
#![allow(dead_code)]
#[path = "../../../crates/contrib-nodes/src/receivers/kafka_receiver/state_kernels.rs"]
mod state_kernels;
pub use state_kernels::*;
#[cfg(any(not(creusot), feature = "known-findings"))]
#[path = "../../../crates/contrib-nodes/src/receivers/kafka_receiver/offset_successor.rs"]
mod offset_successor;

#[cfg(all(creusot, feature = "known-findings"))]
mod findings;
#[cfg(creusot)]
mod model;
#[cfg(creusot)]
mod requirements;
#[cfg(test)]
mod tests;
