// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared production checkpoint transitions.
#![allow(dead_code)]

#[path = "../../../crates/filelog-checkpoint/src/replay_values.rs"]
mod replay_values;
pub use replay_values::*;
#[path = "../../../crates/filelog-checkpoint/src/replay/transitions.rs"]
mod transitions;

#[path = "../../../crates/filelog-checkpoint/src/replay/ownership.rs"]
mod ownership;

#[cfg(creusot)]
mod requirements;
#[cfg(creusot)]
mod transaction;

#[cfg(test)]
mod tests;
