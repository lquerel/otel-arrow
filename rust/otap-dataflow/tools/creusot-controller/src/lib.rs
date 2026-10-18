// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Controller lifecycle scalar kernels and their independent requirements.
#![allow(dead_code)]
#[path = "../../../crates/controller/src/live_control/lifecycle_kernels.rs"]
mod lifecycle_kernels;
pub use lifecycle_kernels::*;

#[cfg(all(creusot, feature = "known-findings"))]
mod findings;
#[cfg(creusot)]
mod protocol;
#[cfg(creusot)]
mod requirements;
#[cfg(test)]
mod tests;
