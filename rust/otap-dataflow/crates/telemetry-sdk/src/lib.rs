// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#![doc = include_str!("../README.md")]

/// Weaver-generated entity and metric-set clients.
pub mod generated;

pub use generated::{entities, metrics};
