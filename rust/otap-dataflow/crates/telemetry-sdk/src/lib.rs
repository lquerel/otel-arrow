// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#![doc = include_str!("../README.md")]

/// Backend-independent contracts used by generated event payloads.
pub mod event;

/// Weaver-generated entity, event, and metric-set clients.
pub mod generated;

pub use generated::{entities, events, metrics};
