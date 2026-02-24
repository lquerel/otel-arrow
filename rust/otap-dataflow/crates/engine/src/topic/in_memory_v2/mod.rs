// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! In-memory topic runtime v2 focused on balanced-mode performance.
//!
//! This implementation uses a native bounded queue per balanced subscription
//! group and keeps dynamic group discovery semantics.

mod runtime;

pub use runtime::{InMemoryPublisherV2, InMemorySubscriberV2, InMemoryTopicRuntimeV2};
