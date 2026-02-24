// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Experimental in-memory topic runtime v3.
//!
//! This backend uses a disruptor-style ring with sequence cursors for both:
//! - balanced groups (one ring per group, competing consumers)
//! - broadcast subscriptions (one shared ring, one cursor per subscriber)
//!
//! Current scope:
//! - optimized for no-outcome publish path
//! - supports balanced + broadcast subscriptions
//! - outcome tracking is intentionally unsupported in this first v3 iteration

mod balanced;
mod broadcast;
mod ring;
mod runtime;

pub use runtime::{InMemoryPublisherV3, InMemorySubscriberV3, InMemoryTopicRuntimeV3};
