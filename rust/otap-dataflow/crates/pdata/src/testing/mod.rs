// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Testing utilities for OTLP and OTAP verification.

#[cfg(any(test, feature = "test-internals"))]
pub mod codec;
pub mod equiv;
pub mod fixtures;
pub mod record_batch;
pub mod round_trip;
