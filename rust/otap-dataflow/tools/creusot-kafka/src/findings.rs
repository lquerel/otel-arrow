// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Requirement probe expected to fail on the unchanged saturation policy.
use crate::next_delivery_generation;
use creusot_std::prelude::*;

/// No excluded boundary: the old token must become unusable after another allocation.
#[ensures(result)]
fn exhausted_generation_is_fresh() -> bool {
    let old = u64::MAX;
    let next = next_delivery_generation(old);
    next != old
}
