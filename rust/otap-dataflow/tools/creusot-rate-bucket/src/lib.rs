// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Creusot pilot for the exact arithmetic module compiled by the engine.

#[path = "../../../crates/engine/src/admission/bucket/math.rs"]
mod math;

pub use math::{nanos_for_rate, next_theoretical_arrival};

#[cfg(creusot)]
mod std_specs;

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: zero, invalid, rounded, and saturated rate costs.
    /// Guarantees: the shared helper preserves rate-bucket boundary behavior.
    #[test]
    fn rate_cost_boundaries() {
        for (allow, interval, units, expected) in [
            (0, 0, 0, 0),
            (0, 10, 1, u64::MAX),
            (10, 0, 1, u64::MAX),
            (3, 10, 5, 17),
            (u64::MAX, u64::MAX, u64::MAX, u64::MAX),
            (1, u64::MAX, 2, u64::MAX),
            (u64::MAX, 1, 1, 1),
        ] {
            assert_eq!(nanos_for_rate(allow, interval, units), expected);
        }
    }

    /// Scenario: idle refill, capped debt, stale limits, and saturated timestamps.
    /// Guarantees: time never regresses and the debt clamp preserves existing debt.
    #[test]
    fn arrival_time_boundaries() {
        for (current, now, cost, debt_limit, expected) in [
            (0, 0, 0, 0, 0),
            (10, 100, 5, 200, 105),
            (100, 100, 20, 110, 110),
            (200, 100, 10, 150, 200),
            (u64::MAX - 1, u64::MAX - 1, 10, u64::MAX, u64::MAX),
            (u64::MAX, 0, u64::MAX, 0, u64::MAX),
        ] {
            assert_eq!(
                next_theoretical_arrival(current, now, cost, debt_limit),
                expected
            );
        }
    }
}
