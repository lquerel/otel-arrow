// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Pure rate-bucket arithmetic, shared by the engine and the Creusot proof target.

#[cfg(creusot)]
use creusot_std::prelude::*;

/// Returns the rounded-up nanosecond cost, saturated to `u64::MAX`.
/// Zero units cost zero; a nonzero charge with a zero rate or interval is invalid.
#[cfg_attr(creusot, ensures(units == 0u64 ==> result == 0u64))]
#[cfg_attr(creusot, ensures(units > 0u64 && (allow == 0u64 || interval_nanos == 0u64) ==> result == u64::MAX))]
#[cfg_attr(creusot, ensures(units > 0u64 && allow > 0u64 && interval_nanos > 0u64 ==> {
    let ideal = (units@ * interval_nanos@ + allow@ - 1) / allow@;
    if ideal <= u64::MAX@ { result@ == ideal } else { result == u64::MAX }
}))]
pub fn nanos_for_rate(allow: u64, interval_nanos: u64, units: u64) -> u64 {
    if units == 0 {
        return 0;
    }
    if allow == 0 || interval_nanos == 0 {
        return u64::MAX;
    }

    let nanos = (u128::from(units) * u128::from(interval_nanos)).div_ceil(u128::from(allow));
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

/// Computes the TAT to publish, clamped so the bucket can never move backwards.
///
/// `debt_limit` caps how far past `now` an over-limit charge may push the
/// bucket, so a sustained flood cannot build unbounded debt that would
/// starve traffic long after the flood stops. The trailing `.max(current)`
/// is the safety net: another thread may already have published a TAT beyond
/// a `debt_limit` this caller computed from an older `now`, and regressing
/// it would hand out capacity that was already spent.
#[cfg_attr(creusot, ensures(result >= current))]
#[cfg_attr(creusot, ensures(result <= if current > debt_limit { current } else { debt_limit }))]
#[cfg_attr(creusot, ensures(current >= debt_limit ==> result == current))]
#[cfg_attr(creusot, ensures(result@ == {
    let base = if current > now { current@ } else { now@ };
    let sum = base + cost@;
    let saturated = if sum > u64::MAX@ { u64::MAX@ } else { sum };
    let capped = if saturated < debt_limit@ { saturated } else { debt_limit@ };
    if capped > current@ { capped } else { current@ }
}))]
pub fn next_theoretical_arrival(current: u64, now: u64, cost: u64, debt_limit: u64) -> u64 {
    current
        .max(now)
        .saturating_add(cost)
        .min(debt_limit)
        .max(current)
}
