// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Existing commit-offset calculation, isolated so its exhaustion can be checked.

#[cfg(creusot)]
use creusot_std::prelude::*;

// Desired safe exhaustion policy. The current implementation does not satisfy it.
#[cfg_attr(creusot, ensures(match lowest {
    Some(low) => result == Some(low),
    None => match high {
        None => result == None,
        Some(high) => if high@ == i64::MAX@ { result == None }
            else { match result { None => false, Some(next) => next@ == high@ + 1 } },
    },
}))]
pub(super) fn committable_offset(lowest: Option<i64>, high: Option<i64>) -> Option<i64> {
    lowest.or_else(|| high.map(|offset| offset + 1))
}
