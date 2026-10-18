// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Decisions shared by the runtime overlays and the finite-map transaction proof.

#[cfg(creusot)]
use creusot_std::prelude::DeepModel as Identity;
#[cfg(creusot)]
use creusot_std::prelude::*;

// The proof needs value equality; normal builds accept every PartialEq identity.
#[cfg(not(creusot))]
pub(super) trait Identity {}
#[cfg(not(creusot))]
impl<T> Identity for T {}

use crate::{LifecycleState, Locator, SnapshotRecord};

/// A tombstone is an explicit release, not a missing overlay entry.
#[cfg_attr(creusot, ensures(result == match staged { Some(owner) => owner, None => base }))]
pub(super) fn effective_owner<I>(base: Option<I>, staged: Option<Option<I>>) -> Option<I> {
    match staged {
        Some(owner) => owner,
        None => base,
    }
}

/// A live claim may be retained by its owner, but cannot be stolen.
#[cfg_attr(creusot, check(ghost))]
#[cfg_attr(creusot, ensures(result == (!occupied || same_owner)))]
pub(super) fn claim_available(occupied: bool, same_owner: bool) -> bool {
    !occupied || same_owner
}

/// A non-administrative replacement must end with a fresh, different identity.
#[cfg_attr(creusot, ensures(result == match replacement {
    None => false,
    Some(id) => id.deep_model() != removed.deep_model() && registered && !existed,
}))]
pub(super) fn replacement_allowed<I: PartialEq + Identity>(
    removed: I,
    replacement: Option<I>,
    registered: bool,
    existed: bool,
) -> bool {
    match replacement {
        None => false,
        Some(id) => id != removed && registered && !existed,
    }
}

/// Quarantined records retain ownership; finalized records release it.
#[cfg_attr(creusot, ensures(result.deep_model() == match record.lifecycle_state {
    LifecycleState::RotatedFinalized => None,
    _ => Some(record.locator.deep_model()),
}))]
pub(super) fn live_locator(record: &SnapshotRecord) -> Option<Locator> {
    (record.lifecycle_state != LifecycleState::RotatedFinalized).then_some(record.locator)
}
