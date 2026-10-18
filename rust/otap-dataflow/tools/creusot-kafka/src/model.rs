// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Finite pending-set model. Concrete BTreeSet/HashMap refinement remains tested.

use crate::PartitionCursor;
use creusot_std::{logic::FSet, prelude::*};

#[logic(open(crate))]
#[variant(pending.len())]
#[ensures(match result {
    None => pending.is_empty(),
    Some(low) => pending.contains(low) && forall<offset> pending.contains(offset) ==> low@ <= offset@,
})]
pub(crate) fn minimum(pending: FSet<i64>) -> Option<i64> {
    pearlite! {
        if pending.is_empty() { None } else {
            let first = pending.peek();
            match minimum(pending.remove(first)) {
                None => Some(first),
                Some(other) => Some(if first@ < other@ { first } else { other }),
            }
        }
    }
}

pub struct Ledger {
    pub pending: FSet<i64>,
    pub high: Option<i64>,
    pub generation: u64,
    pub count: Int,
}

#[logic(open(crate))]
pub(crate) fn valid(state: Ledger) -> bool {
    state.count == state.pending.len()
}

#[logic(open(crate))]
#[requires(valid(state))]
#[ensures(valid(result))]
#[ensures(generation@ < state.generation@ ==> result == state)]
#[ensures(generation@ >= state.generation@ ==> result.pending.contains(offset))]
pub(crate) fn model_track(state: Ledger, offset: i64, generation: u64) -> Ledger {
    pearlite! {
        if generation@ < state.generation@ { state }
        else if generation@ > state.generation@ {
            Ledger { pending: FSet::singleton(offset), high: None, generation, count: 1 }
        } else {
            Ledger { pending: state.pending.insert(offset), high: state.high, generation,
                count: state.count + if state.pending.contains(offset) { 0 } else { 1 } }
        }
    }
}

#[logic(open(crate))]
#[requires(valid(state))]
#[ensures(valid(result))]
#[ensures(!state.pending.contains(offset) ==> result == state)]
#[ensures(result.generation == state.generation)]
pub(crate) fn model_acknowledge(state: Ledger, offset: i64) -> Ledger {
    pearlite! {
        if !state.pending.contains(offset) { state } else {
            Ledger { pending: state.pending.remove(offset), generation: state.generation,
                count: state.count - 1,
                high: Some(match state.high { None => offset, Some(high) => if high@ > offset@ { high } else { offset } }) }
        }
    }
}

#[logic(open(crate))]
#[requires(valid(state))]
#[ensures(valid(result))]
#[ensures(!state.pending.contains(failed) || generation != state.generation ==> result == state)]
#[ensures(state.pending.contains(failed) && generation == state.generation ==>
    result.count == 1 && result.pending == FSet::singleton(minimum(state.pending).unwrap_logic()))]
pub(crate) fn model_rewind(state: Ledger, failed: i64, generation: u64) -> Ledger {
    pearlite! {
        if generation != state.generation || !state.pending.contains(failed) { state }
        else {
            let lowest = minimum(state.pending).unwrap_logic();
            Ledger { pending: FSet::singleton(lowest), generation, count: 1,
                high: match state.high {
                    None => None,
                    Some(high) => if lowest@ == i64::MIN@ { None } else {
                        // Keep the same typed cap as the runtime's checked subtraction.
                        Some(if high@ < lowest@ - 1 { high } else { lowest - 1i64 })
                    },
                } }
        }
    }
}

/// The mathematical commit boundary handles sparse offsets without inventing pending messages.
#[logic(open(crate))]
#[ensures(forall<p> state.pending.contains(p) ==> match result { None => false, Some(commit) => commit <= p@ })]
pub(crate) fn commit_boundary(state: Ledger) -> Option<Int> {
    pearlite! { match minimum(state.pending) {
        Some(low) => Some(low@),
        None => match state.high { None => None, Some(high) => Some(high@ + 1) },
    } }
}

/// The production cursor's insertion bookkeeping agrees with arbitrary finite pending sets.
#[requires(cursor.last_lowest == minimum(*pending))]
#[ensures((^cursor).last_lowest == minimum((*pending).insert(offset)))]
fn tracked_cursor_matches_set(
    cursor: &mut PartitionCursor,
    pending: Snapshot<FSet<i64>>,
    offset: i64,
) {
    cursor.tracked(offset);
}

/// A concrete set adapter supplying its true new minimum gets exact cursor agreement after ack.
#[requires(new_lowest == minimum((*pending).remove(offset)))]
#[requires((*pending).contains(offset))]
#[ensures((^cursor).last_lowest == minimum((*pending).remove(offset)))]
#[ensures(match (^cursor).high_water_mark { None => false, Some(high) => high@ >= offset@ })]
fn acknowledged_cursor_matches_set(
    cursor: &mut PartitionCursor,
    pending: Snapshot<FSet<i64>>,
    offset: i64,
    new_lowest: Option<i64>,
) {
    let _ = cursor.acknowledged(offset, new_lowest);
}
