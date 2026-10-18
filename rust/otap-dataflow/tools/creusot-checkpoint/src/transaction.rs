// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Finite-map protocol model, not a replacement for the runtime HashMap adapter.
//! Identity/locator labels are arbitrary; payload tokens stand for complete records.

use crate::ownership::{claim_available, effective_owner};
use creusot_std::prelude::Clone;
use creusot_std::{logic::FMap, prelude::*};

#[derive(DeepModel, Clone, Copy)]
pub struct Record {
    pub live: Option<Int>,
    pub payload: Int,
}

pub struct Tables {
    pub records: FMap<Int, Record>,
    pub owners: FMap<Int, Int>,
}

pub struct State {
    pub tables: Tables,
    pub sequence: u64,
}

#[derive(DeepModel, Clone, Copy)]
pub struct Edit {
    pub id: Int,
    pub record: Option<Record>,
    // Namespace, transition, and resulting-record validation are caller inputs.
    pub valid: bool,
}

#[logic(open(crate))]
pub(crate) fn consistent(t: Tables) -> bool {
    pearlite! {
        (forall<id, r: Record> t.records.get(id) == Some(r) ==>
            match r.live { None => true, Some(loc) => t.owners.get(loc) == Some(id) }) &&
        (forall<loc, id> t.owners.get(loc) == Some(id) ==>
            exists<r: Record> t.records.get(id) == Some(r) && r.live == Some(loc))
    }
}

#[logic(open(crate))]
pub(crate) fn live(record: Option<Record>) -> Option<Int> {
    match record {
        None => None,
        Some(r) => r.live,
    }
}

// Declarative admission checks the records, independently of the index lookup.
#[logic(open(crate))]
pub(crate) fn allowed(t: Tables, edit: Edit) -> bool {
    pearlite! { edit.valid && match live(edit.record) {
        None => true,
        Some(loc) => forall<id, r: Record>
            t.records.get(id) == Some(r) && r.live == Some(loc) ==> id == edit.id,
    } }
}

#[logic(open(crate))]
pub(crate) fn edited(t: Tables, edit: Edit) -> Tables {
    pearlite! {
        let old = live(t.records.get(edit.id));
        let new = live(edit.record);
        let owners = if old == new { t.owners } else {
            let released = match old { None => t.owners, Some(loc) => t.owners.remove(loc) };
            match new { None => released, Some(loc) => released.insert(loc, edit.id) }
        };
        Tables {
            records: match edit.record {
                None => t.records.remove(edit.id),
                Some(record) => t.records.insert(edit.id, record),
            },
            owners,
        }
    }
}

#[logic(open(crate))]
#[variant(edits.len())]
pub(crate) fn evaluate(t: Tables, edits: Seq<Edit>) -> Option<Tables> {
    pearlite! {
        if edits.len() == 0 { Some(t) }
        else if !allowed(t, edits[0]) { None }
        else { evaluate(edited(t, edits[0]), edits.pop_front()) }
    }
}

/// Apply one validated edit to a private staged table. Failure may dirty this copy.
#[check(ghost)]
#[requires(consistent(*t))]
#[ensures(result == allowed(*t, edit))]
#[ensures(result ==> consistent(^t) && ^t == edited(*t, edit))]
fn stage_edit(t: &mut Tables, edit: Edit) -> bool {
    if !edit.valid {
        return false;
    }
    let old = t.records.get_ghost(&edit.id).copied();
    let old_locator = match old {
        None => None,
        Some(r) => r.live,
    };
    let new_locator = match edit.record {
        None => None,
        Some(r) => r.live,
    };
    let changed = match (old_locator, new_locator) {
        (None, None) => false,
        (Some(a), Some(b)) => a != b,
        _ => true,
    };
    if changed {
        if let Some(loc) = old_locator {
            let _ = t.owners.remove_ghost(&loc);
        }
        if let Some(loc) = new_locator {
            let owner = t.owners.get_ghost(&loc).copied();
            let same_owner = match owner {
                None => false,
                Some(id) => id == edit.id,
            };
            if !claim_available(owner.is_some(), same_owner) {
                return false;
            }
            let _ = t.owners.insert_ghost(loc, edit.id);
        }
    }
    match edit.record {
        None => {
            let _ = t.records.remove_ghost(&edit.id);
        }
        Some(record) => {
            let _ = t.records.insert_ghost(edit.id, record);
        }
    }
    true
}

#[check(ghost)]
#[requires(consistent(*t))]
#[variant(edits.len())]
#[ensures(result == (evaluate(*t, edits) != None))]
#[ensures(result ==> consistent(^t) && Some(^t) == evaluate(*t, edits))]
fn stage_all(t: &mut Tables, mut edits: Seq<Edit>) -> bool {
    if let Some(edit) = edits.pop_front_ghost() {
        if !stage_edit(t, edit) {
            return false;
        }
        stage_all(t, edits)
    } else {
        true
    }
}

#[logic(open(crate))]
pub(crate) fn transaction_allowed(
    state: State,
    edits: Seq<Edit>,
    tail_valid: bool,
    limit: Int,
) -> bool {
    pearlite! {
        state.sequence@ < u64::MAX@ && tail_valid && match evaluate(state.tables, edits) {
            None => false,
            Some(t) => t.records.len() <= limit,
        }
    }
}

/// Publish only the completely accepted transaction; every rejection preserves all state.
#[check(ghost)]
#[requires(consistent(state.tables))]
#[ensures(result == transaction_allowed(*state, edits, tail_valid, limit))]
#[ensures(!result ==> ^state == *state)]
#[ensures(consistent((^state).tables))]
#[ensures(result ==> (^state).sequence@ == state.sequence@ + 1 &&
    Some((^state).tables) == evaluate(state.tables, edits) &&
    (^state).tables.records.len() <= limit)]
pub(crate) fn replay_transaction(
    state: &mut State,
    edits: Seq<Edit>,
    tail_valid: bool,
    limit: Int,
) -> bool {
    let Some(sequence) = state.sequence.checked_add(1) else {
        return false;
    };
    let mut staged = Tables {
        records: state.tables.records,
        owners: state.tables.owners,
    };
    if !stage_all(&mut staged, edits) {
        return false;
    }
    if !tail_valid || staged.records.len_ghost() > limit {
        return false;
    }
    state.tables = staged;
    state.sequence = sequence;
    true
}

/// Pointwise overlay correspondence includes explicit deletion and untouched entries.
#[logic(open(crate))]
#[ensures(forall<key> result.get(key) == match edits.get(key) {
    None => base.get(key), Some(value) => value,
})]
pub(crate) fn overlay<V>(base: FMap<Int, V>, edits: FMap<Int, Option<V>>) -> FMap<Int, V> {
    base.map(|(_, value)| Some(value))
        .merge(edits, |(_, staged)| staged)
        .filter_map(|(_, value)| value)
}

/// The runtime selector implements the mathematical overlay's deletion semantics.
#[ensures(result == None)]
fn released_overlay_hides_owner(owner: u64) -> Option<u64> {
    effective_owner(Some(owner), Some(None))
}

/// A late admission/audit failure rolls back the complete transaction, including its sequence.
#[check(ghost)]
#[requires(consistent(state.tables))]
#[ensures(^state == *state)]
fn rejected_tail_rolls_back(state: &mut State, edits: Seq<Edit>, limit: Int) {
    let _ = replay_transaction(state, edits, false, limit);
}

/// No-op transactions are accepted, so rejection of every request cannot satisfy the model.
#[check(ghost)]
#[requires(consistent(state.tables))]
#[requires(state.sequence@ < u64::MAX@ && state.tables.records.len() <= limit)]
#[requires(edits.len() == 0)]
#[ensures(result)]
#[ensures((^state).tables == state.tables && (^state).sequence@ == state.sequence@ + 1)]
fn accepts_unchanged_transaction(state: &mut State, edits: Seq<Edit>, limit: Int) -> bool {
    replay_transaction(state, edits, true, limit)
}

/// A claimed locator cannot be stolen even when the caller's semantic validation succeeds.
#[check(ghost)]
#[requires(consistent(state.tables))]
#[requires(edits.len() == 1 && edits[0].valid)]
#[requires(exists<loc, owner> live(edits[0].record) == Some(loc) &&
    state.tables.owners.get(loc) == Some(owner) && owner != edits[0].id)]
#[ensures(!result && ^state == *state)]
fn collision_rolls_back(state: &mut State, edits: Seq<Edit>, limit: Int) -> bool {
    replay_transaction(state, edits, true, limit)
}

/// Exhaustion rejects before any transaction can publish a new record or claim.
#[check(ghost)]
#[requires(consistent(state.tables) && state.sequence@ == u64::MAX@)]
#[ensures(!result && ^state == *state)]
fn sequence_exhaustion_rolls_back(state: &mut State, edits: Seq<Edit>, limit: Int) -> bool {
    replay_transaction(state, edits, true, limit)
}

/// The two-way record/index invariant implies that no two live records share a locator.
#[check(ghost)]
#[requires(consistent(state.tables))]
#[ensures(forall<a, b, ra: Record, rb: Record, loc>
    (^state).tables.records.get(a) == Some(ra) && ra.live == Some(loc) &&
    (^state).tables.records.get(b) == Some(rb) && rb.live == Some(loc) ==> a == b)]
fn replay_preserves_unique_owners(
    state: &mut State,
    edits: Seq<Edit>,
    tail_valid: bool,
    limit: Int,
) {
    let _ = replay_transaction(state, edits, tail_valid, limit);
}
