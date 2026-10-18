// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Unresolved requirements, selected separately from the positive inventory.
use crate::protocol::Publication;
use crate::*;
use creusot_std::prelude::*;

/// CTRL-001: a pending recovery publication outlives its worker ownership.
#[ensures(result)]
fn late_publication_preserves_new_owner() -> bool {
    let mut cursor = RecoveryCursor {
        serving: 0,
        worker: Some(7),
        candidate: Some(1),
        cancelled: false,
        restarts: 1,
    };
    let mut publication = Publication {
        selected: 0,
        observed: 0,
        pending: None,
    };
    if cursor.promote(7, 1, false, true) {
        publication.accepted_recovery(1);
        publication.commit_new_owner(&cursor, 2);
        publication.publish_pending();
    }
    publication.observed == publication.selected
}

/// CTRL-002: proposed shutdown-absorbing mutation policy; the current engine
/// admission predicate receives no global-shutdown input at all.
#[ensures(!result)]
fn shutdown_closes_mutation_authority() -> bool {
    let owner = Ownership {
        global_shutdown: true,
        engine_operation: false,
        rollout: false,
        shutdown: false,
        reservation: None,
    };
    owner.permits_pipeline_reservation() || permits_engine_operation(false, false)
}
