// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Explicit two-store publication protocol. This is a model of the adapter
//! phases, not a proof of their mutex, HashMap, or observed-store refinement.
use crate::RecoveryCursor;
use creusot_std::prelude::*;
use creusot_std::prelude::{Clone, PartialEq};

#[derive(DeepModel, Clone, Copy, PartialEq, Eq)]
pub struct Publication {
    pub selected: u64,
    pub observed: u64,
    pub pending: Option<u64>,
}

impl Publication {
    #[ensures((^self).selected == target && (^self).pending == Some(target) && (^self).observed == self.observed)]
    pub fn accepted_recovery(&mut self, target: u64) {
        self.selected = target;
        self.pending = Some(target);
    }

    /// A newer explicit operation commits and clears the serving override.
    /// The old worker's queued call survives because its worker token was
    /// already released by the production promotion kernel.
    #[requires(cursor.worker == None)]
    #[ensures((^self).selected == target && (^self).observed == target && (^self).pending == self.pending)]
    pub fn commit_new_owner(&mut self, cursor: &RecoveryCursor, target: u64) {
        let _ = cursor;
        self.selected = target;
        self.observed = target;
    }

    /// Exact unguarded publication performed by the current runtime adapter.
    #[ensures((^self).selected == self.selected && (^self).pending == None)]
    #[ensures((^self).observed == match self.pending { None => self.observed, Some(generation) => generation })]
    pub fn publish_pending(&mut self) {
        if let Some(generation) = self.pending {
            self.observed = generation;
            self.pending = None;
        }
    }
}

#[ensures(result)]
fn uninterrupted_publication_is_consistent() -> bool {
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
        publication.publish_pending();
    }
    publication.observed == publication.selected
}
