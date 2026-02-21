// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use crate::topic::{
    TopicOutcomeInterest, TopicOutcomeNack, TopicPublishOutcome, TopicPublishOutcomeFuture,
    TopicRuntimeError,
};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

pub(super) struct PublishOutcomeTracker {
    state: Mutex<PublishOutcomeTrackerState>,
}

struct PublishOutcomeTrackerState {
    interest: TopicOutcomeInterest,
    // Number of delivered balanced queues waiting for ack/nack.
    pending_deliveries: usize,
    completed: bool,
    tx: Option<oneshot::Sender<TopicPublishOutcome>>,
}

impl PublishOutcomeTracker {
    pub(super) fn new(
        interest: TopicOutcomeInterest,
    ) -> (Arc<Self>, oneshot::Receiver<TopicPublishOutcome>) {
        let (tx, rx) = oneshot::channel();
        (
            Arc::new(Self {
                state: Mutex::new(PublishOutcomeTrackerState {
                    interest,
                    pending_deliveries: 0,
                    completed: false,
                    tx: Some(tx),
                }),
            }),
            rx,
        )
    }

    pub(super) fn interest(&self) -> TopicOutcomeInterest {
        match self.state.lock() {
            Ok(state) => state.interest,
            Err(_) => TopicOutcomeInterest::None,
        }
    }

    pub(super) fn reserve_delivery(&self) {
        if let Ok(mut state) = self.state.lock() {
            if !state.completed {
                state.pending_deliveries += 1;
            }
        }
    }

    pub(super) fn complete_if_idle(&self) {
        if let Ok(mut state) = self.state.lock() {
            if state.completed || state.pending_deliveries != 0 {
                return;
            }
            if let Some(tx) = state.tx.take() {
                let _ignore_closed = tx.send(TopicPublishOutcome::Ack);
            }
            state.completed = true;
        }
    }

    pub(super) fn on_ack(&self) {
        if let Ok(mut state) = self.state.lock() {
            if state.completed {
                return;
            }
            if state.pending_deliveries > 0 {
                state.pending_deliveries -= 1;
            }
            if state.pending_deliveries == 0 {
                if let Some(tx) = state.tx.take() {
                    let _ignore_closed = tx.send(TopicPublishOutcome::Ack);
                }
                state.completed = true;
            }
        }
    }

    pub(super) fn on_nack(&self, nack: TopicOutcomeNack) {
        if let Ok(mut state) = self.state.lock() {
            if state.completed {
                return;
            }
            if let Some(tx) = state.tx.take() {
                let _ignore_closed = tx.send(TopicPublishOutcome::Nack(nack));
            }
            state.completed = true;
            state.pending_deliveries = 0;
        }
    }

    pub(super) fn on_publish_drop(&self, reason: impl Into<String>) {
        self.on_nack(TopicOutcomeNack::transient(reason));
    }
}

pub(super) fn immediate_outcome_future(outcome: TopicPublishOutcome) -> TopicPublishOutcomeFuture {
    Box::pin(async move { Ok(outcome) })
}

pub(super) fn tracker_outcome_future(
    rx: oneshot::Receiver<TopicPublishOutcome>,
) -> TopicPublishOutcomeFuture {
    Box::pin(async move {
        rx.await.map_err(|_| TopicRuntimeError::Internal {
            message: "topic publish outcome receiver unexpectedly closed".to_owned(),
        })
    })
}
