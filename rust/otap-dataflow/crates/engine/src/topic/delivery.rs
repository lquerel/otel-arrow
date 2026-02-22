// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::{TopicOutcomeInterest, TopicOutcomeNack, TopicRuntimeError};
use async_trait::async_trait;

#[async_trait]
pub(crate) trait DeliveryAckHandler<T>: Send {
    /// Returns publisher outcome interest for this delivery.
    fn outcome_interest(&self) -> TopicOutcomeInterest {
        TopicOutcomeInterest::None
    }

    /// Resolves delivery as acknowledged.
    async fn ack(self: Box<Self>, payload: T) -> Result<(), TopicRuntimeError>;

    /// Resolves delivery as rejected.
    async fn nack(
        self: Box<Self>,
        payload: T,
        nack: TopicOutcomeNack,
    ) -> Result<(), TopicRuntimeError>;
}

enum DeliveryAckHandlerSlot<T> {
    None,
    Handler(Box<dyn DeliveryAckHandler<T>>),
}

/// Delivery-side outcome handle that can be stored independently from payload flow.
pub struct TopicDeliveryOutcomeHandle<T> {
    ack_handler: DeliveryAckHandlerSlot<T>,
}

impl<T> TopicDeliveryOutcomeHandle<T> {
    pub(crate) fn new(ack_handler: Box<dyn DeliveryAckHandler<T>>) -> Self {
        Self {
            ack_handler: DeliveryAckHandlerSlot::Handler(ack_handler),
        }
    }

    fn none() -> Self {
        Self {
            ack_handler: DeliveryAckHandlerSlot::None,
        }
    }

    /// Returns publisher interest for this delivery.
    #[must_use]
    pub fn publisher_outcome_interest(&self) -> TopicOutcomeInterest {
        match &self.ack_handler {
            DeliveryAckHandlerSlot::None => TopicOutcomeInterest::None,
            DeliveryAckHandlerSlot::Handler(handler) => handler.outcome_interest(),
        }
    }

    /// Resolves this delivery as acknowledged using the provided payload.
    pub async fn ack(self, payload: T) -> Result<(), TopicRuntimeError> {
        match self.ack_handler {
            DeliveryAckHandlerSlot::None => Ok(()),
            DeliveryAckHandlerSlot::Handler(handler) => handler.ack(payload).await,
        }
    }

    /// Resolves this delivery as rejected using the provided payload and details.
    pub async fn nack_with_reason(
        self,
        payload: T,
        nack: TopicOutcomeNack,
    ) -> Result<(), TopicRuntimeError> {
        match self.ack_handler {
            DeliveryAckHandlerSlot::None => Ok(()),
            DeliveryAckHandlerSlot::Handler(handler) => handler.nack(payload, nack).await,
        }
    }

    /// Resolves this delivery as rejected using a default transient reason.
    pub async fn nack(self, payload: T) -> Result<(), TopicRuntimeError> {
        self.nack_with_reason(
            payload,
            TopicOutcomeNack::transient("topic delivery rejected"),
        )
        .await
    }
}

/// Delivered message wrapper that carries acknowledgement operations.
pub struct TopicDelivery<T> {
    payload: T,
    ack_handler: DeliveryAckHandlerSlot<T>,
}

impl<T> TopicDelivery<T> {
    /// Creates a runtime delivery object.
    pub(crate) fn new(payload: T, ack_handler: Box<dyn DeliveryAckHandler<T>>) -> Self {
        Self {
            payload,
            ack_handler: DeliveryAckHandlerSlot::Handler(ack_handler),
        }
    }

    /// Creates a runtime delivery object without Ack/Nack settlement handling.
    pub(crate) fn new_without_ack(payload: T) -> Self {
        Self {
            payload,
            ack_handler: DeliveryAckHandlerSlot::None,
        }
    }

    /// Returns the delivered payload.
    #[must_use]
    pub fn payload(&self) -> &T {
        &self.payload
    }

    /// Returns publisher outcome interest for this delivery.
    #[must_use]
    pub fn publisher_outcome_interest(&self) -> TopicOutcomeInterest {
        match &self.ack_handler {
            DeliveryAckHandlerSlot::None => TopicOutcomeInterest::None,
            DeliveryAckHandlerSlot::Handler(handler) => handler.outcome_interest(),
        }
    }

    /// Splits this delivery into payload + outcome handle.
    ///
    /// Useful when payload forwarding and Ack/Nack resolution happen in different
    /// stages (for example through pipeline control messages).
    pub fn into_parts(self) -> (T, TopicDeliveryOutcomeHandle<T>) {
        let TopicDelivery {
            payload,
            ack_handler,
        } = self;
        let outcome_handle = match ack_handler {
            DeliveryAckHandlerSlot::None => TopicDeliveryOutcomeHandle::none(),
            DeliveryAckHandlerSlot::Handler(handler) => TopicDeliveryOutcomeHandle::new(handler),
        };
        (payload, outcome_handle)
    }

    /// Resolves this delivery as acknowledged.
    pub async fn ack(self) -> Result<(), TopicRuntimeError> {
        let TopicDelivery {
            payload,
            ack_handler,
        } = self;
        match ack_handler {
            DeliveryAckHandlerSlot::None => Ok(()),
            DeliveryAckHandlerSlot::Handler(handler) => handler.ack(payload).await,
        }
    }

    /// Resolves this delivery as rejected with explicit reason.
    pub async fn nack_with_reason(self, nack: TopicOutcomeNack) -> Result<(), TopicRuntimeError> {
        let TopicDelivery {
            payload,
            ack_handler,
        } = self;
        match ack_handler {
            DeliveryAckHandlerSlot::None => Ok(()),
            DeliveryAckHandlerSlot::Handler(handler) => handler.nack(payload, nack).await,
        }
    }

    /// Resolves this delivery as rejected.
    pub async fn nack(self) -> Result<(), TopicRuntimeError> {
        self.nack_with_reason(TopicOutcomeNack::transient("topic delivery rejected"))
            .await
    }
}
