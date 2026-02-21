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

/// Delivery-side outcome handle that can be stored independently from payload flow.
pub struct TopicDeliveryOutcomeHandle<T> {
    ack_handler: Option<Box<dyn DeliveryAckHandler<T>>>,
}

impl<T> TopicDeliveryOutcomeHandle<T> {
    pub(crate) fn new(ack_handler: Box<dyn DeliveryAckHandler<T>>) -> Self {
        Self {
            ack_handler: Some(ack_handler),
        }
    }

    /// Returns publisher interest for this delivery.
    #[must_use]
    pub fn publisher_outcome_interest(&self) -> TopicOutcomeInterest {
        self.ack_handler
            .as_ref()
            .map_or(TopicOutcomeInterest::None, |handler| {
                handler.outcome_interest()
            })
    }

    /// Resolves this delivery as acknowledged using the provided payload.
    pub async fn ack(mut self, payload: T) -> Result<(), TopicRuntimeError> {
        let handler = self
            .ack_handler
            .take()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: "delivery ack handler missing during ack".to_owned(),
            })?;
        handler.ack(payload).await
    }

    /// Resolves this delivery as rejected using the provided payload and details.
    pub async fn nack_with_reason(
        mut self,
        payload: T,
        nack: TopicOutcomeNack,
    ) -> Result<(), TopicRuntimeError> {
        let handler = self
            .ack_handler
            .take()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: "delivery ack handler missing during nack".to_owned(),
            })?;
        handler.nack(payload, nack).await
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
    payload: Option<T>,
    ack_handler: Option<Box<dyn DeliveryAckHandler<T>>>,
}

impl<T> TopicDelivery<T> {
    /// Creates a runtime delivery object.
    pub(crate) fn new(payload: T, ack_handler: Box<dyn DeliveryAckHandler<T>>) -> Self {
        Self {
            payload: Some(payload),
            ack_handler: Some(ack_handler),
        }
    }

    /// Returns the delivered payload.
    #[must_use]
    pub fn payload(&self) -> &T {
        self.payload
            .as_ref()
            .expect("payload should be present until ack/nack consumes the delivery")
    }

    /// Returns publisher outcome interest for this delivery.
    #[must_use]
    pub fn publisher_outcome_interest(&self) -> TopicOutcomeInterest {
        self.ack_handler
            .as_ref()
            .map_or(TopicOutcomeInterest::None, |handler| {
                handler.outcome_interest()
            })
    }

    /// Splits this delivery into payload + outcome handle.
    ///
    /// Useful when payload forwarding and Ack/Nack resolution happen in different
    /// stages (for example through pipeline control messages).
    pub fn into_parts(mut self) -> Result<(T, TopicDeliveryOutcomeHandle<T>), TopicRuntimeError> {
        let payload = self
            .payload
            .take()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: "delivery payload missing during split".to_owned(),
            })?;
        let handler = self
            .ack_handler
            .take()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: "delivery ack handler missing during split".to_owned(),
            })?;
        Ok((payload, TopicDeliveryOutcomeHandle::new(handler)))
    }

    /// Resolves this delivery as acknowledged.
    pub async fn ack(mut self) -> Result<(), TopicRuntimeError> {
        let payload = self
            .payload
            .take()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: "delivery payload missing during ack".to_owned(),
            })?;
        let handler = self
            .ack_handler
            .take()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: "delivery ack handler missing during ack".to_owned(),
            })?;
        handler.ack(payload).await
    }

    /// Resolves this delivery as rejected with explicit reason.
    pub async fn nack_with_reason(
        mut self,
        nack: TopicOutcomeNack,
    ) -> Result<(), TopicRuntimeError> {
        let payload = self
            .payload
            .take()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: "delivery payload missing during nack".to_owned(),
            })?;
        let handler = self
            .ack_handler
            .take()
            .ok_or_else(|| TopicRuntimeError::Internal {
                message: "delivery ack handler missing during nack".to_owned(),
            })?;
        handler.nack(payload, nack).await
    }

    /// Resolves this delivery as rejected.
    pub async fn nack(self) -> Result<(), TopicRuntimeError> {
        self.nack_with_reason(TopicOutcomeNack::transient("topic delivery rejected"))
            .await
    }
}
