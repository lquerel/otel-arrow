// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use crate::topic::{
    DeliveryAckHandler, TopicDelivery, TopicOutcomeInterest, TopicOutcomeNack, TopicRuntimeError,
    TopicSubscriber,
};
use async_trait::async_trait;
use otap_df_config::TopicName;
use otap_df_config::topic::TopicBroadcastOnLagPolicy;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

pub(super) struct InMemoryBroadcastSubscriber<T> {
    pub(super) topic_name: TopicName,
    pub(super) receiver: broadcast::Receiver<T>,
    // Copied from topic policy at subscription time.
    pub(super) on_lag: TopicBroadcastOnLagPolicy,
    // Subscriber-local redelivery queue used only for nack().
    pub(super) retry_queue: Arc<Mutex<VecDeque<T>>>,
}

impl<T> InMemoryBroadcastSubscriber<T> {
    /// Pops one locally nacked payload queued for subscriber-local redelivery.
    fn pop_retry_payload(&self) -> Result<Option<T>, TopicRuntimeError> {
        let mut queue = self
            .retry_queue
            .lock()
            .map_err(|_| TopicRuntimeError::Internal {
                message: "broadcast retry queue lock poisoned".to_owned(),
            })?;
        Ok(queue.pop_front())
    }
}

#[async_trait]
impl<T> TopicSubscriber<T> for InMemoryBroadcastSubscriber<T>
where
    T: Clone + Send + 'static,
{
    async fn recv(&mut self) -> Result<TopicDelivery<T>, TopicRuntimeError> {
        if let Some(payload) = self.pop_retry_payload()? {
            return Ok(TopicDelivery::new(
                payload,
                Box::new(BroadcastAckHandler {
                    retry_queue: self.retry_queue.clone(),
                }),
            ));
        }

        let payload = loop {
            match self.receiver.recv().await {
                Ok(payload) => break payload,
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(TopicRuntimeError::ChannelClosed {
                        topic: self.topic_name.clone(),
                    });
                }
                // Lag is detected on receiver side in Tokio broadcast.
                // For `drop_oldest`, we keep reading until a valid next payload is available.
                Err(broadcast::error::RecvError::Lagged(_)) => match &self.on_lag {
                    TopicBroadcastOnLagPolicy::DropOldest => continue,
                    TopicBroadcastOnLagPolicy::Disconnect => {
                        return Err(TopicRuntimeError::ChannelClosed {
                            topic: self.topic_name.clone(),
                        });
                    }
                },
            }
        };

        Ok(TopicDelivery::new(
            payload,
            Box::new(BroadcastAckHandler {
                retry_queue: self.retry_queue.clone(),
            }),
        ))
    }
}

struct BroadcastAckHandler<T> {
    retry_queue: Arc<Mutex<VecDeque<T>>>,
}

#[async_trait]
impl<T> DeliveryAckHandler<T> for BroadcastAckHandler<T>
where
    T: Send + 'static,
{
    fn outcome_interest(&self) -> TopicOutcomeInterest {
        TopicOutcomeInterest::None
    }

    async fn ack(self: Box<Self>, _payload: T) -> Result<(), TopicRuntimeError> {
        Ok(())
    }

    async fn nack(
        self: Box<Self>,
        payload: T,
        _nack: TopicOutcomeNack,
    ) -> Result<(), TopicRuntimeError> {
        // Broadcast nack is modeled as local redelivery for this subscriber only.
        let mut queue = self
            .retry_queue
            .lock()
            .map_err(|_| TopicRuntimeError::Internal {
                message: "broadcast retry queue lock poisoned".to_owned(),
            })?;
        queue.push_back(payload);
        Ok(())
    }
}
