// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded, non-destructive native observation. One ring is shared by all observers.
//! No callback into extension code runs on a producer or while holding registry locks.

use crate::attributes::{AttributeSetHandler, AttributeValue};
use crate::descriptor::{MeasurementAttributeDescriptor, MetricsDescriptor};
use crate::entity::EntityAttributeSet;
use crate::event::{EngineEvent, EventType, LogEvent};
use crate::instrument::DistributionValue;
use crate::metrics::MetricValue;
use crate::registry::{EntityKey, MetricSetKey};
use otel_arrow_dfe_config::observer::EngineObserverSettings;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Instant, SystemTime};
use tokio::sync::broadcast;

/// Reserved output target. Events emitted through `observer_event!` are exported normally
/// but excluded from every observer's input to prevent observer-to-observer feedback.
pub const OBSERVER_OUTPUT_TARGET: &str = "otel.observer.output";
const MAX_FIELDS: usize = 256;
const MAX_CONTEXT: usize = 32;
const MAX_ATTRIBUTE_NODES: usize = 512;

/// A collected producer metric set before univariate projection or export reset.
/// Values retain producer semantics; they are not a registry export transaction.
#[derive(Debug)]
pub struct ObservedMetricSet {
    /// Complete generational producer key within `registry_epoch`.
    pub key: MetricSetKey,
    /// Entity owning this set.
    pub entity_key: EntityKey,
    /// Shared entity definition, retained even if the producer unregisters.
    pub entity: Arc<EntityAttributeSet>,
    /// Static ordered field schema.
    pub descriptor: &'static MetricsDescriptor,
    /// Measurement bucket index.
    pub bucket: usize,
    /// Static measurement enum definitions, used to interpret the bucket.
    pub measurement_attributes: &'static [MeasurementAttributeDescriptor],
    /// Shared fixed item attributes.
    pub registration_attributes: Arc<[(String, String)]>,
    /// Owned values moved from collection, in descriptor order, including distributions.
    pub values: Vec<MetricValue>,
}

/// Structured internal log plus the definitions its entity context could resolve.
#[derive(Debug)]
pub struct ObservedLog {
    /// Encoded log body/attributes, with static callsite metadata and entity keys.
    pub event: LogEvent,
    /// Resolved context definitions; missing/retired keys are absent.
    pub entities: Vec<(EntityKey, Arc<EntityAttributeSet>)>,
}

/// Observation category for a frame rejected by the payload/work limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationKind {
    /// Producer metric set.
    Metrics,
    /// Structured internal log.
    Log,
    /// Applied lifecycle event.
    Lifecycle,
}

/// Native input shared across independent observers.
#[derive(Debug)]
pub enum EngineObservation {
    /// Collected multivariate metrics, with identity and definitions intact.
    Metrics(ObservedMetricSet),
    /// Structured log event.
    Log(ObservedLog),
    /// Lifecycle event handled by the observed-state store.
    Lifecycle(EngineEvent),
    /// Explicit loss of one oversized or unsupported-shape observation.
    Rejected(ObservationKind),
}

/// An admitted lifecycle copy waiting for the state store to finish applying the event.
pub struct PreparedLifecycleObservation(EngineEvent);

/// Immutable process-local envelope; sequence order is publication order, not cross-signal causality.
#[derive(Debug)]
pub struct ObservationRecord {
    /// Fresh random identity for this registry lifetime; keys are not globally unique alone.
    pub registry_epoch: u128,
    /// Monotonic publication sequence. Lag is reported separately by each receiver.
    pub sequence: u64,
    /// Consumer-host monotonic observation time.
    pub observed_at: Instant,
    /// Wall-clock observation time (producer event time also survives in event payloads).
    pub time: SystemTime,
    /// Shared native payload.
    pub data: EngineObservation,
}

/// Subscription/admission errors, independent of engine data-path errors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ObservationError {
    /// Configured process-wide instance budget reached.
    #[error("engine observer instance limit reached")]
    Capacity,
    /// Instance identity is empty, too long or already subscribed.
    #[error("engine observer ID must be unique, nonempty and at most 128 bytes")]
    InvalidId,
}

/// Bounded shared observation source. It exposes neither registry mutation nor export draining.
#[derive(Clone, Debug)]
pub struct EngineObservationHandle {
    inner: Arc<Hub>,
}
#[derive(Debug)]
struct Hub {
    settings: EngineObserverSettings,
    epoch: u128,
    sender: OnceLock<broadcast::Sender<Arc<ObservationRecord>>>,
    instances: Mutex<HashSet<Arc<str>>>,
    sequence: Mutex<u64>,
    rejected: AtomicU64,
}
impl EngineObservationHandle {
    pub(crate) fn new(settings: EngineObserverSettings) -> Self {
        Self {
            inner: Arc::new(Hub {
                settings,
                epoch: uuid::Uuid::new_v4().as_u128(),
                sender: OnceLock::new(),
                instances: Mutex::new(HashSet::new()),
                sequence: Mutex::new(1),
                rejected: AtomicU64::new(0),
            }),
        }
    }
    /// Registry lifetime identity, also present on every frame.
    #[must_use]
    pub fn registry_epoch(&self) -> u128 {
        self.inner.epoch
    }
    /// Subscribe independently; slow readers lose their own unread prefix, never block publishers.
    pub fn subscribe(&self, id: &str) -> Result<EngineObservationSubscription, ObservationError> {
        let mut instances = self.inner.instances.lock();
        if id.is_empty() || id.len() > 128 || instances.contains(id) {
            return Err(ObservationError::InvalidId);
        }
        if instances.len() >= self.inner.settings.max_observers {
            return Err(ObservationError::Capacity);
        }
        let sender = self
            .inner
            .sender
            .get_or_init(|| broadcast::channel(self.inner.settings.channel_capacity).0);
        let receiver = sender.subscribe();
        let id: Arc<str> = id.into();
        let _ = instances.insert(id.clone());
        Ok(EngineObservationSubscription {
            source: self.clone(),
            id,
            receiver,
            lagged: 0,
        })
    }
    /// Number of live subscriptions, excluding clones of read/output capabilities.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.inner.instances.lock().len()
    }
    /// Source observations rejected by payload/work admission limits.
    #[must_use]
    pub fn rejected_observations(&self) -> u64 {
        self.inner.rejected.load(Ordering::Relaxed)
    }
    pub(crate) fn active(&self) -> bool {
        self.inner
            .sender
            .get()
            .is_some_and(|s| s.receiver_count() != 0)
    }
    pub(crate) fn budget(&self) -> Budget {
        Budget {
            left: self.inner.settings.max_payload_bytes,
            nodes: MAX_ATTRIBUTE_NODES,
        }
    }
    pub(crate) fn publish(&self, data: EngineObservation) {
        let Some(sender) = self.inner.sender.get() else {
            return;
        };
        if sender.receiver_count() == 0 {
            return;
        }
        // Keep assignment and send together: several source threads publish concurrently.
        let mut next = self.inner.sequence.lock();
        let Some(after) = next.checked_add(1) else {
            return;
        };
        let sequence = *next;
        *next = after;
        let _ = sender.send(Arc::new(ObservationRecord {
            registry_epoch: self.inner.epoch,
            sequence,
            observed_at: Instant::now(),
            time: SystemTime::now(),
            data,
        }));
    }
    pub(crate) fn reject(&self, kind: ObservationKind) {
        let _ = self
            .inner
            .rejected
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(1))
            });
        self.publish(EngineObservation::Rejected(kind));
    }
    /// Admit a bounded event copy before state application; does nothing without subscribers.
    #[must_use]
    pub(crate) fn prepare_lifecycle(
        &self,
        event: &EngineEvent,
    ) -> Option<PreparedLifecycleObservation> {
        if !self.active() {
            return None;
        }
        let mut b = self.budget();
        let mut valid = b.take(size_of::<EngineEvent>())
            && b.take(event.key.pipeline_group_id.len())
            && b.take(event.key.pipeline_id.len())
            && b.take(event.node_id.as_ref().map_or(0, |n| n.len()))
            && b.take(event.message.as_ref().map_or(0, String::len));
        if let EventType::Error(error) = &event.r#type
            && let Some(summary) = error.summary()
        {
            valid = valid
                && b.take(summary.error_kind().len())
                && b.take(summary.message().len())
                && b.take(summary.source().map_or(0, str::len))
                && b.take(summary.node().map_or(0, |(n, _)| n.len()));
        }
        if valid {
            Some(PreparedLifecycleObservation(event.clone()))
        } else {
            self.reject(ObservationKind::Lifecycle);
            None
        }
    }
    /// Publish a prepared lifecycle event after releasing the observed-state lock.
    pub(crate) fn record_lifecycle(&self, event: PreparedLifecycleObservation) {
        self.publish(EngineObservation::Lifecycle(event.0));
    }
}

/// An independent receiver over the shared overwrite ring. Do not retain frames without a budget.
pub struct EngineObservationSubscription {
    source: EngineObservationHandle,
    id: Arc<str>,
    receiver: broadcast::Receiver<Arc<ObservationRecord>>,
    lagged: u64,
}
impl EngineObservationSubscription {
    /// Name of this configured instance.
    #[must_use]
    pub fn observer_id(&self) -> &str {
        &self.id
    }
    /// Output capability for this instance. Outputs follow normal telemetry filtering/delivery.
    #[must_use]
    pub fn emitter(&self) -> ObserverEventEmitter {
        ObserverEventEmitter {
            id: self.id.clone(),
        }
    }
    /// Total observations this receiver has skipped because it fell behind.
    #[must_use]
    pub fn lagged_observations(&self) -> u64 {
        self.lagged
    }
    /// Receive one shared record; `Lagged` is an explicit evidence gap, not producer backpressure.
    pub async fn recv(&mut self) -> Result<Arc<ObservationRecord>, broadcast::error::RecvError> {
        let result = self.receiver.recv().await;
        if let Err(broadcast::error::RecvError::Lagged(count)) = &result {
            self.lagged = self.lagged.saturating_add(*count);
        }
        result
    }
    /// Nonblocking receive, useful for bounded draining.
    pub fn try_recv(&mut self) -> Result<Arc<ObservationRecord>, broadcast::error::TryRecvError> {
        let result = self.receiver.try_recv();
        if let Err(broadcast::error::TryRecvError::Lagged(count)) = &result {
            self.lagged = self.lagged.saturating_add(*count);
        }
        result
    }
}
impl Drop for EngineObservationSubscription {
    fn drop(&mut self) {
        let _ = self.source.inner.instances.lock().remove(&self.id);
    }
}

/// Identifies output events without granting access to engine control operations.
#[derive(Clone, Debug)]
pub struct ObserverEventEmitter {
    id: Arc<str>,
}
impl ObserverEventEmitter {
    /// Identity automatically attached by `observer_event!`.
    #[must_use]
    pub fn observer_id(&self) -> &str {
        &self.id
    }
}

/// Emit a named OTel event through the normal logging pipeline. The source ring excludes
/// this reserved target for every observer, including observers other than the emitter.
/// `Ok`/delivery acknowledgement is intentionally not implied by logging.
///
/// ```ignore
/// otel_arrow_dfe_telemetry::observer_event!(context.events, INFO, "observer.sample", count = 42);
/// ```
#[macro_export]
macro_rules! observer_event {
    ($emitter:expr,$level:ident,$name:expr $(,$($fields:tt)+)?)=>{{
        let emitter=&$emitter;
        $crate::otel_event!(target:$crate::observer::OBSERVER_OUTPUT_TARGET,$crate::Level::$level,$name,
            observer_id=emitter.observer_id() $(,$($fields)+)?);
    }};
}

pub(crate) struct Budget {
    left: usize,
    nodes: usize,
}
impl Budget {
    pub fn take(&mut self, size: usize) -> bool {
        if size > self.left {
            return false;
        }
        self.left -= size;
        true
    }
    pub fn entity(&mut self, entity: &EntityAttributeSet) -> bool {
        let values = entity.attribute_values();
        if values.len() > MAX_ATTRIBUTE_NODES || !self.take(size_of_val(values)) {
            return false;
        }
        values.iter().all(|value| self.attribute(value, 0))
    }
    fn attribute(&mut self, value: &AttributeValue, depth: usize) -> bool {
        if self.nodes == 0 || depth > 8 {
            return false;
        }
        self.nodes -= 1;
        match value {
            AttributeValue::String(s) => self.take(s.capacity()),
            AttributeValue::Map(map) => {
                if map.len() > self.nodes {
                    return false;
                }
                map.iter().all(|(key, value)| {
                    self.take(key.capacity() + 3 * size_of::<AttributeValue>())
                        && self.attribute(value, depth + 1)
                })
            }
            _ => true,
        }
    }
    pub fn metrics(&mut self, values: &[MetricValue], capacity: usize) -> bool {
        if values.len() > MAX_FIELDS
            || capacity > MAX_FIELDS
            || !self.take(capacity * size_of::<MetricValue>())
        {
            return false;
        }
        values.iter().all(|value| match value {
            MetricValue::Distribution(DistributionValue::Basic(v)) => self.take(size_of_val(&**v)),
            MetricValue::Distribution(DistributionValue::Normal(v)) => self.take(size_of_val(&**v)),
            MetricValue::Distribution(DistributionValue::Detailed(v)) => {
                self.take(size_of_val(&**v))
            }
            _ => true,
        })
    }
    pub fn registration(&mut self, attrs: &[(String, String)]) -> bool {
        attrs.len() <= MAX_ATTRIBUTE_NODES
            && self.take(size_of_val(attrs))
            && attrs
                .iter()
                .all(|(k, v)| self.take(k.capacity()) && self.take(v.capacity()))
    }
}

/// Maximum context count admitted without cloning unbounded producer data.
pub(crate) const MAX_LOG_CONTEXT: usize = MAX_CONTEXT;

#[cfg(test)]
mod tests;
