// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::attributes::AttributeSetHandler;
use crate::descriptor::{
    AttributeField, AttributeValueType, AttributesDescriptor, Instrument, MetricValueType,
    MetricsField, Temporality,
};
use crate::event::{ObservedEvent, ObservedEventReporter};
use crate::metrics::{MetricSet, MetricSetHandler, MetricSetSnapshot};
use crate::registry::TelemetryRegistryHandle;
use crate::self_tracing::LogContext;
use crate::tracing_init::{ProviderSetup, TracingSetup};
use otel_arrow_dfe_config::DeployedPipelineKey;
use tokio::sync::broadcast::error::TryRecvError;

static DESCRIPTOR: MetricsDescriptor = MetricsDescriptor {
    name: "observer.test",
    metrics: &[MetricsField {
        name: "records",
        unit: "1",
        brief: "Test records",
        instrument: Instrument::Counter,
        temporality: Some(Temporality::Delta),
        value_type: MetricValueType::U64,
    }],
};
static ATTRIBUTES: AttributesDescriptor = AttributesDescriptor {
    name: "observer.test.entity",
    fields: &[AttributeField {
        key: "node.id",
        r#type: AttributeValueType::String,
        brief: "Test node",
    }],
};
struct Attributes(Vec<AttributeValue>);
impl AttributeSetHandler for Attributes {
    fn descriptor(&self) -> &'static AttributesDescriptor {
        &ATTRIBUTES
    }
    fn attribute_values(&self) -> &[AttributeValue] {
        &self.0
    }
}
#[derive(Default, Debug)]
struct Metrics;
impl MetricSetHandler for Metrics {
    fn descriptor(&self) -> &'static MetricsDescriptor {
        &DESCRIPTOR
    }
    fn snapshot_values(&self) -> Vec<MetricValue> {
        vec![0u64.into()]
    }
    fn clear_values(&mut self) {}
    fn needs_flush(&self) -> bool {
        false
    }
}
fn event() -> EngineEvent {
    EngineEvent::admitted(
        DeployedPipelineKey {
            pipeline_group_id: "g".into(),
            pipeline_id: "p".into(),
            core_id: 0,
            deployment_generation: 0,
        },
        None,
    )
}
fn publish(source: &EngineObservationHandle) {
    source.record_lifecycle(source.prepare_lifecycle(&event()).unwrap());
}
fn snapshot(key: MetricSetKey, metrics: Vec<MetricValue>) -> MetricSetSnapshot {
    MetricSetSnapshot {
        key,
        descriptor: &DESCRIPTOR,
        measurement_attributes: &[],
        bucket: 0,
        metrics,
    }
}

#[derive(Debug, Default)]
struct DistributionMetrics(crate::instrument::Mmsc);
impl MetricSetHandler for DistributionMetrics {
    fn descriptor(&self) -> &'static MetricsDescriptor {
        static DESCRIPTOR: MetricsDescriptor = MetricsDescriptor {
            name: "observer.test.latency",
            metrics: &[MetricsField {
                name: "latency",
                unit: "s",
                brief: "Test summary",
                instrument: Instrument::Mmsc,
                temporality: None,
                value_type: MetricValueType::F64,
            }],
        };
        &DESCRIPTOR
    }
    fn snapshot_values(&self) -> Vec<MetricValue> {
        vec![self.0.into()]
    }
    fn clear_values(&mut self) {
        self.0 = Default::default();
    }
    fn needs_flush(&self) -> bool {
        true
    }
}
#[derive(Clone, Copy)]
struct Outcome(usize);
impl crate::attributes::MeasurementAttributeSet for Outcome {
    const CARDINALITY: usize = 2;
    const DESCRIPTORS: &'static [MeasurementAttributeDescriptor] =
        &[MeasurementAttributeDescriptor {
            key: "outcome",
            variants: &["ack", "nack"],
        }];
    fn bucket_index(&self) -> usize {
        self.0
    }
}
struct Route(Vec<AttributeValue>);
impl AttributeSetHandler for Route {
    fn descriptor(&self) -> &'static AttributesDescriptor {
        static DESCRIPTOR: AttributesDescriptor = AttributesDescriptor {
            name: "observer.test.route",
            fields: &[AttributeField {
                key: "route",
                r#type: AttributeValueType::String,
                brief: "Route",
            }],
        };
        &DESCRIPTOR
    }
    fn attribute_values(&self) -> &[AttributeValue] {
        &self.0
    }
}
impl crate::attributes::AttributeSetKeySchema for Route {
    const KEY_SCHEMA: &'static [crate::attributes::AttributeKeySchema] =
        &[crate::attributes::AttributeKeySchema::Key("route")];
}
impl crate::metrics::MeasurementMetricSetHandler for DistributionMetrics {
    type MeasurementAttributes = Outcome;
}
impl crate::metrics::RegistrationMetricSetHandler for DistributionMetrics {
    type RegistrationAttributes = Route;
}

/// Scenario: the live collector receives a distribution snapshot for one measurement bucket.
/// Guarantees: observers retain item attributes and distributions while the normal export is intact.
#[tokio::test]
async fn collector_preserves_bucket_definitions_and_distributions() {
    let registry = TelemetryRegistryHandle::new();
    let mut observer = registry.observations().subscribe("reader").unwrap();
    let mut set = registry
        .register_metric_set_with_registration_and_measurement_attributes::<DistributionMetrics>(
            Attributes(vec![AttributeValue::String("node".into())]),
            &Route(vec![AttributeValue::String("left".into())]),
        );
    set.with(Outcome(1)).0.record(1.5);
    set.with(Outcome(1)).0.record(2.5);
    let snapshots = set.terminal_snapshots();
    assert_eq!(snapshots.len(), 1);
    let expected = snapshots[0].metrics.clone();
    let (collector, reporter) =
        crate::collector::InternalCollector::new(&Default::default(), registry.clone());
    let cancel = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(Arc::new(collector).run(cancel.clone()));
    reporter
        .report_snapshot(snapshots.into_iter().next().unwrap())
        .await
        .unwrap();
    reporter.flush().await.unwrap();
    let record = observer.try_recv().unwrap();
    let EngineObservation::Metrics(metrics) = &record.data else {
        panic!("expected metrics")
    };
    assert_eq!(metrics.bucket, 1);
    assert_eq!(metrics.measurement_attributes[0].key, "outcome");
    assert_eq!(metrics.measurement_attributes[0].variants, &["ack", "nack"]);
    assert_eq!(
        &*metrics.registration_attributes,
        &[("route".into(), "left".into())]
    );
    assert_eq!(metrics.values, expected);
    assert_eq!(
        registry.drain_metric_export_batch().metric_sets[0].values,
        expected
    );
    cancel.cancel();
    task.await.unwrap().unwrap();
}

/// Scenario: two observers consume a native metric snapshot and its producer unregisters.
/// Guarantees: values and definitions are shared, exact, non-destructive and registry-scoped.
#[test]
fn native_metrics_share_frames_without_draining_exports() {
    let registry = TelemetryRegistryHandle::new();
    let source = registry.observations();
    let mut a = source.subscribe("a").unwrap();
    let mut b = source.subscribe("b").unwrap();
    let metrics: MetricSet<Metrics> =
        registry.register_metric_set(Attributes(vec![AttributeValue::String("node".into())]));
    let key = metrics.key;
    let large = (1u64 << 54) + 1;
    registry.collect_snapshot(snapshot(key, vec![large.into()]));
    let first = a.try_recv().unwrap();
    let second = b.try_recv().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(first.registry_epoch, source.registry_epoch());
    assert_ne!(
        first.registry_epoch,
        TelemetryRegistryHandle::new()
            .observations()
            .registry_epoch()
    );
    let EngineObservation::Metrics(set) = &first.data else {
        panic!("expected metrics")
    };
    assert_eq!(set.key, key);
    assert!(std::ptr::eq(set.descriptor, &DESCRIPTOR));
    assert_eq!(set.values, vec![large.into()]);
    assert_eq!(
        set.entity.attribute_values(),
        &[AttributeValue::String("node".into())]
    );
    let batch = registry.drain_metric_export_batch();
    assert_eq!(batch.metric_sets[0].values, vec![large.into()]);
    assert!(registry.drain_metric_export_batch().metric_sets.is_empty());
    assert!(registry.unregister_metric_set(key));
    assert_eq!(set.entity.attribute_values().len(), 1);
    let retained = Arc::downgrade(&first);
    drop(first);
    drop(second);
    assert!(retained.upgrade().is_none());
}

/// Scenario: one observer stops reading while a peer consumes each published record.
/// Guarantees: only the slow observer loses records, with exact lag and a bounded retained tail.
#[test]
fn slow_consumer_has_independent_loss_and_bounded_retention() {
    let source = EngineObservationHandle::new(EngineObserverSettings {
        channel_capacity: 2,
        ..Default::default()
    });
    let mut slow = source.subscribe("slow").unwrap();
    let mut fast = source.subscribe("fast").unwrap();
    let mut oldest = None;
    for n in 1..=5 {
        publish(&source);
        let record = fast.try_recv().unwrap();
        assert_eq!(record.sequence, n);
        if n == 1 {
            oldest = Some(Arc::downgrade(&record));
        }
    }
    assert!(oldest.unwrap().upgrade().is_none());
    assert!(matches!(slow.try_recv(), Err(TryRecvError::Lagged(3))));
    assert_eq!(slow.lagged_observations(), 3);
    assert_eq!(slow.try_recv().unwrap().sequence, 4);
    assert_eq!(slow.try_recv().unwrap().sequence, 5);
    assert!(matches!(slow.try_recv(), Err(TryRecvError::Empty)));
    assert_eq!(fast.lagged_observations(), 0);
}

/// Scenario: subscriptions reach the configured instance cap or reuse an identity.
/// Guarantees: admission is bounded, invalid IDs fail, and dropping a subscriber frees its slot.
#[test]
fn instance_admission_and_release_are_bounded() {
    let source = EngineObservationHandle::new(EngineObserverSettings {
        max_observers: 1,
        ..Default::default()
    });
    assert!(matches!(
        source.subscribe(""),
        Err(ObservationError::InvalidId)
    ));
    assert!(matches!(
        source.subscribe(&"x".repeat(129)),
        Err(ObservationError::InvalidId)
    ));
    let sub = source.subscribe("one").unwrap();
    assert!(matches!(
        source.subscribe("one"),
        Err(ObservationError::InvalidId)
    ));
    assert!(matches!(
        source.subscribe("two"),
        Err(ObservationError::Capacity)
    ));
    drop(sub);
    assert!(!source.active());
    assert!(source.prepare_lifecycle(&event()).is_none());
    let _replacement = source.subscribe("two").unwrap();
    assert_eq!(source.subscriber_count(), 1);
}

/// Scenario: concurrent engine sources publish to the same observation stream.
/// Guarantees: sequence assignment and publication have one consistent total order.
#[test]
fn concurrent_publication_preserves_sequence_order() {
    let source = EngineObservationHandle::new(Default::default());
    let mut sub = source.subscribe("reader").unwrap();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let source = &source;
            let _ = scope.spawn(move || {
                for _ in 0..32 {
                    publish(source);
                }
            });
        }
    });
    for expected in 1..=128 {
        assert_eq!(sub.try_recv().unwrap().sequence, expected);
    }
}

/// Scenario: an oversized lifecycle message and over-capacity metric allocation arrive.
/// Guarantees: source retention rejects them explicitly while normal metric aggregation succeeds.
#[test]
fn oversized_payloads_are_rejected_without_affecting_aggregation() {
    let registry = TelemetryRegistryHandle::new();
    let source = registry.observations();
    let mut sub = source.subscribe("reader").unwrap();
    let mut oversized = event();
    oversized.message = Some("x".repeat(64 * 1024));
    assert!(source.prepare_lifecycle(&oversized).is_none());
    assert!(matches!(
        sub.try_recv().unwrap().data,
        EngineObservation::Rejected(ObservationKind::Lifecycle)
    ));
    let metrics: MetricSet<Metrics> =
        registry.register_metric_set(Attributes(vec![AttributeValue::String("node".into())]));
    let mut values = Vec::with_capacity(257);
    values.push(17u64.into());
    registry.collect_snapshot(snapshot(metrics.key, values));
    assert!(matches!(
        sub.try_recv().unwrap().data,
        EngineObservation::Rejected(ObservationKind::Metrics)
    ));
    assert_eq!(source.rejected_observations(), 2);
    assert_eq!(
        registry.drain_metric_export_batch().metric_sets[0].values,
        vec![17u64.into()]
    );
}

/// Scenario: entity metadata is excessively deep or wide, or a boxed distribution is collected.
/// Guarantees: admission bounds traversal work and accounts for distribution allocations.
#[test]
fn metadata_work_and_distribution_storage_are_admitted_with_limits() {
    let source = EngineObservationHandle::new(Default::default());
    let mut value = AttributeValue::Boolean(true);
    for _ in 0..10 {
        value = AttributeValue::Map([("nested".into(), value)].into());
    }
    assert!(!source.budget().attribute(&value, 0));
    let wide = AttributeValue::Map(
        (0..513)
            .map(|n| (n.to_string(), AttributeValue::UInt(n)))
            .collect(),
    );
    assert!(!source.budget().attribute(&wide, 0));
    let distribution = MetricValue::Distribution(DistributionValue::Basic(Box::default()));
    assert!(source.budget().metrics(&[distribution], 1));
    let mut tiny = Budget {
        left: size_of::<MetricValue>(),
        nodes: 512,
    };
    assert!(!tiny.metrics(
        &[MetricValue::Distribution(DistributionValue::Basic(
            Box::default()
        ))],
        1
    ));
}

/// Scenario: a normal engine log and an observer-produced named OTel event are emitted.
/// Guarantees: outputs reach normal logging with identity but never feed any observer input.
#[test]
fn output_events_are_exported_and_excluded_from_all_observer_inputs() {
    let registry = TelemetryRegistryHandle::new();
    let source = registry.observations();
    let mut producer = source.subscribe("producer").unwrap();
    let mut peer = source.subscribe("peer").unwrap();
    let emitter = producer.emitter();
    let (tx, rx) = flume::bounded(4);
    let reporter = ObservedEventReporter::new(Default::default(), tx).with_observation(registry);
    let setup = TracingSetup::new(
        ProviderSetup::InternalAsync { reporter },
        serde_yaml::from_str("info").unwrap(),
        LogContext::new,
    );
    setup.with_subscriber_ignoring_env(|| {
        crate::otel_info!("engine.sample", records = 1);
        crate::observer_event!(emitter, INFO, "observer.sample", records = 2);
    });
    assert!(matches!(
        producer.try_recv().unwrap().data,
        EngineObservation::Log(_)
    ));
    assert!(matches!(
        peer.try_recv().unwrap().data,
        EngineObservation::Log(_)
    ));
    assert!(matches!(producer.try_recv(), Err(TryRecvError::Empty)));
    assert!(matches!(peer.try_recv(), Err(TryRecvError::Empty)));
    let _normal = rx.try_recv().unwrap();
    let ObservedEvent::Log(output) = rx.try_recv().unwrap() else {
        panic!("expected log")
    };
    assert_eq!(
        output.record.callsite_id.0.metadata().name(),
        "observer.sample"
    );
    assert_eq!(
        output.record.callsite_id.0.metadata().target(),
        OBSERVER_OUTPUT_TARGET
    );
    assert!(
        output
            .record
            .body_attrs_bytes
            .windows("producer".len())
            .any(|s| s == b"producer")
    );
}
