// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Prometheus metrics exporter for pipeline metrics.

use crate::OTAP_EXPORTER_FACTORIES;
use crate::metrics::ExporterPDataMetrics;
use crate::pdata::OtapPdata;
use async_trait::async_trait;
use axum::extract::State;
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use linkme::distributed_slice;
use otap_df_config::SignalType;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::ExporterConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{AckMsg, NackMsg, NodeControlMsg};
use otap_df_engine::error::{Error, ExporterErrorKind};
use otap_df_engine::exporter::ExporterWrapper;
use otap_df_engine::local::exporter::{EffectHandler, Exporter};
use otap_df_engine::message::Message;
use otap_df_engine::message::MessageChannel;
use otap_df_engine::node::NodeId;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_engine::wiring_contract::WiringContract;
use otap_df_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otap_df_pdata::views::metrics::{
    AggregationTemporality, DataPointFlags, DataType, DataView, GaugeView, HistogramDataPointView,
    HistogramView, MetricView, MetricsView, NumberDataPointView, ResourceMetricsView,
    ScopeMetricsView, SumView, SummaryDataPointView, SummaryView, Value as NumberDataValue,
    ValueAtQuantileView,
};
use otap_df_pdata::views::otlp::bytes::metrics::RawMetricsData;
use otap_df_pdata::{OtapPayload, OtlpProtoBytes};
use otap_df_pdata_views::views::common::{AnyValueView, AttributeView, InstrumentationScopeView};
use otap_df_pdata_views::views::resource::ResourceView;
use otap_df_telemetry::attributes::AttributeValue;
use otap_df_telemetry::descriptor::{Instrument, MetricValueType, Temporality};
use otap_df_telemetry::metrics::{MetricSet, MetricValue};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::hash::{Hash, Hasher};
use std::net::TcpListener as StdTcpListener;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const PROMETHEUS_EXPORTER_URN: &str = "urn:otel:exporter:prometheus";
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
const JSON_CONTENT_TYPE: &str = "application/json; charset=utf-8";
const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 9464;
const DEFAULT_PATH: &str = "/metrics";
const DEFAULT_MAX_ACTIVE_SERIES: usize = 100_000;
const DEFAULT_STALE_SERIES_TTL_SECS: u64 = 15 * 60;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default = "default_host")]
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default = "default_path")]
    path: String,
    #[serde(default)]
    json_path: Option<String>,
    #[serde(default = "default_max_active_series")]
    max_active_series: usize,
    #[serde(default = "default_stale_series_ttl", with = "humantime_serde")]
    stale_series_ttl: Duration,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct OwnerKey {
    pipeline_group_id: String,
    pipeline_id: String,
    node_name: String,
}

struct PrometheusExporter {
    owner: OwnerKey,
    config: Config,
    partition_id: usize,
    partition_count: usize,
    pdata_metrics: MetricSet<ExporterPDataMetrics>,
}

#[derive(Debug)]
struct RegistryEntry {
    owner: OwnerKey,
    config: Config,
    coordinator: Arc<Coordinator>,
    refcount: usize,
}

#[derive(Debug)]
struct Coordinator {
    config: Config,
    store: PartitionedMetricStore,
    self_metrics: Arc<SelfMetrics>,
    shutdown: CancellationToken,
    server_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
struct SelfMetrics {
    active_series: Arc<AtomicU64>,
    dropped_new_series: AtomicU64,
    dropped_unsupported_points: AtomicU64,
    metadata_conflicts: AtomicU64,
    scrapes: AtomicU64,
    scrape_errors: AtomicU64,
    last_scrape_duration_micros: AtomicU64,
}

#[derive(Debug)]
struct PartitionedMetricStore {
    admission: Mutex<AdmissionState>,
    partitions: Vec<Mutex<HashMap<u64, PartitionEntry>>>,
    max_active_series: usize,
    stale_series_ttl: Duration,
    active_series: Arc<AtomicU64>,
}

#[derive(Debug, Default)]
struct AdmissionState {
    next_id: u64,
    by_identity: HashMap<SeriesIdentity, Arc<SeriesSlot>>,
}

#[derive(Debug, Clone)]
struct SeriesSlot {
    id: u64,
    owner_partition: usize,
    identity: Arc<SeriesIdentity>,
}

#[derive(Debug, Clone)]
struct PartitionEntry {
    slot: Arc<SeriesSlot>,
    state: SeriesState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SeriesKind {
    Gauge,
    CounterCumulative,
    CounterDelta,
    HistogramCumulative,
    HistogramDelta,
    Summary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StoredValueType {
    U64,
    F64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PromMetricType {
    Gauge,
    Counter,
    Histogram,
    Summary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SeriesIdentity {
    family_name: String,
    description: String,
    unit: String,
    labels: Vec<(String, String)>,
    kind: SeriesKind,
    value_type: Option<StoredValueType>,
}

#[derive(Debug, Clone)]
enum SeriesState {
    Scalar(ScalarState),
    Histogram(HistogramState),
    Summary(SummaryState),
}

#[derive(Debug, Clone)]
struct ScalarState {
    value: ScalarValue,
    timestamp_unix_nano: u64,
    start_time_unix_nano: u64,
    last_seen: Instant,
}

#[derive(Debug, Clone)]
struct HistogramState {
    count: u64,
    sum: Option<f64>,
    bucket_counts: Vec<u64>,
    explicit_bounds: Vec<f64>,
    min: Option<f64>,
    max: Option<f64>,
    timestamp_unix_nano: u64,
    start_time_unix_nano: u64,
    last_seen: Instant,
}

#[derive(Debug, Clone)]
struct SummaryState {
    count: u64,
    sum: f64,
    quantiles: Vec<(f64, f64)>,
    timestamp_unix_nano: u64,
    last_seen: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ScalarValue {
    U64(u64),
    F64(f64),
}

#[derive(Debug, Serialize)]
struct JsonSnapshot {
    timestamp: String,
    metric_sets: Vec<JsonMetricSet>,
}

#[derive(Debug, Serialize)]
struct JsonMetricSet {
    name: String,
    brief: String,
    attributes: HashMap<String, AttributeValue>,
    metrics: Vec<JsonMetric>,
}

#[derive(Debug, Serialize)]
struct JsonMetric {
    name: String,
    unit: String,
    brief: String,
    instrument: Instrument,
    #[serde(skip_serializing_if = "Option::is_none")]
    temporality: Option<Temporality>,
    value_type: MetricValueType,
    value: MetricValue,
}

static REGISTRY: LazyLock<Mutex<HashMap<String, RegistryEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
static PROMETHEUS_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: PROMETHEUS_EXPORTER_URN,
    create: factory_create,
    wiring_contract: WiringContract::UNRESTRICTED,
    validate_config,
};

fn default_host() -> String {
    DEFAULT_HOST.to_owned()
}

const fn default_port() -> u16 {
    DEFAULT_PORT
}

fn default_path() -> String {
    DEFAULT_PATH.to_owned()
}

const fn default_max_active_series() -> usize {
    DEFAULT_MAX_ACTIVE_SERIES
}

fn default_stale_series_ttl() -> Duration {
    Duration::from_secs(DEFAULT_STALE_SERIES_TTL_SECS)
}

fn parse_config(config: &JsonValue) -> Result<Config, ConfigError> {
    let config: Config =
        serde_json::from_value(config.clone()).map_err(|e| ConfigError::InvalidUserConfig {
            error: format!("failed to parse prometheus exporter config: {e}"),
        })?;
    validate_path(&config.path, "path")?;
    if let Some(json_path) = config.json_path.as_deref() {
        validate_path(json_path, "json_path")?;
        if json_path == config.path {
            return Err(ConfigError::InvalidUserConfig {
                error: "prometheus exporter config requires `json_path` to differ from `path`"
                    .to_owned(),
            });
        }
    }
    if config.max_active_series == 0 {
        return Err(ConfigError::InvalidUserConfig {
            error: "prometheus exporter config requires `max_active_series` > 0".to_owned(),
        });
    }
    Ok(config)
}

fn validate_config(config: &JsonValue) -> Result<(), ConfigError> {
    let _ = parse_config(config)?;
    Ok(())
}

fn validate_path(path: &str, field: &str) -> Result<(), ConfigError> {
    if !path.starts_with('/') {
        return Err(ConfigError::InvalidUserConfig {
            error: format!("prometheus exporter `{field}` must start with '/'"),
        });
    }
    Ok(())
}

fn factory_create(
    pipeline: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    exporter_config: &ExporterConfig,
) -> Result<ExporterWrapper<OtapPdata>, ConfigError> {
    let config = parse_config(&node_config.config)?;
    let owner = OwnerKey {
        pipeline_group_id: pipeline.pipeline_group_id().to_string(),
        pipeline_id: pipeline.pipeline_id().to_string(),
        node_name: node.name.to_string(),
    };
    let pdata_metrics = pipeline.register_metrics::<ExporterPDataMetrics>();

    Ok(ExporterWrapper::local(
        PrometheusExporter {
            owner,
            config,
            partition_id: pipeline.core_id(),
            partition_count: pipeline.num_cores().max(1),
            pdata_metrics,
        },
        node,
        node_config,
        exporter_config,
    ))
}

#[async_trait(?Send)]
impl Exporter<OtapPdata> for PrometheusExporter {
    async fn start(
        mut self: Box<Self>,
        mut msg_chan: MessageChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let telemetry_timer_cancel = effect_handler
            .start_periodic_telemetry(Duration::from_secs(1))
            .await?;

        let coordinator = attach_coordinator(
            self.owner.clone(),
            self.config.clone(),
            self.partition_count,
            effect_handler.exporter_id().clone(),
        )
        .await?;

        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    detach_coordinator(&self.owner, &self.config).await;
                    _ = telemetry_timer_cancel.cancel().await;
                    return Ok(TerminalState::new(deadline, [self.pdata_metrics]));
                }
                Message::Control(NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                }) => {
                    _ = metrics_reporter.report(&mut self.pdata_metrics);
                }
                Message::PData(pdata) => {
                    if pdata.signal_type() != SignalType::Metrics {
                        self.pdata_metrics.inc_failed(pdata.signal_type());
                        effect_handler
                            .notify_nack(NackMsg::new_permanent(
                                "prometheus exporter only accepts metrics payloads",
                                pdata,
                            ))
                            .await?;
                        continue;
                    }

                    self.pdata_metrics.inc_consumed(SignalType::Metrics);

                    match ingest_metrics_payload(
                        &coordinator,
                        self.partition_id,
                        pdata.payload_ref(),
                    ) {
                        Ok(()) => {
                            self.pdata_metrics.inc_exported(SignalType::Metrics);
                            effect_handler.notify_ack(AckMsg::new(pdata)).await?;
                        }
                        Err(error) => {
                            self.pdata_metrics.inc_failed(SignalType::Metrics);
                            effect_handler
                                .notify_nack(NackMsg::new_permanent(error.as_str(), pdata))
                                .await?;
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

async fn attach_coordinator(
    owner: OwnerKey,
    config: Config,
    partition_count: usize,
    exporter_id: NodeId,
) -> Result<Arc<Coordinator>, Error> {
    let bind_key = format!("{}:{}", config.host, config.port);
    {
        let mut registry = REGISTRY.lock();
        if let Some(entry) = registry.get_mut(&bind_key) {
            if entry.owner != owner
                || entry.config.path != config.path
                || entry.config.json_path != config.json_path
            {
                return Err(Error::ExporterError {
                    exporter: exporter_id,
                    kind: ExporterErrorKind::Configuration,
                    error: format!(
                        "prometheus exporter endpoint {bind_key} already owned by {}.{}:{}",
                        entry.owner.pipeline_group_id,
                        entry.owner.pipeline_id,
                        entry.owner.node_name
                    ),
                    source_detail: String::new(),
                });
            }
            entry.refcount += 1;
            return Ok(entry.coordinator.clone());
        }
    }

    let self_metrics = Arc::new(SelfMetrics::default());
    let coordinator = Arc::new(Coordinator {
        config: config.clone(),
        store: PartitionedMetricStore::new(
            partition_count,
            config.max_active_series,
            config.stale_series_ttl,
            self_metrics.active_series.clone(),
        ),
        self_metrics,
        shutdown: CancellationToken::new(),
        server_task: Mutex::new(None),
    });

    start_http_server(coordinator.clone(), exporter_id.clone()).await?;

    let mut registry = REGISTRY.lock();
    if let Some(entry) = registry.get_mut(&bind_key) {
        entry.refcount += 1;
        return Ok(entry.coordinator.clone());
    }
    let _ = registry.insert(
        bind_key,
        RegistryEntry {
            owner,
            config,
            coordinator: coordinator.clone(),
            refcount: 1,
        },
    );

    Ok(coordinator)
}

async fn detach_coordinator(owner: &OwnerKey, config: &Config) {
    let bind_key = format!("{}:{}", config.host, config.port);
    let mut shutdown = None;
    {
        let mut registry = REGISTRY.lock();
        if let Some(entry) = registry.get_mut(&bind_key) {
            if &entry.owner != owner {
                return;
            }
            entry.refcount = entry.refcount.saturating_sub(1);
            if entry.refcount == 0 {
                shutdown = Some(entry.coordinator.shutdown.clone());
                let _ = registry.remove(&bind_key);
            }
        }
    }
    if let Some(token) = shutdown {
        token.cancel();
    }
}

async fn start_http_server(
    coordinator: Arc<Coordinator>,
    exporter_id: NodeId,
) -> Result<(), Error> {
    let bind_addr = format!("{}:{}", coordinator.config.host, coordinator.config.port);
    let std_listener =
        StdTcpListener::bind(bind_addr.as_str()).map_err(|e| Error::ExporterError {
            exporter: exporter_id.clone(),
            kind: ExporterErrorKind::Transport,
            error: format!("failed to bind prometheus exporter HTTP listener on {bind_addr}"),
            source_detail: e.to_string(),
        })?;
    std_listener
        .set_nonblocking(true)
        .map_err(|e| Error::ExporterError {
            exporter: exporter_id.clone(),
            kind: ExporterErrorKind::Transport,
            error: format!("failed to configure nonblocking listener on {bind_addr}"),
            source_detail: e.to_string(),
        })?;
    let listener = TcpListener::from_std(std_listener).map_err(|e| Error::ExporterError {
        exporter: exporter_id,
        kind: ExporterErrorKind::Transport,
        error: format!("failed to create tokio listener on {bind_addr}"),
        source_detail: e.to_string(),
    })?;

    let mut router = Router::new().route(
        coordinator.config.path.as_str(),
        get(handle_prometheus_scrape),
    );
    if let Some(json_path) = coordinator.config.json_path.as_deref() {
        router = router.route(json_path, get(handle_json_scrape));
    }
    let router = router.with_state(coordinator.clone());

    let shutdown = coordinator.shutdown.clone();
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                shutdown.cancelled().await;
            })
            .await;
    });

    *coordinator.server_task.lock() = Some(task);
    Ok(())
}

async fn handle_prometheus_scrape(State(coordinator): State<Arc<Coordinator>>) -> Response {
    let start = Instant::now();
    let (body, metadata_conflicts) = coordinator.render_prometheus();
    if metadata_conflicts > 0 {
        _ = coordinator
            .self_metrics
            .metadata_conflicts
            .fetch_add(metadata_conflicts as u64, Ordering::Relaxed);
    }
    _ = coordinator
        .self_metrics
        .scrapes
        .fetch_add(1, Ordering::Relaxed);
    coordinator
        .self_metrics
        .last_scrape_duration_micros
        .store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
    let mut response = body.into_response();
    let _ = response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(PROMETHEUS_CONTENT_TYPE),
    );
    response
}

async fn handle_json_scrape(State(coordinator): State<Arc<Coordinator>>) -> Response {
    let start = Instant::now();
    let snapshot = coordinator.render_json();
    _ = coordinator
        .self_metrics
        .scrapes
        .fetch_add(1, Ordering::Relaxed);
    coordinator
        .self_metrics
        .last_scrape_duration_micros
        .store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
    let mut response = Json(snapshot).into_response();
    let _ = response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(JSON_CONTENT_TYPE),
    );
    response
}

impl Default for SelfMetrics {
    fn default() -> Self {
        Self {
            active_series: Arc::new(AtomicU64::new(0)),
            dropped_new_series: AtomicU64::new(0),
            dropped_unsupported_points: AtomicU64::new(0),
            metadata_conflicts: AtomicU64::new(0),
            scrapes: AtomicU64::new(0),
            scrape_errors: AtomicU64::new(0),
            last_scrape_duration_micros: AtomicU64::new(0),
        }
    }
}

impl PartitionedMetricStore {
    fn new(
        partition_count: usize,
        max_active_series: usize,
        stale_series_ttl: Duration,
        active_series: Arc<AtomicU64>,
    ) -> Self {
        let partitions = (0..partition_count.max(1))
            .map(|_| Mutex::new(HashMap::new()))
            .collect();
        Self {
            admission: Mutex::new(AdmissionState::default()),
            partitions,
            max_active_series,
            stale_series_ttl,
            active_series,
        }
    }

    fn partition(&self, partition_id: usize) -> &Mutex<HashMap<u64, PartitionEntry>> {
        &self.partitions[partition_id.min(self.partitions.len() - 1)]
    }

    fn resolve_slot(
        &self,
        identity: SeriesIdentity,
        preferred_partition: usize,
        self_metrics: &SelfMetrics,
    ) -> Option<Arc<SeriesSlot>> {
        let mut admission = self.admission.lock();
        if let Some(slot) = admission.by_identity.get(&identity) {
            return Some(slot.clone());
        }
        if admission.by_identity.len() >= self.max_active_series {
            _ = self_metrics
                .dropped_new_series
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let slot = Arc::new(SeriesSlot {
            id: admission.next_id,
            owner_partition: preferred_partition.min(self.partitions.len() - 1),
            identity: Arc::new(identity.clone()),
        });
        admission.next_id += 1;
        let _ = admission.by_identity.insert(identity, slot.clone());
        self.active_series
            .store(admission.by_identity.len() as u64, Ordering::Relaxed);
        Some(slot)
    }

    fn delete_slot(&self, slot: &SeriesSlot) {
        let mut partition = self.partition(slot.owner_partition).lock();
        let _ = partition.remove(&slot.id);
        drop(partition);

        let mut admission = self.admission.lock();
        let _ = admission.by_identity.remove(slot.identity.as_ref());
        self.active_series
            .store(admission.by_identity.len() as u64, Ordering::Relaxed);
    }

    fn record_scalar(
        &self,
        preferred_partition: usize,
        identity: SeriesIdentity,
        point_value: ScalarValue,
        point_time: u64,
        point_start_time: u64,
        flags: DataPointFlags,
        self_metrics: &SelfMetrics,
    ) {
        let Some(slot) = self.resolve_slot(identity, preferred_partition, self_metrics) else {
            return;
        };
        if flags.no_recorded_value() {
            self.delete_slot(&slot);
            return;
        }

        let mut partition = self.partition(slot.owner_partition).lock();
        let entry = partition.entry(slot.id).or_insert_with(|| PartitionEntry {
            slot: slot.clone(),
            state: SeriesState::Scalar(ScalarState {
                value: point_value,
                timestamp_unix_nano: point_time,
                start_time_unix_nano: point_start_time,
                last_seen: Instant::now(),
            }),
        });

        match &mut entry.state {
            SeriesState::Scalar(state) => {
                apply_scalar_update(
                    &slot.identity.kind,
                    state,
                    point_value,
                    point_time,
                    point_start_time,
                );
                state.last_seen = Instant::now();
            }
            _ => {
                entry.state = SeriesState::Scalar(ScalarState {
                    value: point_value,
                    timestamp_unix_nano: point_time,
                    start_time_unix_nano: point_start_time,
                    last_seen: Instant::now(),
                });
            }
        }
    }

    fn record_histogram(
        &self,
        preferred_partition: usize,
        identity: SeriesIdentity,
        point: HistogramState,
        flags: DataPointFlags,
        self_metrics: &SelfMetrics,
    ) {
        let Some(slot) = self.resolve_slot(identity, preferred_partition, self_metrics) else {
            return;
        };
        if flags.no_recorded_value() {
            self.delete_slot(&slot);
            return;
        }

        let mut partition = self.partition(slot.owner_partition).lock();
        let entry = partition.entry(slot.id).or_insert_with(|| PartitionEntry {
            slot: slot.clone(),
            state: SeriesState::Histogram(point.clone()),
        });

        match &mut entry.state {
            SeriesState::Histogram(state) => {
                apply_histogram_update(&slot.identity.kind, state, point);
                state.last_seen = Instant::now();
            }
            _ => {
                entry.state = SeriesState::Histogram(point);
            }
        }
    }

    fn record_summary(
        &self,
        preferred_partition: usize,
        identity: SeriesIdentity,
        point: SummaryState,
        flags: DataPointFlags,
        self_metrics: &SelfMetrics,
    ) {
        let Some(slot) = self.resolve_slot(identity, preferred_partition, self_metrics) else {
            return;
        };
        if flags.no_recorded_value() {
            self.delete_slot(&slot);
            return;
        }

        let mut partition = self.partition(slot.owner_partition).lock();
        let entry = partition.entry(slot.id).or_insert_with(|| PartitionEntry {
            slot,
            state: SeriesState::Summary(point.clone()),
        });

        match &mut entry.state {
            SeriesState::Summary(state) => {
                if point.timestamp_unix_nano >= state.timestamp_unix_nano {
                    *state = point;
                }
                state.last_seen = Instant::now();
            }
            _ => {
                entry.state = SeriesState::Summary(point);
            }
        }
    }

    fn snapshot(&self) -> Vec<PartitionEntry> {
        let mut snapshot = Vec::new();
        let now = Instant::now();
        let mut expired_slots = Vec::new();

        for partition in &self.partitions {
            let mut guard = partition.lock();
            let mut expired_ids = Vec::new();
            for (series_id, entry) in guard.iter() {
                if now.duration_since(entry.state.last_seen()) > self.stale_series_ttl {
                    expired_ids.push(*series_id);
                } else {
                    snapshot.push(entry.clone());
                }
            }
            for series_id in expired_ids {
                if let Some(entry) = guard.remove(&series_id) {
                    expired_slots.push(entry.slot);
                }
            }
        }

        if !expired_slots.is_empty() {
            let mut admission = self.admission.lock();
            for slot in expired_slots {
                let _ = admission.by_identity.remove(slot.identity.as_ref());
            }
            self.active_series
                .store(admission.by_identity.len() as u64, Ordering::Relaxed);
        }

        snapshot
    }
}

impl SeriesState {
    fn last_seen(&self) -> Instant {
        match self {
            Self::Scalar(state) => state.last_seen,
            Self::Histogram(state) => state.last_seen,
            Self::Summary(state) => state.last_seen,
        }
    }
}

impl Coordinator {
    fn render_prometheus(&self) -> (String, usize) {
        let snapshot = self.store.snapshot();
        let mut families: BTreeMap<String, FamilyRenderState> = BTreeMap::new();
        let mut metadata_conflicts = 0;

        for entry in snapshot {
            let family_name = entry.slot.identity.family_name.clone();
            let prom_type = prom_metric_type(&entry.slot.identity.kind);
            let family = families
                .entry(family_name.clone())
                .or_insert_with(|| FamilyRenderState {
                    name: family_name,
                    description: entry.slot.identity.description.clone(),
                    unit: entry.slot.identity.unit.clone(),
                    prom_type,
                    samples: Vec::new(),
                    type_conflict: false,
                });

            if family.prom_type != prom_type {
                family.type_conflict = true;
                metadata_conflicts += 1;
                continue;
            }
            if family.description != entry.slot.identity.description {
                metadata_conflicts += 1;
            }
            if family.unit != entry.slot.identity.unit {
                metadata_conflicts += 1;
            }

            render_prometheus_samples(&entry.slot.identity, &entry.state, &mut family.samples);
        }

        let mut body = String::new();
        for family in families.into_values() {
            if family.type_conflict {
                continue;
            }
            if !family.description.is_empty() {
                let _ = writeln!(
                    &mut body,
                    "# HELP {} {}",
                    family.name,
                    escape_prom_label_value(family.description.as_str())
                );
            }
            let _ = writeln!(
                &mut body,
                "# TYPE {} {}",
                family.name,
                family.prom_type.as_str()
            );
            for sample in family.samples {
                let _ = writeln!(&mut body, "{sample}");
            }
        }

        render_self_metrics(&mut body, &self.self_metrics);

        (body, metadata_conflicts)
    }

    fn render_json(&self) -> JsonSnapshot {
        let timestamp = chrono::Utc::now().to_rfc3339();
        let snapshot = self.store.snapshot();
        let mut groups: BTreeMap<JsonGroupKey, Vec<JsonMetric>> = BTreeMap::new();

        for entry in snapshot {
            let set_name = entry
                .slot
                .identity
                .labels
                .iter()
                .find_map(|(key, value)| (key == "job").then_some(value.clone()))
                .unwrap_or_else(|| "metrics".to_owned());
            let key = JsonGroupKey {
                name: set_name,
                brief: entry.slot.identity.description.clone(),
                labels: entry.slot.identity.labels.clone(),
            };
            let metrics = groups.entry(key).or_default();
            render_json_metrics(&entry.slot.identity, &entry.state, metrics);
        }

        let mut metric_sets = Vec::with_capacity(groups.len() + 1);
        for (key, mut metrics) in groups {
            metrics.sort_by(|left, right| left.name.cmp(&right.name));
            metric_sets.push(JsonMetricSet {
                name: key.name,
                brief: key.brief,
                attributes: key
                    .labels
                    .into_iter()
                    .map(|(name, value)| (name, AttributeValue::String(value)))
                    .collect(),
                metrics,
            });
        }

        metric_sets.push(render_self_metrics_json(&self.self_metrics));
        JsonSnapshot {
            timestamp,
            metric_sets,
        }
    }
}

#[derive(Debug)]
struct FamilyRenderState {
    name: String,
    description: String,
    unit: String,
    prom_type: PromMetricType,
    samples: Vec<String>,
    type_conflict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct JsonGroupKey {
    name: String,
    brief: String,
    labels: Vec<(String, String)>,
}

fn render_prometheus_samples(
    identity: &SeriesIdentity,
    state: &SeriesState,
    out: &mut Vec<String>,
) {
    match state {
        SeriesState::Scalar(state) => {
            out.push(render_scalar_sample(
                identity.family_name.as_str(),
                identity.labels.as_slice(),
                state.value.to_f64(),
            ));
        }
        SeriesState::Histogram(state) => {
            let mut running = 0u64;
            for (idx, bucket_count) in state.bucket_counts.iter().enumerate() {
                running = running.saturating_add(*bucket_count);
                let le = state
                    .explicit_bounds
                    .get(idx)
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "+Inf".to_owned());
                out.push(render_scalar_sample_with_extra_label(
                    format!("{}_bucket", identity.family_name).as_str(),
                    identity.labels.as_slice(),
                    "le",
                    le.as_str(),
                    running as f64,
                ));
            }
            out.push(render_scalar_sample(
                format!("{}_sum", identity.family_name).as_str(),
                identity.labels.as_slice(),
                state.sum.unwrap_or_default(),
            ));
            out.push(render_scalar_sample(
                format!("{}_count", identity.family_name).as_str(),
                identity.labels.as_slice(),
                state.count as f64,
            ));
        }
        SeriesState::Summary(state) => {
            for (quantile, value) in &state.quantiles {
                out.push(render_scalar_sample_with_extra_label(
                    identity.family_name.as_str(),
                    identity.labels.as_slice(),
                    "quantile",
                    quantile.to_string().as_str(),
                    *value,
                ));
            }
            out.push(render_scalar_sample(
                format!("{}_sum", identity.family_name).as_str(),
                identity.labels.as_slice(),
                state.sum,
            ));
            out.push(render_scalar_sample(
                format!("{}_count", identity.family_name).as_str(),
                identity.labels.as_slice(),
                state.count as f64,
            ));
        }
    }
}

fn render_self_metrics(body: &mut String, metrics: &SelfMetrics) {
    let scalars = [
        (
            "otap_prometheus_exporter_active_series",
            "gauge",
            metrics.active_series.load(Ordering::Relaxed) as f64,
        ),
        (
            "otap_prometheus_exporter_dropped_new_series_total",
            "counter",
            metrics.dropped_new_series.load(Ordering::Relaxed) as f64,
        ),
        (
            "otap_prometheus_exporter_dropped_unsupported_points_total",
            "counter",
            metrics.dropped_unsupported_points.load(Ordering::Relaxed) as f64,
        ),
        (
            "otap_prometheus_exporter_metadata_conflicts_total",
            "counter",
            metrics.metadata_conflicts.load(Ordering::Relaxed) as f64,
        ),
        (
            "otap_prometheus_exporter_scrapes_total",
            "counter",
            metrics.scrapes.load(Ordering::Relaxed) as f64,
        ),
        (
            "otap_prometheus_exporter_scrape_errors_total",
            "counter",
            metrics.scrape_errors.load(Ordering::Relaxed) as f64,
        ),
        (
            "otap_prometheus_exporter_last_scrape_duration_seconds",
            "gauge",
            metrics.last_scrape_duration_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        ),
    ];

    for (name, prom_type, value) in scalars {
        let _ = writeln!(body, "# TYPE {name} {prom_type}");
        let _ = writeln!(body, "{name} {value}");
    }
}

fn render_self_metrics_json(metrics: &SelfMetrics) -> JsonMetricSet {
    JsonMetricSet {
        name: "prometheus.exporter".to_owned(),
        brief: "Prometheus exporter self-metrics".to_owned(),
        attributes: HashMap::new(),
        metrics: vec![
            JsonMetric {
                name: "active_series".to_owned(),
                unit: "{series}".to_owned(),
                brief: "Active series stored by the exporter".to_owned(),
                instrument: Instrument::Gauge,
                temporality: None,
                value_type: MetricValueType::U64,
                value: MetricValue::U64(metrics.active_series.load(Ordering::Relaxed)),
            },
            JsonMetric {
                name: "dropped_new_series".to_owned(),
                unit: "{series}".to_owned(),
                brief: "New series dropped because the active series cap was reached".to_owned(),
                instrument: Instrument::Counter,
                temporality: Some(Temporality::Cumulative),
                value_type: MetricValueType::U64,
                value: MetricValue::U64(metrics.dropped_new_series.load(Ordering::Relaxed)),
            },
            JsonMetric {
                name: "dropped_unsupported_points".to_owned(),
                unit: "{point}".to_owned(),
                brief: "Unsupported metric points dropped during ingestion".to_owned(),
                instrument: Instrument::Counter,
                temporality: Some(Temporality::Cumulative),
                value_type: MetricValueType::U64,
                value: MetricValue::U64(metrics.dropped_unsupported_points.load(Ordering::Relaxed)),
            },
            JsonMetric {
                name: "metadata_conflicts".to_owned(),
                unit: "{conflict}".to_owned(),
                brief: "Metric family metadata conflicts observed during rendering".to_owned(),
                instrument: Instrument::Counter,
                temporality: Some(Temporality::Cumulative),
                value_type: MetricValueType::U64,
                value: MetricValue::U64(metrics.metadata_conflicts.load(Ordering::Relaxed)),
            },
            JsonMetric {
                name: "scrapes".to_owned(),
                unit: "{scrape}".to_owned(),
                brief: "Successful scrape requests served by the exporter".to_owned(),
                instrument: Instrument::Counter,
                temporality: Some(Temporality::Cumulative),
                value_type: MetricValueType::U64,
                value: MetricValue::U64(metrics.scrapes.load(Ordering::Relaxed)),
            },
            JsonMetric {
                name: "scrape_errors".to_owned(),
                unit: "{error}".to_owned(),
                brief: "Scrape errors served by the exporter".to_owned(),
                instrument: Instrument::Counter,
                temporality: Some(Temporality::Cumulative),
                value_type: MetricValueType::U64,
                value: MetricValue::U64(metrics.scrape_errors.load(Ordering::Relaxed)),
            },
            JsonMetric {
                name: "last_scrape_duration_seconds".to_owned(),
                unit: "s".to_owned(),
                brief: "Duration of the most recent scrape".to_owned(),
                instrument: Instrument::Gauge,
                temporality: None,
                value_type: MetricValueType::F64,
                value: MetricValue::F64(
                    metrics.last_scrape_duration_micros.load(Ordering::Relaxed) as f64
                        / 1_000_000.0,
                ),
            },
        ],
    }
}

fn render_json_metrics(identity: &SeriesIdentity, state: &SeriesState, out: &mut Vec<JsonMetric>) {
    match state {
        SeriesState::Scalar(state) => {
            out.push(JsonMetric {
                name: identity.family_name.clone(),
                unit: identity.unit.clone(),
                brief: identity.description.clone(),
                instrument: json_instrument(&identity.kind),
                temporality: json_temporality(&identity.kind),
                value_type: state.value.metric_value_type(),
                value: state.value.metric_value(),
            });
        }
        SeriesState::Histogram(state) => {
            out.push(JsonMetric {
                name: format!("{}.count", identity.family_name),
                unit: "{count}".to_owned(),
                brief: identity.description.clone(),
                instrument: Instrument::Counter,
                temporality: Some(Temporality::Cumulative),
                value_type: MetricValueType::U64,
                value: MetricValue::U64(state.count),
            });
            if let Some(sum) = state.sum {
                out.push(JsonMetric {
                    name: format!("{}.sum", identity.family_name),
                    unit: identity.unit.clone(),
                    brief: identity.description.clone(),
                    instrument: Instrument::Counter,
                    temporality: Some(Temporality::Cumulative),
                    value_type: MetricValueType::F64,
                    value: MetricValue::F64(sum),
                });
            }
            if let Some(min) = state.min {
                out.push(JsonMetric {
                    name: format!("{}.min", identity.family_name),
                    unit: identity.unit.clone(),
                    brief: identity.description.clone(),
                    instrument: Instrument::Gauge,
                    temporality: None,
                    value_type: MetricValueType::F64,
                    value: MetricValue::F64(min),
                });
            }
            if let Some(max) = state.max {
                out.push(JsonMetric {
                    name: format!("{}.max", identity.family_name),
                    unit: identity.unit.clone(),
                    brief: identity.description.clone(),
                    instrument: Instrument::Gauge,
                    temporality: None,
                    value_type: MetricValueType::F64,
                    value: MetricValue::F64(max),
                });
            }
        }
        SeriesState::Summary(state) => {
            out.push(JsonMetric {
                name: format!("{}.count", identity.family_name),
                unit: "{count}".to_owned(),
                brief: identity.description.clone(),
                instrument: Instrument::Counter,
                temporality: Some(Temporality::Cumulative),
                value_type: MetricValueType::U64,
                value: MetricValue::U64(state.count),
            });
            out.push(JsonMetric {
                name: format!("{}.sum", identity.family_name),
                unit: identity.unit.clone(),
                brief: identity.description.clone(),
                instrument: Instrument::Counter,
                temporality: Some(Temporality::Cumulative),
                value_type: MetricValueType::F64,
                value: MetricValue::F64(state.sum),
            });
            for (quantile, value) in &state.quantiles {
                out.push(JsonMetric {
                    name: format!("{}.q{}", identity.family_name, sanitize_quantile(*quantile)),
                    unit: identity.unit.clone(),
                    brief: identity.description.clone(),
                    instrument: Instrument::Gauge,
                    temporality: None,
                    value_type: MetricValueType::F64,
                    value: MetricValue::F64(*value),
                });
            }
        }
    }
}

fn sanitize_quantile(value: f64) -> String {
    let mut rendered = value.to_string();
    rendered = rendered.replace('.', "_");
    rendered
}

fn render_scalar_sample(name: &str, labels: &[(String, String)], value: f64) -> String {
    if labels.is_empty() {
        format!("{name} {value}")
    } else {
        format!("{name}{{{}}} {value}", render_labels(labels))
    }
}

fn render_scalar_sample_with_extra_label(
    name: &str,
    labels: &[(String, String)],
    extra_key: &str,
    extra_value: &str,
    value: f64,
) -> String {
    let mut full_labels = labels.to_vec();
    full_labels.push((extra_key.to_owned(), extra_value.to_owned()));
    full_labels.sort_by(|left, right| left.0.cmp(&right.0));
    format!(
        "{name}{{{}}} {value}",
        render_labels(full_labels.as_slice())
    )
}

fn render_labels(labels: &[(String, String)]) -> String {
    let mut rendered = String::new();
    for (index, (key, value)) in labels.iter().enumerate() {
        if index > 0 {
            rendered.push(',');
        }
        let _ = write!(
            &mut rendered,
            "{}=\"{}\"",
            key,
            escape_prom_label_value(value.as_str())
        );
    }
    rendered
}

fn prom_metric_type(kind: &SeriesKind) -> PromMetricType {
    match kind {
        SeriesKind::Gauge => PromMetricType::Gauge,
        SeriesKind::CounterCumulative | SeriesKind::CounterDelta => PromMetricType::Counter,
        SeriesKind::HistogramCumulative | SeriesKind::HistogramDelta => PromMetricType::Histogram,
        SeriesKind::Summary => PromMetricType::Summary,
    }
}

fn json_instrument(kind: &SeriesKind) -> Instrument {
    match kind {
        SeriesKind::Gauge => Instrument::Gauge,
        SeriesKind::CounterCumulative | SeriesKind::CounterDelta => Instrument::Counter,
        SeriesKind::HistogramCumulative | SeriesKind::HistogramDelta => Instrument::Histogram,
        SeriesKind::Summary => Instrument::Gauge,
    }
}

fn json_temporality(kind: &SeriesKind) -> Option<Temporality> {
    match kind {
        SeriesKind::CounterDelta | SeriesKind::HistogramDelta => Some(Temporality::Delta),
        SeriesKind::CounterCumulative | SeriesKind::HistogramCumulative => {
            Some(Temporality::Cumulative)
        }
        SeriesKind::Gauge | SeriesKind::Summary => None,
    }
}

impl PromMetricType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Gauge => "gauge",
            Self::Counter => "counter",
            Self::Histogram => "histogram",
            Self::Summary => "summary",
        }
    }
}

impl Hash for SeriesIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.family_name.hash(state);
        self.description.hash(state);
        self.unit.hash(state);
        self.labels.hash(state);
        self.kind.hash(state);
        self.value_type.hash(state);
    }
}

impl ScalarValue {
    fn to_f64(self) -> f64 {
        match self {
            Self::U64(value) => value as f64,
            Self::F64(value) => value,
        }
    }

    fn metric_value_type(self) -> MetricValueType {
        match self {
            Self::U64(_) => MetricValueType::U64,
            Self::F64(_) => MetricValueType::F64,
        }
    }

    fn metric_value(self) -> MetricValue {
        match self {
            Self::U64(value) => MetricValue::U64(value),
            Self::F64(value) => MetricValue::F64(value),
        }
    }
}

fn apply_scalar_update(
    kind: &SeriesKind,
    state: &mut ScalarState,
    point_value: ScalarValue,
    point_time: u64,
    point_start_time: u64,
) {
    match kind {
        SeriesKind::Gauge => {
            if point_time >= state.timestamp_unix_nano {
                state.value = point_value;
                state.timestamp_unix_nano = point_time;
                state.start_time_unix_nano = point_start_time;
            }
        }
        SeriesKind::CounterCumulative => {
            if point_time < state.timestamp_unix_nano {
                return;
            }
            if point_start_time != state.start_time_unix_nano
                || point_value.to_f64() < state.value.to_f64()
            {
                state.value = point_value;
                state.start_time_unix_nano = point_start_time;
                state.timestamp_unix_nano = point_time;
                return;
            }
            state.value = point_value;
            state.timestamp_unix_nano = point_time;
        }
        SeriesKind::CounterDelta => {
            if point_time <= state.timestamp_unix_nano {
                return;
            }
            if point_start_time != state.timestamp_unix_nano {
                state.value = point_value;
                state.start_time_unix_nano = point_start_time;
                state.timestamp_unix_nano = point_time;
                return;
            }
            state.value = add_scalar_values(state.value, point_value);
            state.timestamp_unix_nano = point_time;
        }
        SeriesKind::HistogramCumulative | SeriesKind::HistogramDelta | SeriesKind::Summary => {}
    }
}

fn apply_histogram_update(kind: &SeriesKind, state: &mut HistogramState, point: HistogramState) {
    match kind {
        SeriesKind::HistogramCumulative => {
            if point.timestamp_unix_nano < state.timestamp_unix_nano {
                return;
            }
            if point.start_time_unix_nano != state.start_time_unix_nano
                || point.explicit_bounds != state.explicit_bounds
            {
                *state = point;
                return;
            }
            *state = point;
        }
        SeriesKind::HistogramDelta => {
            if point.timestamp_unix_nano <= state.timestamp_unix_nano {
                return;
            }
            if point.start_time_unix_nano != state.timestamp_unix_nano
                || point.explicit_bounds != state.explicit_bounds
            {
                *state = point;
                return;
            }
            state.count = state.count.saturating_add(point.count);
            state.sum = match (state.sum, point.sum) {
                (Some(left), Some(right)) => Some(left + right),
                (Some(left), None) => Some(left),
                (None, Some(right)) => Some(right),
                (None, None) => None,
            };
            for (index, bucket_count) in point.bucket_counts.iter().enumerate() {
                if let Some(existing) = state.bucket_counts.get_mut(index) {
                    *existing = existing.saturating_add(*bucket_count);
                } else {
                    state.bucket_counts.push(*bucket_count);
                }
            }
            state.min = match (state.min, point.min) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (Some(left), None) => Some(left),
                (None, Some(right)) => Some(right),
                (None, None) => None,
            };
            state.max = match (state.max, point.max) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (Some(left), None) => Some(left),
                (None, Some(right)) => Some(right),
                (None, None) => None,
            };
            state.timestamp_unix_nano = point.timestamp_unix_nano;
            state.last_seen = Instant::now();
        }
        SeriesKind::Gauge
        | SeriesKind::CounterCumulative
        | SeriesKind::CounterDelta
        | SeriesKind::Summary => {}
    }
}

fn add_scalar_values(left: ScalarValue, right: ScalarValue) -> ScalarValue {
    match (left, right) {
        (ScalarValue::U64(left), ScalarValue::U64(right)) => {
            ScalarValue::U64(left.saturating_add(right))
        }
        (left, right) => ScalarValue::F64(left.to_f64() + right.to_f64()),
    }
}

fn ingest_metrics_payload(
    coordinator: &Coordinator,
    partition_id: usize,
    payload: &OtapPayload,
) -> Result<(), String> {
    match payload {
        OtapPayload::OtlpBytes(OtlpProtoBytes::ExportMetricsRequest(bytes)) => {
            let view = RawMetricsData::new(bytes.as_ref());
            ingest_metrics_view(coordinator, partition_id, &view);
            Ok(())
        }
        OtapPayload::OtapArrowRecords(records) => {
            // PData does not expose a direct metrics view over OtapArrowRecords yet, so
            // reuse the existing OTAP -> OTLP conversion until that API exists.
            let otlp_bytes: OtlpProtoBytes = records.clone().try_into().map_err(|error| {
                format!("failed to convert OTAP metrics to OTLP bytes: {error}")
            })?;
            match otlp_bytes {
                OtlpProtoBytes::ExportMetricsRequest(bytes) => {
                    let view = RawMetricsData::new(bytes.as_ref());
                    ingest_metrics_view(coordinator, partition_id, &view);
                    Ok(())
                }
                _ => Err("unexpected non-metrics payload after OTAP conversion".to_owned()),
            }
        }
        _ => Err("prometheus exporter only accepts metrics payloads".to_owned()),
    }
}

fn ingest_metrics_view<M>(coordinator: &Coordinator, partition_id: usize, data: &M)
where
    M: MetricsView,
{
    let mut batch_cache: HashMap<SeriesIdentity, Arc<SeriesSlot>> = HashMap::new();

    for resource_metrics in data.resources() {
        let resource_attrs = resource_metrics
            .resource()
            .map(|resource| collect_attributes(resource.attributes()))
            .unwrap_or_default();
        for scope_metrics in resource_metrics.scopes() {
            let scope_labels = scope_metrics
                .scope()
                .map(collect_scope_labels)
                .unwrap_or_default();
            for metric in scope_metrics.metrics() {
                let metric_name = decode_str(metric.name());
                let description = decode_str(metric.description());
                let unit = decode_str(metric.unit());
                let Some(metric_data) = metric.data() else {
                    continue;
                };

                match metric_data.value_type() {
                    DataType::Gauge => {
                        if let Some(gauge) = metric_data.as_gauge() {
                            let family_name =
                                translate_metric_name(metric_name.as_str(), unit.as_str(), false);
                            for point in gauge.data_points() {
                                let Some(point_value) = point.value() else {
                                    _ = coordinator
                                        .self_metrics
                                        .dropped_unsupported_points
                                        .fetch_add(1, Ordering::Relaxed);
                                    continue;
                                };
                                let identity = scalar_identity(
                                    family_name.as_str(),
                                    description.as_str(),
                                    unit.as_str(),
                                    build_labels(
                                        resource_attrs.as_slice(),
                                        scope_labels.as_slice(),
                                        collect_attributes(point.attributes()).as_slice(),
                                    ),
                                    SeriesKind::Gauge,
                                    stored_value_type(point_value),
                                );
                                let slot = batch_cache
                                    .entry(identity.clone())
                                    .or_insert_with(|| {
                                        coordinator
                                            .store
                                            .resolve_slot(
                                                identity.clone(),
                                                partition_id,
                                                coordinator.self_metrics.as_ref(),
                                            )
                                            .unwrap_or_else(|| {
                                                Arc::new(SeriesSlot {
                                                    id: u64::MAX,
                                                    owner_partition: partition_id,
                                                    identity: Arc::new(identity.clone()),
                                                })
                                            })
                                    })
                                    .clone();
                                if slot.id == u64::MAX {
                                    continue;
                                }
                                coordinator.store.record_scalar(
                                    partition_id,
                                    identity,
                                    scalar_value_from_number(point_value),
                                    point.time_unix_nano(),
                                    point.start_time_unix_nano(),
                                    point.flags(),
                                    coordinator.self_metrics.as_ref(),
                                );
                            }
                        }
                    }
                    DataType::Sum => {
                        if let Some(sum) = metric_data.as_sum() {
                            let temporality = sum.aggregation_temporality();
                            let monotonic = sum.is_monotonic();
                            if temporality == AggregationTemporality::Delta && !monotonic {
                                _ = coordinator
                                    .self_metrics
                                    .dropped_unsupported_points
                                    .fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            let kind = match (temporality, monotonic) {
                                (AggregationTemporality::Delta, true) => SeriesKind::CounterDelta,
                                (AggregationTemporality::Cumulative, true) => {
                                    SeriesKind::CounterCumulative
                                }
                                (_, false) => SeriesKind::Gauge,
                                _ => SeriesKind::Gauge,
                            };
                            let family_name = translate_metric_name(
                                metric_name.as_str(),
                                unit.as_str(),
                                monotonic,
                            );
                            for point in sum.data_points() {
                                let Some(point_value) = point.value() else {
                                    _ = coordinator
                                        .self_metrics
                                        .dropped_unsupported_points
                                        .fetch_add(1, Ordering::Relaxed);
                                    continue;
                                };
                                let identity = scalar_identity(
                                    family_name.as_str(),
                                    description.as_str(),
                                    unit.as_str(),
                                    build_labels(
                                        resource_attrs.as_slice(),
                                        scope_labels.as_slice(),
                                        collect_attributes(point.attributes()).as_slice(),
                                    ),
                                    kind,
                                    stored_value_type(point_value),
                                );
                                coordinator.store.record_scalar(
                                    partition_id,
                                    identity,
                                    scalar_value_from_number(point_value),
                                    point.time_unix_nano(),
                                    point.start_time_unix_nano(),
                                    point.flags(),
                                    coordinator.self_metrics.as_ref(),
                                );
                            }
                        }
                    }
                    DataType::Histogram => {
                        if let Some(histogram) = metric_data.as_histogram() {
                            let kind = match histogram.aggregation_temporality() {
                                AggregationTemporality::Delta => SeriesKind::HistogramDelta,
                                _ => SeriesKind::HistogramCumulative,
                            };
                            let family_name =
                                translate_metric_name(metric_name.as_str(), unit.as_str(), false);
                            for point in histogram.data_points() {
                                let identity = histogram_identity(
                                    family_name.as_str(),
                                    description.as_str(),
                                    unit.as_str(),
                                    build_labels(
                                        resource_attrs.as_slice(),
                                        scope_labels.as_slice(),
                                        collect_attributes(point.attributes()).as_slice(),
                                    ),
                                    kind,
                                );
                                let histogram_state = HistogramState {
                                    count: point.count(),
                                    sum: point.sum(),
                                    bucket_counts: point.bucket_counts().collect(),
                                    explicit_bounds: point.explicit_bounds().collect(),
                                    min: point.min(),
                                    max: point.max(),
                                    timestamp_unix_nano: point.time_unix_nano(),
                                    start_time_unix_nano: point.start_time_unix_nano(),
                                    last_seen: Instant::now(),
                                };
                                coordinator.store.record_histogram(
                                    partition_id,
                                    identity,
                                    histogram_state,
                                    point.flags(),
                                    coordinator.self_metrics.as_ref(),
                                );
                            }
                        }
                    }
                    DataType::Summary => {
                        if let Some(summary) = metric_data.as_summary() {
                            let family_name =
                                translate_metric_name(metric_name.as_str(), unit.as_str(), false);
                            for point in summary.data_points() {
                                let identity = summary_identity(
                                    family_name.as_str(),
                                    description.as_str(),
                                    unit.as_str(),
                                    build_labels(
                                        resource_attrs.as_slice(),
                                        scope_labels.as_slice(),
                                        collect_attributes(point.attributes()).as_slice(),
                                    ),
                                );
                                let summary_state = SummaryState {
                                    count: point.count(),
                                    sum: point.sum(),
                                    quantiles: point
                                        .quantile_values()
                                        .map(|value| (value.quantile(), value.value()))
                                        .collect(),
                                    timestamp_unix_nano: point.time_unix_nano(),
                                    last_seen: Instant::now(),
                                };
                                coordinator.store.record_summary(
                                    partition_id,
                                    identity,
                                    summary_state,
                                    point.flags(),
                                    coordinator.self_metrics.as_ref(),
                                );
                            }
                        }
                    }
                    DataType::ExponentialHistogram => {
                        _ = coordinator
                            .self_metrics
                            .dropped_unsupported_points
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

fn scalar_identity(
    family_name: &str,
    description: &str,
    unit: &str,
    labels: Vec<(String, String)>,
    kind: SeriesKind,
    value_type: StoredValueType,
) -> SeriesIdentity {
    SeriesIdentity {
        family_name: family_name.to_owned(),
        description: description.to_owned(),
        unit: unit.to_owned(),
        labels,
        kind,
        value_type: Some(value_type),
    }
}

fn histogram_identity(
    family_name: &str,
    description: &str,
    unit: &str,
    labels: Vec<(String, String)>,
    kind: SeriesKind,
) -> SeriesIdentity {
    SeriesIdentity {
        family_name: family_name.to_owned(),
        description: description.to_owned(),
        unit: unit.to_owned(),
        labels,
        kind,
        value_type: None,
    }
}

fn summary_identity(
    family_name: &str,
    description: &str,
    unit: &str,
    labels: Vec<(String, String)>,
) -> SeriesIdentity {
    SeriesIdentity {
        family_name: family_name.to_owned(),
        description: description.to_owned(),
        unit: unit.to_owned(),
        labels,
        kind: SeriesKind::Summary,
        value_type: None,
    }
}

fn build_labels(
    resource_attrs: &[(String, String)],
    scope_labels: &[(String, String)],
    point_attrs: &[(String, String)],
) -> Vec<(String, String)> {
    let mut raw =
        Vec::with_capacity(resource_attrs.len() + scope_labels.len() + point_attrs.len() + 2);
    raw.extend_from_slice(resource_attrs);
    raw.extend_from_slice(scope_labels);
    raw.extend_from_slice(point_attrs);

    let mut derived = Vec::new();
    let service_namespace = raw
        .iter()
        .find_map(|(key, value)| (key == "service.namespace").then_some(value.clone()));
    let service_name = raw
        .iter()
        .find_map(|(key, value)| (key == "service.name").then_some(value.clone()));
    if let Some(service_name) = service_name {
        let job = service_namespace
            .map(|namespace| format!("{namespace}/{service_name}"))
            .unwrap_or(service_name);
        derived.push(("job".to_owned(), job));
    }
    if let Some(instance) = raw
        .iter()
        .find_map(|(key, value)| (key == "service.instance.id").then_some(value.clone()))
    {
        derived.push(("instance".to_owned(), instance));
    }
    raw.extend(derived);

    sanitize_labels(raw)
}

fn sanitize_labels(raw: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut by_sanitized: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for (key, value) in raw {
        let sanitized = sanitize_prom_label_key(key.as_str());
        by_sanitized
            .entry(sanitized)
            .or_default()
            .push((key, value));
    }

    by_sanitized
        .into_iter()
        .map(|(sanitized, mut values)| {
            values.sort_by(|left, right| left.0.cmp(&right.0));
            let joined = values
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>()
                .join(";");
            (sanitized, joined)
        })
        .collect()
}

fn collect_scope_labels<S>(scope: S) -> Vec<(String, String)>
where
    S: InstrumentationScopeView,
{
    let mut labels = Vec::new();
    if let Some(name) = scope.name() {
        labels.push(("otel_scope_name".to_owned(), decode_str(name)));
    }
    if let Some(version) = scope.version() {
        labels.push(("otel_scope_version".to_owned(), decode_str(version)));
    }
    for (key, value) in collect_attributes(scope.attributes()) {
        labels.push((format!("otel_scope_{key}"), value));
    }
    labels
}

fn collect_attributes<I, A>(iter: I) -> Vec<(String, String)>
where
    I: Iterator<Item = A>,
    A: AttributeView,
{
    let mut attrs = Vec::new();
    for attr in iter {
        if let Some(value) = attr.value() {
            attrs.push((decode_str(attr.key()), any_value_to_string(value)));
        }
    }
    attrs
}

fn any_value_to_string<'a, V>(value: V) -> String
where
    V: AnyValueView<'a>,
{
    if let Some(string_value) = value.as_string() {
        return decode_str(string_value);
    }
    if let Some(bool_value) = value.as_bool() {
        return bool_value.to_string();
    }
    if let Some(int_value) = value.as_int64() {
        return int_value.to_string();
    }
    if let Some(double_value) = value.as_double() {
        return double_value.to_string();
    }
    if let Some(bytes_value) = value.as_bytes() {
        return BASE64_STANDARD.encode(bytes_value);
    }
    if let Some(array) = value.as_array() {
        let rendered = array.map(any_value_to_string).collect::<Vec<_>>().join(",");
        return format!("[{rendered}]");
    }
    if let Some(kvlist) = value.as_kvlist() {
        let mut rendered = Vec::new();
        for entry in kvlist {
            let key = decode_str(entry.key());
            let value = entry.value().map(any_value_to_string).unwrap_or_default();
            rendered.push(format!("{key}:{value}"));
        }
        return format!("{{{}}}", rendered.join(","));
    }
    String::new()
}

fn scalar_value_from_number(value: NumberDataValue) -> ScalarValue {
    match value {
        NumberDataValue::Integer(value) => ScalarValue::U64(value.max(0) as u64),
        NumberDataValue::Double(value) => ScalarValue::F64(value),
    }
}

fn stored_value_type(value: NumberDataValue) -> StoredValueType {
    match value {
        NumberDataValue::Integer(_) => StoredValueType::U64,
        NumberDataValue::Double(_) => StoredValueType::F64,
    }
}

fn translate_metric_name(name: &str, unit: &str, is_counter: bool) -> String {
    let mut translated = sanitize_prom_metric_name(name);
    if !unit.is_empty() && unit != "1" {
        let sanitized_unit = sanitize_prom_metric_name(unit);
        if !translated.ends_with(sanitized_unit.as_str()) {
            translated.push('_');
            translated.push_str(sanitized_unit.as_str());
        }
    }
    if is_counter && !translated.ends_with("_total") {
        translated.push_str("_total");
    }
    translated
}

fn decode_str(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn sanitize_prom_metric_name(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for (i, ch) in s.chars().enumerate() {
        let ok = matches!(ch, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | ':');
        if ok && !(i == 0 && ch.is_ascii_digit()) {
            out.push(ch);
        } else if ch == '.' || ch == '-' || ch == ' ' || ch == '/' {
            out.push('_');
        } else if i == 0 && ch.is_ascii_digit() {
            out.push('_');
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "metric".to_owned()
    } else {
        out
    }
}

fn sanitize_prom_label_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for (i, ch) in s.chars().enumerate() {
        let ok = matches!(ch, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | ':');
        if ok && !(i == 0 && ch.is_ascii_digit()) {
            out.push(ch);
        } else if ch == '.' || ch == '-' || ch == ' ' || ch == '/' {
            out.push('_');
        } else if i == 0 && ch.is_ascii_digit() {
            out.push('_');
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "label".to_owned()
    } else {
        out
    }
}

fn escape_prom_label_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => {
                out.push('\\');
                out.push('\\');
            }
            '"' => {
                out.push('\\');
                out.push('"');
            }
            '\n' => {
                out.push('\\');
                out.push('n');
            }
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use otap_df_engine::config::ExporterConfig;
    use otap_df_engine::context::ControllerContext;
    use otap_df_engine::testing::exporter::TestRuntime;
    use otap_df_engine::testing::test_node;
    use otap_df_pdata::OtlpProtoBytes;
    use otap_df_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use otap_df_pdata::proto::opentelemetry::common::v1::{
        AnyValue, InstrumentationScope, KeyValue,
    };
    use otap_df_pdata::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
        summary_data_point::ValueAtQuantile,
    };
    use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
    use otap_df_telemetry::registry::TelemetryRegistryHandle;
    use prost::Message as _;
    use reqwest::Client;
    use std::net::TcpListener as StdTestTcpListener;
    use std::sync::Arc;
    use std::time::Instant;

    fn encode_metrics_request(metrics: Vec<Metric>) -> Bytes {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue::new("service.name", AnyValue::new_string("engine")),
                        KeyValue::new("service.instance.id", AnyValue::new_string("a")),
                    ],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "scope".to_owned(),
                        version: "1.0.0".to_owned(),
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                    }),
                    metrics,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
            ..Default::default()
        };
        let mut buf = Vec::new();
        request
            .encode(&mut buf)
            .expect("metrics request should encode");
        Bytes::from(buf)
    }

    fn gauge_metric(name: &str, value: i64) -> Metric {
        Metric::build()
            .name(name)
            .unit("1")
            .data_gauge(Gauge::new(vec![
                NumberDataPoint::build()
                    .time_unix_nano(10u64)
                    .start_time_unix_nano(1u64)
                    .value_int(value)
                    .attributes(vec![KeyValue::new(
                        "node.id",
                        AnyValue::new_string("node-a"),
                    )])
                    .finish(),
            ]))
            .finish()
    }

    fn delta_counter_metric(name: &str, value: i64, start: u64, end: u64) -> Metric {
        Metric::build()
            .name(name)
            .unit("1")
            .data_sum(Sum::new(
                AggregationTemporality::Delta,
                true,
                vec![
                    NumberDataPoint::build()
                        .time_unix_nano(end)
                        .start_time_unix_nano(start)
                        .value_int(value)
                        .attributes(vec![KeyValue::new(
                            "node.id",
                            AnyValue::new_string("node-a"),
                        )])
                        .finish(),
                ],
            ))
            .finish()
    }

    fn histogram_metric(name: &str) -> Metric {
        Metric::build()
            .name(name)
            .unit("ms")
            .data_histogram(Histogram::new(
                AggregationTemporality::Cumulative,
                vec![
                    HistogramDataPoint::build()
                        .time_unix_nano(20u64)
                        .start_time_unix_nano(1u64)
                        .count(3u64)
                        .sum(9.0)
                        .bucket_counts(vec![1, 2])
                        .explicit_bounds(vec![5.0])
                        .finish(),
                ],
            ))
            .finish()
    }

    fn summary_metric(name: &str) -> Metric {
        Metric::build()
            .name(name)
            .unit("ms")
            .data_summary(Summary::new(vec![
                SummaryDataPoint::build()
                    .time_unix_nano(30u64)
                    .start_time_unix_nano(1u64)
                    .count(2u64)
                    .sum(6.0)
                    .quantile_values(vec![
                        ValueAtQuantile::new(0.5, 3.0),
                        ValueAtQuantile::new(0.9, 5.0),
                    ])
                    .finish(),
            ]))
            .finish()
    }

    fn exporter_wrapper(port: u16) -> ExporterWrapper<OtapPdata> {
        let controller = ControllerContext::new(TelemetryRegistryHandle::new());
        let pipeline = controller.pipeline_context_with("default".into(), "main".into(), 0, 1, 0);
        let mut node_config = NodeUserConfig::new_exporter_config(PROMETHEUS_EXPORTER_URN);
        node_config.config = serde_json::json!({
            "host": "127.0.0.1",
            "port": port,
            "path": "/metrics",
            "json_path": "/metrics.json"
        });
        let node_config = Arc::new(node_config);
        let exporter_config = ExporterConfig::new("test_exporter");
        (PROMETHEUS_EXPORTER.create)(
            pipeline,
            test_node("prometheus"),
            node_config,
            &exporter_config,
        )
        .expect("prometheus exporter should be created")
    }

    fn pick_available_port() -> u16 {
        let listener =
            StdTestTcpListener::bind("127.0.0.1:0").expect("ephemeral port bind should succeed");
        listener
            .local_addr()
            .expect("listener should expose a local address")
            .port()
    }

    #[test]
    fn validate_config_rejects_relative_json_path() {
        let error = validate_config(&serde_json::json!({
            "json_path": "metrics.json"
        }))
        .expect_err("config should be rejected");
        assert!(matches!(error, ConfigError::InvalidUserConfig { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires loopback listener permissions"]
    async fn exporter_serves_prometheus_and_json() {
        let port = pick_available_port();
        let exporter = exporter_wrapper(port);
        let bytes = encode_metrics_request(vec![
            gauge_metric("queue.depth", 7),
            delta_counter_metric("requests", 3, 10, 20),
            histogram_metric("latency"),
            summary_metric("summary"),
        ]);
        TestRuntime::<OtapPdata>::new()
            .set_exporter(exporter)
            .run_test(move |ctx| async move {
                ctx.send_pdata(OtapPdata::new_default(OtapPayload::OtlpBytes(
                    OtlpProtoBytes::ExportMetricsRequest(bytes),
                )))
                .await
                .expect("metrics payload should send");
                ctx.sleep(Duration::from_millis(100)).await;
                ctx.send_shutdown(Instant::now() + Duration::from_secs(1), "test shutdown")
                    .await
                    .expect("shutdown should send");
            })
            .run_validation(move |_ctx, result| async move {
                result.expect("exporter should stop cleanly");

                let client = Client::new();
                let prom = client
                    .get(format!("http://127.0.0.1:{port}/metrics"))
                    .send()
                    .await
                    .expect("prom scrape should succeed")
                    .text()
                    .await
                    .expect("prometheus body should decode");
                assert!(prom.contains("queue_depth"));
                assert!(prom.contains("requests_total"));
                assert!(prom.contains("latency_ms_bucket"));

                let json = client
                    .get(format!("http://127.0.0.1:{port}/metrics.json"))
                    .send()
                    .await
                    .expect("json scrape should succeed")
                    .text()
                    .await
                    .expect("json body should decode");
                let json: serde_json::Value =
                    serde_json::from_str(json.as_str()).expect("json body should parse");
                assert!(json["metric_sets"].is_array());
            });
    }
}
