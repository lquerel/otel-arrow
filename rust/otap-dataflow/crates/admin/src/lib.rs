// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! HTTP server for exposing admin endpoints.

mod dashboard;
pub mod error;
mod health;
mod pipeline;
mod pipeline_group;
mod telemetry;

use axum::Router;
use reqwest::Client;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::Mutex as TokioMutex;
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;

use crate::error::Error;
use otap_df_config::engine::{HttpAdminMetricsProxySettings, HttpAdminSettings};
use otap_df_engine::control::PipelineAdminSender;
use otap_df_state::store::ObservedStateHandle;
use otap_df_telemetry::registry::TelemetryRegistryHandle;
use otap_df_telemetry::{otel_info, otel_warn};

/// Shared state for the HTTP admin server.
#[derive(Clone)]
struct AppState {
    /// The observed state store for querying the current state of the entire system.
    observed_state_store: ObservedStateHandle,

    /// The metrics registry for querying current metrics.
    metrics_registry: TelemetryRegistryHandle,

    /// The control message senders for controlling pipelines.
    ctrl_msg_senders: Arc<TokioMutex<Vec<Arc<dyn PipelineAdminSender>>>>,

    /// Optional upstream exporter proxy used to serve same-origin metrics.
    metrics_proxy: Option<MetricsProxyState>,
}

#[derive(Clone)]
struct MetricsProxyState {
    config: HttpAdminMetricsProxySettings,
    client: Client,
    cache: Arc<StdMutex<HashMap<MetricsProxyCacheKey, CachedMetricsProxyResponse>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MetricsProxyCacheKey {
    Prometheus,
    Json,
}

#[derive(Clone)]
struct CachedMetricsProxyResponse {
    body: Arc<Vec<u8>>,
    content_type: String,
    expires_at: Instant,
}

impl MetricsProxyState {
    fn new(config: HttpAdminMetricsProxySettings) -> Self {
        Self {
            config,
            client: Client::new(),
            cache: Arc::new(StdMutex::new(HashMap::new())),
        }
    }
}

/// Run the admin HTTP server until shutdown is requested.
pub async fn run(
    config: HttpAdminSettings,
    observed_store: ObservedStateHandle,
    ctrl_msg_senders: Vec<Arc<dyn PipelineAdminSender>>,
    metrics_registry: TelemetryRegistryHandle,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let metrics_proxy = config.metrics_proxy.clone().map(MetricsProxyState::new);
    let app_state = AppState {
        observed_state_store: observed_store,
        metrics_registry,
        metrics_proxy,
        ctrl_msg_senders: Arc::new(TokioMutex::new(ctrl_msg_senders)),
    };

    let app = Router::new()
        .merge(health::routes())
        .merge(telemetry::routes())
        .merge(pipeline_group::routes())
        .merge(pipeline::routes())
        .merge(dashboard::routes())
        .layer(ServiceBuilder::new())
        .with_state(app_state);

    // Parse the configured bind address.
    let addr = config.bind_address.parse::<SocketAddr>().map_err(|e| {
        let details = format!("{e}");
        otel_warn!(
            "endpoint.parse_address_failed",
            bind_address = config.bind_address.as_str(),
            error = details.as_str(),
            message = "Failed to parse admin server bind address"
        );
        Error::InvalidBindAddress {
            bind_address: config.bind_address.clone(),
            details,
        }
    })?;

    // Bind the TCP listener.
    let listener = TcpListener::bind(&addr).await.map_err(|e| {
        let addr_str = addr.to_string();
        let details = format!("{e}");
        otel_warn!(
            "endpoint.bind_failed",
            bind_address = addr_str.as_str(),
            error = details.as_str(),
            message = "Failed to bind admin server"
        );
        Error::BindFailed {
            addr: addr_str,
            details,
        }
    })?;

    let addr_str = addr.to_string();
    otel_info!(
        "endpoint.start",
        bind_address = addr_str.as_str(),
        message = "Admin HTTP server listening"
    );

    // Start serving requests, with graceful shutdown on signal.
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel.cancelled().await;
        })
        .await
        .map_err(|e| Error::ServerError {
            addr: addr.to_string(),
            details: format!("{e}"),
        })
}
