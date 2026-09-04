// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Azure Monitor Exporter for OTAP.
//!
//! Sends OpenTelemetry logs to Azure Monitor using the Data Collection Rules (DCR) API.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = AZURE_MONITOR_EXPORTER_URN,
    target = "microsoft.exporter.azure_monitor",
);

use linkme::distributed_slice;
use otel_arrow_dfe_engine::ExporterFactory;
use otel_arrow_dfe_engine::component_config::{
    ConfigSnapshotPolicy, ResolvedNodeConfig, resolve_typed_config,
};
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::node::NodeId;
use std::sync::Arc;

use otel_arrow_dfe_otap::OTAP_EXPORTER_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;

mod client;
/// Configuration types for the Azure Monitor Exporter.
pub mod config;
mod error;
mod exporter;
mod gzip_batcher;
mod heartbeat;
mod in_flight_exports;
/// Metrics types for the Azure Monitor Exporter.
pub mod metrics;
mod state;
mod transformer;

pub use client::LogsIngestionClient;
pub use config::Config;
pub use error::Error;
pub use exporter::AzureMonitorExporter;
pub use gzip_batcher::{FinalizeResult, GzipBatcher, GzipResult, PushResult};
pub use heartbeat::Heartbeat;
pub use metrics::{
    AzureMonitorExporterExportMetrics, AzureMonitorExporterHeartbeatMetrics,
    AzureMonitorExporterHttpMetrics, AzureMonitorExporterMetricsRc,
    AzureMonitorExporterOperationalMetrics, ExportSignalAttributes,
};
pub use transformer::Transformer;

use otel_arrow_dfe_engine::capability::auth::bearer_token_provider::BearerTokenProvider;

/// URN identifying the Azure Monitor Exporter in configuration pipelines.
pub const AZURE_MONITOR_EXPORTER_URN: &str = "urn:microsoft:exporter:azure_monitor";

/// Register Azure Monitor Exporter with the OTAP exporter factory.
///
/// Uses the `distributed_slice` macro for automatic discovery by the dataflow engine.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Exporter)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static AZURE_MONITOR_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: AZURE_MONITOR_EXPORTER_URN,
    create: |pipeline_ctx: PipelineContext,
             node: NodeId,
             node_config: Arc<ResolvedNodeConfig>,
             exporter_config: &ExporterConfig,
             capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let cfg = node_config.component_config::<Config>()?;

        // Resolve the bound bearer token provider capability. The exporter relies
        // on an extension (e.g.`azure_identity_auth`) bound to this node via the
        // `bearer_token_provider` capability to acquire credentials.
        let token_provider = capabilities
            .require_local::<BearerTokenProvider>()
            .map_err(|e| otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error: e.to_string(),
            })?;

        Ok(ExporterWrapper::local(
            AzureMonitorExporter::new(pipeline_ctx, cfg.as_ref().clone(), token_provider).map_err(
                |e| otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                    error: e.to_string(),
                },
            )?,
            node,
            node_config.effective(),
            exporter_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    resolve_config: resolve_typed_config::<Config>,
    snapshot_policy: ConfigSnapshotPolicy::TypedSafe,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_urn_constant() {
        assert_eq!(
            AZURE_MONITOR_EXPORTER_URN,
            "urn:microsoft:exporter:azure_monitor"
        );
    }
}
