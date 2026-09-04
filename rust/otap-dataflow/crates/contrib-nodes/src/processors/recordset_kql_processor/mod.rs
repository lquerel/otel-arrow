// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

pub(crate) mod config;
// Moved from `engine-recordset-otlp-bridge` crate which has different lint
// settings. Lint compliance will be addressed in a follow-up.
otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = processor::RECORDSET_KQL_PROCESSOR_URN,
    target = "microsoft.processor.recordset_kql",
);

#[allow(
    elided_lifetimes_in_paths,
    missing_docs,
    unsafe_code,
    unused_qualifications,
    unused_results,
    clippy::explicit_into_iter_loop,
    clippy::must_use_candidate,
    clippy::print_stdout,
    clippy::unwrap_used,
    rust_2018_idioms
)]
pub mod otlp_bridge;
pub(crate) mod processor;

use self::config::RecordsetKqlProcessorConfig;
use self::processor::RecordsetKqlProcessor;
use otel_arrow_dfe_otap::pdata::OtapPdata;

use otel_arrow_dfe_config::error::Error as ConfigError;
use otel_arrow_dfe_engine::{
    component_config::ResolvedNodeConfig, config::ProcessorConfig, context::PipelineContext,
    node::NodeId, processor::ProcessorWrapper,
};
use std::sync::Arc;

/// Factory function to create a KQL processor
pub fn create_recordset_kql_processor(
    pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<ResolvedNodeConfig>,
    processor_config: &ProcessorConfig,
    _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError> {
    let config = node_config.component_config::<RecordsetKqlProcessorConfig>()?;

    let processor =
        RecordsetKqlProcessor::with_pipeline_ctx(pipeline_ctx, config.as_ref().clone())?;

    Ok(ProcessorWrapper::local(
        processor,
        node,
        node_config.effective(),
        processor_config,
    ))
}
