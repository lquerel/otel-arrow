// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics for the Weaver live-check processor.

use otap_df_telemetry::instrument::{Counter, Gauge};
use otap_df_telemetry_macros::metric_set;

/// Metrics emitted by the Weaver live-check processor.
#[metric_set(name = "weaver_live_check.processor.metrics")]
#[derive(Debug, Default, Clone)]
pub struct WeaverLiveCheckMetrics {
    /// Number of telemetry batches examined.
    #[metric(unit = "{batch}")]
    pub batches_processed: Counter<u64>,

    /// Number of telemetry items in examined batches.
    #[metric(unit = "{item}")]
    pub items_processed: Counter<u64>,

    /// Number of finding log records successfully emitted to the findings port.
    #[metric(unit = "{finding}")]
    pub findings_emitted: Counter<u64>,

    /// Number of finding log records dropped because the findings side-stream was unavailable.
    #[metric(unit = "{finding}")]
    pub findings_dropped: Counter<u64>,

    /// Number of informational findings emitted.
    #[metric(unit = "{finding}")]
    pub findings_information: Counter<u64>,

    /// Number of improvement findings emitted.
    #[metric(unit = "{finding}")]
    pub findings_improvement: Counter<u64>,

    /// Number of violation findings emitted.
    #[metric(unit = "{finding}")]
    pub findings_violation: Counter<u64>,

    /// Number of normalization, checking, or encoding failures handled fail-open.
    #[metric(unit = "{error}")]
    pub processing_errors: Counter<u64>,

    /// Total attribute names observed for registry coverage accounting.
    #[metric(unit = "{attribute}")]
    pub attribute_names_seen: Counter<u64>,

    /// Total attribute names that matched the configured registry.
    #[metric(unit = "{attribute}")]
    pub attribute_names_matched: Counter<u64>,

    /// Total metric names observed for registry coverage accounting.
    #[metric(unit = "{metric}")]
    pub metric_names_seen: Counter<u64>,

    /// Total metric names that matched the configured registry.
    #[metric(unit = "{metric}")]
    pub metric_names_matched: Counter<u64>,

    /// Total event names observed for registry coverage accounting.
    #[metric(unit = "{event}")]
    pub event_names_seen: Counter<u64>,

    /// Total event names that matched the configured registry.
    #[metric(unit = "{event}")]
    pub event_names_matched: Counter<u64>,

    /// Cumulative total coverage candidates seen so far.
    #[metric(unit = "{item}")]
    pub coverage_items_seen: Gauge<u64>,

    /// Cumulative total coverage candidates that matched the registry.
    #[metric(unit = "{item}")]
    pub coverage_items_matched: Gauge<u64>,

    /// Ratio of matched coverage candidates to all observed candidates.
    #[metric(unit = "{ratio}")]
    pub coverage_ratio: Gauge<f64>,
}
