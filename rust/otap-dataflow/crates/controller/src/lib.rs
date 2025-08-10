// SPDX-License-Identifier: Apache-2.0

//! OTAP Dataflow Engine Controller
//!
//! This controller manages and monitors the execution of pipelines within the current process.
//! It uses a thread-per-core model, where each thread is pinned to a specific CPU core.
//! This approach maximizes multi-core efficiency and reduces contention between threads,
//! ensuring that each pipeline runs on a dedicated core for predictable and optimal CPU usage.
//!
//! Future work includes:
//! - TODO: Status and health checks for pipelines
//! - TODO: Graceful shutdown of pipelines
//! - TODO: Auto-restart threads in case of panic
//! - TODO: Live pipeline updates
//! - TODO: Better resource control
//! - TODO: Monitoring
//! - TODO: Support pipeline groups

use otap_df_config::{pipeline::PipelineConfig, pipeline_group::Quota};
use otap_df_engine::PipelineFactory;
use std::thread;
pub mod numa;
use std::sync::Arc;

/// Error types and helpers for the controller module.
pub mod error;

/// Controller for managing pipelines in a thread-per-core model.
///
/// # Thread Safety
/// This struct is designed to be used in multi-threaded contexts. Each pipeline is run on a
/// dedicated thread pinned to a CPU core.
/// Intended for use as a long-lived process controller.
pub struct Controller<PData: 'static + Clone + Send + Sync + std::fmt::Debug> {
    /// The pipeline factory used to build runtime pipelines.
    pipeline_factory: &'static PipelineFactory<PData>,
    numa_provider: Arc<dyn NumaProvider>,
}

impl<PData: 'static + Clone + Send + Sync + std::fmt::Debug> Controller<PData> {
    /// Creates a new controller with the given pipeline factory.
    pub fn new(pipeline_factory: &'static PipelineFactory<PData>, numa_provider: Arc<dyn NumaProvider>) -> Self {
        Self { pipeline_factory, numa_provider }
    }
    /// Access the underlying NUMA provider (primarily for testing / diagnostics).
    pub fn numa_provider(&self) -> &Arc<dyn NumaProvider> { &self.numa_provider }

    /// Starts the controller with the given pipeline configuration and quota.
    pub fn run_forever(&self, pipeline: PipelineConfig, quota: Quota) -> Result<(), error::Error> {
        let provider = self.numa_provider.clone();
        // Get available CPU cores for pinning
        let all_core_ids =
            core_affinity::get_core_ids().ok_or_else(|| error::Error::InternalError {
                message: "Failed to get available CPU cores".to_owned(),
            })?;
        // Discover topology (optional)
        let discovered = provider.discover(&all_core_ids);
        // Determine requested core count
        let num_cpu_cores = all_core_ids.len();
        let num_requested_cores = if quota.num_cores == 0 { num_cpu_cores } else { quota.num_cores.min(num_cpu_cores) };

        let selected: Vec<(core_affinity::CoreId, Option<usize>)> = if let Some(nodes) = &discovered {
            let sel = provider.select_cores(nodes, quota.numa_strategy, num_requested_cores, &quota.preferred_numa_nodes);
            if sel.is_empty() { all_core_ids.iter().take(num_requested_cores).cloned().map(|c| (c, None)).collect() } else { sel }
        } else {
            all_core_ids.iter().take(num_requested_cores).cloned().map(|c| (c, None)).collect()
        };

        // (Future) Prepare per-node state insertion point
        if let Some(nodes) = &discovered {
            for n in nodes { let _ = provider.prepare_node_local_state(n.id); }
        }

        // Start one thread per selected core
        let mut threads = Vec::with_capacity(selected.len());
        for (thread_id, (core_id, node_opt)) in selected.into_iter().enumerate() {
            let pipeline_config = pipeline.clone();
            let pipeline_factory = self.pipeline_factory;
            let provider_ref = provider.clone();
            let thread_name = match node_opt { Some(node) => format!("pipeline-node-{node}-core-{}", core_id.id), None => format!("pipeline-core-{}", core_id.id) };
            let handle = thread::Builder::new().name(thread_name).spawn(move || {
                if let Some(nid) = node_opt { provider_ref.bind_memory(nid); }
                provider_ref.on_worker_start(node_opt);
                let r = Self::run_pipeline_thread(core_id, pipeline_config, pipeline_factory);
                provider_ref.on_worker_stop(node_opt);
                r
            }).map_err(|e| error::Error::InternalError { message: format!("Failed to spawn thread {thread_id}: {e}") })?;
            threads.push(handle);
        }
        // Wait for all threads
        for (thread_id, handle) in threads.into_iter().enumerate() {
            match handle.join() {
                Ok(Ok(_)) => {},
                Ok(Err(e)) => { return Err(e); },
                Err(e) => { return Err(error::Error::InternalError { message: format!("Failed to join thread pipeline-core-{thread_id:?}: {e:?}") }); }
            }
        }
        Ok(())
    }

    /// Runs a single pipeline in the current thread.
    fn run_pipeline_thread(
        core_id: core_affinity::CoreId,
        pipeline_config: PipelineConfig,
        pipeline_factory: &'static PipelineFactory<PData>,
    ) -> Result<Vec<()>, error::Error> {
        // Pin thread to specific core
        if !core_affinity::set_for_current(core_id) {
            // Continue execution even if pinning fails.
            // This is acceptable because the OS will still schedule the thread, but performance may be less predictable.
            // ToDo Add a warning here once logging is implemented.
        }

        // Build the runtime pipeline from the configuration AFTER pinning to benefit from first-touch locality
        let runtime_pipeline = pipeline_factory
            .build(pipeline_config.clone())
            .map_err(|e| error::Error::PipelineRuntimeError {
                source: Box::new(e),
            })?;

        // Start the pipeline (this will use the current thread's Tokio runtime)
        runtime_pipeline
            .run_forever()
            .map_err(|e| error::Error::PipelineRuntimeError {
                source: Box::new(e),
            })
    }
}

pub use numa::{NumaProvider, NumaNode, build_numa_provider};

// --- Private helpers ---
// Remove old NUMA helper functions; now located in numa module
