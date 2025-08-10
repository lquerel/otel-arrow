//! Create and run a multi-core pipeline

use mimalloc_rust::*;
use otap_df_cli_core::{base_command, parse_engine_args, run_engine};
use otap_df_controller::{NumaProvider, build_numa_provider};
use otap_df_otap::OTAP_PIPELINE_FACTORY;
use std::sync::Arc;

#[global_allocator]
static GLOBAL_MIMALLOC: GlobalMiMalloc = GlobalMiMalloc;

#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
enum ArgNumaStrategy { Spread, Pack, Auto }

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse arguments first
    let cmd = base_command(env!("CARGO_PKG_NAME"));
    let matches = cmd.get_matches();
    let engine_args = parse_engine_args(&matches);

    // Always build the NUMA provider (independent of system info banner)
    let provider: Arc<dyn NumaProvider> = build_numa_provider().into();

    if engine_args.show_system_info {
        println!("{}", system_info(Some(&*provider), Some(engine_args.num_cores)));
    }

    let res = run_engine(&OTAP_PIPELINE_FACTORY, engine_args.clone(), Some(provider));
    if let Ok(_) = res { println!("Pipeline run successfully"); } else if let Err(e) = &res { eprintln!("{e}"); }
    res
}

fn system_info(provider: Option<&dyn NumaProvider>, requested_cores: Option<usize>) -> String {
    #[allow(unused_variables)]
    let provider = provider; // retain for cfg branches
    #[cfg(feature = "numa")]
    let numa_section = {
        if let Some(p) = provider {
            let all_cores = core_affinity::get_core_ids().unwrap_or_default();
            if let Some(nodes) = p.discover(&all_cores) {
                if nodes.is_empty() { format!("NUMA: no explicit nodes (treat as 1). PUs: {}", all_cores.len()) } else {
                    let mut detail = String::new();
                    for n in &nodes { detail.push_str(&format!(" - node {}: {} CPUs\n", n.id, n.cores.len())); }
                    format!("NUMA nodes detected: {}\n{}", nodes.len(), detail.trim_end())
                }
            } else { "NUMA: discovery unavailable (fallback to affinity only)".to_string() }
        } else {
            "NUMA: provider unavailable".to_string()
        }
    };
    #[cfg(not(feature = "numa"))]
    let numa_section = "NUMA optimization: disabled at compile time (build without 'numa' feature)".to_string();

    let available_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let build_mode = if cfg!(debug_assertions) { "debug" } else { "release" };
    let debug_warning = if cfg!(debug_assertions) {
        "\n\n⚠️  WARNING: This binary was compiled in debug mode. Use 'cargo build --release' for optimal performance."
    } else { "" };

    let receivers: Vec<&str> = OTAP_PIPELINE_FACTORY.get_receiver_factory_map().keys().copied().collect();
    let processors: Vec<&str> = OTAP_PIPELINE_FACTORY.get_processor_factory_map().keys().copied().collect();
    let exporters: Vec<&str> = OTAP_PIPELINE_FACTORY.get_exporter_factory_map().keys().copied().collect();
    let mut receivers = receivers; let mut processors = processors; let mut exporters = exporters;
    receivers.sort(); processors.sort(); exporters.sort();

    let requested_line = if let Some(req) = requested_cores { if req > 0 && req != available_cores { format!("\nRequested CPU cores: {} (--num-cores)", req) } else { String::new() } } else { String::new() };

    let sys_info_sep = "================= SYSTEM INFO ================= (use --no-system-info to suppress this banner)";
    let plugin_info_sep = "================= PLUGIN INFO =================";
    let end_sep = "==============================================";
    format!(
        "{sys_info_sep}\nAvailable CPU cores: {cores}{requested}\nBuild mode: {mode}\nDefault memory allocator: mimalloc\n{numa}\n\n{plugin_info_sep}\nReceivers: {r}\nProcessors: {p}\nExporters: {e}\n\n{warn}\n{end_sep}\n",
        cores = available_cores,
        requested = requested_line,
        mode = build_mode,
        numa = numa_section,
        r = receivers.join(", "),
        p = processors.join(", "),
        e = exporters.join(", "),
        warn = debug_warning,
    )
}
