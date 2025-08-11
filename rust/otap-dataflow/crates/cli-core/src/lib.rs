// SPDX-License-Identifier: Apache-2.0
//! CLI core: reusable argument parsing, engine launcher, and extension hooks.

use std::path::PathBuf;
use std::sync::Arc;
use clap::{Arg, Command, ArgMatches, builder::PossibleValue};
use otap_df_config::pipeline_group::{Quota, NumaStrategy};
use otap_df_config::pipeline::PipelineConfig;
use otap_df_controller::{Controller, build_numa_provider, NumaProvider};
use otap_df_engine::PipelineFactory;

/// Core engine arguments (OSS baseline).
#[derive(Debug, Clone)]
/// Parsed engine arguments (stable OSS baseline; extend externally via additional clap args).
pub struct EngineArgs {
    /// Path to pipeline configuration file.
    pub pipeline: PathBuf,
    /// Number of cores to request (0 => all available).
    pub num_cores: usize,
    /// NUMA placement strategy.
    pub numa_strategy: NumaStrategy,
    /// Preferred NUMA node ids (empty => all discovered).
    pub preferred_numa_nodes: Vec<usize>,
    /// Whether to print system information banner (disabled with --no-system-info).
    pub show_system_info: bool,
}

/// Build the base clap Command (can be extended by proprietary crates).
pub fn base_command(_bin_name: &str) -> Command {
    Command::new("otap-df")
        .arg(Arg::new("pipeline")
            .short('p')
            .long("pipeline")
            .num_args(1)
            .required(true)
            .help("Path to the pipeline configuration file (.json, .yaml, or .yml)"))
        .arg(Arg::new("num-cores")
            .long("num-cores")
            .num_args(1)
            .value_parser(clap::value_parser!(usize))
            .default_value("0")
            .help("Number of cores to use (0 for all available)"))
        .arg(Arg::new("numa-strategy")
            .long("numa-strategy")
            .value_parser([PossibleValue::new("spread"), PossibleValue::new("pack"), PossibleValue::new("auto")])
            .default_value("spread")
            .help("NUMA strategy (spread, pack, auto). Ignored if compiled without the 'numa' feature."))
        .arg(Arg::new("preferred-numa-nodes")
            .long("preferred-numa-nodes")
            .value_delimiter(',')
            .value_parser(clap::value_parser!(usize))
            .num_args(0..)
            .help("Comma-separated list of preferred NUMA node ids. Omit for all discovered nodes."))
        .arg(Arg::new("no-system-info")
            .long("no-system-info")
            .action(clap::ArgAction::SetTrue)
            .help("Suppress system information banner at startup"))
        .after_help("Examples:\n  otap-df --pipeline configs/otlp-perf.yaml --num-cores 4 --numa-strategy spread\n  otap-df -p configs/otlp-perf.yaml")
}

/// Parse EngineArgs from ArgMatches (after any extension args have been added).
pub fn parse_engine_args(m: &ArgMatches) -> EngineArgs {
    let pipeline = m.get_one::<String>("pipeline").expect("pipeline required").into();
    let num_cores = *m.get_one::<usize>("num-cores").expect("num-cores has default");
    let numa_strategy = match m.get_one::<String>("numa-strategy").map(|s| s.as_str()).unwrap_or("spread") {
        "pack" => NumaStrategy::Pack,
        "auto" => NumaStrategy::Auto,
        _ => NumaStrategy::Spread,
    };
    let preferred_numa_nodes: Vec<usize> = m.get_many::<usize>("preferred-numa-nodes").map(|vals| vals.copied().collect()).unwrap_or_default();
    let show_system_info = !m.get_flag("no-system-info");
    EngineArgs { pipeline, num_cores, numa_strategy, preferred_numa_nodes, show_system_info }
}

/// Construct Quota from EngineArgs.
pub fn quota_from_args(args: &EngineArgs) -> Quota {
    Quota { num_cores: args.num_cores, numa_strategy: args.numa_strategy, preferred_numa_nodes: args.preferred_numa_nodes.clone() }
}

/// Run the engine given already parsed EngineArgs and an optional injected NUMA provider.
pub fn run_engine<PData: 'static + Clone + Send + Sync + std::fmt::Debug>(
    factory: &'static PipelineFactory<PData>,
    args: EngineArgs,
    provider: Option<Arc<dyn NumaProvider>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let provider = provider.unwrap_or_else(|| build_numa_provider().into());
    let controller = Controller::new(factory, provider);
    let pipeline_cfg = PipelineConfig::from_file(
        "default_pipeline_group".into(),
        "default_pipeline".into(),
        &args.pipeline,
    )?;
    let quota = quota_from_args(&args);
    controller.run_forever(pipeline_cfg, quota)?;
    Ok(())
}

/// Extension point: configuration validation hook signature.
pub type ConfigValidator = fn(&PipelineConfig) -> Result<(), String>;

/// Run engine with additional validators (executed after load, before run).
pub fn run_engine_with_validators<PData: 'static + Clone + Send + Sync + std::fmt::Debug>(
    factory: &'static PipelineFactory<PData>,
    args: EngineArgs,
    provider: Option<Arc<dyn NumaProvider>>,
    validators: &[ConfigValidator],
) -> Result<(), Box<dyn std::error::Error>> {
    let provider = provider.unwrap_or_else(|| build_numa_provider().into());
    let controller = Controller::new(factory, provider);
    let pipeline_cfg = PipelineConfig::from_file(
        "default_pipeline_group".into(),
        "default_pipeline".into(),
        &args.pipeline,
    )?;
    for v in validators { v(&pipeline_cfg).map_err(|e| format!("validation failed: {e}"))?; }
    let quota = quota_from_args(&args);
    controller.run_forever(pipeline_cfg, quota)?;
    Ok(())
}
