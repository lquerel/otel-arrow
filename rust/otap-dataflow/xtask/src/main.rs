// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! See <https://github.com/matklad/cargo-xtask/>.
//!
//! This binary defines various auxiliary build commands, which are not
//! expressible with just `cargo`.
//!
//! This binary is integrated into the `cargo` command line by using an alias in
//! `.cargo/config`.

// This crate is a CLI tool and can use stdout and stderr for logging.
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

use std::io::{BufRead, BufReader};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

mod component_inventory;
mod crates_publish;
mod diagnostics;
mod genproto;
mod publish_policy;
mod structure_check;

#[cfg(not(tarpaulin_include))]
fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let task = args.next();

    match task {
        None => print_help(),
        Some(task) => match task.as_str() {
            "check" => check_all(parse_check_options(&args.collect::<Vec<_>>())?),
            "quick-check" => {
                ensure_no_extra_args("quick-check", &args.collect::<Vec<_>>())?;
                quick_check()
            }
            "check-benches" => {
                ensure_no_extra_args("check-benches", &args.collect::<Vec<_>>())?;
                check_benches()
            }
            "compile-proto" => {
                ensure_no_extra_args("compile-proto", &args.collect::<Vec<_>>())?;
                genproto::compile_proto()?;
                Ok(())
            }
            "structure-check" => {
                ensure_no_extra_args("structure-check", &args.collect::<Vec<_>>())?;
                structure_check::run()
            }
            "component-inventory" => component_inventory::run(&args.collect::<Vec<_>>()),
            "crates-publish" => crates_publish::run(&args.collect::<Vec<_>>()),
            "help" => {
                ensure_no_extra_args("help", &args.collect::<Vec<_>>())?;
                print_help()
            }
            _ => {
                eprintln!("Unknown task: {task}");
                print_help()
            }
        },
    }
}

/// Prints help message.
#[cfg(not(tarpaulin_include))]
pub fn print_help() -> anyhow::Result<()> {
    println!(
        "
Usage: Execute the command using `cargo xtask <task>`, e.g., `cargo xtask check`.

Tasks:
  - check [--diagnostics] [--nextest]: Run the required full validation suite: structure check, cargo fmt --all, cargo clippy --workspace --all-targets, a no_std build of the crates that support it, and workspace tests. By default tests use cargo test --workspace. With --nextest, run nextest and then cargo test --workspace --doc. The optional diagnostics flag prints end-of-run timing and hotspot summaries.
  - quick-check: Run a faster iterative subset: structure check, cargo fmt --all, cargo clippy --workspace --lib --bins --tests, and cargo test --workspace --lib --bins --tests --no-run. This is not a replacement for `cargo xtask check`.
  - check-benches: Lint and compile bench targets only.
  - structure-check: Validate the entire structure of the project.
  - compile-proto: Compile the protobufs files
  - component-inventory [--check <baseline>] [--update-baseline] [--format <table|json|yaml>]: Manage and verify the component inventory baseline.
  - crates-publish <plan|check|preflight VERSION [FORECAST_PATH]|publish VERSION>: Plan, validate, preflight, or publish crates.io packages.
"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CheckOptions {
    diagnostics: bool,
    nextest: bool,
}

fn parse_check_options(args: &[String]) -> anyhow::Result<CheckOptions> {
    let mut options = CheckOptions::default();
    for arg in args {
        match arg.as_str() {
            "--diagnostics" => options.diagnostics = true,
            "--nextest" => options.nextest = true,
            _ => anyhow::bail!("Unknown `cargo xtask check` argument: {arg}"),
        }
    }
    Ok(options)
}

fn ensure_no_extra_args(task: &str, args: &[String]) -> anyhow::Result<()> {
    if args.is_empty() {
        Ok(())
    } else {
        anyhow::bail!(
            "`cargo xtask {task}` does not accept extra arguments: {}",
            args.join(" ")
        );
    }
}

fn check_all(options: CheckOptions) -> anyhow::Result<()> {
    let mut diagnostics = options
        .diagnostics
        .then(diagnostics::DiagnosticsCollector::new);

    run_structure_step(diagnostics.as_mut())?;
    run_component_inventory_step(diagnostics.as_mut())?;
    format_all(diagnostics.as_mut())?;
    clippy_all(options, diagnostics.as_mut())?;
    build_no_std()?;
    test_all(options, diagnostics.as_mut())?;

    if let Some(diagnostics) = diagnostics.as_ref() {
        diagnostics.print_summary();
    }

    Ok(())
}

fn quick_check() -> anyhow::Result<()> {
    structure_check::run()?;
    component_inventory::run(&[
        "--check".to_string(),
        "components-baseline.json".to_string(),
    ])?;
    format_all(None)?;
    clippy_quick()?;
    test_quick()?;
    Ok(())
}

fn check_benches() -> anyhow::Result<()> {
    clippy_benches()?;
    compile_benches()?;
    Ok(())
}

fn run_structure_step(
    mut diagnostics: Option<&mut diagnostics::DiagnosticsCollector>,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let result = structure_check::run();
    let duration = start.elapsed();

    if let Some(diagnostics) = &mut diagnostics {
        diagnostics.record_step("structure", duration, step_status_from_result(&result));
    }

    if result.is_err()
        && let Some(diagnostics) = &mut diagnostics
    {
        diagnostics.print_summary();
    }

    result
}

fn run_component_inventory_step(
    mut diagnostics: Option<&mut diagnostics::DiagnosticsCollector>,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let result = component_inventory::run(&[
        "--check".to_string(),
        "components-baseline.json".to_string(),
    ]);
    let duration = start.elapsed();

    if let Some(diagnostics) = &mut diagnostics {
        diagnostics.record_step(
            "component-inventory",
            duration,
            step_status_from_result(&result),
        );
    }

    if result.is_err()
        && let Some(diagnostics) = &mut diagnostics
    {
        diagnostics.print_summary();
    }

    result
}

fn format_all(
    mut diagnostics: Option<&mut diagnostics::DiagnosticsCollector>,
) -> anyhow::Result<()> {
    println!("\u{1F680} Formatting workspace with cargo fmt...");
    let start = Instant::now();
    let result = run("cargo", &["fmt", "--all"]);
    let duration = start.elapsed();

    if let Some(diagnostics) = &mut diagnostics {
        diagnostics.record_step("fmt", duration, step_status_from_result(&result));
    }

    if result.is_err()
        && let Some(diagnostics) = &mut diagnostics
    {
        diagnostics.print_summary();
    }

    result?;
    println!("\u{2705} Formatting completed successfully.\n");
    Ok(())
}

fn clippy_all(
    options: CheckOptions,
    diagnostics: Option<&mut diagnostics::DiagnosticsCollector>,
) -> anyhow::Result<()> {
    println!("\u{1F680} Linting workspace with cargo clippy...");
    let mut args = vec![
        "clippy".to_owned(),
        "--workspace".to_owned(),
        "--all-features".to_owned(),
        "--all-targets".to_owned(),
        "--".to_owned(),
        "-D".to_owned(),
        "warnings".to_owned(),
    ];
    if options.diagnostics {
        args.push("--timings".to_owned());
    }
    args.extend(["--", "-D", "warnings"].into_iter().map(ToOwned::to_owned));

    let start = Instant::now();
    let result = if options.diagnostics {
        let outcome = run_capture("cargo", &args)?;
        let status = outcome.status;
        if let Some(diagnostics) = diagnostics {
            diagnostics.record_compile_step("clippy", &outcome.lines);
            diagnostics.record_step("clippy", start.elapsed(), step_status_from_status(status));
            if !status.success() {
                diagnostics.print_summary();
            }
        }
        ensure_success("cargo", status)
    } else {
        let result = run(
            "cargo",
            &[
                "clippy",
                "--workspace",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
        );
        if let Some(diagnostics) = diagnostics {
            diagnostics.record_step("clippy", start.elapsed(), step_status_from_result(&result));
            if result.is_err() {
                diagnostics.print_summary();
            }
        }
        result
    };

    result?;
    println!("\u{2705} Clippy linting passed without warnings.\n");
    Ok(())
}

fn clippy_quick() -> anyhow::Result<()> {
    println!("\u{1F680} Linting workspace targets for fast developer checks...");
    run(
        "cargo",
        &[
            "clippy",
            "--workspace",
            "--lib",
            "--bins",
            "--tests",
            "--",
            "-D",
            "warnings",
        ],
    )?;
    println!("\u{2705} Fast clippy linting passed without warnings.\n");
    Ok(())
}

fn build_no_std() -> anyhow::Result<()> {
    // A `no_std` crate builds against `std` in every other configuration, so
    // nothing else in the suite notices a stray `use std::` until a downstream
    // embedded build fails. Each crate here declares `#![no_std]` when its
    // `std` feature is off.
    const NO_STD_PACKAGES: &[&str] = &["otel-arrow-dfe-expohisto"];

    println!("\u{1F680} Building no_std crates without default features...");
    for package in NO_STD_PACKAGES {
        run("cargo", &["build", "-p", package, "--no-default-features"])?;
    }
    println!("\u{2705} no_std builds succeeded.\n");
    Ok(())
}

fn clippy_benches() -> anyhow::Result<()> {
    println!("\u{1F680} Linting workspace bench targets with cargo clippy...");
    run(
        "cargo",
        &["clippy", "--workspace", "--benches", "--", "-D", "warnings"],
    )?;
    println!("\u{2705} Bench clippy linting passed without warnings.\n");
    Ok(())
}

fn test_all(
    options: CheckOptions,
    diagnostics: Option<&mut diagnostics::DiagnosticsCollector>,
) -> anyhow::Result<()> {
    if options.nextest {
        return test_with_nextest(options, diagnostics);
    }
    println!("\u{1F680} Running workspace tests with cargo test...");
    let mut args = vec!["test".to_owned(), "--workspace".to_owned()];
    if options.diagnostics {
        args.push("--timings".to_owned());
    }

    let start = Instant::now();
    let result = if options.diagnostics {
        let outcome = run_capture("cargo", &args)?;
        let status = outcome.status;
        if let Some(diagnostics) = diagnostics {
            diagnostics.record_compile_step("test", &outcome.lines);
            diagnostics.record_test_binaries(&outcome.lines);
            diagnostics.record_step("test", start.elapsed(), step_status_from_status(status));
            if !status.success() {
                diagnostics.print_summary();
            }
        }
        ensure_success("cargo", status)
    } else {
        let result = run("cargo", &["test", "--workspace"]);
        if let Some(diagnostics) = diagnostics {
            diagnostics.record_step("test", start.elapsed(), step_status_from_result(&result));
            if result.is_err() {
                diagnostics.print_summary();
            }
        }
        result
    };

    result?;
    println!("\u{2705} All tests passed successfully.\n");
    Ok(())
}

fn test_with_nextest(
    options: CheckOptions,
    mut diagnostics: Option<&mut diagnostics::DiagnosticsCollector>,
) -> anyhow::Result<()> {
    println!("Running workspace tests with nextest, followed by doctests...");
    if let Some(collector) = diagnostics.as_deref_mut() {
        collector.note(
            "nextest reports individual tests; Cargo's per-binary test timing summary is unavailable with --nextest".to_owned(),
        );
    }
    let result = run_nextest_test_steps(options.diagnostics, |step, args| {
        let start = Instant::now();
        if let Some(collector) = diagnostics.as_deref_mut() {
            let result = run_capture("cargo", args).and_then(|outcome| {
                collector.record_compile_step(step, &outcome.lines);
                ensure_success("cargo", outcome.status)
            });
            collector.record_step(step, start.elapsed(), step_status_from_result(&result));
            result
        } else {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            run("cargo", &args)
        }
    });
    if result.is_err()
        && let Some(collector) = diagnostics.as_ref()
    {
        collector.print_summary();
    }
    result?;
    println!("All workspace tests and doctests passed successfully.\n");
    Ok(())
}

fn run_nextest_test_steps(
    timings: bool,
    mut execute: impl FnMut(&'static str, &[String]) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut nextest = vec!["nextest", "run", "--locked", "--workspace"];
    let mut doctests = vec!["test", "--locked", "--workspace", "--doc"];
    if timings {
        nextest.push("--timings");
        doctests.push("--timings");
    }
    let nextest: Vec<String> = nextest.into_iter().map(str::to_owned).collect();
    let doctests: Vec<String> = doctests.into_iter().map(str::to_owned).collect();
    let tests_result = execute("test", &nextest);
    // Doctests remain part of the full check even if unit/integration tests fail.
    let docs_result = execute("doctest", &doctests);
    match (tests_result, docs_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error.context("nextest workspace tests failed")),
        (Ok(()), Err(error)) => Err(error.context("workspace doctests failed")),
        (Err(tests), Err(docs)) => {
            anyhow::bail!(
                "nextest workspace tests failed: {tests:#}; workspace doctests failed: {docs:#}"
            )
        }
    }
}

fn test_quick() -> anyhow::Result<()> {
    println!("\u{1F680} Compiling fast workspace test targets (no benches/examples/doctests)...");
    run(
        "cargo",
        &[
            "test",
            "--workspace",
            "--lib",
            "--bins",
            "--tests",
            "--no-run",
        ],
    )?;
    println!("\u{2705} Fast workspace test targets compiled successfully.\n");
    Ok(())
}

fn compile_benches() -> anyhow::Result<()> {
    println!("\u{1F680} Compiling workspace bench targets with cargo check...");
    run("cargo", &["check", "--workspace", "--benches"])?;
    println!("\u{2705} Bench targets compiled successfully.\n");
    Ok(())
}

fn run(command: &str, args: &[&str]) -> anyhow::Result<()> {
    let status = Command::new(command).args(args).status()?;

    ensure_success(command, status)
}

struct CommandOutcome {
    status: ExitStatus,
    lines: Vec<diagnostics::CapturedLine>,
}

fn run_capture(command: &str, args: &[String]) -> anyhow::Result<CommandOutcome> {
    let mut child = Command::new(command)
        .args(args)
        .env("CARGO_TERM_COLOR", "always")
        .env("CARGO_TERM_HYPERLINKS", "false")
        .env("CLICOLOR_FORCE", "1")
        .env_remove("NO_COLOR")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("failed to capture stdout from `{command}`"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("failed to capture stderr from `{command}`"))?;

    let (tx, rx) = mpsc::channel::<diagnostics::CapturedLine>();
    let stdout_tx = tx.clone();
    let stdout_handle = thread::spawn(move || stream_lines(stdout, false, stdout_tx));
    let stderr_handle = thread::spawn(move || stream_lines(stderr, true, tx));

    let mut lines = Vec::new();
    for line in rx {
        if line.is_stderr {
            eprintln!("{}", line.text);
        } else {
            println!("{}", line.text);
        }
        lines.push(line);
    }

    stdout_handle
        .join()
        .map_err(|_| anyhow::anyhow!("stdout reader thread panicked"))??;
    stderr_handle
        .join()
        .map_err(|_| anyhow::anyhow!("stderr reader thread panicked"))??;

    let status = child.wait()?;
    Ok(CommandOutcome { status, lines })
}

fn stream_lines<R: std::io::Read>(
    reader: R,
    is_stderr: bool,
    tx: mpsc::Sender<diagnostics::CapturedLine>,
) -> anyhow::Result<()> {
    for line in BufReader::new(reader).lines() {
        tx.send(diagnostics::CapturedLine {
            is_stderr,
            text: line?,
        })?;
    }
    Ok(())
}

fn ensure_success(command: &str, status: ExitStatus) -> anyhow::Result<()> {
    if status.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Command `{}` failed with status {}",
            command,
            status
        ))
    }
}

fn step_status_from_result(result: &anyhow::Result<()>) -> diagnostics::StepStatus {
    if result.is_ok() {
        diagnostics::StepStatus::Succeeded
    } else {
        diagnostics::StepStatus::Failed
    }
}

fn step_status_from_status(status: ExitStatus) -> diagnostics::StepStatus {
    if status.success() {
        diagnostics::StepStatus::Succeeded
    } else {
        diagnostics::StepStatus::Failed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: A developer requests nextest and timing diagnostics for a full check.
    /// Guarantees: Both options are accepted together without changing the default runner.
    #[test]
    fn parse_check_options_accepts_nextest_and_diagnostics() {
        let options = parse_check_options(&["--nextest".into(), "--diagnostics".into()])
            .expect("both check options should parse");
        assert!(options.nextest && options.diagnostics);
        assert!(!parse_check_options(&[]).unwrap().nextest);
    }

    /// Scenario: Nextest fails during a full check that also needs doctest coverage.
    /// Guarantees: Doctests still run and the overall check reports the nextest failure.
    #[test]
    fn nextest_failure_does_not_skip_doctests() {
        let mut ran_doctests = false;
        let result = run_nextest_test_steps(true, |step, args| {
            assert!(args.iter().any(|arg| arg == "--workspace"));
            assert!(args.iter().any(|arg| arg == "--timings"));
            if step == "test" {
                anyhow::bail!("unit test failure");
            }
            ran_doctests = args.iter().any(|arg| arg == "--doc");
            Ok(())
        });
        assert!(ran_doctests);
        assert!(format!("{:#}", result.unwrap_err()).contains("unit test failure"));
    }

    /// Scenario: Unit/integration tests pass but a documentation example fails.
    /// Guarantees: The fast full check cannot report success when doctests fail.
    #[test]
    fn nextest_check_propagates_doctest_failure() {
        let result = run_nextest_test_steps(false, |step, _| {
            if step == "doctest" {
                anyhow::bail!("documentation example failure");
            }
            Ok(())
        });
        assert!(format!("{:#}", result.unwrap_err()).contains("documentation example failure"));
    }

    /// Scenario: Both the nextest suite and documentation examples fail.
    /// Guarantees: The full-check error retains both failures for diagnosis.
    #[test]
    fn nextest_check_reports_both_failures() {
        let result = run_nextest_test_steps(false, |step, _| anyhow::bail!("{step} failed"));
        let message = format!("{:#}", result.unwrap_err());
        assert!(message.contains("nextest workspace tests failed: test failed"));
        assert!(message.contains("workspace doctests failed: doctest failed"));
    }

    #[test]
    fn parse_check_options_accepts_diagnostics_flag() {
        let args = vec!["--diagnostics".to_owned()];
        let options = parse_check_options(&args).expect("flag should parse");
        assert!(options.diagnostics);
    }

    #[test]
    fn parse_check_options_rejects_unknown_flag() {
        let args = vec!["--unknown".to_owned()];
        assert!(parse_check_options(&args).is_err());
    }

    #[test]
    fn non_check_tasks_reject_extra_args() {
        let args = vec!["--diagnostics".to_owned()];
        assert!(ensure_no_extra_args("quick-check", &args).is_err());
    }
}
