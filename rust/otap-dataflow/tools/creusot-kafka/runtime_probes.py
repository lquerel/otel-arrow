#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Explicitly reproduce the two documented, unresolved Kafka boundary findings."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

PROOF = Path(__file__).resolve().parent
REPO = PROOF.parents[3]
WORKSPACE = REPO / "rust/otap-dataflow"
PREFIX = "receivers::kafka_receiver::"
PROBES = (
    ("KAFKA-001", PREFIX + "offset_tracker::tests::exhausted_offset_requires_safe_commit_boundary",
     "attempt to add with overflow"),
    ("KAFKA-002", PREFIX + "retry::tests::exhausted_generation_requires_stale_feedback_rejection",
     "obsolete delivery token remains accepted after exhaustion"),
)


def require_finding(code, output, function, diagnostic):
    if (code == 0 or function + " ... FAILED" not in output or diagnostic not in output
            or "test result: FAILED" not in output or "could not compile" in output):
        raise RuntimeError(f"Expected the documented runtime finding in {function}; update its disposition if fixed")


def main():
    reports = PROOF / "target/runtime-probes"
    reports.mkdir(parents=True, exist_ok=True)
    inputs = sorted((WORKSPACE / "crates/contrib-nodes/src/receivers/kafka_receiver").rglob("*.rs"))
    result = {"status": "incomplete", "git_commit": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(), "source_sha256": {
            str(path.relative_to(REPO)): hashlib.sha256(path.read_bytes()).hexdigest() for path in inputs}}
    result_path = reports / "result.json"
    result_path.write_text(json.dumps(result, indent=2) + "\n")
    for finding, function, diagnostic in PROBES:
        completed = subprocess.run([
            "cargo", "+1.98.1", "test", "--locked", "-p", "otel-arrow-dfe-contrib-nodes",
            "--features", "kafka", "--lib", function, "--", "--exact", "--ignored", "--test-threads=1",
        ], cwd=WORKSPACE, env={**os.environ, "RUST_BACKTRACE": "0"}, capture_output=True, text=True, timeout=1800)
        output = completed.stdout + completed.stderr
        (reports / f"{finding}.log").write_text(output)
        require_finding(completed.returncode, output, function, diagnostic)
        print(f"CONFIRMED: {finding} at {function}", flush=True)
    result["status"] = "confirmed_known_findings"
    result["findings"] = [entry[0] for entry in PROBES]
    result_path.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
