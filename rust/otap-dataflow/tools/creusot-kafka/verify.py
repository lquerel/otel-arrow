#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Prove Kafka scalar kernels and the pending-set model; reproduce unresolved boundary findings."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

PROOF = Path(__file__).resolve().parent
REPO = PROOF.parents[3]
RELATIVE = PROOF.relative_to(REPO)
INFRA = PROOF.parent / "creusot-rate-bucket"
KAFKA = Path("rust/otap-dataflow/crates/contrib-nodes/src/receivers/kafka_receiver")
KERNEL = KAFKA / "state_kernels.rs"
SUCCESSOR = KAFKA / "offset_successor.rs"
MODEL = RELATIVE / "src/model.rs"
SOURCES = (KERNEL, SUCCESSOR)
ADAPTERS = tuple(KAFKA / name for name in (
    "mod.rs", "identity.rs", "offset_tracker.rs", "retry.rs", "receiver/offset_feedback.rs",
    "offset_tracker/tests.rs", "offset_tracker/conformance.rs",
))
_spec = importlib.util.spec_from_file_location("rate_verifier", INFRA / "verify.py")
_infra = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_infra)
IMAGE = _infra.IMAGE
run = _infra.run
require_rejected = _infra.require_rejected

MUTATIONS = (
    ("equal-generation-is-stale", KERNEL, "generation < self.generation", "generation <= self.generation", "prepare_track"),
    ("new-generation-retains-old-ack", KERNEL, "self.high_water_mark = None;", "let _ = self.high_water_mark;", "prepare_track"),
    ("tracking-raises-minimum", KERNEL, "offset < previous", "offset > previous", "tracked"),
    ("ack-lowers-high-water", KERNEL, "offset > high", "offset < high", "acknowledged"),
    ("rewind-keeps-later-ack", KERNEL, "high.min(cap)", "high.max(cap)", "rewound"),
    ("stale-revocation-removes-new-state", KERNEL, "tracked <= revoked", "tracked >= revoked", "revoke_allowed"),
    ("feedback-ignores-new-assignment", KERNEL, "tracked.unwrap_or(0).max(assigned)", "tracked.unwrap_or(0).min(assigned)", "classify_offset_feedback"),
    ("stale-token-starts-replay", KERNEL, "current != feedback || !matches!", "false || !matches!", "mark_replaying"),
    ("wrong-offset-completes-replay", KERNEL, "current == feedback && rewind == offset && matches!(self, Self::Replaying)", "current == feedback && matches!(self, Self::Replaying)", "completes"),
    ("wrong-deadline-becomes-due", KERNEL, "if deadline != observed", "if deadline != observed && current != feedback", "mark_due"),
    ("model-counts-duplicate-insert", MODEL, "state.count + if state.pending.contains(offset) { 0 } else { 1 }", "state.count + 1", "model_track"),
    ("model-ack-leaks-count", MODEL, "count: state.count - 1", "count: state.count", "model_acknowledge"),
)
CONTRACT_CONTROLS = (
    ("stale-phase-preservation", KERNEL,
     "ensures(if result { ^self == Self::Replaying } else { ^self == *self })",
     "mark_replaying", "stale_replay_cannot_start"),
)
REPAIR = ("lowest.or_else(|| high.map(|offset| offset + 1))",
          "lowest.or_else(|| high.and_then(|offset| offset.checked_add(1)))")

def require_proved(code, output, translated, expected):
    """Require the complete reviewed translation inventory and its main obligations."""
    if not expected or code != 0 or not re.search(rf"Proved \({len(expected)} files\)", output):
        raise RuntimeError("Expected the complete Kafka proof inventory to succeed")
    actual = {str(path.relative_to(translated).with_suffix(""))
              for path in translated.rglob("*.coma")}
    if actual != set(expected):
        raise RuntimeError(f"Translation inventory changed: missing={set(expected) - actual}, "
                           f"unexpected={actual - set(expected)}")
    for relative, goals in expected.items():
        path = translated / relative / "proof.json"
        certificate = json.loads(path.read_text())
        actual_goals = set(certificate.get("proofs", {}).get("Coma", {}))
        if not goals or actual_goals != set(goals):
            raise RuntimeError(f"Proof obligations changed in {relative}")


def require_selected_proved(code, output, translated, expected, function):
    """Require the unmodified control target to prove with its exact negative-search budget."""
    paths = [path for path in expected if path.endswith("/" + function)]
    if code != 0 or len(paths) != 1:
        raise RuntimeError(f"Expected a positive proof for {function}")
    if f"Proved (verif/{paths[0]}.coma)" not in output:
        raise RuntimeError(f"Expected the selected proof summary for {function}")
    path = translated / paths[0]
    if not path.with_suffix(".coma").is_file():
        raise RuntimeError(f"Missing positive-control translation for {function}")
    certificate = json.loads((path / "proof.json").read_text())
    if set(certificate.get("proofs", {}).get("Coma", {})) != set(expected[paths[0]]):
        raise RuntimeError(f"Positive-control obligations changed for {function}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--image", default=IMAGE)
    args = parser.parse_args()
    if args.image != IMAGE and not args.skip_build:
        parser.error("--image requires --skip-build")
    if not args.skip_build:
        subprocess.run([str(INFRA / "build.sh")], check=True)
    expected = json.loads((PROOF / "expected-proofs.json").read_text())
    repair_expected = json.loads((PROOF / "expected-repair-proofs.json").read_text())
    reports = PROOF / "target/reports"
    shutil.rmtree(reports, ignore_errors=True)
    reports.mkdir(parents=True)
    inputs = [*(REPO / p for p in (*SOURCES, *ADAPTERS)), *sorted(PROOF.glob("src/*.rs")),
              *(PROOF / name for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "why3find.json",
                  "why3find-negative.json", "verify.py", "runtime_probes.py", "expected-proofs.json", "expected-repair-proofs.json")),
              *(INFRA / name for name in ("Dockerfile", ".dockerignore", "provers.sha256", "build.sh", "verify.py"))]
    report = {"git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
              "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO, text=True)),
              "image_id": subprocess.check_output(["docker", "image", "inspect", args.image, "--format", "{{.Id}}"], text=True).strip(),
              "source_sha256": {str(p.relative_to(REPO)): hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs},
              "status": "incomplete"}
    report_path = reports / "result.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    with tempfile.TemporaryDirectory(prefix="verification-", dir=PROOF / "target") as temporary:
        root = Path(temporary)
        target = root / RELATIVE
        shutil.copytree(PROOF, target, ignore=shutil.ignore_patterns("target", "verif", ".why3find", "_creusot_erasure", "__pycache__"))
        for relative in SOURCES:
            destination = root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(REPO / relative, destination)
        command = ["docker", "run", "--rm", "--platform", "linux/amd64", "--user", f"{os.getuid()}:{os.getgid()}",
                   "--env", "CARGO_HOME=/work/.cargo-home", "--env", "XDG_CONFIG_HOME=/work/.config",
                   "--env", "XDG_CACHE_HOME=/work/.cache", "--mount", f"type=bind,source={root},target=/work",
                   "--workdir", f"/work/{RELATIVE}", args.image]
        for label, arguments in (("versions", ("cargo", "creusot", "version")), ("tests", ("cargo", "+1.98.1", "test", "--locked"))):
            code, _ = run([*command, *arguments], reports / f"{label}.log")
            if code != 0:
                raise RuntimeError(f"Kafka verifier {label} failed")

        def prove(label, selection=(), findings=False):
            shutil.rmtree(target / "verif", ignore_errors=True)
            (target / "src/lib.rs").touch()
            extra = ("--features", "known-findings") if findings else ()
            return run([*command, "cargo", "creusot", *selection, "--no-cache", "--why3find-arg=-f", "--", "--locked", *extra], reports / f"{label}.log")

        require_proved(*prove("baseline"), target / "verif", expected)
        shutil.copytree(target / "verif", reports / "verif")
        config = target / "why3find.json"
        original_config = config.read_text()
        config.write_text((target / "why3find-negative.json").read_text())
        positives = sorted({m[4] for m in MUTATIONS} | {c[4] for c in CONTRACT_CONTROLS} | {"fresh_before_exhaustion"})
        for function in positives:
            require_selected_proved(*prove(f"positive-{function}", (function,)), target / "verif", expected, function)
        for label, relative, old, new, function in MUTATIONS:
            path = root / relative
            original = path.read_text()
            if original.count(old) != 1:
                raise RuntimeError(f"Mutation anchor is not unique: {label}")
            path.write_text(original.replace(old, new))
            require_rejected(*prove(label, (function,)), function)
            path.write_text(original)
            print(f"PASS: rejected {label}", flush=True)
        for label, relative, contract, producer, consumer in CONTRACT_CONTROLS:
            path = root / relative
            original = path.read_text()
            if original.count(contract) != 1:
                raise RuntimeError(f"Contract anchor is not unique: {label}")
            path.write_text(original.replace(contract, "ensures(true)"))
            require_selected_proved(*prove(f"weak-{label}-producer", (producer,)), target / "verif", expected, producer)
            require_rejected(*prove(f"weak-{label}-consumer", (consumer,)), consumer)
            path.write_text(original)
        # Known findings are unchanged-code failures, separate from deliberate defects.
        successor = root / SUCCESSOR
        original = successor.read_text()
        if original.count(REPAIR[0]) != 1:
            raise RuntimeError("Successor repair-control anchor is not unique")
        successor.write_text(original.replace(*REPAIR))
        require_selected_proved(*prove("KAFKA-001-repair-control", ("committable_offset",), True),
                                target / "verif", repair_expected, "committable_offset")
        successor.write_text(original)
        require_rejected(*prove("KAFKA-001-original", ("committable_offset",), True), "committable_offset")
        require_rejected(*prove("KAFKA-002-original", ("exhausted_generation_is_fresh",), True), "exhausted_generation_is_fresh")
        config.write_text(original_config)
        require_proved(*prove("restored"), target / "verif", expected)
    report.update(status="verified_with_known_findings", positive_proof_files=len(expected),
                  negative_controls=[m[0] for m in MUTATIONS] + [f"weak-{c[0]}" for c in CONTRACT_CONTROLS],
                  same_budget_positive_controls=positives,
                  known_findings=["KAFKA-001: commit successor overflow", "KAFKA-002: saturated delivery token reuse"],
                  boundary="Concrete collections, FFI, broker behavior, and the unresolved findings are not proved safe")
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Verified {len(expected)} positive proof files; {len(MUTATIONS) + len(CONTRACT_CONTROLS)} negative controls rejected. TWO UNRESOLVED FINDINGS reproduced. Reports: {reports}")


if __name__ == "__main__":
    main()
