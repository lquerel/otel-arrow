#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Verify shared lifecycle guards and reproduce the publication bug and shutdown policy gap."""
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
CONTROLLER = Path("rust/otap-dataflow/crates/controller/src")
KERNEL = CONTROLLER / "live_control/lifecycle_kernels.rs"
SOURCES = (KERNEL,)
ADAPTERS = tuple(CONTROLLER / name for name in (
    "live_control/mod.rs", "live_control/state.rs", "live_control/planning.rs",
    "live_control/runtime.rs", "live_control_tests.rs", "live_control/lifecycle_conformance.rs",
))
_spec = importlib.util.spec_from_file_location("rate_verifier", INFRA / "verify.py")
_infra = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_infra)
IMAGE = _infra.IMAGE
run = _infra.run
require_rejected = _infra.require_rejected

# Mutations are restricted to one executable body, never its contract.
MUTATIONS = (
    ("stale-worker-accepted", "may_prepare", "owns_token(self.worker, worker)", "true"),
    ("preemption-ignored", "may_prepare", "!preempted", "true"),
    ("wrong-launch-candidate", "commit_launch", "self.candidate != Some(target)", "false"),
    ("wrong-restart-ordinal", "commit_launch", "self.restarts.checked_add(1) != Some(attempt)", "false"),
    ("dead-candidate-promoted", "promote", "|| !target_active", ""),
    ("promotion-keeps-worker", "promote", "self.worker = None;", "let _ = self.worker;"),
    ("stale-worker-cleans-new-owner", "release_worker", "!owns_token(self.worker, worker)", "false"),
    ("shutdown-reservation-defers-failure", "shutdown_owns_failures", "self.reservation == Some(OperationKind::Shutdown)", "false"),
    ("active-record-omitted", "lifecycle_work_pending", "|| active_record", ""),
    ("foreign-engine-operation-accepted", "engine_operation_authorized", "&& equal", ""),
)
CONTRACT_CONTROLS = (
    ("promotion-frame", KERNEL,
     "ensures(if result { (^self).serving == target && (^self).worker == None && (^self).candidate == None && (^self).restarts == self.restarts && (^self).cancelled == self.cancelled } else { ^self == *self })",
     "promote", "stale_worker_promotion_is_noop"),
)


def mutate_body(source, function, old, new):
    """Reject drift and duplicate anchors before changing an executable function body."""
    marker = "fn " + function + "("
    if source.count(marker) != 1:
        raise RuntimeError(f"Function anchor is not unique: {function}")
    begin = source.index("{", source.index(marker))
    depth, end = 1, begin + 1
    while depth and end < len(source):
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    body = source[begin:end]
    if depth or body.count(old) != 1:
        raise RuntimeError(f"Body mutation anchor is not unique: {function}: {old}")
    return source[:begin] + body.replace(old, new) + source[end:]


def require_proved(code, output, translated, expected):
    """Require the complete reviewed translation inventory and its main obligations."""
    if not expected or code != 0 or not re.search(rf"Proved \({len(expected)} files\)", output):
        raise RuntimeError("Expected the complete controller proof inventory to succeed")
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
    reports = PROOF / "target/reports"
    shutil.rmtree(reports, ignore_errors=True)
    reports.mkdir(parents=True)
    inputs = [*(REPO / p for p in (*SOURCES, *ADAPTERS)), *sorted(PROOF.glob("src/*.rs")),
              *sorted(PROOF.glob("*.py")), *sorted(PROOF.glob("*.json")),
              *(PROOF / name for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml")),
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
                raise RuntimeError(f"Controller verifier {label} failed")

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
        positives = sorted({m[1] for m in MUTATIONS} | {c[4] for c in CONTRACT_CONTROLS} |
                           {"uninterrupted_publication_is_consistent", "permits_pipeline_reservation", "permits_engine_operation"})
        for function in positives:
            require_selected_proved(*prove(f"positive-{function}", (function,)), target / "verif", expected, function)
        path = root / KERNEL
        original = path.read_text()
        for label, function, old, new in MUTATIONS:
            path.write_text(mutate_body(original, function, old, new))
            require_rejected(*prove(label, (function,)), function)
            path.write_text(original)
            print(f"PASS: rejected {label}", flush=True)
        for label, relative, contract, producer, consumer in CONTRACT_CONTROLS:
            if original.count(contract) != 1 or relative != KERNEL:
                raise RuntimeError(f"Contract anchor is not unique: {label}")
            path.write_text(original.replace(contract, "ensures(true)"))
            require_selected_proved(*prove(f"weak-{label}-producer", (producer,)), target / "verif", expected, producer)
            require_rejected(*prove(f"weak-{label}-consumer", (consumer,)), consumer)
            path.write_text(original)
        # Unchanged-code requirement failures are distinct from deliberate defects.
        for label, function in (("CTRL-001", "late_publication_preserves_new_owner"),
                                ("CTRL-002", "shutdown_closes_mutation_authority")):
            require_rejected(*prove(label, (function,), True), function)
        config.write_text(original_config)
        require_proved(*prove("restored"), target / "verif", expected)
    report.update(status="verified_with_known_findings", positive_proof_files=len(expected),
                  positive_obligations=sum(len(goals) for goals in expected.values()),
                  negative_controls=[m[0] for m in MUTATIONS] + [f"weak-{c[0]}" for c in CONTRACT_CONTROLS],
                  same_budget_positive_controls=positives,
                  confirmed_bugs=["CTRL-001: stale recovery publication after newer rollout commit"],
                  open_policy_requirements=["CTRL-002: reject reconciliation mutations after global shutdown"],
                  boundary="Collections, mutex scheduling, worker termination, and overall shutdown/reconciliation safety are not proved")
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Verified {len(expected)} positive proof files; {len(MUTATIONS) + len(CONTRACT_CONTROLS)} negative controls rejected. CTRL-001 BUG and CTRL-002 POLICY GAP reproduced. Reports: {reports}")


if __name__ == "__main__":
    main()
