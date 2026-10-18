#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Verify production checkpoint transitions with pinned tooling and isolated negative controls."""

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
VALUES = Path("rust/otap-dataflow/crates/filelog-checkpoint/src/replay_values.rs")
KERNEL = Path("rust/otap-dataflow/crates/filelog-checkpoint/src/replay/transitions.rs")
OWNERSHIP = Path("rust/otap-dataflow/crates/filelog-checkpoint/src/replay/ownership.rs")
MODEL = RELATIVE / "src/transaction.rs"
ADAPTER = Path("rust/otap-dataflow/crates/filelog-checkpoint/src/replay.rs")
RUNTIME_TESTS = ADAPTER.parent / "replay/transaction_tests.rs"
SOURCES = (VALUES, KERNEL, OWNERSHIP)

# Share the image pin, logging, timeout, and proof-rejection checks with the rate target.
_spec = importlib.util.spec_from_file_location("rate_verifier", INFRA / "verify.py")
_infra = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_infra)
IMAGE = _infra.IMAGE
run = _infra.run
require_rejected = _infra.require_rejected

# Mutate shared executable decisions and the protocol model, keeping contracts intact.
MUTATIONS = (
    ("stale-epoch-accepted", KERNEL,
     "require(record, LifecycleState::Active, op.expected_file_epoch)?;",
     "require(record, LifecycleState::Active, record.file_epoch)?;", "check_progress"),
    ("offset-regression-accepted", KERNEL,
     "\n        || op.new_committed_offset < record.committed_offset", "", "check_progress"),
    ("known-end-continuation-dropped", KERNEL,
     "                if !same {", "                if !same && op.new_framing_resume != FramingResume::Clean {",
     "check_progress"),
    ("zero-delta-guard-replaced", KERNEL,
     "if op.new_committed_frontier_guard != record.committed_frontier_guard\n            || op.new_framing_resume != record.framing_resume",
     "if op.new_framing_resume != record.framing_resume", "check_progress"),
    ("reset-keeps-old-epoch", KERNEL,
     "record.file_epoch = op.resulting_epoch;\n    record.committed_offset = 0;",
     "record.file_epoch = op.expected_active_epoch;\n    record.committed_offset = 0;",
     "reset_after_truncate"),
    ("keep-failed-updates-timestamp", KERNEL,
     "            return Err(TransitionError::KeepFailed);\n        }\n        return Ok(());",
     "            return Err(TransitionError::KeepFailed);\n        }\n        record.last_seen_time_unix_nano = op.action_time_unix_nano;\n        return Ok(());",
     "reset_quarantined"),
    ("finalization-leaves-active", KERNEL,
     "record.lifecycle_state = LifecycleState::RotatedFinalized;",
     "record.lifecycle_state = LifecycleState::Active;", "apply_progress_fields"),
    ("tombstone-reuses-base-owner", OWNERSHIP,
     "        Some(owner) => owner,\n        None => base,",
     "        Some(None) => base,\n        Some(owner) => owner,\n        None => base,", "effective_owner"),
    ("claim-steals-live-locator", OWNERSHIP,
     "    !occupied || same_owner\n}", "    occupied || same_owner\n}", "claim_available"),
    ("replacement-reuses-existing-id", OWNERSHIP,
     "        Some(id) => id != removed && registered && !existed,\n    }\n}",
     "        Some(id) => id != removed && registered,\n    }\n}", "replacement_allowed"),
    ("quarantine-releases-locator", OWNERSHIP,
     "record.lifecycle_state != LifecycleState::RotatedFinalized).then_some(record.locator)",
     "record.lifecycle_state == LifecycleState::Active).then_some(record.locator)", "live_locator"),
    ("model-leaks-released-claim", MODEL,
     "let _ = t.owners.remove_ghost(&loc);", "let _ = loc;", "stage_edit"),
    ("model-publishes-rejected-prefix", MODEL,
     "if !stage_all(&mut staged, edits) {\n        return false;",
     "if !stage_all(&mut staged, edits) {\n        state.tables = staged;\n        return false;", "replay_transaction"),
    ("model-advances-sequence-before-validation", MODEL,
     "let mut staged = Tables", "state.sequence = sequence;\n    let mut staged = Tables", "replay_transaction"),
    ("model-ignores-final-admission", MODEL,
     "if !tail_valid || staged.records.len_ghost() > limit {", "if !tail_valid {", "replay_transaction"),
)
CONTRACT = "ensures((result == Ok(())) == progress_allowed(*record, *op))"
CONTRACT_CONTROLS = (
    ("progress-acceptance", KERNEL, CONTRACT, "check_progress", "accepted_progress_applies_atomically"),
    ("transaction-rollback", MODEL, "ensures(!result ==> ^state == *state)",
     "replay_transaction", "rejected_tail_rolls_back"),
)



def require_proved(code, output, translated, expected):
    """Require the complete reviewed translation inventory and its main obligations."""
    if not expected or code != 0 or not re.search(rf"Proved \({len(expected)} files\)", output):
        raise RuntimeError("Expected the complete checkpoint proof inventory to succeed")
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
    parser.add_argument("--skip-build", action="store_true", help="reuse the pinned local verifier image")
    parser.add_argument("--image", default=IMAGE, help="local image to use with --skip-build")
    args = parser.parse_args()
    if args.image != IMAGE and not args.skip_build:
        parser.error("--image requires --skip-build")
    if not args.skip_build:
        subprocess.run([str(INFRA / "build.sh")], check=True)

    expected = json.loads((PROOF / "expected-proofs.json").read_text())
    reports = PROOF / "target/reports"
    if reports.exists():
        shutil.rmtree(reports)
    reports.mkdir(parents=True)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO, text=True))
    image_id = subprocess.check_output(
        ["docker", "image", "inspect", args.image, "--format", "{{.Id}}"], text=True).strip()
    inputs = [*(REPO / relative for relative in (*SOURCES, ADAPTER, RUNTIME_TESTS)), *sorted(PROOF.glob("src/*.rs")),
              *(PROOF / name for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml",
                                           "why3find.json", "why3find-negative.json", "verify.py", "expected-proofs.json")),
              *(INFRA / name for name in ("Dockerfile", ".dockerignore", "provers.sha256",
                                           "build.sh", "verify.py"))]
    report = {"git_commit": commit, "dirty": dirty, "image_id": image_id,
              "source_sha256": {str(path.relative_to(REPO)): hashlib.sha256(path.read_bytes()).hexdigest()
                                for path in inputs}, "status": "incomplete"}
    report_path = reports / "result.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")

    # The checkout is never mounted; mutations and generated output live in a disposable copy.
    with tempfile.TemporaryDirectory(prefix="verification-", dir=PROOF / "target") as temporary:
        root = Path(temporary)
        target = root / RELATIVE
        shutil.copytree(PROOF, target, ignore=shutil.ignore_patterns(
            "target", "verif", ".why3find", "_creusot_erasure", "__pycache__"))
        for relative in SOURCES:
            destination = root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(REPO / relative, destination)
        command = ["docker", "run", "--rm", "--platform", "linux/amd64",
                   "--user", f"{os.getuid()}:{os.getgid()}",
                   "--env", "CARGO_HOME=/work/.cargo-home",
                   "--env", "XDG_CONFIG_HOME=/work/.config",
                   "--env", "XDG_CACHE_HOME=/work/.cache",
                   "--mount", f"type=bind,source={root},target=/work",
                   "--workdir", f"/work/{RELATIVE}", args.image]
        code, _ = run([*command, "cargo", "creusot", "version"], reports / "versions.log")
        if code != 0:
            raise RuntimeError("Could not inspect verifier versions")
        code, _ = run([*command, "cargo", "+1.98.1", "test", "--locked"], reports / "tests.log")
        if code != 0:
            raise RuntimeError("Shared checkpoint transition tests failed")

        def prove(label, selection=()):
            shutil.rmtree(target / "verif", ignore_errors=True)
            (target / "src/lib.rs").touch()
            return run([*command, "cargo", "creusot", *selection, "--no-cache", "--why3find-arg=-f",
                        "--", "--locked"], reports / f"{label}.log")

        require_proved(*prove("baseline"), target / "verif", expected)
        shutil.copytree(target / "verif", reports / "verif")
        config_path = target / "why3find.json"
        original_config = config_path.read_text()
        negative_config = (target / "why3find-negative.json").read_text()
        config_path.write_text(negative_config)
        positive_controls = sorted({entry[4] for entry in MUTATIONS} |
                                   {entry[4] for entry in CONTRACT_CONTROLS})
        for function in positive_controls:
            require_selected_proved(*prove(f"positive-control-{function}", (function,)),
                                    target / "verif", expected, function)
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
            config_path.write_text(original_config)
            path = root / relative
            original = path.read_text()
            if original.count(contract) != 1:
                raise RuntimeError(f"Contract mutation anchor is not unique: {label}")
            path.write_text(original.replace(contract, "ensures(true)"))
            require_selected_proved(*prove(f"weakened-{label}-producer", (producer,)),
                                    target / "verif", expected, producer)
            config_path.write_text(negative_config)
            require_rejected(*prove(f"weakened-{label}-consumer", (consumer,)), consumer)
            path.write_text(original)
            print(f"PASS: weakened {label} contract rejected by its consumer", flush=True)
        config_path.write_text(original_config)
        require_proved(*prove("restored"), target / "verif", expected)
    report["status"] = "proved"
    report["proof_files"] = len(expected)
    report["negative_budget_positive_controls"] = positive_controls
    report["negative_controls"] = [entry[0] for entry in MUTATIONS] + [f"weakened-{entry[0]}" for entry in CONTRACT_CONTROLS]
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"All {len(expected)} proof files proved; all {len(MUTATIONS) + len(CONTRACT_CONTROLS)} negative controls rejected. "
          f"Reports: {reports}")


if __name__ == "__main__":
    main()
