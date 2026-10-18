#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Verify production ID selection with pinned tooling and isolated negative controls."""

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
BITMAP = Path("rust/otap-dataflow/crates/pdata/src/otap/filter/id_bitmap.rs")
WORDS = BITMAP.with_suffix("") / "words.rs"
MASK = Path("rust/otap-dataflow/crates/query-engine/src/pipeline/id_mask.rs")
SOURCES = (BITMAP, WORDS, MASK)

# Share the image pin, logging, timeout, and proof-rejection checks with the rate target.
_spec = importlib.util.spec_from_file_location("rate_verifier", INFRA / "verify.py")
_infra = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_infra)
IMAGE = _infra.IMAGE
run = _infra.run
require_rejected = _infra.require_rejected

# Mutate only executable production code, keeping the contracts intact.
MUTATIONS = (
    ("and-uses-union", MASK, "lhs.intersect_with(&rhs);\n                pool.release(rhs);\n                Self::Some(lhs)",
     "lhs.union_with(&rhs);\n                pool.release(rhs);\n                Self::Some(lhs)", "combine_and"),
    ("complement-or-uses-union", MASK, "lhs.intersect_with(&rhs);\n                pool.release(rhs);\n                Self::NotSome(lhs)",
     "lhs.union_with(&rhs);\n                pool.release(rhs);\n                Self::NotSome(lhs)", "combine_or"),
    ("difference-keeps-overlap", WORDS, "Operation::Difference => lhs & !rhs,",
     "Operation::Difference => lhs & rhs,", "combine_word"),
    ("clear-retains-bits", WORDS, "words[i] = zero_word();",
     "words[i] = words[i];", "clear_words"),
    ("pool-skips-clear", BITMAP, "                bm.clear();",
     "                let _ = &mut bm;", "acquire"),
    ("wrong-page-offset", BITMAP, "((id / 65536) as usize, (id % 65536) as usize)",
     "((id / 65535) as usize, (id % 65536) as usize)", "page_and_bit"),
)


def require_proved(code, output, translated, expected):
    """Require the complete reviewed translation inventory and its main obligations."""
    if not expected or code != 0 or not re.search(rf"Proved \({len(expected)} files\)", output):
        raise RuntimeError("Expected the complete ID selection proof inventory to succeed")
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
    inputs = [*(REPO / relative for relative in SOURCES), *sorted(PROOF.glob("src/*.rs")),
              *(PROOF / name for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml",
                                           "why3find.json", "verify.py", "expected-proofs.json")),
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
            raise RuntimeError("Shared ID selection membership tests failed")

        def prove(label):
            shutil.rmtree(target / "verif", ignore_errors=True)
            (target / "src/lib.rs").touch()
            return run([*command, "cargo", "creusot", "--no-cache", "--why3find-arg=-f",
                        "--", "--locked"], reports / f"{label}.log")

        require_proved(*prove("baseline"), target / "verif", expected)
        shutil.copytree(target / "verif", reports / "verif")
        for label, relative, old, new, function in MUTATIONS:
            path = root / relative
            original = path.read_text()
            if original.count(old) != 1:
                raise RuntimeError(f"Mutation anchor is not unique: {label}")
            path.write_text(original.replace(old, new))
            require_rejected(*prove(label), function)
            path.write_text(original)
            print(f"PASS: rejected {label}", flush=True)
        require_proved(*prove("restored"), target / "verif", expected)
    report["status"] = "proved"
    report["proof_files"] = len(expected)
    report["negative_controls"] = [entry[0] for entry in MUTATIONS]
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"All {len(expected)} proof files proved; all {len(MUTATIONS)} negative controls rejected. "
          f"Reports: {reports}")


if __name__ == "__main__":
    main()
