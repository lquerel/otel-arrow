#!/usr/bin/env python3
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Check shared production arithmetic with fresh proofs and isolated mutations."""

import argparse
import hashlib
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
MATH = Path("rust/otap-dataflow/crates/engine/src/admission/bucket/math.rs")
FUNCTIONS = ("nanos_for_rate", "next_theoretical_arrival")
IMAGE = "otel-arrow-creusot-rate-bucket:0.14.0"


def require_proved(code, output, translated):
    """Require success for both expected production functions, never an empty selection."""
    if code != 0 or not re.search(r"Proved \(2 files\)", output):
        raise RuntimeError("Expected both arithmetic proof files to succeed")
    for function in FUNCTIONS:
        path = translated / "otel_arrow_dfe_rate_bucket_proof_rlib/math" / function
        if not path.with_suffix(".coma").is_file():
            raise RuntimeError(f"Missing translation for {function}")
        certificate = json.loads((path / "proof.json").read_text())
        if f"vc_{function}" not in certificate.get("proofs", {}).get("Coma", {}):
            raise RuntimeError(f"Missing proof obligation for {function}")


def require_rejected(code, output, function):
    """Count only a proof rejection at the intended goal as a successful control."""
    if (code == 0 or "proof failed" not in output or f"vc_{function}" not in output
            or "Compilation failed" in output or "can not calibrate" in output):
        raise RuntimeError(f"Expected a proof rejection at vc_{function}")


def run(command, log, **kwargs):
    result = subprocess.run(command, capture_output=True, text=True, timeout=300, **kwargs)
    output = result.stdout + result.stderr
    log.write_text(output)
    print(output, end="", flush=True)
    return result.returncode, output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true", help="use an already built verifier image")
    parser.add_argument("--image", default=IMAGE, help="local image to use with --skip-build")
    args = parser.parse_args()
    if args.image != IMAGE and not args.skip_build:
        parser.error("--image requires --skip-build")
    if not args.skip_build:
        subprocess.run([str(PROOF / "build.sh")], check=True)

    reports = PROOF / "target/reports"
    if reports.exists():
        shutil.rmtree(reports)
    reports.mkdir(parents=True)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO, text=True))
    image_id = subprocess.check_output(
        ["docker", "image", "inspect", args.image, "--format", "{{.Id}}"], text=True).strip()
    inputs = [REPO / MATH, *sorted(PROOF.glob("src/*.rs")),
              *(PROOF / name for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml",
                                           "why3find.json", "Dockerfile", ".dockerignore", "provers.sha256",
                                           "build.sh", "verify.py"))]
    report = {"git_commit": commit, "dirty": dirty, "image_id": image_id,
              "source_sha256": {str(path.relative_to(REPO)): hashlib.sha256(path.read_bytes()).hexdigest()
                                for path in inputs}, "status": "incomplete"}
    report_path = reports / "result.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")

    # Mount only disposable proof inputs. Mutations never change the checkout.
    with tempfile.TemporaryDirectory(prefix="verification-", dir=PROOF / "target") as temporary:
        root = Path(temporary)
        target = root / RELATIVE
        shutil.copytree(PROOF, target, ignore=shutil.ignore_patterns(
            "target", "verif", ".why3find", "_creusot_erasure", "__pycache__"))
        (root / MATH).parent.mkdir(parents=True)
        shutil.copy2(REPO / MATH, root / MATH)
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
            raise RuntimeError("Shared arithmetic boundary tests failed")

        def prove(label):
            shutil.rmtree(target / "verif", ignore_errors=True)
            (target / "src/lib.rs").touch()
            return run([*command, "cargo", "creusot", "--no-cache", "--why3find-arg=-f",
                        "--", "--locked"], reports / f"{label}.log")

        require_proved(*prove("baseline"), target / "verif")
        shutil.copytree(target / "verif", reports / "verif")
        original = (root / MATH).read_text()
        for label, old, new, function in [
            ("floor-instead-of-ceiling", ".div_ceil(u128::from(allow))", " / u128::from(allow)",
             "nanos_for_rate"),
            ("regressing-arrival", ".min(debt_limit)\n        .max(current)", ".min(debt_limit)",
             "next_theoretical_arrival"),
        ]:
            if original.count(old) != 1:
                raise RuntimeError(f"Mutation anchor is not unique: {label}")
            (root / MATH).write_text(original.replace(old, new))
            require_rejected(*prove(label), function)
            print(f"PASS: rejected {label}", flush=True)
        (root / MATH).write_text(original)
        require_proved(*prove("restored"), target / "verif")
    report["status"] = "proved"
    report["functions"] = list(FUNCTIONS)
    report["negative_controls"] = 2
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Both functions proved; both negative controls rejected. Reports: {reports}")


if __name__ == "__main__":
    main()
