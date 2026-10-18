#!/usr/bin/env bash
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

proof_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
sources="$proof_dir/target/toolchain-sources"

fetch_archive() {
    local url="$1" expected="$2" archive="$3"
    mkdir -p -- "$(dirname -- "$archive")"
    if ! test -f "$archive" || ! printf '%s  %s\n' "$expected" "$archive" | sha256sum --check --status; then
        curl --fail --location --connect-timeout 15 --max-time 600 --retry 2 \
            "$url" --output "$archive.tmp"
        printf '%s  %s\n' "$expected" "$archive.tmp" | sha256sum --check
        mv -- "$archive.tmp" "$archive"
    fi
}

if [[ $# -gt 1 || (${1:-} != "" && ${1:-} != --prepare-only) ]]; then
    echo "Usage: $0 [--prepare-only]" >&2
    exit 2
fi

# Creusot 0.14.0's Why3 revision and a frozen OCaml package-index snapshot.
fetch_archive \
    https://gitlab.inria.fr/why3/why3/-/archive/05bf4ddf55cb3042f34c3bc0ed3aff56a226978a/why3-05bf4ddf55cb3042f34c3bc0ed3aff56a226978a.tar.gz \
    fc84ae17de6a79ea253cb7df96afd8e4eae562a99b43379a4cf5d12efa701b92 \
    "$sources/why3/why3.tar.gz"
fetch_archive \
    https://github.com/ocaml/opam-repository/archive/eba24de6376079a3f9dfa4a9c9ca842475cd1bbc.tar.gz \
    78ffd1af31f56a2c0f541588adb97ffa110feaf3d377cf4d6f3525b50a33dfe7 \
    "$sources/opam/opam.tar.gz"

if [[ ${1:-} == --prepare-only ]]; then
    exit 0
fi

docker buildx build --load --platform linux/amd64 \
    --build-context "why3-source=$sources/why3" \
    --build-context "opam-source=$sources/opam" \
    --tag otel-arrow-creusot-rate-bucket:0.14.0 "$proof_dir"
