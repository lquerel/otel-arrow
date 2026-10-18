# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS compiler
RUN apt-get update && apt-get install -y --no-install-recommends \
    opam libzmq3-dev libgmp-dev zlib1g-dev unzip pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV CREUSOT_DATA_HOME=/opt/creusot OPAMROOT=/opt/opam OPAMJOBS=2 CARGO_BUILD_JOBS=2
RUN rustup toolchain install nightly-2026-08-03 --profile minimal \
    --component rustfmt,rustc-dev,llvm-tools
RUN git clone --depth 1 --branch v0.14.0 https://github.com/creusot-rs/creusot.git /opt/creusot-src \
    && test "$(git -C /opt/creusot-src rev-parse HEAD)" = 65349025c67e69fcd7479f1f420fa5904f69bc27
# Creusot 0.14.0's installer otherwise ignores Cargo.lock in child commands.
RUN test "$(grep -c '"install", "--path"' /opt/creusot-src/creusot-install/src/main.rs)" -eq 2 \
    && sed -i 's/"install", "--path"/"install", "--locked", "--path"/g; s/"run", "--bin"/"run", "--locked", "--bin"/g' /opt/creusot-src/creusot-install/src/main.rs
WORKDIR /opt/creusot-src
RUN cargo run --locked --bin creusot-install -- prelude cargo-creusot creusot-rustc provers why3-conf --provers-parallelism 2 \
    && rm -rf target
FROM compiler AS ocaml
# Freeze the OCaml package index as well as Creusot's Why3/Why3find pins.
COPY --from=opam-source opam.tar.gz /tmp/opam.tar.gz
RUN echo '78ffd1af31f56a2c0f541588adb97ffa110feaf3d377cf4d6f3525b50a33dfe7  /tmp/opam.tar.gz' | sha256sum --check \
    && mkdir /opt/opam-repository \
    && tar -xzf /tmp/opam.tar.gz -C /opt/opam-repository --strip-components=1 \
    && opam init --bare --disable-sandboxing --no-setup -y default /opt/opam-repository \
    && opam --cli=2.1 var --global in-creusot-ci=true \
    && opam switch create -y /opt/creusot ocaml-base-compiler.5.3.0
FROM ocaml AS verifier
RUN apt-get update && apt-get install -y --no-install-recommends rsync \
    && rm -rf /var/lib/apt/lists/*
COPY --from=why3-source why3.tar.gz /tmp/why3.tar.gz
RUN echo 'fc84ae17de6a79ea253cb7df96afd8e4eae562a99b43379a4cf5d12efa701b92  /tmp/why3.tar.gz' | sha256sum --check \
    && mkdir /opt/why3-src \
    && tar -xzf /tmp/why3.tar.gz -C /opt/why3-src --strip-components=1 \
    && curl -fL https://gitlab.inria.fr/why3/why3/-/raw/05bf4ddf55cb3042f34c3bc0ed3aff56a226978a/opam/why3.opam -o /opt/why3-src/why3.opam \
    && curl -fL https://gitlab.inria.fr/why3/why3/-/raw/05bf4ddf55cb3042f34c3bc0ed3aff56a226978a/opam/why3-ide.opam -o /opt/why3-src/why3-ide.opam \
    && echo '73500ee29f524fd786dfd4ea2aaa9e581a45b7ce17a1357276fd2583c176a707  /opt/why3-src/why3.opam' | sha256sum --check \
    && echo 'a789e896bf10e9ae2c11f1938ac2f8d48195e8edac621dd9511be55e9aaace8f  /opt/why3-src/why3-ide.opam' | sha256sum --check \
    && sed -i 's|git+https://gitlab.inria.fr/why3/why3.git#05bf4ddf55cb3042f34c3bc0ed3aff56a226978a|file:///opt/why3-src|g' creusot-deps.opam \
    && cargo run --locked --bin creusot-install -- why3 \
    && rm -rf target /opt/opam/download-cache
WORKDIR /work
CMD ["cargo", "creusot"]

# The bundled Z3 5.1.0 binary needs glibc >= 2.38 and a newer libstdc++.
# Keep the expensive compiler/OCaml build layers cached and run them on Trixie.
FROM rust:1.98.1-trixie@sha256:a8a5f0a1e5fe7dfe1d352591e4a1c7dd2c08fd70475cae872cf3458ba0df0546 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgmp10 libzmq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=verifier /usr/local/cargo /usr/local/cargo
COPY --from=verifier /usr/local/rustup /usr/local/rustup
COPY --from=verifier /opt/creusot /opt/creusot
ENV CREUSOT_DATA_HOME=/opt/creusot CARGO_BUILD_JOBS=2
COPY provers.sha256 /opt/creusot/provers.sha256
RUN sha256sum --check /opt/creusot/provers.sha256 \
    && /opt/creusot/bin/z3 --version
WORKDIR /work
CMD ["cargo", "creusot"]
