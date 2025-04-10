# OTAP Dataflow Library

> Note: This Rust library will be the main deliverable of phase 2 of the otel-arrow project, as defined in this PR.
>
> The other Rust projects located in the same root directory as this project will gradually be integrated into it.

## Overview

TBD

## 🚧 Workspace Structure

```text
.
├── Cargo.toml
├── crates
│   ├── config         # OTAP Dataflow Configuration Model
│   ├── crate-two      # Purpose of crate-two
│   └── crate-n        # Purpose of crate-n
└── examples           # Examples or demo applications
```

## 🚀 Quickstart Guide

### 📥 Installation

TBD

### 🎯 Usage Example

TBD

## 📚 Documentation

- Developer/Contributing Guide (TBD)
- Architecture/Design Docs (TBD)

## 🛠️ Development Setup

**Requirements**:

- Rust >= 1.86.0
- Cargo

**Clone & Build**:

```bash
git clone https://github.com/open-telemetry/otel-arrow
cd otel-arrow/rust/otap-dataflow
cargo build --workspace
```

**Run Tests**:

```bash
cargo test --workspace
```

**Run Examples**:

```bash
cargo run --example <example_name>
```

## 🧩 Contributing

- Contribution Guidelines (TBD)
- Code of Conduct (TBD)

Before submitting a PR, please run the following commands:

```bash
# Check formatting and lints
cargo fmt -- --check
cargo clippy --workspace --all-targets -- -D warnings

# Run tests
cargo test --workspace
```

## 📝 License

Licensed under the [Apache License, Version 2.0](LICENSE).

## 📞 Support & Community

CNCF Slack channel: [#otel-arrow](https://slack.cncf.io/)

## 🌟 Roadmap

TBD

## ✅ Changelog

- CHANGELOG.md (TBD)

