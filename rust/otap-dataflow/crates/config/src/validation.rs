// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Validation helpers for node configuration.
//!
//! These helpers support component resolver implementations that reject
//! submitted configuration before constructing a resolved runtime value.
//!
//! **Scope:** validation performs *static* checks -- it verifies
//! that the config value can be deserialized into the expected type. It does
//! **not** detect runtime issues such as port conflicts, unreachable endpoints,
//! missing files, or other conditions that only manifest when the engine starts.
//! Those errors will still surface at startup time.

use crate::error::Error;

/// Validator for components that accept **no** user configuration.
///
/// Accepts `Value::Null` (config key omitted / set to `null`) and empty
/// objects `{}`. Rejects anything else so that typos or misplaced config
/// blocks are caught early.
///
/// # Example
/// ```ignore
/// no_config(raw)?;
/// ```
pub fn no_config(config: &serde_json::Value) -> Result<(), Error> {
    match config {
        serde_json::Value::Null => Ok(()),
        serde_json::Value::Object(map) if map.is_empty() => Ok(()),
        _ => Err(Error::InvalidUserConfig {
            error: format!(
                "This component does not accept configuration, but received: {}",
                config
            ),
        }),
    }
}
