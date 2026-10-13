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

/// Deserializes configuration into `T`.
pub fn deserialize_typed_config<T: serde::de::DeserializeOwned>(
    config: &serde_json::Value,
) -> Result<T, Error> {
    serde_json::from_value(config.clone()).map_err(|error| Error::InvalidUserConfig {
        error: error.to_string(),
    })
}

/// Validates that a JSON config value can be deserialized into the expected
/// configuration type `T`.
///
/// Resolver implementations may use this helper for an auxiliary static check.
/// Factory registrations must return their typed value through a resolver.
///
/// # Example
/// ```ignore
/// validate_typed_config::<AuxiliaryConfig>(raw)?;
/// ```
pub fn validate_typed_config<T: serde::de::DeserializeOwned>(
    raw: &serde_json::Value,
) -> Result<(), Error> {
    let _: T = deserialize_typed_config(raw)?;
    Ok(())
}

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
