// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the Weaver live-check processor.

use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use serde::{Deserialize, Serialize};
use weaver_common::result::WResult;
use weaver_common::vdir::VirtualDirectoryPath;
use weaver_forge::registry::ResolvedRegistry;
use weaver_resolver::SchemaResolver;
use weaver_semconv::registry_repo::RegistryRepo;

use super::FINDINGS_PORT;

/// Processor configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Path to the semantic-conventions registry to validate against.
    pub registry_path: VirtualDirectoryPath,
}

/// Loaded registry plus the URL/path string that should be attached to findings.
#[derive(Clone, Debug)]
pub struct LoadedRegistry {
    /// Resolved registry used by `weaver_live_check`.
    pub resolved_registry: ResolvedRegistry,
    /// Stable string form of the configured registry source.
    pub registry_url: String,
}

impl Config {
    /// Load and resolve the configured semantic-conventions registry.
    pub fn load_registry(&self) -> Result<LoadedRegistry, ConfigError> {
        let registry_repo = RegistryRepo::try_new("main", &self.registry_path).map_err(|err| {
            ConfigError::InvalidUserConfig {
                error: format!("failed to open registry '{}': {err}", self.registry_path),
            }
        })?;

        let registry = match SchemaResolver::load_semconv_repository(registry_repo, false) {
            WResult::Ok(registry) | WResult::OkWithNFEs(registry, _) => registry,
            WResult::FatalErr(err) => {
                return Err(ConfigError::InvalidUserConfig {
                    error: format!(
                        "failed to load semantic registry '{}': {err}",
                        self.registry_path
                    ),
                });
            }
        };

        let resolved_schema = match SchemaResolver::resolve(registry, true) {
            WResult::Ok(resolved_schema) | WResult::OkWithNFEs(resolved_schema, _) => {
                resolved_schema
            }
            WResult::FatalErr(err) => {
                return Err(ConfigError::InvalidUserConfig {
                    error: format!(
                        "failed to resolve semantic registry '{}': {err}",
                        self.registry_path
                    ),
                });
            }
        };

        let resolved_registry = ResolvedRegistry::try_from_resolved_registry(
            &resolved_schema.registry,
            resolved_schema.catalog(),
        )
        .map_err(|err| ConfigError::InvalidUserConfig {
            error: format!(
                "failed to materialize semantic registry '{}': {err}",
                self.registry_path
            ),
        })?;

        let registry_url = if resolved_registry.registry_url.is_empty() {
            self.registry_path.to_string()
        } else {
            resolved_registry.registry_url.clone()
        };

        Ok(LoadedRegistry {
            resolved_registry,
            registry_url,
        })
    }
}

/// Validate node output configuration for the fixed `findings` side-stream.
pub fn validate_node_outputs(node_config: &NodeUserConfig) -> Result<(), ConfigError> {
    let findings_declared = node_config.outputs.iter().any(|port| port == FINDINGS_PORT);

    if findings_declared && node_config.outputs.len() == 1 {
        return Err(ConfigError::InvalidUserConfig {
            error: format!(
                "processor output '{FINDINGS_PORT}' is reserved for findings; declare a separate passthrough output and set default_output explicitly"
            ),
        });
    }

    if node_config.outputs.len() > 1 && node_config.default_output.is_none() {
        return Err(ConfigError::InvalidUserConfig {
            error: format!(
                "processor has multiple outputs; set default_output explicitly when using '{FINDINGS_PORT}' alongside passthrough telemetry"
            ),
        });
    }

    if node_config.default_output.as_deref() == Some(FINDINGS_PORT) {
        return Err(ConfigError::InvalidUserConfig {
            error: format!(
                "default_output must not be '{FINDINGS_PORT}'; that port is reserved for finding events"
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn cargo_home() -> std::path::PathBuf {
        std::env::var_os("CARGO_HOME")
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(std::path::PathBuf::from)
                    .map(|home| home.join(".cargo"))
            })
            .expect("CARGO_HOME or HOME must be set for tests")
    }

    fn find_weaver_registry_fixture() -> std::path::PathBuf {
        let checkouts = cargo_home().join("git").join("checkouts");
        let entries = fs::read_dir(&checkouts).expect("cargo git checkouts should exist");

        for entry in entries.flatten() {
            let Ok(revisions) = fs::read_dir(entry.path()) else {
                continue;
            };
            for revision in revisions.flatten() {
                let candidate = revision
                    .path()
                    .join("crates/weaver_codegen_test/semconv_registry");
                if candidate.is_dir() {
                    return candidate;
                }
            }
        }

        panic!("failed to locate a local weaver semconv test fixture under {checkouts:?}");
    }

    #[test]
    fn config_loads_local_registry_fixture() {
        let config = Config {
            registry_path: VirtualDirectoryPath::LocalFolder {
                path: find_weaver_registry_fixture().display().to_string(),
            },
        };

        let loaded = config.load_registry().expect("registry should load");
        assert!(!loaded.resolved_registry.groups.is_empty());
        assert!(!loaded.registry_url.is_empty());
    }

    #[test]
    fn config_rejects_invalid_shape() {
        let err = serde_json::from_value::<Config>(serde_json::json!({
            "registry_path": "./registry",
            "unexpected": true
        }))
        .expect_err("unknown field must be rejected");

        assert!(err.to_string().contains("unexpected"));
    }

    #[test]
    fn config_reports_registry_load_failure() {
        let missing = std::env::temp_dir().join("missing-weaver-live-check-registry");
        let config = Config {
            registry_path: VirtualDirectoryPath::LocalFolder {
                path: missing.display().to_string(),
            },
        };

        let err = config
            .load_registry()
            .expect_err("missing registry must fail");
        assert!(err.to_string().contains("failed to"));
    }

    #[test]
    fn node_output_validation_requires_explicit_passthrough_default() {
        let mut node_config =
            NodeUserConfig::new_processor_config("urn:otel:processor:weaver_live_check");
        node_config.outputs = vec!["telemetry".into(), FINDINGS_PORT.into()];

        let err = validate_node_outputs(&node_config).expect_err("default_output is required");
        assert!(err.to_string().contains("default_output"));

        node_config.default_output = Some(FINDINGS_PORT.into());
        let err = validate_node_outputs(&node_config).expect_err("findings cannot be the default");
        assert!(err.to_string().contains("reserved"));

        node_config.default_output = Some("telemetry".into());
        validate_node_outputs(&node_config).expect("valid output configuration");
    }
}
