// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

use crate::secret::RedactedString;

/// Common TLS configuration for both client and server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Path to the TLS cert to use for TLS required connections.
    pub cert_file: Option<PathBuf>,

    /// In memory PEM encoded TLS cert to use for TLS required connections.
    pub cert_pem: Option<String>,

    /// Path to the TLS key to use for TLS required connections.
    pub key_file: Option<PathBuf>,

    /// In memory PEM encoded TLS key to use for TLS required connections.
    pub key_pem: Option<RedactedString>,
    // TODO: Implement these fields
    // /// MinVersion sets the minimum TLS version that is acceptable.
    // /// If not set, TLS 1.2 will be used.
    // pub min_version: Option<String>,

    // /// MaxVersion sets the maximum TLS version that is acceptable.
    // pub max_version: Option<String>,

    // /// CipherSuites is a list of TLS cipher suites that the TLS transport can use.
    // pub cipher_suites: Option<Vec<String>>,

    // /// Trusted platform module configuration
    // pub tpm_config: Option<TpmConfig>,
    /// Minimum interval between checks for file-backed TLS material.
    /// Change detection and explicit `null` behavior are consumer-specific.
    /// Defaults to 5 minutes ("5m").
    /// Format: Standard duration string (e.g., "30s", "5m", "1h").
    #[serde(default = "default_reload_interval", with = "humantime_serde")]
    #[schemars(with = "Option<String>")]
    pub reload_interval: Option<Duration>,
}

const fn default_reload_interval() -> Option<Duration> {
    Some(Duration::from_secs(300))
}

/// TLS configuration specific to client connections.
///
/// This configuration is used by components that initiate TLS connections, such as:
/// - **Exporters**: When sending telemetry to a backend.
/// - **Scrapers/clients**: When pulling data from a target (e.g., Prometheus scrape, OTLP/HTTP client).
#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema, Default)]
pub struct TlsClientConfig {
    /// Common TLS configuration (client certificate + key for mTLS).
    #[serde(flatten)]
    pub config: TlsConfig,

    /// Path to a PEM bundle of CA certificates to trust when verifying the server.
    pub ca_file: Option<PathBuf>,

    /// In-memory PEM bundle of CA certificates to trust when verifying the server.
    pub ca_pem: Option<String>,

    /// Controls whether system CA certificates are loaded for server certificate verification.
    ///
    /// Defaults to true when omitted.
    pub include_system_ca_certs_pool: Option<bool>,

    /// Override the server name used for TLS SNI and certificate verification.
    ///
    /// This is typically needed when connecting by IP address or when the endpoint hostname
    /// does not match the certificate.
    #[serde(rename = "server_name_override")]
    pub server_name: Option<String>,

    /// Insecure disables TLS.
    #[serde(default)]
    pub insecure: Option<bool>,

    /// InsecureSkipVerify would enable TLS but skip server certificate verification.
    ///
    /// NOTE: This is currently **not supported** in the Rust collector implementation.
    /// If set to `true`, startup will fail with a clear error instead of silently
    /// proceeding with normal certificate verification.
    ///
    /// TODO: Implement this behind a clearly-labeled "dangerous" option once the
    /// tonic/rustls client stack supports plugging in a custom verifier.
    #[serde(default)]
    pub insecure_skip_verify: Option<bool>,
}

/// TLS configuration specific to server connections (terminating TLS/mTLS).
///
/// This configuration is used by components that accept inbound TLS connections, such as:
/// - **Servers/receivers**: When accepting telemetry from agents or other collectors.
#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema, Default)]
pub struct TlsServerConfig {
    /// Common TLS configuration.
    #[serde(flatten)]
    pub config: TlsConfig,

    /// Path to the TLS cert to use by the server to verify a client certificate.
    pub client_ca_file: Option<PathBuf>,

    /// In memory PEM encoded cert to use by the server to verify a client certificate.
    pub client_ca_pem: Option<String>,

    /// Controls whether system CA certificates are loaded for client certificate verification.
    pub include_system_ca_certs_pool: Option<bool>,
    // /// Path to the Certificate Revocation List (CRL) to use by the server to verify a client certificate.
    // pub client_crl_file: Option<PathBuf>,
    /// Enable file watching for client CA certificate hot-reload.
    /// When enabled, changes to `client_ca_file` will be detected and the
    /// client certificate verifier will be automatically updated without restart.
    /// Requires `client_ca_file` to be set. Defaults to false.
    #[serde(default)]
    pub watch_client_ca: bool,

    /// Timeout for TLS handshake. Defaults to 10 seconds.
    /// Prevents slow/malicious clients from holding connection slots.
    #[serde(default = "default_handshake_timeout", with = "humantime_serde")]
    #[schemars(with = "Option<String>")]
    pub handshake_timeout: Option<Duration>,
}

const fn default_handshake_timeout() -> Option<Duration> {
    Some(Duration::from_secs(10))
}

// Deserialize a closed, flat wire schema rather than combining flatten with
// deny_unknown_fields (which serde does not support). Serialization and schema
// generation retain the public flat TLS envelope.
macro_rules! deserialize_tls {
    ($target:ident { $($(#[$attr:meta])* $field:ident: $ty:ty),* $(,)? }) => {
        impl<'de> Deserialize<'de> for $target {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Wire {
                    cert_file: Option<PathBuf>,
                    cert_pem: Option<String>,
                    key_file: Option<PathBuf>,
                    key_pem: Option<RedactedString>,
                    #[serde(default = "default_reload_interval", with = "humantime_serde")]
                    reload_interval: Option<Duration>,
                    $($(#[$attr])* $field: $ty),*
                }
                let wire = Wire::deserialize(deserializer)?;
                Ok(Self {
                    config: TlsConfig {
                        cert_file: wire.cert_file,
                        cert_pem: wire.cert_pem,
                        key_file: wire.key_file,
                        key_pem: wire.key_pem,
                        reload_interval: wire.reload_interval,
                    },
                    $($field: wire.$field),*
                })
            }
        }
    };
}

deserialize_tls!(TlsClientConfig {
    ca_file: Option<PathBuf>,
    ca_pem: Option<String>,
    include_system_ca_certs_pool: Option<bool>,
    #[serde(rename = "server_name_override")]
    server_name: Option<String>,
    insecure: Option<bool>,
    insecure_skip_verify: Option<bool>,
});

deserialize_tls!(TlsServerConfig {
    client_ca_file: Option<PathBuf>,
    client_ca_pem: Option<String>,
    include_system_ca_certs_pool: Option<bool>,
    #[serde(default)]
    watch_client_ca: bool,
    #[serde(default = "default_handshake_timeout", with = "humantime_serde")]
    handshake_timeout: Option<Duration>,
});

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: Client/server TLS schemas receive misspelled security settings or common fields.
    /// Guarantees: Unknown fields are rejected instead of silently dropping TLS intent.
    #[test]
    fn tls_wire_schemas_reject_unknown_fields() {
        for value in [
            serde_json::json!({"key_pm": "private-key"}),
            serde_json::json!({"insecure_skip_verfy": true}),
        ] {
            assert!(serde_json::from_value::<TlsConfig>(value.clone()).is_err());
            assert!(serde_json::from_value::<TlsClientConfig>(value.clone()).is_err());
            assert!(serde_json::from_value::<TlsServerConfig>(value).is_err());
        }
    }

    /// Scenario: Flat client/server TLS input includes inline keys, overrides, defaults, or explicit nulls.
    /// Guarantees: Strict decoding preserves existing wire shape/defaults while serialization hides keys.
    #[test]
    fn strict_tls_decoding_preserves_flat_fields_and_defaults() {
        let client: TlsClientConfig = serde_json::from_value(serde_json::json!({
            "key_pem": "private-key", "cert_file": "cert.pem", "server_name_override": "public.example"
        })).unwrap();
        assert_eq!(
            client.config.key_pem.as_ref().unwrap().expose(),
            "private-key"
        );
        assert_eq!(
            client.config.reload_interval,
            Some(Duration::from_secs(300))
        );
        let encoded = serde_json::to_value(&client).unwrap();
        assert_eq!(encoded["key_pem"], "[REDACTED]");
        assert_eq!(encoded["server_name_override"], "public.example");
        assert!(encoded.get("config").is_none());
        let server: TlsServerConfig =
            serde_json::from_value(serde_json::json!({"reload_interval": null})).unwrap();
        assert_eq!(server.config.reload_interval, None);
        assert_eq!(server.handshake_timeout, Some(Duration::from_secs(10)));
        assert!(!server.watch_client_ca);
    }
}
