use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

/// Default Unix Domain Socket path used by both the validator plugin and local consumers.
pub const DEFAULT_SOCKET_PATH: &str = "/tmp/solana_validator_stream.sock";

/// Default observer node label stamped onto outgoing [`crate::ValidatorStreamFrame`] envelopes.
pub const DEFAULT_OBSERVER_ID: &str = "local-agave-validator";

/// Default bounded channel capacity before non-blocking drops engage.
pub const DEFAULT_BUFFER_CAPACITY: usize = 65_536;

/// Default reconnect retry interval in milliseconds when the UDS consumer is absent.
pub const DEFAULT_RECONNECT_BACKOFF_MS: u64 = 50;

/// Errors raised while loading or validating [`PluginConfig`].
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read plugin config at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid plugin config JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("channel_buffer_capacity must be at least 1")]
    ZeroCapacity,
}

/// Runtime configuration passed to Agave via `--geyser-plugin-config <file.json>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginConfig {
    #[serde(default)]
    pub libpath: Option<String>,
    #[serde(default = "default_socket_path")]
    pub uds_socket_path: String,
    #[serde(default = "default_observer_id")]
    pub observer_validator_id: String,
    #[serde(default = "default_true")]
    pub enable_bank_status: bool,
    #[serde(default = "default_true")]
    pub enable_block_footer: bool,
    #[serde(default = "default_true")]
    pub enable_update_parent: bool,
    #[serde(default = "default_true")]
    pub enable_deshred: bool,
    #[serde(default = "default_true")]
    pub enable_entries: bool,
    #[serde(default = "default_buffer_capacity")]
    pub channel_buffer_capacity: usize,
    #[serde(default = "default_reconnect_backoff_ms")]
    pub reconnect_backoff_ms: u64,
}

fn default_socket_path() -> String {
    DEFAULT_SOCKET_PATH.to_string()
}

fn default_observer_id() -> String {
    DEFAULT_OBSERVER_ID.to_string()
}

fn default_true() -> bool {
    true
}

fn default_buffer_capacity() -> usize {
    DEFAULT_BUFFER_CAPACITY
}

fn default_reconnect_backoff_ms() -> u64 {
    DEFAULT_RECONNECT_BACKOFF_MS
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            libpath: None,
            uds_socket_path: default_socket_path(),
            observer_validator_id: default_observer_id(),
            enable_bank_status: true,
            enable_block_footer: true,
            enable_update_parent: true,
            enable_deshred: true,
            enable_entries: true,
            channel_buffer_capacity: default_buffer_capacity(),
            reconnect_backoff_ms: default_reconnect_backoff_ms(),
        }
    }
}

impl PluginConfig {
    /// Parses and validates a JSON string into [`PluginConfig`].
    pub fn from_json_str(raw: &str) -> Result<Self, ConfigError> {
        let cfg: Self = serde_json::from_str(raw)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Reads, parses, and validates a JSON file on disk.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path_ref = path.as_ref();
        let data = std::fs::read_to_string(path_ref).map_err(|source| ConfigError::Io {
            path: path_ref.display().to_string(),
            source,
        })?;
        Self::from_json_str(&data)
    }

    /// Checks structural invariants on parsed config fields.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.channel_buffer_capacity == 0 {
            return Err(ConfigError::ZeroCapacity);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_json_uses_sensible_defaults() {
        let cfg = PluginConfig::from_json_str("{}").expect("valid defaults");
        assert_eq!(cfg.uds_socket_path, "/tmp/solana_validator_stream.sock");
        assert_eq!(cfg.observer_validator_id, "local-agave-validator");
        assert_eq!(cfg.channel_buffer_capacity, 65_536);
        assert_eq!(cfg.reconnect_backoff_ms, 50);
        assert!(cfg.enable_bank_status);
        assert!(cfg.enable_block_footer);
        assert!(cfg.enable_update_parent);
        assert!(cfg.enable_deshred);
        assert!(cfg.enable_entries);
    }

    #[test]
    fn custom_json_overrides_fields() {
        let raw = r#"{
            "libpath": "./target/release/libsolana_validator_stream.so",
            "uds_socket_path": "/tmp/custom_validator.sock",
            "observer_validator_id": "validator-blr-02",
            "enable_deshred": false,
            "channel_buffer_capacity": 8192,
            "reconnect_backoff_ms": 15
        }"#;

        let cfg = PluginConfig::from_json_str(raw).expect("parse custom config");
        assert_eq!(
            cfg.libpath.as_deref(),
            Some("./target/release/libsolana_validator_stream.so")
        );
        assert_eq!(cfg.uds_socket_path, "/tmp/custom_validator.sock");
        assert_eq!(cfg.observer_validator_id, "validator-blr-02");
        assert!(!cfg.enable_deshred);
        assert!(cfg.enable_entries);
        assert_eq!(cfg.channel_buffer_capacity, 8_192);
        assert_eq!(cfg.reconnect_backoff_ms, 15);
    }

    #[test]
    fn zero_capacity_is_rejected() {
        let err = PluginConfig::from_json_str(r#"{"channel_buffer_capacity": 0}"#)
            .expect_err("zero capacity should fail");
        assert!(matches!(err, ConfigError::ZeroCapacity));
    }
}
