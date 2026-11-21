//! In-process Agave Geyser plugin and Unix Domain Socket NDJSON frame streamer
//! for low-latency Solana validator callbacks.

pub mod config;
pub mod events;
pub mod ipc;

pub use config::{ConfigError, PluginConfig, DEFAULT_SOCKET_PATH};
pub use events::{StreamPayload, ValidatorStreamFrame};
pub use ipc::{IpcStats, StreamIpcWriter};

/// Crate wire protocol version marker.
pub const STREAM_PROTOCOL_VERSION: u32 = 1;
