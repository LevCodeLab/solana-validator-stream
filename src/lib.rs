//! In-process Agave Geyser plugin and Unix Domain Socket NDJSON frame streamer
//! for low-latency Solana validator callbacks.

pub mod events;

pub use events::{StreamPayload, ValidatorStreamFrame};

/// Crate wire protocol version marker.
pub const STREAM_PROTOCOL_VERSION: u32 = 1;
