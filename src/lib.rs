//! In-process Agave Geyser plugin and Unix Domain Socket NDJSON frame streamer
//! for low-latency Solana validator callbacks.

/// Crate wire protocol version marker.
pub const STREAM_PROTOCOL_VERSION: u32 = 1;
