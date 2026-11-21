//! In-process Agave Geyser plugin and Unix Domain Socket NDJSON frame streamer
//! for low-latency Solana validator callbacks.

pub mod config;
pub mod events;
pub mod ipc;
pub mod plugin;

pub use config::{ConfigError, PluginConfig, DEFAULT_SOCKET_PATH};
pub use events::{StreamPayload, ValidatorStreamFrame};
pub use ipc::{IpcStats, StreamIpcWriter};
pub use plugin::ValidatorStreamPlugin;

/// Crate wire protocol version marker.
pub const STREAM_PROTOCOL_VERSION: u32 = 1;

/// Dynamic C ABI entry point resolved by the Agave validator plugin loader.
///
/// # Safety
/// The Agave validator runtime assumes ownership of the returned raw pointer
/// and manages its teardown through [`agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin::on_unload`].
#[no_mangle]
#[allow(improper_ctypes_definitions)]
pub unsafe extern "C" fn _create_plugin(
) -> *mut dyn agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin {
    let plugin = Box::new(ValidatorStreamPlugin::new());
    Box::into_raw(plugin)
}
