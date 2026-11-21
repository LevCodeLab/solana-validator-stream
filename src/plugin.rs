use crate::config::{ConfigError, PluginConfig};
use crate::events::{StreamPayload, ValidatorStreamFrame};
use crate::ipc::{IpcStats, StreamIpcWriter};
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaBlockInfoVersions,
    ReplicaDeshredUpdateParentInfoVersions, ReplicaEntryInfoVersions,
    ReplicaEntryUpdateParentInfoVersions, Result as GeyserResult, SlotStatus,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

/// In-process Agave Geyser plugin streaming slot, bank, deshred, entry, and block
/// footer callbacks over a local Unix Domain Socket.
#[derive(Debug)]
pub struct ValidatorStreamPlugin {
    config: PluginConfig,
    ipc_writer: Option<StreamIpcWriter>,
    sequence_counter: Arc<AtomicU64>,
}

impl ValidatorStreamPlugin {
    /// Creates an uninitialized plugin instance prior to `on_load`.
    pub fn new() -> Self {
        Self {
            config: PluginConfig::default(),
            ipc_writer: None,
            sequence_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Creates a plugin instance and immediately starts its background UDS writer.
    pub fn with_config(config: PluginConfig) -> Self {
        let writer = StreamIpcWriter::with_backoff(
            config.uds_socket_path.clone(),
            config.channel_buffer_capacity,
            config.reconnect_backoff_ms,
        );
        Self {
            config,
            ipc_writer: Some(writer),
            sequence_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Reads and validates a JSON config file, starting or replacing the IPC writer.
    pub fn load_config(&mut self, config_file: &str) -> Result<(), ConfigError> {
        info!("loading solana-validator-stream config from {}", config_file);
        let config = PluginConfig::from_file(config_file)?;
        self.ipc_writer = Some(StreamIpcWriter::with_backoff(
            config.uds_socket_path.clone(),
            config.channel_buffer_capacity,
            config.reconnect_backoff_ms,
        ));
        self.config = config;
        info!(
            "solana-validator-stream ready on UDS {}",
            self.config.uds_socket_path
        );
        Ok(())
    }

    /// Returns the active plugin configuration.
    pub fn config(&self) -> &PluginConfig {
        &self.config
    }

    /// Returns current IPC sent, dropped, reconnect, and connection status metrics.
    pub fn stats(&self) -> IpcStats {
        if let Some(ref writer) = self.ipc_writer {
            writer.stats()
        } else {
            IpcStats {
                sent_count: 0,
                dropped_count: 0,
                reconnect_count: 0,
                connected: false,
            }
        }
    }

    fn current_unix_nanos() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }

    /// Wraps a [`StreamPayload`] in a sequenced [`ValidatorStreamFrame`] and queues it.
    pub fn dispatch_payload(&self, payload: StreamPayload) -> bool {
        if let Some(ref writer) = self.ipc_writer {
            let seq = self.sequence_counter.fetch_add(1, Ordering::Relaxed);
            let frame = ValidatorStreamFrame::new(
                seq,
                self.config.observer_validator_id.clone(),
                Self::current_unix_nanos(),
                payload,
            );
            writer.dispatch(frame)
        } else {
            false
        }
    }

    /// Emits a [`StreamPayload::SlotStatus`] notification.
    pub fn notify_slot_status(
        &self,
        slot: u64,
        parent_slot: Option<u64>,
        bank_id: Option<u64>,
        status: &str,
    ) -> bool {
        self.dispatch_payload(StreamPayload::SlotStatus {
            slot,
            parent_slot,
            bank_id,
            status: status.to_string(),
        })
    }

    /// Emits a [`StreamPayload::BankLifecycle`] notification when enabled.
    pub fn update_bank_status(
        &self,
        slot: u64,
        parent_slot: Option<u64>,
        status: &str,
        bank_id: u64,
        bank_hash: Option<String>,
    ) -> bool {
        if !self.config.enable_bank_status {
            return false;
        }
        self.dispatch_payload(StreamPayload::BankLifecycle {
            slot,
            bank_id,
            parent_slot,
            status: status.to_string(),
            bank_hash,
        })
    }

    /// Emits a [`StreamPayload::BlockFooter`] notification when enabled.
    #[allow(clippy::too_many_arguments)]
    pub fn notify_block_footer(
        &self,
        slot: u64,
        bank_id: u64,
        bank_hash: String,
        producer_time_nanos: u64,
        user_agent: String,
        final_cert: Option<Vec<u8>>,
        notar_cert: Option<Vec<u8>>,
        skip_cert: Option<Vec<u8>>,
    ) -> bool {
        if !self.config.enable_block_footer {
            return false;
        }
        self.dispatch_payload(StreamPayload::BlockFooter {
            slot,
            bank_id,
            bank_hash,
            producer_time_nanos,
            user_agent,
            final_cert,
            notar_cert,
            skip_cert,
        })
    }

    /// Emits a [`StreamPayload::UpdateParent`] marker when enabled.
    pub fn notify_update_parent(
        &self,
        slot: u64,
        cleared_bank_id: Option<u64>,
        parent_slot: u64,
        parent_block_id: Option<String>,
        fec_set_index: Option<u32>,
        source: &str,
    ) -> bool {
        if !self.config.enable_update_parent {
            return false;
        }
        self.dispatch_payload(StreamPayload::UpdateParent {
            slot,
            cleared_bank_id,
            parent_slot,
            parent_block_id,
            fec_set_index,
            source: source.to_string(),
        })
    }

    /// Emits a [`StreamPayload::DeshredTx`] pre-execution transaction frame when enabled.
    pub fn notify_deshred(
        &self,
        slot: u64,
        signature: String,
        raw_bytes: Vec<u8>,
        static_accounts: Vec<String>,
        fec_set_index: Option<u64>,
    ) -> bool {
        if !self.config.enable_deshred {
            return false;
        }
        self.dispatch_payload(StreamPayload::DeshredTx {
            slot,
            signature,
            raw_bytes,
            static_accounts,
            pre_execution_timestamp_nanos: Self::current_unix_nanos(),
            fec_set_index,
        })
    }

    /// Emits a [`StreamPayload::Entry`] execution batch summary when enabled.
    pub fn notify_entry(
        &self,
        slot: u64,
        bank_id: Option<u64>,
        entry_index: u64,
        tx_count: u64,
        starting_tx_index: u64,
    ) -> bool {
        if !self.config.enable_entries {
            return false;
        }
        self.dispatch_payload(StreamPayload::Entry {
            slot,
            bank_id,
            entry_index,
            tx_count,
            starting_tx_index,
        })
    }

    /// Emits a [`StreamPayload::BlockMeta`] sealed block notification.
    pub fn notify_block_metadata(
        &self,
        slot: u64,
        blockhash: String,
        parent_slot: u64,
        parent_blockhash: String,
        executed_tx_count: u64,
    ) -> bool {
        self.dispatch_payload(StreamPayload::BlockMeta {
            slot,
            blockhash,
            parent_slot,
            parent_blockhash,
            executed_tx_count,
        })
    }

    /// Emits a [`StreamPayload::Heartbeat`] liveness marker.
    pub fn notify_heartbeat(&self, active_slots: u64) -> bool {
        self.dispatch_payload(StreamPayload::Heartbeat {
            timestamp_nanos: Self::current_unix_nanos(),
            active_slots,
        })
    }
}

impl Default for ValidatorStreamPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl GeyserPlugin for ValidatorStreamPlugin {
    fn name(&self) -> &'static str {
        "solana-validator-stream"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> GeyserResult<()> {
        self.load_config(config_file)
            .map_err(|err| GeyserPluginError::ConfigFileReadError {
                msg: err.to_string(),
            })
    }

    fn on_unload(&mut self) {
        info!("unloading solana-validator-stream plugin");
        self.ipc_writer = None;
    }

    fn update_slot_status(
        &self,
        slot: solana_clock::Slot,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> GeyserResult<()> {
        self.notify_slot_status(slot, parent, None, status.as_str());
        Ok(())
    }

    fn update_bank_status(
        &self,
        slot: solana_clock::Slot,
        parent: Option<u64>,
        status: &SlotStatus,
        bank_id: solana_clock::BankId,
    ) -> GeyserResult<()> {
        ValidatorStreamPlugin::update_bank_status(self, slot, parent, status.as_str(), bank_id, None);
        Ok(())
    }

    fn notify_block_metadata_for_bank(
        &self,
        blockinfo: ReplicaBlockInfoVersions,
        _bank_id: solana_clock::BankId,
    ) -> GeyserResult<()> {
        match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(b) => {
                self.notify_block_metadata(b.slot, b.blockhash.to_string(), 0, String::new(), 0);
            }
            ReplicaBlockInfoVersions::V0_0_2(b) => {
                self.notify_block_metadata(
                    b.slot,
                    b.blockhash.to_string(),
                    b.parent_slot,
                    b.parent_blockhash.to_string(),
                    b.executed_transaction_count,
                );
            }
            ReplicaBlockInfoVersions::V0_0_3(b) => {
                self.notify_block_metadata(
                    b.slot,
                    b.blockhash.to_string(),
                    b.parent_slot,
                    b.parent_blockhash.to_string(),
                    b.executed_transaction_count,
                );
            }
            ReplicaBlockInfoVersions::V0_0_4(b) => {
                self.notify_block_metadata(
                    b.slot,
                    b.blockhash.to_string(),
                    b.parent_slot,
                    b.parent_blockhash.to_string(),
                    b.executed_transaction_count,
                );
            }
        }
        Ok(())
    }

    fn notify_entry_for_bank(
        &self,
        entry: ReplicaEntryInfoVersions,
        bank_id: solana_clock::BankId,
    ) -> GeyserResult<()> {
        match entry {
            ReplicaEntryInfoVersions::V0_0_1(e) => {
                self.notify_entry(
                    e.slot,
                    Some(bank_id),
                    e.index as u64,
                    e.executed_transaction_count,
                    0,
                );
            }
            ReplicaEntryInfoVersions::V0_0_2(e) => {
                self.notify_entry(
                    e.slot,
                    Some(bank_id),
                    e.index as u64,
                    e.executed_transaction_count,
                    e.starting_transaction_index as u64,
                );
            }
        }
        Ok(())
    }

    fn notify_entry_update_parent(
        &self,
        update_parent: ReplicaEntryUpdateParentInfoVersions,
    ) -> GeyserResult<()> {
        match update_parent {
            ReplicaEntryUpdateParentInfoVersions::V0_0_1(u) => {
                self.notify_update_parent(
                    u.slot,
                    Some(u.cleared_bank_id),
                    u.parent_slot,
                    Some(u.parent_block_id.to_string()),
                    None,
                    "entry",
                );
            }
        }
        Ok(())
    }

    fn notify_deshred_update_parent(
        &self,
        update_parent: ReplicaDeshredUpdateParentInfoVersions,
    ) -> GeyserResult<()> {
        match update_parent {
            ReplicaDeshredUpdateParentInfoVersions::V0_0_1(u) => {
                self.notify_update_parent(
                    u.slot,
                    None,
                    u.parent_slot,
                    Some(u.parent_block_id.to_string()),
                    Some(u.update_parent_fec_set_index),
                    "deshred",
                );
            }
        }
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        false
    }

    fn account_data_snapshot_notifications_enabled(&self) -> bool {
        false
    }

    fn transaction_notifications_enabled(&self) -> bool {
        false
    }

    fn entry_notifications_enabled(&self) -> bool {
        self.config.enable_entries
    }

    fn block_footer_notifications_enabled(&self) -> bool {
        self.config.enable_block_footer
    }

    fn deshred_transaction_notifications_enabled(&self) -> bool {
        self.config.enable_deshred
    }
}
