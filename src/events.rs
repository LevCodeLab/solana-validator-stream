use serde::{Deserialize, Serialize};

/// Newline-delimited JSON (NDJSON) envelope shipped from the Agave validator
/// Geyser plugin over Unix Domain Sockets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorStreamFrame {
    pub sequence: u64,
    pub observer_validator: String,
    pub observed_at_nanos: u64,
    pub payload: StreamPayload,
}

impl ValidatorStreamFrame {
    /// Constructs a new wire frame ready for NDJSON serialization.
    pub fn new(
        sequence: u64,
        observer_validator: impl Into<String>,
        observed_at_nanos: u64,
        payload: StreamPayload,
    ) -> Self {
        Self {
            sequence,
            observer_validator: observer_validator.into(),
            observed_at_nanos,
            payload,
        }
    }

    /// Encodes the frame as compact JSON followed by a trailing `\n` byte.
    pub fn to_ndjson_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = serde_json::to_vec(self)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// Decodes a single NDJSON line into a `ValidatorStreamFrame`.
    pub fn from_ndjson_line(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line.trim_end_matches(&['\r', '\n'][..]))
    }
}

/// Typed validator callback payloads carried inside a [`ValidatorStreamFrame`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum StreamPayload {
    /// Slot status transition (Processed, Confirmed, Rooted, Dead, CreatedBank).
    SlotStatus {
        slot: u64,
        parent_slot: Option<u64>,
        bank_id: Option<u64>,
        status: String,
    },
    /// Bank-specific lifecycle transition inside replay / banking stages.
    BankLifecycle {
        slot: u64,
        bank_id: u64,
        parent_slot: Option<u64>,
        status: String,
        bank_hash: Option<String>,
    },
    /// Alpenglow block footer notification with optional certificate bytes.
    BlockFooter {
        slot: u64,
        bank_id: u64,
        bank_hash: String,
        producer_time_nanos: u64,
        user_agent: String,
        final_cert: Option<Vec<u8>>,
        notar_cert: Option<Vec<u8>>,
        skip_cert: Option<Vec<u8>>,
    },
    /// Fast leader handover UpdateParent marker from entry or deshred path.
    UpdateParent {
        slot: u64,
        cleared_bank_id: Option<u64>,
        parent_slot: u64,
        parent_block_id: Option<String>,
        fec_set_index: Option<u32>,
        source: String,
    },
    /// Pre-execution reconstructed transaction from shred assembly.
    DeshredTx {
        slot: u64,
        signature: String,
        raw_bytes: Vec<u8>,
        static_accounts: Vec<String>,
        pre_execution_timestamp_nanos: u64,
        fec_set_index: Option<u64>,
    },
    /// Ledger entry execution summary for a bank.
    Entry {
        slot: u64,
        bank_id: Option<u64>,
        entry_index: u64,
        tx_count: u64,
        starting_tx_index: u64,
    },
    /// Sealed block metadata notification.
    BlockMeta {
        slot: u64,
        blockhash: String,
        parent_slot: u64,
        parent_blockhash: String,
        executed_tx_count: u64,
    },
    /// Streamer liveness heartbeat.
    Heartbeat {
        timestamp_nanos: u64,
        active_slots: u64,
    },
}

impl StreamPayload {
    /// Returns the slot associated with this payload, or `None` for heartbeats.
    pub fn slot(&self) -> Option<u64> {
        match self {
            Self::SlotStatus { slot, .. }
            | Self::BankLifecycle { slot, .. }
            | Self::BlockFooter { slot, .. }
            | Self::UpdateParent { slot, .. }
            | Self::DeshredTx { slot, .. }
            | Self::Entry { slot, .. }
            | Self::BlockMeta { slot, .. } => Some(*slot),
            Self::Heartbeat { .. } => None,
        }
    }

    /// Returns a short static label for metrics and debug output.
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::SlotStatus { .. } => "SlotStatus",
            Self::BankLifecycle { .. } => "BankLifecycle",
            Self::BlockFooter { .. } => "BlockFooter",
            Self::UpdateParent { .. } => "UpdateParent",
            Self::DeshredTx { .. } => "DeshredTx",
            Self::Entry { .. } => "Entry",
            Self::BlockMeta { .. } => "BlockMeta",
            Self::Heartbeat { .. } => "Heartbeat",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_all_payload_variants_as_ndjson() {
        let variants = vec![
            StreamPayload::SlotStatus {
                slot: 312_000_001,
                parent_slot: Some(312_000_000),
                bank_id: Some(7),
                status: "Processed".to_string(),
            },
            StreamPayload::BankLifecycle {
                slot: 312_000_001,
                bank_id: 7,
                parent_slot: Some(312_000_000),
                status: "Confirmed".to_string(),
                bank_hash: Some("bank_hash_demo_312000001".to_string()),
            },
            StreamPayload::BlockFooter {
                slot: 312_000_001,
                bank_id: 7,
                bank_hash: "bank_hash_demo_312000001".to_string(),
                producer_time_nanos: 1_760_000_000_123_456_789,
                user_agent: "agave/2.2.0".to_string(),
                final_cert: Some(vec![0xaa, 0xbb, 0xcc]),
                notar_cert: Some(vec![0x01, 0x02]),
                skip_cert: None,
            },
            StreamPayload::UpdateParent {
                slot: 312_000_002,
                cleared_bank_id: Some(7),
                parent_slot: 312_000_000,
                parent_block_id: Some("block_id_parent_312000000".to_string()),
                fec_set_index: Some(4),
                source: "deshred".to_string(),
            },
            StreamPayload::DeshredTx {
                slot: 312_000_002,
                signature: "sig_demo_tx_0001".to_string(),
                raw_bytes: vec![1, 2, 3, 4, 5],
                static_accounts: vec![
                    "acct_program_compute_budget".to_string(),
                    "acct_payer_local_test".to_string(),
                ],
                pre_execution_timestamp_nanos: 1_760_000_000_222_333_444,
                fec_set_index: Some(12),
            },
            StreamPayload::Entry {
                slot: 312_000_002,
                bank_id: Some(8),
                entry_index: 19,
                tx_count: 64,
                starting_tx_index: 512,
            },
            StreamPayload::BlockMeta {
                slot: 312_000_002,
                blockhash: "blockhash_demo_312000002".to_string(),
                parent_slot: 312_000_001,
                parent_blockhash: "blockhash_demo_312000001".to_string(),
                executed_tx_count: 1_842,
            },
            StreamPayload::Heartbeat {
                timestamp_nanos: 1_760_000_000_999_000_000,
                active_slots: 3,
            },
        ];

        for (idx, payload) in variants.into_iter().enumerate() {
            let expected_slot = payload.slot();
            let frame = ValidatorStreamFrame::new(
                (idx + 1) as u64,
                "validator-test-01",
                1_760_000_000_000_000_000 + idx as u64,
                payload,
            );

            let encoded = frame.to_ndjson_bytes().expect("ndjson serialize");
            assert_eq!(encoded.last(), Some(&b'\n'));

            let line = std::str::from_utf8(&encoded).expect("valid utf8");
            let decoded = ValidatorStreamFrame::from_ndjson_line(line).expect("ndjson parse");
            assert_eq!(decoded, frame);
            assert_eq!(decoded.payload.slot(), expected_slot);
            assert!(!decoded.payload.kind_name().is_empty());
        }
    }
}
