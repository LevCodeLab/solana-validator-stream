use solana_validator_stream::{
    PluginConfig, StreamPayload, ValidatorStreamPlugin, ValidatorStreamReader,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sock_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| format!("/tmp/svs_demo_{}.sock", std::process::id()));

    let (mut reader, mut rx) = ValidatorStreamReader::bind(&sock_path, 1024)?;
    println!("listening on UDS: {sock_path}");

    let config = PluginConfig {
        uds_socket_path: sock_path.clone(),
        observer_validator_id: "validator-mumbai-01".to_string(),
        reconnect_backoff_ms: 10,
        ..PluginConfig::default()
    };

    let plugin = ValidatorStreamPlugin::with_config(config);

    let slot = 312_849_104u64;
    plugin.notify_slot_status(slot, Some(slot - 1), Some(42), "CreatedBank");
    plugin.notify_deshred(
        slot,
        "tx_deshred_0482".to_string(),
        vec![1, 2, 3, 4, 5, 6, 7, 8],
        vec![
            "acct_compute_budget".to_string(),
            "acct_fee_payer".to_string(),
        ],
        Some(12),
    );
    plugin.notify_entry(slot, Some(42), 18, 64, 512);
    plugin.notify_update_parent(
        slot + 1,
        Some(41),
        slot,
        Some("block_id_312849104".to_string()),
        Some(2),
        "deshred",
    );
    plugin.notify_block_footer(
        slot,
        42,
        "bank_hash_312849104".to_string(),
        now_nanos(),
        "agave/2.2.0".to_string(),
        Some(vec![0xaa; 32]),
        None,
        None,
    );
    plugin.update_bank_status(
        slot,
        Some(slot - 1),
        "Confirmed",
        42,
        Some("bank_hash_312849104".to_string()),
    );

    for _ in 0..6 {
        if let Ok(Some(frame)) = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            let hop_us = now_nanos().saturating_sub(frame.observed_at_nanos) as f64 / 1_000.0;
            let detail = match &frame.payload {
                StreamPayload::SlotStatus { slot, status, .. } => {
                    format!("slot={slot} status={status}")
                }
                StreamPayload::DeshredTx {
                    slot,
                    signature,
                    fec_set_index,
                    ..
                } => {
                    format!("slot={slot} fec_set={fec_set_index:?} sig={signature}")
                }
                StreamPayload::Entry {
                    slot,
                    entry_index,
                    tx_count,
                    ..
                } => {
                    format!("slot={slot} entry_idx={entry_index} tx_count={tx_count}")
                }
                StreamPayload::UpdateParent {
                    slot,
                    parent_slot,
                    source,
                    ..
                } => {
                    format!("slot={slot} parent={parent_slot} src={source}")
                }
                StreamPayload::BlockFooter {
                    slot, user_agent, ..
                } => {
                    format!("slot={slot} agent={user_agent}")
                }
                StreamPayload::BankLifecycle {
                    slot,
                    bank_id,
                    status,
                    ..
                } => {
                    format!("slot={slot} bank_id={bank_id} status={status}")
                }
                other => format!("slot={:?}", other.slot()),
            };

            println!(
                "seq={:<4} +{:<6.1}us {:<14} {}",
                frame.sequence,
                hop_us,
                frame.payload.kind_name(),
                detail
            );
        }
    }

    let snap = reader.counters().snapshot();
    let writer_stats = plugin.stats();
    println!(
        "summary :: rx={} sent={} dropped={} seq_gaps={} parse_err={}",
        snap.frames_received_total,
        writer_stats.sent_count,
        writer_stats.dropped_count,
        snap.sequence_gaps_detected,
        snap.frames_parse_failed
    );

    reader.shutdown();
    Ok(())
}
