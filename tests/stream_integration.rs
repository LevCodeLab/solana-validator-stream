use agave_geyser_plugin_interface::geyser_plugin_interface::{GeyserPlugin, SlotStatus};
use solana_validator_stream::{
    _create_plugin, BlockingStreamReader, PluginConfig, StreamPayload, ValidatorStreamFrame,
    ValidatorStreamPlugin, ValidatorStreamReader, DEFAULT_SOCKET_PATH,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::time::timeout;

static SOCK_NONCE: AtomicU64 = AtomicU64::new(1);

fn unique_sock_path(tag: &str) -> String {
    let n = SOCK_NONCE.fetch_add(1, Ordering::Relaxed);
    format!("/tmp/svs_{}_{}_{}.sock", tag, std::process::id(), n)
}

#[tokio::test]
async fn uds_roundtrip_all_eight_payload_variants() {
    let sock_path = unique_sock_path("roundtrip");
    let (mut reader, mut rx) =
        ValidatorStreamReader::bind(&sock_path, 64).expect("bind async UDS reader");

    let config = PluginConfig {
        uds_socket_path: sock_path.clone(),
        observer_validator_id: "validator-integration-01".to_string(),
        reconnect_backoff_ms: 10,
        ..PluginConfig::default()
    };

    let plugin = ValidatorStreamPlugin::with_config(config);

    assert!(plugin.notify_slot_status(400_000_001, Some(400_000_000), Some(11), "Processed"));
    assert!(plugin.update_bank_status(
        400_000_001,
        Some(400_000_000),
        "Confirmed",
        11,
        Some("bank_hash_400000001".to_string()),
    ));
    assert!(plugin.notify_block_footer(
        400_000_001,
        11,
        "bank_hash_400000001".to_string(),
        1_760_100_000_000_000_000,
        "agave/2.2.0".to_string(),
        Some(vec![0x10, 0x20, 0x30]),
        Some(vec![0x40]),
        None,
    ));
    assert!(plugin.notify_update_parent(
        400_000_002,
        Some(11),
        400_000_000,
        Some("parent_block_400000000".to_string()),
        Some(3),
        "deshred",
    ));
    assert!(plugin.notify_deshred(
        400_000_002,
        "sig_integration_tx_01".to_string(),
        vec![9, 8, 7, 6],
        vec!["acct_sys_program".to_string(), "acct_fee_payer".to_string()],
        Some(7),
    ));
    assert!(plugin.notify_entry(400_000_003, Some(12), 4, 32, 128));
    assert!(plugin.notify_block_metadata(
        400_000_004,
        "blockhash_400000004".to_string(),
        400_000_003,
        "blockhash_400000003".to_string(),
        912,
    ));
    assert!(plugin.notify_heartbeat(2));

    let mut received = Vec::with_capacity(8);
    for _ in 0..8 {
        let frame = timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timed out waiting for UDS frame")
            .expect("reader channel closed unexpectedly");
        received.push(frame);
    }

    for (idx, frame) in received.iter().enumerate() {
        assert_eq!(frame.sequence, (idx + 1) as u64);
        assert_eq!(frame.observer_validator, "validator-integration-01");
    }

    assert!(matches!(
        &received[0].payload,
        StreamPayload::SlotStatus { slot: 400_000_001, status, .. } if status == "Processed"
    ));
    assert!(matches!(
        &received[1].payload,
        StreamPayload::BankLifecycle { slot: 400_000_001, bank_id: 11, status, .. } if status == "Confirmed"
    ));
    assert!(matches!(
        &received[2].payload,
        StreamPayload::BlockFooter { slot: 400_000_001, bank_id: 11, final_cert: Some(cert), .. } if cert == &vec![0x10, 0x20, 0x30]
    ));
    assert!(matches!(
        &received[3].payload,
        StreamPayload::UpdateParent { slot: 400_000_002, cleared_bank_id: Some(11), fec_set_index: Some(3), .. }
    ));
    assert!(matches!(
        &received[4].payload,
        StreamPayload::DeshredTx { slot: 400_000_002, signature, fec_set_index: Some(7), .. } if signature == "sig_integration_tx_01"
    ));
    assert!(matches!(
        &received[5].payload,
        StreamPayload::Entry { slot: 400_000_003, entry_index: 4, tx_count: 32, starting_tx_index: 128, .. }
    ));
    assert!(matches!(
        &received[6].payload,
        StreamPayload::BlockMeta { slot: 400_000_004, executed_tx_count: 912, .. }
    ));
    assert!(matches!(
        &received[7].payload,
        StreamPayload::Heartbeat { active_slots: 2, .. }
    ));

    let snap = reader.counters().snapshot();
    assert_eq!(snap.frames_received_total, 8);
    assert_eq!(snap.slot_events, 1);
    assert_eq!(snap.bank_events, 1);
    assert_eq!(snap.block_footer_events, 1);
    assert_eq!(snap.update_parent_events, 1);
    assert_eq!(snap.deshred_events, 1);
    assert_eq!(snap.entry_events, 1);
    assert_eq!(snap.block_meta_events, 1);
    assert_eq!(snap.heartbeat_events, 1);
    assert_eq!(snap.frames_parse_failed, 0);
    assert_eq!(snap.sequence_gaps_detected, 0);
    assert_eq!(snap.last_slot, 400_000_004);
    assert_eq!(snap.last_sequence, 8);

    let writer_stats = plugin.stats();
    assert_eq!(writer_stats.sent_count, 8);
    assert_eq!(writer_stats.dropped_count, 0);
    assert!(writer_stats.connected);

    reader.shutdown();
}

#[test]
fn auto_reconnect_on_broken_pipe_delivers_subsequent_frames() {
    let sock_path = unique_sock_path("reconnect");

    let reader_one = BlockingStreamReader::bind(&sock_path).expect("bind first blocking reader");
    let config = PluginConfig {
        uds_socket_path: sock_path.clone(),
        observer_validator_id: "validator-reconnect".to_string(),
        reconnect_backoff_ms: 10,
        ..PluginConfig::default()
    };
    let plugin = ValidatorStreamPlugin::with_config(config);

    let first_recv = thread::spawn(move || {
        let mut session = reader_one.accept().expect("accept first session");
        let frame = session
            .next_frame()
            .expect("read first frame")
            .expect("expected frame on first session");
        frame
    });

    assert!(plugin.notify_slot_status(500_001, Some(500_000), None, "Processed"));
    let frame_one = first_recv.join().expect("join first reader thread");
    assert_eq!(frame_one.sequence, 1);

    // First reader + session have now dropped, closing the peer socket and unlinking the path.
    // Dispatching while disconnected hits EPIPE / ENOENT and triggers reconnect mode.
    assert!(plugin.notify_slot_status(500_002, Some(500_001), None, "Confirmed"));
    thread::sleep(Duration::from_millis(35));

    let reader_two = BlockingStreamReader::bind(&sock_path).expect("re-bind second blocking reader");
    let second_recv = thread::spawn(move || {
        let mut session = reader_two.accept().expect("accept reconnected session");
        let f_a = session
            .next_frame()
            .expect("read frame after reconnect")
            .expect("expected frame after reconnect");
        let f_b = session
            .next_frame()
            .expect("read follow-up frame")
            .expect("expected follow-up frame");
        (f_a, f_b)
    });

    assert!(plugin.notify_slot_status(500_003, Some(500_002), None, "Rooted"));

    let (recovered, followup) = second_recv.join().expect("join second reader thread");
    assert_eq!(recovered.sequence, 2);
    assert_eq!(followup.sequence, 3);

    let stats = plugin.stats();
    assert!(stats.reconnect_count >= 1);
    assert_eq!(stats.sent_count, 3);
}

#[test]
fn bounded_ring_backpressure_drops_without_stalling_caller() {
    let sock_path = unique_sock_path("backpressure");
    let config = PluginConfig {
        uds_socket_path: sock_path,
        channel_buffer_capacity: 4,
        reconnect_backoff_ms: 40,
        ..PluginConfig::default()
    };

    let plugin = ValidatorStreamPlugin::with_config(config);
    let started = Instant::now();

    let mut accepted = 0usize;
    let mut dropped = 0usize;
    for idx in 0..250u64 {
        let ok = plugin.notify_deshred(
            600_000 + idx,
            format!("sig_burst_{idx}"),
            vec![1, 2, 3, 4],
            vec!["acct_program".to_string()],
            Some(idx),
        );
        if ok {
            accepted += 1;
        } else {
            dropped += 1;
        }
    }

    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(50),
        "dispatch loop blocked for {elapsed:?}"
    );
    assert!(accepted <= 5);
    assert!(dropped >= 245);
    assert_eq!(plugin.stats().dropped_count, dropped as u64);
}

#[tokio::test]
async fn sample_config_json_and_agave_geyser_trait_lifecycle() {
    let sample =
        PluginConfig::from_file("config/validator-stream.json").expect("parse sample config");
    assert_eq!(sample.uds_socket_path, DEFAULT_SOCKET_PATH);
    assert_eq!(sample.channel_buffer_capacity, 65_536);

    let sock_path = unique_sock_path("trait_lifecycle");
    let cfg_path = format!("/tmp/svs_cfg_{}.json", std::process::id());
    let cfg_body = serde_json::to_string_pretty(&PluginConfig {
        libpath: Some("./target/release/libsolana_validator_stream.dylib".to_string()),
        uds_socket_path: sock_path.clone(),
        observer_validator_id: "agave-c-abi-node".to_string(),
        reconnect_backoff_ms: 10,
        ..PluginConfig::default()
    })
    .expect("serialize temp config");
    std::fs::write(&cfg_path, cfg_body).expect("write temp config");

    let (mut reader, mut rx) =
        ValidatorStreamReader::bind(&sock_path, 16).expect("bind reader for C ABI plugin");

    // Exercise the dynamic C ABI entrypoint used by Agave's libloading loader.
    let mut boxed_plugin: Box<dyn GeyserPlugin> = unsafe { Box::from_raw(_create_plugin()) };
    assert_eq!(boxed_plugin.name(), "solana-validator-stream");

    boxed_plugin
        .on_load(&cfg_path, false)
        .expect("on_load with valid config file");

    assert!(boxed_plugin.entry_notifications_enabled());
    assert!(boxed_plugin.block_footer_notifications_enabled());
    assert!(boxed_plugin.deshred_transaction_notifications_enabled());
    assert!(!boxed_plugin.account_data_notifications_enabled());

    boxed_plugin
        .update_slot_status(777_001, Some(777_000), &SlotStatus::Processed)
        .expect("trait update_slot_status");
    boxed_plugin
        .update_bank_status(777_001, Some(777_000), &SlotStatus::Confirmed, 99)
        .expect("trait update_bank_status");

    let f1 = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout f1")
        .expect("closed f1");
    let f2 = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout f2")
        .expect("closed f2");

    assert_eq!(f1.observer_validator, "agave-c-abi-node");
    assert!(matches!(
        f1.payload,
        StreamPayload::SlotStatus { slot: 777_001, .. }
    ));
    assert!(matches!(
        f2.payload,
        StreamPayload::BankLifecycle {
            slot: 777_001,
            bank_id: 99,
            ..
        }
    ));

    boxed_plugin.on_unload();
    reader.shutdown();
    let _ = std::fs::remove_file(&cfg_path);
}

#[tokio::test]
async fn reader_tracks_sequence_gaps_and_malformed_ndjson() {
    let sock_path = unique_sock_path("gaps");
    let (mut reader, mut rx) =
        ValidatorStreamReader::bind(&sock_path, 16).expect("bind reader for gap test");

    let mut client = UnixStream::connect(&sock_path)
        .await
        .expect("connect raw UDS client");

    let f1 = ValidatorStreamFrame::new(
        1,
        "raw-peer",
        100,
        StreamPayload::Heartbeat {
            timestamp_nanos: 100,
            active_slots: 1,
        },
    );
    let f5 = ValidatorStreamFrame::new(
        5,
        "raw-peer",
        200,
        StreamPayload::Heartbeat {
            timestamp_nanos: 200,
            active_slots: 2,
        },
    );

    client
        .write_all(&f1.to_ndjson_bytes().unwrap())
        .await
        .unwrap();
    client
        .write_all(b"{\"not_valid_frame\": true\n")
        .await
        .unwrap();
    client
        .write_all(&f5.to_ndjson_bytes().unwrap())
        .await
        .unwrap();
    client.flush().await.unwrap();

    let got_one = timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let got_five = timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(got_one.sequence, 1);
    assert_eq!(got_five.sequence, 5);

    let snap = reader.counters().snapshot();
    assert_eq!(snap.frames_received_total, 2);
    assert_eq!(snap.frames_parse_failed, 1);
    assert_eq!(snap.sequence_gaps_detected, 3);

    reader.shutdown();
}
