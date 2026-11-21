use crate::events::{StreamPayload, ValidatorStreamFrame};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader as StdBufReader};
use std::os::unix::net::{UnixListener as StdUnixListener, UnixStream as StdUnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, BufReader as TokioBufReader};
use tokio::net::UnixListener as TokioUnixListener;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

/// Errors produced by [`ValidatorStreamReader`] or [`BlockingStreamReader`].
#[derive(Debug, Error)]
pub enum ReaderError {
    #[error("failed to bind UDS listener at {path}: {source}")]
    Bind {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("UDS I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid NDJSON frame: {0}")]
    Parse(#[from] serde_json::Error),
}

/// Lock-free atomic telemetry counters maintained by UDS frame consumers.
#[derive(Debug, Default)]
pub struct StreamTelemetryCounters {
    pub frames_received_total: AtomicU64,
    pub slot_events: AtomicU64,
    pub bank_events: AtomicU64,
    pub block_footer_events: AtomicU64,
    pub update_parent_events: AtomicU64,
    pub deshred_events: AtomicU64,
    pub entry_events: AtomicU64,
    pub block_meta_events: AtomicU64,
    pub heartbeat_events: AtomicU64,
    pub frames_parse_failed: AtomicU64,
    pub sequence_gaps_detected: AtomicU64,
    pub frames_dropped_consumer: AtomicU64,
    pub last_slot: AtomicU64,
    pub last_sequence: AtomicU64,
}

/// Serializable point-in-time view of [`StreamTelemetryCounters`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetrySnapshot {
    pub frames_received_total: u64,
    pub slot_events: u64,
    pub bank_events: u64,
    pub block_footer_events: u64,
    pub update_parent_events: u64,
    pub deshred_events: u64,
    pub entry_events: u64,
    pub block_meta_events: u64,
    pub heartbeat_events: u64,
    pub frames_parse_failed: u64,
    pub sequence_gaps_detected: u64,
    pub frames_dropped_consumer: u64,
    pub last_slot: u64,
    pub last_sequence: u64,
}

impl StreamTelemetryCounters {
    /// Records a decoded frame and updates sequence-gap and per-variant counters.
    pub fn record_frame(&self, frame: &ValidatorStreamFrame) {
        self.frames_received_total.fetch_add(1, Ordering::Relaxed);

        let prev_seq = self.last_sequence.swap(frame.sequence, Ordering::Relaxed);
        if prev_seq > 0 && frame.sequence > prev_seq + 1 {
            self.sequence_gaps_detected
                .fetch_add(frame.sequence - (prev_seq + 1), Ordering::Relaxed);
        }

        if let Some(slot) = frame.payload.slot() {
            self.last_slot.store(slot, Ordering::Relaxed);
        }

        match &frame.payload {
            StreamPayload::SlotStatus { .. } => {
                self.slot_events.fetch_add(1, Ordering::Relaxed);
            }
            StreamPayload::BankLifecycle { .. } => {
                self.bank_events.fetch_add(1, Ordering::Relaxed);
            }
            StreamPayload::BlockFooter { .. } => {
                self.block_footer_events.fetch_add(1, Ordering::Relaxed);
            }
            StreamPayload::UpdateParent { .. } => {
                self.update_parent_events.fetch_add(1, Ordering::Relaxed);
            }
            StreamPayload::DeshredTx { .. } => {
                self.deshred_events.fetch_add(1, Ordering::Relaxed);
            }
            StreamPayload::Entry { .. } => {
                self.entry_events.fetch_add(1, Ordering::Relaxed);
            }
            StreamPayload::BlockMeta { .. } => {
                self.block_meta_events.fetch_add(1, Ordering::Relaxed);
            }
            StreamPayload::Heartbeat { .. } => {
                self.heartbeat_events.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Captures a consistent snapshot of all counters.
    pub fn snapshot(&self) -> TelemetrySnapshot {
        TelemetrySnapshot {
            frames_received_total: self.frames_received_total.load(Ordering::Relaxed),
            slot_events: self.slot_events.load(Ordering::Relaxed),
            bank_events: self.bank_events.load(Ordering::Relaxed),
            block_footer_events: self.block_footer_events.load(Ordering::Relaxed),
            update_parent_events: self.update_parent_events.load(Ordering::Relaxed),
            deshred_events: self.deshred_events.load(Ordering::Relaxed),
            entry_events: self.entry_events.load(Ordering::Relaxed),
            block_meta_events: self.block_meta_events.load(Ordering::Relaxed),
            heartbeat_events: self.heartbeat_events.load(Ordering::Relaxed),
            frames_parse_failed: self.frames_parse_failed.load(Ordering::Relaxed),
            sequence_gaps_detected: self.sequence_gaps_detected.load(Ordering::Relaxed),
            frames_dropped_consumer: self.frames_dropped_consumer.load(Ordering::Relaxed),
            last_slot: self.last_slot.load(Ordering::Relaxed),
            last_sequence: self.last_sequence.load(Ordering::Relaxed),
        }
    }
}

/// Async Tokio Unix Domain Socket listener that accepts validator plugin connections
/// and streams decoded [`ValidatorStreamFrame`] items into an `mpsc::Receiver`.
pub struct ValidatorStreamReader {
    socket_path: String,
    counters: Arc<StreamTelemetryCounters>,
    stop_tx: Option<oneshot::Sender<()>>,
}

impl ValidatorStreamReader {
    /// Binds the given Unix Domain Socket path (removing any stale socket file first)
    /// and spawns a background reader task.
    pub fn bind(
        socket_path: impl Into<String>,
        channel_capacity: usize,
    ) -> Result<(Self, mpsc::Receiver<ValidatorStreamFrame>), ReaderError> {
        let path = socket_path.into();
        if Path::new(&path).exists() {
            let _ = std::fs::remove_file(&path);
        }

        let listener = TokioUnixListener::bind(&path).map_err(|source| ReaderError::Bind {
            path: path.clone(),
            source,
        })?;

        let (frame_tx, frame_rx) = mpsc::channel(channel_capacity.max(1));
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let counters = Arc::new(StreamTelemetryCounters::default());
        let task_counters = Arc::clone(&counters);
        let task_path = path.clone();

        tokio::spawn(async move {
            info!("ValidatorStreamReader listening on UDS {}", task_path);
            loop {
                tokio::select! {
                    _ = &mut stop_rx => {
                        debug!("ValidatorStreamReader shutdown signal received for {}", task_path);
                        break;
                    }
                    accept_res = listener.accept() => {
                        match accept_res {
                            Ok((stream, _addr)) => {
                                info!("accepted validator plugin UDS connection on {}", task_path);
                                let mut reader = TokioBufReader::new(stream);
                                let mut line = String::new();

                                loop {
                                    line.clear();
                                    tokio::select! {
                                        _ = &mut stop_rx => {
                                            return;
                                        }
                                        read_res = reader.read_line(&mut line) => {
                                            match read_res {
                                                Ok(0) => break,
                                                Ok(_) => {
                                                    if line.trim().is_empty() {
                                                        continue;
                                                    }
                                                    match ValidatorStreamFrame::from_ndjson_line(&line) {
                                                        Ok(frame) => {
                                                            task_counters.record_frame(&frame);
                                                            if frame_tx.send(frame).await.is_err() {
                                                                task_counters
                                                                    .frames_dropped_consumer
                                                                    .fetch_add(1, Ordering::Relaxed);
                                                                return;
                                                            }
                                                        }
                                                        Err(err) => {
                                                            task_counters
                                                                .frames_parse_failed
                                                                .fetch_add(1, Ordering::Relaxed);
                                                            warn!("malformed NDJSON frame on {}: {}", task_path, err);
                                                        }
                                                    }
                                                }
                                                Err(err) => {
                                                    warn!("UDS read error on {}: {}", task_path, err);
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                warn!("UDS accept error on {}: {}", task_path, err);
                                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                            }
                        }
                    }
                }
            }
        });

        Ok((
            Self {
                socket_path: path,
                counters,
                stop_tx: Some(stop_tx),
            },
            frame_rx,
        ))
    }

    /// Returns the shared telemetry counter handle.
    pub fn counters(&self) -> Arc<StreamTelemetryCounters> {
        Arc::clone(&self.counters)
    }

    /// Returns the bound Unix Domain Socket path.
    pub fn socket_path(&self) -> &str {
        &self.socket_path
    }

    /// Stops the background listener task and unlinks the UDS socket file.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
            if Path::new(&self.socket_path).exists() {
                let _ = std::fs::remove_file(&self.socket_path);
            }
        }
    }
}

impl Drop for ValidatorStreamReader {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Blocking stdlib UDS listener for lightweight CLI tools and single-threaded consumers.
pub struct BlockingStreamReader {
    socket_path: String,
    listener: StdUnixListener,
    counters: Arc<StreamTelemetryCounters>,
}

impl BlockingStreamReader {
    /// Binds a blocking `std::os::unix::net::UnixListener` at `socket_path`.
    pub fn bind(socket_path: impl Into<String>) -> Result<Self, ReaderError> {
        let path = socket_path.into();
        if Path::new(&path).exists() {
            let _ = std::fs::remove_file(&path);
        }
        let listener = StdUnixListener::bind(&path).map_err(|source| ReaderError::Bind {
            path: path.clone(),
            source,
        })?;
        Ok(Self {
            socket_path: path,
            listener,
            counters: Arc::new(StreamTelemetryCounters::default()),
        })
    }

    /// Accepts the next incoming validator plugin connection.
    pub fn accept(&self) -> Result<BlockingStreamSession, ReaderError> {
        let (stream, _addr) = self.listener.accept()?;
        Ok(BlockingStreamSession {
            reader: StdBufReader::new(stream),
            line_buf: String::new(),
            counters: Arc::clone(&self.counters),
        })
    }

    /// Returns the shared telemetry counter handle.
    pub fn counters(&self) -> Arc<StreamTelemetryCounters> {
        Arc::clone(&self.counters)
    }

    /// Returns the bound Unix Domain Socket path.
    pub fn socket_path(&self) -> &str {
        &self.socket_path
    }
}

impl Drop for BlockingStreamReader {
    fn drop(&mut self) {
        if Path::new(&self.socket_path).exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }
}

/// Active blocking NDJSON session returned by [`BlockingStreamReader::accept`].
pub struct BlockingStreamSession {
    reader: StdBufReader<StdUnixStream>,
    line_buf: String,
    counters: Arc<StreamTelemetryCounters>,
}

impl BlockingStreamSession {
    /// Reads and parses the next frame, returning `Ok(None)` when the peer closes the stream.
    pub fn next_frame(&mut self) -> Result<Option<ValidatorStreamFrame>, ReaderError> {
        loop {
            self.line_buf.clear();
            let bytes_read = self.reader.read_line(&mut self.line_buf)?;
            if bytes_read == 0 {
                return Ok(None);
            }
            if self.line_buf.trim().is_empty() {
                continue;
            }
            match ValidatorStreamFrame::from_ndjson_line(&self.line_buf) {
                Ok(frame) => {
                    self.counters.record_frame(&frame);
                    return Ok(Some(frame));
                }
                Err(err) => {
                    self.counters
                        .frames_parse_failed
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(ReaderError::Parse(err));
                }
            }
        }
    }
}
