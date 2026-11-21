use crate::config::DEFAULT_RECONNECT_BACKOFF_MS;
use crate::events::ValidatorStreamFrame;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Point-in-time snapshot of [`StreamIpcWriter`] health and backpressure counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpcStats {
    pub sent_count: u64,
    pub dropped_count: u64,
    pub reconnect_count: u64,
    pub connected: bool,
}

#[derive(Debug, Default)]
struct SharedWriterStats {
    sent_count: AtomicU64,
    dropped_count: AtomicU64,
    reconnect_count: AtomicU64,
    connected: AtomicBool,
    shutdown: AtomicBool,
}

#[derive(Debug)]
struct WriterInner {
    sender: SyncSender<ValidatorStreamFrame>,
    stats: Arc<SharedWriterStats>,
}

impl Drop for WriterInner {
    fn drop(&mut self) {
        self.stats.shutdown.store(true, Ordering::SeqCst);
    }
}

/// Non-blocking IPC streamer shipping validator Geyser callbacks over a Unix Domain Socket
/// as newline-delimited JSON (NDJSON).
///
/// Calls to [`StreamIpcWriter::dispatch`] use a bounded channel (`try_send`) so Agave
/// replay and banking threads never stall even if the downstream consumer hangs or restarts.
#[derive(Clone, Debug)]
pub struct StreamIpcWriter {
    inner: Arc<WriterInner>,
}

impl StreamIpcWriter {
    /// Spawns a background UDS writer worker with the default reconnect backoff.
    pub fn new(socket_path: impl Into<String>, capacity: usize) -> Self {
        Self::with_backoff(socket_path, capacity, DEFAULT_RECONNECT_BACKOFF_MS)
    }

    /// Spawns a background UDS writer worker with a custom reconnect backoff in milliseconds.
    pub fn with_backoff(
        socket_path: impl Into<String>,
        capacity: usize,
        reconnect_backoff_ms: u64,
    ) -> Self {
        let bounded_cap = capacity.max(1);
        let (tx, rx) = sync_channel::<ValidatorStreamFrame>(bounded_cap);
        let stats = Arc::new(SharedWriterStats::default());
        let worker_stats = Arc::clone(&stats);
        let path = socket_path.into();

        thread::Builder::new()
            .name("solana-validator-stream-ipc".to_string())
            .spawn(move || {
                Self::worker_loop(path, reconnect_backoff_ms, rx, worker_stats);
            })
            .expect("failed to spawn solana-validator-stream-ipc worker thread");

        Self {
            inner: Arc::new(WriterInner { sender: tx, stats }),
        }
    }

    /// Queues a frame without blocking the caller thread.
    ///
    /// Returns `true` if queued into the bounded buffer, or `false` if dropped due to
    /// backpressure or worker shutdown.
    pub fn dispatch(&self, frame: ValidatorStreamFrame) -> bool {
        match self.inner.sender.try_send(frame) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.inner
                    .stats
                    .dropped_count
                    .fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// Total frames flushed across the Unix Domain Socket.
    pub fn sent_count(&self) -> u64 {
        self.inner.stats.sent_count.load(Ordering::Relaxed)
    }

    /// Total frames dropped due to bounded channel saturation.
    pub fn dropped_count(&self) -> u64 {
        self.inner.stats.dropped_count.load(Ordering::Relaxed)
    }

    /// Number of times the writer re-established a UDS connection after an initial attach.
    pub fn reconnect_count(&self) -> u64 {
        self.inner.stats.reconnect_count.load(Ordering::Relaxed)
    }

    /// Whether the background worker currently holds an active UDS stream.
    pub fn is_connected(&self) -> bool {
        self.inner.stats.connected.load(Ordering::SeqCst)
    }

    /// Captures all writer metrics in a single struct.
    pub fn stats(&self) -> IpcStats {
        IpcStats {
            sent_count: self.sent_count(),
            dropped_count: self.dropped_count(),
            reconnect_count: self.reconnect_count(),
            connected: self.is_connected(),
        }
    }

    fn worker_loop(
        socket_path: String,
        reconnect_backoff_ms: u64,
        receiver: Receiver<ValidatorStreamFrame>,
        stats: Arc<SharedWriterStats>,
    ) {
        let mut stream_opt: Option<UnixStream> = None;
        let mut had_connection = false;
        let backoff = Duration::from_millis(reconnect_backoff_ms.max(1));

        while let Ok(frame) = receiver.recv() {
            let bytes = match frame.to_ndjson_bytes() {
                Ok(buf) => buf,
                Err(e) => {
                    error!(
                        "failed to serialize validator frame seq={}: {}",
                        frame.sequence, e
                    );
                    continue;
                }
            };

            loop {
                if stream_opt.is_none() {
                    match UnixStream::connect(&socket_path) {
                        Ok(stream) => {
                            let _ = stream.set_nonblocking(false);
                            if had_connection {
                                stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                            }
                            had_connection = true;
                            stats.connected.store(true, Ordering::SeqCst);
                            info!("validator stream writer connected to UDS {}", socket_path);
                            stream_opt = Some(stream);
                        }
                        Err(e) => {
                            stats.connected.store(false, Ordering::SeqCst);
                            if stats.shutdown.load(Ordering::SeqCst) {
                                return;
                            }
                            debug!("waiting for UDS listener at {}: {}", socket_path, e);
                            thread::sleep(backoff);
                            continue;
                        }
                    }
                }

                if let Some(ref mut stream) = stream_opt {
                    match stream.write_all(&bytes).and_then(|()| stream.flush()) {
                        Ok(()) => {
                            stats.sent_count.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        Err(e) => {
                            warn!("UDS write error on {}: {}, reconnecting", socket_path, e);
                            stream_opt = None;
                            stats.connected.store(false, Ordering::SeqCst);
                            if stats.shutdown.load(Ordering::SeqCst) {
                                return;
                            }
                            thread::sleep(backoff);
                        }
                    }
                }
            }
        }

        stats.connected.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::StreamPayload;

    #[test]
    fn non_blocking_dispatch_increments_drop_counter_when_full() {
        let sock = format!(
            "/tmp/test_validator_stream_absent_{}.sock",
            std::process::id()
        );
        let _ = std::fs::remove_file(&sock);

        let writer = StreamIpcWriter::with_backoff(sock, 2, 5);
        for seq in 1..=32 {
            let frame = ValidatorStreamFrame::new(
                seq,
                "validator-unit",
                1_760_000_000_000_000_000,
                StreamPayload::Heartbeat {
                    timestamp_nanos: 1_760_000_000_000_000_000,
                    active_slots: 1,
                },
            );
            let _ = writer.dispatch(frame);
        }

        assert!(writer.dropped_count() >= 28);
        assert_eq!(writer.sent_count(), 0);
        assert!(!writer.is_connected());
    }
}
