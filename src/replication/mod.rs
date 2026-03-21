pub mod replica_client;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use smallvec::SmallVec;
use tokio::sync::{broadcast, mpsc};

const REPLICATION_INGRESS_CAP: usize = 16384;

const DEFAULT_BACKLOG_BYTES: usize = 64 * 1024 * 1024;
const REPLICATION_BROADCAST_CAP: usize = 8192;

#[derive(Debug, Clone)]
pub struct ReplicationEvent {
    pub start_offset: u64,
    pub end_offset: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone)]
struct BacklogEntry {
    start_offset: u64,
    end_offset: u64,
    payload: Bytes,
}

#[derive(Debug)]
struct BacklogState {
    entries: VecDeque<BacklogEntry>,
    total_bytes: usize,
    max_bytes: usize,
}

#[derive(Debug)]
struct QueuedWrite {
    argv: SmallVec<[Bytes; 8]>,
}

#[derive(Debug)]
pub struct ReplicationSubscription {
    pub history: Vec<Bytes>,
    pub receiver: broadcast::Receiver<ReplicationEvent>,
}

#[derive(Debug, Clone)]
pub enum PsyncDecision {
    Continue {
        start_offset: u64,
    },
    FullResync {
        replid: String,
        current_offset: u64,
    },
}

#[derive(Debug)]
pub struct ReplicationManager {
    replid: String,
    offset: AtomicU64,
    last_ack_offset: AtomicU64,
    last_ack_ms: AtomicU64,
    ack_count: AtomicU64,
    capture_writes: AtomicBool,
    ingress_started: AtomicBool,
    dropped_ingress_writes: AtomicU64,
    ingress_tx: mpsc::Sender<QueuedWrite>,
    ingress_rx: Mutex<Option<mpsc::Receiver<QueuedWrite>>>,
    backlog: Mutex<BacklogState>,
    tx: broadcast::Sender<ReplicationEvent>,
}

impl ReplicationManager {
    pub fn new() -> Self {
        Self::with_backlog_bytes(DEFAULT_BACKLOG_BYTES)
    }

    pub fn with_backlog_bytes(backlog_bytes: usize) -> Self {
        let (tx, _) = broadcast::channel(REPLICATION_BROADCAST_CAP);
        let (ingress_tx, ingress_rx) = mpsc::channel(REPLICATION_INGRESS_CAP);
        Self {
            replid: generate_replid(),
            offset: AtomicU64::new(0),
            last_ack_offset: AtomicU64::new(0),
            last_ack_ms: AtomicU64::new(0),
            ack_count: AtomicU64::new(0),
            capture_writes: AtomicBool::new(false),
            ingress_started: AtomicBool::new(false),
            dropped_ingress_writes: AtomicU64::new(0),
            ingress_tx,
            ingress_rx: Mutex::new(Some(ingress_rx)),
            backlog: Mutex::new(BacklogState {
                entries: VecDeque::new(),
                total_bytes: 0,
                max_bytes: backlog_bytes.max(1024),
            }),
            tx,
        }
    }

    pub fn replid(&self) -> &str {
        &self.replid
    }

    pub fn current_offset(&self) -> u64 {
        self.offset.load(Ordering::Acquire)
    }

    pub fn should_capture_writes(&self) -> bool {
        self.capture_writes.load(Ordering::Relaxed)
    }

    pub fn dropped_ingress_writes(&self) -> u64 {
        self.dropped_ingress_writes.load(Ordering::Relaxed)
    }

    pub fn record_ack(&self, ack_offset: u64) {
        self.last_ack_offset.store(ack_offset, Ordering::Release);
        self.last_ack_ms.store(now_ms(), Ordering::Release);
        self.ack_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn replication_metrics(&self) -> ReplicationMetrics {
        let master_offset = self.current_offset();
        let last_ack_offset = self.last_ack_offset.load(Ordering::Acquire);
        let last_ack_ms = self.last_ack_ms.load(Ordering::Acquire);
        let now = now_ms();
        ReplicationMetrics {
            replid: self.replid.clone(),
            master_offset,
            last_ack_offset,
            lag_bytes: master_offset.saturating_sub(last_ack_offset),
            last_ack_age_ms: if last_ack_ms == 0 {
                None
            } else {
                Some(now.saturating_sub(last_ack_ms))
            },
            ack_count: self.ack_count.load(Ordering::Relaxed),
            dropped_ingress_writes: self.dropped_ingress_writes(),
            capture_writes: self.should_capture_writes(),
        }
    }

    pub fn enable_capture_writes(&self) {
        self.capture_writes.store(true, Ordering::Relaxed);
    }

    pub fn start_ingress_worker(self: &Arc<Self>) {
        if self.ingress_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(mut rx) = self
            .ingress_rx
            .lock()
            .expect("replication ingress poisoned")
            .take()
        else {
            return;
        };
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            while let Some(write) = rx.recv().await {
                if !manager.should_capture_writes() {
                    continue;
                }
                let payload = encode_queued_write(&write.argv);
                manager.append_command(payload);
            }
        });
    }

    pub fn try_enqueue_write_argv(&self, argv: &[&[u8]]) {
        if !self.should_capture_writes() {
            return;
        }
        let mut owned = SmallVec::<[Bytes; 8]>::with_capacity(argv.len());
        for arg in argv {
            owned.push(Bytes::copy_from_slice(arg));
        }
        let write = QueuedWrite { argv: owned };
        if let Err(err) = self.ingress_tx.try_send(write) {
            if matches!(err, mpsc::error::TrySendError::Full(_)) {
                self.dropped_ingress_writes.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn append_command(&self, payload: Bytes) {
        if payload.is_empty() || !self.should_capture_writes() {
            return;
        }
        let len = payload.len() as u64;
        let end_offset = self.offset.fetch_add(len, Ordering::AcqRel) + len;
        let start_offset = end_offset.saturating_sub(len).saturating_add(1);
        let event = ReplicationEvent {
            start_offset,
            end_offset,
            payload,
        };
        {
            let mut backlog = self.backlog.lock().expect("replication backlog poisoned");
            backlog.total_bytes = backlog.total_bytes.saturating_add(event.payload.len());
            backlog.entries.push_back(BacklogEntry {
                start_offset: event.start_offset,
                end_offset: event.end_offset,
                payload: event.payload.clone(),
            });
            while backlog.total_bytes > backlog.max_bytes {
                if let Some(front) = backlog.entries.pop_front() {
                    backlog.total_bytes = backlog.total_bytes.saturating_sub(front.payload.len());
                } else {
                    break;
                }
            }
        }
        let _ = self.tx.send(event);
    }

    pub fn negotiate_psync(&self, requested_replid: &str, requested_offset: i64) -> PsyncDecision {
        self.enable_capture_writes();
        if requested_offset < 0
            || requested_replid == "?"
            || !requested_replid.eq_ignore_ascii_case(self.replid())
        {
            return PsyncDecision::FullResync {
                replid: self.replid.clone(),
                current_offset: self.current_offset(),
            };
        }
        let req_offset = requested_offset as u64;
        if self.can_partial_resync(req_offset) {
            PsyncDecision::Continue {
                start_offset: req_offset.saturating_add(1),
            }
        } else {
            PsyncDecision::FullResync {
                replid: self.replid.clone(),
                current_offset: self.current_offset(),
            }
        }
    }

    pub fn subscribe_from(&self, start_offset: u64) -> ReplicationSubscription {
        self.enable_capture_writes();
        let receiver = self.tx.subscribe();
        let history = {
            let backlog = self.backlog.lock().expect("replication backlog poisoned");
            backlog
                .entries
                .iter()
                .filter(|entry| entry.end_offset >= start_offset)
                .map(|entry| entry.payload.clone())
                .collect()
        };
        ReplicationSubscription { history, receiver }
    }

    fn can_partial_resync(&self, requested_offset: u64) -> bool {
        let backlog = self.backlog.lock().expect("replication backlog poisoned");
        if backlog.entries.is_empty() {
            return requested_offset == self.current_offset();
        }
        let first = backlog.entries.front().map(|v| v.start_offset).unwrap_or(0);
        let current = self.current_offset();
        requested_offset >= first.saturating_sub(1) && requested_offset <= current
    }
}

fn generate_replid() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let pid = std::process::id() as u128;
    let mut seed = now.as_nanos() ^ (pid << 32);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0_u8; 40];
    for item in &mut out {
        seed ^= seed << 7;
        seed ^= seed >> 9;
        seed ^= seed << 8;
        *item = HEX[(seed & 0x0f) as usize];
    }
    String::from_utf8(out.to_vec()).unwrap_or_else(|_| "0000000000000000000000000000000000000000".to_string())
}

#[derive(Debug, Clone)]
pub struct ReplicationMetrics {
    pub replid: String,
    pub master_offset: u64,
    pub last_ack_offset: u64,
    pub lag_bytes: u64,
    pub last_ack_age_ms: Option<u64>,
    pub ack_count: u64,
    pub dropped_ingress_writes: u64,
    pub capture_writes: bool,
}

fn encode_queued_write(argv: &[Bytes]) -> Bytes {
    let mut out = Vec::with_capacity(64);
    out.push(b'*');
    out.extend_from_slice(argv.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for arg in argv {
        out.push(b'$');
        out.extend_from_slice(arg.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    Bytes::from(out)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
