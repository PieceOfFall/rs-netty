use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

/// Snapshot handle for per-connection counters.
///
/// Cloning this handle is cheap. Counter reads use relaxed atomics and are
/// intended for monitoring, not synchronization.
#[derive(Clone)]
pub struct ConnectionStats {
    inner: Arc<ConnectionStatsInner>,
}

struct ConnectionStatsInner {
    connected_at: Instant,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    frames_read: AtomicU64,
    frames_written: AtomicU64,
}

impl ConnectionStats {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(ConnectionStatsInner {
                connected_at: Instant::now(),
                bytes_read: AtomicU64::new(0),
                bytes_written: AtomicU64::new(0),
                frames_read: AtomicU64::new(0),
                frames_written: AtomicU64::new(0),
            }),
        }
    }

    /// Instant when the connection stats were created.
    pub fn connected_at(&self) -> Instant {
        self.inner.connected_at
    }

    /// Total bytes read from the socket.
    pub fn bytes_read(&self) -> u64 {
        self.inner.bytes_read.load(Ordering::Relaxed)
    }

    /// Total bytes written to the socket.
    pub fn bytes_written(&self) -> u64 {
        self.inner.bytes_written.load(Ordering::Relaxed)
    }

    /// Total decoded frames.
    pub fn frames_read(&self) -> u64 {
        self.inner.frames_read.load(Ordering::Relaxed)
    }

    /// Total encoded frames.
    pub fn frames_written(&self) -> u64 {
        self.inner.frames_written.load(Ordering::Relaxed)
    }

    pub(crate) fn add_bytes_read(&self, value: usize) {
        self.inner
            .bytes_read
            .fetch_add(value as u64, Ordering::Relaxed);
    }

    pub(crate) fn add_bytes_written(&self, value: usize) {
        self.inner
            .bytes_written
            .fetch_add(value as u64, Ordering::Relaxed);
    }

    pub(crate) fn add_frame_read(&self) {
        self.inner.frames_read.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_frame_written(&self) {
        self.inner.frames_written.fetch_add(1, Ordering::Relaxed);
    }
}
