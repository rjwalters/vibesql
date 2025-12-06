// ============================================================================
// Persistence Engine
// ============================================================================
//
// Async persistence engine that receives WAL entries from the main thread
// and writes them to disk in batches using a dedicated background thread.
//
// ## Architecture
//
// ```text
// ┌─────────────────┐     bounded channel      ┌──────────────────┐
// │  Main Thread    │ ──────────────────────▶  │  WAL Writer      │
// │  (DB ops)       │   WalEntry messages      │  Thread          │
// └─────────────────┘                          └────────┬─────────┘
//                                                       │
//                                                       ▼
//                                              ┌──────────────────┐
//                                              │  WAL File        │
//                                              └──────────────────┘
// ```
//
// ## Batching Strategy
//
// Entries are batched before writing to reduce fsync overhead:
// - Time-based: flush every N ms (default 50ms)
// - Count-based: flush every M entries (default 1000)
// - Whichever threshold is reached first triggers a flush
//
// ## WASM Compatibility
//
// WASM builds don't have threads, so the engine uses a buffered
// no-op implementation that stores entries in memory.

use super::durability::{DurabilityConfig, DurabilityMode};
use super::entry::{Lsn, WalEntry, WalOp};
use super::writer::WalWriter;
use crate::StorageError;

/// Default channel capacity (number of entries)
pub const DEFAULT_CHANNEL_CAPACITY: usize = 10_000;

/// Default time-based flush threshold in milliseconds
pub const DEFAULT_FLUSH_INTERVAL_MS: u64 = 50;

/// Default count-based flush threshold
pub const DEFAULT_FLUSH_COUNT: usize = 1000;

/// Configuration for the persistence engine
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// Maximum number of entries in the channel before backpressure
    pub channel_capacity: usize,
    /// Time-based flush interval in milliseconds
    pub flush_interval_ms: u64,
    /// Count-based flush threshold
    pub flush_count: usize,
    /// Durability mode controlling WAL behavior
    pub durability_mode: DurabilityMode,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval_ms: DEFAULT_FLUSH_INTERVAL_MS,
            flush_count: DEFAULT_FLUSH_COUNT,
            durability_mode: DurabilityMode::Lazy,
        }
    }
}

impl PersistenceConfig {
    /// Create configuration from a DurabilityConfig
    pub fn from_durability_config(config: &DurabilityConfig) -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval_ms: config.wal_flush_interval_ms,
            flush_count: config.wal_flush_batch_size,
            durability_mode: config.mode,
        }
    }

    /// Create configuration for volatile mode (no persistence)
    pub fn volatile() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval_ms: 0,
            flush_count: usize::MAX,
            durability_mode: DurabilityMode::Volatile,
        }
    }

    /// Create configuration for lazy mode (batched writes)
    pub fn lazy() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval_ms: DEFAULT_FLUSH_INTERVAL_MS,
            flush_count: DEFAULT_FLUSH_COUNT,
            durability_mode: DurabilityMode::Lazy,
        }
    }

    /// Create configuration for durable mode (sync on commit)
    pub fn durable() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval_ms: 0,
            flush_count: 1,
            durability_mode: DurabilityMode::Durable,
        }
    }

    /// Create configuration for paranoid mode (sync on every op)
    pub fn paranoid() -> Self {
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval_ms: 0,
            flush_count: 1,
            durability_mode: DurabilityMode::Paranoid,
        }
    }

    /// Returns true if this configuration writes to WAL
    pub fn writes_wal(&self) -> bool {
        self.durability_mode.writes_wal()
    }

    /// Returns true if this configuration syncs on commit
    pub fn sync_on_commit(&self) -> bool {
        self.durability_mode.sync_on_commit()
    }

    /// Returns true if this configuration syncs on every operation
    pub fn sync_on_every_op(&self) -> bool {
        self.durability_mode.sync_on_every_op()
    }
}

/// Message types sent to the WAL writer thread
#[derive(Debug)]
pub enum WalMessage {
    /// A WAL entry to persist
    Entry(WalEntry),
    /// Force an immediate flush
    Flush,
    /// Flush and send completion notification
    FlushAndNotify(FlushNotifier),
    /// Shutdown the writer thread
    Shutdown,
}

// ============================================================================
// Native (non-WASM) Implementation
// ============================================================================

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::fs::OpenOptions;
    use std::io::{BufWriter, Seek, Write};
    use std::path::Path;
    use std::sync::mpsc::{self, RecvTimeoutError, SyncSender};
    use std::sync::Arc;
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use parking_lot::Mutex;

    use super::*;

    /// Flush completion notifier using a condition variable
    #[derive(Clone)]
    pub struct FlushNotifier {
        completed: Arc<(parking_lot::Mutex<bool>, parking_lot::Condvar)>,
    }

    impl std::fmt::Debug for FlushNotifier {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FlushNotifier").finish()
        }
    }

    impl FlushNotifier {
        pub fn new() -> Self {
            Self { completed: Arc::new((parking_lot::Mutex::new(false), parking_lot::Condvar::new())) }
        }

        /// Signal that the flush is complete
        pub fn notify(&self) {
            let (lock, cvar) = &*self.completed;
            let mut completed = lock.lock();
            *completed = true;
            cvar.notify_all();
        }

        /// Wait for the flush to complete
        pub fn wait(&self) {
            let (lock, cvar) = &*self.completed;
            let mut completed = lock.lock();
            while !*completed {
                cvar.wait(&mut completed);
            }
        }

        /// Wait for the flush to complete with a timeout
        pub fn wait_timeout(&self, timeout: Duration) -> bool {
            let (lock, cvar) = &*self.completed;
            let mut completed = lock.lock();
            while !*completed {
                let result = cvar.wait_for(&mut completed, timeout);
                if result.timed_out() {
                    return false;
                }
            }
            true
        }
    }

    impl Default for FlushNotifier {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Statistics about the persistence engine
    #[derive(Debug, Clone, Default)]
    pub struct PersistenceStats {
        /// Total entries sent to the engine
        pub entries_sent: u64,
        /// Total entries written to disk
        pub entries_written: u64,
        /// Total batches written
        pub batches_written: u64,
        /// Total bytes written
        pub bytes_written: u64,
        /// Number of time-based flushes
        pub time_flushes: u64,
        /// Number of count-based flushes
        pub count_flushes: u64,
        /// Number of explicit flushes
        pub explicit_flushes: u64,
        /// Number of entries discarded in volatile mode
        pub volatile_discards: u64,
        /// Number of sync-on-commit flushes (Durable/Paranoid mode)
        pub commit_syncs: u64,
        /// Number of sync-on-op flushes (Paranoid mode only)
        pub op_syncs: u64,
        /// Average flush latency in microseconds
        pub avg_flush_latency_us: u64,
        /// Maximum flush latency in microseconds
        pub max_flush_latency_us: u64,
        /// Number of pending entries in channel (snapshot)
        pub pending_entries: u64,
        /// Total flush latency in microseconds (used for avg calculation)
        total_flush_latency_us: u64,
        /// Number of flush latency samples (used for avg calculation)
        flush_latency_samples: u64,
    }

    impl PersistenceStats {
        /// Record a flush latency measurement
        pub fn record_flush_latency(&mut self, duration: Duration) {
            let latency_us = duration.as_micros() as u64;

            // Update max
            if latency_us > self.max_flush_latency_us {
                self.max_flush_latency_us = latency_us;
            }

            // Update running average
            self.total_flush_latency_us += latency_us;
            self.flush_latency_samples += 1;
            self.avg_flush_latency_us =
                self.total_flush_latency_us / self.flush_latency_samples;
        }
    }

    /// Persistence engine that manages async WAL writing
    pub struct PersistenceEngine {
        /// Sender for WAL messages
        sender: SyncSender<WalMessage>,
        /// Handle to the writer thread
        handle: Option<JoinHandle<()>>,
        /// Shared statistics
        stats: Arc<Mutex<PersistenceStats>>,
        /// Next LSN to assign
        next_lsn: Arc<Mutex<Lsn>>,
        /// Whether the engine has been shut down
        shutdown: Arc<Mutex<bool>>,
        /// Configuration (stored for durability mode checks)
        config: PersistenceConfig,
    }

    impl std::fmt::Debug for PersistenceEngine {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PersistenceEngine")
                .field("next_lsn", &*self.next_lsn.lock())
                .field("shutdown", &*self.shutdown.lock())
                .field("stats", &*self.stats.lock())
                .finish_non_exhaustive()
        }
    }

    impl PersistenceEngine {
        /// Create a new persistence engine writing to a file
        pub fn new<P: AsRef<Path>>(
            path: P,
            config: PersistenceConfig,
        ) -> Result<Self, StorageError> {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path.as_ref())
                .map_err(|e| StorageError::IoError(e.to_string()))?;

            let writer = BufWriter::new(file);
            Self::with_writer(writer, config)
        }

        /// Create a new persistence engine with a custom writer
        pub fn with_writer<W: Write + Seek + Send + 'static>(
            writer: W,
            config: PersistenceConfig,
        ) -> Result<Self, StorageError> {
            let (sender, receiver) = mpsc::sync_channel(config.channel_capacity);
            let stats = Arc::new(Mutex::new(PersistenceStats::default()));
            let next_lsn = Arc::new(Mutex::new(1u64));
            let shutdown = Arc::new(Mutex::new(false));

            let stats_clone = stats.clone();
            let flush_interval = Duration::from_millis(config.flush_interval_ms);
            let flush_count = config.flush_count;

            let handle = thread::spawn(move || {
                Self::writer_loop(writer, receiver, stats_clone, flush_interval, flush_count);
            });

            Ok(Self { sender, handle: Some(handle), stats, next_lsn, shutdown, config })
        }

        /// The main writer loop running in the background thread
        fn writer_loop<W: Write + Seek>(
            writer: W,
            receiver: mpsc::Receiver<WalMessage>,
            stats: Arc<Mutex<PersistenceStats>>,
            flush_interval: Duration,
            flush_count: usize,
        ) {
            let mut wal_writer = match WalWriter::create(writer) {
                Ok(w) => w,
                Err(e) => {
                    log::error!("Failed to create WAL writer: {}", e);
                    return;
                }
            };

            // Cap batch capacity at a reasonable size to prevent allocation overflow
            let batch_capacity = flush_count.min(DEFAULT_FLUSH_COUNT);
            let mut batch: Vec<WalEntry> = Vec::with_capacity(batch_capacity);
            let mut last_flush = Instant::now();
            let mut pending_notifiers: Vec<FlushNotifier> = Vec::new();

            loop {
                // Calculate timeout until next time-based flush
                let elapsed = last_flush.elapsed();
                let timeout = flush_interval.saturating_sub(elapsed);

                match receiver.recv_timeout(timeout) {
                    Ok(WalMessage::Entry(entry)) => {
                        batch.push(entry);

                        // Count-based flush
                        if batch.len() >= flush_count {
                            Self::flush_batch(
                                &mut wal_writer,
                                &mut batch,
                                &stats,
                                &mut pending_notifiers,
                                true,
                            );
                            last_flush = Instant::now();
                        }
                    }
                    Ok(WalMessage::Flush) => {
                        Self::flush_batch(
                            &mut wal_writer,
                            &mut batch,
                            &stats,
                            &mut pending_notifiers,
                            false,
                        );
                        last_flush = Instant::now();
                        stats.lock().explicit_flushes += 1;
                    }
                    Ok(WalMessage::FlushAndNotify(notifier)) => {
                        pending_notifiers.push(notifier);
                        Self::flush_batch(
                            &mut wal_writer,
                            &mut batch,
                            &stats,
                            &mut pending_notifiers,
                            false,
                        );
                        last_flush = Instant::now();
                        stats.lock().explicit_flushes += 1;
                    }
                    Ok(WalMessage::Shutdown) => {
                        // Flush remaining entries before shutdown
                        Self::flush_batch(
                            &mut wal_writer,
                            &mut batch,
                            &stats,
                            &mut pending_notifiers,
                            false,
                        );
                        break;
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        // Time-based flush
                        if !batch.is_empty() {
                            Self::flush_batch(
                                &mut wal_writer,
                                &mut batch,
                                &stats,
                                &mut pending_notifiers,
                                false,
                            );
                            stats.lock().time_flushes += 1;
                        }
                        last_flush = Instant::now();
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        // Channel closed, flush and exit
                        Self::flush_batch(
                            &mut wal_writer,
                            &mut batch,
                            &stats,
                            &mut pending_notifiers,
                            false,
                        );
                        break;
                    }
                }
            }

            log::debug!("WAL writer thread shutting down");
        }

        /// Flush a batch of entries to disk
        fn flush_batch<W: Write + Seek>(
            writer: &mut WalWriter<W>,
            batch: &mut Vec<WalEntry>,
            stats: &Arc<Mutex<PersistenceStats>>,
            pending_notifiers: &mut Vec<FlushNotifier>,
            is_count_flush: bool,
        ) {
            if batch.is_empty() {
                // Still notify waiters even if batch is empty
                for notifier in pending_notifiers.drain(..) {
                    notifier.notify();
                }
                return;
            }

            let mut bytes_written = 0u64;
            let entries_count = batch.len() as u64;

            // Start timing the flush operation
            let flush_start = Instant::now();

            for entry in batch.drain(..) {
                // Estimate bytes (actual size varies with entry content)
                bytes_written += 32; // Approximate overhead per entry
                if let Err(e) = writer.append(&entry) {
                    log::error!("Failed to write WAL entry: {}", e);
                    // Continue trying to write other entries
                }
            }

            if let Err(e) = writer.flush() {
                log::error!("Failed to flush WAL: {}", e);
            }

            // Measure flush latency
            let flush_duration = flush_start.elapsed();

            // Update stats
            {
                let mut stats = stats.lock();
                stats.entries_written += entries_count;
                stats.batches_written += 1;
                stats.bytes_written += bytes_written;
                if is_count_flush {
                    stats.count_flushes += 1;
                }
                stats.record_flush_latency(flush_duration);
            }

            // Notify all waiters
            for notifier in pending_notifiers.drain(..) {
                notifier.notify();
            }
        }

        /// Send a WAL operation to be persisted
        ///
        /// This method is non-blocking unless the channel is full (backpressure).
        /// Returns the LSN assigned to the entry.
        ///
        /// In volatile mode, entries are not sent to the WAL - just an LSN is returned.
        pub fn send(&self, op: WalOp) -> Result<Lsn, StorageError> {
            let lsn = {
                let mut next_lsn = self.next_lsn.lock();
                let lsn = *next_lsn;
                *next_lsn += 1;
                lsn
            };

            // In volatile mode, skip WAL entirely
            if !self.config.writes_wal() {
                self.stats.lock().volatile_discards += 1;
                return Ok(lsn);
            }

            let timestamp_ms = current_timestamp_ms();
            let entry = WalEntry::new(lsn, timestamp_ms, op);

            self.sender
                .send(WalMessage::Entry(entry))
                .map_err(|_| StorageError::IoError("WAL writer thread terminated".to_string()))?;

            self.stats.lock().entries_sent += 1;

            Ok(lsn)
        }

        /// Send a pre-built WAL entry to be persisted
        ///
        /// In volatile mode, entries are not sent to the WAL.
        pub fn send_entry(&self, entry: WalEntry) -> Result<Lsn, StorageError> {
            let lsn = entry.lsn;

            // Update next_lsn if needed
            {
                let mut next_lsn = self.next_lsn.lock();
                if entry.lsn >= *next_lsn {
                    *next_lsn = entry.lsn + 1;
                }
            }

            // In volatile mode, skip WAL entirely
            if !self.config.writes_wal() {
                self.stats.lock().volatile_discards += 1;
                return Ok(lsn);
            }

            self.sender
                .send(WalMessage::Entry(entry))
                .map_err(|_| StorageError::IoError("WAL writer thread terminated".to_string()))?;

            self.stats.lock().entries_sent += 1;

            Ok(lsn)
        }

        /// Trigger an immediate flush of pending entries
        ///
        /// This is non-blocking - it signals the writer thread to flush
        /// but doesn't wait for completion.
        pub fn flush(&self) -> Result<(), StorageError> {
            self.sender
                .send(WalMessage::Flush)
                .map_err(|_| StorageError::IoError("WAL writer thread terminated".to_string()))
        }

        /// Flush pending entries and wait for completion
        ///
        /// This blocks until all pending entries have been written to disk.
        pub fn sync(&self) -> Result<(), StorageError> {
            let notifier = FlushNotifier::new();
            self.sender
                .send(WalMessage::FlushAndNotify(notifier.clone()))
                .map_err(|_| StorageError::IoError("WAL writer thread terminated".to_string()))?;
            notifier.wait();
            Ok(())
        }

        /// Flush pending entries and wait for completion with timeout
        ///
        /// Returns true if the flush completed within the timeout, false otherwise.
        pub fn sync_timeout(&self, timeout: Duration) -> Result<bool, StorageError> {
            let notifier = FlushNotifier::new();
            self.sender
                .send(WalMessage::FlushAndNotify(notifier.clone()))
                .map_err(|_| StorageError::IoError("WAL writer thread terminated".to_string()))?;
            Ok(notifier.wait_timeout(timeout))
        }

        /// Get the next LSN that will be assigned
        pub fn next_lsn(&self) -> Lsn {
            *self.next_lsn.lock()
        }

        /// Get current statistics
        pub fn stats(&self) -> PersistenceStats {
            self.stats.lock().clone()
        }

        /// Get the current durability mode
        pub fn durability_mode(&self) -> DurabilityMode {
            self.config.durability_mode
        }

        /// Get the current configuration
        pub fn config(&self) -> &PersistenceConfig {
            &self.config
        }

        /// Shutdown the persistence engine gracefully
        ///
        /// This flushes all pending entries and waits for the writer thread to terminate.
        pub fn shutdown(&mut self) -> Result<(), StorageError> {
            {
                let mut shutdown = self.shutdown.lock();
                if *shutdown {
                    return Ok(());
                }
                *shutdown = true;
            }

            // Signal shutdown
            let _ = self.sender.send(WalMessage::Shutdown);

            // Wait for thread to finish
            if let Some(handle) = self.handle.take() {
                handle.join().map_err(|_| {
                    StorageError::IoError("WAL writer thread panicked".to_string())
                })?;
            }

            Ok(())
        }

        /// Check if the engine has been shut down
        pub fn is_shutdown(&self) -> bool {
            *self.shutdown.lock()
        }
    }

    impl Drop for PersistenceEngine {
        fn drop(&mut self) {
            if let Err(e) = self.shutdown() {
                log::error!("Error during PersistenceEngine shutdown: {}", e);
            }
        }
    }

    /// Get current timestamp in milliseconds since epoch
    fn current_timestamp_ms() -> u64 {
        use instant::SystemTime;
        SystemTime::now()
            .duration_since(instant::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

}

// ============================================================================
// WASM (no-op/buffered) Implementation
// ============================================================================

#[cfg(target_arch = "wasm32")]
mod wasm {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// Flush completion notifier (no-op for WASM)
    #[derive(Debug, Clone, Default)]
    pub struct FlushNotifier;

    impl FlushNotifier {
        pub fn new() -> Self {
            Self
        }

        pub fn notify(&self) {}
        pub fn wait(&self) {}
    }

    /// Statistics about the persistence engine (WASM version)
    #[derive(Debug, Clone, Default)]
    pub struct PersistenceStats {
        /// Total entries sent to the engine
        pub entries_sent: u64,
        /// Total entries written to disk
        pub entries_written: u64,
        /// Total batches written
        pub batches_written: u64,
        /// Total bytes written
        pub bytes_written: u64,
        /// Number of time-based flushes
        pub time_flushes: u64,
        /// Number of count-based flushes
        pub count_flushes: u64,
        /// Number of explicit flushes
        pub explicit_flushes: u64,
        /// Number of entries discarded in volatile mode
        pub volatile_discards: u64,
        /// Number of sync-on-commit flushes (Durable/Paranoid mode)
        pub commit_syncs: u64,
        /// Number of sync-on-op flushes (Paranoid mode only)
        pub op_syncs: u64,
        /// Average flush latency in microseconds (always 0 for WASM)
        pub avg_flush_latency_us: u64,
        /// Maximum flush latency in microseconds (always 0 for WASM)
        pub max_flush_latency_us: u64,
        /// Number of pending entries in channel (snapshot)
        pub pending_entries: u64,
    }

    /// Persistence engine for WASM (buffered, no background thread)
    ///
    /// In WASM, we can't spawn threads, so this implementation buffers
    /// entries in memory. In a real browser environment, you would
    /// periodically flush to IndexedDB or the Origin Private File System.
    pub struct PersistenceEngine {
        /// Buffered entries
        buffer: Arc<Mutex<Vec<WalEntry>>>,
        /// Statistics
        stats: Arc<Mutex<PersistenceStats>>,
        /// Next LSN to assign
        next_lsn: Arc<Mutex<Lsn>>,
        /// Configuration
        config: PersistenceConfig,
    }

    impl std::fmt::Debug for PersistenceEngine {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PersistenceEngine")
                .field("next_lsn", &*self.next_lsn.lock().unwrap())
                .field("buffer_len", &self.buffer.lock().unwrap().len())
                .field("config", &self.config)
                .finish_non_exhaustive()
        }
    }

    impl PersistenceEngine {
        /// Create a new WASM persistence engine
        pub fn new(config: PersistenceConfig) -> Result<Self, StorageError> {
            Ok(Self {
                buffer: Arc::new(Mutex::new(Vec::with_capacity(config.flush_count))),
                stats: Arc::new(Mutex::new(PersistenceStats::default())),
                next_lsn: Arc::new(Mutex::new(1u64)),
                config,
            })
        }

        /// Send a WAL operation to be persisted
        ///
        /// In volatile mode, entries are not buffered.
        pub fn send(&self, op: WalOp) -> Result<Lsn, StorageError> {
            let lsn = {
                let mut next_lsn = self.next_lsn.lock().unwrap();
                let lsn = *next_lsn;
                *next_lsn += 1;
                lsn
            };

            // In volatile mode, skip buffering
            if !self.config.writes_wal() {
                self.stats.lock().unwrap().volatile_discards += 1;
                return Ok(lsn);
            }

            let timestamp_ms = current_timestamp_ms();
            let entry = WalEntry::new(lsn, timestamp_ms, op);

            {
                let mut buffer = self.buffer.lock().unwrap();
                buffer.push(entry);
            }

            self.stats.lock().unwrap().entries_sent += 1;

            Ok(lsn)
        }

        /// Send a pre-built WAL entry to be persisted
        ///
        /// In volatile mode, entries are not buffered.
        pub fn send_entry(&self, entry: WalEntry) -> Result<Lsn, StorageError> {
            let lsn = entry.lsn;

            {
                let mut next_lsn = self.next_lsn.lock().unwrap();
                if entry.lsn >= *next_lsn {
                    *next_lsn = entry.lsn + 1;
                }
            }

            // In volatile mode, skip buffering
            if !self.config.writes_wal() {
                self.stats.lock().unwrap().volatile_discards += 1;
                return Ok(lsn);
            }

            {
                let mut buffer = self.buffer.lock().unwrap();
                buffer.push(entry);
            }

            self.stats.lock().unwrap().entries_sent += 1;

            Ok(lsn)
        }

        /// Trigger a flush (no-op in WASM - would need async to IndexedDB)
        pub fn flush(&self) -> Result<(), StorageError> {
            // In a real implementation, this would schedule an async write
            // to IndexedDB or OPFS
            log::debug!("WASM flush requested (buffered only)");
            Ok(())
        }

        /// Sync (no-op in WASM)
        pub fn sync(&self) -> Result<(), StorageError> {
            self.flush()
        }

        /// Get the next LSN that will be assigned
        pub fn next_lsn(&self) -> Lsn {
            *self.next_lsn.lock().unwrap()
        }

        /// Get current statistics
        pub fn stats(&self) -> PersistenceStats {
            self.stats.lock().unwrap().clone()
        }

        /// Get the current durability mode
        pub fn durability_mode(&self) -> DurabilityMode {
            self.config.durability_mode
        }

        /// Get the current configuration
        pub fn config(&self) -> &PersistenceConfig {
            &self.config
        }

        /// Get buffered entries (WASM-specific)
        pub fn buffered_entries(&self) -> Vec<WalEntry> {
            self.buffer.lock().unwrap().clone()
        }

        /// Clear buffered entries (WASM-specific)
        pub fn clear_buffer(&self) -> Vec<WalEntry> {
            let mut buffer = self.buffer.lock().unwrap();
            std::mem::take(&mut *buffer)
        }

        /// Shutdown (no-op in WASM)
        pub fn shutdown(&mut self) -> Result<(), StorageError> {
            Ok(())
        }

        /// Check if the engine has been shut down (always false for WASM)
        pub fn is_shutdown(&self) -> bool {
            false
        }
    }

    /// Get current timestamp in milliseconds since epoch
    fn current_timestamp_ms() -> u64 {
        use instant::SystemTime;
        SystemTime::now()
            .duration_since(instant::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

// ============================================================================
// Public API (platform-agnostic)
// ============================================================================

#[cfg(not(target_arch = "wasm32"))]
pub use native::{FlushNotifier, PersistenceEngine, PersistenceStats};

#[cfg(target_arch = "wasm32")]
pub use wasm::{FlushNotifier, PersistenceEngine, PersistenceStats};

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::time::Duration;

    use vibesql_types::SqlValue;

    use super::*;

    #[test]
    fn test_persistence_engine_create() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let engine =
            PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();

        assert_eq!(engine.next_lsn(), 1);
        assert!(!engine.is_shutdown());
    }

    #[test]
    fn test_send_entry() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let engine =
            PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();

        let lsn = engine
            .send(WalOp::Insert {
                table_id: 1,
                row_id: 100,
                values: vec![SqlValue::Integer(42)],
            })
            .unwrap();

        assert_eq!(lsn, 1);
        assert_eq!(engine.next_lsn(), 2);

        let stats = engine.stats();
        assert_eq!(stats.entries_sent, 1);
    }

    #[test]
    fn test_send_multiple_entries() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let engine =
            PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();

        for i in 1..=10 {
            let lsn = engine
                .send(WalOp::Insert {
                    table_id: 1,
                    row_id: i as u64,
                    values: vec![SqlValue::Integer(i)],
                })
                .unwrap();
            assert_eq!(lsn, i as u64);
        }

        assert_eq!(engine.next_lsn(), 11);
        assert_eq!(engine.stats().entries_sent, 10);
    }

    #[test]
    fn test_sync_flushes_entries() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let mut engine =
            PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();

        // Send some entries
        for i in 1..=5 {
            engine
                .send(WalOp::Insert {
                    table_id: 1,
                    row_id: i as u64,
                    values: vec![SqlValue::Integer(i)],
                })
                .unwrap();
        }

        // Sync should flush all entries
        engine.sync().unwrap();

        let stats = engine.stats();
        assert_eq!(stats.entries_sent, 5);
        assert!(stats.entries_written >= 5);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_shutdown_flushes_pending() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let mut engine =
            PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();

        // Send entries without explicit sync
        for i in 1..=3 {
            engine
                .send(WalOp::Insert {
                    table_id: 1,
                    row_id: i as u64,
                    values: vec![SqlValue::Integer(i)],
                })
                .unwrap();
        }

        // Shutdown should flush pending entries
        engine.shutdown().unwrap();

        assert!(engine.is_shutdown());
    }

    #[test]
    fn test_config_defaults() {
        let config = PersistenceConfig::default();
        assert_eq!(config.channel_capacity, DEFAULT_CHANNEL_CAPACITY);
        assert_eq!(config.flush_interval_ms, DEFAULT_FLUSH_INTERVAL_MS);
        assert_eq!(config.flush_count, DEFAULT_FLUSH_COUNT);
    }

    #[test]
    fn test_flush_non_blocking() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let engine =
            PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();

        engine
            .send(WalOp::Insert {
                table_id: 1,
                row_id: 1,
                values: vec![SqlValue::Integer(1)],
            })
            .unwrap();

        // Flush should return immediately
        engine.flush().unwrap();
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_sync_with_timeout() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let mut engine =
            PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();

        engine
            .send(WalOp::Insert {
                table_id: 1,
                row_id: 1,
                values: vec![SqlValue::Integer(1)],
            })
            .unwrap();

        // Should complete within timeout
        let completed = engine.sync_timeout(Duration::from_secs(5)).unwrap();
        assert!(completed);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_count_based_flush() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        // Set a low flush count to trigger count-based flush
        let config = PersistenceConfig { flush_count: 5, ..Default::default() };

        let mut engine = PersistenceEngine::with_writer(cursor, config).unwrap();

        // Send more than flush_count entries
        for i in 1..=10 {
            engine
                .send(WalOp::Insert {
                    table_id: 1,
                    row_id: i as u64,
                    values: vec![SqlValue::Integer(i)],
                })
                .unwrap();
        }

        // Give the writer thread time to process
        std::thread::sleep(Duration::from_millis(100));

        let stats = engine.stats();
        // Should have triggered at least one count-based flush
        assert!(stats.count_flushes >= 1);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_flush_latency_tracking() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        let mut engine =
            PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();

        // Send some entries
        for i in 1..=5 {
            engine
                .send(WalOp::Insert {
                    table_id: 1,
                    row_id: i as u64,
                    values: vec![SqlValue::Integer(i)],
                })
                .unwrap();
        }

        // Sync to trigger a flush
        engine.sync().unwrap();

        let stats = engine.stats();

        // Latency should be recorded (at least one sample)
        // avg and max should be > 0 since we did at least one flush
        assert!(
            stats.avg_flush_latency_us > 0 || stats.max_flush_latency_us > 0,
            "Flush latency should be recorded after sync"
        );

        // Max should be >= avg
        assert!(
            stats.max_flush_latency_us >= stats.avg_flush_latency_us,
            "Max latency should be >= avg latency"
        );

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_flush_latency_max_tracking() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        // Use low flush count to trigger multiple flushes
        let config = PersistenceConfig { flush_count: 2, ..Default::default() };

        let mut engine = PersistenceEngine::with_writer(cursor, config).unwrap();

        // Send enough entries to trigger multiple flushes
        for i in 1..=10 {
            engine
                .send(WalOp::Insert {
                    table_id: 1,
                    row_id: i as u64,
                    values: vec![SqlValue::Integer(i)],
                })
                .unwrap();
        }

        // Give writer thread time to process
        std::thread::sleep(Duration::from_millis(100));

        // Final sync to ensure all entries flushed
        engine.sync().unwrap();

        let stats = engine.stats();

        // Multiple flushes should have occurred
        assert!(
            stats.batches_written >= 1,
            "At least one batch should have been written"
        );

        // Latency metrics should be populated
        // Note: On very fast systems, latency could be 0 microseconds
        // so we just verify the tracking doesn't break
        assert!(
            stats.max_flush_latency_us >= stats.avg_flush_latency_us,
            "Max latency ({}) should be >= avg latency ({})",
            stats.max_flush_latency_us,
            stats.avg_flush_latency_us
        );

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_volatile_mode_discards_entries() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        // Create engine in volatile mode
        let config = PersistenceConfig::volatile();
        let mut engine = PersistenceEngine::with_writer(cursor, config).unwrap();

        // Send some entries
        for i in 1..=5 {
            let lsn = engine
                .send(WalOp::Insert {
                    table_id: 1,
                    row_id: i as u64,
                    values: vec![SqlValue::Integer(i)],
                })
                .unwrap();
            // LSNs should still be assigned
            assert_eq!(lsn, i as u64);
        }

        // Check stats - entries should be discarded, not sent
        let stats = engine.stats();
        assert_eq!(stats.volatile_discards, 5);
        assert_eq!(stats.entries_sent, 0);

        // Verify mode
        assert_eq!(engine.durability_mode(), DurabilityMode::Volatile);
        assert!(!engine.config().writes_wal());

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_lazy_mode_sends_entries() {
        let buf = Vec::new();
        let cursor = Cursor::new(buf);

        // Create engine in lazy mode (default)
        let config = PersistenceConfig::lazy();
        let mut engine = PersistenceEngine::with_writer(cursor, config).unwrap();

        // Send some entries
        for i in 1..=3 {
            engine
                .send(WalOp::Insert {
                    table_id: 1,
                    row_id: i as u64,
                    values: vec![SqlValue::Integer(i)],
                })
                .unwrap();
        }

        // Check stats - entries should be sent
        let stats = engine.stats();
        assert_eq!(stats.entries_sent, 3);
        assert_eq!(stats.volatile_discards, 0);

        // Verify mode
        assert_eq!(engine.durability_mode(), DurabilityMode::Lazy);
        assert!(engine.config().writes_wal());
        assert!(!engine.config().sync_on_commit());

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_config_presets() {
        // Volatile mode
        let volatile = PersistenceConfig::volatile();
        assert_eq!(volatile.durability_mode, DurabilityMode::Volatile);
        assert!(!volatile.writes_wal());

        // Lazy mode
        let lazy = PersistenceConfig::lazy();
        assert_eq!(lazy.durability_mode, DurabilityMode::Lazy);
        assert!(lazy.writes_wal());
        assert!(!lazy.sync_on_commit());

        // Durable mode
        let durable = PersistenceConfig::durable();
        assert_eq!(durable.durability_mode, DurabilityMode::Durable);
        assert!(durable.writes_wal());
        assert!(durable.sync_on_commit());
        assert!(!durable.sync_on_every_op());

        // Paranoid mode
        let paranoid = PersistenceConfig::paranoid();
        assert_eq!(paranoid.durability_mode, DurabilityMode::Paranoid);
        assert!(paranoid.writes_wal());
        assert!(paranoid.sync_on_commit());
        assert!(paranoid.sync_on_every_op());
    }

    #[test]
    fn test_config_from_durability_config() {
        use crate::wal::durability::DurabilityConfig;

        let dur_config = DurabilityConfig::durable();
        let config = PersistenceConfig::from_durability_config(&dur_config);

        assert_eq!(config.durability_mode, DurabilityMode::Durable);
        assert_eq!(config.flush_interval_ms, dur_config.wal_flush_interval_ms);
        assert_eq!(config.flush_count, dur_config.wal_flush_batch_size);
    }

    #[test]
    fn test_durability_mode_getter() {
        // Test each mode
        for (mode, config) in [
            (DurabilityMode::Volatile, PersistenceConfig::volatile()),
            (DurabilityMode::Lazy, PersistenceConfig::lazy()),
            (DurabilityMode::Durable, PersistenceConfig::durable()),
            (DurabilityMode::Paranoid, PersistenceConfig::paranoid()),
        ] {
            let buf: Vec<u8> = Vec::new();
            let cursor = Cursor::new(buf);
            let mut engine = PersistenceEngine::with_writer(cursor, config).unwrap();
            assert_eq!(engine.durability_mode(), mode);
            engine.shutdown().unwrap();
        }
    }
}
