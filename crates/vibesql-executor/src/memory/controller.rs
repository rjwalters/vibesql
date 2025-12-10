//! Memory controller for bounded query execution
//!
//! This module provides memory budget management for operators that need to
//! track and limit memory usage, supporting disk spilling when memory is exhausted.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    MemoryController                          │
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
//! │  │ Budget Pool │  │  Tracking   │  │ Spill Mgr   │         │
//! │  │ (configurable)│ │ (per-operator)│ │ (temp files)│        │
//! │  └─────────────┘  └─────────────┘  └─────────────┘         │
//! └─────────────────────────────────────────────────────────────┘
//!            │                │                │
//!            ▼                ▼                ▼
//! ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
//! │ External     │  │ External     │  │ External     │
//! │ Sort         │  │ Aggregate    │  │ Hash Join    │
//! └──────────────┘  └──────────────┘  └──────────────┘
//! ```
//!
//! # Design Decisions
//!
//! 1. **Thread-safe**: Uses atomic operations for concurrent access
//! 2. **Non-blocking reservations**: `try_reserve` never blocks, operators
//!    decide whether to spill based on return value
//! 3. **Configurable via environment**: Memory limits can be overridden
//! 4. **Platform-aware**: Different defaults for native vs WASM

use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Default memory budget: 1GB
/// Conservative default that works on most systems
pub const DEFAULT_MEMORY_BUDGET: usize = 1024 * 1024 * 1024; // 1 GB

/// Default spill threshold: 80%
/// Start spilling when 80% of budget is used
pub const DEFAULT_SPILL_THRESHOLD: f64 = 0.8;

/// Default target partition size for external operators: 64MB
/// Tuned for good I/O efficiency while limiting memory per partition
pub const DEFAULT_TARGET_PARTITION_BYTES: usize = 64 * 1024 * 1024; // 64 MB

/// Minimum memory for an operator: 4MB
/// Below this, operators may not function correctly
pub const MIN_OPERATOR_MEMORY: usize = 4 * 1024 * 1024; // 4 MB

/// Configuration for memory-bounded execution
#[derive(Debug, Clone)]
pub struct MemoryConfig {
    /// Total memory budget for query execution
    pub budget_bytes: usize,

    /// Directory for spill files (defaults to system temp)
    pub temp_directory: PathBuf,

    /// Threshold at which to trigger spilling (0.0 - 1.0)
    pub spill_threshold: f64,

    /// Target size for each partition in external operators
    pub target_partition_bytes: usize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        // Check environment variables for overrides
        let budget_bytes = env::var("VIBESQL_MEMORY_LIMIT")
            .ok()
            .and_then(|s| parse_memory_size(&s))
            .unwrap_or(DEFAULT_MEMORY_BUDGET);

        let temp_directory = env::var("VIBESQL_TEMP_DIR")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| env::temp_dir().join("vibesql"));

        let spill_threshold = env::var("VIBESQL_SPILL_THRESHOLD")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|&t| (0.0..=1.0).contains(&t))
            .unwrap_or(DEFAULT_SPILL_THRESHOLD);

        let target_partition_bytes = env::var("VIBESQL_PARTITION_SIZE")
            .ok()
            .and_then(|s| parse_memory_size(&s))
            .unwrap_or(DEFAULT_TARGET_PARTITION_BYTES);

        Self {
            budget_bytes,
            temp_directory,
            spill_threshold,
            target_partition_bytes,
        }
    }
}

impl MemoryConfig {
    /// Create a new configuration with the specified budget
    pub fn with_budget(budget_bytes: usize) -> Self {
        Self {
            budget_bytes,
            ..Default::default()
        }
    }

    /// Create a configuration with a specific temp directory
    pub fn with_temp_dir(mut self, path: PathBuf) -> Self {
        self.temp_directory = path;
        self
    }

    /// Set the spill threshold (0.0 - 1.0)
    pub fn with_spill_threshold(mut self, threshold: f64) -> Self {
        self.spill_threshold = threshold.clamp(0.0, 1.0);
        self
    }
}

/// Statistics snapshot from the memory controller
///
/// A point-in-time view of memory usage and spill statistics.
/// Useful for monitoring, debugging, and query profiling.
#[derive(Debug, Clone)]
pub struct MemoryStats {
    /// Total memory budget
    pub budget_bytes: usize,
    /// Currently reserved memory
    pub reserved_bytes: usize,
    /// Peak memory usage (high water mark)
    pub peak_bytes: usize,
    /// Total bytes written to disk during spills
    pub bytes_spilled: usize,
    /// Number of spill operations performed
    pub spill_count: usize,
    /// Number of currently active reservations
    pub active_reservations: usize,
    /// Spill threshold (0.0 - 1.0)
    pub spill_threshold: f64,
}

impl MemoryStats {
    /// Get memory utilization as a percentage (0.0 - 1.0)
    pub fn utilization(&self) -> f64 {
        if self.budget_bytes == 0 {
            0.0
        } else {
            self.reserved_bytes as f64 / self.budget_bytes as f64
        }
    }

    /// Check if memory is under pressure (above spill threshold)
    pub fn is_under_pressure(&self) -> bool {
        self.utilization() >= self.spill_threshold
    }

    /// Get available memory
    pub fn available_bytes(&self) -> usize {
        self.budget_bytes.saturating_sub(self.reserved_bytes)
    }
}

impl std::fmt::Display for MemoryStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Memory: {}/{} ({:.1}%), peak: {}, spilled: {} ({} ops)",
            format_bytes(self.reserved_bytes),
            format_bytes(self.budget_bytes),
            self.utilization() * 100.0,
            format_bytes(self.peak_bytes),
            format_bytes(self.bytes_spilled),
            self.spill_count,
        )
    }
}

/// Format bytes as human-readable string
fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2}GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2}MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2}KB", bytes as f64 / 1024.0)
    } else {
        format!("{}B", bytes)
    }
}

/// Parse memory size strings like "4GB", "512MB", "1024K", "1073741824"
fn parse_memory_size(s: &str) -> Option<usize> {
    let s = s.trim().to_uppercase();

    // Try parsing as pure number first
    if let Ok(n) = s.parse::<usize>() {
        return Some(n);
    }

    // Parse with suffix
    let (num_str, multiplier) = if let Some(num) = s.strip_suffix("GB") {
        (num, 1024 * 1024 * 1024)
    } else if let Some(num) = s.strip_suffix("G") {
        (num, 1024 * 1024 * 1024)
    } else if let Some(num) = s.strip_suffix("MB") {
        (num, 1024 * 1024)
    } else if let Some(num) = s.strip_suffix("M") {
        (num, 1024 * 1024)
    } else if let Some(num) = s.strip_suffix("KB") {
        (num, 1024)
    } else if let Some(num) = s.strip_suffix("K") {
        (num, 1024)
    } else {
        return None;
    };

    num_str.trim().parse::<usize>().ok().map(|n| n * multiplier)
}

/// Global memory controller for query execution
///
/// Manages a shared memory budget across all operators in a query.
/// Thread-safe and supports concurrent reservations.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use vibesql_executor::memory::{MemoryController, MemoryConfig};
///
/// let controller = Arc::new(MemoryController::new(MemoryConfig::with_budget(1024 * 1024 * 1024)));
///
/// // Create a reservation for a sort operator
/// let mut reservation = controller.create_reservation();
///
/// // Try to reserve memory for sorted runs
/// if reservation.try_grow(1024 * 1024) {
///     // Memory reserved, proceed
/// } else {
///     // Need to spill to disk
/// }
/// ```
pub struct MemoryController {
    /// Configuration
    config: MemoryConfig,

    /// Total reserved memory across all operators (bytes)
    reserved: AtomicUsize,

    /// Number of active reservations (for debugging/metrics)
    active_reservations: AtomicUsize,

    /// Total bytes spilled to disk (for metrics)
    bytes_spilled: AtomicUsize,

    /// Number of spill operations (for metrics)
    spill_count: AtomicUsize,

    /// Peak memory usage (high water mark)
    peak_memory: AtomicUsize,
}

impl MemoryController {
    /// Create a new memory controller with the given configuration
    pub fn new(config: MemoryConfig) -> Self {
        Self {
            config,
            reserved: AtomicUsize::new(0),
            active_reservations: AtomicUsize::new(0),
            bytes_spilled: AtomicUsize::new(0),
            spill_count: AtomicUsize::new(0),
            peak_memory: AtomicUsize::new(0),
        }
    }

    /// Create a memory controller with default configuration
    pub fn with_defaults() -> Self {
        Self::new(MemoryConfig::default())
    }

    /// Create a memory controller with a specific budget
    pub fn with_budget(budget_bytes: usize) -> Self {
        Self::new(MemoryConfig::with_budget(budget_bytes))
    }

    /// Get the total memory budget
    pub fn budget(&self) -> usize {
        self.config.budget_bytes
    }

    /// Get the currently reserved memory
    pub fn reserved(&self) -> usize {
        self.reserved.load(Ordering::Relaxed)
    }

    /// Get available memory (budget - reserved)
    pub fn available(&self) -> usize {
        let budget = self.config.budget_bytes;
        let reserved = self.reserved.load(Ordering::Relaxed);
        budget.saturating_sub(reserved)
    }

    /// Get the spill threshold in bytes
    pub fn spill_threshold_bytes(&self) -> usize {
        (self.config.budget_bytes as f64 * self.config.spill_threshold) as usize
    }

    /// Check if memory pressure is high (above spill threshold)
    pub fn should_spill(&self) -> bool {
        self.reserved() >= self.spill_threshold_bytes()
    }

    /// Get the temporary directory for spill files
    pub fn temp_directory(&self) -> &PathBuf {
        &self.config.temp_directory
    }

    /// Get the target partition size for external operators
    pub fn target_partition_bytes(&self) -> usize {
        self.config.target_partition_bytes
    }

    /// Create a new memory reservation
    ///
    /// Returns a reservation handle that tracks memory for a single operator.
    /// When the reservation is dropped, its memory is released.
    pub fn create_reservation(self: &Arc<Self>) -> MemoryReservation {
        self.active_reservations.fetch_add(1, Ordering::Relaxed);
        MemoryReservation {
            controller: Arc::clone(self),
            reserved: 0,
        }
    }

    /// Record that bytes were spilled to disk (for metrics)
    pub fn record_spill(&self, bytes: usize) {
        self.bytes_spilled.fetch_add(bytes, Ordering::Relaxed);
        self.spill_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total bytes spilled to disk
    pub fn bytes_spilled(&self) -> usize {
        self.bytes_spilled.load(Ordering::Relaxed)
    }

    /// Get number of spill operations
    pub fn spill_count(&self) -> usize {
        self.spill_count.load(Ordering::Relaxed)
    }

    /// Get peak memory usage
    pub fn peak_memory(&self) -> usize {
        self.peak_memory.load(Ordering::Relaxed)
    }

    /// Get number of active reservations
    pub fn active_reservations(&self) -> usize {
        self.active_reservations.load(Ordering::Relaxed)
    }

    /// Get comprehensive statistics snapshot
    pub fn stats(&self) -> MemoryStats {
        MemoryStats {
            budget_bytes: self.config.budget_bytes,
            reserved_bytes: self.reserved.load(Ordering::Relaxed),
            peak_bytes: self.peak_memory.load(Ordering::Relaxed),
            bytes_spilled: self.bytes_spilled.load(Ordering::Relaxed),
            spill_count: self.spill_count.load(Ordering::Relaxed),
            active_reservations: self.active_reservations.load(Ordering::Relaxed),
            spill_threshold: self.config.spill_threshold,
        }
    }

    /// Try to reserve memory, returning true if successful
    ///
    /// This is the internal method called by MemoryReservation
    fn try_reserve(&self, bytes: usize) -> bool {
        let budget = self.config.budget_bytes;

        loop {
            let current = self.reserved.load(Ordering::Relaxed);
            let new_reserved = current.saturating_add(bytes);

            if new_reserved > budget {
                return false;
            }

            // Try to atomically update
            match self.reserved.compare_exchange_weak(
                current,
                new_reserved,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Update peak memory if this is a new high
                    self.update_peak_memory(new_reserved);
                    return true;
                }
                Err(_) => continue, // Retry on contention
            }
        }
    }

    /// Update peak memory tracking
    fn update_peak_memory(&self, current: usize) {
        let mut peak = self.peak_memory.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_memory.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
    }

    /// Release reserved memory
    ///
    /// This is the internal method called by MemoryReservation
    fn release(&self, bytes: usize) {
        self.reserved.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Decrement active reservation count
    fn release_reservation(&self) {
        self.active_reservations.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A memory reservation for a single operator
///
/// Tracks memory usage for one operator (sort, aggregate, join, etc.)
/// and automatically releases memory when dropped.
///
/// # Example
///
/// ```rust,ignore
/// let controller = Arc::new(MemoryController::with_budget(1024 * 1024 * 1024));
/// let mut reservation = controller.create_reservation();
///
/// // Accumulate memory as we process data
/// for batch in batches {
///     let batch_size = batch.size_in_bytes();
///
///     if !reservation.try_grow(batch_size) {
///         // Memory exhausted - spill current data to disk
///         spill_to_disk(&accumulated_data);
///         reservation.shrink(accumulated_data.size_in_bytes());
///     }
///
///     // Process batch...
/// }
/// ```
pub struct MemoryReservation {
    /// Reference to the parent controller
    controller: Arc<MemoryController>,

    /// Memory reserved by this operator (bytes)
    reserved: usize,
}

impl MemoryReservation {
    /// Get the amount of memory reserved by this operator
    pub fn reserved(&self) -> usize {
        self.reserved
    }

    /// Try to grow the reservation by the specified amount
    ///
    /// Returns true if the memory was reserved, false if there's
    /// not enough budget available. The caller should spill to disk
    /// if this returns false.
    pub fn try_grow(&mut self, additional: usize) -> bool {
        if self.controller.try_reserve(additional) {
            self.reserved = self.reserved.saturating_add(additional);
            true
        } else {
            false
        }
    }

    /// Shrink the reservation by the specified amount
    ///
    /// Call this after spilling data to disk to free up budget
    /// for new data.
    pub fn shrink(&mut self, bytes: usize) {
        let to_release = bytes.min(self.reserved);
        self.controller.release(to_release);
        self.reserved = self.reserved.saturating_sub(to_release);
    }

    /// Release all reserved memory
    ///
    /// Equivalent to `shrink(self.reserved())`
    pub fn release_all(&mut self) {
        if self.reserved > 0 {
            self.controller.release(self.reserved);
            self.reserved = 0;
        }
    }

    /// Check if memory pressure is high and spilling is recommended
    pub fn should_spill(&self) -> bool {
        self.controller.should_spill()
    }

    /// Check if trying to grow by the given amount would exceed budget
    ///
    /// This is a non-mutating check useful for deciding whether to
    /// accumulate more data or trigger a spill first.
    pub fn would_exceed_budget(&self, additional: usize) -> bool {
        let current_total = self.controller.reserved();
        current_total.saturating_add(additional) > self.controller.budget()
    }

    /// Get the controller's budget
    pub fn budget(&self) -> usize {
        self.controller.budget()
    }

    /// Get the temporary directory for spill files
    pub fn temp_directory(&self) -> &PathBuf {
        self.controller.temp_directory()
    }

    /// Get the target partition size
    pub fn target_partition_bytes(&self) -> usize {
        self.controller.target_partition_bytes()
    }

    /// Record that data was spilled to disk
    pub fn record_spill(&self, bytes: usize) {
        self.controller.record_spill(bytes);
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        // Release all reserved memory when the reservation is dropped
        if self.reserved > 0 {
            self.controller.release(self.reserved);
        }
        self.controller.release_reservation();
    }
}

// MemoryController is thread-safe
unsafe impl Send for MemoryController {}
unsafe impl Sync for MemoryController {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_memory_size_parsing() {
        assert_eq!(parse_memory_size("1024"), Some(1024));
        assert_eq!(parse_memory_size("1KB"), Some(1024));
        assert_eq!(parse_memory_size("1K"), Some(1024));
        assert_eq!(parse_memory_size("1MB"), Some(1024 * 1024));
        assert_eq!(parse_memory_size("1M"), Some(1024 * 1024));
        assert_eq!(parse_memory_size("1GB"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_size("1G"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_size("4GB"), Some(4 * 1024 * 1024 * 1024));
        assert_eq!(parse_memory_size("512mb"), Some(512 * 1024 * 1024));
        assert_eq!(parse_memory_size(" 100 "), Some(100));
        assert_eq!(parse_memory_size("invalid"), None);
    }

    #[test]
    fn test_controller_creation() {
        let controller = Arc::new(MemoryController::with_budget(1024 * 1024));
        assert_eq!(controller.budget(), 1024 * 1024);
        assert_eq!(controller.reserved(), 0);
        assert_eq!(controller.available(), 1024 * 1024);
    }

    #[test]
    fn test_reservation_try_grow() {
        let controller = Arc::new(MemoryController::with_budget(1024));
        let mut reservation = controller.create_reservation();

        assert!(reservation.try_grow(512));
        assert_eq!(reservation.reserved(), 512);
        assert_eq!(controller.reserved(), 512);

        assert!(reservation.try_grow(256));
        assert_eq!(reservation.reserved(), 768);
        assert_eq!(controller.reserved(), 768);

        // Should fail - would exceed budget
        assert!(!reservation.try_grow(512));
        assert_eq!(reservation.reserved(), 768);
        assert_eq!(controller.reserved(), 768);

        // Can still grow within budget
        assert!(reservation.try_grow(256));
        assert_eq!(reservation.reserved(), 1024);
    }

    #[test]
    fn test_reservation_shrink() {
        let controller = Arc::new(MemoryController::with_budget(1024));
        let mut reservation = controller.create_reservation();

        reservation.try_grow(1024);
        assert_eq!(reservation.reserved(), 1024);

        reservation.shrink(512);
        assert_eq!(reservation.reserved(), 512);
        assert_eq!(controller.reserved(), 512);

        // Shrink more than reserved clamps to reserved
        reservation.shrink(1024);
        assert_eq!(reservation.reserved(), 0);
        assert_eq!(controller.reserved(), 0);
    }

    #[test]
    fn test_reservation_drop_releases_memory() {
        let controller = Arc::new(MemoryController::with_budget(1024));

        {
            let mut reservation = controller.create_reservation();
            reservation.try_grow(512);
            assert_eq!(controller.reserved(), 512);
        }

        // Memory should be released when reservation is dropped
        assert_eq!(controller.reserved(), 0);
    }

    #[test]
    fn test_multiple_reservations() {
        let controller = Arc::new(MemoryController::with_budget(1024));

        let mut res1 = controller.create_reservation();
        let mut res2 = controller.create_reservation();

        assert!(res1.try_grow(300));
        assert!(res2.try_grow(300));
        assert_eq!(controller.reserved(), 600);

        assert!(res1.try_grow(200));
        assert!(res2.try_grow(200));
        assert_eq!(controller.reserved(), 1000);

        // Both reservations together exceed budget
        assert!(!res1.try_grow(100));
        assert!(!res2.try_grow(100));

        // Drop one reservation
        drop(res1);
        assert_eq!(controller.reserved(), 500);

        // Now res2 can grow
        assert!(res2.try_grow(400));
        assert_eq!(controller.reserved(), 900);
    }

    #[test]
    fn test_should_spill() {
        let config = MemoryConfig {
            budget_bytes: 1000,
            spill_threshold: 0.8, // 800 bytes
            temp_directory: std::env::temp_dir(),
            target_partition_bytes: DEFAULT_TARGET_PARTITION_BYTES,
        };
        let controller = Arc::new(MemoryController::new(config));

        let mut reservation = controller.create_reservation();

        reservation.try_grow(700);
        assert!(!reservation.should_spill());

        reservation.try_grow(100);
        assert!(reservation.should_spill());
    }

    #[test]
    fn test_spill_tracking() {
        let controller = Arc::new(MemoryController::with_budget(1024));
        assert_eq!(controller.bytes_spilled(), 0);

        controller.record_spill(100);
        assert_eq!(controller.bytes_spilled(), 100);

        controller.record_spill(200);
        assert_eq!(controller.bytes_spilled(), 300);
    }

    #[test]
    fn test_active_reservation_count() {
        let controller = Arc::new(MemoryController::with_budget(1024));
        assert_eq!(controller.active_reservations(), 0);

        let _res1 = controller.create_reservation();
        assert_eq!(controller.active_reservations(), 1);

        let _res2 = controller.create_reservation();
        assert_eq!(controller.active_reservations(), 2);

        drop(_res1);
        assert_eq!(controller.active_reservations(), 1);

        drop(_res2);
        assert_eq!(controller.active_reservations(), 0);
    }

    #[test]
    fn test_concurrent_reservations() {
        use std::thread;

        let controller = Arc::new(MemoryController::with_budget(10_000));
        let mut handles = vec![];

        // Spawn 10 threads, each trying to reserve 500 bytes
        for _ in 0..10 {
            let controller = Arc::clone(&controller);
            handles.push(thread::spawn(move || {
                let mut reservation = controller.create_reservation();
                reservation.try_grow(500);
                // Hold reservation for a bit
                thread::sleep(std::time::Duration::from_millis(10));
                reservation.reserved()
            }));
        }

        // Wait for all threads and collect results
        let reserved: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

        // All threads should have reserved 500 bytes
        assert_eq!(reserved, 5000);

        // All memory should be released after threads complete
        assert_eq!(controller.reserved(), 0);
    }

    #[test]
    fn test_memory_stats() {
        let config = MemoryConfig {
            budget_bytes: 1000,
            spill_threshold: 0.8,
            temp_directory: std::env::temp_dir(),
            target_partition_bytes: DEFAULT_TARGET_PARTITION_BYTES,
        };
        let controller = Arc::new(MemoryController::new(config));

        // Initial stats
        let stats = controller.stats();
        assert_eq!(stats.budget_bytes, 1000);
        assert_eq!(stats.reserved_bytes, 0);
        assert_eq!(stats.peak_bytes, 0);
        assert_eq!(stats.bytes_spilled, 0);
        assert_eq!(stats.spill_count, 0);
        assert_eq!(stats.active_reservations, 0);
        assert_eq!(stats.spill_threshold, 0.8);

        // After reservations
        let mut res = controller.create_reservation();
        res.try_grow(500);

        let stats = controller.stats();
        assert_eq!(stats.reserved_bytes, 500);
        assert_eq!(stats.peak_bytes, 500);
        assert_eq!(stats.active_reservations, 1);

        // Test utilization
        assert!((stats.utilization() - 0.5).abs() < 0.001);
        assert_eq!(stats.available_bytes(), 500);
        assert!(!stats.is_under_pressure()); // 50% < 80%

        // Go above spill threshold
        res.try_grow(400);
        let stats = controller.stats();
        assert!(stats.is_under_pressure()); // 90% >= 80%
    }

    #[test]
    fn test_peak_memory_tracking() {
        let controller = Arc::new(MemoryController::with_budget(1000));

        // Reserve and release
        {
            let mut res = controller.create_reservation();
            res.try_grow(800);
            assert_eq!(controller.peak_memory(), 800);
        }

        // Memory released but peak preserved
        assert_eq!(controller.reserved(), 0);
        assert_eq!(controller.peak_memory(), 800);

        // New peak
        {
            let mut res = controller.create_reservation();
            res.try_grow(900);
            assert_eq!(controller.peak_memory(), 900);
        }

        // Lower usage doesn't affect peak
        {
            let mut res = controller.create_reservation();
            res.try_grow(100);
            assert_eq!(controller.peak_memory(), 900);
        }
    }

    #[test]
    fn test_memory_stats_display() {
        let stats = MemoryStats {
            budget_bytes: 1024 * 1024 * 1024,
            reserved_bytes: 512 * 1024 * 1024,
            peak_bytes: 950 * 1024 * 1024,
            bytes_spilled: 2 * 1024 * 1024 * 1024,
            spill_count: 3,
            active_reservations: 2,
            spill_threshold: 0.8,
        };

        let display = format!("{}", stats);
        assert!(display.contains("512.00MB"));
        assert!(display.contains("1.00GB"));
        assert!(display.contains("50.0%"));
        assert!(display.contains("950.00MB"));
        assert!(display.contains("2.00GB"));
        assert!(display.contains("3 ops"));
    }

    #[test]
    fn test_spill_count_tracking() {
        let controller = Arc::new(MemoryController::with_budget(1024));

        assert_eq!(controller.spill_count(), 0);
        assert_eq!(controller.bytes_spilled(), 0);

        controller.record_spill(100);
        assert_eq!(controller.spill_count(), 1);
        assert_eq!(controller.bytes_spilled(), 100);

        controller.record_spill(200);
        assert_eq!(controller.spill_count(), 2);
        assert_eq!(controller.bytes_spilled(), 300);

        controller.record_spill(50);
        assert_eq!(controller.spill_count(), 3);
        assert_eq!(controller.bytes_spilled(), 350);
    }

    #[test]
    fn test_format_bytes_helper() {
        // Test the format_bytes function through MemoryStats Display
        let make_stats = |bytes| MemoryStats {
            budget_bytes: bytes,
            reserved_bytes: bytes,
            peak_bytes: bytes,
            bytes_spilled: 0,
            spill_count: 0,
            active_reservations: 0,
            spill_threshold: 0.8,
        };

        let s = format!("{}", make_stats(500));
        assert!(s.contains("500B"));

        let s = format!("{}", make_stats(2048));
        assert!(s.contains("2.00KB"));

        let s = format!("{}", make_stats(5 * 1024 * 1024));
        assert!(s.contains("5.00MB"));

        let s = format!("{}", make_stats(3 * 1024 * 1024 * 1024));
        assert!(s.contains("3.00GB"));
    }
}
