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
}

impl MemoryController {
    /// Create a new memory controller with the given configuration
    pub fn new(config: MemoryConfig) -> Self {
        Self {
            config,
            reserved: AtomicUsize::new(0),
            active_reservations: AtomicUsize::new(0),
            bytes_spilled: AtomicUsize::new(0),
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
    }

    /// Get total bytes spilled to disk
    pub fn bytes_spilled(&self) -> usize {
        self.bytes_spilled.load(Ordering::Relaxed)
    }

    /// Get number of active reservations
    pub fn active_reservations(&self) -> usize {
        self.active_reservations.load(Ordering::Relaxed)
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
                Ok(_) => return true,
                Err(_) => continue, // Retry on contention
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
}
