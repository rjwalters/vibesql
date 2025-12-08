//! Memory monitoring utility for benchmark graceful degradation
//!
//! Provides runtime memory monitoring to detect memory pressure before
//! the OS sends SIGKILL, allowing graceful handling of memory-intensive
//! operations in benchmarks.
//!
//! # Usage
//!
//! ```text
//! use memory_monitor::MemoryMonitor;
//!
//! let monitor = MemoryMonitor::new();
//!
//! // Check before executing a memory-intensive query
//! if monitor.check_pressure() {
//!     eprintln!("[SKIP] Query skipped due to memory pressure");
//!     return;
//! }
//!
//! // Get current memory stats for logging
//! let stats = monitor.current_stats();
//! eprintln!("Memory: {} / {} ({:.1}% used)",
//!     format_bytes(stats.used_bytes),
//!     format_bytes(stats.total_bytes),
//!     stats.usage_percent
//! );
//! ```

// Allow dead_code for API completeness - these are public utilities that may be used
// by other benchmarks or future enhancements
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use sysinfo::System;

/// macOS-specific memory detection using host_statistics64
///
/// The sysinfo crate's `available_memory()` can return incorrect values (often 0)
/// on macOS, particularly on Apple Silicon systems with large amounts of RAM.
/// This module provides a fallback using direct Mach API calls.
#[cfg(target_os = "macos")]
mod macos_memory {
    use std::mem::MaybeUninit;

    // Mach types and constants
    type MachPort = u32;
    type KernReturn = i32;

    const HOST_VM_INFO64: i32 = 4;
    const HOST_VM_INFO64_COUNT: u32 = 38; // sizeof(vm_statistics64_data_t) / sizeof(integer_t)
    const KERN_SUCCESS: i32 = 0;

    #[repr(C)]
    #[derive(Debug, Default)]
    struct VmStatistics64 {
        free_count: u64,
        active_count: u64,
        inactive_count: u64,
        wire_count: u64,
        zero_fill_count: u64,
        reactivations: u64,
        pageins: u64,
        pageouts: u64,
        faults: u64,
        cow_faults: u64,
        lookups: u64,
        hits: u64,
        purges: u64,
        purgeable_count: u64,
        speculative_count: u64,
        decompressions: u64,
        compressions: u64,
        swapins: u64,
        swapouts: u64,
        compressor_page_count: u64,
        throttled_count: u64,
        external_page_count: u64,
        internal_page_count: u64,
        total_uncompressed_pages_in_compressor: u64,
    }

    extern "C" {
        fn mach_host_self() -> MachPort;
        fn host_statistics64(
            host: MachPort,
            flavor: i32,
            host_info: *mut VmStatistics64,
            count: *mut u32,
        ) -> KernReturn;
        // vm_page_size is a global variable, not a function
        static vm_page_size: usize;
    }

    /// Get available memory on macOS using Mach APIs
    /// Returns (total_bytes, available_bytes) or None if the call fails
    pub fn get_available_memory() -> Option<(u64, u64)> {
        unsafe {
            let host = mach_host_self();
            let page_size = vm_page_size as u64;

            let mut vm_stats = MaybeUninit::<VmStatistics64>::zeroed();
            let mut count = HOST_VM_INFO64_COUNT;

            let result = host_statistics64(host, HOST_VM_INFO64, vm_stats.as_mut_ptr(), &mut count);

            if result != KERN_SUCCESS {
                return None;
            }

            let stats = vm_stats.assume_init();

            // Available memory includes:
            // - Free pages (never used or explicitly freed)
            // - Inactive pages (recently used but can be reclaimed)
            // - Purgeable pages (cached but deletable)
            // - Speculative pages (prefetched but unused)
            let available_pages = stats.free_count
                + stats.inactive_count
                + stats.purgeable_count
                + stats.speculative_count;

            // Total physical memory pages
            let total_pages = stats.free_count
                + stats.active_count
                + stats.inactive_count
                + stats.wire_count
                + stats.compressor_page_count
                + stats.speculative_count;

            // Use saturating multiplication to avoid overflow in debug mode
            let available_bytes = available_pages.saturating_mul(page_size);
            let total_bytes = total_pages.saturating_mul(page_size);

            Some((total_bytes, available_bytes))
        }
    }
}

/// Default memory pressure threshold (80% of available RAM)
const DEFAULT_THRESHOLD_PERCENT: f64 = 80.0;

/// Environment variable to override the default threshold
const THRESHOLD_ENV_VAR: &str = "VIBESQL_MEMORY_THRESHOLD";

/// Memory statistics snapshot
#[derive(Debug, Clone)]
pub struct MemoryStats {
    /// Total system memory in bytes
    pub total_bytes: u64,
    /// Currently used memory in bytes
    pub used_bytes: u64,
    /// Available memory in bytes
    pub available_bytes: u64,
    /// Memory usage as a percentage (0-100)
    pub usage_percent: f64,
}

/// Memory pressure result
#[derive(Debug, Clone)]
pub enum MemoryPressure {
    /// Memory usage is within acceptable limits
    Ok(MemoryStats),
    /// Memory usage exceeds threshold
    High { stats: MemoryStats, threshold_percent: f64 },
}

impl MemoryPressure {
    /// Returns true if memory pressure is high
    pub fn is_high(&self) -> bool {
        matches!(self, MemoryPressure::High { .. })
    }

    /// Get the underlying memory stats
    pub fn stats(&self) -> &MemoryStats {
        match self {
            MemoryPressure::Ok(stats) => stats,
            MemoryPressure::High { stats, .. } => stats,
        }
    }
}

/// Memory monitor for detecting memory pressure during benchmark execution
pub struct MemoryMonitor {
    /// Threshold as a percentage (0-100)
    threshold_percent: f64,
    /// High-water mark for memory usage during benchmark run
    high_water_mark_bytes: AtomicU64,
    /// System info handle
    system: System,
}

impl MemoryMonitor {
    /// Create a new memory monitor with default threshold (80%)
    ///
    /// The threshold can be overridden via the VIBESQL_MEMORY_THRESHOLD environment
    /// variable (value should be a percentage like "75" for 75%).
    pub fn new() -> Self {
        let threshold_percent = std::env::var(THRESHOLD_ENV_VAR)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(DEFAULT_THRESHOLD_PERCENT)
            .clamp(10.0, 99.0);

        Self {
            threshold_percent,
            high_water_mark_bytes: AtomicU64::new(0),
            system: System::new_all(),
        }
    }

    /// Create a memory monitor with a specific threshold percentage
    pub fn with_threshold(threshold_percent: f64) -> Self {
        Self {
            threshold_percent: threshold_percent.clamp(10.0, 99.0),
            high_water_mark_bytes: AtomicU64::new(0),
            system: System::new_all(),
        }
    }

    /// Refresh memory information and get current stats
    ///
    /// On macOS, this uses a fallback to direct Mach API calls when sysinfo
    /// returns suspicious values (like 0 available memory), which can happen
    /// on Apple Silicon systems with large amounts of RAM.
    pub fn current_stats(&mut self) -> MemoryStats {
        self.system.refresh_memory();

        let total = self.system.total_memory();
        let sysinfo_available = self.system.available_memory();

        // On macOS, sysinfo can return 0 for available_memory on some systems
        // (particularly Apple Silicon with high RAM). Use our native fallback
        // when this happens.
        #[cfg(target_os = "macos")]
        let (total, available) = {
            // If sysinfo returns 0 or suspiciously low available memory (< 1% of total),
            // fall back to native Mach API
            if sysinfo_available == 0 || (total > 0 && sysinfo_available < total / 100) {
                if let Some((_native_total, native_available)) =
                    macos_memory::get_available_memory()
                {
                    // Use sysinfo's total (from sysctl) as it's more reliable,
                    // but use our native available memory calculation
                    (total, native_available.min(total))
                } else {
                    (total, sysinfo_available)
                }
            } else {
                (total, sysinfo_available)
            }
        };

        #[cfg(not(target_os = "macos"))]
        let available = sysinfo_available;

        let used = total.saturating_sub(available);
        let usage_percent = if total > 0 { (used as f64 / total as f64) * 100.0 } else { 0.0 };

        // Update high-water mark
        self.high_water_mark_bytes.fetch_max(used, Ordering::Relaxed);

        MemoryStats {
            total_bytes: total,
            used_bytes: used,
            available_bytes: available,
            usage_percent,
        }
    }

    /// Check if memory pressure exceeds the threshold
    ///
    /// Returns `MemoryPressure::High` if current usage exceeds the threshold,
    /// otherwise returns `MemoryPressure::Ok` with current stats.
    pub fn check_pressure(&mut self) -> MemoryPressure {
        let stats = self.current_stats();

        if stats.usage_percent >= self.threshold_percent {
            MemoryPressure::High { stats, threshold_percent: self.threshold_percent }
        } else {
            MemoryPressure::Ok(stats)
        }
    }

    /// Quick check if memory pressure is high (convenience method)
    pub fn is_pressure_high(&mut self) -> bool {
        self.check_pressure().is_high()
    }

    /// Get the configured threshold percentage
    pub fn threshold_percent(&self) -> f64 {
        self.threshold_percent
    }

    /// Get the high-water mark (peak memory usage) in bytes
    pub fn high_water_mark_bytes(&self) -> u64 {
        self.high_water_mark_bytes.load(Ordering::Relaxed)
    }

    /// Reset the high-water mark (call between benchmark groups)
    pub fn reset_high_water_mark(&self) {
        self.high_water_mark_bytes.store(0, Ordering::Relaxed);
    }
}

impl Default for MemoryMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute optimal parallelism level based on available memory
///
/// Returns a parallelism level (1, 2, 4, or 8) based on the percentage
/// of free system memory. This allows benchmark runners to scale their
/// parallelism dynamically without risking OOM.
///
/// | Free Memory | Parallelism |
/// |-------------|-------------|
/// | > 70%       | 8 workers   |
/// | 50-70%      | 4 workers   |
/// | 30-50%      | 2 workers   |
/// | < 30%       | 1 (sequential) |
///
/// The `PARALLEL_QUERIES` environment variable can override this:
/// - `PARALLEL_QUERIES=0` disables parallelism (returns 1)
/// - `PARALLEL_QUERIES=N` forces N workers (clamped to 1-16)
pub fn compute_parallelism() -> usize {
    // Check for environment variable override
    if let Ok(val) = std::env::var("PARALLEL_QUERIES") {
        if let Ok(n) = val.parse::<usize>() {
            if n == 0 {
                return 1; // Explicit disable
            }
            return n.clamp(1, 16);
        }
    }

    // Get current memory stats
    let mut monitor = MemoryMonitor::new();
    let stats = monitor.current_stats();

    // Calculate free percentage
    let free_percent = if stats.total_bytes > 0 {
        (stats.available_bytes as f64 / stats.total_bytes as f64) * 100.0
    } else {
        0.0
    };

    // Determine parallelism based on free memory
    let parallelism = if free_percent > 70.0 {
        8
    } else if free_percent > 50.0 {
        4
    } else if free_percent > 30.0 {
        2
    } else {
        1
    };

    // Log the decision for debugging
    if std::env::var("PARALLEL_DEBUG").is_ok() {
        eprintln!(
            "[PARALLEL] Free memory: {:.1}% ({} / {}), parallelism: {}",
            free_percent,
            format_bytes(stats.available_bytes),
            format_bytes(stats.total_bytes),
            parallelism
        );
    }

    parallelism
}

/// Format bytes as a human-readable string
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Result of a memory-guarded operation
#[derive(Debug)]
pub enum GuardedResult<T> {
    /// Operation completed successfully
    Success(T),
    /// Operation skipped due to memory pressure
    Skipped { reason: String, stats: MemoryStats },
}

impl<T> GuardedResult<T> {
    /// Returns true if the operation was skipped
    pub fn was_skipped(&self) -> bool {
        matches!(self, GuardedResult::Skipped { .. })
    }

    /// Convert to Option, returning None if skipped
    pub fn ok(self) -> Option<T> {
        match self {
            GuardedResult::Success(val) => Some(val),
            GuardedResult::Skipped { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::{format_bytes, MemoryMonitor};

    #[test]
    fn test_memory_monitor_creation() {
        let monitor = MemoryMonitor::new();
        assert!(monitor.threshold_percent() >= 10.0);
        assert!(monitor.threshold_percent() <= 99.0);
    }

    #[test]
    fn test_custom_threshold() {
        let monitor = MemoryMonitor::with_threshold(50.0);
        assert_eq!(monitor.threshold_percent(), 50.0);
    }

    #[test]
    fn test_threshold_clamping() {
        let low = MemoryMonitor::with_threshold(5.0);
        assert_eq!(low.threshold_percent(), 10.0);

        let high = MemoryMonitor::with_threshold(150.0);
        assert_eq!(high.threshold_percent(), 99.0);
    }

    #[test]
    fn test_current_stats() {
        let mut monitor = MemoryMonitor::new();
        let stats = monitor.current_stats();

        assert!(stats.total_bytes > 0);
        assert!(stats.usage_percent >= 0.0);
        assert!(stats.usage_percent <= 100.0);

        // The fix for issue #3197: verify that available_bytes is not 0
        // (which would incorrectly indicate 100% memory usage)
        assert!(
            stats.available_bytes > 0,
            "available_bytes should not be 0 (was {}, total: {}, usage: {:.1}%)",
            stats.available_bytes,
            stats.total_bytes,
            stats.usage_percent
        );

        // Sanity check: usage should not be 100% on a functioning system
        assert!(
            stats.usage_percent < 99.0,
            "Memory usage should not be 100% on a normal system (was {:.1}%)",
            stats.usage_percent
        );
    }

    #[test]
    fn test_high_water_mark() {
        let mut monitor = MemoryMonitor::new();

        // Get initial stats to set high-water mark
        let _ = monitor.current_stats();
        let hwm1 = monitor.high_water_mark_bytes();
        assert!(hwm1 > 0);

        // Reset and verify
        monitor.reset_high_water_mark();
        assert_eq!(monitor.high_water_mark_bytes(), 0);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1_572_864), "1.50 MB");
        assert_eq!(format_bytes(1_610_612_736), "1.50 GB");
    }

    #[test]
    fn test_compute_parallelism() {
        use super::compute_parallelism;

        // Test that compute_parallelism returns a valid value (1, 2, 4, or 8)
        let parallelism = compute_parallelism();
        assert!(
            parallelism == 1 || parallelism == 2 || parallelism == 4 || parallelism == 8,
            "parallelism should be 1, 2, 4, or 8, got {}",
            parallelism
        );

        // At minimum, parallelism should be 1
        assert!(parallelism >= 1, "parallelism should be at least 1");
    }

    /// Test macOS-specific memory detection fallback
    #[cfg(target_os = "macos")]
    #[test]
    fn test_macos_native_memory_detection() {
        use super::macos_memory;

        // Verify our native Mach API implementation works
        let result = macos_memory::get_available_memory();
        assert!(result.is_some(), "macOS native memory detection should succeed");

        let (total, available) = result.unwrap();
        assert!(total > 0, "total memory should be positive");
        assert!(available > 0, "available memory should be positive");
        assert!(available <= total, "available should not exceed total");

        // On a running system, there should be some available memory
        let usage_percent = ((total - available) as f64 / total as f64) * 100.0;
        assert!(
            usage_percent < 99.0,
            "Native macOS detection: usage should not be 100% (was {:.1}%)",
            usage_percent
        );
    }
}
