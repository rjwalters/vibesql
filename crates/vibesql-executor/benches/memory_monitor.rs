//! Memory monitoring utility for benchmark graceful degradation
//!
//! Provides runtime memory monitoring to detect memory pressure before
//! the OS sends SIGKILL, allowing graceful handling of memory-intensive
//! operations in benchmarks.
//!
//! # Usage
//!
//! ```ignore
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

use sysinfo::System;
use std::sync::atomic::{AtomicU64, Ordering};

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
    High {
        stats: MemoryStats,
        threshold_percent: f64,
    },
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
    pub fn current_stats(&mut self) -> MemoryStats {
        self.system.refresh_memory();

        let total = self.system.total_memory();
        let available = self.system.available_memory();
        let used = total.saturating_sub(available);
        let usage_percent = if total > 0 {
            (used as f64 / total as f64) * 100.0
        } else {
            0.0
        };

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
            MemoryPressure::High {
                stats,
                threshold_percent: self.threshold_percent,
            }
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
    Skipped {
        reason: String,
        stats: MemoryStats,
    },
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
}
