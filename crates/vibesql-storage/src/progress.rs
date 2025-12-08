//! Progress tracking for long-running DDL operations
//!
//! This module provides progress indicators for operations like:
//! - Index creation (rows processed, estimated time)
//! - Statistics computation
//! - Large INSERT/UPDATE operations
//!
//! Progress output can be controlled via the `DDL_PROGRESS` environment variable.
//! Set `DDL_PROGRESS=1` to enable progress indicators.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

/// Default interval between progress reports (5 seconds)
const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// Minimum number of items before showing progress
/// (avoid noise for tiny operations)
const MIN_ITEMS_FOR_PROGRESS: usize = 1000;

/// Progress enabled states
const STATE_UNINITIALIZED: u8 = 0;
const STATE_DISABLED: u8 = 1;
const STATE_ENABLED: u8 = 2;

/// Global flag for whether progress reporting is enabled (lazy initialized)
static PROGRESS_STATE: AtomicU8 = AtomicU8::new(STATE_UNINITIALIZED);

/// Check if progress reporting is enabled
///
/// On first call, reads the `DDL_PROGRESS` environment variable.
/// Subsequent calls use the cached value.
pub fn is_enabled() -> bool {
    let state = PROGRESS_STATE.load(Ordering::Relaxed);
    if state == STATE_UNINITIALIZED {
        // Initialize from environment variable
        let enabled = std::env::var("DDL_PROGRESS")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);
        let new_state = if enabled { STATE_ENABLED } else { STATE_DISABLED };
        // Use compare_exchange to handle race conditions
        let _ = PROGRESS_STATE.compare_exchange(
            STATE_UNINITIALIZED,
            new_state,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        enabled
    } else {
        state == STATE_ENABLED
    }
}

/// Enable progress reporting programmatically (useful for tests)
pub fn enable() {
    PROGRESS_STATE.store(STATE_ENABLED, Ordering::Relaxed);
}

/// Disable progress reporting programmatically
pub fn disable() {
    PROGRESS_STATE.store(STATE_DISABLED, Ordering::Relaxed);
}

/// Progress tracker for long-running operations
///
/// Tracks progress and emits log messages at regular intervals.
///
/// # Example
///
/// ```text
/// use vibesql_storage::progress::ProgressTracker;
///
/// let mut tracker = ProgressTracker::new("Creating index 'idx_users'", Some(1_000_000));
/// for (i, row) in rows.iter().enumerate() {
///     // ... process row ...
///     tracker.update(i + 1);
/// }
/// tracker.finish();
/// ```
pub struct ProgressTracker {
    /// Description of the operation
    operation: String,
    /// Total number of items (if known)
    total: Option<usize>,
    /// Current progress count
    current: usize,
    /// When the operation started
    start_time: Instant,
    /// When we last reported progress
    last_report_time: Instant,
    /// Interval between reports
    report_interval: Duration,
    /// Whether we've reported anything yet
    has_reported: bool,
}

impl ProgressTracker {
    /// Create a new progress tracker
    ///
    /// # Arguments
    /// * `operation` - Description of the operation (e.g., "Creating index 'idx_users'")
    /// * `total` - Total number of items if known
    pub fn new(operation: impl Into<String>, total: Option<usize>) -> Self {
        let now = Instant::now();
        Self {
            operation: operation.into(),
            total,
            current: 0,
            start_time: now,
            last_report_time: now,
            report_interval: DEFAULT_REPORT_INTERVAL,
            has_reported: false,
        }
    }

    /// Set a custom report interval
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.report_interval = interval;
        self
    }

    /// Update the progress count
    ///
    /// Automatically reports if enough time has passed since the last report.
    pub fn update(&mut self, current: usize) {
        self.current = current;

        // Only report if enabled and we have enough items
        if !is_enabled() {
            return;
        }

        // Skip small operations
        if let Some(total) = self.total {
            if total < MIN_ITEMS_FOR_PROGRESS {
                return;
            }
        }

        // Check if enough time has passed since last report
        let now = Instant::now();
        if now.duration_since(self.last_report_time) >= self.report_interval {
            self.report_progress();
            self.last_report_time = now;
        }
    }

    /// Force a progress report regardless of timing
    pub fn report(&mut self) {
        if !is_enabled() {
            return;
        }
        self.report_progress();
        self.last_report_time = Instant::now();
    }

    /// Called when the operation is complete
    pub fn finish(&mut self) {
        if !is_enabled() {
            return;
        }

        // Skip small operations
        if let Some(total) = self.total {
            if total < MIN_ITEMS_FOR_PROGRESS && !self.has_reported {
                return;
            }
        }

        let elapsed = self.start_time.elapsed();
        let elapsed_str = format_duration(elapsed);

        if let Some(total) = self.total {
            let rate = if elapsed.as_secs_f64() > 0.0 {
                self.current as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            };
            log::info!(
                "{}: completed {} / {} items (100%) [{} elapsed, {:.0} items/sec]",
                self.operation,
                format_number(self.current),
                format_number(total),
                elapsed_str,
                rate
            );
        } else {
            log::info!(
                "{}: completed {} items [{} elapsed]",
                self.operation,
                format_number(self.current),
                elapsed_str
            );
        }
    }

    /// Internal method to report current progress
    fn report_progress(&mut self) {
        self.has_reported = true;
        let elapsed = self.start_time.elapsed();
        let elapsed_str = format_duration(elapsed);

        if let Some(total) = self.total {
            let percentage = if total > 0 {
                (self.current as f64 / total as f64 * 100.0) as u8
            } else {
                0
            };

            // Estimate remaining time
            let remaining_str = if self.current > 0 && elapsed.as_secs_f64() > 0.0 {
                let rate = self.current as f64 / elapsed.as_secs_f64();
                let remaining_items = total.saturating_sub(self.current);
                let remaining_secs = remaining_items as f64 / rate;
                format!(", ~{} remaining", format_duration(Duration::from_secs_f64(remaining_secs)))
            } else {
                String::new()
            };

            log::info!(
                "{}: {} / {} items ({}%) [{} elapsed{}]",
                self.operation,
                format_number(self.current),
                format_number(total),
                percentage,
                elapsed_str,
                remaining_str
            );
        } else {
            log::info!(
                "{}: {} items [{} elapsed]",
                self.operation,
                format_number(self.current),
                elapsed_str
            );
        }
    }
}

/// Format a number with thousands separators
fn format_number(n: usize) -> String {
    let s = n.to_string();
    let chars: Vec<char> = s.chars().collect();
    let mut result = String::new();

    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(*c);
    }

    result
}

/// Format a duration in a human-readable way
fn format_duration(d: Duration) -> String {
    let total_secs = d.as_secs();

    if total_secs < 60 {
        format!("{}s", total_secs)
    } else if total_secs < 3600 {
        let mins = total_secs / 60;
        let secs = total_secs % 60;
        format!("{}m {}s", mins, secs)
    } else {
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        let secs = total_secs % 60;
        format!("{}h {}m {}s", hours, mins, secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(100), "100");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1234567), "1,234,567");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_secs(5)), "5s");
        assert_eq!(format_duration(Duration::from_secs(65)), "1m 5s");
        assert_eq!(format_duration(Duration::from_secs(3665)), "1h 1m 5s");
    }

    #[test]
    fn test_progress_tracker_disabled() {
        disable();
        let mut tracker = ProgressTracker::new("Test operation", Some(100));
        tracker.update(50);
        tracker.finish();
        // Should complete without errors (and no output since disabled)
    }

    #[test]
    fn test_progress_tracker_enabled() {
        enable();
        let mut tracker = ProgressTracker::new("Test operation", Some(10000))
            .with_interval(Duration::from_millis(1)); // Short interval for testing
        tracker.update(5000);
        tracker.report();
        tracker.finish();
        disable();
    }
}
