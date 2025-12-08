//! Performance profiling and debug utilities for understanding bottlenecks
//!
//! This module provides profiling infrastructure that integrates with the
//! structured debug output system. When `VIBESQL_DEBUG_FORMAT=json`, profiling
//! output is emitted as JSON for machine parsing.
//!
//! # Environment Variables
//!
//! ## Umbrella Debug Flag
//!
//! `VIBESQL_DEBUG` provides unified control over all semantic debug logging:
//!
//! - `VIBESQL_DEBUG=1` - Enable all semantic logging (optimizer decisions)
//! - `VIBESQL_DEBUG=optimizer` - Enable only optimizer-related logging
//! - `VIBESQL_DEBUG=scan` - Enable only scan path logging
//! - `VIBESQL_DEBUG=dml` - Enable only DML-related logging
//!
//! ## Individual Flags (for fine-grained control)
//!
//! These flags work independently or are auto-enabled by `VIBESQL_DEBUG`:
//!
//! - `JOIN_REORDER_VERBOSE` - Join reorder decisions and costs
//! - `SUBQUERY_TRANSFORM_VERBOSE` - Subquery-to-join transformations
//! - `TABLE_ELIM_VERBOSE` - Table elimination decisions
//! - `SCAN_PATH_VERBOSE` - Scan path selection (index vs table scan)
//! - `INDEX_SELECT_DEBUG` - Index selection decisions
//!
//! ## Phase Timing Flags
//!
//! - `DELETE_PROFILE` - Delete operation phase timing
//! - `JOIN_PROFILE` - Join execution phase timing
//! - `RANGE_SCAN_PROFILE` - Range scan phase timing

use crate::debug_output::{self, Category, DebugEvent};
use instant::Instant;
use std::sync::LazyLock;

/// Debug level from VIBESQL_DEBUG environment variable (lazily initialized)
/// 0 = disabled, 1 = all, 2 = optimizer, 3 = scan, 4 = dml
static DEBUG_LEVEL: LazyLock<u8> = LazyLock::new(|| {
    std::env::var("VIBESQL_DEBUG")
        .ok()
        .and_then(|v| match v.to_lowercase().as_str() {
            "1" | "true" | "all" => Some(1),
            "optimizer" => Some(2),
            "scan" => Some(3),
            "dml" => Some(4),
            _ => v.parse().ok(),
        })
        .unwrap_or(0)
});

/// Profiling enabled flag (lazily initialized from VIBESQL_PROFILE)
static PROFILING_ENABLED: LazyLock<bool> =
    LazyLock::new(|| std::env::var("VIBESQL_PROFILE").is_ok());

/// Debug categories enabled via VIBESQL_DEBUG
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DebugCategory {
    /// All debug logging enabled
    All,
    /// Optimizer decisions (join reorder, subquery transform, table elimination)
    Optimizer,
    /// Scan path selection (index vs table scan, columnar)
    Scan,
    /// DML operations (cost model, delete profiling)
    Dml,
    /// Disabled
    None,
}

// Thread-local cache for debug checks to avoid repeated env var lookups
thread_local! {
    static SCAN_PATH_VERBOSE_ENABLED: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// Initialize profiling based on environment variable.
/// Also initializes the debug output format.
pub fn init() {
    // Initialize debug output format first
    debug_output::init();

    // Force lazy initialization of profiling flag
    if *PROFILING_ENABLED {
        debug_output::debug_event(Category::Profile, "init", "PROFILE")
            .text("Profiling enabled")
            .field_bool("enabled", true)
            .emit();
    }
}

/// Check if profiling is enabled
pub fn is_enabled() -> bool {
    *PROFILING_ENABLED
}

/// Get the current debug category
pub fn debug_category() -> DebugCategory {
    match *DEBUG_LEVEL {
        0 => DebugCategory::None,
        1 => DebugCategory::All,
        2 => DebugCategory::Optimizer,
        3 => DebugCategory::Scan,
        4 => DebugCategory::Dml,
        _ => DebugCategory::None,
    }
}

/// Check if a specific debug category is enabled
pub fn is_debug_enabled(category: DebugCategory) -> bool {
    let current = debug_category();
    current == DebugCategory::All || current == category
}

/// Check if scan path debug logging is enabled
/// Returns true if VIBESQL_DEBUG=1, VIBESQL_DEBUG=scan, or SCAN_PATH_VERBOSE=1
#[inline]
pub fn is_scan_debug_enabled() -> bool {
    // Use thread-local cache to avoid repeated env var lookups
    SCAN_PATH_VERBOSE_ENABLED.with(|cache| {
        if let Some(enabled) = cache.get() {
            return enabled;
        }

        let enabled =
            is_debug_enabled(DebugCategory::Scan) || std::env::var("SCAN_PATH_VERBOSE").is_ok();

        cache.set(Some(enabled));
        enabled
    })
}

/// A profiling timer that logs elapsed time when dropped
pub struct ProfileTimer {
    label: &'static str,
    start: Instant,
    enabled: bool,
}

impl ProfileTimer {
    /// Create a new profiling timer
    pub fn new(label: &'static str) -> Self {
        let enabled = is_enabled();
        Self { label, start: Instant::now(), enabled }
    }
}

impl Drop for ProfileTimer {
    fn drop(&mut self) {
        if self.enabled {
            let elapsed = self.start.elapsed();
            DebugEvent::new(Category::Profile, "timer", "PROFILE")
                .text(format!(
                    "{} took {:.3}ms ({:.0}µs)",
                    self.label,
                    elapsed.as_secs_f64() * 1000.0,
                    elapsed.as_micros()
                ))
                .field_str("label", self.label)
                .field_duration_ms("duration_ms", elapsed)
                .field_duration_us("duration_us", elapsed)
                .emit();
        }
    }
}

/// Macro to create a profiling scope
#[macro_export]
macro_rules! profile {
    ($label:expr) => {
        let _timer = $crate::profiling::ProfileTimer::new($label);
    };
}
