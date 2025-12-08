//! Performance profiling and debug utilities for understanding bottlenecks
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
//! - `DML_COST_DEBUG` - DML cost model reasoning
//! - `SCAN_PATH_VERBOSE` - Scan path selection (index vs table scan)
//! - `INDEX_SELECT_DEBUG` - Index selection decisions
//!
//! ## Phase Timing Flags
//!
//! - `DELETE_PROFILE` - Delete operation phase timing
//! - `JOIN_PROFILE` - Join execution phase timing
//! - `RANGE_SCAN_PROFILE` - Range scan phase timing

use instant::Instant;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

/// Global flag to enable/disable profiling (disabled by default)
/// Set VIBESQL_PROFILE=1 environment variable to enable
static PROFILING_ENABLED: AtomicBool = AtomicBool::new(false);

/// Debug level from VIBESQL_DEBUG environment variable
/// 0 = disabled, 1 = all, other values for specific categories
static DEBUG_LEVEL: AtomicU8 = AtomicU8::new(0);

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

/// Initialize profiling and debug flags based on environment variables
pub fn init() {
    // Initialize VIBESQL_PROFILE
    if std::env::var("VIBESQL_PROFILE").is_ok() {
        PROFILING_ENABLED.store(true, Ordering::Relaxed);
        eprintln!("[PROFILE] Profiling enabled");
    }

    // Initialize VIBESQL_DEBUG
    if let Ok(value) = std::env::var("VIBESQL_DEBUG") {
        let level = match value.to_lowercase().as_str() {
            "1" | "true" | "all" => 1,
            "optimizer" => 2,
            "scan" => 3,
            "dml" => 4,
            _ => {
                // Try parsing as number
                value.parse::<u8>().unwrap_or(0)
            }
        };
        DEBUG_LEVEL.store(level, Ordering::Relaxed);
        if level > 0 {
            let category = match level {
                1 => "all",
                2 => "optimizer",
                3 => "scan",
                4 => "dml",
                _ => "custom",
            };
            eprintln!("[DEBUG] VibeSQL debug enabled: category={}", category);
        }
    }
}

/// Check if profiling is enabled
pub fn is_enabled() -> bool {
    PROFILING_ENABLED.load(Ordering::Relaxed)
}

/// Get the current debug category
pub fn debug_category() -> DebugCategory {
    match DEBUG_LEVEL.load(Ordering::Relaxed) {
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

/// Check if optimizer debug logging is enabled
/// Returns true if VIBESQL_DEBUG=1, VIBESQL_DEBUG=optimizer, or individual flag is set
#[inline]
pub fn is_optimizer_debug_enabled() -> bool {
    is_debug_enabled(DebugCategory::Optimizer)
}

/// Check if scan path debug logging is enabled
/// Returns true if VIBESQL_DEBUG=1, VIBESQL_DEBUG=scan, SCAN_PATH_VERBOSE=1,
/// or legacy TABLE_SCAN_DEBUG/COLUMNAR_DEBUG flags
#[inline]
pub fn is_scan_debug_enabled() -> bool {
    // Use thread-local cache to avoid repeated env var lookups
    SCAN_PATH_VERBOSE_ENABLED.with(|cache| {
        if let Some(enabled) = cache.get() {
            return enabled;
        }

        let enabled = is_debug_enabled(DebugCategory::Scan)
            || std::env::var("SCAN_PATH_VERBOSE").is_ok()
            || std::env::var("TABLE_SCAN_DEBUG").is_ok()
            || std::env::var("COLUMNAR_DEBUG").is_ok();

        cache.set(Some(enabled));
        enabled
    })
}

/// Check if DML debug logging is enabled
/// Returns true if VIBESQL_DEBUG=1, VIBESQL_DEBUG=dml, or DML_COST_DEBUG is set
#[inline]
pub fn is_dml_debug_enabled() -> bool {
    is_debug_enabled(DebugCategory::Dml) || std::env::var("DML_COST_DEBUG").is_ok()
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
            eprintln!(
                "[PROFILE] {} took {:.3}ms ({:.0}µs)",
                self.label,
                elapsed.as_secs_f64() * 1000.0,
                elapsed.as_micros()
            );
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
