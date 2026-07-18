// ============================================================================
// Per-Table Access-Pattern Signal (Phase 1 — measurement only)
// ============================================================================
//
// A cheap, always-on per-table usage counter used to make the columnar
// representation cache adaptive in a later phase (#6199, Phase 2). This module
// only *observes* how each table is used; it changes no dispatch, promotion, or
// eviction behavior. Phase 2 is the sole consumer.
//
// The read/scan path operates on `&Database` (a shared immutable ref) and scans
// tables `&self`, so the signal must use interior mutability — it must never be
// threaded through `&mut`. It follows the existing `Database.search_count:
// AtomicU64` precedent (mutated through `&self` with `Ordering::Relaxed`) and,
// like `search_count`, is in-memory only: it resets on open and is never
// persisted to disk.
//
// Home: `Database.table_access_signals`, an interior-mutable
// `RwLock<HashMap<String, TableAccessSignal>>` keyed by the uppercased table
// name (mirroring `normalize_cache_key` in `columnar_cache.rs`). Native builds
// use `parking_lot::RwLock`; wasm uses `std::sync::RwLock`, matching the
// `columnar_cache.rs` cfg split.

use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(not(target_arch = "wasm32"))]
use parking_lot::RwLock;
#[cfg(target_arch = "wasm32")]
use std::sync::RwLock;

use super::core::Database;

/// Normalize a table name into an access-signal map key (case-insensitive).
///
/// Mirrors `normalize_cache_key` in `columnar_cache.rs` so a table maps to the
/// same signal entry regardless of the casing used at each call site.
#[inline]
pub(super) fn normalize_signal_key(table_name: &str) -> String {
    table_name.to_uppercase()
}

/// Interior-mutable per-table access counters.
///
/// Every field is an `AtomicU64` mutated through a shared `&` with
/// `Ordering::Relaxed` — the counters are advisory statistics, so relaxed
/// ordering (no cross-counter synchronization) is sufficient and cheapest.
#[derive(Debug, Default)]
pub(super) struct TableAccessSignal {
    /// Analytical scans of the table (the columnar-eligible full-scan path).
    pub(super) scan_count: AtomicU64,
    /// Primary-key / point lookups of the table.
    pub(super) point_lookup_count: AtomicU64,
    /// Running sum of projected column counts across recorded scans.
    pub(super) projection_width_sum: AtomicU64,
    /// Number of scans that contributed to `projection_width_sum`, so the
    /// average projection width can be derived.
    pub(super) projection_width_n: AtomicU64,
    /// Writes (INSERT/UPDATE/DELETE/TRUNCATE/ALTER) routed through the DML
    /// invalidation funnel.
    pub(super) write_count: AtomicU64,
}

/// Plain `Copy` snapshot of a [`TableAccessSignal`].
///
/// Returned by [`Database::table_access_signal`] so callers observe a stable,
/// atomically-loaded view without touching the internal atomics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AccessSignalSnapshot {
    /// Analytical scans of the table.
    pub scan_count: u64,
    /// Primary-key / point lookups of the table.
    pub point_lookup_count: u64,
    /// Running sum of projected column counts across recorded scans.
    pub projection_width_sum: u64,
    /// Number of scans that contributed to `projection_width_sum`.
    pub projection_width_n: u64,
    /// Writes routed through the DML invalidation funnel.
    pub write_count: u64,
}

impl AccessSignalSnapshot {
    /// Average projected column width across recorded scans, or `None` when no
    /// scan has recorded a width yet.
    pub fn avg_projection_width(&self) -> Option<f64> {
        if self.projection_width_n == 0 {
            None
        } else {
            Some(self.projection_width_sum as f64 / self.projection_width_n as f64)
        }
    }
}

// ----------------------------------------------------------------------------
// RwLock API shim: `parking_lot::RwLock` returns guards directly while
// `std::sync::RwLock` returns `Result`. Isolate that difference here so the
// recording/accessor logic below stays platform-agnostic.
// ----------------------------------------------------------------------------

type SignalMap = HashMap<String, TableAccessSignal>;

#[cfg(not(target_arch = "wasm32"))]
#[inline]
fn with_read<R>(lock: &RwLock<SignalMap>, f: impl FnOnce(&SignalMap) -> R) -> R {
    f(&lock.read())
}

#[cfg(target_arch = "wasm32")]
#[inline]
fn with_read<R>(lock: &RwLock<SignalMap>, f: impl FnOnce(&SignalMap) -> R) -> R {
    f(&lock.read().expect("access-signal lock poisoned"))
}

#[cfg(not(target_arch = "wasm32"))]
#[inline]
fn with_write<R>(lock: &RwLock<SignalMap>, f: impl FnOnce(&mut SignalMap) -> R) -> R {
    f(&mut lock.write())
}

#[cfg(target_arch = "wasm32")]
#[inline]
fn with_write<R>(lock: &RwLock<SignalMap>, f: impl FnOnce(&mut SignalMap) -> R) -> R {
    f(&mut lock.write().expect("access-signal lock poisoned"))
}

impl Database {
    /// Apply `f` to the signal for `table_name`, creating a zeroed entry on
    /// first touch.
    ///
    /// Fast path: a shared read lock is enough because the counters are atomics
    /// mutated through `&`. Only inserting a brand-new entry needs the write
    /// lock, so steady-state recording never contends on it.
    fn with_access_signal(&self, table_name: &str, f: impl Fn(&TableAccessSignal)) {
        let key = normalize_signal_key(table_name);

        // Fast path: entry already exists — record under a shared read lock.
        let existed = with_read(&self.table_access_signals, |map| match map.get(&key) {
            Some(sig) => {
                f(sig);
                true
            }
            None => false,
        });
        if existed {
            return;
        }

        // Slow path: create the entry under a write lock, then record. Another
        // thread may have inserted it first; `entry(..).or_default()` handles
        // that race without clobbering existing counts.
        with_write(&self.table_access_signals, |map| {
            let sig = map.entry(key).or_default();
            f(sig);
        });
    }

    /// Record one analytical scan of `table_name` with the given projected
    /// column width (the scan's output column count).
    pub fn record_scan(&self, table_name: &str, projection_width: u64) {
        self.with_access_signal(table_name, |sig| {
            sig.scan_count.fetch_add(1, Ordering::Relaxed);
            sig.projection_width_sum.fetch_add(projection_width, Ordering::Relaxed);
            sig.projection_width_n.fetch_add(1, Ordering::Relaxed);
        });
    }

    /// Record one primary-key / point lookup of `table_name`.
    pub(crate) fn record_point_lookup(&self, table_name: &str) {
        self.with_access_signal(table_name, |sig| {
            sig.point_lookup_count.fetch_add(1, Ordering::Relaxed);
        });
    }

    /// Record one write (INSERT/UPDATE/DELETE/TRUNCATE/ALTER) to `table_name`.
    pub(crate) fn record_write(&self, table_name: &str) {
        self.with_access_signal(table_name, |sig| {
            sig.write_count.fetch_add(1, Ordering::Relaxed);
        });
    }

    /// Snapshot the access signal for `table_name`, or `None` if the table has
    /// never been touched. Mirrors `columnar_cache_stats()` in shape.
    pub fn table_access_signal(&self, table_name: &str) -> Option<AccessSignalSnapshot> {
        let key = normalize_signal_key(table_name);
        with_read(&self.table_access_signals, |map| {
            map.get(&key).map(|sig| AccessSignalSnapshot {
                scan_count: sig.scan_count.load(Ordering::Relaxed),
                point_lookup_count: sig.point_lookup_count.load(Ordering::Relaxed),
                projection_width_sum: sig.projection_width_sum.load(Ordering::Relaxed),
                projection_width_n: sig.projection_width_n.load(Ordering::Relaxed),
                write_count: sig.write_count.load(Ordering::Relaxed),
            })
        })
    }

    /// Reset all per-table access signals, clearing every entry.
    pub fn reset_access_signals(&self) {
        with_write(&self.table_access_signals, |map| map.clear());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_count_and_projection_width() {
        let db = Database::new();

        // Record N scans of table "T" with projection widths 2, 4, 6.
        db.record_scan("T", 2);
        db.record_scan("T", 4);
        db.record_scan("T", 6);

        let sig = db.table_access_signal("T").expect("signal should exist after scans");
        assert_eq!(sig.scan_count, 3, "scan_count should equal number of scans");
        assert_eq!(sig.projection_width_n, 3);
        assert_eq!(sig.projection_width_sum, 12);
        assert_eq!(sig.avg_projection_width(), Some(4.0), "avg width = mean of recorded widths");
        assert_eq!(sig.point_lookup_count, 0);
        assert_eq!(sig.write_count, 0);
    }

    #[test]
    fn test_point_lookup_count() {
        let db = Database::new();

        for _ in 0..5 {
            db.record_point_lookup("orders");
        }

        let sig = db.table_access_signal("orders").expect("signal should exist");
        assert_eq!(sig.point_lookup_count, 5, "point_lookup_count should equal number of lookups");
        assert_eq!(sig.scan_count, 0);
    }

    #[test]
    fn test_write_count() {
        let db = Database::new();

        db.record_write("t");
        db.record_write("t");

        let sig = db.table_access_signal("t").expect("signal should exist");
        assert_eq!(sig.write_count, 2);
    }

    #[test]
    fn test_avg_projection_width_none_when_no_scans() {
        let db = Database::new();
        db.record_point_lookup("t");

        let sig = db.table_access_signal("t").expect("signal should exist");
        assert_eq!(sig.avg_projection_width(), None, "no scans yet => no avg width");
    }

    #[test]
    fn test_tables_are_independent() {
        let db = Database::new();

        db.record_scan("a", 3);
        db.record_scan("a", 3);
        db.record_point_lookup("b");

        let a = db.table_access_signal("a").expect("a exists");
        let b = db.table_access_signal("b").expect("b exists");

        assert_eq!(a.scan_count, 2);
        assert_eq!(a.point_lookup_count, 0);
        assert_eq!(b.scan_count, 0);
        assert_eq!(b.point_lookup_count, 1);
    }

    #[test]
    fn test_case_insensitive_key() {
        let db = Database::new();

        // Mixed casing must all land on the same entry.
        db.record_scan("Users", 2);
        db.record_scan("users", 2);
        db.record_point_lookup("USERS");

        let via_lower = db.table_access_signal("users").expect("exists");
        let via_upper = db.table_access_signal("USERS").expect("exists");
        assert_eq!(via_lower, via_upper, "casing should not create distinct entries");
        assert_eq!(via_lower.scan_count, 2);
        assert_eq!(via_lower.point_lookup_count, 1);
    }

    #[test]
    fn test_unknown_table_is_none() {
        let db = Database::new();
        assert_eq!(db.table_access_signal("never_touched"), None);
    }

    #[test]
    fn test_reset_access_signals() {
        let db = Database::new();

        db.record_scan("t", 4);
        db.record_point_lookup("t");
        db.record_write("t");
        assert!(db.table_access_signal("t").is_some());

        db.reset_access_signals();
        assert_eq!(db.table_access_signal("t"), None, "reset clears all entries");
    }

    #[test]
    fn test_clone_resets_signals() {
        let db = Database::new();
        db.record_scan("t", 4);

        let cloned = db.clone();
        assert_eq!(
            cloned.table_access_signal("t"),
            None,
            "cloned database starts with empty access signals (like search_count)"
        );
    }
}
