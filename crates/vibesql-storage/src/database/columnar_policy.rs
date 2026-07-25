// ============================================================================
// Adaptive Columnar Policy (Phase 2 of #6199)
// ============================================================================
//
// This module is the *consumer* of the Phase 1 per-table access-pattern signal
// (`access_signal.rs`). It converts structural observations — row count plus the
// scan / point-lookup / write mix — into two decisions:
//
//   1. **Dispatch** (`should_use_columnar`): should a row-oriented table use its
//      columnar representation for an analytical scan, or stay on the row path?
//      This replaces the old hardcoded `SIMD_COLUMNAR_THRESHOLD = 500`
//      row-count-only gate in the executor.
//
//   2. **Hotness** (`columnar_hotness`): a single scalar ordering cached tables
//      by how analytically-hot they are, used by `ColumnarCache` to evict the
//      *coldest* resident table under memory pressure instead of the merely
//      least-recently-used one. Hot analytical tables therefore stay resident
//      while colder tables are reclaimed first.
//
// ## Hard constraint: structural signals only, never wall-clock
//
// Per #6199, the benchmark machine is always loaded so measured latency is
// untrustworthy. Every decision here is derived from *counts* (rows, scans,
// point lookups, writes) — a heuristic cost model, not a measured one. This
// keeps the policy deterministic and path-assertion-testable.

use super::access_signal::AccessSignalSnapshot;

/// Minimum row count below which columnar conversion is never worth it,
/// regardless of how analytically the table is accessed. Below this the
/// row-by-row path wins because the row→columnar conversion overhead dominates
/// the SIMD scan savings. This preserves the semantics of the executor's
/// former `SIMD_COLUMNAR_THRESHOLD = 500` constant for the row-oriented path.
pub const MIN_COLUMNAR_ROWS: usize = 500;

/// How many competing structural accesses (point lookups + writes) a single
/// analytical scan "earns" before the table is judged non-analytical and kept
/// on the row path. With a factor of 4 a table stays columnar-eligible as long
/// as scans are at least ~20% of its competing point-lookup/write traffic.
///
/// This is deliberately lenient: a freshly-scanned large table (scans ≥ 1, no
/// competing traffic yet) is columnar-eligible, matching the pre-Phase-2
/// behavior where any large table took the columnar path on first scan. Only
/// tables whose access is *dominated* by point lookups or writes are demoted.
pub const SCAN_DOMINANCE_FACTOR: u64 = 4;

/// Structural decision: should a row-oriented table of `row_count` rows use its
/// columnar representation for an analytical scan?
///
/// Driven purely by structural signals (never wall-clock):
///
/// - Tables below [`MIN_COLUMNAR_ROWS`] always take the row path.
/// - A large table with no recorded access history yet defaults to columnar
///   (legacy behavior: a large table converted on its first analytical scan).
/// - A large table with history is columnar-eligible unless its access is
///   *dominated* by point lookups and/or writes — i.e. unless
///   `scan_count * SCAN_DOMINANCE_FACTOR < point_lookups + writes`. Point-lookup
///   -dominated and write-thrashed tables therefore stay on the row path and are
///   never converted/cached.
///
/// Native `STORAGE COLUMNAR` tables are authoritative and are handled by their
/// own always-resident path; this function is only consulted for the transparent
/// row-table representation cache.
pub fn should_use_columnar(row_count: usize, signal: Option<AccessSignalSnapshot>) -> bool {
    if row_count < MIN_COLUMNAR_ROWS {
        return false;
    }
    match signal {
        // Large table, no access history yet → default to columnar.
        None => true,
        Some(sig) => {
            let scans = sig.scan_count;
            let competing = sig.point_lookup_count.saturating_add(sig.write_count);
            // Analytical/hot iff scans account for a meaningful share of the
            // structural accesses. Otherwise (point-lookup-dominated or
            // write-thrashed) keep the row path.
            scans.saturating_mul(SCAN_DOMINANCE_FACTOR) >= competing
        }
    }
}

/// Structural "hotness" of a cached table: higher means more analytically hot
/// and therefore more worth keeping resident under memory pressure.
///
/// Defined as the net analytical pressure — scans discounted by competing
/// point-lookup and write traffic — saturating at 0. A table scanned many times
/// with little competing traffic is hot; a table with lots of competing traffic
/// (even if occasionally scanned) is cold and is reclaimed first. This is the
/// ordering key for [`crate::columnar_cache::ColumnarCache`]'s hotness-aware
/// eviction; it never uses wall-clock time.
pub fn columnar_hotness(signal: Option<AccessSignalSnapshot>) -> u64 {
    match signal {
        None => 0,
        Some(sig) => {
            let competing = sig.point_lookup_count.saturating_add(sig.write_count);
            sig.scan_count.saturating_sub(competing)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signal(scans: u64, lookups: u64, writes: u64) -> AccessSignalSnapshot {
        AccessSignalSnapshot { scan_count: scans, point_lookup_count: lookups, write_count: writes }
    }

    #[test]
    fn small_table_never_columnar() {
        // Below the floor, no access pattern makes columnar worthwhile.
        assert!(!should_use_columnar(0, None));
        assert!(!should_use_columnar(MIN_COLUMNAR_ROWS - 1, None));
        assert!(!should_use_columnar(MIN_COLUMNAR_ROWS - 1, Some(signal(1_000_000, 0, 0))));
    }

    #[test]
    fn large_table_no_history_defaults_to_columnar() {
        assert!(should_use_columnar(MIN_COLUMNAR_ROWS, None));
        assert!(should_use_columnar(10_000, None));
    }

    #[test]
    fn freshly_scanned_large_table_is_columnar() {
        // scans=1, no competing traffic: the first analytical scan converts.
        assert!(should_use_columnar(10_000, Some(signal(1, 0, 0))));
    }

    #[test]
    fn hot_analytical_table_is_columnar() {
        // Many scans, little competing traffic.
        assert!(should_use_columnar(10_000, Some(signal(1000, 5, 2))));
    }

    #[test]
    fn point_lookup_dominated_table_stays_row_path() {
        // Occasional scan, overwhelmingly point lookups → not columnar.
        assert!(!should_use_columnar(10_000, Some(signal(1, 1000, 0))));
    }

    #[test]
    fn write_thrashed_table_stays_row_path() {
        // Occasional scan, overwhelmingly writes → not columnar.
        assert!(!should_use_columnar(10_000, Some(signal(2, 0, 1000))));
    }

    #[test]
    fn boundary_of_dominance_factor() {
        // scans * 4 == competing → still eligible (>=).
        assert!(should_use_columnar(10_000, Some(signal(10, 40, 0))));
        // one more competing access tips it over into row-path.
        assert!(!should_use_columnar(10_000, Some(signal(10, 41, 0))));
    }

    #[test]
    fn hotness_orders_hot_above_cold() {
        let hot = columnar_hotness(Some(signal(1000, 0, 0)));
        let warm = columnar_hotness(Some(signal(100, 0, 0)));
        let cold = columnar_hotness(Some(signal(5, 0, 0)));
        assert!(hot > warm, "more scans => hotter ({hot} vs {warm})");
        assert!(warm > cold, "more scans => hotter ({warm} vs {cold})");
    }

    #[test]
    fn hotness_discounts_competing_traffic() {
        // Same scan count, but the one with competing traffic is colder.
        let clean = columnar_hotness(Some(signal(100, 0, 0)));
        let contended = columnar_hotness(Some(signal(100, 60, 30)));
        assert!(clean > contended, "competing traffic lowers hotness");
        assert_eq!(clean, 100);
        assert_eq!(contended, 10); // 100 - (60 + 30)
    }

    #[test]
    fn hotness_saturates_at_zero() {
        // Competing traffic dwarfs scans → floor at 0 (never underflows).
        assert_eq!(columnar_hotness(Some(signal(1, 100, 100))), 0);
        assert_eq!(columnar_hotness(None), 0);
    }
}
