//! Path-assertion + parity tests for #6199 Phase 3: incremental maintenance of
//! the columnar representation cache across writes.
//!
//! Before Phase 3, every INSERT dropped the whole cached columnar copy
//! (`ColumnarCache::invalidate`), so an interleaved write-plus-scan workload
//! re-converted the entire table from scratch on every scan-after-write — a full
//! O(rows) rebuild per write. Phase 3 instead appends the inserted rows to the
//! resident columnar copy in place, so the table is converted once and then kept
//! in sync incrementally.
//!
//! These end-to-end tests drive the real SQL executor and assert on *structural*
//! `columnar_cache_stats()` counters — never wall-clock timing (the benchmark
//! machine is always loaded):
//!   - `conversions` stays flat across a mix of INSERTs and scans (no rebuild per write), while
//!     `incremental_updates` climbs, and
//!   - result parity: the incrementally-maintained columnar path returns byte-identical rows to the
//!     row path forced by disabling the representation cache (`columnar_cache_budget = 0`).
//!
//! No wall-clock assertions anywhere.

mod common;
use common::{execute_sql, run_select, run_select_values};
use vibesql_storage::{Database, DatabaseConfig};

/// Rows to exceed the `MIN_COLUMNAR_ROWS` (500) floor comfortably.
const N: i64 = 600;
/// Number of interleaved write+scan iterations.
const ITERS: i64 = 50;

/// Create `table(id INTEGER PRIMARY KEY, v INTEGER)` with `N` rows; `v` cycles
/// 0..100 so `WHERE v >= k` is columnar-foldable and selective.
fn setup_big(db: &mut Database, table: &str) {
    execute_sql(db, &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, v INTEGER)"));
    let mut ins = String::new();
    for i in 0..N {
        ins.push_str(&format!("INSERT INTO {table} VALUES ({i}, {});", i % 100));
    }
    execute_sql(db, &ins);
}

/// Run the interleaved write-plus-scan workload against `db`: warm the cache
/// with one analytical scan, then alternate a single-row INSERT with a scan.
/// Scans stay dominant over writes so the table remains analytically hot (the
/// adaptive dispatch keeps taking the columnar path).
fn write_plus_scan_workload(db: &mut Database) {
    const SCAN: &str = "SELECT id FROM t WHERE v >= 50 ORDER BY id";
    let _ = run_select(db, SCAN); // warm → resident (one conversion)
    for k in 0..ITERS {
        let id = N + k;
        execute_sql(db, &format!("INSERT INTO t VALUES ({id}, {})", id % 100));
        let _ = run_select(db, SCAN);
    }
}

/// A SQL write-plus-scan workload must not rebuild the columnar copy on every
/// write: `conversions` stays at 1 (the initial warm conversion) while
/// `incremental_updates` climbs with the inserts.
#[test]
fn sql_write_plus_scan_does_not_rebuild_per_write() {
    let mut db = Database::new();
    setup_big(&mut db, "t");
    db.reset_access_signals(); // ignore bulk-load writes

    write_plus_scan_workload(&mut db);

    let stats = db.columnar_cache_stats();
    assert_eq!(
        stats.conversions, 1,
        "the table must be converted exactly once, then maintained incrementally \
         (got conversions={})",
        stats.conversions
    );
    assert!(
        stats.incremental_updates >= ITERS as u64,
        "each INSERT into the resident table must be maintained incrementally \
         (got incremental_updates={}, expected >= {})",
        stats.incremental_updates,
        ITERS
    );
}

/// Result parity: the incrementally-maintained columnar path returns
/// byte-identical rows to the row path (representation cache disabled with
/// `budget = 0`) after the same interleaved write-plus-scan workload.
#[test]
fn sql_incremental_columnar_result_parity_with_row_path() {
    const FINAL: &str = "SELECT id, v FROM t ORDER BY id";

    // Columnar path: default budget, adaptive dispatch + incremental upkeep.
    let mut columnar_db = Database::new();
    setup_big(&mut columnar_db, "t");
    columnar_db.reset_access_signals();
    write_plus_scan_workload(&mut columnar_db);
    let columnar_rows = run_select_values(&columnar_db, FINAL);

    // Sanity: the columnar path was actually exercised (and not rebuilt per write).
    assert_eq!(
        columnar_db.columnar_cache_stats().conversions,
        1,
        "parity setup sanity: columnar path taken and maintained incrementally"
    );

    // Row path: disable the representation cache entirely.
    let mut cfg = DatabaseConfig::server_default();
    cfg.columnar_cache_budget = 0;
    let mut row_db = Database::with_config(cfg);
    setup_big(&mut row_db, "t");
    row_db.reset_access_signals();
    write_plus_scan_workload(&mut row_db);
    let row_rows = run_select_values(&row_db, FINAL);

    assert_eq!(
        columnar_rows, row_rows,
        "incremental columnar maintenance must return byte-identical rows to the row path"
    );
    assert_eq!(
        row_db.columnar_cache_stats().conversions,
        0,
        "budget=0 must fully disable the representation cache (no conversions)"
    );
    // Every inserted row is present in both paths.
    assert_eq!(columnar_rows.len(), (N + ITERS) as usize, "all rows visible after the workload");
}
