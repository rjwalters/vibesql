//! Path-assertion + parity tests for #6199 Phase 2: adaptive columnar dispatch.
//!
//! The former dispatch gate for the row-table representation cache was a bare
//! `all_rows.len() >= SIMD_COLUMNAR_THRESHOLD` row count. Phase 2 replaces it
//! with `Database::should_use_columnar`, a purely *structural* decision driven
//! by the per-table access signal (scan / point-lookup / write mix) — never by
//! wall-clock timing (the benchmark machine is always loaded).
//!
//! These end-to-end tests drive the real executor scan path and assert on
//! `columnar_cache_stats().conversions` (a structural stat, not a timer):
//!   - a hot, scan-dominated large table takes the columnar path and is converted + cached,
//!   - a large table whose access is dominated by point lookups is NOT converted (it stays on the
//!     row path),
//!   - result parity: the adaptive columnar path returns byte-identical rows to the row path forced
//!     by disabling the representation cache (`columnar_cache_budget = 0`, the documented
//!     `VIBESQL_DISABLE_COLUMNAR=1` parity lever for the representation cache).
//!
//! No wall-clock assertions anywhere.

mod common;
use common::{execute_sql, run_select};
use vibesql_storage::{Database, DatabaseConfig};
use vibesql_types::SqlValue;

/// Rows to exceed the `MIN_COLUMNAR_ROWS` (500) floor comfortably.
const N: i64 = 600;

/// Create `table(id INTEGER PRIMARY KEY, v INTEGER)` with `N` rows; `v` cycles
/// 0..100 so a `WHERE v >= k` predicate is columnar-foldable and selective.
fn setup_big(db: &mut Database, table: &str) {
    execute_sql(db, &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, v INTEGER)"));
    let mut ins = String::new();
    for i in 0..N {
        ins.push_str(&format!("INSERT INTO {table} VALUES ({i}, {});", i % 100));
    }
    execute_sql(db, &ins);
}

#[test]
fn hot_analytical_table_takes_columnar_path() {
    let mut db = Database::new();
    setup_big(&mut db, "hot");
    // Ignore the writes from bulk-loading; measure only the analytical scan.
    db.reset_access_signals();

    let before = db.columnar_cache_stats().conversions;
    let rows = run_select(&db, "SELECT id FROM hot WHERE v >= 0 ORDER BY id");
    assert_eq!(rows.len(), N as usize, "all rows satisfy v >= 0");
    let after = db.columnar_cache_stats().conversions;

    assert!(
        after > before,
        "a hot, scan-first large table must take the columnar path (convert + cache); \
         conversions before={before} after={after}"
    );
}

#[test]
fn point_lookup_dominated_table_is_never_converted() {
    let mut db = Database::new();
    setup_big(&mut db, "cold");
    db.reset_access_signals();

    // Overwhelm the access signal with point lookups so the table is judged
    // non-analytical: scans * SCAN_DOMINANCE_FACTOR will stay far below the
    // point-lookup count even after the scan below records one scan.
    for k in 0..1000 {
        db.get_row_by_pk("cold", &SqlValue::Integer(k % N)).unwrap();
    }

    let before = db.columnar_cache_stats().conversions;
    let rows = run_select(&db, "SELECT id FROM cold WHERE v >= 0 ORDER BY id");
    assert_eq!(rows.len(), N as usize, "row path returns the same rows");
    let after = db.columnar_cache_stats().conversions;

    assert_eq!(
        after, before,
        "a point-lookup-dominated table must NOT be converted/cached (stays on the row path); \
         conversions before={before} after={after}"
    );
}

#[test]
fn adaptive_columnar_result_parity_with_row_path() {
    const QUERY: &str = "SELECT id, v FROM t WHERE v >= 37 ORDER BY id";

    // Columnar path: fresh scan on a large table takes the adaptive columnar
    // dispatch (default budget).
    let mut columnar_db = Database::new();
    setup_big(&mut columnar_db, "t");
    columnar_db.reset_access_signals();
    let conv_before = columnar_db.columnar_cache_stats().conversions;
    let columnar_rows = run_select(&columnar_db, QUERY);
    assert!(
        columnar_db.columnar_cache_stats().conversions > conv_before,
        "parity setup sanity: the columnar path must actually be taken"
    );

    // Row path: disable the representation cache (budget 0). `get_columnar`
    // returns None, so the cached-columnar filter fails and the scan falls
    // through to row-by-row filtering — the documented parity lever.
    let mut cfg = DatabaseConfig::server_default();
    cfg.columnar_cache_budget = 0;
    let mut row_db = Database::with_config(cfg);
    setup_big(&mut row_db, "t");
    let row_rows = run_select(&row_db, QUERY);

    assert_eq!(
        columnar_rows, row_rows,
        "adaptive columnar dispatch must return byte-identical rows to the row path"
    );
    // Guard: the row-path database must never have converted anything.
    assert_eq!(
        row_db.columnar_cache_stats().conversions,
        0,
        "budget=0 must fully disable the representation cache (no conversions)"
    );
}
