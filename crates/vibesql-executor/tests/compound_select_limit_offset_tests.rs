//! Regression tests for issue #5847:
//! "top-level compound SELECT ignores OFFSET — UNION ALL ... LIMIT 1 OFFSET 1
//! returns 0 rows".
//!
//! Root cause: LIMIT/OFFSET was applied TWICE for compound (set-operation)
//! SELECTs whose left branch has no FROM clause (or hits the COUNT(*) fast
//! path). `execute_select_without_from`
//! (`select/executor/nonagg/without_from.rs`) and the COUNT(*) fast path in
//! `select/executor/aggregation/mod.rs` applied LIMIT/OFFSET to the LEFT-branch
//! result before the set operation ran, and then the set-operation handler in
//! `execute_with_ctes` applied it AGAIN to the combined result. For
//! `SELECT 1 UNION ALL SELECT 2 LIMIT 1 OFFSET 1` the first application turned
//! `[1]` into `[]` (offset 1 >= len 1), so the combined result was `[2]`, and
//! the second application turned `[2]` into `[]` — 0 rows instead of `2`.
//!
//! The fix mirrors the guard already present in `apply_eager_projection`
//! (`nonagg/materialized.rs`): skip LIMIT/OFFSET on the left branch when
//! `stmt.set_operation.is_some()`, deferring it to the set-operation handler.
//!
//! Every expected value below was verified against `sqlite3`.

use vibesql_executor::{CreateTableExecutor, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute one or more non-SELECT SQL statements separated by ';'.
fn execute_sql(db: &mut Database, sql: &str) {
    for sql_stmt in sql.split(';') {
        let trimmed = sql_stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(trimmed).expect("Failed to parse SQL");
        match stmt {
            vibesql_ast::Statement::CreateTable(s) => {
                CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
            }
            vibesql_ast::Statement::Insert(s) => {
                InsertExecutor::execute(db, &s).expect("INSERT failed");
            }
            other => panic!("Unsupported statement type: {:?}", other),
        }
    }
}

/// Execute a SELECT and return the first column of each row, preserving order.
fn select_first_col(db: &Database, sql: &str) -> Vec<SqlValue> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => panic!("Expected SELECT statement, got {:?}", other),
    };
    let executor = vibesql_executor::SelectExecutor::new(db);
    executor
        .execute(&select_stmt)
        .expect("SELECT execution failed")
        .iter()
        .map(|row| row.values[0].clone())
        .collect()
}

fn i(n: i64) -> SqlValue {
    SqlValue::Integer(n)
}

// ---------------------------------------------------------------------------
// FROM-less compound left branch (the reported reproducer + variations)
// ---------------------------------------------------------------------------

/// The exact reproducer from the issue. Before the fix this returned 0 rows.
#[test]
fn test_union_all_limit1_offset1_reproducer() {
    let db = Database::new();
    let rows = select_first_col(&db, "SELECT 1 UNION ALL SELECT 2 LIMIT 1 OFFSET 1");
    assert_eq!(rows, vec![i(2)], "UNION ALL LIMIT 1 OFFSET 1 must return the second row (2)");
}

#[test]
fn test_union_all_limit1_offset0() {
    let db = Database::new();
    let rows = select_first_col(&db, "SELECT 1 UNION ALL SELECT 2 LIMIT 1 OFFSET 0");
    assert_eq!(rows, vec![i(1)]);
}

#[test]
fn test_union_all_limit2_offset0() {
    let db = Database::new();
    let rows = select_first_col(&db, "SELECT 1 UNION ALL SELECT 2 LIMIT 2 OFFSET 0");
    assert_eq!(rows, vec![i(1), i(2)]);
}

#[test]
fn test_union_all_limit0_returns_empty() {
    let db = Database::new();
    let rows = select_first_col(&db, "SELECT 1 UNION ALL SELECT 2 LIMIT 0 OFFSET 0");
    assert_eq!(rows, Vec::<SqlValue>::new());
}

/// LIMIT -1 means "unlimited" in SQLite; OFFSET still applies.
#[test]
fn test_union_all_limit_negative_one_offset1() {
    let db = Database::new();
    let rows = select_first_col(&db, "SELECT 1 UNION ALL SELECT 2 LIMIT -1 OFFSET 1");
    assert_eq!(rows, vec![i(2)]);
}

/// OFFSET beyond the combined result count yields no rows.
#[test]
fn test_union_all_offset_beyond_count() {
    let db = Database::new();
    let rows = select_first_col(&db, "SELECT 1 UNION ALL SELECT 2 LIMIT 5 OFFSET 5");
    assert_eq!(rows, Vec::<SqlValue>::new());
}

/// Three-way UNION ALL: LIMIT/OFFSET applies to the fully combined result.
#[test]
fn test_triple_union_all_limit_offset() {
    let db = Database::new();
    let rows =
        select_first_col(&db, "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 LIMIT 2 OFFSET 1");
    assert_eq!(rows, vec![i(2), i(3)]);
}

#[test]
fn test_union_distinct_limit1_offset1() {
    let db = Database::new();
    let rows = select_first_col(&db, "SELECT 1 UNION SELECT 2 LIMIT 1 OFFSET 1");
    assert_eq!(rows, vec![i(2)]);
}

#[test]
fn test_intersect_limit_offset() {
    let db = Database::new();
    // {1} INTERSECT {1} = {1}; OFFSET 1 skips past it → empty (matches sqlite3).
    let rows = select_first_col(&db, "SELECT 1 INTERSECT SELECT 1 LIMIT 1 OFFSET 1");
    assert_eq!(rows, Vec::<SqlValue>::new());
}

#[test]
fn test_except_limit_offset0() {
    let db = Database::new();
    let rows = select_first_col(&db, "SELECT 1 EXCEPT SELECT 2 LIMIT 1 OFFSET 0");
    assert_eq!(rows, vec![i(1)]);
}

// ---------------------------------------------------------------------------
// FROM-based compound branches
// ---------------------------------------------------------------------------

fn setup_t() -> Database {
    let mut db = Database::new();
    execute_sql(&mut db, "CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (10),(20),(30)");
    db
}

#[test]
fn test_from_left_branch_union_all_limit_offset() {
    let db = setup_t();
    // {10,20,30} UNION ALL {99} = {10,20,30,99}; LIMIT 2 OFFSET 1 → {20,30}.
    let rows = select_first_col(&db, "SELECT x FROM t UNION ALL SELECT 99 LIMIT 2 OFFSET 1");
    assert_eq!(rows, vec![i(20), i(30)]);
}

#[test]
fn test_from_left_branch_union_all_offset_into_right() {
    let db = setup_t();
    let rows = select_first_col(&db, "SELECT x FROM t UNION ALL SELECT 99 LIMIT 2 OFFSET 3");
    assert_eq!(rows, vec![i(99)]);
}

#[test]
fn test_from_left_branch_except_order_limit_offset() {
    let db = setup_t();
    // {10,20,30} EXCEPT {20} = {10,30}; ORDER BY x LIMIT 1 OFFSET 1 → {30}.
    let rows =
        select_first_col(&db, "SELECT x FROM t EXCEPT SELECT 20 ORDER BY x LIMIT 1 OFFSET 1");
    assert_eq!(rows, vec![i(30)]);
}

// ---------------------------------------------------------------------------
// COUNT(*) aggregation fast-path left branch
// ---------------------------------------------------------------------------

/// Covers the COUNT(*) fast-path guard in aggregation/mod.rs. Before the fix,
/// LIMIT/OFFSET was applied to the single COUNT(*) left-branch row, dropping it,
/// and then again to the combined result.
#[test]
fn test_count_star_fast_path_union_all_limit_offset() {
    let db = setup_t();
    // {3} UNION ALL {1} = {3,1}; LIMIT 1 OFFSET 1 → {1}.
    let rows = select_first_col(&db, "SELECT COUNT(*) FROM t UNION ALL SELECT 1 LIMIT 1 OFFSET 1");
    assert_eq!(rows, vec![i(1)]);
}

#[test]
fn test_count_star_fast_path_union_all_offset0() {
    let db = setup_t();
    let rows = select_first_col(&db, "SELECT COUNT(*) FROM t UNION ALL SELECT 1 LIMIT 1 OFFSET 0");
    assert_eq!(rows, vec![i(3)]);
}

// ---------------------------------------------------------------------------
// Non-compound guard: single SELECT must STILL apply LIMIT/OFFSET
// (ensures the guard did not over-reach and break the common case)
// ---------------------------------------------------------------------------

#[test]
fn test_single_select_without_from_still_applies_limit_offset() {
    let db = Database::new();
    // Not a compound: LIMIT 0 must still truncate to empty.
    let rows = select_first_col(&db, "SELECT 1 LIMIT 0");
    assert_eq!(rows, Vec::<SqlValue>::new());
}

#[test]
fn test_single_count_star_still_applies_limit_offset() {
    let db = setup_t();
    // Not a compound: COUNT(*) LIMIT 1 OFFSET 1 skips the single row → empty.
    let rows = select_first_col(&db, "SELECT COUNT(*) FROM t LIMIT 1 OFFSET 1");
    assert_eq!(rows, Vec::<SqlValue>::new());
}

#[test]
fn test_single_from_select_still_applies_limit_offset() {
    let db = setup_t();
    let rows = select_first_col(&db, "SELECT x FROM t LIMIT 1 OFFSET 1");
    assert_eq!(rows, vec![i(20)]);
}
