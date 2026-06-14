//! Regression tests for issue #5524.
//!
//! An in-memory `ORDER BY <pk>` index-ordered scan returned the wrong rows
//! after a DML statement compacted (or tombstoned) rows mid-statement, because
//! the user-defined B-tree index's lazy `pending_deletions` adjustment got out
//! of sync with the table's physical row positions. The unordered scan and
//! `count(*)` returned correct results, but the index-ordered `ORDER BY` scan
//! consulted stale physical positions and produced empty / wrong output.
//!
//! These tests drive pure in-memory `Database` usage (no persistence reload,
//! which would otherwise mask the bug by rebuilding indices) and assert that
//! the index-ordered scan matches the unordered scan exactly.

use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::select::SelectExecutor;
use crate::{
    CreateTableExecutor, DeleteExecutor, InsertExecutor, UpdateExecutor,
};

fn create_test_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);
    db
}

/// Execute a non-SELECT statement.
fn exec(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}"));
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).unwrap();
        }
        vibesql_ast::Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).unwrap();
        }
        vibesql_ast::Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).unwrap();
        }
        vibesql_ast::Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).unwrap();
        }
        other => panic!("unexpected statement type for exec(): {other:?}"),
    }
}

/// Execute a SELECT and return the rows as Vec<Vec<SqlValue>>.
fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}"));
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("expected SELECT statement: {sql:?}");
    };
    let executor = SelectExecutor::new(db);
    executor
        .execute(&select_stmt)
        .unwrap_or_else(|e| panic!("execute {sql:?}: {e:?}"))
        .into_iter()
        .map(|r| r.values.to_vec())
        .collect()
}

/// Same query but sorted in Rust so it can be compared against the
/// index-ordered scan regardless of natural scan order.
fn query_sorted(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let mut rows = query(db, sql);
    rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    rows
}

/// The exact repro from issue #5524: an `UPDATE OR REPLACE` that deletes a
/// conflicting row mid-statement, after a prior tombstone exists, leaving one
/// live row. The index-ordered `ORDER BY a` scan must return that one row.
#[test]
fn test_order_by_pk_after_mid_statement_compaction_5524() {
    let mut db = create_test_db();

    exec(&mut db, "CREATE TABLE t1(a INT PRIMARY KEY, b) WITHOUT ROWID");
    exec(&mut db, "INSERT INTO t1 VALUES (1, 'one'), (2, 'two'), (3, 'three')");
    // Tombstone at idx0.
    exec(&mut db, "DELETE FROM t1 WHERE a = 1");
    exec(&mut db, "INSERT OR REPLACE INTO t1 VALUES (2, 'three')");
    // Deletes the conflicting (3,three); should compact/tombstone mid-statement.
    exec(&mut db, "UPDATE OR REPLACE t1 SET a = 3 WHERE a = 2");

    let unordered = query_sorted(&db, "SELECT a, b FROM t1");
    let count: i64 = match query(&db, "SELECT count(*) FROM t1")[0][0] {
        SqlValue::Integer(n) => n,
        ref v => panic!("count not integer: {v:?}"),
    };
    let ordered_asc = query(&db, "SELECT a, b FROM t1 ORDER BY a");
    let ordered_desc = query(&db, "SELECT a, b FROM t1 ORDER BY a DESC");

    assert_eq!(
        count as usize,
        unordered.len(),
        "count(*) must match unordered row count"
    );

    // The core assertion: the index-ordered scan must return the same live
    // rows as the unordered scan.
    assert_eq!(
        ordered_asc, unordered,
        "ORDER BY a (ASC) must return the same live rows as the unordered scan"
    );

    let mut desc_resorted = ordered_desc.clone();
    desc_resorted.sort_by(|x, y| format!("{x:?}").cmp(&format!("{y:?}")));
    assert_eq!(
        desc_resorted, unordered,
        "ORDER BY a DESC must return the same live rows as the unordered scan"
    );

    // Matches sqlite3 3.51.0: final live state is exactly (3, 'three').
    assert_eq!(
        ordered_asc,
        vec![vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("three"))]],
        "final state should be the single row (3, 'three')"
    );
}

/// Plain DELETE that crosses the compaction threshold mid-statement, leaving a
/// gap. Index-ordered scan must still match the unordered scan.
#[test]
fn test_order_by_pk_after_delete_compaction_with_gaps_5524() {
    let mut db = create_test_db();

    exec(&mut db, "CREATE TABLE t2(a INT PRIMARY KEY, b)");
    exec(
        &mut db,
        "INSERT INTO t2 VALUES (10,'a'),(20,'b'),(30,'c'),(40,'d'),(50,'e')",
    );
    // Delete a majority to force compaction (> 50% deleted).
    exec(&mut db, "DELETE FROM t2 WHERE a IN (10, 30, 50)");

    let unordered = query_sorted(&db, "SELECT a, b FROM t2");
    let ordered = query(&db, "SELECT a, b FROM t2 ORDER BY a");

    assert_eq!(ordered, unordered, "ORDER BY a must match unordered scan after compaction");
    assert_eq!(
        ordered,
        vec![
            vec![SqlValue::Integer(20), SqlValue::Varchar(arcstr::ArcStr::from("b"))],
            vec![SqlValue::Integer(40), SqlValue::Varchar(arcstr::ArcStr::from("d"))],
        ]
    );
}

/// Multiple indexes: a secondary index ORDER BY must also remain correct after
/// a compacting delete.
#[test]
fn test_order_by_secondary_index_after_compaction_5524() {
    let mut db = create_test_db();

    exec(&mut db, "CREATE TABLE t3(a INT PRIMARY KEY, b INT, c)");
    db.create_index(
        "idx_t3_b".to_string(),
        "t3".to_string(),
        false,
        vec![vibesql_ast::IndexColumn::Column {
            column_name: "b".to_string(),
            prefix_length: None,
            direction: vibesql_ast::OrderDirection::Asc,
        }],
    )
    .unwrap();

    exec(
        &mut db,
        "INSERT INTO t3 VALUES (1,50,'x'),(2,40,'y'),(3,30,'z'),(4,20,'w'),(5,10,'v')",
    );
    // Delete majority -> compaction, then both PK and secondary index must be
    // rebuilt to the post-compaction positions.
    exec(&mut db, "DELETE FROM t3 WHERE a IN (1, 2, 3)");

    let unordered = query_sorted(&db, "SELECT a, b FROM t3");

    let by_pk = query(&db, "SELECT a, b FROM t3 ORDER BY a");
    assert_eq!(by_pk, unordered, "ORDER BY pk must match unordered scan");

    let by_b = query(&db, "SELECT a, b FROM t3 ORDER BY b");
    assert_eq!(
        by_b,
        vec![
            vec![SqlValue::Integer(5), SqlValue::Integer(10)],
            vec![SqlValue::Integer(4), SqlValue::Integer(20)],
        ],
        "ORDER BY secondary index column must match post-compaction order"
    );
}

/// Tombstone-only path (no compaction): deleting a minority via the in-place
/// mark path still keeps the index-ordered scan correct.
#[test]
fn test_order_by_pk_after_partial_delete_no_compaction_5524() {
    let mut db = create_test_db();

    exec(&mut db, "CREATE TABLE t4(a INT PRIMARY KEY, b)");
    exec(
        &mut db,
        "INSERT INTO t4 VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')",
    );
    // Delete a single row (minority -> no compaction, just a tombstone).
    exec(&mut db, "DELETE FROM t4 WHERE a = 3");

    let unordered = query_sorted(&db, "SELECT a, b FROM t4");
    let ordered = query(&db, "SELECT a, b FROM t4 ORDER BY a");
    assert_eq!(ordered, unordered, "ORDER BY a must match unordered scan with a tombstone");
    assert_eq!(ordered.len(), 4);
}
