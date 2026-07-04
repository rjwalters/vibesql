//! Regression tests for issue #5836: the `ON CONFLICT ... DO UPDATE` arm
//! must enforce the same constraints as a normal UPDATE (upsert4.test,
//! 1.x.5-6).
//!
//! Before the fix, `INSERT ... ON CONFLICT(a) DO UPDATE SET c='one'` wrote
//! duplicate values into a UNIQUE column instead of failing. SQLite aborts
//! the statement and leaves the table untouched.
//!
//! Expected error wording verified against sqlite3:
//! - `UNIQUE constraint failed: t1.c`             (UNIQUE column)
//! - `UNIQUE constraint failed: t1.a`             (PRIMARY KEY)
//! - `UNIQUE constraint failed: t5.x, t5.y`       (composite unique index)
//! - `UNIQUE constraint failed: index 't6e'`      (expression index)
//! - `NOT NULL constraint failed: t4.b`           (NOT NULL)
//! - `CHECK constraint failed: <name-or-expr>`    (CHECK)

use vibesql_executor::{CreateIndexExecutor, CreateTableExecutor, ExecutorError, InsertExecutor};
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Parse and execute a CREATE TABLE / CREATE INDEX / INSERT statement.
fn run_stmt(db: &mut Database, sql: &str) -> Result<usize, ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            CreateTableExecutor::execute(&create_table, db).map(|_| 0)
        }
        vibesql_ast::Statement::CreateIndex(create_index) => {
            CreateIndexExecutor::execute(&create_index, db).map(|_| 0)
        }
        vibesql_ast::Statement::Insert(insert) => InsertExecutor::execute(db, &insert),
        other => panic!("Unsupported statement in test: {:?}", other),
    }
}

fn setup(db: &mut Database, stmts: &[&str]) {
    for sql in stmts {
        run_stmt(db, sql).unwrap_or_else(|e| panic!("setup failed for {:?}: {}", sql, e));
    }
}

/// Collect a column's values across all live rows (storage order).
fn column_values(db: &Database, table: &str, col_idx: usize) -> Vec<SqlValue> {
    db.get_table(table).unwrap().scan_live().map(|(_, row)| row.values[col_idx].clone()).collect()
}

/// upsert4 1.x.5: DO UPDATE that duplicates a UNIQUE column value must fail
/// with SQLite's exact message, and 1.x.6: the table must be unchanged.
#[test]
fn do_update_violating_unique_column_fails() {
    let mut db = Database::new();
    setup(
        &mut db,
        &[
            "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, c TEXT UNIQUE)",
            "INSERT INTO t1 VALUES(1, NULL, 'one')",
            "INSERT INTO t1 VALUES(2, NULL, 'two')",
        ],
    );

    let err = run_stmt(
        &mut db,
        "INSERT INTO t1 VALUES(2, NULL, 'zero') ON CONFLICT (a) DO UPDATE SET c = 'one'",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t1.c"),
        "expected 'UNIQUE constraint failed: t1.c', got: {}",
        err
    );

    // Statement aborted: row 2 keeps c='two' (upsert4 1.x.6).
    assert_eq!(
        column_values(&db, "t1", 2),
        vec![SqlValue::Varchar("one".into()), SqlValue::Varchar("two".into())]
    );
}

/// DO UPDATE that duplicates the PRIMARY KEY must fail (SQLite reports PK
/// violations as UNIQUE constraint failures).
#[test]
fn do_update_violating_primary_key_fails() {
    let mut db = Database::new();
    setup(
        &mut db,
        &[
            "CREATE TABLE t2(a INT PRIMARY KEY, b INTEGER, c TEXT UNIQUE)",
            "INSERT INTO t2 VALUES(1, NULL, 'one')",
            "INSERT INTO t2 VALUES(2, NULL, 'two')",
        ],
    );

    let err = run_stmt(
        &mut db,
        "INSERT INTO t2 VALUES(9, NULL, 'two') ON CONFLICT (c) DO UPDATE SET a = 1",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t2.a"),
        "expected 'UNIQUE constraint failed: t2.a', got: {}",
        err
    );

    // Statement aborted: primary keys unchanged.
    assert_eq!(column_values(&db, "t2", 0), vec![SqlValue::Integer(1), SqlValue::Integer(2)]);
}

/// DO UPDATE that nulls a NOT NULL column must fail.
#[test]
fn do_update_violating_not_null_fails() {
    let mut db = Database::new();
    setup(
        &mut db,
        &[
            "CREATE TABLE t4(a INTEGER PRIMARY KEY, b INTEGER NOT NULL)",
            "INSERT INTO t4 VALUES(1, 5)",
        ],
    );

    let err =
        run_stmt(&mut db, "INSERT INTO t4 VALUES(1, 7) ON CONFLICT (a) DO UPDATE SET b = NULL")
            .unwrap_err();
    assert!(
        err.to_string().contains("NOT NULL constraint failed: t4.b"),
        "expected 'NOT NULL constraint failed: t4.b', got: {}",
        err
    );

    assert_eq!(column_values(&db, "t4", 1), vec![SqlValue::Integer(5)]);
}

/// DO UPDATE that duplicates a composite CREATE UNIQUE INDEX key must fail,
/// reporting the index's columns.
#[test]
fn do_update_violating_unique_index_fails() {
    let mut db = Database::new();
    setup(
        &mut db,
        &[
            "CREATE TABLE t5(a INTEGER PRIMARY KEY, x TEXT, y TEXT)",
            "CREATE UNIQUE INDEX t5xy ON t5(x, y)",
            "INSERT INTO t5 VALUES(1, 'p', 'q')",
            "INSERT INTO t5 VALUES(2, 'r', 's')",
        ],
    );

    let err = run_stmt(
        &mut db,
        "INSERT INTO t5 VALUES(2, 'z', 'z') ON CONFLICT (a) DO UPDATE SET x = 'p', y = 'q'",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("UNIQUE constraint failed: t5.x, t5.y"),
        "expected 'UNIQUE constraint failed: t5.x, t5.y', got: {}",
        err
    );

    assert_eq!(
        column_values(&db, "t5", 1),
        vec![SqlValue::Varchar("p".into()), SqlValue::Varchar("r".into())]
    );
}

/// A DO UPDATE that leaves the unique key unchanged (or writes the
/// conflicting row's own values back) must NOT self-conflict.
#[test]
fn do_update_keeping_own_key_succeeds() {
    let mut db = Database::new();
    setup(
        &mut db,
        &[
            "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, c TEXT UNIQUE)",
            "INSERT INTO t1 VALUES(1, NULL, 'one')",
            "INSERT INTO t1 VALUES(2, NULL, 'two')",
        ],
    );

    // Rewrites c to its current value: no self-conflict (upsert4 1.x.3-4
    // shape: only b changes; here c is explicitly set to itself too).
    let n = run_stmt(
        &mut db,
        "INSERT INTO t1 VALUES(2, NULL, 'zero') ON CONFLICT (a) DO UPDATE SET b = 1, c = 'two'",
    )
    .unwrap();
    assert_eq!(n, 1);

    assert_eq!(column_values(&db, "t1", 1), vec![SqlValue::Null, SqlValue::Integer(1)]);
    assert_eq!(
        column_values(&db, "t1", 2),
        vec![SqlValue::Varchar("one".into()), SqlValue::Varchar("two".into())]
    );
}

/// A DO UPDATE moving the key to a genuinely fresh value succeeds (upsert4
/// 1.x.8 shape: SET (c, a) = ('four', 4) — no other row holds those keys).
#[test]
fn do_update_moving_key_to_fresh_value_succeeds() {
    let mut db = Database::new();
    setup(
        &mut db,
        &[
            "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, c TEXT UNIQUE)",
            "INSERT INTO t1 VALUES(1, NULL, 'one')",
            "INSERT INTO t1 VALUES(2, NULL, 'two')",
        ],
    );

    let n = run_stmt(
        &mut db,
        "INSERT INTO t1 VALUES(1, NULL, NULL) ON CONFLICT (a) DO UPDATE SET c = 'four', a = 4",
    )
    .unwrap();
    assert_eq!(n, 1);

    assert_eq!(column_values(&db, "t1", 0), vec![SqlValue::Integer(4), SqlValue::Integer(2)]);
    assert_eq!(
        column_values(&db, "t1", 2),
        vec![SqlValue::Varchar("four".into()), SqlValue::Varchar("two".into())]
    );
}
