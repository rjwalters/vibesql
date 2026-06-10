//! Integration tests for SQLite upsert: INSERT ... ON CONFLICT DO UPDATE / DO NOTHING
//!
//! Issue #5269: the DO UPDATE arm must execute (with `excluded.` support,
//! conflict-target matching, and the optional WHERE clause) instead of
//! failing with a UNIQUE constraint error.

use vibesql_executor::InsertExecutor;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute a single SQL statement against the database.
/// Returns the affected-row count for INSERT statements.
fn exec(db: &mut Database, sql: &str) -> Result<usize, vibesql_executor::ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("parse error for {sql:?}: {e:?}"));
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            vibesql_executor::CreateTableExecutor::execute(&create, db).unwrap();
            Ok(0)
        }
        vibesql_ast::Statement::CreateIndex(create) => {
            vibesql_executor::CreateIndexExecutor::execute(&create, db).unwrap();
            Ok(0)
        }
        vibesql_ast::Statement::Insert(insert) => InsertExecutor::execute(db, &insert),
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

/// Fetch all rows of a table ordered by physical position.
fn rows(db: &Database, table: &str) -> Vec<Vec<SqlValue>> {
    db.get_table(table)
        .unwrap()
        .scan_live()
        .map(|(_, row)| row.values.to_vec())
        .collect()
}

fn int(v: i64) -> SqlValue {
    SqlValue::Integer(v)
}

#[test]
fn test_do_update_basic_repro() {
    // Repro from issue #5269
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 10)").unwrap();
    let n = exec(&mut db, "INSERT INTO t VALUES (1, 99) ON CONFLICT(a) DO UPDATE SET b=42")
        .expect("upsert should succeed");
    assert_eq!(n, 1, "updated row counts toward affected rows");
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(42)]]);
}

#[test]
fn test_do_update_excluded_in_set() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 10)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 77) ON CONFLICT(a) DO UPDATE SET b=excluded.b")
        .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(77)]]);
}

#[test]
fn test_do_update_mixed_expression() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 10)").unwrap();
    // b = old b + excluded.b * 2 = 10 + 5*2 = 20
    exec(
        &mut db,
        "INSERT INTO t VALUES (1, 5) ON CONFLICT(a) DO UPDATE SET b = b + excluded.b * 2",
    )
    .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(20)]]);
}

#[test]
fn test_do_update_where_false_skips_silently() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 10)").unwrap();
    let n = exec(
        &mut db,
        "INSERT INTO t VALUES (1, 99) ON CONFLICT(a) DO UPDATE SET b=42 WHERE b > 100",
    )
    .expect("WHERE-false upsert must not error");
    assert_eq!(n, 0, "skipped row is neither inserted nor updated");
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(10)]]);
}

#[test]
fn test_do_update_where_references_excluded() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 10)").unwrap();
    // Only update when the incoming value is larger than the stored one
    exec(
        &mut db,
        "INSERT INTO t VALUES (1, 50) ON CONFLICT(a) DO UPDATE SET b=excluded.b \
         WHERE excluded.b > b",
    )
    .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(50)]]);
    exec(
        &mut db,
        "INSERT INTO t VALUES (1, 7) ON CONFLICT(a) DO UPDATE SET b=excluded.b \
         WHERE excluded.b > b",
    )
    .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(50)]], "smaller value must not overwrite");
}

#[test]
fn test_conflict_on_other_constraint_still_errors() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT UNIQUE, c INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 2, 3)").unwrap();
    // Conflict is on b (UNIQUE), but the target names a (PK): the update arm
    // must NOT fire and the UNIQUE error must surface (SQLite semantics).
    let err = exec(&mut db, "INSERT INTO t VALUES (9, 2, 4) ON CONFLICT(a) DO UPDATE SET c=99")
        .expect_err("conflict on non-target constraint must error");
    let msg = format!("{err:?}");
    assert!(msg.contains("UNIQUE constraint"), "unexpected error: {msg}");
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(2), int(3)]]);
}

#[test]
fn test_omitted_target_catches_any_conflict() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT UNIQUE, c INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 2, 3)").unwrap();
    // Conflict on the UNIQUE b constraint; no target named.
    exec(&mut db, "INSERT INTO t VALUES (9, 2, 4) ON CONFLICT DO UPDATE SET c=excluded.c")
        .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(2), int(4)]]);
}

#[test]
fn test_multi_row_mixed_fresh_and_conflicting() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    let n = exec(
        &mut db,
        "INSERT INTO t VALUES (1, 11), (3, 30), (2, 21) \
         ON CONFLICT(a) DO UPDATE SET b=excluded.b",
    )
    .unwrap();
    assert_eq!(n, 3);
    let mut all = rows(&db, "t");
    all.sort_by(|x, y| x[0].partial_cmp(&y[0]).unwrap());
    assert_eq!(all, vec![
        vec![int(1), int(11)],
        vec![int(2), int(21)],
        vec![int(3), int(30)],
    ]);
}

#[test]
fn test_same_statement_double_conflict_applies_twice() {
    // SQLite applies the update arm once per conflicting candidate row
    // (upsert1-400: two 'one' rows bump the counter twice).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a TEXT UNIQUE, b INT DEFAULT 1)").unwrap();
    exec(&mut db, "INSERT INTO t(a) VALUES('one'),('two')").unwrap();
    exec(
        &mut db,
        "INSERT INTO t(a) VALUES('one'),('one'),('three') ON CONFLICT(a) DO UPDATE SET b=b+1",
    )
    .unwrap();
    let mut all = rows(&db, "t");
    all.sort_by_key(|r| format!("{:?}", r[0]));
    assert_eq!(all.len(), 3);
    // 'one' was updated twice: 1 -> 2 -> 3
    let one = all
        .iter()
        .find(|r| matches!(&r[0], SqlValue::Varchar(s) if s.as_str() == "one"))
        .unwrap();
    assert_eq!(one[1], int(3));
}

#[test]
fn test_conflict_target_on_unique_index() {
    // Conflict detection must also consider CREATE UNIQUE INDEX indexes
    // (upsert1-730 series), not just PK/table-level UNIQUE constraints.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT, b INT, c INT)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX ta ON t(a)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 2, 3)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 9, 33) ON CONFLICT(a) DO UPDATE SET c=excluded.c")
        .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(2), int(33)]]);
}

#[test]
fn test_null_in_target_column_never_conflicts() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT UNIQUE, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (NULL, 1)").unwrap();
    // NULLs never conflict under UNIQUE semantics: this is a fresh insert.
    let n = exec(&mut db, "INSERT INTO t VALUES (NULL, 2) ON CONFLICT(a) DO UPDATE SET b=99")
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(rows(&db, "t").len(), 2);
}

#[test]
fn test_unknown_target_column_errors() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    let err = exec(&mut db, "INSERT INTO t VALUES (1, 2) ON CONFLICT(x) DO NOTHING")
        .expect_err("unknown target column must error");
    assert!(format!("{err:?}").contains("no such column: x"), "got: {err:?}");
}

#[test]
fn test_non_unique_target_column_errors() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    let err = exec(&mut db, "INSERT INTO t VALUES (1, 2) ON CONFLICT(b) DO NOTHING")
        .expect_err("non-unique target column must error");
    assert!(
        format!("{err:?}")
            .contains("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"),
        "got: {err:?}"
    );
}

#[test]
fn test_do_nothing_unchanged() {
    // Regression: ON CONFLICT ... DO NOTHING behavior is preserved.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 10)").unwrap();
    let n = exec(&mut db, "INSERT INTO t VALUES (1, 99) ON CONFLICT(a) DO NOTHING").unwrap();
    assert_eq!(n, 0);
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(10)]]);
}

#[test]
fn test_on_duplicate_key_update_unchanged() {
    // Regression: MySQL-style ON DUPLICATE KEY UPDATE path is preserved.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 10)").unwrap();
    let n = exec(&mut db, "INSERT INTO t VALUES (1, 99) ON DUPLICATE KEY UPDATE b=42").unwrap();
    assert_eq!(n, 1);
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(42)]]);
}

#[test]
fn test_do_update_updates_target_column_itself() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 10)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 99) ON CONFLICT(a) DO UPDATE SET a=2, b=excluded.b")
        .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(2), int(99)]]);
}

#[test]
fn test_returning_post_update_row() {
    // RETURNING integration (post-#5270): the update arm returns the
    // post-UPDATE row, like ON DUPLICATE KEY UPDATE.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 10)").unwrap();

    let stmt = vibesql_parser::Parser::parse_sql(
        "INSERT INTO t VALUES (1, 5) ON CONFLICT(a) DO UPDATE SET b = b + excluded.b RETURNING a, b",
    )
    .unwrap();
    let insert = match stmt {
        vibesql_ast::Statement::Insert(insert) => insert,
        other => panic!("expected INSERT, got {other:?}"),
    };
    let (count, result) = InsertExecutor::execute_returning(&mut db, &insert).unwrap();
    assert_eq!(count, 1);
    let result = result.expect("RETURNING must produce a result set");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values.to_vec(), vec![int(1), int(15)]);
}

#[test]
fn test_insert_select_source_with_on_conflict() {
    // INSERT INTO ... SELECT ... ON CONFLICT parses and executes
    // (upsert1-500/1300; parser previously rejected the trailing ON).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(x INT PRIMARY KEY, y INT UNIQUE)").unwrap();
    exec(
        &mut db,
        "INSERT INTO t(x,y) SELECT 1,2 WHERE true ON CONFLICT(x) DO UPDATE SET y=excluded.y",
    )
    .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(2)]]);
}
