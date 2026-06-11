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
    let outcome = InsertExecutor::execute_returning(&mut db, &insert).unwrap();
    assert_eq!(outcome.affected_rows, 1);
    // The single affected row was handled via the DO UPDATE arm
    assert_eq!(outcome.upsert_updated_rows, 1);
    let result = outcome.returning.expect("RETURNING must produce a result set");
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

#[test]
fn test_delete_all_then_reinsert_unique_index_value() {
    // Regression (added during review of PR #5277): the DELETE truncate fast
    // path cleared the table but left database-level index data stale, so
    // re-inserting a previously-deleted unique value failed with a spurious
    // UNIQUE constraint error (upsert1-710/740/770).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT, b INT)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX ta ON t(a)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 2)").unwrap();

    let delete_stmt = match vibesql_parser::Parser::parse_sql("DELETE FROM t").unwrap() {
        vibesql_ast::Statement::Delete(d) => d,
        other => panic!("expected DELETE, got {other:?}"),
    };
    vibesql_executor::DeleteExecutor::execute(&delete_stmt, &mut db).unwrap();

    exec(&mut db, "INSERT INTO t VALUES (1, 3)")
        .expect("reinsert after DELETE FROM must not raise a spurious UNIQUE error");
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(3)]]);
}

#[test]
fn test_insert_with_unique_expression_index_does_not_panic() {
    // Regression (added during review of PR #5277): INSERT into a table with
    // a UNIQUE expression index panicked via expect_column_name() in both the
    // executor's enforce_unique_indexes and storage's
    // check_unique_constraints_for_insert (upsert1-800).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT, b INT)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX tab ON t(a+b)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 2)")
        .expect("insert with unique expression index must not panic");
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(2)]]);
}

// ============================================================================
// Issue #5279: subqueries in the DO UPDATE arm (SET and WHERE)
//
// Reference results below were verified against sqlite3 during curation.
// ============================================================================

/// Setup shared by the subquery tests: t(a,b) with row (1, 5) and
/// other(k,b) with row (1, 100).
fn setup_subquery_db() -> Database {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 5)").unwrap();
    exec(&mut db, "CREATE TABLE other(k INT, b INT)").unwrap();
    exec(&mut db, "INSERT INTO other VALUES (1, 100)").unwrap();
    db
}

#[test]
fn test_do_update_set_plain_scalar_subquery() {
    // Previously failed with "Subquery execution requires database reference".
    let mut db = setup_subquery_db();
    exec(&mut db, "INSERT INTO t VALUES (1, 0) ON CONFLICT(a) DO UPDATE SET b = (SELECT 42)")
        .expect("plain scalar subquery in SET must execute");
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(42)]]);
}

#[test]
fn test_do_update_subquery_inner_scope_wins() {
    // Case 1: unqualified `b` inside the subquery resolves to other.b
    // (innermost scope), not the outer row's b. SQLite: b = 100.
    let mut db = setup_subquery_db();
    exec(
        &mut db,
        "INSERT INTO t VALUES (1, 0) ON CONFLICT(a) DO UPDATE SET b = (SELECT max(b) FROM other)",
    )
    .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(100)]]);
}

#[test]
fn test_do_update_subquery_excluded_alias_shadows_pseudo_table() {
    // Case 2: a FROM alias named `excluded` shadows the upsert pseudo-table,
    // so excluded.b resolves to other.b. SQLite: b = 100.
    let mut db = setup_subquery_db();
    exec(
        &mut db,
        "INSERT INTO t VALUES (1, 0) ON CONFLICT(a) DO UPDATE \
         SET b = (SELECT excluded.b FROM other AS excluded)",
    )
    .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(100)]]);
}

#[test]
fn test_do_update_subquery_correlated_excluded_ref() {
    // Case 3: correlated excluded. ref inside a non-shadowing subquery
    // resolves to the would-be-inserted row. SQLite: b = 7 + 100 = 107.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INT PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES (1, 5)").unwrap();
    exec(&mut db, "CREATE TABLE other(k INT, c INT)").unwrap();
    exec(&mut db, "INSERT INTO other VALUES (1, 100)").unwrap();
    exec(
        &mut db,
        "INSERT INTO t VALUES (1, 7) ON CONFLICT(a) DO UPDATE \
         SET b = (SELECT excluded.b + max(c) FROM other)",
    )
    .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(107)]]);
}

#[test]
fn test_do_update_subquery_excluded_in_subquery_where() {
    // Case 4: correlated excluded. ref in the subquery's WHERE clause.
    // Insert value 0: excluded.b + 1 = 1 matches o.k = 1 -> max(o.b) = 100.
    let mut db = setup_subquery_db();
    exec(&mut db, "INSERT INTO other VALUES (2, 200)").unwrap();
    exec(
        &mut db,
        "INSERT INTO t VALUES (1, 0) ON CONFLICT(a) DO UPDATE \
         SET b = (SELECT max(o.b) FROM other o WHERE o.k = excluded.b + 1)",
    )
    .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(100)]]);
}

#[test]
fn test_do_update_subquery_qualified_target_ref_correlates() {
    // Case 5: t.b (target-table-qualified) correlates to the existing row
    // while unqualified b stays inner-scope. SQLite: b = 5 + 100 = 105.
    let mut db = setup_subquery_db();
    exec(
        &mut db,
        "INSERT INTO t VALUES (1, 0) ON CONFLICT(a) DO UPDATE \
         SET b = (SELECT t.b + b FROM other)",
    )
    .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(105)]]);
}

#[test]
fn test_do_update_where_exists_subquery() {
    // EXISTS in the DO UPDATE ... WHERE clause previously failed with
    // "EXISTS requires database reference".
    let mut db = setup_subquery_db();
    exec(
        &mut db,
        "INSERT INTO t VALUES (1, 0) ON CONFLICT(a) DO UPDATE SET b = 42 \
         WHERE EXISTS (SELECT 1 FROM other)",
    )
    .expect("EXISTS in DO UPDATE WHERE must execute");
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(42)]]);
}

#[test]
fn test_do_update_where_exists_subquery_false_skips() {
    // EXISTS over an empty table is false: the row is silently dropped.
    let mut db = setup_subquery_db();
    exec(&mut db, "CREATE TABLE empty_t(x INT)").unwrap();
    let n = exec(
        &mut db,
        "INSERT INTO t VALUES (1, 0) ON CONFLICT(a) DO UPDATE SET b = 42 \
         WHERE EXISTS (SELECT 1 FROM empty_t)",
    )
    .unwrap();
    assert_eq!(n, 0, "skipped row is neither inserted nor updated");
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(5)]]);
}

#[test]
fn test_do_update_excluded_unknown_column_still_errors() {
    // Top-level excluded.<unknown> keeps SQLite's prepare-time error.
    let mut db = setup_subquery_db();
    let err = exec(
        &mut db,
        "INSERT INTO t VALUES (1, 0) ON CONFLICT(a) DO UPDATE SET b = excluded.nope",
    )
    .expect_err("unknown excluded column must error");
    assert!(
        format!("{err:?}").contains("no such column: excluded.nope"),
        "got: {err:?}"
    );
}

#[test]
fn test_do_update_in_subquery_in_set() {
    // IN (SELECT ...) inside a SET expression (CASE) executes with the db
    // reference and resolves the excluded. ref in the outer scope.
    let mut db = setup_subquery_db();
    exec(
        &mut db,
        "INSERT INTO t VALUES (1, 1) ON CONFLICT(a) DO UPDATE \
         SET b = CASE WHEN excluded.a IN (SELECT k FROM other) THEN 77 ELSE 0 END",
    )
    .unwrap();
    assert_eq!(rows(&db, "t"), vec![vec![int(1), int(77)]]);
}

// ============================================================================
// Expression-index and partial-index conflict targets (issue #5278,
// upsert1-200/201/210/300/310/320)
// ============================================================================

#[test]
fn test_expression_index_target_do_nothing() {
    // upsert1-200: ON CONFLICT(a+b) matches CREATE UNIQUE INDEX t1x1 ON t1(a+b)
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT, c DEFAULT 0)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX t1x1 ON t1(a+b)").unwrap();
    let n = exec(&mut db, "INSERT INTO t1(a,b) VALUES(7,8) ON CONFLICT(a+b) DO NOTHING").unwrap();
    assert_eq!(n, 1);
    // Both rows conflict on a+b=15 with the existing (7,8) row.
    let n =
        exec(&mut db, "INSERT INTO t1(a,b) VALUES(8,7),(9,6) ON CONFLICT(a+b) DO NOTHING").unwrap();
    assert_eq!(n, 0, "conflicting rows must be silently skipped");
    assert_eq!(rows(&db, "t1"), vec![vec![int(7), int(8), int(0)]]);
}

#[test]
fn test_expression_index_violation_error_format() {
    // upsert1-201: a conflict on a NON-targeted unique expression index must
    // raise SQLite's index-name error format, not be silently skipped.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT, c DEFAULT 0)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX t1x1 ON t1(a+b)").unwrap();
    exec(&mut db, "INSERT INTO t1(a,b) VALUES(7,8)").unwrap();
    let err = exec(&mut db, "INSERT INTO t1(a,b) VALUES(8,7),(9,6) ON CONFLICT(a) DO NOTHING")
        .expect_err("conflict on non-targeted expression index must error");
    assert!(format!("{err:?}").contains("UNIQUE constraint failed: index 't1x1'"), "got: {err:?}");
}

#[test]
fn test_insert_or_ignore_with_unique_expression_index_no_panic() {
    // Regression test for the pre-existing panic: check_would_violate_constraints
    // called expect_column_name() on expression-index components
    // ("Expression indexes are not supported in this context").
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT, c DEFAULT 0)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX t1x1 ON t1(a+b)").unwrap();
    exec(&mut db, "INSERT INTO t1(a,b) VALUES(7,8)").unwrap();
    let stmt =
        vibesql_parser::Parser::parse_sql("INSERT OR IGNORE INTO t1(a,b) VALUES(8,7)").unwrap();
    let vibesql_ast::Statement::Insert(insert) = stmt else { panic!("expected INSERT") };
    let n = InsertExecutor::execute(&mut db, &insert).expect("OR IGNORE must not panic or error");
    assert_eq!(n, 0, "conflicting row must be ignored");
    assert_eq!(rows(&db, "t1"), vec![vec![int(7), int(8), int(0)]]);
}

#[test]
fn test_plain_insert_unique_expression_index_enforced() {
    // A plain INSERT violating a unique expression index must error with
    // SQLite's index-name format (previously the violation was not enforced).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX t1x1 ON t1(a+b)").unwrap();
    exec(&mut db, "INSERT INTO t1(a,b) VALUES(7,8)").unwrap();
    let err = exec(&mut db, "INSERT INTO t1(a,b) VALUES(8,7)")
        .expect_err("duplicate expression-index key must error");
    assert!(format!("{err:?}").contains("UNIQUE constraint failed: index 't1x1'"), "got: {err:?}");
}

#[test]
fn test_null_expression_index_key_never_conflicts() {
    // NULL expression keys never conflict under UNIQUE semantics.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX t1x1 ON t1(a+b)").unwrap();
    exec(&mut db, "INSERT INTO t1(a,b) VALUES(1,NULL)").unwrap();
    let n = exec(&mut db, "INSERT INTO t1(a,b) VALUES(2,NULL)").unwrap();
    assert_eq!(n, 1, "NULL keys must not conflict");
    assert_eq!(rows(&db, "t1").len(), 2);
}

#[test]
fn test_expression_target_structural_mismatch_errors() {
    // upsert1-210: a+(+b) must NOT match the index on a+b.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX t1x1 ON t1(a+b)").unwrap();
    let err = exec(&mut db, "INSERT INTO t1(a,b) VALUES(9,10) ON CONFLICT(a+(+b)) DO NOTHING")
        .expect_err("structurally different expression target must not match");
    assert!(
        format!("{err:?}")
            .contains("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"),
        "got: {err:?}"
    );
}

#[test]
fn test_partial_index_target_do_nothing() {
    // upsert1-320: ON CONFLICT(b) WHERE b>10 matches the partial unique
    // index, including conflicts with earlier rows of the same batch.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT, c DEFAULT 0)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX t1x1 ON t1(b) WHERE b>10").unwrap();
    let n = exec(
        &mut db,
        "INSERT INTO t1(a,b) VALUES(1,2),(3,2),(4,20),(5,20) \
         ON CONFLICT(b) WHERE b>10 DO NOTHING",
    )
    .unwrap();
    // (5,20) conflicts with (4,20) through the partial index; the duplicate
    // b=2 rows are outside the index and both insert.
    assert_eq!(n, 3);
    assert_eq!(
        rows(&db, "t1"),
        vec![
            vec![int(1), int(2), int(0)],
            vec![int(3), int(2), int(0)],
            vec![int(4), int(20), int(0)],
        ]
    );
}

#[test]
fn test_bare_column_target_does_not_match_partial_index() {
    // upsert1-300: ON CONFLICT(b) without WHERE must not match a partial index.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX t1x1 ON t1(b) WHERE b>10").unwrap();
    let err = exec(&mut db, "INSERT INTO t1(a,b) VALUES(1,2),(3,2) ON CONFLICT(b) DO NOTHING")
        .expect_err("bare column target must not match a partial index");
    assert!(
        format!("{err:?}")
            .contains("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"),
        "got: {err:?}"
    );
}

#[test]
fn test_mismatched_target_where_does_not_match_partial_index() {
    // upsert1-310: WHERE b!=10 must not match the index predicate WHERE b>10.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX t1x1 ON t1(b) WHERE b>10").unwrap();
    let err = exec(
        &mut db,
        "INSERT INTO t1(a,b) VALUES(1,2),(3,2) ON CONFLICT(b) WHERE b!=10 DO NOTHING",
    )
    .expect_err("mismatched target WHERE must not match the index predicate");
    assert!(
        format!("{err:?}")
            .contains("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"),
        "got: {err:?}"
    );
}

#[test]
fn test_expression_index_target_do_update() {
    // The DO UPDATE arm must also match expression-index targets.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT, c DEFAULT 0)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX t1x1 ON t1(a+b)").unwrap();
    exec(&mut db, "INSERT INTO t1(a,b) VALUES(7,8)").unwrap();
    let n = exec(
        &mut db,
        "INSERT INTO t1(a,b) VALUES(8,7) ON CONFLICT(a+b) DO UPDATE SET c=excluded.a",
    )
    .unwrap();
    assert_eq!(n, 1, "update arm counts toward affected rows");
    assert_eq!(rows(&db, "t1"), vec![vec![int(7), int(8), int(8)]]);
}

#[test]
fn test_targeted_do_nothing_other_plain_constraint_still_errors() {
    // A targeted DO NOTHING must not suppress conflicts on other plain
    // unique constraints (SQLite semantics; companion to upsert1-201).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t1(a,b) VALUES(1,5)").unwrap();
    let err = exec(&mut db, "INSERT INTO t1(a,b) VALUES(2,5) ON CONFLICT(a) DO NOTHING")
        .expect_err("conflict on the non-targeted UNIQUE column must error");
    assert!(format!("{err:?}").contains("UNIQUE constraint failed"), "got: {err:?}");
}
