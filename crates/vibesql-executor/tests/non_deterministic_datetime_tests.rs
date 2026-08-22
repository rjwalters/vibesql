//! Evaluation-time rejection of non-deterministic date/time functions in
//! schema-attached expressions (CHECK constraints, generated columns, index
//! expressions, partial-index WHERE predicates).
//!
//! SQLite semantics (date.c / vdbeapi.c `sqlite3NotPureFunc`), verified by
//! the conformance file `date2.test`:
//!
//! - The rejection happens when the schema expression is EVALUATED (INSERT/UPDATE/CREATE INDEX
//!   build), never at DDL time: `CREATE TABLE t(a CHECK(a < julianday('now')))` succeeds.
//! - The trigger can come from row data (`date(x)` with the value `'now'`).
//! - Triggers: resolving the current time (`'now'`, zero-argument `date()`) or applying the
//!   `'localtime'`/`'utc'` modifiers.
//! - Error text: `non-deterministic use of <fn>() in a CHECK constraint` / `in a generated column`
//!   / `in an index` (lowercase function name; partial-index WHERE clauses also report "an index").
//!
//! Issue #5313.

use vibesql_executor::{
    CreateIndexExecutor, CreateTableExecutor, DeleteExecutor, InsertExecutor, UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Execute a single SQL statement, returning the error message on failure.
fn try_sql(db: &mut Database, sql: &str) -> Result<(), String> {
    let stmt = Parser::parse_sql(sql.trim()).expect("Failed to parse SQL");
    use vibesql_ast::Statement;
    let result = match &stmt {
        Statement::CreateTable(s) => CreateTableExecutor::execute(s, db).map(|_| ()),
        Statement::CreateIndex(s) => CreateIndexExecutor::execute(s, db).map(|_| ()),
        Statement::Insert(s) => InsertExecutor::execute(db, s).map(|_| ()),
        Statement::Update(s) => UpdateExecutor::execute(s, db).map(|_| ()),
        Statement::Delete(s) => DeleteExecutor::execute(s, db).map(|_| ()),
        other => panic!("Unsupported statement type in test: {:?}", other),
    };
    result.map_err(|e| e.to_string())
}

/// Execute SQL statements (separated by ';'), panicking on any failure.
fn run_sql(db: &mut Database, sql: &str) {
    for stmt in sql.split(';') {
        if stmt.trim().is_empty() {
            continue;
        }
        if let Err(e) = try_sql(db, stmt) {
            panic!("SQL failed: {}\nerror: {}", stmt.trim(), e);
        }
    }
}

/// Assert `sql` fails with exactly `expected` as the error message.
fn assert_sql_error(db: &mut Database, sql: &str, expected: &str) {
    match try_sql(db, sql) {
        Ok(()) => panic!("expected error `{}`, but SQL succeeded: {}", expected, sql),
        Err(e) => assert_eq!(e, expected, "wrong error for: {}", sql),
    }
}

fn row_count(db: &Database, table: &str) -> usize {
    db.get_table(table).expect("table not found").scan_live().count()
}

// ============================================================================
// CHECK constraints (date2-100..130, date2-600..604)
// ============================================================================

#[test]
fn check_constraint_literal_rows_accepted_now_value_rejected() {
    // date2-100/110/120/130: the trigger comes from ROW DATA, not schema text
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t1(x, y, CHECK( date(x) BETWEEN '2017-07-01' AND '2017-07-31' ));
         INSERT INTO t1(x,y) VALUES('2017-07-20','one')",
    );
    assert_sql_error(
        &mut db,
        "INSERT INTO t1(x,y) VALUES('now','two')",
        "non-deterministic use of date() in a CHECK constraint",
    );
    // The rejected row must not be inserted (date2-120)
    assert_eq!(row_count(&db, "t1"), 1);
    // An ordinary CHECK violation still reports the plain CHECK error (date2-130)
    let err = try_sql(&mut db, "INSERT INTO t1(x,y) VALUES('2017-08-01','two')").unwrap_err();
    assert!(err.starts_with("CHECK constraint failed:"), "got: {}", err);
}

#[test]
fn check_constraint_with_now_in_schema_rejected_at_insert_not_ddl() {
    // date2-600: CREATE TABLE succeeds; the INSERT raises the error
    let mut db = Database::new();
    run_sql(&mut db, "CREATE TABLE t600(a REAL CHECK( a < julianday('now') ))");
    assert_sql_error(
        &mut db,
        "INSERT INTO t600(a) VALUES(1.0)",
        "non-deterministic use of julianday() in a CHECK constraint",
    );
    // date2-604: the offending call inside the CHECK fires even when the
    // VALUES expression itself uses julianday('now') legitimately
    assert_sql_error(
        &mut db,
        "INSERT INTO t600(a) VALUES(julianday('now')+10)",
        "non-deterministic use of julianday() in a CHECK constraint",
    );
}

#[test]
fn check_constraint_literal_only_calls_remain_accepted() {
    // date2-601/602/603
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t601(a REAL, b TEXT, CHECK( a < julianday(b) ));
         INSERT INTO t601(a,b) VALUES(1.0, '1970-01-01')",
    );
    let err = try_sql(&mut db, "INSERT INTO t601(a,b) VALUES(1e100, '1970-01-01')").unwrap_err();
    assert!(err.starts_with("CHECK constraint failed:"), "got: {}", err);
    assert_sql_error(
        &mut db,
        "INSERT INTO t601(a,b) VALUES(10, 'now')",
        "non-deterministic use of julianday() in a CHECK constraint",
    );
}

#[test]
fn check_constraint_rejects_now_on_update() {
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t1(x, CHECK( date(x) BETWEEN '2017-07-01' AND '2017-07-31' ));
         INSERT INTO t1(x) VALUES('2017-07-20')",
    );
    assert_sql_error(
        &mut db,
        "UPDATE t1 SET x='now'",
        "non-deterministic use of date() in a CHECK constraint",
    );
}

#[test]
fn check_constraint_localtime_modifier_rejected() {
    // 'localtime'/'utc' trigger even with a literal base value
    let mut db = Database::new();
    run_sql(&mut db, "CREATE TABLE t(x, CHECK( datetime(x,'localtime') IS NOT NULL ))");
    assert_sql_error(
        &mut db,
        "INSERT INTO t(x) VALUES('2020-01-01')",
        "non-deterministic use of datetime() in a CHECK constraint",
    );
}

#[test]
fn ordinary_contexts_unaffected() {
    // datetime('now') etc. keep working outside schema-attached expressions
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE plain(a, b);
         INSERT INTO plain(a, b) VALUES(julianday('now'), datetime('now', 'localtime'))",
    );
    assert_eq!(row_count(&db, "plain"), 1);
}

// ============================================================================
// Generated columns (date2-140, date3-620)
// ============================================================================

#[test]
fn generated_column_zero_arg_date_rejected_at_insert() {
    // date2-140: zero-argument date() defaults to 'now'
    let mut db = Database::new();
    run_sql(&mut db, "CREATE TABLE t1(x, y, z AS (date()))");
    assert_sql_error(
        &mut db,
        "INSERT INTO t1(x,y) VALUES(1,2)",
        "non-deterministic use of date() in a generated column",
    );
}

#[test]
fn generated_column_julianday_now_rejected_at_insert() {
    // date3-620 (lives in date2.test)
    let mut db = Database::new();
    run_sql(&mut db, "CREATE TABLE t620(a, b AS (a+julianday('now')))");
    assert_sql_error(
        &mut db,
        "INSERT INTO t620 VALUES(10)",
        "non-deterministic use of julianday() in a generated column",
    );
}

#[test]
fn generated_column_literal_only_call_accepted() {
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t(a, b AS (julianday(a)));
         INSERT INTO t(a) VALUES('2000-01-01')",
    );
    assert_eq!(row_count(&db, "t"), 1);
}

// ============================================================================
// Index expressions (date2-200..220, date2-610..612)
// ============================================================================

#[test]
fn expression_index_insert_of_now_value_rejected() {
    // date2-200/210/220
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t2(x,y);
         INSERT INTO t2(x,y) VALUES(1, '2017-07-20'), (2, 'xyzzy');
         CREATE INDEX t2y ON t2(date(y))",
    );
    assert_sql_error(
        &mut db,
        "INSERT INTO t2(x,y) VALUES(3, 'now')",
        "non-deterministic use of date() in an index",
    );
    // date2-220: the offending row must NOT be in the table
    assert_eq!(row_count(&db, "t2"), 2);
}

#[test]
fn expression_index_update_to_now_value_rejected() {
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t2(x,y);
         INSERT INTO t2(x,y) VALUES(1, '2017-07-20');
         CREATE INDEX t2y ON t2(date(y))",
    );
    assert_sql_error(
        &mut db,
        "UPDATE t2 SET y='now'",
        "non-deterministic use of date() in an index",
    );
}

#[test]
fn expression_index_with_now_in_schema_create_on_empty_table_succeeds() {
    // date2-610/611/612: CREATE INDEX evaluates nothing on an empty table;
    // the first INSERT that evaluates the expression fails
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t610(a,b);
         CREATE INDEX t610x1 ON t610(julianday('now')+b)",
    );
    assert_sql_error(
        &mut db,
        "INSERT INTO t610(a,b) VALUES(123,456)",
        "non-deterministic use of julianday() in an index",
    );

    // date2-611: literal-only evaluation through julianday(a)+b is fine
    run_sql(
        &mut db,
        "CREATE TABLE t611(a,b);
         CREATE INDEX t611x1 ON t611(julianday(a)+b);
         INSERT INTO t611(a,b) VALUES('1970-01-01',10.0)",
    );
    // date2-612: row data 'now' rejected
    assert_sql_error(
        &mut db,
        "INSERT INTO t611(a,b) VALUES('now',10.0)",
        "non-deterministic use of julianday() in an index",
    );
}

// ============================================================================
// CREATE INDEX build over existing rows (date2-300..331)
// ============================================================================

/// Build a table with literal julianday values plus one 'now' text row.
fn setup_table_with_now_row(db: &mut Database, table: &str) {
    run_sql(db, &format!("CREATE TABLE {t}(a INTEGER PRIMARY KEY, b)", t = table));
    for i in 1..=10 {
        run_sql(
            db,
            &format!(
                "INSERT INTO {t}(a,b) VALUES({i}, julianday('2017-07-01')+{i})",
                t = table,
                i = i
            ),
        );
    }
    run_sql(db, &format!("UPDATE {t} SET b='now' WHERE a=5", t = table));
}

#[test]
fn create_expression_index_fails_when_existing_row_is_now() {
    // date2-310: the build evaluates datetime(b) on the 'now' row
    let mut db = Database::new();
    setup_table_with_now_row(&mut db, "t3");
    assert_sql_error(
        &mut db,
        "CREATE INDEX t3b1 ON t3(datetime(b))",
        "non-deterministic use of datetime() in an index",
    );
    // date2-320: a partial index whose WHERE excludes the 'now' row succeeds,
    // including reusing the SAME index name (the failed CREATE INDEX must not
    // leave a phantom catalog entry)
    run_sql(&mut db, "CREATE INDEX t3b1 ON t3(datetime(b)) WHERE typeof(b)='real'");
}

#[test]
fn create_partial_index_fails_when_predicate_hits_now_row() {
    // date2-410/420/430
    let mut db = Database::new();
    setup_table_with_now_row(&mut db, "t4");
    assert_sql_error(
        &mut db,
        "CREATE INDEX t4b1 ON t4(b) WHERE date(b) BETWEEN '2017-06-01' AND '2017-08-31'",
        "non-deterministic use of date() in an index",
    );
    // date2-420: after deleting the offending row, the same CREATE succeeds
    run_sql(
        &mut db,
        "DELETE FROM t4 WHERE a=5;
         CREATE INDEX t4b1 ON t4(b) WHERE date(b) BETWEEN '2017-06-01' AND '2017-08-31'",
    );
    // date2-430: inserting 'now' re-evaluates the predicate and fails
    assert_sql_error(
        &mut db,
        "INSERT INTO t4(a,b) VALUES(9999,'now')",
        "non-deterministic use of date() in an index",
    );
    assert_eq!(row_count(&db, "t4"), 9);
}

// ============================================================================
// UPDATE OR IGNORE / OR REPLACE conflict clauses (issue #5324)
//
// SQLite aborts the statement even under OR IGNORE / OR REPLACE: the
// non-deterministic error is a runtime SQL function error, not a constraint
// conflict, so conflict resolution does not apply. Verified with sqlite3
// 3.51.0:
//
//     CREATE TABLE t2(a INT, b TEXT);
//     CREATE INDEX i2 ON t2(date(b));
//     INSERT INTO t2 VALUES(1,'2024-01-01');
//     UPDATE OR IGNORE t2 SET b='now';
//     -- Runtime error: non-deterministic use of date() in an index
//     -- row remains b='2024-01-01'
// ============================================================================

/// Fetch column `col` of every live row in `table` as display strings.
fn column_values(db: &Database, table: &str, col: usize) -> Vec<String> {
    db.get_table(table)
        .expect("table not found")
        .scan_live()
        .map(|(_, row)| format!("{}", row.values[col]))
        .collect()
}

fn setup_expression_index_table(db: &mut Database) {
    run_sql(
        db,
        "CREATE TABLE t2(a INT, b TEXT);
         INSERT INTO t2 VALUES(1, '2024-01-01');
         CREATE INDEX i2 ON t2(date(b))",
    );
}

#[test]
fn update_or_ignore_expression_index_now_value_rejected() {
    let mut db = Database::new();
    setup_expression_index_table(&mut db);
    assert_sql_error(
        &mut db,
        "UPDATE OR IGNORE t2 SET b='now'",
        "non-deterministic use of date() in an index",
    );
    // The statement must abort with no mutation: the row keeps its old value
    assert_eq!(column_values(&db, "t2", 1), vec!["2024-01-01"]);
}

#[test]
fn update_or_replace_expression_index_now_value_rejected() {
    let mut db = Database::new();
    setup_expression_index_table(&mut db);
    assert_sql_error(
        &mut db,
        "UPDATE OR REPLACE t2 SET b='now'",
        "non-deterministic use of date() in an index",
    );
    assert_eq!(column_values(&db, "t2", 1), vec!["2024-01-01"]);
}

#[test]
fn update_or_ignore_and_or_replace_deterministic_values_still_work() {
    let mut db = Database::new();
    setup_expression_index_table(&mut db);
    run_sql(&mut db, "UPDATE OR IGNORE t2 SET b='2024-02-02'");
    assert_eq!(column_values(&db, "t2", 1), vec!["2024-02-02"]);
    run_sql(&mut db, "UPDATE OR REPLACE t2 SET b='2024-03-03'");
    assert_eq!(column_values(&db, "t2", 1), vec!["2024-03-03"]);
}

#[test]
fn update_or_ignore_partial_index_predicate_now_value_rejected() {
    // The partial-index WHERE predicate counts as an index expression
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t6(a INT, b TEXT);
         INSERT INTO t6 VALUES(1, '2024-01-01');
         CREATE INDEX i6 ON t6(b) WHERE date(b) > '2020-01-01'",
    );
    assert_sql_error(
        &mut db,
        "UPDATE OR IGNORE t6 SET b='now'",
        "non-deterministic use of date() in an index",
    );
    assert_eq!(column_values(&db, "t6", 1), vec!["2024-01-01"]);
}

#[test]
fn update_or_ignore_check_constraint_now_value_aborts() {
    // A non-deterministic use inside a CHECK constraint is a statement-level
    // error under OR IGNORE, not an ignorable conflict (SQLite aborts).
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t7(x, CHECK( date(x) BETWEEN '2017-07-01' AND '2017-07-31' ));
         INSERT INTO t7(x) VALUES('2017-07-20')",
    );
    assert_sql_error(
        &mut db,
        "UPDATE OR IGNORE t7 SET x='now'",
        "non-deterministic use of date() in a CHECK constraint",
    );
    assert_eq!(column_values(&db, "t7", 0), vec!["2017-07-20"]);
}

#[test]
fn update_from_or_ignore_expression_index_now_value_rejected() {
    // The UPDATE ... FROM dispatch path has its own IGNORE/REPLACE branches
    let mut db = Database::new();
    setup_expression_index_table(&mut db);
    run_sql(&mut db, "CREATE TABLE src(a INT, v TEXT); INSERT INTO src VALUES(1, 'now')");
    assert_sql_error(
        &mut db,
        "UPDATE OR IGNORE t2 SET b=src.v FROM src WHERE t2.a=src.a",
        "non-deterministic use of date() in an index",
    );
    assert_eq!(column_values(&db, "t2", 1), vec!["2024-01-01"]);
}

#[test]
fn update_from_or_replace_expression_index_now_value_rejected() {
    let mut db = Database::new();
    setup_expression_index_table(&mut db);
    run_sql(&mut db, "CREATE TABLE src(a INT, v TEXT); INSERT INTO src VALUES(1, 'now')");
    assert_sql_error(
        &mut db,
        "UPDATE OR REPLACE t2 SET b=src.v FROM src WHERE t2.a=src.a",
        "non-deterministic use of date() in an index",
    );
    assert_eq!(column_values(&db, "t2", 1), vec!["2024-01-01"]);
}

// ============================================================================
// 'localtime' / 'utc' modifiers from row data (date2-500..520)
// ============================================================================

#[test]
fn partial_index_predicate_rejects_row_supplied_localtime_and_utc() {
    let mut db = Database::new();
    run_sql(
        &mut db,
        "CREATE TABLE t5(y,m);
         INSERT INTO t5(y,m) VALUES(julianday('2017-07-01')+1, '+10 days');
         INSERT INTO t5(y,m) VALUES(julianday('2017-07-01')+2, 'start of month');
         INSERT INTO t5(y,m) VALUES(julianday('2017-07-01')+3, 'weekday 1');
         CREATE INDEX t5x1 ON t5(y) WHERE datetime(y,m) IS NOT NULL",
    );
    // date2-510
    assert_sql_error(
        &mut db,
        "INSERT INTO t5(y,m) VALUES('2017-07-20','localtime')",
        "non-deterministic use of datetime() in an index",
    );
    // date2-520
    assert_sql_error(
        &mut db,
        "INSERT INTO t5(y,m) VALUES('2017-07-20','utc')",
        "non-deterministic use of datetime() in an index",
    );
    assert_eq!(row_count(&db, "t5"), 3);
}
