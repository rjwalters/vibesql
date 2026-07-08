//! Regression tests for issue #5989 (residual scope, json102-1011): pushing a
//! left-only WHERE guard into a lateral TVF dependent join.
//!
//! The canonical shape is the malformed-JSON guard idiom:
//!
//! ```sql
//! SELECT user.name
//!   FROM user, json_each(user.phone)
//!  WHERE json_valid(user.phone)      -- left-only guard
//!    AND json_each.value LIKE '704-%';
//! ```
//!
//! After a preceding `UPDATE` sets `user.phone` to a scalar (non-JSON-array)
//! string for some rows, `json_each(user.phone)` would error ("malformed JSON")
//! on those rows. sqlite3 evaluates the `json_valid(user.phone)` guard against
//! the cross product but short-circuits so the guard excludes the malformed-JSON
//! left rows *before* `json_each` runs on them.
//!
//! VibeSQL's dependent-join loop previously evaluated the TVF for every left row,
//! so a malformed `phone` errored the whole query. This test suite verifies the
//! predicate-pushdown fix: WHERE conjuncts that reference only left-side columns
//! are evaluated per left row before the TVF, and rows failing them are skipped.
//!
//! Differential behavior (verified against sqlite3 3.51.0):
//!
//! - **With the `json_valid` guard**: malformed rows are filtered; no error.
//! - **Without the guard**: `json_each` reaches a malformed value and errors
//!   ("malformed JSON") — we match sqlite3's error behavior rather than silently
//!   filtering.
//! - **A guard referencing a TVF column** (`json_each.value ...`) is NOT pushed;
//!   it is still evaluated post-join, so it cannot pre-filter left rows.

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            vibesql_executor::InsertExecutor::execute(db, &insert).unwrap();
        }
        vibesql_ast::Statement::Update(update) => {
            vibesql_executor::UpdateExecutor::execute(&update, db).unwrap();
        }
        other => panic!("Unsupported statement in test setup: {:?}", other),
    }
}

/// Run a SELECT, returning `Ok(rows)` or the executor error, so tests can assert
/// both the success (guard filters) and error (no-guard malformed JSON) paths.
fn try_query(
    db: &vibesql_storage::Database,
    sql: &str,
) -> Result<Vec<Vec<SqlValue>>, vibesql_executor::ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor
            .execute(&select_stmt)
            .map(|rows| rows.into_iter().map(|row| row.values.to_vec()).collect())
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

fn query(db: &vibesql_storage::Database, sql: &str) -> Vec<Vec<SqlValue>> {
    try_query(db, sql).unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e))
}

/// Build the json102-1000/1010 fixture: after the UPDATE, Alice and Dave keep
/// JSON-array `phone` values; Bob and Cindy have scalar (non-array) strings
/// extracted from their single-element arrays.
fn setup_user_table() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE user(name TEXT, phone TEXT)");
    run_stmt(
        &mut db,
        r#"INSERT INTO user(name, phone) VALUES
            ('Alice', '["919-555-2345","804-555-3621"]'),
            ('Bob',   '["201-555-8872"]'),
            ('Cindy', '["704-555-9983"]'),
            ('Dave',  '["336-555-8421","704-555-4321","803-911-4421"]')"#,
    );
    // Mirror json102-1010: single-element arrays become scalar strings, which are
    // no longer valid JSON arrays -> json_each would error on them.
    run_stmt(
        &mut db,
        "UPDATE user SET phone = json_extract(phone, '$[0]') WHERE json_array_length(phone) < 2",
    );
    db
}

/// With the `json_valid(user.phone)` guard, malformed (scalar-string) `phone`
/// rows are filtered before json_each, so the query succeeds. Only Dave's array
/// contains a '704-' number reachable through the join. sqlite3 3.51.0: Dave.
#[test]
fn json_valid_guard_filters_malformed_rows_no_error() {
    let db = setup_user_table();
    let rows = query(
        &db,
        "SELECT user.name FROM user, json_each(user.phone) \
         WHERE json_valid(user.phone) AND json_each.value LIKE '704-%'",
    );
    assert_eq!(
        rows,
        vec![vec![SqlValue::Varchar("Dave".into())]],
        "guard should filter, got {:?}",
        rows
    );
}

/// The exact json102-1011 query (UNION of the scalar-LIKE branch and the guarded
/// json_each branch). sqlite3 3.51.0: Cindy, Dave.
#[test]
fn json102_1011_full_union_query() {
    let db = setup_user_table();
    let mut rows = query(
        &db,
        "SELECT name FROM user WHERE phone LIKE '704-%' \
         UNION \
         SELECT user.name FROM user, json_each(user.phone) \
         WHERE json_valid(user.phone) AND json_each.value LIKE '704-%'",
    );
    rows.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
    assert_eq!(
        rows,
        vec![vec![SqlValue::Varchar("Cindy".into())], vec![SqlValue::Varchar("Dave".into())],],
        "json102-1011 expected {{Cindy, Dave}}, got {:?}",
        rows
    );
}

/// Without the `json_valid` guard, `json_each` reaches a malformed (scalar) phone
/// value and errors. We match sqlite3 3.51.0, which reports "malformed JSON"
/// rather than silently filtering the offending rows.
#[test]
fn no_guard_malformed_json_errors_matching_sqlite() {
    let db = setup_user_table();
    let result = try_query(
        &db,
        "SELECT user.name FROM user, json_each(user.phone) WHERE json_each.value LIKE '704-%'",
    );
    let err = result.expect_err("expected a malformed-JSON error without the guard");
    let msg = format!("{}", err);
    assert!(
        msg.to_lowercase().contains("malformed json"),
        "expected a malformed-JSON error, got: {}",
        msg
    );
}

/// A guard that references a TVF column (`json_each.value`) is NOT pushed into
/// the per-left-row pre-filter — it can only be evaluated after the join. So it
/// cannot pre-filter malformed left rows; the malformed row still reaches
/// json_each and errors. This confirms we only push left-only conjuncts.
#[test]
fn guard_referencing_tvf_column_is_not_pushed() {
    let db = setup_user_table();
    // `json_each.value IS NOT NULL` references only a TVF column. It is not a
    // left-only guard, so it stays post-join and cannot suppress the malformed
    // json_each evaluation on Bob's/Cindy's scalar phone -> still errors.
    let result = try_query(
        &db,
        "SELECT user.name FROM user, json_each(user.phone) WHERE json_each.value IS NOT NULL",
    );
    let err = result.expect_err("a TVF-column-only guard must not be pushed, so still errors");
    assert!(
        format!("{}", err).to_lowercase().contains("malformed json"),
        "expected malformed-JSON error, got: {}",
        err
    );
}

/// The pushed left-only guard must not over-filter the happy path: when every
/// left row is valid JSON, all matching rows are returned. Here both Alice and
/// Dave have valid arrays; only Dave's contains a '704-' number.
#[test]
fn guard_does_not_overfilter_valid_rows() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE user(name TEXT, phone TEXT)");
    run_stmt(
        &mut db,
        r#"INSERT INTO user(name, phone) VALUES
            ('Alice', '["919-555-2345","804-555-3621"]'),
            ('Dave',  '["336-555-8421","704-555-4321"]')"#,
    );
    let rows = query(
        &db,
        "SELECT user.name FROM user, json_each(user.phone) \
         WHERE json_valid(user.phone) AND json_each.value LIKE '704-%'",
    );
    assert_eq!(rows, vec![vec![SqlValue::Varchar("Dave".into())]], "got {:?}", rows);
}
