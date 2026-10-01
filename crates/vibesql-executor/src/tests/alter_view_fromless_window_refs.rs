//! ALTER TABLE ... RENAME re-resolves every view. A FROM-less view whose named
//! `WINDOW` frame bound contains a scalar subquery whose CTE body mentions a
//! bare column has nothing to resolve it against, so SQLite aborts with
//! `error in view <v>: no such column: <c>` (altertab3.test 19.1.2 / 19.3.2).
//! Bare names elsewhere in the window definition (PARTITION BY / ORDER BY /
//! frame bound outside a CTE) are not resolved by SQLite and must not block
//! the RENAME.

use vibesql_ast::Statement;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

fn exec(db: &mut Database, sql: &str) -> Result<(), ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute_with_source(&s, db, Some(sql)).map(|_| ())
        }
        Statement::CreateView(mut s) => {
            s.sql_definition = Some(sql.to_string());
            crate::advanced_objects::execute_create_view(&s, db)
        }
        Statement::AlterTable(s) => {
            crate::alter::AlterTableExecutor::execute_with_source(&s, db, Some(sql)).map(|_| ())
        }
        other => panic!("unexpected statement: {:?}", other),
    }
}

fn rename_result(view_sql: &str) -> Result<(), ExecutorError> {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE a(a, h)").unwrap();
    exec(&mut db, view_sql).unwrap();
    exec(&mut db, "ALTER TABLE a RENAME TO g")
}

#[test]
fn frame_bound_subquery_cte_missing_column_rejects_rename() {
    let err = rename_result(
        "CREATE VIEW q AS SELECT 123 WINDOW x AS (RANGE BETWEEN UNBOUNDED PRECEDING AND \
         INDEXED() OVER(PARTITION BY (WITH x AS(VALUES(col1)) VALUES(453))) FOLLOWING)",
    )
    .unwrap_err();
    assert!(err.to_string().contains("error in view q: no such column: col1"), "{err}");
}

#[test]
fn count_frame_bound_missing_column_rejects_rename() {
    let err = rename_result(
        "CREATE VIEW q AS SELECT 99 WINDOW x AS (RANGE BETWEEN UNBOUNDED PRECEDING AND \
         count(*)OVER(PARTITION BY (WITH a AS(VALUES(2),(x3))VALUES(0))) FOLLOWING)",
    )
    .unwrap_err();
    assert!(err.to_string().contains("error in view q: no such column: x3"), "{err}");
}

#[test]
fn resolvable_fromless_window_view_allows_rename() {
    rename_result(
        "CREATE VIEW q AS SELECT 1 WINDOW x AS (ORDER BY 1 ROWS BETWEEN 1 PRECEDING AND \
         count(*) OVER (PARTITION BY (WITH c AS (VALUES(2)) VALUES(0))) FOLLOWING)",
    )
    .expect("valid view must not block RENAME");
}

/// SQLite does not resolve bare names in a named window's ORDER BY on the
/// ALTER re-parse, alias or not, so these views must not block RENAME.
#[test]
fn alias_in_window_order_by_allows_rename() {
    rename_result("CREATE VIEW q AS SELECT 1 AS zz WINDOW x AS (ORDER BY zz)")
        .expect("select-list alias in WINDOW ORDER BY must not block RENAME");
}

/// Same as above for named-window PARTITION BY.
#[test]
fn alias_in_window_partition_by_allows_rename() {
    rename_result("CREATE VIEW q AS SELECT 1 AS zz WINDOW x AS (PARTITION BY zz)")
        .expect("select-list alias in WINDOW PARTITION BY must not block RENAME");
}

/// A non-alias bare name in WINDOW ORDER BY is accepted by sqlite3 3.54.0.
#[test]
fn non_alias_in_window_order_by_allows_rename() {
    rename_result("CREATE VIEW q AS SELECT 1 AS zz WINDOW x AS (ORDER BY yy)")
        .expect("bare name in WINDOW ORDER BY must not block RENAME");
}

/// A non-alias bare name in WINDOW PARTITION BY is accepted by sqlite3 3.54.0.
#[test]
fn non_alias_in_window_partition_by_allows_rename() {
    rename_result("CREATE VIEW q AS SELECT 1 AS zz WINDOW x AS (PARTITION BY yy)")
        .expect("bare name in WINDOW PARTITION BY must not block RENAME");
}

/// A bare name directly in a frame bound is accepted by sqlite3 3.54.0.
#[test]
fn bare_name_in_frame_bound_allows_rename() {
    rename_result(
        "CREATE VIEW q AS SELECT 1 WINDOW x AS (ROWS BETWEEN yy PRECEDING AND CURRENT ROW)",
    )
    .expect("bare name in frame bound must not block RENAME");
}

/// A bare name in a nested window function's PARTITION BY inside a frame
/// bound (outside any CTE) is accepted by sqlite3 3.54.0.
#[test]
fn bare_name_in_frame_bound_window_partition_allows_rename() {
    rename_result(
        "CREATE VIEW q AS SELECT 1 WINDOW x AS (ROWS BETWEEN UNBOUNDED PRECEDING AND \
         count(*) OVER (PARTITION BY yy) FOLLOWING)",
    )
    .expect("bare name in nested window PARTITION BY must not block RENAME");
}

/// A CTE with an unresolvable column directly in the window's own PARTITION
/// BY is accepted by sqlite3 3.54.0.
#[test]
fn cte_in_window_partition_by_allows_rename() {
    rename_result(
        "CREATE VIEW q AS SELECT 1 WINDOW x AS (PARTITION BY (WITH c AS (VALUES(yy)) VALUES(0)))",
    )
    .expect("CTE in WINDOW PARTITION BY must not block RENAME");
}

/// Same for the window's own ORDER BY.
#[test]
fn cte_in_window_order_by_allows_rename() {
    rename_result(
        "CREATE VIEW q AS SELECT 1 WINDOW x AS (ORDER BY (WITH c AS (VALUES(yy)) VALUES(0)))",
    )
    .expect("CTE in WINDOW ORDER BY must not block RENAME");
}

/// A CTE with an unresolvable column directly in a frame bound is rejected
/// by sqlite3 3.54.0 (`no such column: yy`).
#[test]
fn cte_in_frame_bound_rejects_rename() {
    let err = rename_result(
        "CREATE VIEW q AS SELECT 1 WINDOW x AS (ROWS BETWEEN \
         (WITH c AS (VALUES(yy)) VALUES(0)) PRECEDING AND CURRENT ROW)",
    )
    .unwrap_err();
    assert!(err.to_string().contains("error in view q: no such column: yy"), "{err}");
}

/// A SELECT-bodied CTE reached through a nested window function's ORDER BY
/// in a frame bound is rejected by sqlite3 3.54.0.
#[test]
fn select_cte_in_frame_bound_window_order_by_rejects_rename() {
    let err = rename_result(
        "CREATE VIEW q AS SELECT 1 WINDOW x AS (ROWS BETWEEN UNBOUNDED PRECEDING AND \
         count(*) OVER (ORDER BY (WITH c AS (SELECT yy) VALUES(0))) FOLLOWING)",
    )
    .unwrap_err();
    assert!(err.to_string().contains("error in view q: no such column: yy"), "{err}");
}
