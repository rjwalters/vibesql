//! ALTER TABLE ... RENAME re-resolves every view. A FROM-less view whose named
//! `WINDOW` definition (partition / order / frame bound, including CTE bodies
//! of scalar subqueries nested there) mentions a bare column has nothing to
//! resolve it against, so SQLite aborts with `error in view <v>: no such
//! column: <c>` (altertab3.test 19.1.2 / 19.3.2).

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
