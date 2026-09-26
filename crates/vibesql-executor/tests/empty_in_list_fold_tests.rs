//! End-to-end tests for issue #6733: `<expr> [NOT] IN ()` is folded to a
//! constant at parse time like SQLite, so a function-free left operand is
//! never resolved (query time), never renamed by `ALTER TABLE ... RENAME`, and
//! never re-validated. An operand holding a function call is kept, resolved,
//! and renamed as usual. Expected results come from sqlite3 3.54.0.

use vibesql_ast::Statement;
use vibesql_executor::{AlterTableExecutor, CreateTableExecutor, SelectExecutor, ViewExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn exec(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(create) => {
            CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
        }
        Statement::CreateView(mut create) => {
            // Stamp the verbatim text like the CLI does, so `sqlite_master.sql`
            // (and the RENAME rewrite) sees the user's spelling.
            create.sql_definition = Some(sql.to_string());
            ViewExecutor::execute_create_view(&create, db).expect("CREATE VIEW");
        }
        Statement::AlterTable(alter) => {
            AlterTableExecutor::execute_with_source(&alter, db, Some(sql)).expect("ALTER TABLE");
        }
        other => panic!("unsupported statement in test: {other:?}"),
    }
}

fn try_query(db: &Database, sql: &str) -> Result<Vec<Vec<SqlValue>>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| e.to_string())?;
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result =
        SelectExecutor::new(db).execute_with_columns(&select).map_err(|e| e.to_string())?;
    Ok(result.rows.into_iter().map(|r| r.values.to_vec()).collect())
}

fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    try_query(db, sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn object_sql(db: &Database, name: &str) -> String {
    let rows = query(db, &format!("SELECT sql FROM sqlite_master WHERE name='{name}'"));
    assert_eq!(rows.len(), 1, "expected one sqlite_master row for {name}");
    match &rows[0][0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
        other => panic!("expected text, got {other:?}"),
    }
}

fn is_false(v: &SqlValue) -> bool {
    matches!(v, SqlValue::Boolean(false) | SqlValue::Integer(0) | SqlValue::Bigint(0))
}

fn t1_with_row() -> Database {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b)");
    let stmt = Parser::parse_sql("INSERT INTO t1 VALUES(1, 2)").unwrap();
    let Statement::Insert(insert) = stmt else { unreachable!() };
    vibesql_executor::InsertExecutor::execute(&mut db, &insert).expect("INSERT");
    db
}

#[test]
fn discarded_operand_is_not_resolved_in_select_list() {
    let db = t1_with_row();
    let rows = query(&db, "SELECT nosuch IN () FROM t1");
    assert_eq!(rows.len(), 1);
    assert!(is_false(&rows[0][0]), "got {:?}", rows[0][0]);
}

#[test]
fn folded_select_item_keeps_its_source_text_as_column_name() {
    // sqlite3: `SELECT 5 IN (), 5 NOT IN (), TRUE` has headers
    // `5 IN ()|5 NOT IN ()|TRUE`, not the folded literal's value. The
    // select-list source text here is the parser's token-joined
    // reconstruction (the same one every non-literal expression gets).
    let db = t1_with_row();
    let Statement::Select(select) =
        Parser::parse_sql("SELECT 5 IN (), 5 NOT IN (), TRUE FROM t1").unwrap()
    else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(&db).execute_with_columns(&select).unwrap();
    assert_eq!(result.columns, vec!["5IN()", "5NOTIN()", "TRUE"]);
}

#[test]
fn discarded_operand_is_not_resolved_in_where_or() {
    let db = t1_with_row();
    let rows = query(&db, "SELECT * FROM t1 WHERE a=1 OR (nosuch IN ())");
    assert_eq!(rows, vec![vec![SqlValue::Integer(1), SqlValue::Integer(2)]]);
}

#[test]
fn discarded_operand_in_where_returns_no_rows() {
    let db = t1_with_row();
    assert!(query(&db, "SELECT * FROM t1 WHERE nosuch IN ()").is_empty());
    assert_eq!(query(&db, "SELECT * FROM t1 WHERE nosuch NOT IN ()").len(), 1);
}

#[test]
fn function_operand_is_still_resolved() {
    let db = t1_with_row();
    let err = try_query(&db, "SELECT abs(nosuch) IN () FROM t1")
        .expect_err("an operand holding a function call is kept and resolved");
    assert!(err.to_lowercase().contains("nosuch"), "unexpected error: {err}");
}

#[test]
fn rename_column_leaves_discarded_operand_alone() {
    // altertab3.test 3.0-3.2
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b, c, d)");
    exec(&mut db, "CREATE VIEW v1 AS SELECT * FROM t1 WHERE a=1 OR (b IN ())");
    exec(&mut db, "ALTER TABLE t1 RENAME b TO bbb");
    assert_eq!(object_sql(&db, "v1"), "CREATE VIEW v1 AS SELECT * FROM t1 WHERE a=1 OR (b IN ())");
    // The view still works: the kept `b` is never resolved.
    assert!(query(&db, "SELECT * FROM v1").is_empty());
}

#[test]
fn rename_column_still_renames_function_operand() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t2(c0)");
    exec(&mut db, "CREATE VIEW v2 AS SELECT * FROM t2 WHERE likelihood(c0, 1.0) IN ()");
    exec(&mut db, "ALTER TABLE t2 RENAME COLUMN c0 TO c1");
    assert_eq!(
        object_sql(&db, "v2"),
        "CREATE VIEW v2 AS SELECT * FROM t2 WHERE likelihood(c1, 1.0) IN ()"
    );
}

#[test]
fn rename_table_leaves_discarded_subquery_operand_alone() {
    // altertab3.test 10.1-10.2
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b, c)");
    exec(&mut db, "CREATE TABLE t2(a, b, c)");
    let view = "CREATE VIEW v1 AS SELECT * FROM t1 WHERE (\n    SELECT t1.a FROM t1, t2\n  ) IN () OR t1.a=5";
    exec(&mut db, view);
    exec(&mut db, "ALTER TABLE t2 RENAME TO t3");
    assert_eq!(object_sql(&db, "v1"), view);
    assert!(query(&db, "SELECT * FROM v1").is_empty());
}

#[test]
fn view_whose_discarded_operand_names_missing_column_survives_alter() {
    // SQLite never resolves the discarded operand, so the view is valid and a
    // later ALTER's schema re-validation does not trip over it.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b)");
    exec(&mut db, "CREATE VIEW v1 AS SELECT a FROM t1 WHERE nosuch IN () OR a > 0");
    exec(&mut db, "ALTER TABLE t1 RENAME b TO bb");
    exec(&mut db, "ALTER TABLE t1 RENAME TO t9");
    assert!(query(&db, "SELECT * FROM v1").is_empty());
}
