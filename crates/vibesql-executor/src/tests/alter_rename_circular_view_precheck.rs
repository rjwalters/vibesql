//! `ALTER TABLE ... RENAME TO` aborts when a view in the schema is circularly
//! defined, matching SQLite's schema re-parse (`altertab3.test` 22.2/22.4/
//! 23.3, Part of #6174): `error in view <name>: view <name> is circularly
//! defined`. The cycle may be direct (`CREATE VIEW v AS SELECT * FROM v`) or
//! hidden behind a CTE body, and a CTE that merely *shadows* the view's own
//! name is not a cycle.

use vibesql_ast::Statement;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

fn exec(db: &mut Database, sql: &str) -> Result<String, ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute_with_source(&s, db, Some(sql))
        }
        Statement::CreateView(mut s) => {
            s.sql_definition = Some(sql.to_string());
            crate::ViewExecutor::execute_create_view(&s, db)
        }
        Statement::AlterTable(s) => {
            crate::alter::AlterTableExecutor::execute_with_source(&s, db, Some(sql))
        }
        other => panic!("unexpected statement: {:?}", other),
    }
}

#[test]
fn rename_table_rejects_direct_self_referencing_view() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)").unwrap();
    // Lax view creation: the body is not validated until first use.
    let _ = exec(&mut db, "CREATE VIEW v2(b) AS WITH x AS (SELECT * FROM v2) SELECT * FROM v2");
    let err = exec(&mut db, "ALTER TABLE t1 RENAME TO t4").unwrap_err();
    assert_eq!(err.to_string(), "error in view v2: view v2 is circularly defined");
    // Atomic: the table keeps its old name.
    assert!(db.get_table("t1").is_some());
    assert!(db.get_table("t4").is_none());
}

#[test]
fn rename_table_rejects_cycle_hidden_in_referenced_cte_body() {
    // altertab3.test 22.4: the CTE is used, so its body's `FROM v2` is a cycle.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)").unwrap();
    exec(&mut db, "CREATE VIEW v2(b) AS WITH t3 AS (SELECT b FROM v2) SELECT * FROM t3").unwrap();
    let err = exec(&mut db, "ALTER TABLE t1 RENAME TO t4").unwrap_err();
    assert_eq!(err.to_string(), "error in view v2: view v2 is circularly defined");
}

#[test]
fn unused_cte_body_referencing_the_view_is_not_a_cycle() {
    // altertab3.test 22.6: `t3` is never selected from, so SQLite never
    // expands its body and the RENAME succeeds.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)").unwrap();
    exec(&mut db, "CREATE VIEW v2(b) AS WITH t3 AS (SELECT b FROM v2) VALUES(1)").unwrap();
    exec(&mut db, "ALTER TABLE t1 RENAME TO t4").unwrap();
    assert!(db.get_table("t4").is_some());
}

#[test]
fn acyclic_view_chain_is_not_a_cycle() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)").unwrap();
    exec(&mut db, "CREATE VIEW va AS SELECT a FROM t1").unwrap();
    exec(&mut db, "CREATE VIEW vb AS SELECT a FROM va").unwrap();
    exec(&mut db, "ALTER TABLE t1 RENAME TO t2").unwrap();
}

#[test]
fn cte_shadowing_the_view_name_is_not_a_cycle() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a)").unwrap();
    exec(&mut db, "CREATE VIEW v9 AS WITH v9 AS (SELECT 1 AS x) SELECT x FROM v9").unwrap();
    exec(&mut db, "ALTER TABLE t1 RENAME TO t5").unwrap();
    assert!(db.get_table("t5").is_some());
}
