//! Regression tests for composite foreign-key child-key column ordering.
//!
//! A composite `FOREIGN KEY(f, d) REFERENCES pp(b, c)` maps the child key
//! positionally onto the parent key: `f -> b`, `d -> c`. Historically the
//! INSERT validator extracted the child-key values in ascending *table-column*
//! order rather than the FK's *declared* order, so when the FK column list did
//! not match the table's column declaration order (e.g. `d` declared before
//! `f` but listed as `FOREIGN KEY(f, d)`) the child key was assembled as
//! `[d, f]` and mis-aligned against the parent `(b, c)` columns. That caused a
//! spurious "FOREIGN KEY constraint failed" on an INSERT whose parent row
//! actually existed (SQLite fkey2-9.2.*).
//!
//! See issue #6170.

use vibesql_executor::{CreateTableExecutor, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn new_db() -> Database {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    db
}

fn exec_create(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE");
    match stmt {
        vibesql_ast::Statement::CreateTable(c) => {
            CreateTableExecutor::execute(&c, db).expect("CREATE TABLE");
        }
        _ => panic!("expected CREATE TABLE"),
    }
}

fn try_insert(db: &mut Database, sql: &str) -> Result<(), String> {
    let stmt = Parser::parse_sql(sql).expect("parse INSERT");
    match stmt {
        vibesql_ast::Statement::Insert(i) => {
            InsertExecutor::execute(db, &i).map(|_| ()).map_err(|e| format!("{e:?}"))
        }
        _ => panic!("expected INSERT"),
    }
}

/// The child key is `FOREIGN KEY(f, d)` where `d` is declared before `f` in the
/// child table. Positional mapping is `f -> b`, `d -> c`. An INSERT whose
/// `(f, d)` values match an existing parent `(b, c)` row must succeed.
#[test]
fn composite_fk_child_columns_out_of_declaration_order_insert_succeeds() {
    let mut db = new_db();
    exec_create(&mut db, "CREATE TABLE pp(a, b, c, PRIMARY KEY(b, c))");
    exec_create(&mut db, "CREATE TABLE cc(d, e, f, FOREIGN KEY(f, d) REFERENCES pp(b, c))");
    // Parent row: b = 5, c = 6.
    try_insert(&mut db, "INSERT INTO pp VALUES(4, 5, 6)").expect("parent insert");

    // Child row: d = 6, f = 5 => FK (f, d) = (5, 6) => parent (b = 5, c = 6). Match.
    try_insert(&mut db, "INSERT INTO cc VALUES(6, 'A', 5)")
        .expect("child insert with matching parent must succeed");
}

/// The mirror case: with the same schema, an INSERT whose `(f, d)` values do
/// NOT correspond to any parent `(b, c)` row must still be rejected. This
/// guards against a fix that simply ignores column order (which would make the
/// wrong-order values spuriously match).
#[test]
fn composite_fk_child_columns_out_of_declaration_order_missing_parent_rejected() {
    let mut db = new_db();
    exec_create(&mut db, "CREATE TABLE pp(a, b, c, PRIMARY KEY(b, c))");
    exec_create(&mut db, "CREATE TABLE cc(d, e, f, FOREIGN KEY(f, d) REFERENCES pp(b, c))");
    // Parent row: b = 6, c = 5 (the transposed values).
    try_insert(&mut db, "INSERT INTO pp VALUES(4, 6, 5)").expect("parent insert");

    // Child row: d = 6, f = 5 => FK (f, d) = (5, 6) => needs parent (b = 5, c = 6),
    // which does NOT exist (parent has b = 6, c = 5). Must be rejected.
    let err = try_insert(&mut db, "INSERT INTO cc VALUES(6, 'A', 5)")
        .expect_err("child insert without matching parent must fail");
    assert!(
        err.contains("FOREIGN KEY") || err.to_ascii_lowercase().contains("foreign key"),
        "expected a FOREIGN KEY violation, got: {err}"
    );
}
