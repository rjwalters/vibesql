//! INSERT/UPDATE/DELETE on a view whose body references a since-dropped table
//! must report the missing table schema-qualified (`main.<name>`), matching
//! sqlite3 3.51.0 (trigger4-3.1/3.2/3.3).
//!
//! A view body resolves its tables against the database schema, so when one of
//! those tables is missing sqlite3 names it with the implicit `main.` prefix.
//! VibeSQL surfaces that during the INSTEAD OF view-DML path while building the
//! view pseudo-schema. Verified against sqlite3 3.51.0:
//!
//! ```text
//! sqlite> insert into test values(7,8,9);
//! Parse error: no such table: main.test2
//! ```

use vibesql_executor::{
    CreateTableExecutor, DeleteExecutor, InsertExecutor, TriggerExecutor, UpdateExecutor,
    ViewExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Parse and execute a single DDL/DML statement, panicking on failure.
fn exec_ok(db: &mut Database, sql: &str) {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateView(s) => {
            ViewExecutor::execute_create_view(&s, db).expect("CREATE VIEW failed");
        }
        Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
                .expect("CREATE TRIGGER failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        Statement::DropTable(s) => {
            vibesql_executor::DropTableExecutor::execute(&s, db).expect("DROP TABLE failed");
        }
        other => panic!("unsupported statement in exec_ok: {other:?}"),
    }
}

/// Build the trigger4 fixture and drop `test2` so the view body is now dangling.
fn setup_dangling_view(db: &mut Database) {
    exec_ok(db, "create table test1(id integer primary key, a)");
    exec_ok(db, "create table test2(id integer, b)");
    exec_ok(
        db,
        "create view test as select test1.id as id, a as a, b as b \
         from test1 join test2 on test2.id = test1.id",
    );
    exec_ok(
        db,
        "create trigger I_test instead of insert on test begin \
           insert into test1 (id,a) values (NEW.id,NEW.a); \
           insert into test2 (id,b) values (NEW.id,NEW.b); \
         end",
    );
    exec_ok(
        db,
        "create trigger U_test instead of update on test begin \
           update test1 set a=NEW.a where id=NEW.id; \
           update test2 set b=NEW.b where id=NEW.id; \
         end",
    );
    exec_ok(
        db,
        "create trigger D_test instead of delete on test begin \
           delete from test1 where id=OLD.id; \
           delete from test2 where id=OLD.id; \
         end",
    );
    exec_ok(db, "drop table test2");
}

/// The error string the shim translates to `no such table: main.test2`.
fn assert_main_test2(err: vibesql_executor::ExecutorError) {
    assert_eq!(
        err,
        vibesql_executor::ExecutorError::TableNotFound("main.test2".to_string()),
        "expected the missing view-body table to be reported as main.test2"
    );
}

#[test]
fn insert_on_view_with_dropped_body_table_qualifies_main() {
    // trigger4-3.1 / 3.2
    let mut db = Database::new();
    setup_dangling_view(&mut db);

    let stmt = Parser::parse_sql("insert into test values(7,8,9)").unwrap();
    let vibesql_ast::Statement::Insert(insert) = stmt else { panic!("expected INSERT") };
    let err = InsertExecutor::execute(&mut db, &insert).unwrap_err();
    assert_main_test2(err);
}

#[test]
fn update_on_view_with_dropped_body_table_qualifies_main() {
    // trigger4-3.3
    let mut db = Database::new();
    setup_dangling_view(&mut db);

    let stmt = Parser::parse_sql("update test set a=222 where id=1").unwrap();
    let vibesql_ast::Statement::Update(update) = stmt else { panic!("expected UPDATE") };
    let err = UpdateExecutor::execute(&update, &mut db).unwrap_err();
    assert_main_test2(err);
}

#[test]
fn delete_on_view_with_dropped_body_table_qualifies_main() {
    let mut db = Database::new();
    setup_dangling_view(&mut db);

    let stmt = Parser::parse_sql("delete from test where id=1").unwrap();
    let vibesql_ast::Statement::Delete(delete) = stmt else { panic!("expected DELETE") };
    let err = DeleteExecutor::execute(&delete, &mut db).unwrap_err();
    assert_main_test2(err);
}
