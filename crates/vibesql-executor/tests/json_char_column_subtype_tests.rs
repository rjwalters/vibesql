//! Regression tests for issue #6007: the JSON "J" subtype marker used to tag
//! json_each / json_tree container `value` columns must NOT mis-fire on ordinary
//! fixed-width `CHAR(n)` columns.
//!
//! The subtype is carried on [`SqlValue::Character`] (see `node_value_column` in
//! `select/scan/table_function.rs`), which is also how a `CHAR(n)` column
//! materialises. A CHAR column holding container-shaped text (e.g. `'[1,2,3]'`)
//! must still QUOTE when fed to a JSON construction function, matching sqlite3
//! 3.51, while a genuine json_tree container `value` embeds as a sub-document.
//!
//! The fix gates the runtime marker on the argument expression: a bare read of a
//! column declared with a real string type (CHAR/VARCHAR) is not eligible. The
//! json_each / json_tree `value` column is declared dynamic (`DataType::Null`),
//! so it stays eligible.
//!
//! Differential expectations verified against sqlite3 3.51:
//!
//! | Query                                                  | sqlite3 / VibeSQL     |
//! |--------------------------------------------------------|-----------------------|
//! | `json_insert('{}','$.a',c)` over `CHAR(20)` `'[1,2,3]'`| `{"a":"[1,2,3]   ..."}`|
//! | `json_insert('{}','$.a',c)` over `CHAR(7)`  `'[1,2,3]'`| `{"a":"[1,2,3]"}`      |
//! | `json_insert('{}','$.a',c)` over `VARCHAR`  `'[1,2,3]'`| `{"a":"[1,2,3]"}`      |
//! | `json_insert('{}','$.a',value)` FROM json_tree(array)  | `{"a":[1,2,3]}`        |
//! | `subtype(c)` over a CHAR column                        | `0`                   |
//! | `subtype(value)` for a json_tree container            | `74`                  |

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
        other => panic!("Unsupported statement in test setup: {:?}", other),
    }
}

/// Run a single-column, single-row SELECT and return the scalar value.
fn scalar(db: &vibesql_storage::Database, sql: &str) -> SqlValue {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement: {}", sql);
    };
    let executor = SelectExecutor::new(db);
    let rows = executor
        .execute(&select_stmt)
        .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e));
    assert_eq!(rows.len(), 1, "expected exactly one row for: {}", sql);
    let values = rows.into_iter().next().unwrap().values;
    assert_eq!(values.len(), 1, "expected exactly one column for: {}", sql);
    values.into_iter().next().unwrap()
}

fn text(v: &SqlValue) -> String {
    match v {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
        other => panic!("expected text value, got {:?}", other),
    }
}

// --- CHAR(n): container-shaped text must QUOTE (never embed) -----------------

#[test]
fn json_insert_char20_column_quotes_container_text() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(c CHAR(20))");
    run_stmt(&mut db, "INSERT INTO t VALUES('[1,2,3]')");
    // CHAR(20) space-pads to width 20 (SQLite likewise); the value is a quoted
    // string, NOT an embedded array.
    let got = text(&scalar(&db, "SELECT json_insert('{}','$.a',c) FROM t"));
    assert_eq!(got, "{\"a\":\"[1,2,3]             \"}");
}

#[test]
fn json_insert_char7_snug_column_quotes_container_text() {
    // A snug CHAR(7) holding exactly '[1,2,3]' has no padding, so it is byte-for
    // byte identical to a TVF container marker — the discriminator must be the
    // column's declared type, not the value's content.
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(c CHAR(7))");
    run_stmt(&mut db, "INSERT INTO t VALUES('[1,2,3]')");
    let got = text(&scalar(&db, "SELECT json_insert('{}','$.a',c) FROM t"));
    assert_eq!(got, "{\"a\":\"[1,2,3]\"}");
}

#[test]
fn json_object_and_array_char_column_quote_container_text() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(c CHAR(7))");
    run_stmt(&mut db, "INSERT INTO t VALUES('[1,2,3]')");
    assert_eq!(text(&scalar(&db, "SELECT json_object('k', c) FROM t")), "{\"k\":\"[1,2,3]\"}");
    assert_eq!(text(&scalar(&db, "SELECT json_array(c) FROM t")), "[\"[1,2,3]\"]");
}

#[test]
fn subtype_of_char_column_is_zero() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(c CHAR(7))");
    run_stmt(&mut db, "INSERT INTO t VALUES('[1,2,3]')");
    assert_eq!(scalar(&db, "SELECT subtype(c) FROM t"), SqlValue::Integer(0));
}

// --- VARCHAR sibling: identical content also QUOTES -------------------------

#[test]
fn json_insert_varchar_column_quotes_container_text() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(c VARCHAR(20))");
    run_stmt(&mut db, "INSERT INTO t VALUES('[1,2,3]')");
    let got = text(&scalar(&db, "SELECT json_insert('{}','$.a',c) FROM t"));
    assert_eq!(got, "{\"a\":\"[1,2,3]\"}");
}

#[test]
fn subtype_of_varchar_column_is_zero() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t(c VARCHAR(20))");
    run_stmt(&mut db, "INSERT INTO t VALUES('[1,2,3]')");
    assert_eq!(scalar(&db, "SELECT subtype(c) FROM t"), SqlValue::Integer(0));
}

// --- json_tree container value must still EMBED (json101-5.10) --------------

#[test]
fn json_insert_json_tree_container_value_embeds() {
    let db = vibesql_storage::Database::new();
    // json101-5.10: the `value` column of a container node carries the J subtype
    // and embeds as a sub-document.
    let got = text(&scalar(
        &db,
        "SELECT json_insert('{}','$.a',value) FROM json_tree('[1,2,3]') WHERE atom IS NULL",
    ));
    assert_eq!(got, "{\"a\":[1,2,3]}");
}

#[test]
fn json_insert_json_tree_scalar_string_atom_quotes() {
    let db = vibesql_storage::Database::new();
    // json101-5.11: a JSON *string* atom is a scalar, not a container, so it
    // quotes.
    let got =
        text(&scalar(&db, "SELECT json_insert('{}','$.a',value) FROM json_tree('\"[1,2,3]\"')"));
    assert_eq!(got, "{\"a\":\"[1,2,3]\"}");
}

#[test]
fn subtype_of_json_tree_container_value_is_json() {
    let db = vibesql_storage::Database::new();
    assert_eq!(
        scalar(&db, "SELECT subtype(value) FROM json_tree('[1,2,3]') WHERE atom IS NULL"),
        SqlValue::Integer(74)
    );
}
