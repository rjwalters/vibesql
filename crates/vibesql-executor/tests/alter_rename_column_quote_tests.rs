//! `ALTER TABLE ... RENAME COLUMN` replacement quoting and `sqlite_master.sql`
//! header canonicalization, matching sqlite3 (verified against 3.54.0).
//! Part of #6174 (the non-DQS half of alterqf.test 2.1's expected output).
//!
//! - SQLite's `bQuote` rule: when the new name is written as a *quoted* token in the `ALTER`
//!   statement (`'x'`, `"x"`, `[x]`, `` `x` ``), every rewritten reference is emitted double-quoted
//!   — in the table text, its indexes, triggers and views — even where the replaced token was bare.
//! - A replaced token that was itself quoted re-emits the new name double-quoted inside trigger and
//!   view bodies too (`new."b"` -> `new."d"`), as it already did in table/index text.
//! - `sqlite_master.sql` / `sqlite_temp_master.sql` rebuild the header of a table / view / trigger
//!   as `CREATE TABLE|VIEW|TRIGGER ` + the original text from the unqualified object name: no
//!   `TEMP`, no `IF NOT EXISTS`, no schema qualifier, uppercase keywords.

use vibesql_executor::{
    AlterTableExecutor, CreateIndexExecutor, CreateTableExecutor, SelectExecutor, TriggerExecutor,
    ViewExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute one statement, recording its verbatim text the way the CLI does.
fn exec(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse");
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute_with_source(&s, db, Some(sql)).expect("CREATE TABLE");
        }
        vibesql_ast::Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute_with_source(&s, db, Some(sql)).expect("CREATE INDEX");
        }
        vibesql_ast::Statement::CreateView(mut s) => {
            s.sql_definition = Some(sql.to_string());
            ViewExecutor::execute_create_view(&s, db).expect("CREATE VIEW");
        }
        vibesql_ast::Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql)).expect("CREATE TRIGGER");
        }
        vibesql_ast::Statement::AlterTable(s) => {
            AlterTableExecutor::execute_with_source(&s, db, Some(sql)).expect("ALTER TABLE");
        }
        other => panic!("unsupported statement in test: {other:?}"),
    }
}

/// `SELECT sql FROM <schema_table>` (creation order), as plain strings.
fn schema_sql(db: &Database, schema_table: &str) -> Vec<String> {
    let sql = format!("SELECT sql FROM {schema_table}");
    let vibesql_ast::Statement::Select(select) = Parser::parse_sql(&sql).expect("parse") else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    result
        .rows
        .into_iter()
        .filter_map(|r| match &r.values[0] {
            SqlValue::Varchar(s) | SqlValue::Character(s) => Some(s.to_string()),
            _ => None,
        })
        .collect()
}

#[test]
fn quoted_new_name_quotes_every_rewritten_reference() {
    // alterqf.test 2.0/2.1 without the DQS (double-quoted string) parts.
    let mut db = Database::new();
    exec(
        &mut db,
        "CREATE TABLE x1(\n    one, two, three, PRIMARY KEY(one),\n    CHECK (three!='xyz'), CHECK (two!=\"one\")\n) WITHOUT ROWID",
    );
    exec(&mut db, "CREATE INDEX x1i ON x1(one+\"two\"+'four') WHERE 'five'");
    exec(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON x1 BEGIN\n  UPDATE x1 SET two=new.three || 'new' WHERE one=new.one||'';\nEND",
    );
    exec(&mut db, "CREATE VIEW v AS SELECT two, x1.two FROM x1");

    exec(&mut db, "ALTER TABLE x1 RENAME two TO 'four'");

    assert_eq!(
        schema_sql(&db, "sqlite_schema"),
        vec![
            "CREATE TABLE x1(\n    one, \"four\", three, PRIMARY KEY(one),\n    CHECK (three!='xyz'), CHECK (\"four\"!=\"one\")\n) WITHOUT ROWID".to_string(),
            "CREATE INDEX x1i ON x1(one+\"four\"+'four') WHERE 'five'".to_string(),
            "CREATE TRIGGER tr AFTER INSERT ON x1 BEGIN\n  UPDATE x1 SET \"four\"=new.three || 'new' WHERE one=new.one||'';\nEND".to_string(),
            "CREATE VIEW v AS SELECT \"four\", x1.\"four\" FROM x1".to_string(),
        ]
    );
}

#[test]
fn every_quote_style_counts_as_a_quoted_new_name() {
    // sqlite3: `RENAME a TO "zz"` / `[w]` / `` `c d` `` all re-emit "...".
    for (new_token, expected) in [
        ("\"zz\"", "CREATE TABLE m(\"zz\", CHECK(\"zz\">0))"),
        ("[zz]", "CREATE TABLE m(\"zz\", CHECK(\"zz\">0))"),
        ("`zz`", "CREATE TABLE m(\"zz\", CHECK(\"zz\">0))"),
        ("zz", "CREATE TABLE m(zz, CHECK(zz>0))"),
    ] {
        let mut db = Database::new();
        exec(&mut db, "CREATE TABLE m(a, CHECK(a>0))");
        exec(&mut db, &format!("ALTER TABLE m RENAME a TO {new_token}"));
        assert_eq!(schema_sql(&db, "sqlite_schema"), vec![expected.to_string()], "{new_token}");
    }
}

#[test]
fn quoted_replaced_token_stays_quoted_in_trigger_and_view_bodies() {
    // sqlite3: a quoted `"b"` / `[b]` reference renamed to bare `d` becomes `"d"`
    // in trigger and view bodies, exactly as in the table and index text.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a,b)");
    exec(&mut db, "CREATE TABLE log(x)");
    exec(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.\"b\" + new.b); END",
    );
    exec(&mut db, "CREATE VIEW v AS SELECT \"b\", [b], b FROM t");
    exec(&mut db, "CREATE INDEX i ON t(\"b\", b+1)");

    exec(&mut db, "ALTER TABLE t RENAME b TO d");

    assert_eq!(
        schema_sql(&db, "sqlite_schema"),
        vec![
            "CREATE TABLE t(a,d)".to_string(),
            "CREATE TABLE log(x)".to_string(),
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO log VALUES(new.\"d\" + new.d); END"
                .to_string(),
            "CREATE VIEW v AS SELECT \"d\", \"d\", d FROM t".to_string(),
            "CREATE INDEX i ON t(\"d\", d+1)".to_string(),
        ]
    );
}

#[test]
fn schema_sql_header_is_canonicalized_like_sqlite() {
    let mut db = Database::new();
    exec(&mut db, "create   table m(a,b)");
    exec(&mut db, "CREATE TEMP TRIGGER IF NOT EXISTS tr2 AFTER INSERT ON m BEGIN SELECT 1; END");
    exec(&mut db, "CREATE TEMPORARY VIEW IF NOT EXISTS v AS SELECT 1");
    exec(&mut db, "CREATE TRIGGER IF NOT EXISTS main.tr1 AFTER INSERT ON m BEGIN SELECT 1; END");
    exec(&mut db, "CREATE VIEW IF NOT EXISTS main.\"vv\" AS SELECT 2");

    assert_eq!(
        schema_sql(&db, "sqlite_temp_schema"),
        vec![
            "CREATE VIEW v AS SELECT 1".to_string(),
            "CREATE TRIGGER tr2 AFTER INSERT ON m BEGIN SELECT 1; END".to_string(),
        ]
    );
    assert_eq!(
        schema_sql(&db, "sqlite_schema"),
        vec![
            "CREATE TABLE m(a,b)".to_string(),
            "CREATE TRIGGER tr1 AFTER INSERT ON m BEGIN SELECT 1; END".to_string(),
            "CREATE VIEW \"vv\" AS SELECT 2".to_string(),
        ]
    );
}

#[test]
fn temp_trigger_named_like_a_keyword_keeps_its_name() {
    // alterqf.test 2.0: `CREATE TEMP TRIGGER AFTER INSERT ON x1 ...` names the
    // trigger `AFTER`; SQLite stores `CREATE TRIGGER AFTER INSERT ON x1 ...`.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE x1(one, two)");
    exec(
        &mut db,
        "CREATE TEMP TRIGGER AFTER INSERT ON x1 BEGIN\n  UPDATE x1 SET two=new.two WHERE one=new.one;\nEND",
    );
    exec(&mut db, "ALTER TABLE x1 RENAME two TO 'four'");
    assert_eq!(
        schema_sql(&db, "sqlite_temp_schema"),
        vec![
            "CREATE TRIGGER AFTER INSERT ON x1 BEGIN\n  UPDATE x1 SET \"four\"=new.\"four\" WHERE one=new.one;\nEND"
                .to_string()
        ]
    );
}
