//! End-to-end regression tests for issue #5882: autoindex ordinal numbering on
//! WITHOUT ROWID tables must match SQLite.
//!
//! In SQLite the PRIMARY KEY of a WITHOUT ROWID table *is* the table B-tree and
//! gets no `sqlite_autoindex_*` entry, so a `UNIQUE` constraint on the same
//! table is the first real index and is named `sqlite_autoindex_<t>_1`:
//!
//! ```sql
//! CREATE TABLE t(a, b, UNIQUE(b), PRIMARY KEY(a)) WITHOUT ROWID;
//! SELECT type, name FROM sqlite_master;
//! -- sqlite3 3.51.0: table|t , index|sqlite_autoindex_t_1
//! ```
//!
//! Before the fix, VibeSQL materialized the WITHOUT ROWID PK as
//! `sqlite_autoindex_t_1` (later hidden from sqlite_master by #5879) but still
//! burned the `_1` ordinal, so the UNIQUE index landed on `_2`. The fix names
//! the internal PK index outside the autoindex namespace and does not consume
//! an ordinal.

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Create a table, preserving verbatim source text so the binary reload path
/// can re-derive the schema (and its implicit indexes) by re-parsing the DDL.
fn create(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE, got: {sql}");
    };
    CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
}

fn insert(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse INSERT");
    let Statement::Insert(ins) = stmt else {
        panic!("expected INSERT, got: {sql}");
    };
    InsertExecutor::execute(db, &ins).expect("INSERT");
}

/// Index names reported by `sqlite_master`, in listing order.
fn index_names(db: &Database) -> Vec<String> {
    let stmt =
        Parser::parse_sql("SELECT name FROM sqlite_master WHERE type='index'").expect("parse");
    let Statement::Select(select) = stmt else { panic!("expected SELECT") };
    SelectExecutor::new(db)
        .execute(&select)
        .expect("sqlite_master query")
        .into_iter()
        .map(|row| match &row.values[0] {
            SqlValue::Varchar(s) => s.to_string(),
            other => panic!("expected VARCHAR index name, got {other:?}"),
        })
        .collect()
}

/// Rows of a SELECT as raw value vectors.
fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = Parser::parse_sql(sql).expect("parse SELECT");
    let Statement::Select(select) = stmt else { panic!("expected SELECT") };
    SelectExecutor::new(db)
        .execute(&select)
        .unwrap_or_else(|e| panic!("query failed: {sql} -- {e:?}"))
        .into_iter()
        .map(|row| row.values.to_vec())
        .collect()
}

/// Save to a binary `.vbsql` and reload — the cross-process reopen path.
fn reopen_binary(db: &Database, tag: &str) -> Database {
    let path =
        std::env::temp_dir().join(format!("vibesql_5882_{tag}_{}.vbsql", std::process::id()));
    db.save_binary(&path).expect("save_binary");
    let reloaded = Database::load_binary(&path).expect("load_binary");
    std::fs::remove_file(&path).ok();
    reloaded
}

/// The exact reproducer from the issue: the UNIQUE index must be `_1`, not `_2`.
#[test]
fn without_rowid_pk_does_not_consume_autoindex_ordinal() {
    let mut db = Database::new();
    create(&mut db, "CREATE TABLE t(a, b, UNIQUE(b), PRIMARY KEY(a)) WITHOUT ROWID");

    let names = index_names(&db);
    assert!(
        names.contains(&"sqlite_autoindex_t_1".to_string()),
        "UNIQUE index must be sqlite_autoindex_t_1, got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "sqlite_autoindex_t_2"),
        "no _2 ordinal should exist (PK must not burn _1), got {names:?}"
    );
    // Exactly one visible index (the UNIQUE); the internal PK index is hidden.
    assert_eq!(names.len(), 1, "expected only the UNIQUE index visible, got {names:?}");
}

/// A WITHOUT ROWID table with two UNIQUE constraints numbers them `_1`, `_2`.
#[test]
fn without_rowid_multiple_unique_number_from_one() {
    let mut db = Database::new();
    create(&mut db, "CREATE TABLE t2(a, b, c, UNIQUE(b), UNIQUE(c), PRIMARY KEY(a)) WITHOUT ROWID");

    let names = index_names(&db);
    assert!(
        names.contains(&"sqlite_autoindex_t2_1".to_string()),
        "first UNIQUE must be _1, got {names:?}"
    );
    assert!(
        names.contains(&"sqlite_autoindex_t2_2".to_string()),
        "second UNIQUE must be _2, got {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "sqlite_autoindex_t2_3"),
        "no _3 ordinal should exist, got {names:?}"
    );
    assert_eq!(names.len(), 2, "expected exactly two visible UNIQUE indexes, got {names:?}");
}

/// A WITHOUT ROWID table with only a PK produces no `sqlite_autoindex_*` at all.
#[test]
fn without_rowid_pk_only_produces_no_visible_autoindex() {
    let mut db = Database::new();
    create(&mut db, "CREATE TABLE t3(a, b, PRIMARY KEY(a)) WITHOUT ROWID");
    assert!(index_names(&db).is_empty(), "PK-only WITHOUT ROWID table must list no index");
}

/// Regression guard: an ordinary rowid table with a non-alias (TEXT) PK plus a
/// UNIQUE still names them `_1` (PK) and `_2` (UNIQUE).
#[test]
fn rowid_text_pk_plus_unique_numbering_unchanged() {
    let mut db = Database::new();
    create(&mut db, "CREATE TABLE r(a TEXT PRIMARY KEY, b, UNIQUE(b))");

    let names = index_names(&db);
    assert!(
        names.contains(&"sqlite_autoindex_r_1".to_string()),
        "TEXT PK autoindex must be _1, got {names:?}"
    );
    assert!(
        names.contains(&"sqlite_autoindex_r_2".to_string()),
        "UNIQUE autoindex must be _2, got {names:?}"
    );
}

/// Regression guard: an INTEGER PRIMARY KEY (rowid alias) produces no autoindex.
#[test]
fn integer_pk_produces_no_autoindex() {
    let mut db = Database::new();
    create(&mut db, "CREATE TABLE ipk(a INTEGER PRIMARY KEY, b)");
    assert!(index_names(&db).is_empty(), "INTEGER PRIMARY KEY must produce no autoindex");
}

/// The PK-prefix fast path (`WHERE a=? ORDER BY b DESC LIMIT 1`) must still
/// return correct results on a WITHOUT ROWID table whose multi-column PK index
/// was renamed out of the autoindex namespace (issue #5882 sneaky regression).
#[test]
fn without_rowid_pk_prefix_lookup_still_correct() {
    let mut db = Database::new();
    create(&mut db, "CREATE TABLE wr(a, b, v, PRIMARY KEY(a, b)) WITHOUT ROWID");
    insert(&mut db, "INSERT INTO wr VALUES (1, 10, 'x'), (1, 20, 'y'), (1, 30, 'z'), (2, 5, 'q')");

    // DESC LIMIT 1 -> largest b for a=1 is 30 ('z'); this drives the fast path.
    let desc = query(&db, "SELECT v FROM wr WHERE a = 1 ORDER BY b DESC LIMIT 1");
    assert_eq!(desc, vec![vec![SqlValue::Varchar("z".into())]], "DESC prefix lookup wrong");

    // ASC LIMIT 1 -> smallest b for a=1 is 10 ('x').
    let asc = query(&db, "SELECT v FROM wr WHERE a = 1 ORDER BY b ASC LIMIT 1");
    assert_eq!(asc, vec![vec![SqlValue::Varchar("x".into())]], "ASC prefix lookup wrong");

    // Full equality on the whole PK still resolves the right row.
    let both = query(&db, "SELECT v FROM wr WHERE a = 2 AND b = 5");
    assert_eq!(both, vec![vec![SqlValue::Varchar("q".into())]], "full PK lookup wrong");
}

/// Numbering, hiding, and PK lookups all survive a binary save/reload, since
/// the internal PK index is regenerated from the persisted DDL.
#[test]
fn without_rowid_numbering_survives_reload() {
    let mut db = Database::new();
    create(&mut db, "CREATE TABLE t(a, b, v, UNIQUE(b), PRIMARY KEY(a)) WITHOUT ROWID");
    insert(&mut db, "INSERT INTO t VALUES (1, 100, 'one'), (2, 200, 'two')");

    let before = index_names(&db);
    assert_eq!(before, vec!["sqlite_autoindex_t_1".to_string()], "pre-reload names: {before:?}");

    let reloaded = reopen_binary(&db, "numbering");

    let after = index_names(&reloaded);
    assert_eq!(
        after,
        vec!["sqlite_autoindex_t_1".to_string()],
        "UNIQUE index must still be _1 after reload, got {after:?}"
    );

    // Data + PK/UNIQUE lookups still work after reload.
    let rows = query(&reloaded, "SELECT v FROM t WHERE a = 2");
    assert_eq!(rows, vec![vec![SqlValue::Varchar("two".into())]], "PK lookup broke after reload");

    // The UNIQUE constraint is still enforced after reload.
    let dup = Parser::parse_sql("INSERT INTO t VALUES (3, 100, 'dup')").expect("parse");
    let Statement::Insert(ins) = dup else { panic!("expected INSERT") };
    let mut reloaded = reloaded;
    assert!(
        InsertExecutor::execute(&mut reloaded, &ins).is_err(),
        "UNIQUE(b) must still reject duplicate after reload"
    );
}
