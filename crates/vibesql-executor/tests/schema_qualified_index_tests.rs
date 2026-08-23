//! Integration tests for schema-qualified `CREATE INDEX` / `DROP INDEX`
//! (issue #6366).
//!
//! SQLite's CREATE/DROP INDEX grammar is `[schema-name .] index-name` — the
//! schema qualifies the *index* name, not the target table. When a schema
//! qualifier is present, the target table is resolved within that exact
//! schema (no temp-shadows-main search), and an unrecognized qualifier is
//! rejected with `unknown database <name>`. Verified against sqlite3 3.51.0:
//!
//! ```sql
//! CREATE TABLE t(x INTEGER);
//! CREATE INDEX main.i1 ON t(x);   -- ok
//! CREATE INDEX bogus.i1 ON t(x);  -- "unknown database bogus"
//! DROP INDEX main.i1;             -- ok
//! DROP INDEX bogus.i1;            -- "no such index: bogus.i1"
//! ```

use std::collections::BTreeMap;

use vibesql_executor::{
    CreateIndexExecutor, CreateTableExecutor, DropIndexExecutor, InsertExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::{database::indexes::IndexData, Database};
use vibesql_types::SqlValue;

fn exec_create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TABLE");
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE")
        }
        other => panic!("expected CREATE TABLE, got {other:?}"),
    };
}

fn exec_insert(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse INSERT");
    match stmt {
        vibesql_ast::Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        other => panic!("expected INSERT, got {other:?}"),
    };
}

/// Collect the contents of an index body as a sorted (key, row_indices) map,
/// bypassing the planner to inspect the storage layer's index body directly
/// (matches the approach in `partial_index_tests.rs`).
fn index_body(db: &Database, index_name: &str) -> BTreeMap<Vec<SqlValue>, Vec<usize>> {
    let data = db.get_index_data(index_name).expect("index not found");
    match data {
        IndexData::InMemory { data, .. } => {
            data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        }
        _ => panic!("expected InMemory index body for this test"),
    }
}

/// Extract the single-column integer keys of an index body, sorted.
///
/// Index bodies normalize all numeric key types to `SqlValue::Double` at
/// insertion time (`normalize_for_comparison`, so range scans can compare
/// mixed numeric types uniformly) — an integer column's stored key is never
/// `SqlValue::Integer`, so this accepts `Double` (the actual on-disk form)
/// alongside `Integer` for robustness.
fn index_int_keys(db: &Database, index_name: &str) -> Vec<i64> {
    let mut keys: Vec<i64> = index_body(db, index_name)
        .into_keys()
        .map(|k| match &k[0] {
            SqlValue::Integer(i) => *i,
            SqlValue::Double(d) => *d as i64,
            other => panic!("unexpected key value: {other:?}"),
        })
        .collect();
    keys.sort_unstable();
    keys
}

/// Parse + execute a CREATE INDEX statement, returning the executor result
/// (caller decides whether to `.unwrap()` or inspect the error).
fn exec_create_index(
    db: &mut Database,
    sql: &str,
) -> Result<String, vibesql_executor::ExecutorError> {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE INDEX");
    match stmt {
        vibesql_ast::Statement::CreateIndex(s) => CreateIndexExecutor::execute(&s, db),
        other => panic!("expected CreateIndex, got {other:?}"),
    }
}

/// Parse + execute a DROP INDEX statement, returning the executor result.
fn exec_drop_index(
    db: &mut Database,
    sql: &str,
) -> Result<String, vibesql_executor::ExecutorError> {
    let stmt = Parser::parse_sql(sql).expect("parse DROP INDEX");
    match stmt {
        vibesql_ast::Statement::DropIndex(s) => DropIndexExecutor::execute(&s, db),
        other => panic!("expected DropIndex, got {other:?}"),
    }
}

/// `CREATE INDEX main.i1 ON t(x)` resolves `t` in `main` and creates the
/// index there (sqlite3 3.51.0 parity).
#[test]
fn create_index_main_schema_qualified() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");

    let result = exec_create_index(&mut db, "CREATE INDEX main.i1 ON t(x)");
    assert!(result.is_ok(), "CREATE INDEX main.i1 should succeed: {:?}", result.err());
    assert!(db.index_exists("i1"));
}

/// `CREATE INDEX temp.i1 ON t(x)` resolves `t` specifically within the
/// session temp schema, even when a same-named `main.t` also exists —
/// matching sqlite3 3.51.0 (temp shadowing is bypassed by an explicit
/// qualifier; it is not needed here, but the qualifier still must route to
/// temp specifically rather than main).
#[test]
fn create_index_temp_schema_qualified_resolves_temp_table() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER, y INTEGER)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t(x INTEGER)");

    let result = exec_create_index(&mut db, "CREATE INDEX temp.i1 ON t(x)");
    assert!(result.is_ok(), "CREATE INDEX temp.i1 should succeed: {:?}", result.err());
    let msg = result.unwrap();
    assert!(
        msg.contains(&format!("{}.t", db.catalog.temp_schema_name())),
        "index should target the temp table, got: {msg}"
    );
}

/// An unrecognized schema qualifier is rejected with SQLite's exact
/// `unknown database <name>` wording (sqlite3 3.51.0 parity).
#[test]
fn create_index_unknown_schema_qualifier_rejected() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");

    let result = exec_create_index(&mut db, "CREATE INDEX bogus.i1 ON t(x)");
    assert!(result.is_err(), "CREATE INDEX bogus.i1 should fail");
    let err = result.unwrap_err().to_string();
    assert_eq!(err, "unknown database bogus", "unexpected error text: {err}");
}

/// `CREATE INDEX main.i1 ON t(x)` must resolve `t` in `main` specifically,
/// not the temp-shadowed table, when both exist (sqlite3 3.51.0 parity).
#[test]
fn create_index_main_schema_qualified_bypasses_temp_shadow() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER, y INTEGER)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t(x INTEGER)");

    let result = exec_create_index(&mut db, "CREATE INDEX main.i1 ON t(x)");
    assert!(result.is_ok(), "CREATE INDEX main.i1 should succeed: {:?}", result.err());
    let msg = result.unwrap();
    assert!(msg.contains("main.t"), "index should target main.t, got: {msg}");
}

/// `DROP INDEX main.i1` drops exactly the main-schema index, matching
/// sqlite3 3.51.0.
#[test]
fn drop_index_main_schema_qualified() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");
    exec_create_index(&mut db, "CREATE INDEX i1 ON t(x)").expect("CREATE INDEX");

    let result = exec_drop_index(&mut db, "DROP INDEX main.i1");
    assert!(result.is_ok(), "DROP INDEX main.i1 should succeed: {:?}", result.err());
    assert!(!db.index_exists("i1"));
}

/// `DROP INDEX temp.i1` drops only the temp-schema index, leaving a
/// same-named main-schema index untouched (sqlite3 3.51.0 parity).
#[test]
fn drop_index_temp_schema_qualified_scoped_to_temp() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");
    exec_create_table(&mut db, "CREATE TEMP TABLE t2(x INTEGER)");
    exec_create_index(&mut db, "CREATE INDEX i1 ON t(x)").expect("CREATE INDEX main.i1");
    exec_create_index(&mut db, "CREATE INDEX i1 ON t2(x)").expect("CREATE INDEX temp.i1");

    let result = exec_drop_index(&mut db, "DROP INDEX temp.i1");
    assert!(result.is_ok(), "DROP INDEX temp.i1 should succeed: {:?}", result.err());

    // The temp-schema i1 is gone, but the main-schema i1 survives untouched.
    let (_, rows) = {
        let stmt = Parser::parse_sql("SELECT name FROM sqlite_master WHERE name = 'i1'")
            .expect("parse SELECT");
        match stmt {
            vibesql_ast::Statement::Select(s) => {
                let r = vibesql_executor::SelectExecutor::new(&db)
                    .execute_with_columns(&s)
                    .expect("SELECT");
                (r.columns, r.rows)
            }
            other => panic!("expected SELECT, got {other:?}"),
        }
    };
    assert_eq!(rows.len(), 1, "main-schema i1 should survive DROP INDEX temp.i1");
}

/// `DROP INDEX bogus.i1` reports the index as not found (no dedicated
/// "unknown database" wording for DROP — sqlite3 3.51.0 always says
/// `no such index: schema.name` for both an unknown schema and an unknown
/// index name in a known schema).
#[test]
fn drop_index_unknown_schema_qualifier_reports_not_found() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");
    exec_create_index(&mut db, "CREATE INDEX i1 ON t(x)").expect("CREATE INDEX");

    let result = exec_drop_index(&mut db, "DROP INDEX bogus.i1");
    assert!(result.is_err(), "DROP INDEX bogus.i1 should fail");
}

/// `DROP INDEX IF EXISTS bogus.i1` is a silent no-op, matching sqlite3.
#[test]
fn drop_index_if_exists_unknown_schema_qualifier_is_noop() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");
    exec_create_index(&mut db, "CREATE INDEX i1 ON t(x)").expect("CREATE INDEX");

    let result = exec_drop_index(&mut db, "DROP INDEX IF EXISTS bogus.i1");
    assert!(result.is_ok(), "DROP INDEX IF EXISTS bogus.i1 should succeed: {:?}", result.err());
    // The main-schema i1 is untouched.
    assert!(db.index_exists("i1"));
}

/// The motivating case from #6310/#6366: an ATTACHed database's name is a
/// legal index-name qualifier, and the target table is resolved inside that
/// attachment's schema.
#[test]
fn create_and_drop_index_qualified_by_attached_schema() {
    let mut db = Database::new();
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH DATABASE");
    exec_create_table(&mut db, "CREATE TABLE aux.t(x INTEGER)");

    let result = exec_create_index(&mut db, "CREATE INDEX aux.i1 ON t(x)");
    assert!(result.is_ok(), "CREATE INDEX aux.i1 should succeed: {:?}", result.err());
    assert!(result.unwrap().contains("aux.t"), "index should target aux.t");
    assert!(db.index_exists("aux.i1"));

    let result = exec_drop_index(&mut db, "DROP INDEX aux.i1");
    assert!(result.is_ok(), "DROP INDEX aux.i1 should succeed: {:?}", result.err());
    assert!(!db.index_exists("aux.i1"));
}

/// A schema-qualified target table that does not exist is still reported as
/// a missing table (not silently treated as "unknown database").
#[test]
fn create_index_schema_qualified_missing_table() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");

    let result = exec_create_index(&mut db, "CREATE INDEX main.i1 ON nonexistent(x)");
    assert!(result.is_err(), "CREATE INDEX main.i1 ON nonexistent(x) should fail");
    assert!(matches!(result.unwrap_err(), vibesql_executor::ExecutorError::TableNotFound(_)));
}

// ============================================================================
// Regression tests for issue #6487: `CREATE INDEX <schema>.<idx> ON <t>(...)`
// must build the physical index body against the schema-qualified table the
// validator resolved, not a bare-name-resolved (and possibly same-named
// `main`) table.
// ============================================================================

/// Shape 1 (previously a hard error): `main.t` lacks the indexed column but
/// `aux.t` has it. Before the fix, `build_btree_index_body` resolved the
/// bare name `t` through storage's own search order and landed on `main.t`,
/// producing `Column 'z' not found in table 't'` even though the validator
/// correctly resolved the index against `aux.t`.
#[test]
fn create_btree_index_attached_schema_missing_column_in_main_builds_against_attached_table() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x TEXT)"); // main.t: no z column
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH DATABASE");
    exec_create_table(&mut db, "CREATE TABLE aux.t(x TEXT, z INTEGER)");
    exec_insert(&mut db, "INSERT INTO aux.t VALUES ('a', 1)");
    exec_insert(&mut db, "INSERT INTO aux.t VALUES ('b', 2)");

    let result = exec_create_index(&mut db, "CREATE INDEX aux.i1 ON t(z)");
    assert!(
        result.is_ok(),
        "CREATE INDEX aux.i1 ON t(z) should succeed against aux.t despite main.t lacking \
         column 'z': {:?}",
        result.err()
    );

    let keys = index_int_keys(&db, "aux.i1");
    assert_eq!(keys, vec![1, 2], "index body should reflect aux.t's z values (1, 2)");
}

/// Shape 2 (previously a silently wrong body): both `main.t` and `aux.t`
/// have the indexed column, so the pre-fix code built a body from `main.t`'s
/// row instead of erroring — the bug was invisible without inspecting the
/// index body directly. Verifies the body reflects `aux.t`'s rows only.
#[test]
fn create_btree_index_attached_schema_colliding_column_builds_against_attached_table_not_main() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x TEXT, z INTEGER)"); // main.t HAS z too
    exec_insert(&mut db, "INSERT INTO t VALUES ('main-row', 100)");
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH DATABASE");
    exec_create_table(&mut db, "CREATE TABLE aux.t(x TEXT, z INTEGER)");
    exec_insert(&mut db, "INSERT INTO aux.t VALUES ('a', 1)");
    exec_insert(&mut db, "INSERT INTO aux.t VALUES ('b', 2)");

    let result = exec_create_index(&mut db, "CREATE INDEX aux.i1 ON t(z)");
    assert!(result.is_ok(), "CREATE INDEX aux.i1 ON t(z) should succeed: {:?}", result.err());

    let keys = index_int_keys(&db, "aux.i1");
    assert_eq!(
        keys,
        vec![1, 2],
        "index body must reflect aux.t's rows (1, 2), not main.t's colliding row (100)"
    );
}

/// Expression-index counterpart of Shape 1: `create_expression_index` /
/// `compute_expression_index_keys` (expression_index.rs) had the identical
/// bare-name bug.
#[test]
fn create_expression_index_attached_schema_missing_column_in_main_builds_against_attached_table() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x TEXT)"); // main.t: no z column
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH DATABASE");
    exec_create_table(&mut db, "CREATE TABLE aux.t(x TEXT, z INTEGER)");
    exec_insert(&mut db, "INSERT INTO aux.t VALUES ('a', 1)");
    exec_insert(&mut db, "INSERT INTO aux.t VALUES ('b', 2)");

    let result = exec_create_index(&mut db, "CREATE INDEX aux.i1 ON t(z + 0)");
    assert!(
        result.is_ok(),
        "CREATE INDEX aux.i1 ON t(z + 0) should succeed against aux.t despite main.t lacking \
         column 'z': {:?}",
        result.err()
    );

    let keys = index_int_keys(&db, "aux.i1");
    assert_eq!(keys, vec![1, 2], "expression index body should reflect aux.t's z values (1, 2)");
}

/// Expression-index counterpart of Shape 2.
#[test]
fn create_expression_index_attached_schema_colliding_column_builds_against_attached_table_not_main()
{
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x TEXT, z INTEGER)"); // main.t HAS z too
    exec_insert(&mut db, "INSERT INTO t VALUES ('main-row', 100)");
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH DATABASE");
    exec_create_table(&mut db, "CREATE TABLE aux.t(x TEXT, z INTEGER)");
    exec_insert(&mut db, "INSERT INTO aux.t VALUES ('a', 1)");
    exec_insert(&mut db, "INSERT INTO aux.t VALUES ('b', 2)");

    let result = exec_create_index(&mut db, "CREATE INDEX aux.i1 ON t(z + 0)");
    assert!(result.is_ok(), "CREATE INDEX aux.i1 ON t(z + 0) should succeed: {:?}", result.err());

    let keys = index_int_keys(&db, "aux.i1");
    assert_eq!(
        keys,
        vec![1, 2],
        "expression index body must reflect aux.t's rows (1, 2), not main.t's colliding row (100)"
    );
}

/// Control case: an ordinary, non-attached schema-qualified table
/// (`main.t`, explicitly qualified) is unaffected by the fix — the index
/// body still reflects `main.t`'s own rows.
#[test]
fn create_btree_index_main_schema_qualified_body_reflects_main_table_rows() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");
    exec_insert(&mut db, "INSERT INTO t VALUES (1)");
    exec_insert(&mut db, "INSERT INTO t VALUES (2)");

    let result = exec_create_index(&mut db, "CREATE INDEX main.i1 ON t(x)");
    assert!(result.is_ok(), "CREATE INDEX main.i1 ON t(x) should succeed: {:?}", result.err());

    let keys = index_int_keys(&db, "i1");
    assert_eq!(keys, vec![1, 2], "index body should reflect main.t's own rows (1, 2)");
}
