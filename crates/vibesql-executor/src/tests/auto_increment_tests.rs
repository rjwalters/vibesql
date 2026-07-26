//! Tests for AUTO_INCREMENT and LAST_INSERT_ROWID() functionality

use vibesql_ast::{
    ColumnConstraint, ColumnConstraintKind, ColumnDef, CreateTableStmt, InsertSource, InsertStmt,
};
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

use crate::{CreateTableExecutor, InsertExecutor, SelectExecutor};

#[test]
fn test_auto_increment_basic_inserts() {
    let mut db = Database::new();

    // Create table with AUTO_INCREMENT
    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::AutoIncrement },
                    ColumnConstraint {
                        name: None,
                        kind: ColumnConstraintKind::PrimaryKey { on_conflict: None },
                    },
                ],
                default_value: None,
                comment: None,
                generated_expr: None,
                // INTEGER PRIMARY KEY AUTOINCREMENT is a genuine rowid-alias column.
                is_exact_integer_type: true,
                type_source: None,
            },
            ColumnDef {
                name: "username".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
        strict: false,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create table: {:?}", result.err());

    // Insert without specifying id - should auto-generate 1
    let insert1 = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "users".to_string(),
        columns: vec!["username".to_string()],
        source: InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            SqlValue::Varchar(arcstr::ArcStr::from("alice")),
        )]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert1);
    assert!(result.is_ok(), "Failed to insert alice: {:?}", result.err());

    // Insert without specifying id - should auto-generate 2
    let insert2 = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "users".to_string(),
        columns: vec!["username".to_string()],
        source: InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            SqlValue::Varchar(arcstr::ArcStr::from("bob")),
        )]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert2);
    assert!(result.is_ok(), "Failed to insert bob: {:?}", result.err());

    // Query to verify - should have auto-generated ids 1 and 2
    let table = db.get_table("users").unwrap();
    let rows = table.scan();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[0], SqlValue::Integer(1)); // First id should be 1
    assert_eq!(rows[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("alice")));
    assert_eq!(rows[1].values[0], SqlValue::Integer(2)); // Second id should be 2
    assert_eq!(rows[1].values[1], SqlValue::Varchar(arcstr::ArcStr::from("bob")));
}

#[test]
fn test_multiple_auto_increment_error() {
    let mut db = Database::new();

    // Should fail - multiple AUTO_INCREMENT columns not allowed
    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "bad".to_string(),
        columns: vec![
            ColumnDef {
                name: "id1".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::AutoIncrement,
                }],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
            ColumnDef {
                name: "id2".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::AutoIncrement,
                }],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
        strict: false,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    let error = result.unwrap_err().to_string();
    assert!(error.contains("Only one AUTO_INCREMENT column allowed"));
}

#[test]
fn test_last_insert_rowid_basic() {
    let mut db = Database::new();

    // Create table with AUTO_INCREMENT
    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::AutoIncrement },
                    ColumnConstraint {
                        name: None,
                        kind: ColumnConstraintKind::PrimaryKey { on_conflict: None },
                    },
                ],
                default_value: None,
                comment: None,
                generated_expr: None,
                // INTEGER PRIMARY KEY AUTOINCREMENT is a genuine rowid-alias column.
                is_exact_integer_type: true,
                type_source: None,
            },
            ColumnDef {
                name: "username".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
        strict: false,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create table: {:?}", result.err());

    // Before any insert, last_insert_rowid should be 0
    assert_eq!(db.last_insert_rowid(), 0);

    // Insert first row - should auto-generate id=1
    let insert1 = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "users".to_string(),
        columns: vec!["username".to_string()],
        source: InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            SqlValue::Varchar(arcstr::ArcStr::from("alice")),
        )]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert1);
    assert!(result.is_ok(), "Failed to insert alice: {:?}", result.err());

    // LAST_INSERT_ROWID should be 1
    assert_eq!(db.last_insert_rowid(), 1);

    // Insert second row - should auto-generate id=2
    let insert2 = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "users".to_string(),
        columns: vec!["username".to_string()],
        source: InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            SqlValue::Varchar(arcstr::ArcStr::from("bob")),
        )]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert2);
    assert!(result.is_ok(), "Failed to insert bob: {:?}", result.err());

    // LAST_INSERT_ROWID should be 2
    assert_eq!(db.last_insert_rowid(), 2);
}

#[test]
fn test_last_insert_rowid_multi_row_insert() {
    let mut db = Database::new();

    // Create table with AUTO_INCREMENT
    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "items".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::AutoIncrement },
                    ColumnConstraint {
                        name: None,
                        kind: ColumnConstraintKind::PrimaryKey { on_conflict: None },
                    },
                ],
                default_value: None,
                comment: None,
                generated_expr: None,
                // INTEGER PRIMARY KEY AUTOINCREMENT is a genuine rowid-alias column.
                is_exact_integer_type: true,
                type_source: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
        strict: false,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create table: {:?}", result.err());

    // Multi-row insert - per SQLite semantics, last_insert_rowid() returns the
    // rowid of the LAST row inserted (not the first)
    let multi_insert = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "items".to_string(),
        columns: vec!["name".to_string()],
        source: InsertSource::Values(vec![
            vec![vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(
                "item1",
            )))],
            vec![vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(
                "item2",
            )))],
            vec![vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(
                "item3",
            )))],
        ]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    let result = InsertExecutor::execute(&mut db, &multi_insert);
    assert!(result.is_ok(), "Failed to multi-row insert: {:?}", result.err());

    // last_insert_rowid() should be 3 (the LAST generated ID, per sqlite3)
    assert_eq!(db.last_insert_rowid(), 3);

    // Verify all rows were inserted with correct IDs
    let table = db.get_table("items").unwrap();
    let rows = table.scan();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
    assert_eq!(rows[2].values[0], SqlValue::Integer(3));
}

#[test]
fn test_last_insert_rowid_no_auto_increment() {
    let mut db = Database::new();

    // Create table WITHOUT AUTO_INCREMENT
    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "manual".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::PrimaryKey { on_conflict: None },
                }],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
        strict: false,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create table: {:?}", result.err());

    // Insert with explicit ID - no auto-generation
    let insert1 = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "manual".to_string(),
        columns: vec!["id".to_string(), "name".to_string()],
        source: InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(100)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("test"))),
        ]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert1);
    assert!(result.is_ok(), "Failed to insert: {:?}", result.err());

    // LAST_INSERT_ROWID should be 100 (the explicit INTEGER PRIMARY KEY value)
    // SQLite semantics: last_insert_rowid() returns the rowid of the most recently
    // inserted row, whether auto-generated or explicitly provided
    assert_eq!(db.last_insert_rowid(), 100);
}

#[test]
fn test_last_insert_rowid_via_select() {
    use vibesql_parser::Parser;

    let mut db = Database::new();

    // Create table with AUTO_INCREMENT
    let stmt = CreateTableStmt {
        temporary: false,
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::AutoIncrement },
                    ColumnConstraint {
                        name: None,
                        kind: ColumnConstraintKind::PrimaryKey { on_conflict: None },
                    },
                ],
                default_value: None,
                comment: None,
                generated_expr: None,
                // INTEGER PRIMARY KEY AUTOINCREMENT is a genuine rowid-alias column.
                is_exact_integer_type: true,
                type_source: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
                is_exact_integer_type: false,
                type_source: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        name_source: None,
        as_query: None,
        without_rowid: false,
        strict: false,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create table: {:?}", result.err());

    // Insert a row
    let insert1 = InsertStmt {
        with_clause: None,
        schema_name: None,
        schema_quoted: false,
        table_quoted: false,
        table_name: "users".to_string(),
        columns: vec!["name".to_string()],
        source: InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            SqlValue::Varchar(arcstr::ArcStr::from("alice")),
        )]]),
        conflict_clause: None,
        on_conflict: vec![],
        on_duplicate_key_update: None,
        returning: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert1);
    assert!(result.is_ok(), "Failed to insert: {:?}", result.err());

    // Query LAST_INSERT_ROWID() via SELECT
    let select_stmt = Parser::parse_sql("SELECT LAST_INSERT_ROWID()").unwrap();
    if let vibesql_ast::Statement::Select(select) = select_stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute_with_columns(&select);
        assert!(result.is_ok(), "Failed to execute SELECT: {:?}", result.err());

        let result = result.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(1));
    } else {
        panic!("Expected SELECT statement");
    }

    // Also test LAST_INSERT_ID() alias
    let select_stmt = Parser::parse_sql("SELECT LAST_INSERT_ID()").unwrap();
    if let vibesql_ast::Statement::Select(select) = select_stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute_with_columns(&select);
        assert!(result.is_ok(), "Failed to execute SELECT: {:?}", result.err());

        let result = result.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], SqlValue::Integer(1));
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Execute a single DDL/DML statement expressed as SQL text against `db`.
///
/// Small dispatch helper used by the INSERT ... SELECT tests below so they can
/// reproduce the exact multi-statement scripts from issue #5886 without hand
/// building every AST node.
fn exec_sql(db: &mut Database, sql: &str) {
    use vibesql_parser::Parser;
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for {sql:?}: {e:?}"));
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            CreateTableExecutor::execute(&create, db)
                .unwrap_or_else(|e| panic!("CREATE failed for {sql:?}: {e:?}"));
        }
        vibesql_ast::Statement::Insert(insert) => {
            InsertExecutor::execute(db, &insert)
                .unwrap_or_else(|e| panic!("INSERT failed for {sql:?}: {e:?}"));
        }
        other => panic!("unsupported statement in exec_sql: {other:?}"),
    }
}

/// Reproduces the exact bug from issue #5886: after a multi-row
/// `INSERT INTO ... SELECT`, `last_insert_rowid()` must return the rowid of the
/// LAST row inserted (sqlite3 returns 2 here), not the first (which was 1).
#[test]
fn test_last_insert_rowid_insert_select() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE src(a INTEGER)");
    exec_sql(&mut db, "INSERT INTO src VALUES(10)");
    exec_sql(&mut db, "INSERT INTO src VALUES(20)");
    exec_sql(&mut db, "CREATE TABLE t1(k INTEGER PRIMARY KEY, v INTEGER)");

    // Two source rows -> two inserted rows with rowids 1 and 2.
    exec_sql(&mut db, "INSERT INTO t1(v) SELECT a FROM src");

    // sqlite3: last_insert_rowid() == 2 (the LAST inserted row)
    assert_eq!(db.last_insert_rowid(), 2);

    // Sanity check: both rows landed with the expected rowids/values.
    let table = db.get_table("t1").unwrap();
    let rows = table.scan();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
}

/// Multi-row `INSERT ... VALUES` with explicit INTEGER PRIMARY KEY values must
/// report the LAST explicit rowid (sqlite3 returns 3 here).
#[test]
fn test_last_insert_rowid_multi_values_explicit_key() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER)");
    exec_sql(&mut db, "INSERT INTO t VALUES(10,1),(20,2),(30,3)");

    // sqlite3: last_insert_rowid() == 30 (rowid of the last row inserted)
    assert_eq!(db.last_insert_rowid(), 30);
}

/// Single explicit-key insert must be unchanged: last_insert_rowid() == the key.
#[test]
fn test_last_insert_rowid_single_explicit_key() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER)");
    exec_sql(&mut db, "INSERT INTO t VALUES(100,999)");

    // sqlite3: last_insert_rowid() == 100
    assert_eq!(db.last_insert_rowid(), 100);
}

/// An INSERT ... SELECT that inserts zero rows must leave last_insert_rowid()
/// at its previous value (sqlite3 semantics).
#[test]
fn test_last_insert_rowid_insert_select_zero_rows() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE src(a INTEGER)");
    exec_sql(&mut db, "CREATE TABLE t1(k INTEGER PRIMARY KEY, v INTEGER)");
    exec_sql(&mut db, "INSERT INTO t1(v) VALUES(7)"); // rowid 1
    assert_eq!(db.last_insert_rowid(), 1);

    // src is empty -> zero rows inserted -> last_insert_rowid() unchanged.
    exec_sql(&mut db, "INSERT INTO t1(v) SELECT a FROM src");
    assert_eq!(db.last_insert_rowid(), 1);
}

/// INSERT OR IGNORE where the LAST row is skipped by a conflict: last_insert_rowid()
/// must reflect the last row *actually inserted* (rowid 6), not the skipped one
/// (sqlite3 returns 6 here).
#[test]
fn test_last_insert_rowid_or_ignore_skips_last_row() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER)");
    exec_sql(&mut db, "INSERT INTO t VALUES(5,1)");
    // (6,2) inserts; (5,3) conflicts on the existing k=5 and is skipped.
    exec_sql(&mut db, "INSERT OR IGNORE INTO t VALUES(6,2),(5,3)");

    // sqlite3: last_insert_rowid() == 6 (last row actually inserted)
    assert_eq!(db.last_insert_rowid(), 6);
}

/// Mixed-batch upsert: the LAST row takes the ON CONFLICT DO UPDATE arm (an
/// update, not an insert) while an earlier row inserts. last_insert_rowid() must
/// report the last row *actually inserted* (rowid 6), NOT the updated row's
/// rowid (5). SQLite excludes pure updates from last_insert_rowid(); verified
/// against sqlite3 3.51.0 (returns 6). Regression guard for issue #5886.
#[test]
fn test_last_insert_rowid_upsert_mixed_batch_excludes_update() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER)");
    exec_sql(&mut db, "INSERT INTO t VALUES(5,1)");
    // (6,2) inserts (no conflict); (5,3) conflicts on k=5 and takes the DO
    // UPDATE arm — an update, so it must not touch last_insert_rowid().
    exec_sql(&mut db, "INSERT INTO t VALUES(6,2),(5,3) ON CONFLICT(k) DO UPDATE SET v=excluded.v");

    // sqlite3: last_insert_rowid() == 6 (last row actually inserted, not 5)
    assert_eq!(db.last_insert_rowid(), 6);
}

/// An upsert batch where EVERY row takes the DO UPDATE arm inserts nothing, so
/// last_insert_rowid() must stay at its prior value (the rowid of the last row
/// actually inserted by an earlier statement). Verified against sqlite3 3.51.0:
/// after inserting rowid 9 then an all-update upsert, last_insert_rowid() == 9.
/// Regression guard for issue #5886.
#[test]
fn test_last_insert_rowid_upsert_all_updates_keeps_prior() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER)");
    exec_sql(&mut db, "INSERT INTO t VALUES(5,1)");
    exec_sql(&mut db, "INSERT INTO t VALUES(9,9)"); // last actual insert -> rowid 9
    assert_eq!(db.last_insert_rowid(), 9);

    // k=5 already exists -> DO UPDATE arm only; nothing inserted.
    exec_sql(&mut db, "INSERT INTO t VALUES(5,3) ON CONFLICT(k) DO UPDATE SET v=excluded.v");

    // sqlite3: last_insert_rowid() unchanged at 9 (no row inserted)
    assert_eq!(db.last_insert_rowid(), 9);
}

/// The bulk-transfer fast path (`INSERT INTO t SELECT * FROM src`, no column
/// list) must also report the last inserted rowid. Source rows are inserted out
/// of rowid order so the value left by the last `INSERT INTO src` (200) differs
/// from the last row copied into `t` in scan order (rowid 300) — sqlite3 == 300.
#[test]
fn test_last_insert_rowid_bulk_transfer() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE src(k INTEGER PRIMARY KEY, v INTEGER)");
    exec_sql(&mut db, "INSERT INTO src VALUES(300,3)");
    exec_sql(&mut db, "INSERT INTO src VALUES(100,1)");
    exec_sql(&mut db, "INSERT INTO src VALUES(200,2)");
    assert_eq!(db.last_insert_rowid(), 200); // last src insert

    exec_sql(&mut db, "CREATE TABLE t(k INTEGER PRIMARY KEY, v INTEGER)");
    exec_sql(&mut db, "INSERT INTO t SELECT * FROM src");

    // sqlite3: last_insert_rowid() == 300 (last row copied, in rowid scan order)
    assert_eq!(db.last_insert_rowid(), 300);
}

/// A single-row INSERT into a table WITHOUT an INTEGER PRIMARY KEY (implicit
/// rowid) must update last_insert_rowid() with the allocated implicit rowid.
/// sqlite3 returns 1 here; VibeSQL previously left it at 0 (issue #5944). This
/// exercises the slow (row-by-row) insert path, which single-row inserts take.
#[test]
fn test_last_insert_rowid_implicit_rowid_table() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(x TEXT)");
    exec_sql(&mut db, "INSERT INTO t VALUES('a')");

    // sqlite3: last_insert_rowid() == 1 (the allocated implicit rowid)
    assert_eq!(db.last_insert_rowid(), 1);
}

/// A multi-row INSERT ... VALUES into an implicit-rowid table must report the
/// rowid of the LAST inserted row. This exercises the fast batch insert path
/// (more than one row, no triggers). sqlite3 returns 3 here (issue #5944).
#[test]
fn test_last_insert_rowid_implicit_rowid_multi_row() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(x TEXT)");
    exec_sql(&mut db, "INSERT INTO t VALUES('a')"); // rowid 1 (slow path)
    assert_eq!(db.last_insert_rowid(), 1);

    // Two more rows -> rowids 2 and 3 via the fast batch path.
    exec_sql(&mut db, "INSERT INTO t VALUES('b'),('c')");

    // sqlite3: last_insert_rowid() == 3 (last row inserted)
    assert_eq!(db.last_insert_rowid(), 3);
}

/// The bulk-transfer fast path (`INSERT INTO t SELECT ...`) into an
/// implicit-rowid destination must also update last_insert_rowid() with the
/// last copied row's allocated rowid. sqlite3 returns 2 here (issue #5944).
#[test]
fn test_last_insert_rowid_implicit_rowid_bulk_transfer() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE src(x TEXT)");
    exec_sql(&mut db, "INSERT INTO src VALUES('d')"); // src rowid 1
    exec_sql(&mut db, "INSERT INTO src VALUES('e')"); // src rowid 2
    assert_eq!(db.last_insert_rowid(), 2);

    exec_sql(&mut db, "CREATE TABLE t(x TEXT)");
    exec_sql(&mut db, "INSERT INTO t SELECT x FROM src");

    // Two rows copied into t get implicit rowids 1 and 2; the last is 2.
    // sqlite3: last_insert_rowid() == 2.
    assert_eq!(db.last_insert_rowid(), 2);
}

/// A WITHOUT ROWID table must NOT update last_insert_rowid() (SQLite
/// R-47220-63683). Inserting into one leaves the prior value untouched.
#[test]
fn test_last_insert_rowid_without_rowid_unchanged() {
    let mut db = Database::new();

    // Establish a prior last_insert_rowid() from a normal rowid table.
    exec_sql(&mut db, "CREATE TABLE r(x TEXT)");
    exec_sql(&mut db, "INSERT INTO r VALUES('a')"); // rowid 1
    assert_eq!(db.last_insert_rowid(), 1);

    // Inserting into a WITHOUT ROWID table must leave last_insert_rowid() at 1.
    exec_sql(&mut db, "CREATE TABLE wr(x TEXT PRIMARY KEY) WITHOUT ROWID");
    exec_sql(&mut db, "INSERT INTO wr VALUES('z')");
    assert_eq!(db.last_insert_rowid(), 1);

    // A multi-row insert into the WITHOUT ROWID table (fast batch path) must
    // also leave it unchanged.
    exec_sql(&mut db, "INSERT INTO wr VALUES('y'),('x')");
    assert_eq!(db.last_insert_rowid(), 1);
}

/// A multi-row `INSERT ... VALUES` into a non-IPK rowid table that supplies
/// EXPLICIT, out-of-order rowid pseudo-column values must report the LAST row's
/// rowid, not the batch max. This exercises the fast batch path; the prior
/// max-rowid readback (issue #5944) returned the batch max (10) here, which
/// diverges from SQLite. Verified against sqlite3 3.51.0: returns 5 (issue
/// #5955).
#[test]
fn test_last_insert_rowid_explicit_out_of_order_non_ipk() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(x TEXT)");
    // rowids 10 then 5 — the LAST inserted row's rowid is 5, below the max (10).
    exec_sql(&mut db, "INSERT INTO t(rowid, x) VALUES(10, 'a'), (5, 'b')");

    // sqlite3: last_insert_rowid() == 5 (rowid of the last row inserted)
    assert_eq!(db.last_insert_rowid(), 5);
}

/// Three explicit out-of-order rowids (3, 1, 2) into a non-IPK rowid table:
/// last_insert_rowid() must be the last row's rowid (2), neither the max (3)
/// nor the min. Verified against sqlite3 3.51.0: returns 2 (issue #5955).
#[test]
fn test_last_insert_rowid_explicit_out_of_order_three_rows() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(x TEXT)");
    exec_sql(&mut db, "INSERT INTO t(rowid, x) VALUES(3, 'a'), (1, 'b'), (2, 'c')");

    // sqlite3: last_insert_rowid() == 2 (rowid of the last row inserted)
    assert_eq!(db.last_insert_rowid(), 2);
}

/// A negative explicit rowid on the LAST row of a non-IPK batch must be
/// reported verbatim, even though it is below every other rowid in the batch
/// (rowids are signed). Verified against sqlite3 3.51.0: returns -3 (issue
/// #5955).
#[test]
fn test_last_insert_rowid_explicit_negative_last_row_non_ipk() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(x TEXT)");
    exec_sql(&mut db, "INSERT INTO t(rowid, x) VALUES(10, 'a'), (-3, 'b')");

    // sqlite3: last_insert_rowid() == -3 (rowid of the last row inserted)
    assert_eq!(db.last_insert_rowid(), -3);
}

/// Mixed batch into a non-IPK rowid table where the LAST row's rowid is NULL
/// (auto-assigned). The auto-assigned rowid is max+1, so last_insert_rowid()
/// must report the allocated value (11), matching the exact rowid the last row
/// received. Verified against sqlite3 3.51.0: returns 11 (issue #5955).
#[test]
fn test_last_insert_rowid_explicit_then_null_non_ipk() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(x TEXT)");
    // rowid 10 then NULL -> the NULL row auto-assigns max+1 = 11.
    exec_sql(&mut db, "INSERT INTO t(rowid, x) VALUES(10, 'a'), (NULL, 'b')");

    // sqlite3: last_insert_rowid() == 11 (the auto-allocated rowid of the last row)
    assert_eq!(db.last_insert_rowid(), 11);
}

/// Mixed batch into a non-IPK rowid table where the FIRST row's rowid is NULL
/// (auto-assigned to 1) and the LAST row supplies an explicit, lower rowid (5,
/// still above 1). last_insert_rowid() must report the last row's explicit
/// value (5). Verified against sqlite3 3.51.0: returns 5 (issue #5955).
#[test]
fn test_last_insert_rowid_null_then_explicit_non_ipk() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(x TEXT)");
    // NULL (auto -> 1) then explicit 5 -> the last row's rowid is 5.
    exec_sql(&mut db, "INSERT INTO t(rowid, x) VALUES(NULL, 'a'), (5, 'b')");

    // sqlite3: last_insert_rowid() == 5 (rowid of the last row inserted)
    assert_eq!(db.last_insert_rowid(), 5);
}

/// In-order explicit ascending rowids into a non-IPK rowid table: the last
/// row's rowid IS the batch max, so the value is unchanged from the pre-#5955
/// behavior — a guard that the fix does not regress the monotonic case.
/// Verified against sqlite3 3.51.0: returns 10 (issue #5955).
#[test]
fn test_last_insert_rowid_explicit_in_order_non_ipk() {
    let mut db = Database::new();

    exec_sql(&mut db, "CREATE TABLE t(x TEXT)");
    exec_sql(&mut db, "INSERT INTO t(rowid, x) VALUES(5, 'a'), (10, 'b')");

    // sqlite3: last_insert_rowid() == 10 (rowid of the last row inserted == max)
    assert_eq!(db.last_insert_rowid(), 10);
}
