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
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::AutoIncrement },
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::PrimaryKey },
                ],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "username".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create table: {:?}", result.err());

    // Insert without specifying id - should auto-generate 1
    let insert1 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["username".to_string()],
        source: InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            SqlValue::Varchar(arcstr::ArcStr::from("alice")),
        )]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert1);
    assert!(result.is_ok(), "Failed to insert alice: {:?}", result.err());

    // Insert without specifying id - should auto-generate 2
    let insert2 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["username".to_string()],
        source: InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            SqlValue::Varchar(arcstr::ArcStr::from("bob")),
        )]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
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
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
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
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::AutoIncrement },
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::PrimaryKey },
                ],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "username".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create table: {:?}", result.err());

    // Before any insert, last_insert_rowid should be 0
    assert_eq!(db.last_insert_rowid(), 0);

    // Insert first row - should auto-generate id=1
    let insert1 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["username".to_string()],
        source: InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            SqlValue::Varchar(arcstr::ArcStr::from("alice")),
        )]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert1);
    assert!(result.is_ok(), "Failed to insert alice: {:?}", result.err());

    // LAST_INSERT_ROWID should be 1
    assert_eq!(db.last_insert_rowid(), 1);

    // Insert second row - should auto-generate id=2
    let insert2 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["username".to_string()],
        source: InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            SqlValue::Varchar(arcstr::ArcStr::from("bob")),
        )]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
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
        if_not_exists: false,
        table_name: "items".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::AutoIncrement },
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::PrimaryKey },
                ],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create table: {:?}", result.err());

    // Multi-row insert - per MySQL semantics, LAST_INSERT_ID returns the FIRST generated ID
    let multi_insert = InsertStmt {
        table_name: "items".to_string(),
        columns: vec!["name".to_string()],
        source: InsertSource::Values(vec![
            vec![vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("item1")))],
            vec![vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("item2")))],
            vec![vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("item3")))],
        ]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    let result = InsertExecutor::execute(&mut db, &multi_insert);
    assert!(result.is_ok(), "Failed to multi-row insert: {:?}", result.err());

    // LAST_INSERT_ROWID should be 1 (the first generated ID, not 3)
    assert_eq!(db.last_insert_rowid(), 1);

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
        if_not_exists: false,
        table_name: "manual".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::PrimaryKey,
                }],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create table: {:?}", result.err());

    // Insert with explicit ID - no auto-generation
    let insert1 = InsertStmt {
        table_name: "manual".to_string(),
        columns: vec!["id".to_string(), "name".to_string()],
        source: InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(100)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("test"))),
        ]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert1);
    assert!(result.is_ok(), "Failed to insert: {:?}", result.err());

    // LAST_INSERT_ROWID should still be 0 (no auto-generated values)
    assert_eq!(db.last_insert_rowid(), 0);
}

#[test]
fn test_last_insert_rowid_via_select() {
    use vibesql_parser::Parser;

    let mut db = Database::new();

    // Create table with AUTO_INCREMENT
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::AutoIncrement },
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::PrimaryKey },
                ],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create table: {:?}", result.err());

    // Insert a row
    let insert1 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["name".to_string()],
        source: InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            SqlValue::Varchar(arcstr::ArcStr::from("alice")),
        )]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
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
