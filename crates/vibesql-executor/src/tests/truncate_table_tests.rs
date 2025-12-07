//! Tests for TRUNCATE TABLE executor

use crate::{CreateTableExecutor, TruncateTableExecutor};
use vibesql_ast::{ColumnDef, CreateTableStmt, TruncateTableStmt};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_truncate_single_table() {
    let mut db = Database::new();

    // Create a table
    let create_stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "USERS".to_string(),
        columns: vec![
            ColumnDef {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert some rows
    db.insert_row(
        "USERS",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    )
    .unwrap();
    db.insert_row(
        "USERS",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
    )
    .unwrap();
    db.insert_row(
        "USERS",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Carol"))]),
    )
    .unwrap();

    assert_eq!(db.get_table("USERS").unwrap().row_count(), 3);

    // Truncate the table
    let truncate_stmt = TruncateTableStmt {
        table_names: vec!["USERS".to_string()],
        if_exists: false,
        cascade: None,
    };

    let rows_deleted = TruncateTableExecutor::execute(&truncate_stmt, &mut db).unwrap();
    assert_eq!(rows_deleted, 3);
    assert_eq!(db.get_table("USERS").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_multiple_tables() {
    let mut db = Database::new();

    // Create first table
    let create_stmt1 = CreateTableStmt {
        if_not_exists: false,
        table_name: "ORDERS".to_string(),
        columns: vec![ColumnDef {
            name: "ID".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt1, &mut db).unwrap();

    // Create second table
    let create_stmt2 = CreateTableStmt {
        if_not_exists: false,
        table_name: "ORDER_ITEMS".to_string(),
        columns: vec![ColumnDef {
            name: "ID".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt2, &mut db).unwrap();

    // Create third table
    let create_stmt3 = CreateTableStmt {
        if_not_exists: false,
        table_name: "ORDER_HISTORY".to_string(),
        columns: vec![ColumnDef {
            name: "ID".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt3, &mut db).unwrap();

    // Insert rows into each table
    db.insert_row("ORDERS", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("ORDERS", Row::new(vec![SqlValue::Integer(2)])).unwrap();

    db.insert_row("ORDER_ITEMS", Row::new(vec![SqlValue::Integer(10)])).unwrap();
    db.insert_row("ORDER_ITEMS", Row::new(vec![SqlValue::Integer(20)])).unwrap();
    db.insert_row("ORDER_ITEMS", Row::new(vec![SqlValue::Integer(30)])).unwrap();

    db.insert_row("ORDER_HISTORY", Row::new(vec![SqlValue::Integer(100)])).unwrap();

    assert_eq!(db.get_table("ORDERS").unwrap().row_count(), 2);
    assert_eq!(db.get_table("ORDER_ITEMS").unwrap().row_count(), 3);
    assert_eq!(db.get_table("ORDER_HISTORY").unwrap().row_count(), 1);

    // Truncate all three tables
    let truncate_stmt = TruncateTableStmt {
        table_names: vec![
            "ORDERS".to_string(),
            "ORDER_ITEMS".to_string(),
            "ORDER_HISTORY".to_string(),
        ],
        if_exists: false,
        cascade: None,
    };

    let rows_deleted = TruncateTableExecutor::execute(&truncate_stmt, &mut db).unwrap();
    assert_eq!(rows_deleted, 6); // 2 + 3 + 1
    assert_eq!(db.get_table("ORDERS").unwrap().row_count(), 0);
    assert_eq!(db.get_table("ORDER_ITEMS").unwrap().row_count(), 0);
    assert_eq!(db.get_table("ORDER_HISTORY").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_if_exists_table_not_exists() {
    let mut db = Database::new();

    // Try to truncate a non-existent table with IF EXISTS
    let truncate_stmt = TruncateTableStmt {
        table_names: vec!["NONEXISTENT".to_string()],
        if_exists: true,
        cascade: None,
    };

    let rows_deleted = TruncateTableExecutor::execute(&truncate_stmt, &mut db).unwrap();
    assert_eq!(rows_deleted, 0);
}

#[test]
fn test_truncate_without_if_exists_table_not_exists() {
    let mut db = Database::new();

    // Try to truncate a non-existent table without IF EXISTS - should error
    let truncate_stmt = TruncateTableStmt {
        table_names: vec!["NONEXISTENT".to_string()],
        if_exists: false,
        cascade: None,
    };

    let result = TruncateTableExecutor::execute(&truncate_stmt, &mut db);
    assert!(result.is_err());
}

#[test]
fn test_truncate_multiple_tables_if_exists_mixed() {
    let mut db = Database::new();

    // Create one table
    let create_stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "EXISTING".to_string(),
        columns: vec![ColumnDef {
            name: "ID".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert rows
    db.insert_row("EXISTING", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("EXISTING", Row::new(vec![SqlValue::Integer(2)])).unwrap();

    // Truncate one existing and one non-existing table with IF EXISTS
    let truncate_stmt = TruncateTableStmt {
        table_names: vec![
            "EXISTING".to_string(),
            "NONEXISTENT1".to_string(),
            "NONEXISTENT2".to_string(),
        ],
        if_exists: true,
        cascade: None,
    };

    let rows_deleted = TruncateTableExecutor::execute(&truncate_stmt, &mut db).unwrap();
    assert_eq!(rows_deleted, 2); // Only from EXISTING
    assert_eq!(db.get_table("EXISTING").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_multiple_tables_all_or_nothing_validation() {
    let mut db = Database::new();

    // Create one table
    let create_stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "EXISTING".to_string(),
        columns: vec![ColumnDef {
            name: "ID".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert rows
    db.insert_row("EXISTING", Row::new(vec![SqlValue::Integer(1)])).unwrap();

    // Try to truncate one existing and one non-existing without IF EXISTS
    // Should fail and not truncate anything
    let truncate_stmt = TruncateTableStmt {
        table_names: vec!["EXISTING".to_string(), "NONEXISTENT".to_string()],
        if_exists: false,
        cascade: None,
    };

    let result = TruncateTableExecutor::execute(&truncate_stmt, &mut db);
    assert!(result.is_err());

    // Verify the existing table was NOT truncated (all-or-nothing)
    assert_eq!(db.get_table("EXISTING").unwrap().row_count(), 1);
}

#[test]
fn test_truncate_empty_table() {
    let mut db = Database::new();

    // Create a table
    let create_stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "EMPTY".to_string(),
        columns: vec![ColumnDef {
            name: "ID".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Truncate empty table
    let truncate_stmt = TruncateTableStmt {
        table_names: vec!["EMPTY".to_string()],
        if_exists: false,
        cascade: None,
    };

    let rows_deleted = TruncateTableExecutor::execute(&truncate_stmt, &mut db).unwrap();
    assert_eq!(rows_deleted, 0);
    assert_eq!(db.get_table("EMPTY").unwrap().row_count(), 0);
}

// ============================================================================
// AUTO_INCREMENT Reset Tests
// ============================================================================

#[test]
fn test_truncate_resets_auto_increment() {
    use crate::InsertExecutor;
    use vibesql_ast::{
        ColumnConstraint, ColumnConstraintKind, Expression, InsertSource, InsertStmt,
    };

    let mut db = Database::new();

    // Create table with AUTO_INCREMENT
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "auto_inc_test".to_string(),
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
                name: "data".to_string(),
                data_type: DataType::Varchar { max_length: Some(10) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&stmt, &mut db).unwrap();

    // Insert 3 rows (ids should be 1, 2, 3)
    for val in ["a", "b", "c"] {
        let insert = InsertStmt {
            table_name: "auto_inc_test".to_string(),
            columns: vec!["data".to_string()],
            source: InsertSource::Values(vec![vec![Expression::Literal(SqlValue::Varchar(
                arcstr::ArcStr::from(val),
            ))]]),
            conflict_clause: None,
            on_duplicate_key_update: None,
        };
        InsertExecutor::execute(&mut db, &insert).unwrap();
    }

    // Verify ids are 1, 2, 3
    let table = db.get_table("auto_inc_test").unwrap();
    assert_eq!(table.scan().len(), 3);
    assert_eq!(table.scan()[0].values[0], SqlValue::Integer(1));
    assert_eq!(table.scan()[1].values[0], SqlValue::Integer(2));
    assert_eq!(table.scan()[2].values[0], SqlValue::Integer(3));

    // TRUNCATE
    let truncate = TruncateTableStmt {
        table_names: vec!["auto_inc_test".to_string()],
        if_exists: false,
        cascade: None,
    };
    TruncateTableExecutor::execute(&truncate, &mut db).unwrap();

    // Insert new row - should get id = 1, not 4
    let insert = InsertStmt {
        table_name: "auto_inc_test".to_string(),
        columns: vec!["data".to_string()],
        source: InsertSource::Values(vec![vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("d")))]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).unwrap();

    // Verify id was reset to 1
    let table = db.get_table("auto_inc_test").unwrap();
    assert_eq!(table.scan().len(), 1);
    assert_eq!(
        table.scan()[0].values[0],
        SqlValue::Integer(1),
        "AUTO_INCREMENT should reset to 1 after TRUNCATE"
    );
}

#[test]
fn test_truncate_resets_auto_increment_multiple_inserts() {
    use crate::InsertExecutor;
    use vibesql_ast::{
        ColumnConstraint, ColumnConstraintKind, Expression, InsertSource, InsertStmt,
    };

    let mut db = Database::new();

    // Create table with AUTO_INCREMENT
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "multi_test".to_string(),
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
                name: "value".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&stmt, &mut db).unwrap();

    // Insert 10 rows to get counter up to 10
    for i in 1..=10 {
        let insert = InsertStmt {
            table_name: "multi_test".to_string(),
            columns: vec!["value".to_string()],
            source: InsertSource::Values(vec![vec![Expression::Literal(SqlValue::Integer(
                i * 100,
            ))]]),
            conflict_clause: None,
            on_duplicate_key_update: None,
        };
        InsertExecutor::execute(&mut db, &insert).unwrap();
    }

    // Verify last id is 10
    let table = db.get_table("multi_test").unwrap();
    assert_eq!(table.scan().len(), 10);
    assert_eq!(table.scan().last().unwrap().values[0], SqlValue::Integer(10));

    // TRUNCATE
    let truncate = TruncateTableStmt {
        table_names: vec!["multi_test".to_string()],
        if_exists: false,
        cascade: None,
    };
    TruncateTableExecutor::execute(&truncate, &mut db).unwrap();

    // Insert new row - should get id = 1, not 11
    let insert = InsertStmt {
        table_name: "multi_test".to_string(),
        columns: vec!["value".to_string()],
        source: InsertSource::Values(vec![vec![Expression::Literal(SqlValue::Integer(9999))]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(&mut db, &insert).unwrap();

    // Verify id was reset to 1
    let table = db.get_table("multi_test").unwrap();
    assert_eq!(table.scan().len(), 1);
    assert_eq!(
        table.scan()[0].values[0],
        SqlValue::Integer(1),
        "AUTO_INCREMENT should reset to 1 after TRUNCATE, not continue from 10"
    );
}

#[test]
fn test_truncate_without_auto_increment() {
    // Verify that TRUNCATE still works on tables without AUTO_INCREMENT
    let mut db = Database::new();

    let create_stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "no_auto_inc".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "data".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert test data
    db.insert_row(
        "no_auto_inc",
        Row::new(vec![SqlValue::Integer(100), SqlValue::Varchar(arcstr::ArcStr::from("test"))]),
    )
    .unwrap();

    // TRUNCATE
    let stmt = TruncateTableStmt {
        table_names: vec!["no_auto_inc".to_string()],
        if_exists: false,
        cascade: None,
    };
    let result = TruncateTableExecutor::execute(&stmt, &mut db);

    // Should succeed without errors
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
    assert_eq!(db.get_table("no_auto_inc").unwrap().row_count(), 0);
}
