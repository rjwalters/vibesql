use vibesql_ast::{
    ColumnConstraint, ColumnConstraintKind, ColumnDef, CreateTableStmt, Expression,
    TableConstraint, TableConstraintKind,
};
use vibesql_storage::Database;
use vibesql_types::DataType;

use crate::{create_table::CreateTableExecutor, errors::ExecutorError};

#[test]
fn test_create_table_with_column_primary_key() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: true, // This should be overridden by the PK constraint
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::PrimaryKey { on_conflict: None },
                }],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("users").unwrap();
    assert_eq!(schema.primary_key, Some(vec!["id".to_string()]));
    assert!(!schema.get_column("id").unwrap().nullable); // PKs are implicitly NOT NULL
}

#[test]
fn test_create_table_with_table_primary_key() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: true, // This should be overridden
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
            ColumnDef {
                name: "tenant_id".to_string(),
                data_type: DataType::Integer,
                nullable: true, // This should be overridden
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
        ],
        table_constraints: vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![
                    vibesql_ast::IndexColumn::Column {
                        column_name: "id".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                    vibesql_ast::IndexColumn::Column {
                        column_name: "tenant_id".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                ],
                on_conflict: None,
            },
        }],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("users").unwrap();
    assert_eq!(schema.primary_key, Some(vec!["id".to_string(), "tenant_id".to_string()]));
    assert!(!schema.get_column("id").unwrap().nullable);
    assert!(!schema.get_column("tenant_id").unwrap().nullable);
}

#[test]
fn test_create_table_with_multiple_primary_keys_fails() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![ColumnDef {
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
        }],
        table_constraints: vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![vibesql_ast::IndexColumn::Column {
                    column_name: "id".to_string(),
                    direction: vibesql_ast::OrderDirection::Asc,
                    prefix_length: None,
                }],
                on_conflict: None,
            },
        }],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(matches!(result, Err(ExecutorError::MultiplePrimaryKeys)));
}

#[test]
fn test_create_table_with_column_unique_constraint() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: false,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::Unique { on_conflict: None },
                }],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("users").unwrap();
    assert_eq!(schema.unique_constraints.len(), 1);
    assert_eq!(schema.unique_constraints[0], vec!["email".to_string()]);
}

#[test]
fn test_create_table_with_table_unique_constraint() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "first_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
            ColumnDef {
                name: "last_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
        ],
        table_constraints: vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::Unique {
                columns: vec![
                    vibesql_ast::IndexColumn::Column {
                        column_name: "first_name".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                    vibesql_ast::IndexColumn::Column {
                        column_name: "last_name".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                ],
                on_conflict: None,
            },
        }],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("users").unwrap();
    assert_eq!(schema.unique_constraints.len(), 1);
    assert_eq!(
        schema.unique_constraints[0],
        vec!["first_name".to_string(), "last_name".to_string()]
    );
}

#[test]
fn test_create_table_with_check_constraint() {
    let mut db = Database::new();
    let check_expr = Expression::BinaryOp {
        left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
            "price", false,
        ))),
        op: vibesql_ast::BinaryOperator::GreaterThan,
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(0))),
    };

    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "products".to_string(),
        columns: vec![ColumnDef {
            name: "price".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![ColumnConstraint {
                name: None,
                kind: ColumnConstraintKind::Check(Box::new(check_expr.clone())),
            }],
            default_value: None,
            comment: None,
            generated_expr: None,
        }],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("products").unwrap();
    assert_eq!(schema.check_constraints.len(), 1);
    assert_eq!(schema.check_constraints[0].1, check_expr);
}

// ============================================================================
// Auto-Index Creation Tests
// Issue #3202: Auto-create indexes for PRIMARY KEY and UNIQUE constraints
// ============================================================================

#[test]
fn test_auto_index_for_single_column_primary_key() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "t1".to_string(),
        columns: vec![ColumnDef {
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
        }],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Verify sqlite_autoindex_t1_1 index was auto-created (SQLite naming convention)
    assert!(db.index_exists("sqlite_autoindex_t1_1"), "Expected sqlite_autoindex_t1_1 index to be auto-created");

    // Verify index metadata in catalog
    let index_meta = db.catalog.get_index("t1", "sqlite_autoindex_t1_1");
    assert!(index_meta.is_some(), "Expected sqlite_autoindex_t1_1 in catalog");
    let index_meta = index_meta.unwrap();
    assert_eq!(index_meta.table_name, "t1");
    assert!(index_meta.is_unique);
    assert_eq!(index_meta.columns.len(), 1);
    assert_eq!(index_meta.columns[0].column_name, "id");
}

#[test]
fn test_auto_index_for_composite_primary_key() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "t2".to_string(),
        columns: vec![
            ColumnDef {
                name: "a".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
            ColumnDef {
                name: "b".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
        ],
        table_constraints: vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![
                    vibesql_ast::IndexColumn::Column {
                        column_name: "a".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                    vibesql_ast::IndexColumn::Column {
                        column_name: "b".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                ],
                on_conflict: None,
            },
        }],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Verify sqlite_autoindex_t2_1 index was auto-created (SQLite naming convention)
    assert!(db.index_exists("sqlite_autoindex_t2_1"), "Expected sqlite_autoindex_t2_1 index to be auto-created");

    // Verify index has both columns
    let index_meta = db.catalog.get_index("t2", "sqlite_autoindex_t2_1").unwrap();
    assert_eq!(index_meta.columns.len(), 2);
    assert_eq!(index_meta.columns[0].column_name, "a");
    assert_eq!(index_meta.columns[1].column_name, "b");
}

#[test]
fn test_auto_index_for_single_unique_constraint() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "t3".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Varchar { max_length: Some(255) },
                nullable: false,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::Unique { on_conflict: None },
                }],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Verify sqlite_autoindex_t3_1 index was auto-created (SQLite naming convention)
    assert!(db.index_exists("sqlite_autoindex_t3_1"), "Expected sqlite_autoindex_t3_1 index to be auto-created");

    // Verify index metadata
    let index_meta = db.catalog.get_index("t3", "sqlite_autoindex_t3_1").unwrap();
    assert_eq!(index_meta.table_name, "t3");
    assert!(index_meta.is_unique);
    assert_eq!(index_meta.columns.len(), 1);
    assert_eq!(index_meta.columns[0].column_name, "email");
}

#[test]
fn test_auto_index_for_multiple_unique_constraints() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "t4".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Varchar { max_length: Some(255) },
                nullable: false,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::Unique { on_conflict: None },
                }],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
            ColumnDef {
                name: "phone".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                nullable: false,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::Unique { on_conflict: None },
                }],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Verify both unique indexes were auto-created (SQLite naming convention)
    assert!(db.index_exists("sqlite_autoindex_t4_1"), "Expected sqlite_autoindex_t4_1 index to be auto-created");
    assert!(db.index_exists("sqlite_autoindex_t4_2"), "Expected sqlite_autoindex_t4_2 index to be auto-created");
}

#[test]
fn test_auto_index_for_composite_unique_constraint() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "t5".to_string(),
        columns: vec![
            ColumnDef {
                name: "a".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
            ColumnDef {
                name: "b".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
        ],
        table_constraints: vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::Unique {
                columns: vec![
                    vibesql_ast::IndexColumn::Column {
                        column_name: "a".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                    vibesql_ast::IndexColumn::Column {
                        column_name: "b".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                ],
                on_conflict: None,
            },
        }],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Verify sqlite_autoindex_t5_1 index was auto-created (SQLite naming convention)
    assert!(db.index_exists("sqlite_autoindex_t5_1"), "Expected sqlite_autoindex_t5_1 index to be auto-created");

    // Verify index has both columns
    let index_meta = db.catalog.get_index("t5", "sqlite_autoindex_t5_1").unwrap();
    assert_eq!(index_meta.columns.len(), 2);
    assert_eq!(index_meta.columns[0].column_name, "a");
    assert_eq!(index_meta.columns[1].column_name, "b");
}

#[test]
fn test_auto_index_for_primary_key_plus_unique() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "t6".to_string(),
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
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Varchar { max_length: Some(255) },
                nullable: false,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::Unique { on_conflict: None },
                }],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Verify both indexes were auto-created
    // PK gets _1, UNIQUE gets _2 (SQLite naming convention)
    assert!(db.index_exists("sqlite_autoindex_t6_1"), "Expected sqlite_autoindex_t6_1 (PK) index to be auto-created");
    assert!(db.index_exists("sqlite_autoindex_t6_2"), "Expected sqlite_autoindex_t6_2 (UNIQUE) index to be auto-created");
}

#[test]
fn test_auto_index_visible_in_catalog() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        if_not_exists: false,
        table_name: "users".to_string(),
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
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Varchar { max_length: Some(255) },
                nullable: false,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::Unique { on_conflict: None },
                }],
                default_value: None,
                comment: None,
                generated_expr: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
        quoted: false,
        as_query: None,
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    // Both indexes should be visible in the catalog
    let indexes = db.catalog.get_table_indexes("users");
    assert_eq!(indexes.len(), 2, "Expected 2 indexes for users table");

    // Index names should follow SQLite naming convention
    let index_names: Vec<&str> = indexes.iter().map(|i| i.name.as_str()).collect();
    assert!(index_names.contains(&"sqlite_autoindex_users_1"), "sqlite_autoindex_users_1 (PK) should be in catalog");
    assert!(index_names.contains(&"sqlite_autoindex_users_2"), "sqlite_autoindex_users_2 (UNIQUE) should be in catalog");
}
