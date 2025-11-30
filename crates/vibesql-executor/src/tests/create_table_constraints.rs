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
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: true, // This should be overridden by the PK constraint
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::PrimaryKey,
                }],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "name".to_string(),
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
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: true, // This should be overridden
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "tenant_id".to_string(),
                data_type: DataType::Integer,
                nullable: true, // This should be overridden
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![
                    vibesql_ast::IndexColumn {
                        column_name: "id".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                    vibesql_ast::IndexColumn {
                        column_name: "tenant_id".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                ],
            },
        }],
        table_options: vec![],
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
        table_name: "users".to_string(),
        columns: vec![ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![ColumnConstraint {
                name: None,
                kind: ColumnConstraintKind::PrimaryKey,
            }],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![vibesql_ast::IndexColumn {
                    column_name: "id".to_string(),
                    direction: vibesql_ast::OrderDirection::Asc,
                    prefix_length: None,
                }],
            },
        }],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(matches!(result, Err(ExecutorError::MultiplePrimaryKeys)));
}

#[test]
fn test_create_table_with_column_unique_constraint() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        table_name: "users".to_string(),
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
                name: "email".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: false,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::Unique,
                }],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
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
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "first_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "last_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::Unique {
                columns: vec![
                    vibesql_ast::IndexColumn {
                        column_name: "first_name".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                    vibesql_ast::IndexColumn {
                        column_name: "last_name".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                ],
            },
        }],
        table_options: vec![],
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
        left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
        op: vibesql_ast::BinaryOperator::GreaterThan,
        right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(0))),
    };

    let stmt = CreateTableStmt {
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
        }],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("products").unwrap();
    assert_eq!(schema.check_constraints.len(), 1);
    assert_eq!(schema.check_constraints[0].1, check_expr);
}
