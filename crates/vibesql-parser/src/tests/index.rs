//! Tests for INDEX DDL statements (CREATE INDEX, DROP INDEX)

use vibesql_ast::Statement;

use crate::Parser;

#[test]
fn test_create_index_simple() {
    let sql = "CREATE INDEX idx ON users(email)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx");
            assert_eq!(stmt.table_name, "users");
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => assert!(!unique),
                other => panic!("Expected BTree index, got: {:?}", other),
            }
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0].column_name, "email");
            assert_eq!(stmt.columns[0].direction, vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_unique_index() {
    let sql = "CREATE UNIQUE INDEX idx ON users(email)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => {
                    assert!(*unique, "Expected unique=true")
                }
                other => panic!("Expected BTree index, got: {:?}", other),
            }
            assert_eq!(stmt.index_name, "idx");
            assert_eq!(stmt.table_name, "users");
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0].column_name, "email");
            assert_eq!(stmt.columns[0].direction, vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_multi_column() {
    let sql = "CREATE INDEX idx ON users(first_name, last_name, email)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx");
            assert_eq!(stmt.table_name, "users");
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => assert!(!unique),
                other => panic!("Expected BTree index, got: {:?}", other),
            }
            assert_eq!(stmt.columns.len(), 3);
            assert_eq!(stmt.columns[0].column_name, "first_name");
            assert_eq!(stmt.columns[0].direction, vibesql_ast::OrderDirection::Asc);
            assert_eq!(stmt.columns[1].column_name, "last_name");
            assert_eq!(stmt.columns[1].direction, vibesql_ast::OrderDirection::Asc);
            assert_eq!(stmt.columns[2].column_name, "email");
            assert_eq!(stmt.columns[2].direction, vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_unique_index_multi_column() {
    let sql = "CREATE UNIQUE INDEX idx ON orders(customer_id, order_date)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => assert!(*unique),
                other => panic!("Expected BTree index, got: {:?}", other),
            }
            assert_eq!(stmt.index_name, "idx");
            assert_eq!(stmt.table_name, "orders");
            assert_eq!(stmt.columns.len(), 2);
            assert_eq!(stmt.columns[0].column_name, "customer_id");
            assert_eq!(stmt.columns[0].direction, vibesql_ast::OrderDirection::Asc);
            assert_eq!(stmt.columns[1].column_name, "order_date");
            assert_eq!(stmt.columns[1].direction, vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_mixed_case_identifiers() {
    let sql = "CREATE INDEX MyIndex ON MyTable(MyColumn)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "myindex");
            assert_eq!(stmt.table_name, "mytable");
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0].column_name, "mycolumn");
            assert_eq!(stmt.columns[0].direction, vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_single_column() {
    let sql = "CREATE INDEX pk ON users(id)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "pk");
            assert_eq!(stmt.table_name, "users");
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0].column_name, "id");
            assert_eq!(stmt.columns[0].direction, vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_index_with_desc() {
    let sql = "CREATE INDEX idx ON users(email DESC, created_at ASC)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx");
            assert_eq!(stmt.table_name, "users");
            match &stmt.index_type {
                vibesql_ast::IndexType::BTree { unique } => assert!(!unique),
                other => panic!("Expected BTree index, got: {:?}", other),
            }
            assert_eq!(stmt.columns.len(), 2);
            assert_eq!(stmt.columns[0].column_name, "email");
            assert_eq!(stmt.columns[0].direction, vibesql_ast::OrderDirection::Desc);
            assert_eq!(stmt.columns[1].column_name, "created_at");
            assert_eq!(stmt.columns[1].direction, vibesql_ast::OrderDirection::Asc);
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_drop_index_simple() {
    let sql = "DROP INDEX idx";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::DropIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx");
        }
        other => panic!("Expected DropIndex, got: {:?}", other),
    }
}

#[test]
fn test_drop_index_mixed_case() {
    let sql = "DROP INDEX MyIndex";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::DropIndex(stmt) => {
            assert_eq!(stmt.index_name, "myindex");
        }
        other => panic!("Expected DropIndex, got: {:?}", other),
    }
}

// Error cases

#[test]
fn test_create_index_missing_index_name() {
    let sql = "CREATE INDEX ON table(col)";
    assert!(Parser::parse_sql(sql).is_err());
}

#[test]
fn test_create_index_missing_table_name() {
    let sql = "CREATE INDEX idx (col)";
    assert!(Parser::parse_sql(sql).is_err());
}

#[test]
fn test_create_index_empty_column_list() {
    let sql = "CREATE INDEX idx ON table()";
    assert!(Parser::parse_sql(sql).is_err());
}

#[test]
fn test_drop_index_missing_index_name() {
    let sql = "DROP INDEX";
    assert!(Parser::parse_sql(sql).is_err());
}

#[test]
fn test_create_spatial_index() {
    let sql = "CREATE SPATIAL INDEX idx_location ON places(geom)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert_eq!(stmt.index_name, "idx_location");
            assert_eq!(stmt.table_name, "places");
            assert!(
                matches!(stmt.index_type, vibesql_ast::IndexType::Spatial),
                "Expected Spatial index, got: {:?}",
                stmt.index_type
            );
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0].column_name, "geom");
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}

#[test]
fn test_create_spatial_index_if_not_exists() {
    let sql = "CREATE SPATIAL INDEX IF NOT EXISTS idx_boundary ON parcels(boundary)";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    match result.unwrap() {
        Statement::CreateIndex(stmt) => {
            assert!(stmt.if_not_exists);
            assert_eq!(stmt.index_name, "idx_boundary");
            assert_eq!(stmt.table_name, "parcels");
            assert!(matches!(stmt.index_type, vibesql_ast::IndexType::Spatial));
            assert_eq!(stmt.columns.len(), 1);
            assert_eq!(stmt.columns[0].column_name, "boundary");
        }
        other => panic!("Expected CreateIndex, got: {:?}", other),
    }
}
