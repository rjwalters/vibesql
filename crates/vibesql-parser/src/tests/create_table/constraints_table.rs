use super::super::*;

// ========================================================================
// Constraint Tests - Table-level constraints
// ========================================================================

#[test]
fn test_parse_create_table_with_table_level_primary_key() {
    let result = Parser::parse_sql(
        "CREATE TABLE order_items (
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            PRIMARY KEY (order_id, product_id)
        );",
    );
    assert!(result.is_ok(), "Should parse table-level PRIMARY KEY");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                vibesql_ast::TableConstraint {
                    kind: vibesql_ast::TableConstraintKind::PrimaryKey { columns },
                    ..
                } => {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(columns[0].column_name, "ORDER_ID");
                    assert_eq!(columns[1].column_name, "PRODUCT_ID");
                }
                _ => panic!("Expected PRIMARY KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_foreign_key() {
    let result = Parser::parse_sql(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        );",
    );
    assert!(result.is_ok(), "Should parse FOREIGN KEY constraint");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                vibesql_ast::TableConstraint {
                    kind:
                        vibesql_ast::TableConstraintKind::ForeignKey {
                            columns,
                            references_table,
                            references_columns,
                            on_delete,
                            on_update,
                        },
                    ..
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0], "CUSTOMER_ID");
                    assert_eq!(references_table, "CUSTOMERS");
                    assert_eq!(references_columns.len(), 1);
                    assert_eq!(references_columns[0], "ID");
                    assert!(on_delete.is_none());
                    assert!(on_update.is_none());
                }
                _ => panic!("Expected FOREIGN KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_foreign_key_on_delete_update() {
    let result = Parser::parse_sql(
        "CREATE TABLE child (
            id INT PRIMARY KEY,
            parent_id INT REFERENCES parent(id) ON DELETE CASCADE ON UPDATE SET NULL
        );",
    );
    assert!(result.is_ok(), "Should parse FOREIGN KEY with ON DELETE/UPDATE");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            // Find the REFERENCES constraint in column constraints
            let column = &create.columns[1]; // parent_id column
            assert_eq!(column.constraints.len(), 1);
            match &column.constraints[0] {
                vibesql_ast::ColumnConstraint {
                    kind:
                        vibesql_ast::ColumnConstraintKind::References {
                            table,
                            column: col,
                            on_delete,
                            on_update,
                        },
                    ..
                } => {
                    assert_eq!(table, "PARENT");
                    assert_eq!(col, "ID");
                    assert_eq!(on_delete, &Some(vibesql_ast::ReferentialAction::Cascade));
                    assert_eq!(on_update, &Some(vibesql_ast::ReferentialAction::SetNull));
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_table_foreign_key_on_delete_update() {
    let result = Parser::parse_sql(
        "CREATE TABLE orders (
            id INT PRIMARY KEY,
            customer_id INT,
            FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE NO ACTION ON UPDATE SET DEFAULT
        );",
    );
    assert!(result.is_ok(), "Should parse table-level FOREIGN KEY with ON DELETE/UPDATE");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                vibesql_ast::TableConstraint {
                    kind:
                        vibesql_ast::TableConstraintKind::ForeignKey {
                            columns,
                            references_table,
                            references_columns,
                            on_delete,
                            on_update,
                        },
                    ..
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0], "CUSTOMER_ID");
                    assert_eq!(references_table, "CUSTOMERS");
                    assert_eq!(references_columns.len(), 1);
                    assert_eq!(references_columns[0], "ID");
                    assert_eq!(on_delete, &Some(vibesql_ast::ReferentialAction::NoAction));
                    assert_eq!(on_update, &Some(vibesql_ast::ReferentialAction::SetDefault));
                }
                _ => panic!("Expected FOREIGN KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_foreign_key_on_delete_only() {
    let result = Parser::parse_sql(
        "CREATE TABLE child (
            id INT PRIMARY KEY,
            parent_id INT REFERENCES parent(id) ON DELETE SET DEFAULT
        );",
    );
    assert!(result.is_ok(), "Should parse FOREIGN KEY with ON DELETE only");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            let column = &create.columns[1];
            match &column.constraints[0] {
                vibesql_ast::ColumnConstraint {
                    kind: vibesql_ast::ColumnConstraintKind::References { on_delete, on_update, .. },
                    ..
                } => {
                    assert_eq!(on_delete, &Some(vibesql_ast::ReferentialAction::SetDefault));
                    assert!(on_update.is_none());
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_table_level_unique() {
    let result = Parser::parse_sql(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email VARCHAR(100),
            username VARCHAR(50),
            UNIQUE (email, username)
        );",
    );
    assert!(result.is_ok(), "Should parse table-level UNIQUE constraint");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                vibesql_ast::TableConstraint {
                    kind: vibesql_ast::TableConstraintKind::Unique { columns },
                    ..
                } => {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(columns[0].column_name, "EMAIL");
                    assert_eq!(columns[1].column_name, "USERNAME");
                }
                _ => panic!("Expected UNIQUE constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_table_level_check() {
    let result = Parser::parse_sql(
        "CREATE TABLE products (
            price NUMERIC(10, 2),
            discount NUMERIC(10, 2),
            CHECK (discount < price)
        );",
    );
    assert!(result.is_ok(), "Should parse table-level CHECK constraint");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            assert!(matches!(
                create.table_constraints[0],
                vibesql_ast::TableConstraint {
                    kind: vibesql_ast::TableConstraintKind::Check { .. },
                    ..
                }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_indexed_column_prefix() {
    // Test from issue #1620: MySQL indexed column prefix syntax
    let result = Parser::parse_sql("CREATE TABLE t7(a TEXT, UNIQUE (a(1)))");
    assert!(result.is_ok(), "Should parse UNIQUE with column prefix: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "T7");
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                vibesql_ast::TableConstraint {
                    kind: vibesql_ast::TableConstraintKind::Unique { columns },
                    ..
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0].column_name, "A");
                    assert_eq!(columns[0].prefix_length, Some(1));
                }
                _ => panic!("Expected UNIQUE constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_primary_key_prefix() {
    let result = Parser::parse_sql("CREATE TABLE t8(name VARCHAR(100), PRIMARY KEY (name(50)))");
    assert!(result.is_ok(), "Should parse PRIMARY KEY with column prefix: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                vibesql_ast::TableConstraint {
                    kind: vibesql_ast::TableConstraintKind::PrimaryKey { columns },
                    ..
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0].column_name, "NAME");
                    assert_eq!(columns[0].prefix_length, Some(50));
                }
                _ => panic!("Expected PRIMARY KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_index_with_column_prefix() {
    let result = Parser::parse_sql("CREATE INDEX idx1 ON users (email(50))");
    assert!(result.is_ok(), "Should parse CREATE INDEX with column prefix: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateIndex(create_idx) => {
            assert_eq!(create_idx.index_name, "IDX1");
            assert_eq!(create_idx.table_name, "USERS");
            assert_eq!(create_idx.columns.len(), 1);
            assert_eq!(create_idx.columns[0].column_name, "EMAIL");
            assert_eq!(create_idx.columns[0].prefix_length, Some(50));
        }
        _ => panic!("Expected CREATE INDEX statement"),
    }
}

// ========================================================================
// Prefix Length Validation Tests
// ========================================================================

#[test]
fn test_parse_unique_constraint_prefix_zero_fails() {
    let result = Parser::parse_sql("CREATE TABLE t1(a TEXT, UNIQUE (a(0)))");
    assert!(result.is_err(), "Should reject prefix length of 0");
    let err = result.unwrap_err();
    assert!(
        err.message.contains("Prefix length must be at least 1"),
        "Expected minimum validation error, got: {}",
        err.message
    );
}

#[test]
fn test_parse_unique_constraint_prefix_too_large_fails() {
    let result = Parser::parse_sql("CREATE TABLE t1(a TEXT, UNIQUE (a(10001)))");
    assert!(result.is_err(), "Should reject prefix length > 10000");
    let err = result.unwrap_err();
    assert!(
        err.message.contains("Prefix length must not exceed 10000"),
        "Expected maximum validation error, got: {}",
        err.message
    );
}

#[test]
fn test_parse_primary_key_prefix_zero_fails() {
    let result = Parser::parse_sql("CREATE TABLE t1(a TEXT, PRIMARY KEY (a(0)))");
    assert!(result.is_err(), "Should reject prefix length of 0");
    let err = result.unwrap_err();
    assert!(
        err.message.contains("Prefix length must be at least 1"),
        "Expected minimum validation error, got: {}",
        err.message
    );
}

#[test]
fn test_parse_primary_key_prefix_too_large_fails() {
    let result = Parser::parse_sql("CREATE TABLE t1(a TEXT, PRIMARY KEY (a(99999999)))");
    assert!(result.is_err(), "Should reject prefix length > 10000");
    let err = result.unwrap_err();
    assert!(
        err.message.contains("Prefix length must not exceed 10000"),
        "Expected maximum validation error, got: {}",
        err.message
    );
}

#[test]
fn test_parse_create_index_prefix_zero_fails() {
    let result = Parser::parse_sql("CREATE INDEX idx1 ON users (email(0))");
    assert!(result.is_err(), "Should reject prefix length of 0");
    let err = result.unwrap_err();
    assert!(
        err.message.contains("length cannot be 0"),
        "Expected zero length validation error, got: {}",
        err.message
    );
}

#[test]
fn test_parse_create_index_prefix_too_large_fails() {
    let result = Parser::parse_sql("CREATE INDEX idx1 ON users (email(10001))");
    assert!(result.is_err(), "Should reject prefix length > 10000");
    let err = result.unwrap_err();
    assert!(
        err.message.contains("too long") || err.message.contains("exceed"),
        "Expected max length validation error, got: {}",
        err.message
    );
}

#[test]
fn test_parse_unique_constraint_prefix_boundary_values() {
    // Test minimum valid value: 1
    let result = Parser::parse_sql("CREATE TABLE t1(a TEXT, UNIQUE (a(1)))");
    assert!(result.is_ok(), "Should accept prefix length of 1: {:?}", result.err());

    // Test maximum valid value: 10000
    let result = Parser::parse_sql("CREATE TABLE t2(a TEXT, UNIQUE (a(10000)))");
    assert!(result.is_ok(), "Should accept prefix length of 10000: {:?}", result.err());
}
