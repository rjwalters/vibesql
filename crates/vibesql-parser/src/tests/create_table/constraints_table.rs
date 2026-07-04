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
                    kind: vibesql_ast::TableConstraintKind::PrimaryKey { columns, .. },
                    ..
                } => {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(columns[0].expect_column_name(), "order_id");
                    assert_eq!(columns[1].expect_column_name(), "product_id");
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
                            ..
                        },
                    ..
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0], "customer_id");
                    assert_eq!(references_table, "customers");
                    assert_eq!(references_columns.len(), 1);
                    assert_eq!(references_columns[0], "id");
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
                            ..
                        },
                    ..
                } => {
                    assert_eq!(table, "parent");
                    assert_eq!(col, &Some("id".to_string()));
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
                            ..
                        },
                    ..
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0], "customer_id");
                    assert_eq!(references_table, "customers");
                    assert_eq!(references_columns.len(), 1);
                    assert_eq!(references_columns[0], "id");
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
                    kind: vibesql_ast::TableConstraintKind::Unique { columns, .. },
                    ..
                } => {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(columns[0].expect_column_name(), "email");
                    assert_eq!(columns[1].expect_column_name(), "username");
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
            assert_eq!(create.table_name, "t7");
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0] {
                vibesql_ast::TableConstraint {
                    kind: vibesql_ast::TableConstraintKind::Unique { columns, .. },
                    ..
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0].expect_column_name(), "a");
                    assert_eq!(columns[0].prefix_length(), Some(1));
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
                    kind: vibesql_ast::TableConstraintKind::PrimaryKey { columns, .. },
                    ..
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0].expect_column_name(), "name");
                    assert_eq!(columns[0].prefix_length(), Some(50));
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
            assert_eq!(create_idx.index_name, "idx1");
            assert_eq!(create_idx.table_name, "users");
            assert_eq!(create_idx.columns.len(), 1);
            assert_eq!(create_idx.columns[0].expect_column_name(), "email");
            assert_eq!(create_idx.columns[0].prefix_length(), Some(50));
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

/// Test ON CONFLICT clause for table-level constraints (SQLite extension)
#[test]
fn test_parse_on_conflict_table_constraints() {
    // Test table-level PRIMARY KEY with ON CONFLICT IGNORE
    let result =
        Parser::parse_sql("CREATE TABLE t1(a INT, b INT, PRIMARY KEY (a, b) ON CONFLICT IGNORE);");
    assert!(result.is_ok(), "Should parse table-level PRIMARY KEY with ON CONFLICT");
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0].kind {
                vibesql_ast::TableConstraintKind::PrimaryKey { columns, on_conflict } => {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(*on_conflict, Some(vibesql_ast::ConflictClause::Ignore));
                }
                _ => panic!("Expected PrimaryKey constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }

    // Test table-level UNIQUE with ON CONFLICT REPLACE
    let result = Parser::parse_sql(
        "CREATE TABLE t2(email TEXT, code TEXT, UNIQUE (email) ON CONFLICT REPLACE);",
    );
    assert!(result.is_ok(), "Should parse table-level UNIQUE with ON CONFLICT");
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0].kind {
                vibesql_ast::TableConstraintKind::Unique { columns, on_conflict } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(*on_conflict, Some(vibesql_ast::ConflictClause::Replace));
                }
                _ => panic!("Expected Unique constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }

    // Test table-level UNIQUE with ON CONFLICT ABORT
    let result =
        Parser::parse_sql("CREATE TABLE t3(a INT, b INT, UNIQUE (a, b) ON CONFLICT ABORT);");
    assert!(result.is_ok(), "Should parse table-level UNIQUE with ON CONFLICT ABORT");
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => match &create.table_constraints[0].kind {
            vibesql_ast::TableConstraintKind::Unique { on_conflict, .. } => {
                assert_eq!(*on_conflict, Some(vibesql_ast::ConflictClause::Abort));
            }
            _ => panic!("Expected Unique constraint"),
        },
        _ => panic!("Expected CREATE TABLE statement"),
    }

    // Test table-level PRIMARY KEY with ON CONFLICT FAIL
    let result = Parser::parse_sql("CREATE TABLE t4(id INT, PRIMARY KEY (id) ON CONFLICT FAIL);");
    assert!(result.is_ok(), "Should parse table-level PRIMARY KEY with ON CONFLICT FAIL");
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => match &create.table_constraints[0].kind {
            vibesql_ast::TableConstraintKind::PrimaryKey { on_conflict, .. } => {
                assert_eq!(*on_conflict, Some(vibesql_ast::ConflictClause::Fail));
            }
            _ => panic!("Expected PrimaryKey constraint"),
        },
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// DEFERRABLE Constraint Tests (Issue #4990) - Table-level FOREIGN KEY
// ========================================================================

/// Test DEFERRABLE constraint on table-level FOREIGN KEY
#[test]
fn test_parse_foreign_key_deferrable() {
    let result =
        Parser::parse_sql("CREATE TABLE t(a INTEGER, FOREIGN KEY(a) REFERENCES p(x) DEFERRABLE)");
    assert!(result.is_ok(), "Should parse FOREIGN KEY with DEFERRABLE: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0].kind {
                vibesql_ast::TableConstraintKind::ForeignKey {
                    columns,
                    references_table,
                    references_columns,
                    deferral,
                    ..
                } => {
                    assert_eq!(columns, &["a".to_string()]);
                    assert_eq!(references_table, "p");
                    assert_eq!(references_columns, &["x".to_string()]);
                    let deferral = deferral.expect("Should have deferral");
                    assert!(deferral.is_deferrable, "Should be deferrable");
                    assert!(!deferral.initially_deferred, "Should default to INITIALLY IMMEDIATE");
                }
                _ => panic!("Expected FOREIGN KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test NOT DEFERRABLE constraint on table-level FOREIGN KEY
#[test]
fn test_parse_foreign_key_not_deferrable() {
    let result = Parser::parse_sql(
        "CREATE TABLE t(a INTEGER, FOREIGN KEY(a) REFERENCES p(x) NOT DEFERRABLE)",
    );
    assert!(result.is_ok(), "Should parse FOREIGN KEY with NOT DEFERRABLE: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => match &create.table_constraints[0].kind {
            vibesql_ast::TableConstraintKind::ForeignKey { deferral, .. } => {
                let deferral = deferral.expect("Should have deferral");
                assert!(!deferral.is_deferrable, "Should NOT be deferrable");
            }
            _ => panic!("Expected FOREIGN KEY constraint"),
        },
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test DEFERRABLE INITIALLY DEFERRED on table-level FOREIGN KEY
#[test]
fn test_parse_foreign_key_deferrable_initially_deferred() {
    let result = Parser::parse_sql(
        "CREATE TABLE t(a INTEGER, FOREIGN KEY(a) REFERENCES p(x) DEFERRABLE INITIALLY DEFERRED)",
    );
    assert!(result.is_ok(), "Should parse DEFERRABLE INITIALLY DEFERRED: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => match &create.table_constraints[0].kind {
            vibesql_ast::TableConstraintKind::ForeignKey { deferral, .. } => {
                let deferral = deferral.expect("Should have deferral");
                assert!(deferral.is_deferrable, "Should be deferrable");
                assert!(deferral.initially_deferred, "Should be INITIALLY DEFERRED");
            }
            _ => panic!("Expected FOREIGN KEY constraint"),
        },
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test DEFERRABLE INITIALLY IMMEDIATE on table-level FOREIGN KEY
#[test]
fn test_parse_foreign_key_deferrable_initially_immediate() {
    let result = Parser::parse_sql(
        "CREATE TABLE t(a INTEGER, FOREIGN KEY(a) REFERENCES p(x) DEFERRABLE INITIALLY IMMEDIATE)",
    );
    assert!(result.is_ok(), "Should parse DEFERRABLE INITIALLY IMMEDIATE: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => match &create.table_constraints[0].kind {
            vibesql_ast::TableConstraintKind::ForeignKey { deferral, .. } => {
                let deferral = deferral.expect("Should have deferral");
                assert!(deferral.is_deferrable, "Should be deferrable");
                assert!(!deferral.initially_deferred, "Should be INITIALLY IMMEDIATE");
            }
            _ => panic!("Expected FOREIGN KEY constraint"),
        },
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test DEFERRABLE with ON DELETE/UPDATE actions on table-level FOREIGN KEY
#[test]
fn test_parse_foreign_key_deferrable_with_actions() {
    let result = Parser::parse_sql(
        "CREATE TABLE t(a INTEGER, FOREIGN KEY(a) REFERENCES p(x) ON DELETE CASCADE ON UPDATE SET NULL DEFERRABLE INITIALLY DEFERRED)"
    );
    assert!(
        result.is_ok(),
        "Should parse with ON DELETE/UPDATE and DEFERRABLE: {:?}",
        result.err()
    );
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => match &create.table_constraints[0].kind {
            vibesql_ast::TableConstraintKind::ForeignKey {
                on_delete, on_update, deferral, ..
            } => {
                assert_eq!(on_delete, &Some(vibesql_ast::ReferentialAction::Cascade));
                assert_eq!(on_update, &Some(vibesql_ast::ReferentialAction::SetNull));
                let deferral = deferral.expect("Should have deferral");
                assert!(deferral.is_deferrable);
                assert!(deferral.initially_deferred);
            }
            _ => panic!("Expected FOREIGN KEY constraint"),
        },
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// COLLATE inside table-level PRIMARY KEY / UNIQUE column lists (issue #5796)
//
// SQLite's grammar allows a per-column COLLATE (and ASC/DESC) inside the
// table-constraint key column list, e.g.
//   CREATE TABLE t1(a, b, c, PRIMARY KEY(a COLLATE nocase, a)) WITHOUT ROWID
// (verified against sqlite3 3.51.0; exercised by alterdropcol 7.0-7.3).
// ========================================================================

#[test]
fn test_parse_table_level_primary_key_with_collate() {
    let result = Parser::parse_sql(
        "CREATE TABLE t1(a, b, c, PRIMARY KEY(a COLLATE nocase, a)) WITHOUT ROWID",
    );
    assert!(result.is_ok(), "Should parse PRIMARY KEY(a COLLATE nocase, a): {:?}", result.err());

    match result.unwrap() {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_constraints.len(), 1);
            match &create.table_constraints[0].kind {
                vibesql_ast::TableConstraintKind::PrimaryKey { columns, .. } => {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(columns[0].expect_column_name(), "a");
                    assert_eq!(columns[1].expect_column_name(), "a");
                }
                _ => panic!("Expected PRIMARY KEY constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_table_level_primary_key_with_collate_and_direction() {
    // COLLATE precedes ASC/DESC in SQLite's indexed-column grammar.
    let result =
        Parser::parse_sql("CREATE TABLE t1(a, b, PRIMARY KEY(a COLLATE nocase DESC, b ASC))");
    assert!(result.is_ok(), "Should parse COLLATE followed by DESC: {:?}", result.err());

    match result.unwrap() {
        vibesql_ast::Statement::CreateTable(create) => match &create.table_constraints[0].kind {
            vibesql_ast::TableConstraintKind::PrimaryKey { columns, .. } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].direction(), vibesql_ast::OrderDirection::Desc);
                assert_eq!(columns[1].direction(), vibesql_ast::OrderDirection::Asc);
            }
            _ => panic!("Expected PRIMARY KEY constraint"),
        },
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_table_level_unique_with_collate() {
    // The same indexed-column grammar backs UNIQUE table constraints.
    let result = Parser::parse_sql("CREATE TABLE t1(a, b, UNIQUE(a COLLATE rtrim))");
    assert!(result.is_ok(), "Should parse UNIQUE(a COLLATE rtrim): {:?}", result.err());
}

#[test]
fn test_parse_table_level_primary_key_collate_keyword_collation_name() {
    // BINARY is a keyword but must be accepted as a collation name.
    let result = Parser::parse_sql("CREATE TABLE t1(a, b, PRIMARY KEY(a COLLATE BINARY))");
    assert!(result.is_ok(), "Should accept keyword collation name: {:?}", result.err());
}

#[test]
fn test_parse_table_level_primary_key_collate_missing_name_is_error() {
    let result = Parser::parse_sql("CREATE TABLE t1(a, b, PRIMARY KEY(a COLLATE))");
    assert!(result.is_err(), "COLLATE without a collation name must be a parse error");
}
