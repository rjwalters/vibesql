use super::super::*;

// ========================================================================
// Constraint Tests (Issue #214) - Column-level constraints
// ========================================================================

#[test]
fn test_parse_create_table_with_primary_key() {
    let result =
        Parser::parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));");
    assert!(result.is_ok(), "Should parse column-level PRIMARY KEY");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "users");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "id");
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                vibesql_ast::ColumnConstraint {
                    kind: vibesql_ast::ColumnConstraintKind::PrimaryKey { on_conflict: None },
                    ..
                }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_unique() {
    let result = Parser::parse_sql("CREATE TABLE users (email VARCHAR(100) UNIQUE);");
    assert!(result.is_ok(), "Should parse UNIQUE constraint");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                vibesql_ast::ColumnConstraint {
                    kind: vibesql_ast::ColumnConstraintKind::Unique { on_conflict: None },
                    ..
                }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_check_constraint() {
    let result =
        Parser::parse_sql("CREATE TABLE products (price NUMERIC(10, 2) CHECK (price > 0));");
    assert!(result.is_ok(), "Should parse CHECK constraint");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                vibesql_ast::ColumnConstraint {
                    kind: vibesql_ast::ColumnConstraintKind::Check(_),
                    ..
                }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_references() {
    let result =
        Parser::parse_sql("CREATE TABLE orders (customer_id INTEGER REFERENCES customers(id));");
    assert!(result.is_ok(), "Should parse REFERENCES constraint");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].constraints.len(), 1);
            match &create.columns[0].constraints[0] {
                vibesql_ast::ColumnConstraint {
                    kind:
                        vibesql_ast::ColumnConstraintKind::References {
                            table,
                            column,
                            on_delete,
                            on_update,
                            ..
                        },
                    ..
                } => {
                    assert_eq!(table, "customers");
                    assert_eq!(column, &Some("id".to_string()));
                    assert!(on_delete.is_none());
                    assert!(on_update.is_none());
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_with_multiple_constraints() {
    let result = Parser::parse_sql(
        "CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            email VARCHAR(100) UNIQUE,
            salary NUMERIC(10, 2) CHECK (salary > 0),
            department_id INTEGER REFERENCES departments(id)
        );",
    );
    assert!(result.is_ok(), "Should parse multiple column constraints");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 4);

            // id has PRIMARY KEY
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                vibesql_ast::ColumnConstraint {
                    kind: vibesql_ast::ColumnConstraintKind::PrimaryKey { on_conflict: None },
                    ..
                }
            ));

            // email has UNIQUE
            assert_eq!(create.columns[1].constraints.len(), 1);
            assert!(matches!(
                create.columns[1].constraints[0],
                vibesql_ast::ColumnConstraint {
                    kind: vibesql_ast::ColumnConstraintKind::Unique { on_conflict: None },
                    ..
                }
            ));

            // salary has CHECK
            assert_eq!(create.columns[2].constraints.len(), 1);
            assert!(matches!(
                create.columns[2].constraints[0],
                vibesql_ast::ColumnConstraint {
                    kind: vibesql_ast::ColumnConstraintKind::Check(_),
                    ..
                }
            ));

            // department_id has REFERENCES
            assert_eq!(create.columns[3].constraints.len(), 1);
            assert!(matches!(
                create.columns[3].constraints[0],
                vibesql_ast::ColumnConstraint {
                    kind: vibesql_ast::ColumnConstraintKind::References { .. },
                    ..
                }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_enum_with_key() {
    // Test for issue #1425: Parser should handle ENUM with KEY constraint
    let result = Parser::parse_sql(
        "CREATE TABLE t1c857 (
            c1 ENUM ('text667805', 'text667806') COMMENT 'text667808',
            c2 ENUM ('0b10000', 'text667809') KEY
        );",
    );
    assert!(result.is_ok(), "Should parse ENUM with KEY constraint");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "t1c857");
            assert_eq!(create.columns.len(), 2);

            // c1 has COMMENT but no KEY
            assert_eq!(create.columns[0].name, "c1");
            assert_eq!(create.columns[0].comment, Some("text667808".to_string()));
            assert_eq!(create.columns[0].constraints.len(), 0);

            // c2 has KEY constraint
            assert_eq!(create.columns[1].name, "c2");
            assert_eq!(create.columns[1].constraints.len(), 1);
            assert!(matches!(
                create.columns[1].constraints[0],
                vibesql_ast::ColumnConstraint { kind: vibesql_ast::ColumnConstraintKind::Key, .. }
            ));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_key_constraint() {
    // Test KEY constraint on various data types
    let result = Parser::parse_sql(
        "CREATE TABLE test_key (
            id INT PRIMARY KEY,
            value VARCHAR(100) KEY,
            status ENUM('active', 'inactive') KEY NOT NULL
        );",
    );
    assert!(result.is_ok(), "Should parse KEY constraint on various types");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 3);

            // value has KEY
            assert!(create.columns[1]
                .constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::Key)));

            // status has KEY and NOT NULL
            assert!(create.columns[2]
                .constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::Key)));
            assert!(create.columns[2]
                .constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::NotNull)));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_parse_create_table_references_without_column() {
    // SQLite allows REFERENCES table without specifying the column
    // The column defaults to the primary key of the referenced table
    let result = Parser::parse_sql(
        "CREATE TABLE track (
            tid INTEGER PRIMARY KEY,
            aid INTEGER NOT NULL REFERENCES album,
            name TEXT
        );",
    );
    assert!(result.is_ok(), "Should parse REFERENCES without column specification");
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.table_name, "track");
            assert_eq!(create.columns.len(), 3);

            // aid has NOT NULL and REFERENCES album (no column specified)
            assert_eq!(create.columns[1].name, "aid");
            assert_eq!(create.columns[1].constraints.len(), 2);

            // Find the REFERENCES constraint
            let refs_constraint = create.columns[1]
                .constraints
                .iter()
                .find(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::References { .. }));

            assert!(refs_constraint.is_some(), "Should have REFERENCES constraint");
            match &refs_constraint.unwrap().kind {
                vibesql_ast::ColumnConstraintKind::References { table, column, .. } => {
                    assert_eq!(table, "album");
                    assert_eq!(column, &None); // Column not specified
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

// ========================================================================
// DEFERRABLE Constraint Tests (Issue #4990) - Column-level
// ========================================================================

/// Test DEFERRABLE constraint on column-level REFERENCES
#[test]
fn test_parse_references_deferrable() {
    let result = Parser::parse_sql("CREATE TABLE t(a INTEGER REFERENCES p(x) DEFERRABLE)");
    assert!(result.is_ok(), "Should parse REFERENCES with DEFERRABLE: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].constraints.len(), 1);
            match &create.columns[0].constraints[0].kind {
                vibesql_ast::ColumnConstraintKind::References {
                    table, column, deferral, ..
                } => {
                    assert_eq!(table, "p");
                    assert_eq!(column, &Some("x".to_string()));
                    let deferral = deferral.expect("Should have deferral");
                    assert!(deferral.is_deferrable, "Should be deferrable");
                    assert!(!deferral.initially_deferred, "Should default to INITIALLY IMMEDIATE");
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test NOT DEFERRABLE constraint on column-level REFERENCES
#[test]
fn test_parse_references_not_deferrable() {
    let result = Parser::parse_sql("CREATE TABLE t(a INTEGER REFERENCES p(x) NOT DEFERRABLE)");
    assert!(result.is_ok(), "Should parse REFERENCES with NOT DEFERRABLE: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match &create.columns[0].constraints[0].kind {
                vibesql_ast::ColumnConstraintKind::References { deferral, .. } => {
                    let deferral = deferral.expect("Should have deferral");
                    assert!(!deferral.is_deferrable, "Should NOT be deferrable");
                    assert!(!deferral.initially_deferred, "initially_deferred should be false");
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test DEFERRABLE INITIALLY DEFERRED on column-level REFERENCES
#[test]
fn test_parse_references_deferrable_initially_deferred() {
    let result = Parser::parse_sql(
        "CREATE TABLE t(a INTEGER REFERENCES p(x) DEFERRABLE INITIALLY DEFERRED)",
    );
    assert!(result.is_ok(), "Should parse DEFERRABLE INITIALLY DEFERRED: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match &create.columns[0].constraints[0].kind {
                vibesql_ast::ColumnConstraintKind::References { deferral, .. } => {
                    let deferral = deferral.expect("Should have deferral");
                    assert!(deferral.is_deferrable, "Should be deferrable");
                    assert!(deferral.initially_deferred, "Should be INITIALLY DEFERRED");
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test DEFERRABLE INITIALLY IMMEDIATE on column-level REFERENCES
#[test]
fn test_parse_references_deferrable_initially_immediate() {
    let result = Parser::parse_sql(
        "CREATE TABLE t(a INTEGER REFERENCES p(x) DEFERRABLE INITIALLY IMMEDIATE)",
    );
    assert!(result.is_ok(), "Should parse DEFERRABLE INITIALLY IMMEDIATE: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match &create.columns[0].constraints[0].kind {
                vibesql_ast::ColumnConstraintKind::References { deferral, .. } => {
                    let deferral = deferral.expect("Should have deferral");
                    assert!(deferral.is_deferrable, "Should be deferrable");
                    assert!(!deferral.initially_deferred, "Should be INITIALLY IMMEDIATE");
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test DEFERRABLE with ON DELETE/UPDATE actions
#[test]
fn test_parse_references_deferrable_with_actions() {
    let result = Parser::parse_sql(
        "CREATE TABLE t(a INTEGER REFERENCES p(x) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)",
    );
    assert!(result.is_ok(), "Should parse with ON DELETE and DEFERRABLE: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match &create.columns[0].constraints[0].kind {
                vibesql_ast::ColumnConstraintKind::References { on_delete, deferral, .. } => {
                    assert_eq!(on_delete, &Some(vibesql_ast::ReferentialAction::Cascade));
                    let deferral = deferral.expect("Should have deferral");
                    assert!(deferral.is_deferrable);
                    assert!(deferral.initially_deferred);
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test error: INITIALLY without DEFERRABLE should fail
/// The INITIALLY keyword is only valid after DEFERRABLE/NOT DEFERRABLE
#[test]
fn test_parse_references_initially_without_deferrable_fails() {
    let result = Parser::parse_sql("CREATE TABLE t(a INTEGER REFERENCES p(x) INITIALLY DEFERRED)");
    assert!(result.is_err(), "Should reject INITIALLY without DEFERRABLE");
}

/// Test that REFERENCES without deferral clause has None deferral
#[test]
fn test_parse_references_no_deferral_clause() {
    let result = Parser::parse_sql("CREATE TABLE t(a INTEGER REFERENCES p(x))");
    assert!(result.is_ok(), "Should parse REFERENCES without deferral: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match &create.columns[0].constraints[0].kind {
                vibesql_ast::ColumnConstraintKind::References { deferral, .. } => {
                    assert!(deferral.is_none(), "Should have no deferral when not specified");
                }
                _ => panic!("Expected REFERENCES constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

/// Test ON CONFLICT clause for column-level constraints (SQLite extension)
#[test]
fn test_parse_on_conflict_column_constraints() {
    // Test PRIMARY KEY with ON CONFLICT
    let result = Parser::parse_sql("CREATE TABLE t1(a INTEGER PRIMARY KEY ON CONFLICT IGNORE);");
    assert!(result.is_ok(), "Should parse PRIMARY KEY with ON CONFLICT");
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].constraints.len(), 1);
            match &create.columns[0].constraints[0].kind {
                vibesql_ast::ColumnConstraintKind::PrimaryKey { on_conflict } => {
                    assert_eq!(*on_conflict, Some(vibesql_ast::ConflictClause::Ignore));
                }
                _ => panic!("Expected PrimaryKey constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }

    // Test UNIQUE with ON CONFLICT REPLACE
    let result = Parser::parse_sql("CREATE TABLE t2(email TEXT UNIQUE ON CONFLICT REPLACE);");
    assert!(result.is_ok(), "Should parse UNIQUE with ON CONFLICT");
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns[0].constraints.len(), 1);
            match &create.columns[0].constraints[0].kind {
                vibesql_ast::ColumnConstraintKind::Unique { on_conflict } => {
                    assert_eq!(*on_conflict, Some(vibesql_ast::ConflictClause::Replace));
                }
                _ => panic!("Expected Unique constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }

    // Test UNIQUE with ON CONFLICT ABORT
    let result = Parser::parse_sql("CREATE TABLE t3(code TEXT UNIQUE ON CONFLICT ABORT);");
    assert!(result.is_ok(), "Should parse UNIQUE with ON CONFLICT ABORT");
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match &create.columns[0].constraints[0].kind {
                vibesql_ast::ColumnConstraintKind::Unique { on_conflict } => {
                    assert_eq!(*on_conflict, Some(vibesql_ast::ConflictClause::Abort));
                }
                _ => panic!("Expected Unique constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }

    // Test PRIMARY KEY with ON CONFLICT ROLLBACK
    let result = Parser::parse_sql("CREATE TABLE t4(id INTEGER PRIMARY KEY ON CONFLICT ROLLBACK);");
    assert!(result.is_ok(), "Should parse PRIMARY KEY with ON CONFLICT ROLLBACK");
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match &create.columns[0].constraints[0].kind {
                vibesql_ast::ColumnConstraintKind::PrimaryKey { on_conflict } => {
                    assert_eq!(*on_conflict, Some(vibesql_ast::ConflictClause::Rollback));
                }
                _ => panic!("Expected PrimaryKey constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }

    // Test UNIQUE with ON CONFLICT FAIL
    let result = Parser::parse_sql("CREATE TABLE t5(name TEXT UNIQUE ON CONFLICT FAIL);");
    assert!(result.is_ok(), "Should parse UNIQUE with ON CONFLICT FAIL");
    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            match &create.columns[0].constraints[0].kind {
                vibesql_ast::ColumnConstraintKind::Unique { on_conflict } => {
                    assert_eq!(*on_conflict, Some(vibesql_ast::ConflictClause::Fail));
                }
                _ => panic!("Expected Unique constraint"),
            }
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
