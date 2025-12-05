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
            assert_eq!(create.table_name, "USERS");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name, "ID");
            assert_eq!(create.columns[0].constraints.len(), 1);
            assert!(matches!(
                create.columns[0].constraints[0],
                vibesql_ast::ColumnConstraint {
                    kind: vibesql_ast::ColumnConstraintKind::PrimaryKey,
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
                    kind: vibesql_ast::ColumnConstraintKind::Unique,
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
                        },
                    ..
                } => {
                    assert_eq!(table, "CUSTOMERS");
                    assert_eq!(column, "ID");
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
                    kind: vibesql_ast::ColumnConstraintKind::PrimaryKey,
                    ..
                }
            ));

            // email has UNIQUE
            assert_eq!(create.columns[1].constraints.len(), 1);
            assert!(matches!(
                create.columns[1].constraints[0],
                vibesql_ast::ColumnConstraint {
                    kind: vibesql_ast::ColumnConstraintKind::Unique,
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
            assert_eq!(create.table_name, "T1C857");
            assert_eq!(create.columns.len(), 2);

            // c1 has COMMENT but no KEY
            assert_eq!(create.columns[0].name, "C1");
            assert_eq!(create.columns[0].comment, Some("text667808".to_string()));
            assert_eq!(create.columns[0].constraints.len(), 0);

            // c2 has KEY constraint
            assert_eq!(create.columns[1].name, "C2");
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
