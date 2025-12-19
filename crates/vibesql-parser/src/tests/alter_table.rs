use super::*;

// ========================================================================
// ALTER TABLE Statement Tests
// ========================================================================

#[test]
fn test_parse_alter_table_add_column() {
    let result = Parser::parse_sql("ALTER TABLE users ADD COLUMN email VARCHAR(100);");
    if let Err(ref e) = result {
        println!("Parse error: {:?}", e);
    }
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => {
            match alter {
                vibesql_ast::AlterTableStmt::AddColumn(add) => {
                    assert_eq!(add.table_name, "users");
                    assert_eq!(add.column_def.name, "email");
                    match add.column_def.data_type {
                        vibesql_types::DataType::Varchar { max_length: Some(100) } => {} // Success
                        _ => panic!("Expected VARCHAR(100) data type"),
                    }
                    assert!(add.column_def.nullable); // NULL by default
                    assert!(add.column_def.constraints.is_empty());
                }
                _ => panic!("Expected ADD COLUMN"),
            }
        }
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_parse_alter_table_drop_column() {
    let result = Parser::parse_sql("ALTER TABLE users DROP COLUMN email;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::DropColumn(drop) => {
                assert_eq!(drop.table_name, "users");
                assert_eq!(drop.column_name, "email");
                assert!(!drop.if_exists);
            }
            _ => panic!("Expected DROP COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_parse_alter_table_drop_column_if_exists() {
    let result = Parser::parse_sql("ALTER TABLE users DROP COLUMN IF EXISTS email;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::DropColumn(drop) => {
                assert_eq!(drop.table_name, "users");
                assert_eq!(drop.column_name, "email");
                assert!(drop.if_exists);
            }
            _ => panic!("Expected DROP COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_parse_alter_table_alter_column_set_not_null() {
    let result = Parser::parse_sql("ALTER TABLE users ALTER COLUMN email SET NOT NULL;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::AlterColumn(alter_col) => match alter_col {
                vibesql_ast::AlterColumnStmt::SetNotNull { table_name, column_name } => {
                    assert_eq!(table_name, "users");
                    assert_eq!(column_name, "email");
                }
                _ => panic!("Expected SET NOT NULL"),
            },
            _ => panic!("Expected ALTER COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_parse_alter_table_alter_column_drop_not_null() {
    let result = Parser::parse_sql("ALTER TABLE users ALTER COLUMN email DROP NOT NULL;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::AlterColumn(alter_col) => match alter_col {
                vibesql_ast::AlterColumnStmt::DropNotNull { table_name, column_name } => {
                    assert_eq!(table_name, "users");
                    assert_eq!(column_name, "email");
                }
                _ => panic!("Expected DROP NOT NULL"),
            },
            _ => panic!("Expected ALTER COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

// ========================================================================
// ALTER TABLE ADD Constraint (without CONSTRAINT keyword) Tests
// SQL:1999 Feature F031-04
// ========================================================================

#[test]
fn test_alter_table_add_check_no_keyword() {
    let result = Parser::parse_sql("ALTER TABLE t ADD CHECK (x > 0);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::AddConstraint(add) => {
                assert_eq!(add.table_name, "t");
                assert!(add.constraint.name.is_none(), "Expected unnamed constraint");
                match add.constraint.kind {
                    vibesql_ast::TableConstraintKind::Check { .. } => {} // Success
                    _ => panic!("Expected CHECK constraint"),
                }
            }
            _ => panic!("Expected ADD CONSTRAINT"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_alter_table_add_unique_no_keyword() {
    let result = Parser::parse_sql("ALTER TABLE t ADD UNIQUE (col);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::AddConstraint(add) => {
                assert_eq!(add.table_name, "t");
                assert!(add.constraint.name.is_none(), "Expected unnamed constraint");
                match &add.constraint.kind {
                    vibesql_ast::TableConstraintKind::Unique { columns } => {
                        assert_eq!(columns.len(), 1);
                        assert_eq!(columns[0].expect_column_name(), "col");
                    }
                    _ => panic!("Expected UNIQUE constraint"),
                }
            }
            _ => panic!("Expected ADD CONSTRAINT"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_alter_table_add_primary_key_no_keyword() {
    let result = Parser::parse_sql("ALTER TABLE t ADD PRIMARY KEY (col);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::AddConstraint(add) => {
                assert_eq!(add.table_name, "t");
                assert!(add.constraint.name.is_none(), "Expected unnamed constraint");
                match &add.constraint.kind {
                    vibesql_ast::TableConstraintKind::PrimaryKey { columns } => {
                        assert_eq!(columns.len(), 1);
                        assert_eq!(columns[0].expect_column_name(), "col");
                    }
                    _ => panic!("Expected PRIMARY KEY constraint"),
                }
            }
            _ => panic!("Expected ADD CONSTRAINT"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_alter_table_add_foreign_key_no_keyword() {
    let result =
        Parser::parse_sql("ALTER TABLE t ADD FOREIGN KEY (col) REFERENCES other(other_col);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::AddConstraint(add) => {
                assert_eq!(add.table_name, "t");
                assert!(add.constraint.name.is_none(), "Expected unnamed constraint");
                match &add.constraint.kind {
                    vibesql_ast::TableConstraintKind::ForeignKey {
                        columns,
                        references_table,
                        references_columns,
                        on_delete,
                        on_update,
                    } => {
                        assert_eq!(columns.len(), 1);
                        assert_eq!(columns[0], "col");
                        assert_eq!(references_table, "other");
                        assert_eq!(references_columns.len(), 1);
                        assert_eq!(references_columns[0], "other_col");
                        assert!(on_delete.is_none());
                        assert!(on_update.is_none());
                    }
                    _ => panic!("Expected FOREIGN KEY constraint"),
                }
            }
            _ => panic!("Expected ADD CONSTRAINT"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_alter_table_add_named_check_with_keyword() {
    // Ensure backward compatibility - named constraints with CONSTRAINT keyword still work
    let result = Parser::parse_sql("ALTER TABLE t ADD CONSTRAINT ck CHECK (x > 0);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::AddConstraint(add) => {
                assert_eq!(add.table_name, "t");
                assert_eq!(add.constraint.name, Some("ck".to_string()));
                match add.constraint.kind {
                    vibesql_ast::TableConstraintKind::Check { .. } => {} // Success
                    _ => panic!("Expected CHECK constraint"),
                }
            }
            _ => panic!("Expected ADD CONSTRAINT"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

// ========================================================================
// ALTER TABLE ADD Column (without COLUMN keyword) Tests
// SQL:1999 Feature F031-04 - COLUMN keyword is optional
// ========================================================================

#[test]
fn test_alter_table_add_column_without_column_keyword() {
    // SQL:1999 allows ADD <column_name> <data_type> without COLUMN keyword
    let result = Parser::parse_sql("ALTER TABLE t1 ADD col1 INT;");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::AddColumn(add) => {
                assert_eq!(add.table_name, "t1");
                assert_eq!(add.column_def.name, "col1");
                match add.column_def.data_type {
                    vibesql_types::DataType::Integer => {} // Success
                    _ => panic!("Expected INTEGER data type"),
                }
                assert!(add.column_def.nullable); // NULL by default
                assert!(add.column_def.constraints.is_empty());
            }
            _ => panic!("Expected ADD COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_alter_table_add_column_bare_with_varchar() {
    let result = Parser::parse_sql("ALTER TABLE users ADD email VARCHAR(100);");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::AddColumn(add) => {
                assert_eq!(add.table_name, "users");
                assert_eq!(add.column_def.name, "email");
                match add.column_def.data_type {
                    vibesql_types::DataType::Varchar { max_length: Some(100) } => {} // Success
                    _ => panic!("Expected VARCHAR(100) data type"),
                }
            }
            _ => panic!("Expected ADD COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_alter_table_add_column_bare_with_not_null() {
    let result = Parser::parse_sql("ALTER TABLE t1 ADD col1 INT NOT NULL;");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::AddColumn(add) => {
                assert_eq!(add.table_name, "t1");
                assert_eq!(add.column_def.name, "col1");
                assert!(!add.column_def.nullable); // NOT NULL specified
            }
            _ => panic!("Expected ADD COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_alter_table_add_column_bare_with_default() {
    let result = Parser::parse_sql("ALTER TABLE t1 ADD status VARCHAR(50) DEFAULT 'active';");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::AddColumn(add) => {
                assert_eq!(add.table_name, "t1");
                assert_eq!(add.column_def.name, "status");
                assert!(add.column_def.default_value.is_some());
            }
            _ => panic!("Expected ADD COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_alter_table_add_column_keyword_still_works() {
    // Ensure backward compatibility - COLUMN keyword still works
    let result = Parser::parse_sql("ALTER TABLE t1 ADD COLUMN col1 INT;");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::AddColumn(add) => {
                assert_eq!(add.table_name, "t1");
                assert_eq!(add.column_def.name, "col1");
            }
            _ => panic!("Expected ADD COLUMN"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}
