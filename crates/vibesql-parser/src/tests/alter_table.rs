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
fn test_parse_alter_table_drop_column_without_column_keyword() {
    // SQLite allows `ALTER TABLE t DROP <col>` as a synonym for
    // `ALTER TABLE t DROP COLUMN <col>` (issue #6174).
    let result = Parser::parse_sql("ALTER TABLE users DROP email;");
    assert!(result.is_ok(), "bare DROP <col> should parse: {result:?}");
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
                    vibesql_ast::TableConstraintKind::Unique { columns, .. } => {
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
                    vibesql_ast::TableConstraintKind::PrimaryKey { columns, .. } => {
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
                        ..
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

#[test]
fn test_parse_alter_table_add_column_without_type() {
    // SQLite allows a typeless column: ADD COLUMN x (BLOB/no affinity).
    let result = Parser::parse_sql("ALTER TABLE t ADD COLUMN x;");
    assert!(result.is_ok(), "err: {:?}", result);
    match result.unwrap() {
        vibesql_ast::Statement::AlterTable(vibesql_ast::AlterTableStmt::AddColumn(add)) => {
            assert_eq!(add.column_def.name, "x");
            assert!(matches!(add.column_def.data_type, vibesql_types::DataType::BinaryLargeObject));
            assert!(add.column_def.nullable);
        }
        other => panic!("Expected ADD COLUMN, got {:?}", other),
    }
}

#[test]
fn test_parse_alter_table_add_column_without_type_with_constraint() {
    // Typeless column followed directly by a constraint / default.
    for sql in [
        "ALTER TABLE t ADD COLUMN x NOT NULL",
        "ALTER TABLE t ADD COLUMN x DEFAULT 0",
        "ALTER TABLE t ADD COLUMN x UNIQUE",
    ] {
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "{sql} -> err: {:?}", result);
        match result.unwrap() {
            vibesql_ast::Statement::AlterTable(vibesql_ast::AlterTableStmt::AddColumn(add)) => {
                assert_eq!(add.column_def.name, "x");
                assert!(matches!(
                    add.column_def.data_type,
                    vibesql_types::DataType::BinaryLargeObject
                ));
            }
            other => panic!("{sql} -> Expected ADD COLUMN, got {:?}", other),
        }
    }
}

#[test]
fn test_parse_alter_table_add_column_bare_no_column_keyword_no_type() {
    // ADD <name> without the COLUMN keyword and without a type.
    let result = Parser::parse_sql("ALTER TABLE t ADD x");
    assert!(result.is_ok(), "err: {:?}", result);
    match result.unwrap() {
        vibesql_ast::Statement::AlterTable(vibesql_ast::AlterTableStmt::AddColumn(add)) => {
            assert_eq!(add.column_def.name, "x");
            assert!(matches!(add.column_def.data_type, vibesql_types::DataType::BinaryLargeObject));
        }
        other => panic!("Expected ADD COLUMN, got {:?}", other),
    }
}

// ========================================================================
// Generated-column ADD COLUMN (issue #5861)
// ========================================================================

#[test]
fn test_parse_alter_table_add_generated_column_typed() {
    // `GENERATED ALWAYS AS (expr)` after an explicit type must populate
    // `generated_expr` on the ColumnDef (previously silently dropped).
    let result = Parser::parse_sql("ALTER TABLE g ADD COLUMN y INTEGER GENERATED ALWAYS AS (x+1)");
    assert!(result.is_ok(), "err: {:?}", result);
    match result.unwrap() {
        vibesql_ast::Statement::AlterTable(vibesql_ast::AlterTableStmt::AddColumn(add)) => {
            assert_eq!(add.column_def.name, "y");
            assert!(
                add.column_def.generated_expr.is_some(),
                "generated_expr must be populated for GENERATED ALWAYS AS"
            );
        }
        other => panic!("Expected ADD COLUMN, got {:?}", other),
    }
}

#[test]
fn test_parse_alter_table_add_generated_column_typed_stored_and_virtual() {
    for sql in [
        "ALTER TABLE g ADD COLUMN y INTEGER GENERATED ALWAYS AS (x+1) STORED",
        "ALTER TABLE g ADD COLUMN y INTEGER GENERATED ALWAYS AS (x+1) VIRTUAL",
    ] {
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "{sql} -> err: {:?}", result);
        match result.unwrap() {
            vibesql_ast::Statement::AlterTable(vibesql_ast::AlterTableStmt::AddColumn(add)) => {
                assert!(
                    add.column_def.generated_expr.is_some(),
                    "{sql}: generated_expr must be populated"
                );
            }
            other => panic!("{sql} -> Expected ADD COLUMN, got {:?}", other),
        }
    }
}

#[test]
fn test_parse_alter_table_add_generated_column_typeless_short_form() {
    // Typeless short form: `ADD COLUMN y AS (x+1)`.
    let result = Parser::parse_sql("ALTER TABLE g ADD COLUMN y AS (x+1)");
    assert!(result.is_ok(), "err: {:?}", result);
    match result.unwrap() {
        vibesql_ast::Statement::AlterTable(vibesql_ast::AlterTableStmt::AddColumn(add)) => {
            assert_eq!(add.column_def.name, "y");
            assert!(
                add.column_def.generated_expr.is_some(),
                "generated_expr must be populated for the typeless AS short form"
            );
            // No explicit type -> BLOB affinity, like a typeless column.
            assert!(matches!(add.column_def.data_type, vibesql_types::DataType::BinaryLargeObject));
        }
        other => panic!("Expected ADD COLUMN, got {:?}", other),
    }
}

#[test]
fn test_parse_alter_table_add_plain_column_has_no_generated_expr() {
    // Regression guard: a plain column must NOT gain a generated expression.
    let result = Parser::parse_sql("ALTER TABLE g ADD COLUMN z INTEGER");
    assert!(result.is_ok(), "err: {:?}", result);
    match result.unwrap() {
        vibesql_ast::Statement::AlterTable(vibesql_ast::AlterTableStmt::AddColumn(add)) => {
            assert!(add.column_def.generated_expr.is_none());
        }
        other => panic!("Expected ADD COLUMN, got {:?}", other),
    }
}

#[test]
fn test_parse_alter_table_add_column_not_null_then_default() {
    // A DEFAULT clause following other column constraints (any order) must be
    // captured, not dropped. Previously `NOT NULL DEFAULT 10` lost the DEFAULT
    // because DEFAULT was only parsed before the constraint loop (alter3-2.4).
    let result = Parser::parse_sql("ALTER TABLE t1 ADD c NOT NULL DEFAULT 10");
    assert!(result.is_ok(), "err: {:?}", result);
    match result.unwrap() {
        vibesql_ast::Statement::AlterTable(vibesql_ast::AlterTableStmt::AddColumn(add)) => {
            assert!(!add.column_def.nullable, "NOT NULL should be captured");
            assert!(add.column_def.default_value.is_some(), "DEFAULT 10 should be captured");
        }
        other => panic!("Expected ADD COLUMN, got {:?}", other),
    }
}

// ========================================================================
// RENAME COLUMN with contextual-keyword column names (issue #5945)
//
// SQLite accepts contextual/fallback keywords (`m`, `key`, `level`, ...) as
// unquoted column names in `ALTER TABLE ... RENAME COLUMN <old> TO <new>`.
// The old and new column names now parse via `parse_column_name()`.
// ========================================================================

#[test]
fn test_parse_rename_column_with_contextual_keyword_m() {
    let result = Parser::parse_sql("ALTER TABLE t1 RENAME COLUMN m TO n");
    assert!(result.is_ok(), "RENAME COLUMN m TO n should parse: {:?}", result.err());
    match result.unwrap() {
        vibesql_ast::Statement::AlterTable(alter) => match alter {
            vibesql_ast::AlterTableStmt::RenameColumn(rename) => {
                assert_eq!(rename.old_column_name, "m");
                assert_eq!(rename.new_column_name, "n");
            }
            _ => panic!("Expected RENAME COLUMN statement"),
        },
        _ => panic!("Expected ALTER TABLE statement"),
    }
}

#[test]
fn test_parse_rename_column_contextual_keyword_to_contextual_keyword() {
    // Both the old and new name may be contextual keywords.
    for (old, new) in [("m", "level"), ("key", "m"), ("level", "key")] {
        let sql = format!("ALTER TABLE t1 RENAME COLUMN {old} TO {new}");
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "`{sql}` should parse: {:?}", result.err());
        match result.unwrap() {
            vibesql_ast::Statement::AlterTable(alter) => match alter {
                vibesql_ast::AlterTableStmt::RenameColumn(rename) => {
                    assert_eq!(rename.old_column_name, old);
                    assert_eq!(rename.new_column_name, new);
                }
                _ => panic!("Expected RENAME COLUMN statement"),
            },
            _ => panic!("Expected ALTER TABLE statement"),
        }
    }
}

#[test]
fn test_parse_rename_column_rejects_reserved_keyword() {
    // Truly reserved words are still rejected as unquoted column names.
    let result = Parser::parse_sql("ALTER TABLE t1 RENAME COLUMN select TO n");
    assert!(result.is_err(), "reserved keyword `select` must be rejected as a column name");
}

#[test]
fn test_parse_add_column_with_check_constraint() {
    // A column-level CHECK on an added column must be captured in the parsed
    // constraints (previously the CHECK tokens were silently dropped).
    let stmt = Parser::parse_sql("ALTER TABLE t1 ADD COLUMN c CHECK(a!=1)").unwrap();
    match stmt {
        vibesql_ast::Statement::AlterTable(vibesql_ast::AlterTableStmt::AddColumn(add)) => {
            let has_check = add.column_def.constraints.iter().any(|c| {
                matches!(c.kind, vibesql_ast::ColumnConstraintKind::Check { .. })
            });
            assert!(has_check, "ADD COLUMN CHECK constraint should be parsed");
        }
        _ => panic!("Expected ALTER TABLE ADD COLUMN statement"),
    }
}
