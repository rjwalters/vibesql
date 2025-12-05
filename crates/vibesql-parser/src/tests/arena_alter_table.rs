//! Tests for arena-allocated ALTER TABLE parsing.

use bumpalo::Bump;
use vibesql_ast::arena::{
    AddColumnStmt, AlterColumnStmt, AlterTableStmt, DropColumnStmt, RenameTableStmt,
    TableConstraintKind,
};
use vibesql_types::DataType;

use crate::arena_parser::ArenaParser;

// ========================================================================
// ADD COLUMN Tests
// ========================================================================

#[test]
fn test_arena_alter_table_add_column() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE users ADD COLUMN email VARCHAR(100);",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AddColumn(AddColumnStmt { table_name, column_def }) => {
            assert_eq!(interner.resolve(*table_name), "USERS");
            assert_eq!(interner.resolve(column_def.name), "EMAIL");
            match &column_def.data_type {
                DataType::Varchar { max_length: Some(100) } => {} // Success
                _ => panic!("Expected VARCHAR(100) data type"),
            }
            assert!(column_def.nullable);
            assert!(column_def.constraints.is_empty());
        }
        _ => panic!("Expected ADD COLUMN"),
    }
}

#[test]
fn test_arena_alter_table_add_column_without_column_keyword() {
    let arena = Bump::new();
    let result =
        ArenaParser::parse_alter_table_sql_with_interner("ALTER TABLE t1 ADD col1 INT;", &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AddColumn(AddColumnStmt { table_name, column_def }) => {
            assert_eq!(interner.resolve(*table_name), "T1");
            assert_eq!(interner.resolve(column_def.name), "COL1");
            match &column_def.data_type {
                DataType::Integer => {} // Success
                _ => panic!("Expected INTEGER data type"),
            }
        }
        _ => panic!("Expected ADD COLUMN"),
    }
}

#[test]
fn test_arena_alter_table_add_column_with_not_null() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE t1 ADD col1 INT NOT NULL;",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, _interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AddColumn(AddColumnStmt { column_def, .. }) => {
            assert!(!column_def.nullable);
        }
        _ => panic!("Expected ADD COLUMN"),
    }
}

#[test]
fn test_arena_alter_table_add_column_with_default() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE t1 ADD status VARCHAR(50) DEFAULT 'active';",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, _interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AddColumn(AddColumnStmt { column_def, .. }) => {
            assert!(column_def.default_value.is_some());
        }
        _ => panic!("Expected ADD COLUMN"),
    }
}

// ========================================================================
// DROP COLUMN Tests
// ========================================================================

#[test]
fn test_arena_alter_table_drop_column() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE users DROP COLUMN email;",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::DropColumn(DropColumnStmt { table_name, column_name, if_exists }) => {
            assert_eq!(interner.resolve(*table_name), "USERS");
            assert_eq!(interner.resolve(*column_name), "EMAIL");
            assert!(!if_exists);
        }
        _ => panic!("Expected DROP COLUMN"),
    }
}

#[test]
fn test_arena_alter_table_drop_column_if_exists() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE users DROP COLUMN IF EXISTS email;",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::DropColumn(DropColumnStmt { table_name, column_name, if_exists }) => {
            assert_eq!(interner.resolve(*table_name), "USERS");
            assert_eq!(interner.resolve(*column_name), "EMAIL");
            assert!(if_exists);
        }
        _ => panic!("Expected DROP COLUMN"),
    }
}

// ========================================================================
// ALTER COLUMN Tests
// ========================================================================

#[test]
fn test_arena_alter_table_alter_column_set_not_null() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE users ALTER COLUMN email SET NOT NULL;",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AlterColumn(AlterColumnStmt::SetNotNull { table_name, column_name }) => {
            assert_eq!(interner.resolve(*table_name), "USERS");
            assert_eq!(interner.resolve(*column_name), "EMAIL");
        }
        _ => panic!("Expected ALTER COLUMN SET NOT NULL"),
    }
}

#[test]
fn test_arena_alter_table_alter_column_drop_not_null() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE users ALTER COLUMN email DROP NOT NULL;",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AlterColumn(AlterColumnStmt::DropNotNull { table_name, column_name }) => {
            assert_eq!(interner.resolve(*table_name), "USERS");
            assert_eq!(interner.resolve(*column_name), "EMAIL");
        }
        _ => panic!("Expected ALTER COLUMN DROP NOT NULL"),
    }
}

#[test]
fn test_arena_alter_table_alter_column_set_default() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active';",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AlterColumn(AlterColumnStmt::SetDefault {
            table_name,
            column_name,
            ..
        }) => {
            assert_eq!(interner.resolve(*table_name), "USERS");
            assert_eq!(interner.resolve(*column_name), "STATUS");
        }
        _ => panic!("Expected ALTER COLUMN SET DEFAULT"),
    }
}

#[test]
fn test_arena_alter_table_alter_column_drop_default() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE users ALTER COLUMN status DROP DEFAULT;",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AlterColumn(AlterColumnStmt::DropDefault { table_name, column_name }) => {
            assert_eq!(interner.resolve(*table_name), "USERS");
            assert_eq!(interner.resolve(*column_name), "STATUS");
        }
        _ => panic!("Expected ALTER COLUMN DROP DEFAULT"),
    }
}

// ========================================================================
// RENAME TABLE Tests
// ========================================================================

#[test]
fn test_arena_alter_table_rename_to() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE users RENAME TO customers;",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::RenameTable(RenameTableStmt { table_name, new_table_name }) => {
            assert_eq!(interner.resolve(*table_name), "USERS");
            assert_eq!(interner.resolve(*new_table_name), "CUSTOMERS");
        }
        _ => panic!("Expected RENAME TABLE"),
    }
}

// ========================================================================
// ADD CONSTRAINT Tests
// ========================================================================

#[test]
fn test_arena_alter_table_add_check_constraint() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE t ADD CHECK (x > 0);",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AddConstraint(add) => {
            assert_eq!(interner.resolve(add.table_name), "T");
            assert!(add.constraint.name.is_none());
            match &add.constraint.kind {
                TableConstraintKind::Check { .. } => {} // Success
                _ => panic!("Expected CHECK constraint"),
            }
        }
        _ => panic!("Expected ADD CONSTRAINT"),
    }
}

#[test]
fn test_arena_alter_table_add_unique_constraint() {
    let arena = Bump::new();
    let result =
        ArenaParser::parse_alter_table_sql_with_interner("ALTER TABLE t ADD UNIQUE (col);", &arena);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AddConstraint(add) => {
            assert_eq!(interner.resolve(add.table_name), "T");
            match &add.constraint.kind {
                TableConstraintKind::Unique { columns } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(interner.resolve(columns[0].column_name), "COL");
                }
                _ => panic!("Expected UNIQUE constraint"),
            }
        }
        _ => panic!("Expected ADD CONSTRAINT"),
    }
}

#[test]
fn test_arena_alter_table_add_primary_key_constraint() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE t ADD PRIMARY KEY (col);",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AddConstraint(add) => {
            assert_eq!(interner.resolve(add.table_name), "T");
            match &add.constraint.kind {
                TableConstraintKind::PrimaryKey { columns } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(interner.resolve(columns[0].column_name), "COL");
                }
                _ => panic!("Expected PRIMARY KEY constraint"),
            }
        }
        _ => panic!("Expected ADD CONSTRAINT"),
    }
}

#[test]
fn test_arena_alter_table_add_foreign_key_constraint() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE t ADD FOREIGN KEY (col) REFERENCES other(other_col);",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AddConstraint(add) => {
            assert_eq!(interner.resolve(add.table_name), "T");
            match &add.constraint.kind {
                TableConstraintKind::ForeignKey {
                    columns,
                    references_table,
                    references_columns,
                    ..
                } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(interner.resolve(columns[0]), "COL");
                    assert_eq!(interner.resolve(*references_table), "OTHER");
                    assert_eq!(references_columns.len(), 1);
                    assert_eq!(interner.resolve(references_columns[0]), "OTHER_COL");
                }
                _ => panic!("Expected FOREIGN KEY constraint"),
            }
        }
        _ => panic!("Expected ADD CONSTRAINT"),
    }
}

#[test]
fn test_arena_alter_table_add_named_constraint() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE t ADD CONSTRAINT ck CHECK (x > 0);",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::AddConstraint(add) => {
            assert_eq!(interner.resolve(add.table_name), "T");
            assert!(add.constraint.name.is_some());
            assert_eq!(interner.resolve(add.constraint.name.unwrap()), "CK");
            match &add.constraint.kind {
                TableConstraintKind::Check { .. } => {} // Success
                _ => panic!("Expected CHECK constraint"),
            }
        }
        _ => panic!("Expected ADD CONSTRAINT"),
    }
}

// ========================================================================
// DROP CONSTRAINT Tests
// ========================================================================

#[test]
fn test_arena_alter_table_drop_constraint() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE users DROP CONSTRAINT pk_users;",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::DropConstraint(drop) => {
            assert_eq!(interner.resolve(drop.table_name), "USERS");
            assert_eq!(interner.resolve(drop.constraint_name), "PK_USERS");
        }
        _ => panic!("Expected DROP CONSTRAINT"),
    }
}

// ========================================================================
// MODIFY COLUMN Tests (MySQL-style)
// ========================================================================

#[test]
fn test_arena_alter_table_modify_column() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE users MODIFY COLUMN email VARCHAR(200);",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::ModifyColumn(modify) => {
            assert_eq!(interner.resolve(modify.table_name), "USERS");
            assert_eq!(interner.resolve(modify.column_name), "EMAIL");
            match &modify.new_column_def.data_type {
                DataType::Varchar { max_length: Some(200) } => {} // Success
                _ => panic!("Expected VARCHAR(200) data type"),
            }
        }
        _ => panic!("Expected MODIFY COLUMN"),
    }
}

// ========================================================================
// CHANGE COLUMN Tests (MySQL-style)
// ========================================================================

#[test]
fn test_arena_alter_table_change_column() {
    let arena = Bump::new();
    let result = ArenaParser::parse_alter_table_sql_with_interner(
        "ALTER TABLE users CHANGE COLUMN email user_email VARCHAR(200);",
        &arena,
    );
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    let (stmt, interner) = result.unwrap();

    match stmt {
        AlterTableStmt::ChangeColumn(change) => {
            assert_eq!(interner.resolve(change.table_name), "USERS");
            assert_eq!(interner.resolve(change.old_column_name), "EMAIL");
            assert_eq!(interner.resolve(change.new_column_def.name), "USER_EMAIL");
            match &change.new_column_def.data_type {
                DataType::Varchar { max_length: Some(200) } => {} // Success
                _ => panic!("Expected VARCHAR(200) data type"),
            }
        }
        _ => panic!("Expected CHANGE COLUMN"),
    }
}
