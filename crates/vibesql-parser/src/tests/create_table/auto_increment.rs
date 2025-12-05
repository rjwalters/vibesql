//! Tests for AUTO_INCREMENT column constraint parsing

use super::super::*;

#[test]
fn test_auto_increment_basic() {
    let result = Parser::parse_sql(
        "CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50));",
    );
    assert!(result.is_ok(), "Failed to parse AUTO_INCREMENT: {:?}", result.err());

    match result.unwrap() {
        vibesql_ast::Statement::CreateTable(stmt) => {
            assert_eq!(stmt.table_name, "USERS");
            assert_eq!(stmt.columns.len(), 2);

            // Check first column has AUTO_INCREMENT constraint
            let id_col = &stmt.columns[0];
            assert_eq!(id_col.name, "ID");
            assert!(id_col
                .constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::AutoIncrement)));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_autoincrement_sqlite_style() {
    let result = Parser::parse_sql("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT);");
    assert!(result.is_ok(), "Failed to parse AUTOINCREMENT: {:?}", result.err());

    match result.unwrap() {
        vibesql_ast::Statement::CreateTable(stmt) => {
            let id_col = &stmt.columns[0];
            assert!(id_col
                .constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::AutoIncrement)));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_auto_increment_with_other_constraints() {
    let result =
        Parser::parse_sql("CREATE TABLE products (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY);");
    assert!(result.is_ok());

    match result.unwrap() {
        vibesql_ast::Statement::CreateTable(stmt) => {
            let id_col = &stmt.columns[0];

            // Should have NOT NULL, AUTO_INCREMENT, and PRIMARY KEY
            assert!(id_col
                .constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::NotNull)));
            assert!(id_col
                .constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::AutoIncrement)));
            assert!(id_col
                .constraints
                .iter()
                .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::PrimaryKey)));
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}
