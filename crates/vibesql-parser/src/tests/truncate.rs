//! Tests for TRUNCATE TABLE parsing

use vibesql_ast::Statement;

#[test]
fn test_truncate_table_basic() {
    let input = "TRUNCATE TABLE users;";
    let mut lexer = crate::lexer::Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();
    let mut parser = crate::Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Statement::TruncateTable(truncate_stmt) => {
            assert_eq!(truncate_stmt.table_names, vec!["users"]);
            assert!(!truncate_stmt.if_exists);
        }
        _ => panic!("Expected TruncateTable statement, got {:?}", stmt),
    }
}

#[test]
fn test_truncate_without_table_keyword() {
    let input = "TRUNCATE users;";
    let mut lexer = crate::lexer::Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();
    let mut parser = crate::Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Statement::TruncateTable(truncate_stmt) => {
            assert_eq!(truncate_stmt.table_names, vec!["users"]);
            assert!(!truncate_stmt.if_exists);
        }
        _ => panic!("Expected TruncateTable statement, got {:?}", stmt),
    }
}

#[test]
fn test_truncate_table_if_exists() {
    let input = "TRUNCATE TABLE IF EXISTS users;";
    let mut lexer = crate::lexer::Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();
    let mut parser = crate::Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Statement::TruncateTable(truncate_stmt) => {
            assert_eq!(truncate_stmt.table_names, vec!["users"]);
            assert!(truncate_stmt.if_exists);
        }
        _ => panic!("Expected TruncateTable statement, got {:?}", stmt),
    }
}

#[test]
fn test_truncate_if_exists_without_table() {
    let input = "TRUNCATE IF EXISTS users;";
    let mut lexer = crate::lexer::Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();
    let mut parser = crate::Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Statement::TruncateTable(truncate_stmt) => {
            assert_eq!(truncate_stmt.table_names, vec!["users"]);
            assert!(truncate_stmt.if_exists);
        }
        _ => panic!("Expected TruncateTable statement, got {:?}", stmt),
    }
}

#[test]
fn test_truncate_qualified_table() {
    let input = "TRUNCATE TABLE myschema.users;";
    let mut lexer = crate::lexer::Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();
    let mut parser = crate::Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Statement::TruncateTable(truncate_stmt) => {
            assert_eq!(truncate_stmt.table_names, vec!["myschema.users"]);
            assert!(!truncate_stmt.if_exists);
        }
        _ => panic!("Expected TruncateTable statement, got {:?}", stmt),
    }
}

#[test]
fn test_truncate_multiple_tables() {
    let input = "TRUNCATE TABLE orders, order_items, order_history;";
    let mut lexer = crate::lexer::Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();
    let mut parser = crate::Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Statement::TruncateTable(truncate_stmt) => {
            assert_eq!(truncate_stmt.table_names, vec!["orders", "order_items", "order_history"]);
            assert!(!truncate_stmt.if_exists);
        }
        _ => panic!("Expected TruncateTable statement, got {:?}", stmt),
    }
}

#[test]
fn test_truncate_multiple_tables_without_table_keyword() {
    let input = "TRUNCATE orders, order_items;";
    let mut lexer = crate::lexer::Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();
    let mut parser = crate::Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Statement::TruncateTable(truncate_stmt) => {
            assert_eq!(truncate_stmt.table_names, vec!["orders", "order_items"]);
            assert!(!truncate_stmt.if_exists);
        }
        _ => panic!("Expected TruncateTable statement, got {:?}", stmt),
    }
}

#[test]
fn test_truncate_multiple_tables_if_exists() {
    let input = "TRUNCATE TABLE IF EXISTS temp_data, staging_data, cache_data;";
    let mut lexer = crate::lexer::Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();
    let mut parser = crate::Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Statement::TruncateTable(truncate_stmt) => {
            assert_eq!(truncate_stmt.table_names, vec!["temp_data", "staging_data", "cache_data"]);
            assert!(truncate_stmt.if_exists);
        }
        _ => panic!("Expected TruncateTable statement, got {:?}", stmt),
    }
}

#[test]
fn test_truncate_multiple_qualified_tables() {
    let input = "TRUNCATE TABLE schema1.table1, schema2.table2;";
    let mut lexer = crate::lexer::Lexer::new(input);
    let tokens = lexer.tokenize().unwrap();
    let mut parser = crate::Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        Statement::TruncateTable(truncate_stmt) => {
            assert_eq!(truncate_stmt.table_names, vec!["schema1.table1", "schema2.table2"]);
            assert!(!truncate_stmt.if_exists);
        }
        _ => panic!("Expected TruncateTable statement, got {:?}", stmt),
    }
}
