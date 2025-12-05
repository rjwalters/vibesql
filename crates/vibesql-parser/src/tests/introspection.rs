//! Tests for database introspection statements (SHOW, DESCRIBE)

use crate::parser::Parser;
use vibesql_ast::*;

#[test]
fn test_show_tables() {
    let stmt = Parser::parse_sql("SHOW TABLES").unwrap();
    assert!(matches!(stmt, Statement::ShowTables(_)));

    if let Statement::ShowTables(show_tables) = stmt {
        assert!(show_tables.database.is_none());
        assert!(show_tables.like_pattern.is_none());
        assert!(show_tables.where_clause.is_none());
    }
}

#[test]
fn test_show_tables_from_database() {
    let stmt = Parser::parse_sql("SHOW TABLES FROM mydb").unwrap();

    if let Statement::ShowTables(show_tables) = stmt {
        assert_eq!(show_tables.database, Some("MYDB".to_string()));
        assert!(show_tables.like_pattern.is_none());
        assert!(show_tables.where_clause.is_none());
    } else {
        panic!("Expected ShowTables statement");
    }
}

#[test]
fn test_show_databases() {
    let stmt = Parser::parse_sql("SHOW DATABASES").unwrap();
    assert!(matches!(stmt, Statement::ShowDatabases(_)));

    if let Statement::ShowDatabases(show_databases) = stmt {
        assert!(show_databases.like_pattern.is_none());
        assert!(show_databases.where_clause.is_none());
    }
}

#[test]
fn test_describe_table() {
    let stmt = Parser::parse_sql("DESCRIBE users").unwrap();
    assert!(matches!(stmt, Statement::Describe(_)));

    if let Statement::Describe(describe) = stmt {
        assert_eq!(describe.table_name, "USERS");
        assert!(describe.column_pattern.is_none());
    }
}

#[test]
fn test_describe_table_with_column() {
    let stmt = Parser::parse_sql("DESCRIBE users name").unwrap();

    if let Statement::Describe(describe) = stmt {
        assert_eq!(describe.table_name, "USERS");
        assert_eq!(describe.column_pattern, Some("NAME".to_string()));
    } else {
        panic!("Expected Describe statement");
    }
}

#[test]
fn test_show_columns() {
    let stmt = Parser::parse_sql("SHOW COLUMNS FROM users").unwrap();

    if let Statement::ShowColumns(show_columns) = stmt {
        assert_eq!(show_columns.table_name, "USERS");
        assert!(!show_columns.full);
        assert!(show_columns.database.is_none());
        assert!(show_columns.like_pattern.is_none());
        assert!(show_columns.where_clause.is_none());
    } else {
        panic!("Expected ShowColumns statement");
    }
}

#[test]
fn test_show_index() {
    let stmt = Parser::parse_sql("SHOW INDEX FROM users").unwrap();

    if let Statement::ShowIndex(show_index) = stmt {
        assert_eq!(show_index.table_name, "USERS");
        assert!(show_index.database.is_none());
    } else {
        panic!("Expected ShowIndex statement");
    }
}

#[test]
fn test_show_create_table() {
    let stmt = Parser::parse_sql("SHOW CREATE TABLE users").unwrap();

    if let Statement::ShowCreateTable(show_create) = stmt {
        assert_eq!(show_create.table_name, "USERS");
    } else {
        panic!("Expected ShowCreateTable statement");
    }
}

// ============================================================================
// Tests for Synonym Keywords
// ============================================================================

#[test]
fn test_show_fields_synonym() {
    let stmt = Parser::parse_sql("SHOW FIELDS FROM users").unwrap();
    assert!(matches!(stmt, Statement::ShowColumns(_)));

    if let Statement::ShowColumns(show_columns) = stmt {
        assert_eq!(show_columns.table_name, "USERS");
    }
}

#[test]
fn test_show_indexes_synonym() {
    let stmt = Parser::parse_sql("SHOW INDEXES FROM users").unwrap();
    assert!(matches!(stmt, Statement::ShowIndex(_)));

    if let Statement::ShowIndex(show_index) = stmt {
        assert_eq!(show_index.table_name, "USERS");
    }
}

#[test]
fn test_show_keys_synonym() {
    let stmt = Parser::parse_sql("SHOW KEYS FROM users").unwrap();
    assert!(matches!(stmt, Statement::ShowIndex(_)));

    if let Statement::ShowIndex(show_index) = stmt {
        assert_eq!(show_index.table_name, "USERS");
    }
}

// ============================================================================
// Tests for LIKE Patterns and Modifiers
// ============================================================================

#[test]
fn test_show_tables_with_like() {
    let stmt = Parser::parse_sql("SHOW TABLES LIKE 'user%'").unwrap();

    if let Statement::ShowTables(show_tables) = stmt {
        assert_eq!(show_tables.like_pattern, Some("user%".to_string()));
        assert!(show_tables.database.is_none());
        assert!(show_tables.where_clause.is_none());
    } else {
        panic!("Expected ShowTables statement");
    }
}

#[test]
fn test_show_databases_with_like() {
    let stmt = Parser::parse_sql("SHOW DATABASES LIKE 'test%'").unwrap();

    if let Statement::ShowDatabases(show_databases) = stmt {
        assert_eq!(show_databases.like_pattern, Some("test%".to_string()));
        assert!(show_databases.where_clause.is_none());
    } else {
        panic!("Expected ShowDatabases statement");
    }
}

#[test]
fn test_show_columns_full() {
    let stmt = Parser::parse_sql("SHOW FULL COLUMNS FROM users").unwrap();

    if let Statement::ShowColumns(show_columns) = stmt {
        assert!(show_columns.full);
        assert_eq!(show_columns.table_name, "USERS");
    } else {
        panic!("Expected ShowColumns statement");
    }
}

#[test]
fn test_show_columns_with_like() {
    let stmt = Parser::parse_sql("SHOW COLUMNS FROM users LIKE 'name%'").unwrap();

    if let Statement::ShowColumns(show_columns) = stmt {
        assert_eq!(show_columns.table_name, "USERS");
        assert_eq!(show_columns.like_pattern, Some("name%".to_string()));
        assert!(!show_columns.full);
    } else {
        panic!("Expected ShowColumns statement");
    }
}

// ============================================================================
// Error Case Tests
// ============================================================================

#[test]
fn test_show_without_target() {
    let result = Parser::parse_sql("SHOW");
    assert!(result.is_err());
}

#[test]
fn test_show_invalid_target() {
    let result = Parser::parse_sql("SHOW INVALID");
    assert!(result.is_err());
}

#[test]
fn test_show_create_without_table() {
    let result = Parser::parse_sql("SHOW CREATE");
    assert!(result.is_err());
}

#[test]
fn test_show_create_invalid_object() {
    let result = Parser::parse_sql("SHOW CREATE INDEX");
    assert!(result.is_err());
}

#[test]
fn test_describe_without_table() {
    let result = Parser::parse_sql("DESCRIBE");
    assert!(result.is_err());
}

#[test]
fn test_show_columns_without_from() {
    let result = Parser::parse_sql("SHOW COLUMNS");
    assert!(result.is_err());
}

#[test]
fn test_show_index_without_from() {
    let result = Parser::parse_sql("SHOW INDEX");
    assert!(result.is_err());
}

// ============================================================================
// EXPLAIN Statement Tests
// ============================================================================

#[test]
fn test_explain_select() {
    let stmt = Parser::parse_sql("EXPLAIN SELECT * FROM users").unwrap();

    if let Statement::Explain(explain) = stmt {
        assert!(!explain.analyze);
        assert_eq!(explain.format, ExplainFormat::Text);
        assert!(matches!(*explain.statement, Statement::Select(_)));
    } else {
        panic!("Expected Explain statement");
    }
}

#[test]
fn test_explain_analyze_select() {
    let stmt = Parser::parse_sql("EXPLAIN ANALYZE SELECT * FROM users").unwrap();

    if let Statement::Explain(explain) = stmt {
        assert!(explain.analyze);
        assert_eq!(explain.format, ExplainFormat::Text);
        assert!(matches!(*explain.statement, Statement::Select(_)));
    } else {
        panic!("Expected Explain statement");
    }
}

#[test]
fn test_explain_format_json() {
    let stmt = Parser::parse_sql("EXPLAIN FORMAT JSON SELECT * FROM users").unwrap();

    if let Statement::Explain(explain) = stmt {
        assert!(!explain.analyze);
        assert_eq!(explain.format, ExplainFormat::Json);
        assert!(matches!(*explain.statement, Statement::Select(_)));
    } else {
        panic!("Expected Explain statement");
    }
}

#[test]
fn test_explain_format_text() {
    let stmt = Parser::parse_sql("EXPLAIN FORMAT TEXT SELECT * FROM users").unwrap();

    if let Statement::Explain(explain) = stmt {
        assert!(!explain.analyze);
        assert_eq!(explain.format, ExplainFormat::Text);
        assert!(matches!(*explain.statement, Statement::Select(_)));
    } else {
        panic!("Expected Explain statement");
    }
}

#[test]
fn test_explain_analyze_format_json() {
    let stmt = Parser::parse_sql("EXPLAIN ANALYZE FORMAT JSON SELECT * FROM users").unwrap();

    if let Statement::Explain(explain) = stmt {
        assert!(explain.analyze);
        assert_eq!(explain.format, ExplainFormat::Json);
        assert!(matches!(*explain.statement, Statement::Select(_)));
    } else {
        panic!("Expected Explain statement");
    }
}

#[test]
fn test_explain_insert() {
    let stmt = Parser::parse_sql("EXPLAIN INSERT INTO users (name) VALUES ('test')").unwrap();

    if let Statement::Explain(explain) = stmt {
        assert!(!explain.analyze);
        assert!(matches!(*explain.statement, Statement::Insert(_)));
    } else {
        panic!("Expected Explain statement");
    }
}

#[test]
fn test_explain_update() {
    let stmt = Parser::parse_sql("EXPLAIN UPDATE users SET name = 'test' WHERE id = 1").unwrap();

    if let Statement::Explain(explain) = stmt {
        assert!(!explain.analyze);
        assert!(matches!(*explain.statement, Statement::Update(_)));
    } else {
        panic!("Expected Explain statement");
    }
}

#[test]
fn test_explain_delete() {
    let stmt = Parser::parse_sql("EXPLAIN DELETE FROM users WHERE id = 1").unwrap();

    if let Statement::Explain(explain) = stmt {
        assert!(!explain.analyze);
        assert!(matches!(*explain.statement, Statement::Delete(_)));
    } else {
        panic!("Expected Explain statement");
    }
}

#[test]
fn test_explain_select_with_join() {
    let stmt = Parser::parse_sql("EXPLAIN SELECT * FROM users u JOIN orders o ON u.id = o.user_id")
        .unwrap();

    if let Statement::Explain(explain) = stmt {
        assert!(matches!(*explain.statement, Statement::Select(_)));
    } else {
        panic!("Expected Explain statement");
    }
}

#[test]
fn test_explain_invalid_statement() {
    // EXPLAIN should not work with CREATE TABLE
    let result = Parser::parse_sql("EXPLAIN CREATE TABLE foo (id INT)");
    assert!(result.is_err());
}

#[test]
fn test_explain_empty() {
    let result = Parser::parse_sql("EXPLAIN");
    assert!(result.is_err());
}
