//! Full-text search integration tests for MATCH...AGAINST

use super::super::*;

#[test]
fn test_match_against_natural_language_single_column() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "ARTICLES".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "title".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(200) },
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "body".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(1000) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "ARTICLES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar("MySQL Database Guide".to_string()),
            vibesql_types::SqlValue::Varchar("Learn about MySQL database management".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ARTICLES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar("MySQL Tutorial".to_string()),
            vibesql_types::SqlValue::Varchar("Learn MySQL database management".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ARTICLES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar("PostgreSQL Features".to_string()),
            vibesql_types::SqlValue::Varchar("Advanced PostgreSQL features".to_string()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT id FROM articles WHERE MATCH(title) AGAINST ('MySQL')",
    )
    .unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let rows = executor.execute(&select_stmt).unwrap();
        assert_eq!(rows.len(), 2, "Should find 2 articles with 'MySQL'");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_match_against_multiple_columns() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "ARTICLES".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "title".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(200) },
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "body".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(1000) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "ARTICLES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar("Database Guide".to_string()),
            vibesql_types::SqlValue::Varchar("Learn SQL and database concepts".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ARTICLES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar("Web Development".to_string()),
            vibesql_types::SqlValue::Varchar("Build websites with web frameworks".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ARTICLES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar("Python Tutorial".to_string()),
            vibesql_types::SqlValue::Varchar("Master SQL with Python".to_string()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT id FROM articles WHERE MATCH(title, body) AGAINST ('SQL')",
    )
    .unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let rows = executor.execute(&select_stmt).unwrap();
        assert_eq!(rows.len(), 2, "Should find 2 articles with 'SQL'");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_match_against_no_matches() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "ARTICLES".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "title".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(200) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "ARTICLES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar("ABC Article".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ARTICLES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar("XYZ Article".to_string()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT id FROM articles WHERE MATCH(title) AGAINST ('database')",
    )
    .unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let rows = executor.execute(&select_stmt).unwrap();
        assert_eq!(rows.len(), 0, "Should find no matches");
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_match_against_boolean_mode_required() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "ARTICLES".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "title".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(200) },
                true,
            ),
            vibesql_catalog::ColumnSchema::new(
                "body".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(1000) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "ARTICLES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar("MySQL and PostgreSQL".to_string()),
            vibesql_types::SqlValue::Varchar("Two popular databases".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ARTICLES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar("Oracle Database".to_string()),
            vibesql_types::SqlValue::Varchar("Enterprise database solution".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ARTICLES",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar("MySQL Features".to_string()),
            vibesql_types::SqlValue::Varchar("Learn about MySQL".to_string()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT id FROM articles WHERE MATCH(title, body) AGAINST ('+mysql' IN BOOLEAN MODE)",
    )
    .unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let rows = executor.execute(&select_stmt).unwrap();
        assert_eq!(rows.len(), 2, "Should find 2 articles with 'mysql'");
    } else {
        panic!("Expected SELECT statement");
    }
}
