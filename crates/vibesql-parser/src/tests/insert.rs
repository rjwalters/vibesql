use super::*;

// ========================================================================
// INSERT Statement Tests
// ========================================================================

#[test]
fn test_parse_insert_basic() {
    let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "USERS");
            assert_eq!(insert.columns.len(), 2);
            assert_eq!(insert.columns[0], "ID");
            assert_eq!(insert.columns[1], "NAME");
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 1); // One row
                    assert_eq!(values[0].len(), 2); // Two values
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_insert_with_default() {
    let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (DEFAULT, 'Alice');");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "USERS");
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 1); // One row
                    assert_eq!(values[0].len(), 2); // Two values
                                                    // Check that first value is DEFAULT
                    match &values[0][0] {
                        vibesql_ast::Expression::Default => {
                            // Success - parsed as Default expression
                        }
                        _ => panic!("Expected DEFAULT expression, got {:?}", values[0][0]),
                    }
                    // Check that second value is a string literal
                    match &values[0][1] {
                        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s)) => {
                            assert_eq!(s.as_str(), "Alice");
                        }
                        _ => panic!("Expected string literal 'Alice', got {:?}", values[0][1]),
                    }
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_insert_multiple_defaults() {
    let result = Parser::parse_sql("INSERT INTO t VALUES (DEFAULT, DEFAULT);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "T");
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 1); // One row
                    assert_eq!(values[0].len(), 2); // Two values
                                                    // Check that both values are DEFAULT
                    match &values[0][0] {
                        vibesql_ast::Expression::Default => {
                            // Success - parsed as Default expression
                        }
                        _ => panic!(
                            "Expected DEFAULT expression for first value, got {:?}",
                            values[0][0]
                        ),
                    }
                    match &values[0][1] {
                        vibesql_ast::Expression::Default => {
                            // Success - parsed as Default expression
                        }
                        _ => panic!(
                            "Expected DEFAULT expression for second value, got {:?}",
                            values[0][1]
                        ),
                    }
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_multiple_rows() {
    let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "USERS");
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 2); // Two rows
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_with_default() {
    let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (DEFAULT, 'Alice');");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            assert_eq!(insert.table_name, "USERS");
            assert_eq!(insert.columns.len(), 2);
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 1);
                    assert_eq!(values[0].len(), 2);
                    // First value should be DEFAULT
                    assert!(matches!(values[0][0], vibesql_ast::Expression::Default));
                    // Second value should be a literal
                    assert!(matches!(values[0][1], vibesql_ast::Expression::Literal(_)));
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}

#[test]
fn test_parse_insert_all_defaults() {
    let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (DEFAULT, DEFAULT);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Insert(insert) => {
            match &insert.source {
                vibesql_ast::InsertSource::Values(values) => {
                    assert_eq!(values.len(), 1);
                    assert_eq!(values[0].len(), 2);
                    // Both values should be DEFAULT
                    assert!(matches!(values[0][0], vibesql_ast::Expression::Default));
                    assert!(matches!(values[0][1], vibesql_ast::Expression::Default));
                }
                _ => panic!("Expected VALUES source"),
            }
        }
        _ => panic!("Expected INSERT statement"),
    }
}
