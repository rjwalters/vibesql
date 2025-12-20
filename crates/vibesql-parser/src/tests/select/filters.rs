use super::super::*;

#[test]
fn test_parse_select_with_where() {
    let result = Parser::parse_sql("SELECT name FROM users WHERE id = 1;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.where_clause.is_some());
            match &select.where_clause.as_ref().unwrap() {
                vibesql_ast::Expression::BinaryOp { op, left, right } => {
                    assert_eq!(*op, vibesql_ast::BinaryOperator::Equal);
                    match **left {
                        vibesql_ast::Expression::ColumnRef(ref col_id)
                            if col_id.column_canonical() == "id" => {}
                        _ => panic!("Expected id column in WHERE"),
                    }
                    match **right {
                        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)) => {}
                        _ => panic!("Expected Integer(1) in WHERE"),
                    }
                }
                _ => panic!("Expected BinaryOp in WHERE clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_with_alias() {
    let result = Parser::parse_sql("SELECT id AS user_id FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { alias, .. } => {
                    assert_eq!(alias.as_ref().unwrap(), "user_id");
                }
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_with_alias_without_as() {
    let result = Parser::parse_sql("SELECT id user_id FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { alias, .. } => {
                    assert_eq!(alias.as_ref().unwrap(), "user_id");
                }
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_mixed_aliases() {
    let result = Parser::parse_sql("SELECT id AS user_id, name username, age FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);

            // First column: id AS user_id
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { alias, .. } => {
                    assert_eq!(alias.as_ref().unwrap(), "user_id");
                }
                _ => panic!("Expected Expression select item"),
            }

            // Second column: name username (without AS)
            match &select.select_list[1] {
                vibesql_ast::SelectItem::Expression { alias, .. } => {
                    assert_eq!(alias.as_ref().unwrap(), "username");
                }
                _ => panic!("Expected Expression select item"),
            }

            // Third column: age (no alias)
            match &select.select_list[2] {
                vibesql_ast::SelectItem::Expression { alias, .. } => {
                    assert!(alias.is_none());
                }
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

/// SQLite compatibility: single-quoted strings can be used as column aliases
/// e.g., SELECT 1 AS 'a' - the 'a' is treated as an identifier, not a string literal
#[test]
fn test_parse_select_with_single_quoted_alias() {
    let result = Parser::parse_sql("SELECT 1 AS 'a', 'hello' AS 'b', 2 AS 'c';");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);

            // First column: 1 AS 'a'
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { alias, .. } => {
                    assert_eq!(alias.as_ref().unwrap(), "a");
                }
                _ => panic!("Expected Expression select item"),
            }

            // Second column: 'hello' AS 'b'
            match &select.select_list[1] {
                vibesql_ast::SelectItem::Expression { alias, .. } => {
                    assert_eq!(alias.as_ref().unwrap(), "b");
                }
                _ => panic!("Expected Expression select item"),
            }

            // Third column: 2 AS 'c'
            match &select.select_list[2] {
                vibesql_ast::SelectItem::Expression { alias, .. } => {
                    assert_eq!(alias.as_ref().unwrap(), "c");
                }
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_precedence() {
    // Test that 1 + 2 * 3 parses as 1 + (2 * 3)
    let result = Parser::parse_sql("SELECT 1 + 2 * 3;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::BinaryOp { op, left, right } => {
                        assert_eq!(*op, vibesql_ast::BinaryOperator::Plus);
                        // Left should be 1
                        match **left {
                            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                                1,
                            )) => {}
                            _ => panic!("Expected left = 1"),
                        }
                        // Right should be 2 * 3
                        match **right {
                            vibesql_ast::Expression::BinaryOp {
                                op: vibesql_ast::BinaryOperator::Multiply,
                                ..
                            } => {}
                            _ => panic!("Expected right = 2 * 3"),
                        }
                    }
                    _ => panic!("Expected BinaryOp"),
                },
                _ => panic!("Expected Expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_parentheses() {
    // Test that (1 + 2) * 3 parses correctly
    let result = Parser::parse_sql("SELECT (1 + 2) * 3;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::BinaryOp { op, left, right } => {
                        assert_eq!(*op, vibesql_ast::BinaryOperator::Multiply);
                        // Left should be (1 + 2)
                        match **left {
                            vibesql_ast::Expression::BinaryOp {
                                op: vibesql_ast::BinaryOperator::Plus,
                                ..
                            } => {}
                            _ => panic!("Expected left = 1 + 2"),
                        }
                        // Right should be 3
                        match **right {
                            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                                3,
                            )) => {}
                            _ => panic!("Expected right = 3"),
                        }
                    }
                    _ => panic!("Expected BinaryOp"),
                },
                _ => panic!("Expected Expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_and_operator() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE age > 18 AND status = 'active';");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.where_clause.is_some());
            match select.where_clause.as_ref().unwrap() {
                vibesql_ast::Expression::BinaryOp { op, .. } => {
                    assert_eq!(*op, vibesql_ast::BinaryOperator::And);
                }
                _ => panic!("Expected AND expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_or_operator() {
    let result =
        Parser::parse_sql("SELECT * FROM users WHERE status = 'active' OR status = 'pending';");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.where_clause.is_some());
            match select.where_clause.as_ref().unwrap() {
                vibesql_ast::Expression::BinaryOp { op, .. } => {
                    assert_eq!(*op, vibesql_ast::BinaryOperator::Or);
                }
                _ => panic!("Expected OR expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_complex_where() {
    // Test: age > 18 AND (status = 'active' OR status = 'pending')
    let result = Parser::parse_sql(
        "SELECT * FROM users WHERE age > 18 AND (status = 'active' OR status = 'pending');",
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.where_clause.is_some());
            // Outer should be AND
            match select.where_clause.as_ref().unwrap() {
                vibesql_ast::Expression::BinaryOp { op, right, .. } => {
                    assert_eq!(*op, vibesql_ast::BinaryOperator::And);
                    // Right side should be OR (in parentheses)
                    match **right {
                        vibesql_ast::Expression::BinaryOp {
                            op: vibesql_ast::BinaryOperator::Or,
                            ..
                        } => {} /* Success */
                        _ => panic!("Expected OR in right side"),
                    }
                }
                _ => panic!("Expected AND expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

// ========================================================================
// TYPE and SQL as Column Names in SELECT (SQLite compatibility - Issue #4187)
// ========================================================================

#[test]
fn test_select_type_column_unquoted() {
    // This is the main use case from issue #4187: querying sqlite_master.type
    let result = Parser::parse_sql("SELECT name FROM sqlite_master WHERE type = 'table';");
    assert!(result.is_ok(), "Should parse unquoted 'type' column: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.where_clause.is_some());
            match select.where_clause.as_ref().unwrap() {
                vibesql_ast::Expression::BinaryOp { op, left, .. } => {
                    assert_eq!(*op, vibesql_ast::BinaryOperator::Equal);
                    match **left {
                        vibesql_ast::Expression::ColumnRef(ref col_id) => {
                            assert_eq!(col_id.column_canonical(), "type");
                        }
                        _ => panic!("Expected type column in WHERE, got {:?}", left),
                    }
                }
                _ => panic!("Expected BinaryOp in WHERE clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_select_sql_column_unquoted() {
    // Querying sqlite_master.sql column
    let result = Parser::parse_sql("SELECT sql FROM sqlite_master WHERE name = 'users';");
    assert!(result.is_ok(), "Should parse unquoted 'sql' column: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        let column = col_id.column_canonical();
                        assert_eq!(column, "sql");
                    }
                    _ => panic!("Expected ColumnRef for sql"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_select_type_and_sql_columns_together() {
    // Full sqlite_master query pattern
    let result =
        Parser::parse_sql("SELECT type, name, sql FROM sqlite_master WHERE type = 'table';");
    assert!(result.is_ok(), "Should parse type and sql columns: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);

            // First column: type
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        let column = col_id.column_canonical();
                        assert_eq!(column, "type");
                    }
                    _ => panic!("Expected ColumnRef for type"),
                },
                _ => panic!("Expected Expression select item"),
            }

            // Third column: sql
            match &select.select_list[2] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        let column = col_id.column_canonical();
                        assert_eq!(column, "sql");
                    }
                    _ => panic!("Expected ColumnRef for sql"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_select_qualified_type_column() {
    // Qualified column reference: table.type
    let result = Parser::parse_sql("SELECT sqlite_master.type FROM sqlite_master;");
    assert!(result.is_ok(), "Should parse qualified type column: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::ColumnRef(col_id) => {
                    let table = col_id.table_canonical();
                    let column = col_id.column_canonical();
                    assert_eq!(table, Some("sqlite_master"));
                    assert_eq!(column, "type");
                }
                _ => panic!("Expected ColumnRef"),
            },
            _ => panic!("Expected Expression select item"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}
