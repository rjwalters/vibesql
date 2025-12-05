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
                        vibesql_ast::Expression::ColumnRef { ref column, .. } if column == "ID" => {
                        }
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
                    assert_eq!(alias.as_ref().unwrap(), "USER_ID");
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
                    assert_eq!(alias.as_ref().unwrap(), "USER_ID");
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
                    assert_eq!(alias.as_ref().unwrap(), "USER_ID");
                }
                _ => panic!("Expected Expression select item"),
            }

            // Second column: name username (without AS)
            match &select.select_list[1] {
                vibesql_ast::SelectItem::Expression { alias, .. } => {
                    assert_eq!(alias.as_ref().unwrap(), "USERNAME");
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
