use super::*;

// ========================================================================
// JOIN Operation Tests
// ========================================================================

#[test]
fn test_parse_simple_join() {
    let result = Parser::parse_sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.from.is_some());
            match select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Join { join_type, left, right, condition, natural } => {
                    // Default JOIN is INNER JOIN
                    assert_eq!(*join_type, vibesql_ast::JoinType::Inner);
                    assert!(!*natural);

                    // Left should be users table
                    match **left {
                        vibesql_ast::FromClause::Table { ref name, .. } if name == "USERS" => {} // Success
                        _ => panic!("Expected left table to be 'users'"),
                    }

                    // Right should be orders table
                    match **right {
                        vibesql_ast::FromClause::Table { ref name, .. } if name == "ORDERS" => {} /* Success */
                        _ => panic!("Expected right table to be 'orders'"),
                    }

                    // Should have ON condition
                    assert!(condition.is_some());
                }
                _ => panic!("Expected JOIN in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_inner_join() {
    let result =
        Parser::parse_sql("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::Inner);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_comma_separated_from() {
    let result = Parser::parse_sql("SELECT * FROM tab0, tab1 AS cor0;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.from.is_some());
            match select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Join { join_type, left, right, condition, natural } => {
                    // Comma should be parsed as CROSS JOIN
                    assert_eq!(*join_type, vibesql_ast::JoinType::Cross);
                    assert!(!*natural);

                    // Left should be tab0 table
                    match **left {
                        vibesql_ast::FromClause::Table { ref name, alias: None, .. }
                            if name == "TAB0" => {} /* Success */
                        _ => panic!("Expected left table to be 'tab0'"),
                    }

                    // Right should be tab1 table with alias cor0
                    match **right {
                        vibesql_ast::FromClause::Table {
                            ref name, alias: Some(ref alias), ..
                        } if name == "TAB1" && alias == "COR0" => {} // Success
                        _ => panic!("Expected right table to be 'tab1' with alias 'cor0'"),
                    }

                    // Should have no condition (CROSS JOIN)
                    assert!(condition.is_none());
                }
                _ => panic!("Expected JOIN in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_left_join() {
    let result =
        Parser::parse_sql("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::LeftOuter);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_left_outer_join() {
    let result = Parser::parse_sql(
        "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id;",
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::LeftOuter);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_right_join() {
    let result =
        Parser::parse_sql("SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::RightOuter);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_multiple_joins() {
    let result = Parser::parse_sql(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id;"
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Should have nested JOINs
            match select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Join { left, .. } => {
                    // Left should also be a JOIN
                    match **left {
                        vibesql_ast::FromClause::Join { .. } => {} // Success - nested JOIN
                        _ => panic!("Expected nested JOIN"),
                    }
                }
                _ => panic!("Expected JOIN"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}
