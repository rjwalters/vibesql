use super::super::*;

// ============================================================================

#[test]
fn test_parse_select_42() {
    let result = Parser::parse_sql("SELECT 42;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias } => {
                    assert!(alias.is_none());
                    match expr {
                        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42)) => {} // Success
                        _ => panic!("Expected Integer(42), got {:?}", expr),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
            assert!(select.from.is_none());
            assert!(select.where_clause.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_string() {
    let result = Parser::parse_sql("SELECT 'hello';");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s))
                        if s.as_str() == "hello" => {} /* Success */
                    _ => panic!("Expected Varchar('hello'), got {:?}", expr),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_arithmetic() {
    let result = Parser::parse_sql("SELECT 1 + 2;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::BinaryOp { op, left, right } => {
                        assert_eq!(*op, vibesql_ast::BinaryOperator::Plus);
                        match **left {
                            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                                1,
                            )) => {}
                            _ => panic!("Expected left = 1"),
                        }
                        match **right {
                            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                                2,
                            )) => {}
                            _ => panic!("Expected right = 2"),
                        }
                    }
                    _ => panic!("Expected BinaryOp, got {:?}", expr),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_star() {
    let result = Parser::parse_sql("SELECT *;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Wildcard { alias: _ } => {} // Success
                _ => panic!("Expected Wildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_from_table() {
    let result = Parser::parse_sql("SELECT * FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.from.is_some());
            match &select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Table { name, alias, .. } => {
                    assert_eq!(name, "USERS");
                    assert!(alias.is_none());
                }
                _ => panic!("Expected table in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_columns() {
    let result = Parser::parse_sql("SELECT id, name, age FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);

            // Check first column (id)
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef { column, .. } if column == "ID" => {}
                    _ => panic!("Expected id column"),
                },
                _ => panic!("Expected Expression select item"),
            }

            // Check second column (name)
            match &select.select_list[1] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef { column, .. } if column == "NAME" => {}
                    _ => panic!("Expected name column"),
                },
                _ => panic!("Expected Expression select item"),
            }

            // Check third column (age)
            match &select.select_list[2] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef { column, .. } if column == "AGE" => {}
                    _ => panic!("Expected age column"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_current_date() {
    let result = Parser::parse_sql("SELECT CURRENT_DATE;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias } => {
                    assert!(alias.is_none());
                    match expr {
                        vibesql_ast::Expression::CurrentDate => {}
                        _ => panic!("Expected CurrentDate, got {:?}", expr),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
            assert!(select.from.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_current_time() {
    let result = Parser::parse_sql("SELECT CURRENT_TIME;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias } => {
                    assert!(alias.is_none());
                    match expr {
                        vibesql_ast::Expression::CurrentTime { precision } => {
                            assert_eq!(*precision, None);
                        }
                        _ => panic!("Expected CurrentTime, got {:?}", expr),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
            assert!(select.from.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_current_timestamp() {
    let result = Parser::parse_sql("SELECT CURRENT_TIMESTAMP;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias } => {
                    assert!(alias.is_none());
                    match expr {
                        vibesql_ast::Expression::CurrentTimestamp { precision } => {
                            assert_eq!(*precision, None);
                        }
                        _ => panic!("Expected CurrentTimestamp, got {:?}", expr),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
            assert!(select.from.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_qualified_wildcard() {
    let result = Parser::parse_sql("SELECT table_name.* FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, alias: _ } => {
                    assert_eq!(qualifier, "TABLE_NAME");
                }
                _ => panic!(
                    "Expected QualifiedWildcard select item, got {:?}",
                    select.select_list[0]
                ),
            }
            // Check FROM clause exists
            assert!(select.from.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_qualified_wildcard_alias() {
    let result = Parser::parse_sql("SELECT alias.* FROM table_name AS alias;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, alias: _ } => {
                    assert_eq!(qualifier, "ALIAS");
                }
                _ => panic!("Expected QualifiedWildcard select item"),
            }
            assert!(select.from.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}
