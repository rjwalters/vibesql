use super::super::*;

// ============================================================================
// Backtick Identifier Tests (MySQL-style)
// ============================================================================

#[test]
fn test_parse_select_with_backtick_column_names() {
    let result = Parser::parse_sql("SELECT `user_id`, `user_name` FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 2);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias } => {
                    assert!(alias.is_none());
                    match expr {
                        vibesql_ast::Expression::ColumnRef { column, .. } => {
                            // Backtick identifiers preserve case
                            assert_eq!(column, "user_id");
                        }
                        _ => panic!("Expected ColumnRef"),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_with_backtick_table_name() {
    let result = Parser::parse_sql("SELECT * FROM `user_table`;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.from.is_some());
            match &select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Table { name, alias, .. } => {
                    // Backtick identifiers preserve case
                    assert_eq!(name, "user_table");
                    assert!(alias.is_none());
                }
                _ => panic!("Expected Table in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_with_backtick_qualified_column() {
    let result = Parser::parse_sql("SELECT `my_table`.`my_column` FROM `my_table`;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef { column, table, .. } => {
                        assert_eq!(column, "my_column");
                        assert_eq!(table.as_ref().unwrap(), "my_table");
                    }
                    _ => panic!("Expected qualified ColumnRef"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_with_backtick_reserved_words() {
    // Reserved words can be used as identifiers when backtick-quoted
    let result = Parser::parse_sql("SELECT `select`, `from` FROM `where`;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 2);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "select");
                    }
                    _ => panic!("Expected ColumnRef"),
                },
                _ => panic!("Expected Expression select item"),
            }
            // Check FROM clause
            match &select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Table { name, .. } => {
                    assert_eq!(name, "where");
                }
                _ => panic!("Expected Table in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_mixed_backtick_and_regular() {
    let result = Parser::parse_sql("SELECT id, `userName`, status FROM `MyTable`;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);
            // First column (regular identifier - uppercased)
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "ID");
                    }
                    _ => panic!("Expected ColumnRef"),
                },
                _ => panic!("Expected Expression select item"),
            }
            // Second column (backtick - preserves case)
            match &select.select_list[1] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "userName");
                    }
                    _ => panic!("Expected ColumnRef"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}
