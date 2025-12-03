use super::super::*;

// ============================================================================
// Tests for SELECT ALL syntax (SQL:1999 E051-01)
// ============================================================================

#[test]
fn test_parse_select_all_literal() {
    let result = Parser::parse_sql("SELECT ALL 42;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // ALL means include duplicates (distinct = false)
            assert!(!select.distinct);
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
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_all_star() {
    let result = Parser::parse_sql("SELECT ALL *;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(!select.distinct);
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
fn test_parse_select_all_from_table() {
    let result = Parser::parse_sql("SELECT ALL A FROM T;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(!select.distinct);
            assert_eq!(select.select_list.len(), 1);
            assert!(select.from.is_some());
            match &select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Table { name, alias, .. } => {
                    assert_eq!(name, "T");
                    assert!(alias.is_none());
                }
                _ => panic!("Expected table in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_all_multiple_columns() {
    let result = Parser::parse_sql("SELECT ALL id, name FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(!select.distinct);
            assert_eq!(select.select_list.len(), 2);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_select_distinct_still_works() {
    // Verify DISTINCT still works after adding ALL support
    let result = Parser::parse_sql("SELECT DISTINCT id FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.distinct);
            assert_eq!(select.select_list.len(), 1);
        }
        _ => panic!("Expected SELECT statement"),
    }
}
