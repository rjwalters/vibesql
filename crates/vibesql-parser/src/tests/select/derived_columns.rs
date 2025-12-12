use super::super::*;

// ============================================================================
// Tests for derived column lists (SQL:1999 E051-07, E051-08)
// ============================================================================

#[test]
fn test_parse_select_wildcard_with_derived_column_list() {
    let result = Parser::parse_sql("SELECT * AS (C, D) FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Wildcard { alias } => {
                    assert!(alias.is_some());
                    let derived_cols = alias.as_ref().unwrap();
                    assert_eq!(derived_cols.len(), 2);
                    assert_eq!(derived_cols[0], "C");
                    assert_eq!(derived_cols[1], "D");
                }
                _ => panic!("Expected Wildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_all_wildcard_with_derived_column_list() {
    let result = Parser::parse_sql("SELECT ALL * AS (C, D) FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Wildcard { alias } => {
                    assert!(alias.is_some());
                    let derived_cols = alias.as_ref().unwrap();
                    assert_eq!(derived_cols.len(), 2);
                    assert_eq!(derived_cols[0], "C");
                    assert_eq!(derived_cols[1], "D");
                }
                _ => panic!("Expected Wildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_distinct_wildcard_with_derived_column_list() {
    let result = Parser::parse_sql("SELECT DISTINCT * AS (C, D) FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.distinct);
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Wildcard { alias } => {
                    assert!(alias.is_some());
                    let derived_cols = alias.as_ref().unwrap();
                    assert_eq!(derived_cols.len(), 2);
                    assert_eq!(derived_cols[0], "C");
                    assert_eq!(derived_cols[1], "D");
                }
                _ => panic!("Expected Wildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_qualified_wildcard_with_derived_column_list() {
    let result = Parser::parse_sql("SELECT table_name.* AS (C, D) FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, alias } => {
                    assert_eq!(qualifier, "table_name");
                    assert!(alias.is_some());
                    let derived_cols = alias.as_ref().unwrap();
                    assert_eq!(derived_cols.len(), 2);
                    assert_eq!(derived_cols[0], "C");
                    assert_eq!(derived_cols[1], "D");
                }
                _ => panic!("Expected QualifiedWildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_alias_wildcard_with_derived_column_list() {
    let result = Parser::parse_sql("SELECT t.* AS (C, D) FROM table_name AS t;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, alias } => {
                    assert_eq!(qualifier, "t");
                    assert!(alias.is_some());
                    let derived_cols = alias.as_ref().unwrap();
                    assert_eq!(derived_cols.len(), 2);
                    assert_eq!(derived_cols[0], "C");
                    assert_eq!(derived_cols[1], "D");
                }
                _ => panic!("Expected QualifiedWildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_wildcard_with_multiple_columns_in_derived_list() {
    let result = Parser::parse_sql("SELECT * AS (A, B, C, D, E) FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Wildcard { alias } => {
                    assert!(alias.is_some());
                    let derived_cols = alias.as_ref().unwrap();
                    assert_eq!(derived_cols.len(), 5);
                    assert_eq!(derived_cols[0], "A");
                    assert_eq!(derived_cols[1], "B");
                    assert_eq!(derived_cols[2], "C");
                    assert_eq!(derived_cols[3], "D");
                    assert_eq!(derived_cols[4], "E");
                }
                _ => panic!("Expected Wildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}
