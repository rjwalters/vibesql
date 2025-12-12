use super::super::*;

/// Tests for using keywords as identifiers (column names).
/// Issue #4302: Parser should allow 'M' as column name since it's only
/// contextually reserved for HNSW vector index configuration.

#[test]
fn test_column_m_in_select() {
    // This was failing with: "Parse error: Expected expression, found Keyword(M)"
    let result = Parser::parse_sql("SELECT m FROM t;");
    assert!(result.is_ok(), "Failed to parse SELECT m FROM t: {:?}", result);
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef { column, .. } => {
                        // Identifiers preserve original case from SQL
                        assert_eq!(column, "m");
                    }
                    _ => panic!("Expected ColumnRef, got {:?}", expr),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_column_m_in_where_clause() {
    // From issue: SELECT k FROM t WHERE (j=1 AND m=1);
    let result = Parser::parse_sql("SELECT k FROM t WHERE j=1 AND m=1;");
    assert!(
        result.is_ok(),
        "Failed to parse WHERE with m column: {:?}",
        result
    );
}

#[test]
fn test_column_m_in_create_table() {
    // From issue: CREATE TABLE t(i, j, k, m, n);
    let result = Parser::parse_sql("CREATE TABLE t(i, j, k, m, n);");
    assert!(
        result.is_ok(),
        "Failed to parse CREATE TABLE with m column: {:?}",
        result
    );
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 5);
            let column_names: Vec<&str> = create.columns.iter().map(|c| c.name.as_str()).collect();
            // Identifiers preserve original case from SQL
            assert!(
                column_names.contains(&"m"),
                "Column 'm' not found in columns: {:?}",
                column_names
            );
        }
        _ => panic!("Expected CREATE TABLE statement"),
    }
}

#[test]
fn test_column_m_uppercase() {
    let result = Parser::parse_sql("SELECT M FROM t;");
    assert!(
        result.is_ok(),
        "Failed to parse SELECT M FROM t: {:?}",
        result
    );
}

#[test]
fn test_column_m_in_expression() {
    let result = Parser::parse_sql("SELECT m + 1 FROM t;");
    assert!(
        result.is_ok(),
        "Failed to parse m in expression: {:?}",
        result
    );
}
