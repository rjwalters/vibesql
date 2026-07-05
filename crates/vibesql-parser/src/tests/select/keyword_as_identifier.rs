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
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        let column = col_id.column_canonical();
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
    assert!(result.is_ok(), "Failed to parse WHERE with m column: {:?}", result);
}

#[test]
fn test_column_m_in_create_table() {
    // From issue: CREATE TABLE t(i, j, k, m, n);
    let result = Parser::parse_sql("CREATE TABLE t(i, j, k, m, n);");
    assert!(result.is_ok(), "Failed to parse CREATE TABLE with m column: {:?}", result);
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
    assert!(result.is_ok(), "Failed to parse SELECT M FROM t: {:?}", result);
}

/// Issue #5842 sub-item 3: an unquoted alias that happens to be a contextual
/// keyword (`m`, the HNSW parameter) must preserve its original source-text
/// case, not be canonicalized to the keyword's uppercase form. SQLite names the
/// column exactly as written: `SELECT max(a) AS m` yields column `m`, not `M`
/// (colname-6.11..6.19). Cover both the standard and arena parser paths since
/// they have independent alias-parsing routines.
fn alias_of_first_select_item(stmt: vibesql_ast::Statement) -> Option<String> {
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { alias, .. } => alias.clone(),
            _ => None,
        },
        _ => None,
    }
}

#[test]
fn test_keyword_alias_preserves_case_standard_parser() {
    let stmt = Parser::parse_sql("SELECT max(a) AS m FROM t").unwrap();
    assert_eq!(alias_of_first_select_item(stmt).as_deref(), Some("m"));

    let stmt = Parser::parse_sql("SELECT 1 AS m").unwrap();
    assert_eq!(alias_of_first_select_item(stmt).as_deref(), Some("m"));

    // Uppercase spelling is preserved as written too.
    let stmt = Parser::parse_sql("SELECT 1 AS M").unwrap();
    assert_eq!(alias_of_first_select_item(stmt).as_deref(), Some("M"));
}

#[test]
fn test_keyword_alias_preserves_case_arena_parser() {
    let stmt = crate::parse_with_arena_fallback("SELECT max(a) AS m FROM t").unwrap();
    assert_eq!(alias_of_first_select_item(stmt).as_deref(), Some("m"));

    let stmt = crate::parse_with_arena_fallback("SELECT 1 AS m").unwrap();
    assert_eq!(alias_of_first_select_item(stmt).as_deref(), Some("m"));

    let stmt = crate::parse_with_arena_fallback("SELECT 1 AS M").unwrap();
    assert_eq!(alias_of_first_select_item(stmt).as_deref(), Some("M"));
}

#[test]
fn test_column_m_in_expression() {
    let result = Parser::parse_sql("SELECT m + 1 FROM t;");
    assert!(result.is_ok(), "Failed to parse m in expression: {:?}", result);
}

// Issue #5670: otherwise-reserved keywords must be accepted as column references
// in SELECT-list / expression position and as table names in FROM position, the
// way SQLite does (table.test table-7.3). The CREATE/INSERT/UPDATE side was fixed
// in #5674; these tests cover the read/expression side.

#[test]
fn test_keyword_release_as_column_in_select() {
    // The exact repro from issue #5670.
    let result = Parser::parse_sql("SELECT release FROM savepoint;");
    assert!(result.is_ok(), "Failed to parse SELECT release FROM savepoint: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        assert_eq!(col_id.column_canonical(), "release");
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
fn test_keyword_release_in_where_and_arithmetic() {
    assert!(Parser::parse_sql("SELECT release + 1 FROM savepoint;").is_ok());
    assert!(Parser::parse_sql("SELECT release FROM savepoint WHERE release = 10;").is_ok());
}

#[test]
fn test_keyword_table_name_in_from() {
    // `savepoint` is a keyword; it must be usable as a table name in FROM position
    // (it was already usable in CREATE TABLE via #5674).
    let result = Parser::parse_sql("SELECT release FROM savepoint;");
    assert!(result.is_ok(), "keyword table name in FROM failed: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from {
            Some(vibesql_ast::FromClause::Table { name, .. }) => {
                // `parse_table_ref` carries the keyword through via its Display form
                // (uppercase); table-name resolution is case-insensitive, so the
                // end-to-end repro resolves regardless. Assert case-insensitively.
                assert!(
                    name.eq_ignore_ascii_case("savepoint"),
                    "expected table name savepoint, got {name:?}"
                );
            }
            other => panic!("Expected FROM table savepoint, got {:?}", other),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_assorted_keyword_column_refs() {
    // A spread of otherwise-reserved, non-operator/non-clause keywords used as
    // column references in expression position.
    for sql in [
        "SELECT asc FROM t;",
        "SELECT desc FROM t;",
        "SELECT key FROM t;",
        "SELECT begin FROM t;",
        "SELECT commit FROM t;",
        "SELECT rollback FROM t;",
        "SELECT match FROM t;",
        "SELECT column FROM t;",
        "SELECT index FROM t;",
    ] {
        assert!(Parser::parse_sql(sql).is_ok(), "expected to parse: {sql}");
    }
}

#[test]
fn test_order_by_keyword_column_disambiguates_direction() {
    // `ORDER BY asc DESC` must parse column `asc` with direction DESC, not as a
    // bare direction. The expression parser stops before ASC/DESC because they are
    // not operators, so they bubble up to the ORDER BY direction slot.
    let stmt = Parser::parse_sql("SELECT asc FROM t ORDER BY asc DESC;")
        .expect("ORDER BY asc DESC should parse");
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            let order_by = select.order_by.expect("expected ORDER BY");
            assert_eq!(order_by.len(), 1);
            assert_eq!(order_by[0].direction, vibesql_ast::OrderDirection::Desc);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

// --- Ambiguity-safety: reserved operator/clause keywords MUST stay reserved ---

#[test]
fn test_reserved_operators_not_treated_as_columns() {
    // These must still parse as their operator/keyword meaning, NOT as column refs.
    assert!(Parser::parse_sql("SELECT a FROM t WHERE a AND b;").is_ok());
    assert!(Parser::parse_sql("SELECT a FROM t WHERE NOT a;").is_ok());
    assert!(Parser::parse_sql("SELECT a FROM t WHERE a IS NULL;").is_ok());
    assert!(Parser::parse_sql("SELECT a FROM t WHERE a IS NOT NULL;").is_ok());
    assert!(Parser::parse_sql("SELECT a FROM t WHERE a IN (1, 2, 3);").is_ok());
    assert!(Parser::parse_sql("SELECT a FROM t WHERE a BETWEEN 1 AND 5;").is_ok());
    assert!(Parser::parse_sql("SELECT a FROM t WHERE a LIKE '%x%';").is_ok());
    assert!(Parser::parse_sql("SELECT a FROM t WHERE a = 1 OR b = 2;").is_ok());
    assert!(Parser::parse_sql("SELECT a FROM t WHERE EXISTS (SELECT 1 FROM t);").is_ok());
    assert!(Parser::parse_sql("SELECT a FROM t WHERE a IN (SELECT b FROM t);").is_ok());
}

#[test]
fn test_reserved_keywords_in_operand_position_still_error() {
    // A reserved clause/operator keyword where an operand is expected must remain
    // a syntax error — it must NOT be silently accepted as a column reference.
    for sql in [
        "SELECT FROM t;",                   // FROM where a select item is expected
        "SELECT a, FROM t;",                // trailing comma then FROM
        "SELECT select FROM t;",            // SELECT in operand position
        "SELECT a FROM t WHERE and b;",     // AND with no left operand
        "SELECT a FROM t WHERE a = where;", // WHERE in operand position
    ] {
        assert!(Parser::parse_sql(sql).is_err(), "expected a syntax error for: {sql}");
    }
}
