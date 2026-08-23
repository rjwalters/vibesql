//! Tests for CAST function parsing
//!
//! Covers:
//! - CAST to integer types (SMALLINT, INTEGER, BIGINT)
//! - CAST to floating-point types (FLOAT, DOUBLE PRECISION)
//! - CAST to string types (VARCHAR)
//! - CAST to date/time types (DATE, TIME, TIMESTAMP)
//! - CAST to exact numeric types (NUMERIC)
//! - CAST in various contexts (WHERE clauses, nested expressions)

use super::*;

// ========================================================================
// CAST Function Tests
// ========================================================================

#[test]
fn test_parse_cast_integer_to_varchar() {
    let result = Parser::parse_sql("SELECT CAST(123 AS VARCHAR(10));");
    assert!(result.is_ok(), "CAST to VARCHAR should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => {
                    match expr {
                        vibesql_ast::Expression::Cast { expr: _, data_type } => {
                            match data_type {
                                vibesql_types::DataType::Varchar { max_length: Some(10) } => {} /* Success */
                                _ => panic!("Expected VARCHAR(10), got {:?}", data_type),
                            }
                        }
                        _ => panic!("Expected CAST expression, got {:?}", expr),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_varchar_to_integer() {
    let result = Parser::parse_sql("SELECT CAST('123' AS INTEGER);");
    assert!(result.is_ok(), "CAST to INTEGER should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => {
                    match expr {
                        vibesql_ast::Expression::Cast { expr: _, data_type } => {
                            match data_type {
                                vibesql_types::DataType::Integer => {} // Success
                                _ => panic!("Expected INTEGER, got {:?}", data_type),
                            }
                        }
                        _ => panic!("Expected CAST expression"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_to_signed() {
    let result = Parser::parse_sql("SELECT CAST(value AS SIGNED);");
    assert!(result.is_ok(), "CAST to SIGNED should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => {
                    match expr {
                        vibesql_ast::Expression::Cast { expr: _, data_type } => {
                            match data_type {
                                vibesql_types::DataType::Integer => {} // SIGNED maps to INTEGER
                                _ => panic!("Expected INTEGER (SIGNED), got {:?}", data_type),
                            }
                        }
                        _ => panic!("Expected CAST expression"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_to_unsigned() {
    let result = Parser::parse_sql("SELECT CAST(value AS UNSIGNED);");
    assert!(result.is_ok(), "CAST to UNSIGNED should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => {
                    match expr {
                        vibesql_ast::Expression::Cast { expr: _, data_type } => {
                            match data_type {
                                vibesql_types::DataType::Unsigned => {} /* UNSIGNED maps to */
                                // Unsigned
                                _ => panic!("Expected UNSIGNED, got {:?}", data_type),
                            }
                        }
                        _ => panic!("Expected CAST expression"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_to_smallint() {
    let result = Parser::parse_sql("SELECT CAST(value AS SMALLINT);");
    assert!(result.is_ok(), "CAST to SMALLINT should parse: {:?}", result);
}

#[test]
fn test_parse_cast_to_bigint() {
    let result = Parser::parse_sql("SELECT CAST(value AS BIGINT);");
    assert!(result.is_ok(), "CAST to BIGINT should parse: {:?}", result);
}

#[test]
fn test_parse_cast_to_float() {
    let result = Parser::parse_sql("SELECT CAST(123 AS FLOAT);");
    assert!(result.is_ok(), "CAST to FLOAT should parse: {:?}", result);
}

#[test]
fn test_parse_cast_to_double_precision() {
    let result = Parser::parse_sql("SELECT CAST(123 AS DOUBLE PRECISION);");
    assert!(result.is_ok(), "CAST to DOUBLE PRECISION should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => {
                    match expr {
                        vibesql_ast::Expression::Cast { expr: _, data_type } => {
                            match data_type {
                                vibesql_types::DataType::DoublePrecision => {} // Success
                                _ => panic!("Expected DOUBLE PRECISION, got {:?}", data_type),
                            }
                        }
                        _ => panic!("Expected CAST expression"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_to_numeric() {
    let result = Parser::parse_sql("SELECT CAST(value AS NUMERIC(10, 2));");
    assert!(result.is_ok(), "CAST to NUMERIC should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => {
                    match expr {
                        vibesql_ast::Expression::Cast { expr: _, data_type } => {
                            match data_type {
                                vibesql_types::DataType::Numeric { precision: 10, scale: 2 } => {} /* Success */
                                _ => panic!("Expected NUMERIC(10, 2), got {:?}", data_type),
                            }
                        }
                        _ => panic!("Expected CAST expression"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_to_date() {
    let result = Parser::parse_sql("SELECT CAST('2024-01-01' AS DATE);");
    assert!(result.is_ok(), "CAST to DATE should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => {
                    match expr {
                        vibesql_ast::Expression::Cast { expr: _, data_type } => {
                            match data_type {
                                vibesql_types::DataType::Date => {} // Success
                                _ => panic!("Expected DATE, got {:?}", data_type),
                            }
                        }
                        _ => panic!("Expected CAST expression"),
                    }
                }
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_to_time() {
    let result = Parser::parse_sql("SELECT CAST(value AS TIME);");
    assert!(result.is_ok(), "CAST to TIME should parse: {:?}", result);
}

#[test]
fn test_parse_cast_to_timestamp() {
    let result = Parser::parse_sql("SELECT CAST(value AS TIMESTAMP);");
    assert!(result.is_ok(), "CAST to TIMESTAMP should parse: {:?}", result);
}

#[test]
fn test_parse_cast_in_where_clause() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE CAST(age AS VARCHAR(10)) = '25';");
    assert!(result.is_ok(), "CAST in WHERE should parse: {:?}", result);
}

#[test]
fn test_parse_cast_nested_expression() {
    let result = Parser::parse_sql("SELECT CAST((value + 10) AS BIGINT);");
    assert!(result.is_ok(), "CAST with nested expression should parse: {:?}", result);
}

#[test]
fn test_parse_multiple_casts() {
    let result =
        Parser::parse_sql("SELECT CAST(a AS INTEGER), CAST(b AS VARCHAR(20)), CAST(c AS FLOAT);");
    assert!(result.is_ok(), "Multiple CASTs should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3, "Should have 3 select items");
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_signed_in_aggregate() {
    // Test case from issue #949: CAST AS SIGNED works fine
    let result = Parser::parse_sql("SELECT DISTINCT - MIN( CAST( NULL AS SIGNED ) ) FROM tab0");
    assert!(result.is_ok(), "CAST AS SIGNED in aggregate should parse: {:?}", result);
}

#[test]
fn test_parse_cast_signed_cross_join_subquery() {
    // Full test case from issue #949: random/aggregates/slt_good_56.test
    // The issue is actually the FROM clause with parenthesized table reference, not CAST AS SIGNED
    let result = Parser::parse_sql(
        "SELECT DISTINCT - MIN( CAST( NULL AS SIGNED ) ) FROM ( tab0 AS cor0 CROSS JOIN tab2 AS cor1 )"
    );
    assert!(result.is_ok(), "CAST AS SIGNED with CROSS JOIN subquery should parse: {:?}", result);
}

#[test]
fn test_parse_cast_empty_typename() {
    // SQLite tolerates a missing type name: CAST(x AS ) gets NUMERIC
    // affinity, same as an unrecognized type name (sqlite3AffinityType("")
    // falls through to NUMERIC). From window1.test 61.1; verified against
    // SQLite 3.51: CAST('seventeen' AS ) → 0 (typeof integer).
    let result = Parser::parse_sql("SELECT CAST(a AS ) FROM t1;");
    assert!(result.is_ok(), "CAST with empty type name should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::Cast { data_type, .. } => {
                    assert_eq!(
                        data_type,
                        &vibesql_types::DataType::Numeric { precision: 38, scale: 0 },
                        "empty type name should map to NUMERIC affinity"
                    );
                }
                _ => panic!("Expected CAST expression, got {:?}", expr),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_cast_empty_typename_in_aggregate() {
    // Regression for window1.test 61.1 (fuzz-derived): CAST(a AS ) inside sum()
    let result = Parser::parse_sql("SELECT sum(CAST(a AS )) FROM t1;");
    assert!(result.is_ok(), "sum(CAST(a AS )) should parse: {:?}", result);
}

// ========================================================================
// CAST to a string-literal type name (issue #6172, e_expr-12.3.50/.51)
//
// SQLite's `ids` production is `ID | STRING`, so a string literal is legal
// wherever an identifier is legal inside a type name. `CAST(x AS 'abcd')`
// therefore parses, with the string's contents becoming the (semantically
// arbitrary) type name verbatim; affinity is then derived from that name
// like any other unrecognized type name (NUMERIC here, since neither
// string matches the CHAR/INT/BLOB/REAL/DOUB/FLOA substring rules).
// ========================================================================

/// Extract the `data_type` of the first select-list item, asserting it is a CAST.
fn cast_data_type(sql: &str) -> vibesql_types::DataType {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("should parse: {}: {:?}", sql, e));
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::Cast { data_type, .. } => data_type.clone(),
                other => panic!("Expected CAST expression, got {:?}", other),
            },
            other => panic!("Expected expression select item, got {:?}", other),
        },
        other => panic!("Expected SELECT statement, got {:?}", other),
    }
}

#[test]
fn test_parse_cast_string_literal_typename(/* issue #6172 */) {
    // e_expr-12.3.50: CAST(x AS 'abcd') is a hard parse error before the fix.
    assert_eq!(
        cast_data_type("SELECT CAST(1 AS 'abcd');"),
        vibesql_types::DataType::UserDefined { type_name: "abcd".to_string() },
        "string-literal type name should be kept verbatim as a UserDefined type"
    );
}

#[test]
fn test_parse_cast_string_literal_typename_with_punctuation(/* issue #6172 */) {
    // e_expr-12.3.51: the string's contents are opaque — spaces and `$` are
    // fine, since it never has to be a legal identifier.
    assert_eq!(
        cast_data_type("SELECT CAST(1 AS 'ab$ $cd');"),
        vibesql_types::DataType::UserDefined { type_name: "ab$ $cd".to_string() },
        "punctuation/space inside the string-literal type name must survive verbatim"
    );
}

#[test]
fn test_parse_cast_string_literal_typename_preserves_case(/* issue #6172 */) {
    // Unlike a bare identifier (lowercased by the lexer), a string literal's
    // contents are not normalized — the type name keeps the user's case.
    assert_eq!(
        cast_data_type("SELECT CAST(x AS 'MixedCase') FROM t1;"),
        vibesql_types::DataType::UserDefined { type_name: "MixedCase".to_string() },
        "string-literal type name should not be case-normalized"
    );
}

#[test]
fn test_parse_cast_string_literal_typename_with_size(/* issue #6172 */) {
    // Same as the delimited-identifier form: an optional size specifier may
    // follow the type name and is discarded for an opaque type.
    assert_eq!(
        cast_data_type("SELECT CAST(1 AS 'abcd'(10));"),
        vibesql_types::DataType::UserDefined { type_name: "abcd".to_string() },
        "optional size specifier after a string-literal type name should parse"
    );
}

#[test]
fn test_parse_cast_string_literal_typename_in_aggregate(/* issue #6172 */) {
    // The fix must hold in nested expression contexts too.
    let result = Parser::parse_sql("SELECT sum(CAST(a AS 'abcd')) FROM t1;");
    assert!(result.is_ok(), "sum(CAST(a AS 'abcd')) should parse: {:?}", result);
}
