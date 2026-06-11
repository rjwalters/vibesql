//! Tests for SQL literal parsing
//!
//! Covers:
//! - DATE literals: DATE '2024-01-01'
//! - TIME literals: TIME '14:30:00'
//! - TIMESTAMP literals: TIMESTAMP '2024-01-01 14:30:00'
//! - INTERVAL literals: INTERVAL '5' YEAR

use super::*;

// ========================================================================
// DATE Literal Tests
// ========================================================================

#[test]
fn test_parse_date_literal() {
    let result = Parser::parse_sql("SELECT DATE '2024-01-01';");
    assert!(result.is_ok(), "DATE literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(s)) => {
                        assert_eq!(s.to_string(), "2024-01-01");
                    }
                    _ => panic!("Expected DATE literal, got {:?}", expr),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_date_literal_in_where() {
    let result = Parser::parse_sql("SELECT * FROM events WHERE event_date = DATE '2024-12-25';");
    assert!(result.is_ok(), "DATE literal in WHERE should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.where_clause.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_date_literal_various_formats() {
    let test_cases = vec![
        "SELECT DATE '2024-01-01';",
        "SELECT DATE '2024-12-31';",
        "SELECT DATE '2000-02-29';", // Leap year
        "SELECT DATE '1999-01-01';",
    ];

    for sql in test_cases {
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "Should parse '{}': {:?}", sql, result);
    }
}

// ========================================================================
// TIME Literal Tests
// ========================================================================

#[test]
fn test_parse_time_literal() {
    let result = Parser::parse_sql("SELECT TIME '14:30:00';");
    assert!(result.is_ok(), "TIME literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Time(s)) => {
                        assert_eq!(s.to_string(), "14:30:00");
                    }
                    _ => panic!("Expected TIME literal, got {:?}", expr),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_time_literal_with_seconds() {
    let result = Parser::parse_sql("SELECT TIME '23:59:59';");
    assert!(result.is_ok(), "TIME with seconds should parse: {:?}", result);
}

#[test]
fn test_parse_time_literal_midnight() {
    let result = Parser::parse_sql("SELECT TIME '00:00:00';");
    assert!(result.is_ok(), "TIME midnight should parse: {:?}", result);
}

#[test]
fn test_parse_time_literal_with_fractional_seconds() {
    let result = Parser::parse_sql("SELECT TIME '14:30:00.123';");
    assert!(result.is_ok(), "TIME with fractional seconds should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Time(s)) => {
                    assert_eq!(s.to_string(), "14:30:00.123");
                }
                _ => panic!("Expected TIME literal"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

// ========================================================================
// TIMESTAMP Literal Tests
// ========================================================================

#[test]
fn test_parse_timestamp_literal() {
    let result = Parser::parse_sql("SELECT TIMESTAMP '2024-01-01 14:30:00';");
    assert!(result.is_ok(), "TIMESTAMP literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Timestamp(s)) => {
                        assert_eq!(s.to_string(), "2024-01-01 14:30:00");
                    }
                    _ => panic!("Expected TIMESTAMP literal, got {:?}", expr),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_timestamp_literal_with_fractional_seconds() {
    let result = Parser::parse_sql("SELECT TIMESTAMP '2024-01-01 14:30:00.123456';");
    assert!(result.is_ok(), "TIMESTAMP with fractional seconds should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Timestamp(s)) => {
                    assert_eq!(s.to_string(), "2024-01-01 14:30:00.123456");
                }
                _ => panic!("Expected TIMESTAMP literal"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_timestamp_literal_in_insert() {
    let result = Parser::parse_sql(
        "INSERT INTO logs (id, created) VALUES (1, TIMESTAMP '2024-01-01 12:00:00');",
    );
    assert!(result.is_ok(), "TIMESTAMP in INSERT should parse: {:?}", result);
}

// ========================================================================
// INTERVAL Literal Tests
// ========================================================================

#[test]
fn test_parse_interval_year_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '5' YEAR;");
    assert!(result.is_ok(), "INTERVAL YEAR literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Interval(s)) => {
                        assert_eq!(s.to_string(), "5 YEAR");
                    }
                    _ => panic!("Expected INTERVAL literal, got {:?}", expr),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_interval_month_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '3' MONTH;");
    assert!(result.is_ok(), "INTERVAL MONTH literal should parse: {:?}", result);
}

#[test]
fn test_parse_interval_day_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '30' DAY;");
    assert!(result.is_ok(), "INTERVAL DAY literal should parse: {:?}", result);
}

#[test]
fn test_parse_interval_hour_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '24' HOUR;");
    assert!(result.is_ok(), "INTERVAL HOUR literal should parse: {:?}", result);
}

#[test]
fn test_parse_interval_year_to_month_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '1-6' YEAR TO MONTH;");
    assert!(result.is_ok(), "INTERVAL YEAR TO MONTH literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Interval(s)) => {
                    assert_eq!(s.to_string(), "1-6 YEAR TO MONTH");
                }
                _ => panic!("Expected INTERVAL literal"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_interval_day_to_hour_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '5 12' DAY TO HOUR;");
    assert!(result.is_ok(), "INTERVAL DAY TO HOUR literal should parse: {:?}", result);
}

#[test]
fn test_parse_interval_day_to_second_literal() {
    let result = Parser::parse_sql("SELECT INTERVAL '5 12:30:45' DAY TO SECOND;");
    assert!(result.is_ok(), "INTERVAL DAY TO SECOND literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Interval(s)) => {
                    assert_eq!(s.to_string(), "5 12:30:45 DAY TO SECOND");
                }
                _ => panic!("Expected INTERVAL literal"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

// ========================================================================
// Mixed Literal Tests
// ========================================================================

#[test]
fn test_parse_mixed_date_time_literals() {
    let result = Parser::parse_sql(
        "SELECT DATE '2024-01-01', TIME '14:30:00', TIMESTAMP '2024-01-01 14:30:00';",
    );
    assert!(result.is_ok(), "Mixed date/time literals should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_date_time_literals_in_comparison() {
    let result = Parser::parse_sql(
        "SELECT * FROM events WHERE event_date >= DATE '2024-01-01' AND start_time < TIME '18:00:00';"
    );
    assert!(result.is_ok(), "Date/time literals in comparison should parse: {:?}", result);
}

// ========================================================================
// Hex and Binary Literal Tests
// ========================================================================

#[test]
fn test_parse_hex_literal_lowercase() {
    let result = Parser::parse_sql("SELECT x'303132';");
    assert!(result.is_ok(), "Hex literal x'303132' should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Blob(bytes)) => {
                        // x'303132' = bytes [0x30, 0x31, 0x32]
                        assert_eq!(bytes, &[0x30, 0x31, 0x32]);
                    }
                    _ => panic!("Expected Blob literal, got {:?}", expr),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_hex_literal_uppercase() {
    let result = Parser::parse_sql("SELECT X'48656C6C6F';");
    assert!(result.is_ok(), "Hex literal X'48656C6C6F' should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Blob(bytes)) => {
                    // X'48656C6C6F' = bytes for "Hello"
                    assert_eq!(bytes, &[0x48, 0x65, 0x6C, 0x6C, 0x6F]);
                }
                _ => panic!("Expected Blob literal, got {:?}", expr),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_hex_literal_empty() {
    let result = Parser::parse_sql("SELECT x'';");
    assert!(result.is_ok(), "Empty hex literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Blob(bytes)) => {
                    // Empty blob
                    assert!(bytes.is_empty());
                }
                _ => panic!("Expected Blob literal, got {:?}", expr),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_hex_literal_odd_length_fails() {
    let result = Parser::parse_sql("SELECT x'123';");
    assert!(result.is_err(), "Hex literal with odd length should fail");
}

#[test]
fn test_parse_hex_literal_invalid_digit_fails() {
    let result = Parser::parse_sql("SELECT x'12GH';");
    assert!(result.is_err(), "Hex literal with invalid digit should fail");
}

#[test]
fn test_parse_binary_literal_lowercase() {
    let result = Parser::parse_sql("SELECT b'01010101';");
    assert!(result.is_ok(), "Binary literal b'01010101' should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s)) => {
                    // b'01010101' = byte 0x55 = "U"
                    assert_eq!(s.as_str(), "U");
                }
                _ => panic!("Expected VARCHAR literal"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_binary_literal_uppercase() {
    let result = Parser::parse_sql("SELECT B'01000001';");
    assert!(result.is_ok(), "Binary literal B'01000001' should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s)) => {
                    // B'01000001' = byte 0x41 = "A"
                    assert_eq!(s.as_str(), "A");
                }
                _ => panic!("Expected VARCHAR literal"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_binary_literal_invalid_length_fails() {
    let result = Parser::parse_sql("SELECT b'0101';");
    assert!(result.is_err(), "Binary literal not divisible by 8 should fail");
}

#[test]
fn test_parse_binary_literal_invalid_digit_fails() {
    let result = Parser::parse_sql("SELECT b'01012345';");
    assert!(result.is_err(), "Binary literal with invalid digit should fail");
}

#[test]
fn test_parse_hex_literal_in_expression() {
    let result = Parser::parse_sql("SELECT x'303132' IN (SELECT * FROM t1);");
    assert!(result.is_ok(), "Hex literal in IN expression should parse: {:?}", result);
}

#[test]
fn test_parse_hex_literal_in_comparison() {
    let result = Parser::parse_sql("SELECT * FROM t WHERE col = x'ABCD';");
    assert!(result.is_ok(), "Hex literal in comparison should parse: {:?}", result);
}

// ========================================================================
// Large Integer Literal Tests
// ========================================================================

#[test]
fn test_parse_i64_min_literal() {
    // i64::MIN (-9223372036854775808) is a special case:
    // The positive value 9223372036854775808 overflows i64, but when negated
    // it becomes i64::MIN which is valid. This should parse as Integer, not Numeric.
    let result = Parser::parse_sql("SELECT -9223372036854775808;");
    assert!(result.is_ok(), "i64::MIN literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(i)) => {
                        assert_eq!(*i, i64::MIN);
                    }
                    _ => panic!("Expected Integer literal for i64::MIN, got {:?}", expr),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_i64_max_literal() {
    // i64::MAX (9223372036854775807) should parse as Integer
    let result = Parser::parse_sql("SELECT 9223372036854775807;");
    assert!(result.is_ok(), "i64::MAX literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(i)) => {
                    assert_eq!(*i, i64::MAX);
                }
                _ => panic!("Expected Integer literal for i64::MAX"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_large_positive_as_numeric() {
    // Values larger than i64::MAX should parse as Numeric
    let result = Parser::parse_sql("SELECT 9223372036854775808;");
    assert!(result.is_ok(), "Large positive literal should parse: {:?}", result);

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => match expr {
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Numeric(_)) => {
                    // This is correct - too large for i64, so it becomes Numeric
                }
                _ => panic!("Expected Numeric literal for value > i64::MAX, got {:?}", expr),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

// ========================================================================
// DATE/TIME as Function Calls (issue #5307)
//
// SQLite supports date('now', ...) and time('now', ...) as scalar
// functions. The DATE/TIME keywords must only be treated as typed-literal
// introducers when followed by a string literal, not by '('.
// ========================================================================

/// Extract the expression of the first select item.
fn first_select_expr(stmt: &vibesql_ast::Statement) -> &vibesql_ast::Expression {
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => expr,
            other => panic!("Expected expression select item, got {:?}", other),
        },
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_date_function_call() {
    // Recursive-descent parser path
    let result = Parser::parse_sql("SELECT date('now');");
    assert!(result.is_ok(), "date('now') should parse as a function call: {:?}", result);

    match first_select_expr(&result.unwrap()) {
        vibesql_ast::Expression::Function { name, args, .. } => {
            assert_eq!(name, "date");
            assert_eq!(args.len(), 1);
        }
        other => panic!("Expected function call, got {:?}", other),
    }
}

#[test]
fn test_parse_time_function_call() {
    // Recursive-descent parser path
    let result = Parser::parse_sql("SELECT time('12:00:00');");
    assert!(result.is_ok(), "time('12:00:00') should parse as a function call: {:?}", result);

    match first_select_expr(&result.unwrap()) {
        vibesql_ast::Expression::Function { name, args, .. } => {
            assert_eq!(name, "time");
            assert_eq!(args.len(), 1);
        }
        other => panic!("Expected function call, got {:?}", other),
    }
}

#[test]
fn test_parse_date_function_call_with_modifiers() {
    let result = Parser::parse_sql("SELECT date('2024-01-01', '+1 day');");
    assert!(result.is_ok(), "date() with modifiers should parse: {:?}", result);

    match first_select_expr(&result.unwrap()) {
        vibesql_ast::Expression::Function { name, args, .. } => {
            assert_eq!(name, "date");
            assert_eq!(args.len(), 2);
        }
        other => panic!("Expected function call, got {:?}", other),
    }
}

#[test]
fn test_parse_date_time_function_call_arena() {
    // Arena parser path (no silent fallback to the recursive-descent parser)
    let result = crate::arena_parser::parse_select_to_owned(
        "SELECT date('now', 'start of month'), time('now', '+1 hour')",
    );
    assert!(result.is_ok(), "arena parser should accept date()/time() calls: {:?}", result);

    let select = result.unwrap();
    assert_eq!(select.select_list.len(), 2);
    for (item, expected) in select.select_list.iter().zip(["date", "time"]) {
        match item {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::Function { name, args, .. } => {
                    assert_eq!(name, expected);
                    assert_eq!(args.len(), 2);
                }
                other => panic!("Expected {} function call, got {:?}", expected, other),
            },
            other => panic!("Expected expression select item, got {:?}", other),
        }
    }
}

#[test]
fn test_parse_typed_literals_arena() {
    // Regression: typed literals must still parse in the arena parser
    let result =
        crate::arena_parser::parse_select_to_owned("SELECT DATE '2024-01-01', TIME '12:00:00'");
    assert!(result.is_ok(), "arena parser should accept typed literals: {:?}", result);

    let select = result.unwrap();
    match &select.select_list[0] {
        vibesql_ast::SelectItem::Expression { expr, .. } => {
            assert!(
                matches!(expr, vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(_))),
                "Expected DATE literal, got {:?}",
                expr
            );
        }
        other => panic!("Expected expression select item, got {:?}", other),
    }
    match &select.select_list[1] {
        vibesql_ast::SelectItem::Expression { expr, .. } => {
            assert!(
                matches!(expr, vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Time(_))),
                "Expected TIME literal, got {:?}",
                expr
            );
        }
        other => panic!("Expected expression select item, got {:?}", other),
    }
}

#[test]
fn test_parse_date_time_function_call_in_insert_and_update() {
    // INSERT/UPDATE always use the recursive-descent parser
    let insert = Parser::parse_sql("INSERT INTO t VALUES (date('now'), time('now'));");
    assert!(insert.is_ok(), "date()/time() should parse inside INSERT: {:?}", insert);

    let update = Parser::parse_sql("UPDATE t SET x = date('now', '+1 day');");
    assert!(update.is_ok(), "date() should parse inside UPDATE: {:?}", update);
}

#[test]
fn test_parse_date_time_as_bare_identifiers() {
    // Regression: bare DATE/TIME (no '(' and no string) remain column references
    let result = Parser::parse_sql("SELECT date, time FROM t;");
    assert!(result.is_ok(), "bare date/time should parse as column refs: {:?}", result);

    let result = crate::arena_parser::parse_select_to_owned("SELECT date, time FROM t");
    assert!(result.is_ok(), "arena: bare date/time should parse as column refs: {:?}", result);
}
