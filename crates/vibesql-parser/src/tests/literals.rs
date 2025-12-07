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
                vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
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
                vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
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
            vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
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
                vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
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
            vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
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
                vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
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
            vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
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
            vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
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
                vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s)) => {
                        // x'303132' = bytes [0x30, 0x31, 0x32] = "012"
                        assert_eq!(s.as_str(), "012");
                    }
                    _ => panic!("Expected VARCHAR literal, got {:?}", expr),
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
            vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s)) => {
                    // X'48656C6C6F' = "Hello"
                    assert_eq!(s.as_str(), "Hello");
                }
                _ => panic!("Expected VARCHAR literal"),
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
            vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s)) => {
                    assert_eq!(s.as_str(), "");
                }
                _ => panic!("Expected VARCHAR literal"),
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
            vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
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
            vibesql_ast::SelectItem::Expression { expr, alias: _ } => match expr {
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
