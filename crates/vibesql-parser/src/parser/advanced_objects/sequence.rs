//! SEQUENCE DDL parsing (SQL:1999)
//! Includes: CREATE/DROP/ALTER SEQUENCE

use vibesql_ast::*;

use crate::{keywords::Keyword, parser::ParseError, token::Token};

/// Parse CREATE SEQUENCE statement
///
/// Syntax: CREATE SEQUENCE sequence_name
///         [START WITH n]
///         [INCREMENT BY n]
///         [MINVALUE n | NO MINVALUE]
///         [MAXVALUE n | NO MAXVALUE]
///         [CYCLE | NO CYCLE]
pub fn parse_create_sequence(parser: &mut crate::Parser) -> Result<CreateSequenceStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Sequence)?;

    let sequence_name = parser.parse_identifier()?;

    let mut start_with = None;
    let mut increment_by = 1; // default
    let mut min_value = None;
    let mut max_value = None;
    let mut cycle = false; // default

    // Parse optional clauses in any order
    loop {
        if parser.try_consume_keyword(Keyword::Start) {
            parser.expect_keyword(Keyword::With)?;
            let num_str = parser.parse_signed_number()?;
            start_with = Some(num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid START WITH value: {}", num_str),
            })?);
        } else if parser.try_consume_keyword(Keyword::Increment) {
            parser.expect_keyword(Keyword::By)?;
            let num_str = parser.parse_signed_number()?;
            increment_by = num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid INCREMENT BY value: {}", num_str),
            })?;
        } else if parser.try_consume_keyword(Keyword::Minvalue) {
            let num_str = parser.parse_signed_number()?;
            min_value =
                Some(num_str.parse::<i64>().map_err(|_| ParseError {
                    message: format!("Invalid MINVALUE: {}", num_str),
                })?);
        } else if parser.try_consume_keyword(Keyword::Maxvalue) {
            let num_str = parser.parse_signed_number()?;
            max_value =
                Some(num_str.parse::<i64>().map_err(|_| ParseError {
                    message: format!("Invalid MAXVALUE: {}", num_str),
                })?);
        } else if parser.try_consume_keyword(Keyword::No) {
            if parser.try_consume_keyword(Keyword::Minvalue) {
                min_value = None;
            } else if parser.try_consume_keyword(Keyword::Maxvalue) {
                max_value = None;
            } else if parser.try_consume_keyword(Keyword::Cycle) {
                cycle = false;
            } else {
                return Err(ParseError {
                    message: "Expected MINVALUE, MAXVALUE, or CYCLE after NO".to_string(),
                });
            }
        } else if parser.try_consume_keyword(Keyword::Cycle) {
            cycle = true;
        } else {
            break;
        }
    }

    Ok(CreateSequenceStmt { sequence_name, start_with, increment_by, min_value, max_value, cycle })
}

/// Parse DROP SEQUENCE statement
///
/// Syntax: DROP SEQUENCE sequence_name [CASCADE | RESTRICT]
pub fn parse_drop_sequence(parser: &mut crate::Parser) -> Result<DropSequenceStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Sequence)?;

    let sequence_name = parser.parse_identifier()?;

    // CASCADE or RESTRICT (defaults to RESTRICT if neither specified)
    let cascade = if parser.try_consume_keyword(Keyword::Cascade) {
        true
    } else {
        parser.try_consume_keyword(Keyword::Restrict); // consume if present
        false
    };

    Ok(DropSequenceStmt { sequence_name, cascade })
}

/// Parse ALTER SEQUENCE statement
///
/// Syntax: ALTER SEQUENCE sequence_name
///         [RESTART [WITH n]]
///         [INCREMENT BY n]
///         [MINVALUE n | NO MINVALUE]
///         [MAXVALUE n | NO MAXVALUE]
///         [CYCLE | NO CYCLE]
pub fn parse_alter_sequence(parser: &mut crate::Parser) -> Result<AlterSequenceStmt, ParseError> {
    parser.expect_keyword(Keyword::Alter)?;
    parser.expect_keyword(Keyword::Sequence)?;

    let sequence_name = parser.parse_identifier()?;

    let mut restart_with = None;
    let mut increment_by = None;
    let mut min_value = None;
    let mut max_value = None;
    let mut cycle = None;

    // Parse optional clauses in any order
    loop {
        if parser.try_consume_keyword(Keyword::Restart) {
            if parser.try_consume_keyword(Keyword::With) {
                let num_str = match parser.peek() {
                    Token::Number(sym) => parser.resolve(*sym),
                    _ => {
                        return Err(ParseError {
                            message: "Expected number after RESTART WITH".to_string(),
                        })
                    }
                };
                parser.advance();
                restart_with = Some(num_str.parse::<i64>().map_err(|_| ParseError {
                    message: format!("Invalid RESTART WITH value: {}", num_str),
                })?);
            } else {
                // RESTART without WITH means restart from original START WITH value
                restart_with = Some(1); // Will be handled by executor to use original start value
            }
        } else if parser.try_consume_keyword(Keyword::Increment) {
            parser.expect_keyword(Keyword::By)?;
            let num_str = match parser.peek() {
                Token::Number(sym) => parser.resolve(*sym),
                _ => {
                    return Err(ParseError {
                        message: "Expected number after INCREMENT BY".to_string(),
                    })
                }
            };
            parser.advance();
            increment_by = Some(num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid INCREMENT BY value: {}", num_str),
            })?);
        } else if parser.try_consume_keyword(Keyword::Minvalue) {
            let num_str = match parser.peek() {
                Token::Number(sym) => parser.resolve(*sym),
                _ => {
                    return Err(ParseError {
                        message: "Expected number after MINVALUE".to_string(),
                    })
                }
            };
            parser.advance();
            min_value =
                Some(Some(num_str.parse::<i64>().map_err(|_| ParseError {
                    message: format!("Invalid MINVALUE: {}", num_str),
                })?));
        } else if parser.try_consume_keyword(Keyword::Maxvalue) {
            let num_str = match parser.peek() {
                Token::Number(sym) => parser.resolve(*sym),
                _ => {
                    return Err(ParseError {
                        message: "Expected number after MAXVALUE".to_string(),
                    })
                }
            };
            parser.advance();
            max_value =
                Some(Some(num_str.parse::<i64>().map_err(|_| ParseError {
                    message: format!("Invalid MAXVALUE: {}", num_str),
                })?));
        } else if parser.try_consume_keyword(Keyword::No) {
            if parser.try_consume_keyword(Keyword::Minvalue) {
                min_value = Some(None); // NO MINVALUE
            } else if parser.try_consume_keyword(Keyword::Maxvalue) {
                max_value = Some(None); // NO MAXVALUE
            } else if parser.try_consume_keyword(Keyword::Cycle) {
                cycle = Some(false);
            } else {
                return Err(ParseError {
                    message: "Expected MINVALUE, MAXVALUE, or CYCLE after NO".to_string(),
                });
            }
        } else if parser.try_consume_keyword(Keyword::Cycle) {
            cycle = Some(true);
        } else {
            break;
        }
    }

    Ok(AlterSequenceStmt { sequence_name, restart_with, increment_by, min_value, max_value, cycle })
}
