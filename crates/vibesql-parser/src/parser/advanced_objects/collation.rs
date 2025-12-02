//! COLLATION and CHARACTER SET DDL parsing (SQL:1999)
//! Includes: CREATE/DROP COLLATION, CREATE/DROP CHARACTER SET

use vibesql_ast::*;

use crate::{keywords::Keyword, parser::ParseError, token::Token};

// ============================================================================
// COLLATION
// ============================================================================

/// Parse CREATE COLLATION statement
///
/// SQL:1999 Syntax:
///   CREATE COLLATION collation_name
///     [FOR character_set]
///     [FROM source_collation]
///     [PAD SPACE | NO PAD]
pub fn parse_create_collation(
    parser: &mut crate::Parser,
) -> Result<CreateCollationStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Collation)?;

    let collation_name = parser.parse_identifier()?;

    // Optional: FOR character_set
    let character_set = if parser.try_consume_keyword(Keyword::For) {
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    // Optional: FROM source_collation
    let source_collation = if parser.try_consume_keyword(Keyword::From) {
        match parser.peek() {
            Token::Identifier(s) | Token::DelimitedIdentifier(s) => {
                let source = parser.resolve(*s);
                parser.advance();
                Some(source)
            }
            Token::String(s) => {
                // SQL:1999 allows string literal for locale (e.g., 'de_DE')
                let source = parser.resolve(*s);
                parser.advance();
                Some(source)
            }
            _ => {
                return Err(ParseError {
                    message: "Expected identifier or string literal after FROM".to_string(),
                });
            }
        }
    } else {
        None
    };

    // Optional: PAD SPACE | NO PAD
    let pad_space = if parser.try_consume_keyword(Keyword::Pad) {
        parser.expect_keyword(Keyword::Space)?;
        Some(true)
    } else if parser.try_consume_keyword(Keyword::No) {
        parser.expect_keyword(Keyword::Pad)?;
        Some(false)
    } else {
        None
    };

    Ok(CreateCollationStmt { collation_name, character_set, source_collation, pad_space })
}

/// Parse DROP COLLATION statement
///
/// Syntax: DROP COLLATION collation_name
pub fn parse_drop_collation(parser: &mut crate::Parser) -> Result<DropCollationStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Collation)?;

    let collation_name = parser.parse_identifier()?;

    Ok(DropCollationStmt { collation_name })
}

// ============================================================================
// CHARACTER SET
// ============================================================================

/// Parse CREATE CHARACTER SET statement
///
/// SQL:1999 Syntax:
///   CREATE CHARACTER SET charset_name [AS]
///     [GET source]
///     [COLLATE FROM collation]
pub fn parse_create_character_set(
    parser: &mut crate::Parser,
) -> Result<CreateCharacterSetStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Character)?;
    parser.expect_keyword(Keyword::Set)?;

    let charset_name = parser.parse_identifier()?;

    // Optional: AS
    parser.try_consume_keyword(Keyword::As);

    // Optional: GET source
    let source = if parser.try_consume_keyword(Keyword::Get) {
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    // Optional: COLLATE FROM collation
    let collation = if parser.try_consume_keyword(Keyword::Collate) {
        parser.expect_keyword(Keyword::From)?;
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    Ok(CreateCharacterSetStmt { charset_name, source, collation })
}

/// Parse DROP CHARACTER SET statement
///
/// Syntax: DROP CHARACTER SET charset_name
pub fn parse_drop_character_set(
    parser: &mut crate::Parser,
) -> Result<DropCharacterSetStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Character)?;
    parser.expect_keyword(Keyword::Set)?;

    let charset_name = parser.parse_identifier()?;

    Ok(DropCharacterSetStmt { charset_name })
}
