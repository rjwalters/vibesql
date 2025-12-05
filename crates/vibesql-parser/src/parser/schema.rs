//! Schema DDL parsing

use vibesql_ast::*;

use crate::{keywords::Keyword, parser::ParseError};

/// Parse CREATE SCHEMA statement
pub fn parse_create_schema(parser: &mut crate::Parser) -> Result<CreateSchemaStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Schema)?;

    let if_not_exists = parser.peek_keyword(Keyword::If) && {
        parser.advance();
        parser.expect_keyword(Keyword::Not)?;
        parser.expect_keyword(Keyword::Exists)?;
        true
    };

    let schema_name = parser.parse_qualified_identifier()?;

    // Parse optional schema elements (CREATE TABLE, etc.)
    let mut schema_elements = Vec::new();
    while parser.peek_keyword(Keyword::Create) {
        // Look ahead to see what kind of CREATE statement this is
        let saved_position = parser.position;
        parser.advance(); // Skip CREATE keyword

        if parser.peek_keyword(Keyword::Table) {
            // Reset to before CREATE and parse the full CREATE TABLE statement
            parser.position = saved_position;
            let table_stmt = parser.parse_create_table_statement()?;
            schema_elements.push(vibesql_ast::SchemaElement::CreateTable(table_stmt));
        } else {
            // Unsupported schema element - restore and break
            parser.position = saved_position;
            break;
        }
    }

    Ok(CreateSchemaStmt { schema_name, if_not_exists, schema_elements })
}

/// Parse DROP SCHEMA statement
pub fn parse_drop_schema(parser: &mut crate::Parser) -> Result<DropSchemaStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Schema)?;

    let if_exists = parser.peek_keyword(Keyword::If) && {
        parser.advance();
        parser.expect_keyword(Keyword::Exists)?;
        true
    };

    let schema_name = parser.parse_qualified_identifier()?;

    let cascade = if parser.peek_keyword(Keyword::Cascade) {
        parser.advance();
        true
    } else if parser.peek_keyword(Keyword::Restrict) {
        parser.advance();
        false
    } else {
        // RESTRICT is the default
        false
    };

    Ok(DropSchemaStmt { schema_name, if_exists, cascade })
}

/// Parse SET SCHEMA statement
pub fn parse_set_schema(parser: &mut crate::Parser) -> Result<SetSchemaStmt, ParseError> {
    // This could be "SET SCHEMA schema_name" or "SET search_path TO schema_name"
    // For now, we'll support the simpler "SET SCHEMA schema_name" form

    parser.expect_keyword(Keyword::Set)?;
    parser.expect_keyword(Keyword::Schema)?;

    let schema_name = parser.parse_qualified_identifier()?;

    Ok(SetSchemaStmt { schema_name })
}

/// Parse SET CATALOG statement
pub fn parse_set_catalog(
    parser: &mut crate::Parser,
) -> Result<vibesql_ast::SetCatalogStmt, ParseError> {
    parser.expect_keyword(Keyword::Set)?;
    parser.expect_keyword(Keyword::Catalog)?;

    let catalog_name = parser.parse_qualified_identifier()?;

    Ok(vibesql_ast::SetCatalogStmt { catalog_name })
}

/// Parse SET NAMES statement
pub fn parse_set_names(
    parser: &mut crate::Parser,
) -> Result<vibesql_ast::SetNamesStmt, ParseError> {
    parser.expect_keyword(Keyword::Set)?;
    parser.expect_keyword(Keyword::Names)?;

    // Parse charset name (can be identifier or string literal)
    let charset_name = match parser.peek() {
        crate::token::Token::String(s) => {
            let val = s.clone();
            parser.advance();
            val
        }
        _ => parser.parse_qualified_identifier()?,
    };

    // Parse optional COLLATE clause (use Collation keyword)
    let collation = if parser.peek_keyword(Keyword::Collation) {
        parser.advance();
        Some(match parser.peek() {
            crate::token::Token::String(s) => {
                let val = s.clone();
                parser.advance();
                val
            }
            _ => parser.parse_qualified_identifier()?,
        })
    } else {
        None
    };

    Ok(vibesql_ast::SetNamesStmt { charset_name, collation })
}

/// Parse SET TIME ZONE statement
pub fn parse_set_time_zone(
    parser: &mut crate::Parser,
) -> Result<vibesql_ast::SetTimeZoneStmt, ParseError> {
    parser.expect_keyword(Keyword::Set)?;
    parser.expect_keyword(Keyword::Time)?;
    parser.expect_keyword(Keyword::Zone)?;

    // Parse zone specification: LOCAL or INTERVAL '...'
    let zone = if parser.peek_keyword(Keyword::Local) {
        parser.advance();
        vibesql_ast::TimeZoneSpec::Local
    } else if parser.peek_keyword(Keyword::Interval) {
        parser.advance();
        // Parse the interval string
        let interval_str = match parser.peek() {
            crate::token::Token::String(s) => {
                let val = s.clone();
                parser.advance();
                val
            }
            _ => {
                return Err(ParseError {
                    message: "Expected string literal after INTERVAL".to_string(),
                });
            }
        };

        // Optionally parse HOUR TO MINUTE or other interval qualifiers
        // For now, we'll accept but ignore the qualifier
        if parser.peek_keyword(Keyword::Hour) {
            parser.advance();
            if parser.peek_keyword(Keyword::To) {
                parser.advance();
                parser.expect_keyword(Keyword::Minute)?;
            }
        }

        vibesql_ast::TimeZoneSpec::Interval(interval_str)
    } else {
        return Err(ParseError {
            message: "Expected LOCAL or INTERVAL after SET TIME ZONE".to_string(),
        });
    };

    Ok(vibesql_ast::SetTimeZoneStmt { zone })
}

/// Parse SET variable statement (MySQL/PostgreSQL extension)
///
/// Syntax:
///   SET [GLOBAL | SESSION] variable_name = expression
///   SET variable_name = expression (defaults to SESSION)
pub fn parse_set_variable(
    parser: &mut crate::Parser,
) -> Result<vibesql_ast::SetVariableStmt, ParseError> {
    parser.expect_keyword(Keyword::Set)?;

    // Check for optional GLOBAL or SESSION scope
    let scope = if parser.peek_keyword(Keyword::Global) {
        parser.advance();
        vibesql_ast::VariableScope::Global
    } else if parser.peek_keyword(Keyword::Session) {
        parser.advance();
        vibesql_ast::VariableScope::Session
    } else {
        // Default to SESSION scope
        vibesql_ast::VariableScope::Session
    };

    // Parse variable name (identifier)
    let variable = parser.parse_identifier()?;

    // Expect '=' sign
    parser.expect_token(crate::token::Token::Symbol('='))?;

    // Parse the value expression
    let value = parser.parse_expression()?;

    Ok(vibesql_ast::SetVariableStmt { scope, variable, value })
}
