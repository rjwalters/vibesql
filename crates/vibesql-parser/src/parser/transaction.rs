//! Transaction control statement parsing (BEGIN, COMMIT, ROLLBACK)

use crate::{keywords::Keyword, parser::ParseError, token::Token};

/// Parse BEGIN [TRANSACTION] [WITH DURABILITY = <mode>] or START TRANSACTION [WITH DURABILITY = <mode>] statement
///
/// Syntax:
/// - BEGIN [TRANSACTION] [WITH DURABILITY = DEFAULT | DURABLE | LAZY | VOLATILE]
/// - START TRANSACTION [WITH DURABILITY = DEFAULT | DURABLE | LAZY | VOLATILE]
pub(super) fn parse_begin_statement(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::BeginStmt, ParseError> {
    // Consume BEGIN or START
    if parser.peek_keyword(Keyword::Begin) {
        parser.consume_keyword(Keyword::Begin)?;
    } else if parser.peek_keyword(Keyword::Start) {
        parser.consume_keyword(Keyword::Start)?;
    } else {
        return Err(ParseError { message: "Expected BEGIN or START".to_string() });
    }

    // Optional TRANSACTION keyword
    if parser.peek_keyword(Keyword::Transaction) {
        parser.consume_keyword(Keyword::Transaction)?;
    }

    // Parse optional WITH DURABILITY = <mode> clause
    let durability = if parser.peek_keyword(Keyword::With) {
        parser.consume_keyword(Keyword::With)?;
        parser.consume_keyword(Keyword::Durability)?;

        // Optional = sign
        parser.try_consume(&Token::Symbol('='));

        // Parse durability mode
        parse_durability_hint(parser)?
    } else {
        vibesql_ast::DurabilityHint::Default
    };

    Ok(vibesql_ast::BeginStmt { durability })
}

/// Parse a durability hint keyword
fn parse_durability_hint(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::DurabilityHint, ParseError> {
    if parser.try_consume_keyword(Keyword::Default) {
        Ok(vibesql_ast::DurabilityHint::Default)
    } else if parser.try_consume_keyword(Keyword::Durable) {
        Ok(vibesql_ast::DurabilityHint::Durable)
    } else if parser.try_consume_keyword(Keyword::Lazy) {
        Ok(vibesql_ast::DurabilityHint::Lazy)
    } else if parser.try_consume_keyword(Keyword::Volatile) {
        Ok(vibesql_ast::DurabilityHint::Volatile)
    } else {
        Err(ParseError {
            message: "Expected durability mode: DEFAULT, DURABLE, LAZY, or VOLATILE".to_string(),
        })
    }
}

/// Parse COMMIT statement
pub(super) fn parse_commit_statement(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::CommitStmt, ParseError> {
    // Consume COMMIT
    parser.consume_keyword(Keyword::Commit)?;

    Ok(vibesql_ast::CommitStmt)
}

/// Parse ROLLBACK statement
pub(super) fn parse_rollback_statement(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::RollbackStmt, ParseError> {
    // Consume ROLLBACK
    parser.consume_keyword(Keyword::Rollback)?;

    Ok(vibesql_ast::RollbackStmt)
}

/// Parse SAVEPOINT statement
pub(super) fn parse_savepoint_statement(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::SavepointStmt, ParseError> {
    // Consume SAVEPOINT
    parser.consume_keyword(Keyword::Savepoint)?;

    // Parse savepoint name (identifier)
    let name = parser.parse_identifier()?;

    Ok(vibesql_ast::SavepointStmt { name })
}

/// Parse ROLLBACK TO SAVEPOINT statement
pub(super) fn parse_rollback_to_savepoint_statement(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::RollbackToSavepointStmt, ParseError> {
    // Consume ROLLBACK
    parser.consume_keyword(Keyword::Rollback)?;

    // Consume TO
    parser.consume_keyword(Keyword::To)?;

    // Consume SAVEPOINT
    parser.consume_keyword(Keyword::Savepoint)?;

    // Parse savepoint name (identifier)
    let name = parser.parse_identifier()?;

    Ok(vibesql_ast::RollbackToSavepointStmt { name })
}

/// Parse RELEASE SAVEPOINT statement
pub(super) fn parse_release_savepoint_statement(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::ReleaseSavepointStmt, ParseError> {
    // Consume RELEASE
    parser.consume_keyword(Keyword::Release)?;

    // Consume SAVEPOINT
    parser.consume_keyword(Keyword::Savepoint)?;

    // Parse savepoint name (identifier)
    let name = parser.parse_identifier()?;

    Ok(vibesql_ast::ReleaseSavepointStmt { name })
}
