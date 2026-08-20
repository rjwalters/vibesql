//! ATTACH / DETACH DATABASE statement parsing (SQLite compatibility).
//!
//! `ATTACH`, `DETACH`, and `DATABASE` are deliberately **not** lexer keywords:
//! promoting them would regress existing SQL that uses `attach` / `detach` /
//! `database` as ordinary identifiers (column names, table names, aliases).
//! No other statement in the grammar begins with a bare identifier, so
//! [`Parser::parse_statement`] dispatches on a leading identifier spelled
//! `ATTACH` / `DETACH` (case-insensitively) instead.
//!
//! Grammar (SQLite):
//! - `ATTACH [DATABASE] <filename> AS <schema-name>`
//! - `DETACH [DATABASE] <schema-name>`
//!
//! Phase 1 (#6310) accepts a string-literal filename only. SQLite allows a
//! general expression there; expression filenames can be added when
//! file-backed attachments land (#6362).

use super::{ParseError, Parser};
use crate::keywords::Keyword;
use crate::token::Token;

impl Parser {
    /// Parse `ATTACH [DATABASE] 'filename' AS schema-name`.
    ///
    /// The caller has verified that the current token is an identifier spelled
    /// `ATTACH`.
    pub(super) fn parse_attach_statement(
        &mut self,
    ) -> Result<vibesql_ast::AttachStmt, ParseError> {
        self.advance(); // consume ATTACH
        self.consume_optional_database_noise_word();

        // Phase 1: the filename must be a string literal (SQLite allows any
        // expression; see module docs).
        let filename = match self.peek() {
            Token::String(s) => {
                let filename = s.clone();
                self.advance();
                filename
            }
            token => return Err(ParseError { message: token.syntax_error() }),
        };

        self.expect_keyword(Keyword::As)?;

        let schema_name = self.parse_database_name()?;
        self.expect_statement_end()?;

        Ok(vibesql_ast::AttachStmt { filename, schema_name })
    }

    /// Parse `DETACH [DATABASE] schema-name`.
    ///
    /// The caller has verified that the current token is an identifier spelled
    /// `DETACH`.
    pub(super) fn parse_detach_statement(
        &mut self,
    ) -> Result<vibesql_ast::DetachStmt, ParseError> {
        self.advance(); // consume DETACH
        self.consume_optional_database_noise_word();

        let schema_name = self.parse_database_name()?;
        self.expect_statement_end()?;

        Ok(vibesql_ast::DetachStmt { schema_name })
    }

    /// Consume the optional `DATABASE` noise word after ATTACH/DETACH.
    ///
    /// `DATABASE` is not a lexer keyword, so it arrives as a plain identifier.
    fn consume_optional_database_noise_word(&mut self) {
        if let Token::Identifier(word) = self.peek() {
            if word.eq_ignore_ascii_case("DATABASE") {
                self.advance();
            }
        }
    }

    /// Parse the database (schema) name position of ATTACH/DETACH.
    ///
    /// SQLite accepts a plain identifier, a quoted identifier, a string
    /// literal, or a (non-reserved) keyword here.
    fn parse_database_name(&mut self) -> Result<String, ParseError> {
        match self.peek() {
            Token::String(s) => {
                let name = s.clone();
                self.advance();
                Ok(name)
            }
            _ => self.parse_identifier_or_keyword(),
        }
    }
}
