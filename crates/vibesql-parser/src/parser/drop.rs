//! DROP TABLE statement parser

use super::{ParseError, Parser};
use crate::{keywords::Keyword, token::Token};

impl Parser {
    /// Parse DROP TABLE statement
    ///
    /// Syntax:
    ///   DROP TABLE [IF EXISTS] table_name
    pub(super) fn parse_drop_table_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropTableStmt, ParseError> {
        // Expect DROP keyword
        self.expect_keyword(Keyword::Drop)?;

        // Expect TABLE keyword
        self.expect_keyword(Keyword::Table)?;

        // Check for optional IF EXISTS
        let if_exists = if self.peek_keyword(Keyword::If) {
            self.consume_keyword(Keyword::If)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse table name (supports schema.table)
        let table_name = self.parse_qualified_identifier()?;

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::DropTableStmt { table_name, if_exists })
    }
}
