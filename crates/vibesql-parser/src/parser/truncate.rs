//! TRUNCATE TABLE statement parser

use super::{ParseError, Parser};
use crate::{keywords::Keyword, token::Token};

impl Parser {
    /// Parse TRUNCATE TABLE statement
    ///
    /// Syntax:
    ///   TRUNCATE [TABLE] [IF EXISTS] table_name [, table_name, ...] [CASCADE | RESTRICT]
    pub(super) fn parse_truncate_table_statement(
        &mut self,
    ) -> Result<vibesql_ast::TruncateTableStmt, ParseError> {
        // Expect TRUNCATE keyword
        self.expect_keyword(Keyword::Truncate)?;

        // Optional TABLE keyword
        if self.peek_keyword(Keyword::Table) {
            self.consume_keyword(Keyword::Table)?;
        }

        // Check for optional IF EXISTS
        let if_exists = if self.peek_keyword(Keyword::If) {
            self.consume_keyword(Keyword::If)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse first table name (supports schema.table)
        let mut table_names = vec![self.parse_qualified_identifier()?];

        // Parse additional table names separated by commas
        while matches!(self.peek(), Token::Comma) {
            self.advance(); // consume comma
            table_names.push(self.parse_qualified_identifier()?);
        }

        // Check for optional CASCADE or RESTRICT
        let cascade = if self.peek_keyword(Keyword::Cascade) {
            self.consume_keyword(Keyword::Cascade)?;
            Some(vibesql_ast::TruncateCascadeOption::Cascade)
        } else if self.peek_keyword(Keyword::Restrict) {
            self.consume_keyword(Keyword::Restrict)?;
            Some(vibesql_ast::TruncateCascadeOption::Restrict)
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::TruncateTableStmt { table_names, if_exists, cascade })
    }
}
