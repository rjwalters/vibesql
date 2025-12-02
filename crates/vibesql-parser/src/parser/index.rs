//! Parser for CREATE INDEX, DROP INDEX, and REINDEX statements

use super::{ParseError, Parser};
use crate::{keywords::Keyword, token::Token};

impl Parser {
    /// Parse CREATE INDEX statement
    ///
    /// Syntax:
    ///   CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON table_name (column_list)
    ///   CREATE FULLTEXT INDEX [IF NOT EXISTS] index_name ON table_name (column_list)
    ///   CREATE SPATIAL INDEX [IF NOT EXISTS] index_name ON table_name (column_list)
    pub(super) fn parse_create_index_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateIndexStmt, ParseError> {
        // Expect CREATE keyword
        self.expect_keyword(Keyword::Create)?;

        // Check for FULLTEXT keyword
        if self.peek_keyword(Keyword::Fulltext) {
            self.advance(); // consume FULLTEXT

            // Expect INDEX keyword
            self.expect_keyword(Keyword::Index)?;

            return self.parse_create_index_columns(vibesql_ast::IndexType::Fulltext);
        }

        // Check for SPATIAL keyword
        if self.peek_keyword(Keyword::Spatial) {
            self.advance(); // consume SPATIAL

            // Expect INDEX keyword
            self.expect_keyword(Keyword::Index)?;

            return self.parse_create_index_columns(vibesql_ast::IndexType::Spatial);
        }

        // Check for optional UNIQUE keyword
        let unique = if self.peek_keyword(Keyword::Unique) {
            self.advance(); // consume UNIQUE
            true
        } else {
            false
        };

        // Expect INDEX keyword
        self.expect_keyword(Keyword::Index)?;

        let index_type = vibesql_ast::IndexType::BTree { unique };

        self.parse_create_index_columns(index_type)
    }

    /// Helper function to parse the common parts of CREATE INDEX after type has been determined
    fn parse_create_index_columns(
        &mut self,
        index_type: vibesql_ast::IndexType,
    ) -> Result<vibesql_ast::CreateIndexStmt, ParseError> {
        // Check for optional IF NOT EXISTS clause
        let if_not_exists = if self.peek_keyword(Keyword::If) {
            self.advance(); // consume IF
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse index name
        let index_name = self.parse_identifier()?;

        // Expect ON keyword
        self.expect_keyword(Keyword::On)?;

        // Parse table name
        let table_name = self.parse_identifier()?;

        // Expect opening parenthesis
        self.expect_token(Token::LParen)?;

        // Parse column list
        let mut columns = Vec::new();
        loop {
            // Parse column name
            let column_name = self.parse_identifier()?;

            // Check for optional prefix length: column_name(length)
            let prefix_length = if self.peek() == &Token::LParen {
                self.advance(); // consume LParen

                // Parse the integer length
                let length = match self.peek() {
                    Token::Number(n) => {
                        let value = self.resolve_str(*n).parse::<i64>().map_err(|_| ParseError {
                            message: "Invalid integer for column prefix length".to_string(),
                        })?;
                        self.advance();

                        // Validate prefix length range (MySQL compatibility)
                        if value < 1 {
                            return Err(ParseError {
                                message: format!(
                                    "Key part '{}' length cannot be 0",
                                    column_name
                                ),
                            });
                        }
                        // MySQL InnoDB limit: 3072 bytes for index prefix length
                        // This is the maximum for utf8mb4 with innodb_large_prefix enabled
                        if value > 3072 {
                            return Err(ParseError {
                                message: "Specified key was too long; max key length is 3072 bytes".to_string(),
                            });
                        }

                        value
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected integer for column prefix length".to_string(),
                        })
                    }
                };

                self.expect_token(Token::RParen)?;
                Some(length as u64)
            } else {
                None
            };

            // Check for optional ASC/DESC
            let direction = if self.peek_keyword(crate::keywords::Keyword::Asc) {
                self.advance(); // consume ASC
                vibesql_ast::OrderDirection::Asc
            } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
                self.advance(); // consume DESC
                vibesql_ast::OrderDirection::Desc
            } else {
                vibesql_ast::OrderDirection::Asc // Default
            };

            columns.push(vibesql_ast::IndexColumn { column_name, direction, prefix_length });

            if self.peek() == &Token::Comma {
                self.advance(); // consume comma
            } else {
                break;
            }
        }

        // Expect closing parenthesis
        self.expect_token(Token::RParen)?;

        Ok(vibesql_ast::CreateIndexStmt { if_not_exists, index_name, table_name, index_type, columns })
    }

    /// Parse DROP INDEX statement
    ///
    /// Syntax:
    ///   DROP INDEX [IF EXISTS] index_name
    pub(super) fn parse_drop_index_statement(&mut self) -> Result<vibesql_ast::DropIndexStmt, ParseError> {
        // Expect DROP keyword
        self.expect_keyword(Keyword::Drop)?;

        // Expect INDEX keyword
        self.expect_keyword(Keyword::Index)?;

        // Check for optional IF EXISTS clause
        let if_exists = if self.peek_keyword(Keyword::If) {
            self.advance(); // consume IF
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse index name
        let index_name = self.parse_identifier()?;

        Ok(vibesql_ast::DropIndexStmt { if_exists, index_name })
    }

    /// Parse REINDEX statement
    ///
    /// Syntax:
    ///   REINDEX [database_name | table_name | index_name]
    pub(super) fn parse_reindex_statement(&mut self) -> Result<vibesql_ast::ReindexStmt, ParseError> {
        // Expect REINDEX keyword
        self.expect_keyword(Keyword::Reindex)?;

        // Check for optional target (database, table, or index name)
        let target = if self.peek() == &Token::Semicolon || self.peek() == &Token::Eof {
            // No target specified - reindex all
            None
        } else {
            // Parse optional identifier (could be database, table, or index name)
            Some(self.parse_identifier()?)
        };

        Ok(vibesql_ast::ReindexStmt { target })
    }

    pub(super) fn parse_analyze_statement(&mut self) -> Result<vibesql_ast::AnalyzeStmt, ParseError> {
        // Expect ANALYZE keyword
        self.expect_keyword(Keyword::Analyze)?;

        // Check for optional table name
        let table_name = if self.peek() == &Token::Semicolon || self.peek() == &Token::Eof {
            // No table specified - analyze all tables
            None
        } else {
            // Parse table name
            Some(self.parse_identifier()?)
        };

        // Check for optional column list (only if table name is present)
        let columns = if table_name.is_some() && self.peek() == &Token::LParen {
            self.advance(); // consume '('

            let mut cols = Vec::new();
            loop {
                cols.push(self.parse_identifier()?);

                if self.peek() == &Token::Comma {
                    self.advance(); // consume ','
                } else {
                    break;
                }
            }

            self.expect_token(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        Ok(vibesql_ast::AnalyzeStmt {
            table_name,
            columns,
        })
    }
}
