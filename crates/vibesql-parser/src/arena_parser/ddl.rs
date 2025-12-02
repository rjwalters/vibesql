//! Arena-allocated DDL statement parsing.
//!
//! This module provides parsing for DDL statements including:
//! - Transaction statements (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)
//! - CREATE/DROP TABLE, INDEX, VIEW
//! - ALTER TABLE
//! - ANALYZE

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::arena::{
    AnalyzeStmt, BeginStmt, CommitStmt, CreateIndexStmt, CreateViewStmt, DropIndexStmt,
    DropTableStmt, DropViewStmt, IndexColumn, IndexType, OrderDirection, ReleaseSavepointStmt,
    RollbackStmt, RollbackToSavepointStmt, SavepointStmt, TruncateCascadeOption, TruncateTableStmt,
};

use super::ArenaParser;
use crate::keywords::Keyword;
use crate::token::Token;
use crate::ParseError;

impl<'arena> ArenaParser<'arena> {
    // ========================================================================
    // Transaction Statements
    // ========================================================================

    /// Parse BEGIN [TRANSACTION] or START TRANSACTION statement.
    pub(crate) fn parse_begin_statement(&mut self) -> Result<BeginStmt, ParseError> {
        if self.peek_keyword(Keyword::Begin) {
            self.consume_keyword(Keyword::Begin)?;
        } else if self.peek_keyword(Keyword::Start) {
            self.consume_keyword(Keyword::Start)?;
        } else {
            return Err(ParseError {
                message: "Expected BEGIN or START".to_string(),
            });
        }

        // Optional TRANSACTION keyword
        self.try_consume_keyword(Keyword::Transaction);

        Ok(BeginStmt)
    }

    /// Parse COMMIT statement.
    pub(crate) fn parse_commit_statement(&mut self) -> Result<CommitStmt, ParseError> {
        self.consume_keyword(Keyword::Commit)?;
        Ok(CommitStmt)
    }

    /// Parse ROLLBACK statement.
    pub(crate) fn parse_rollback_statement(&mut self) -> Result<RollbackStmt, ParseError> {
        self.consume_keyword(Keyword::Rollback)?;
        Ok(RollbackStmt)
    }

    /// Parse ROLLBACK TO SAVEPOINT statement.
    pub(crate) fn parse_rollback_to_savepoint_statement(
        &mut self,
    ) -> Result<RollbackToSavepointStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Rollback)?;
        self.consume_keyword(Keyword::To)?;
        self.consume_keyword(Keyword::Savepoint)?;
        let name = self.parse_arena_identifier()?;
        Ok(RollbackToSavepointStmt { name })
    }

    /// Parse SAVEPOINT statement.
    pub(crate) fn parse_savepoint_statement(
        &mut self,
    ) -> Result<SavepointStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Savepoint)?;
        let name = self.parse_arena_identifier()?;
        Ok(SavepointStmt { name })
    }

    /// Parse RELEASE SAVEPOINT statement.
    pub(crate) fn parse_release_savepoint_statement(
        &mut self,
    ) -> Result<ReleaseSavepointStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Release)?;
        self.consume_keyword(Keyword::Savepoint)?;
        let name = self.parse_arena_identifier()?;
        Ok(ReleaseSavepointStmt { name })
    }

    // ========================================================================
    // CREATE Statements
    // ========================================================================

    /// Parse CREATE INDEX statement.
    pub(crate) fn parse_create_index_statement(
        &mut self,
    ) -> Result<CreateIndexStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Create)?;

        // Check for UNIQUE or FULLTEXT
        let index_type = if self.try_consume_keyword(Keyword::Unique) {
            IndexType::BTree { unique: true }
        } else if self.try_consume_keyword(Keyword::Fulltext) {
            IndexType::Fulltext
        } else if self.try_consume_keyword(Keyword::Spatial) {
            IndexType::Spatial
        } else {
            IndexType::BTree { unique: false }
        };

        self.consume_keyword(Keyword::Index)?;

        // Check for IF NOT EXISTS
        let if_not_exists = if self.try_consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        let index_name = self.parse_arena_identifier()?;

        self.consume_keyword(Keyword::On)?;
        let table_name = self.parse_arena_identifier()?;

        self.expect_token(Token::LParen)?;
        let columns = self.parse_index_columns()?;
        self.expect_token(Token::RParen)?;

        Ok(CreateIndexStmt {
            if_not_exists,
            index_name,
            table_name,
            index_type,
            columns,
        })
    }

    /// Parse CREATE VIEW statement.
    pub(crate) fn parse_create_view_statement(
        &mut self,
    ) -> Result<CreateViewStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Create)?;

        // Check for OR REPLACE
        let or_replace = if self.try_consume_keyword(Keyword::Or) {
            self.expect_keyword(Keyword::Replace)?;
            true
        } else {
            false
        };

        // Check for TEMP/TEMPORARY
        let temporary = self.try_consume_keyword(Keyword::Temp)
            || self.try_consume_keyword(Keyword::Temporary);

        self.consume_keyword(Keyword::View)?;

        let view_name = self.parse_arena_identifier()?;

        // Parse optional column list
        let columns = if self.try_consume(&Token::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        self.consume_keyword(Keyword::As)?;

        let query = self.parse_select_statement()?;

        // Check for WITH CHECK OPTION
        let with_check_option = if self.try_consume_keyword(Keyword::With) {
            self.expect_keyword(Keyword::Check)?;
            self.expect_keyword(Keyword::Option)?;
            true
        } else {
            false
        };

        Ok(CreateViewStmt {
            view_name,
            columns,
            query,
            with_check_option,
            or_replace,
            temporary,
        })
    }

    // ========================================================================
    // DROP Statements
    // ========================================================================

    /// Parse DROP TABLE statement.
    pub(crate) fn parse_drop_table_statement(
        &mut self,
    ) -> Result<DropTableStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Drop)?;
        self.consume_keyword(Keyword::Table)?;

        let if_exists = if self.try_consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        let table_name = self.parse_arena_identifier()?;

        Ok(DropTableStmt {
            table_name,
            if_exists,
        })
    }

    /// Parse DROP INDEX statement.
    pub(crate) fn parse_drop_index_statement(
        &mut self,
    ) -> Result<DropIndexStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Drop)?;
        self.consume_keyword(Keyword::Index)?;

        let if_exists = if self.try_consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        let index_name = self.parse_arena_identifier()?;

        Ok(DropIndexStmt {
            if_exists,
            index_name,
        })
    }

    /// Parse DROP VIEW statement.
    pub(crate) fn parse_drop_view_statement(
        &mut self,
    ) -> Result<DropViewStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Drop)?;
        self.consume_keyword(Keyword::View)?;

        let if_exists = if self.try_consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        let view_name = self.parse_arena_identifier()?;

        let (cascade, restrict) = if self.try_consume_keyword(Keyword::Cascade) {
            (true, false)
        } else if self.try_consume_keyword(Keyword::Restrict) {
            (false, true)
        } else {
            (false, false)
        };

        Ok(DropViewStmt {
            view_name,
            if_exists,
            cascade,
            restrict,
        })
    }

    /// Parse TRUNCATE TABLE statement.
    pub(crate) fn parse_truncate_table_statement(
        &mut self,
    ) -> Result<TruncateTableStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Truncate)?;
        self.try_consume_keyword(Keyword::Table);

        let if_exists = if self.try_consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse table names (can be comma-separated)
        let mut table_names = BumpVec::new_in(self.arena);
        loop {
            table_names.push(self.parse_arena_identifier()?);
            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        let cascade = if self.try_consume_keyword(Keyword::Cascade) {
            Some(TruncateCascadeOption::Cascade)
        } else if self.try_consume_keyword(Keyword::Restrict) {
            Some(TruncateCascadeOption::Restrict)
        } else {
            None
        };

        Ok(TruncateTableStmt {
            table_names,
            if_exists,
            cascade,
        })
    }

    // ========================================================================
    // ANALYZE Statement
    // ========================================================================

    /// Parse ANALYZE statement.
    pub(crate) fn parse_analyze_statement(
        &mut self,
    ) -> Result<AnalyzeStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Analyze)?;

        // Parse optional table name
        let table_name = if let Token::Identifier(_) = self.peek() {
            Some(self.parse_arena_identifier()?)
        } else {
            None
        };

        // Parse optional column list
        let columns = if table_name.is_some() && self.try_consume(&Token::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        Ok(AnalyzeStmt {
            table_name,
            columns,
        })
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    /// Parse index column specifications.
    fn parse_index_columns(
        &mut self,
    ) -> Result<BumpVec<'arena, IndexColumn<'arena>>, ParseError> {
        let mut columns = BumpVec::new_in(self.arena);
        loop {
            let column_name = self.parse_arena_identifier()?;

            // Parse optional prefix length (e.g., name(10))
            let prefix_length = if self.try_consume(&Token::LParen) {
                let len = match self.peek() {
                    Token::Number(n) => n
                        .parse::<u64>()
                        .map_err(|_| ParseError {
                            message: "Invalid prefix length".to_string(),
                        })?,
                    _ => {
                        return Err(ParseError {
                            message: "Expected number for prefix length".to_string(),
                        })
                    }
                };
                self.advance();
                self.expect_token(Token::RParen)?;
                Some(len)
            } else {
                None
            };

            // Parse optional direction
            let direction = if self.try_consume_keyword(Keyword::Desc) {
                OrderDirection::Desc
            } else {
                self.try_consume_keyword(Keyword::Asc);
                OrderDirection::Asc
            };

            columns.push(IndexColumn {
                column_name,
                direction,
                prefix_length,
            });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }
        Ok(columns)
    }
}
