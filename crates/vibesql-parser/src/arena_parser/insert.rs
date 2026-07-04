//! Arena-allocated INSERT statement parsing.

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::arena::{
    Assignment, ConflictClause, InsertSource, InsertStmt, OnConflictAction, OnConflictClause,
    Symbol,
};

use super::ArenaParser;
use crate::{keywords::Keyword, token::Token, ParseError};

/// Result of parsing a table reference (schema.table or just table)
struct TableRefParts {
    schema_name: Option<Symbol>,
    schema_quoted: bool,
    table_name: Symbol,
    table_quoted: bool,
}

impl<'arena> ArenaParser<'arena> {
    /// Parse a table reference with optional schema qualifier.
    /// Supports: tablename, "TableName", schema.table, "schema"."table"
    fn parse_insert_table_ref(&mut self) -> Result<TableRefParts, ParseError> {
        // Parse first identifier and track if it was quoted
        let (first_part, first_quoted) = match self.peek() {
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                (self.intern(&name), false)
            }
            Token::DelimitedIdentifier(name) => {
                let name = name.clone();
                self.advance();
                (self.intern(&name), true)
            }
            _ => {
                return Err(ParseError { message: "Expected table name".to_string() });
            }
        };

        // Check if there's a dot followed by another identifier
        if self.try_consume(&Token::Symbol('.')) {
            let (second_part, second_quoted) = match self.peek() {
                Token::Identifier(name) => {
                    let name = name.clone();
                    self.advance();
                    (self.intern(&name), false)
                }
                Token::DelimitedIdentifier(name) => {
                    let name = name.clone();
                    self.advance();
                    (self.intern(&name), true)
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected identifier after '.'".to_string(),
                    });
                }
            };
            Ok(TableRefParts {
                schema_name: Some(first_part),
                schema_quoted: first_quoted,
                table_name: second_part,
                table_quoted: second_quoted,
            })
        } else {
            Ok(TableRefParts {
                schema_name: None,
                schema_quoted: false,
                table_name: first_part,
                table_quoted: first_quoted,
            })
        }
    }

    /// Parse an INSERT statement (including INSERT OR REPLACE).
    pub(crate) fn parse_insert_statement(
        &mut self,
    ) -> Result<&'arena InsertStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Insert)?;

        // Check for conflict clause: INSERT OR REPLACE|IGNORE|ABORT|ROLLBACK|FAIL
        let conflict_clause = if self.try_consume_keyword(Keyword::Or) {
            if self.try_consume_keyword(Keyword::Replace) {
                Some(ConflictClause::Replace)
            } else if self.try_consume_keyword(Keyword::Ignore) {
                Some(ConflictClause::Ignore)
            } else if self.try_consume_keyword(Keyword::Abort) {
                Some(ConflictClause::Abort)
            } else if self.try_consume_keyword(Keyword::Rollback) {
                Some(ConflictClause::Rollback)
            } else if self.try_consume_keyword(Keyword::Fail) {
                Some(ConflictClause::Fail)
            } else {
                return Err(ParseError {
                    message: "Expected REPLACE, IGNORE, ABORT, ROLLBACK, or FAIL after INSERT OR"
                        .to_string(),
                });
            }
        } else {
            None
        };

        self.consume_keyword(Keyword::Into)?;

        // Parse table name with optional schema qualifier
        let table_ref = self.parse_insert_table_ref()?;

        // Parse column list (optional)
        let columns = if self.try_consume(&Token::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            cols
        } else {
            BumpVec::new_in(self.arena)
        };

        // Parse source: VALUES, SELECT, or DEFAULT VALUES
        let source = if self.try_consume_keyword(Keyword::Default) {
            // INSERT ... DEFAULT VALUES
            self.consume_keyword(Keyword::Values)?;
            InsertSource::DefaultValues
        } else if self.try_consume_keyword(Keyword::Values) {
            // Parse VALUES clause
            let values = self.parse_values_clause()?;
            InsertSource::Values(values)
        } else if self.peek_keyword(Keyword::Select) || self.peek_keyword(Keyword::With) {
            // Parse SELECT subquery
            let query = self.parse_select_statement()?;
            InsertSource::Select(query)
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after INSERT".to_string(),
            });
        };

        // Parse optional ON CONFLICT or ON DUPLICATE KEY UPDATE clause
        let (on_conflict, on_duplicate_key_update) = self.parse_on_clause_for_insert()?;

        // Parse optional RETURNING clause (SQLite 3.35.0+)
        // Syntax: INSERT INTO ... [ON CONFLICT ...] RETURNING expr [, expr ...]
        let returning = if self.try_consume_keyword(Keyword::Returning) {
            Some(self.parse_select_list()?)
        } else {
            None
        };

        // Require end of statement: trailing tokens are a syntax error (issue #5261)
        self.expect_statement_end()?;

        let stmt = InsertStmt {
            with_clause: None,
            schema_name: table_ref.schema_name,
            schema_quoted: table_ref.schema_quoted,
            table_name: table_ref.table_name,
            table_quoted: table_ref.table_quoted,
            columns,
            source,
            conflict_clause,
            on_conflict,
            on_duplicate_key_update,
            returning,
        };

        Ok(self.arena.alloc(stmt))
    }

    /// Parse REPLACE statement (alias for INSERT OR REPLACE).
    pub(crate) fn parse_replace_statement(
        &mut self,
    ) -> Result<&'arena InsertStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Replace)?;
        self.consume_keyword(Keyword::Into)?;

        // Parse table name with optional schema qualifier
        let table_ref = self.parse_insert_table_ref()?;

        // Parse column list (optional)
        let columns = if self.try_consume(&Token::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            cols
        } else {
            BumpVec::new_in(self.arena)
        };

        // Parse source: VALUES, SELECT, or DEFAULT VALUES
        let source = if self.try_consume_keyword(Keyword::Default) {
            // REPLACE ... DEFAULT VALUES
            self.consume_keyword(Keyword::Values)?;
            InsertSource::DefaultValues
        } else if self.try_consume_keyword(Keyword::Values) {
            let values = self.parse_values_clause()?;
            InsertSource::Values(values)
        } else if self.peek_keyword(Keyword::Select) || self.peek_keyword(Keyword::With) {
            let query = self.parse_select_statement()?;
            InsertSource::Select(query)
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after REPLACE".to_string(),
            });
        };

        // Parse optional ON CONFLICT or ON DUPLICATE KEY UPDATE clause
        let (on_conflict, on_duplicate_key_update) = self.parse_on_clause_for_insert()?;

        // Parse optional RETURNING clause (SQLite 3.35.0+)
        // Syntax: INSERT INTO ... [ON CONFLICT ...] RETURNING expr [, expr ...]
        let returning = if self.try_consume_keyword(Keyword::Returning) {
            Some(self.parse_select_list()?)
        } else {
            None
        };

        // Require end of statement: trailing tokens are a syntax error (issue #5261)
        self.expect_statement_end()?;

        let stmt = InsertStmt {
            with_clause: None,
            schema_name: table_ref.schema_name,
            schema_quoted: table_ref.schema_quoted,
            table_name: table_ref.table_name,
            table_quoted: table_ref.table_quoted,
            columns,
            source,
            conflict_clause: Some(ConflictClause::Replace),
            on_conflict,
            on_duplicate_key_update,
            returning,
        };

        Ok(self.arena.alloc(stmt))
    }

    /// Parse VALUES clause: VALUES (row1), (row2), ...
    fn parse_values_clause(
        &mut self,
    ) -> Result<BumpVec<'arena, BumpVec<'arena, vibesql_ast::arena::Expression<'arena>>>, ParseError>
    {
        let mut rows = BumpVec::new_in(self.arena);

        loop {
            self.expect_token(Token::LParen)?;
            let row = self.parse_expression_list()?;
            self.expect_token(Token::RParen)?;
            rows.push(row);

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(rows)
    }

    /// Parse ON clause for INSERT statements (handles both ON CONFLICT and ON DUPLICATE KEY UPDATE)
    ///
    /// SQLite's generalized UPSERT accepts multiple `ON CONFLICT` clauses
    /// (upsert5.test). A target-less clause matches any conflict, so it must
    /// be the *last* clause — a non-terminal target-less clause is a syntax
    /// error (`near "ON": syntax error`, matching sqlite3).
    fn parse_on_clause_for_insert(
        &mut self,
    ) -> Result<
        (BumpVec<'arena, OnConflictClause<'arena>>, Option<BumpVec<'arena, Assignment<'arena>>>),
        ParseError,
    > {
        let mut clauses: BumpVec<'arena, OnConflictClause<'arena>> = BumpVec::new_in(self.arena);

        while self.try_consume_keyword(Keyword::On) {
            if self.try_consume_keyword(Keyword::Conflict) {
                // A target-less clause is a catch-all: SQLite only allows it
                // in the terminal position.
                if clauses.last().is_some_and(|c| c.conflict_target.is_none()) {
                    return Err(ParseError { message: "near \"ON\": syntax error".to_string() });
                }
                clauses.push(self.parse_one_on_conflict_clause()?);
            } else if self.try_consume_keyword(Keyword::Duplicate) {
                // MySQL: ON DUPLICATE KEY UPDATE ... (cannot be mixed with
                // SQLite ON CONFLICT clauses).
                if !clauses.is_empty() {
                    return Err(ParseError { message: "near \"ON\": syntax error".to_string() });
                }
                self.consume_keyword(Keyword::Key)?;
                self.consume_keyword(Keyword::Update)?;
                let assignments = self.parse_assignments()?;
                return Ok((clauses, Some(assignments)));
            } else {
                return Err(ParseError {
                    message: "Expected CONFLICT or DUPLICATE after ON".to_string(),
                });
            }
        }

        Ok((clauses, None))
    }

    /// Parse a single `ON CONFLICT [(target)] DO {NOTHING | UPDATE ...}`
    /// clause. The `ON CONFLICT` keywords have already been consumed.
    fn parse_one_on_conflict_clause(&mut self) -> Result<OnConflictClause<'arena>, ParseError> {
        // Parse optional conflict target (column list).
        //
        // The arena parser only supports plain column-name targets;
        // expression targets, COLLATE, and target-level WHERE predicates
        // fail here and fall back to the standard parser (which retains
        // them — see parser/insert.rs).
        let conflict_target = if self.try_consume(&Token::LParen) {
            let cols = self.parse_identifier_list()?;
            self.expect_token(Token::RParen)?;
            let mut items = BumpVec::new_in(self.arena);
            for col in cols {
                items.push(vibesql_ast::arena::ConflictTargetItem::Column(col));
            }
            Some(items)
        } else {
            None
        };

        self.consume_keyword(Keyword::Do)?;

        let action = if self.try_consume_keyword(Keyword::Nothing) {
            OnConflictAction::DoNothing
        } else if self.try_consume_keyword(Keyword::Update) {
            self.consume_keyword(Keyword::Set)?;

            // Parse assignment list
            let assignments = self.parse_assignments()?;

            // Parse optional WHERE clause
            let where_clause = if self.try_consume_keyword(Keyword::Where) {
                Some(self.parse_expression()?)
            } else {
                None
            };

            OnConflictAction::DoUpdate { assignments, where_clause }
        } else {
            return Err(ParseError { message: "Expected NOTHING or UPDATE after DO".to_string() });
        };

        Ok(OnConflictClause { conflict_target, target_where: None, target_inexact: false, action })
    }
}
