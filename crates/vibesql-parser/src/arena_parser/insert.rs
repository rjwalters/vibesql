//! Arena-allocated INSERT statement parsing.

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::arena::{ConflictClause, InsertSource, InsertStmt, Symbol};

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

        // Check for conflict clause: INSERT OR REPLACE | INSERT OR IGNORE
        let conflict_clause = if self.try_consume_keyword(Keyword::Or) {
            if self.try_consume_keyword(Keyword::Replace) {
                Some(ConflictClause::Replace)
            } else if self.try_consume_keyword(Keyword::Ignore) {
                Some(ConflictClause::Ignore)
            } else {
                return Err(ParseError {
                    message: "Expected REPLACE or IGNORE after INSERT OR".to_string(),
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

        // Parse source: VALUES or SELECT
        let source = if self.try_consume_keyword(Keyword::Values) {
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

        // Parse optional ON DUPLICATE KEY UPDATE clause
        let on_duplicate_key_update = if self.try_consume_keyword(Keyword::On) {
            self.consume_keyword(Keyword::Duplicate)?;
            self.consume_keyword(Keyword::Key)?;
            self.consume_keyword(Keyword::Update)?;
            Some(self.parse_assignments()?)
        } else {
            None
        };

        // Consume optional semicolon
        self.try_consume(&Token::Semicolon);

        let stmt = InsertStmt {
            schema_name: table_ref.schema_name,
            schema_quoted: table_ref.schema_quoted,
            table_name: table_ref.table_name,
            table_quoted: table_ref.table_quoted,
            columns,
            source,
            conflict_clause,
            on_duplicate_key_update,
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

        // Parse source: VALUES or SELECT
        let source = if self.try_consume_keyword(Keyword::Values) {
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

        // Parse optional ON DUPLICATE KEY UPDATE clause
        let on_duplicate_key_update = if self.try_consume_keyword(Keyword::On) {
            self.consume_keyword(Keyword::Duplicate)?;
            self.consume_keyword(Keyword::Key)?;
            self.consume_keyword(Keyword::Update)?;
            Some(self.parse_assignments()?)
        } else {
            None
        };

        // Consume optional semicolon
        self.try_consume(&Token::Semicolon);

        let stmt = InsertStmt {
            schema_name: table_ref.schema_name,
            schema_quoted: table_ref.schema_quoted,
            table_name: table_ref.table_name,
            table_quoted: table_ref.table_quoted,
            columns,
            source,
            conflict_clause: Some(ConflictClause::Replace),
            on_duplicate_key_update,
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
}
