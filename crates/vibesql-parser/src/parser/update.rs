use super::*;

impl Parser {
    /// Parse UPDATE statement
    pub(super) fn parse_update_statement(&mut self) -> Result<vibesql_ast::UpdateStmt, ParseError> {
        self.expect_keyword(Keyword::Update)?;

        // Check for conflict clause: UPDATE OR REPLACE|IGNORE|ABORT|ROLLBACK|FAIL
        let conflict_clause = if self.peek_keyword(Keyword::Or) {
            self.advance(); // consume OR
            if self.peek_keyword(Keyword::Replace) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Replace)
            } else if self.peek_keyword(Keyword::Ignore) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Ignore)
            } else if self.peek_keyword(Keyword::Abort) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Abort)
            } else if self.peek_keyword(Keyword::Rollback) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Rollback)
            } else if self.peek_keyword(Keyword::Fail) {
                self.advance();
                Some(vibesql_ast::ConflictClause::Fail)
            } else {
                return Err(ParseError {
                    message: "Expected REPLACE, IGNORE, ABORT, ROLLBACK, or FAIL after UPDATE OR"
                        .to_string(),
                });
            }
        } else {
            None
        };

        // Parse table name with optional schema qualifier and quoted flag
        // Supports: tablename, "TableName", schema.table, "schema"."table"
        let table_ref = self.parse_table_ref()?;
        // Use full_name() to get combined schema.table format for backward compatibility
        let table_name = table_ref.full_name();
        let quoted = table_ref.is_any_quoted();

        // Parse SET keyword
        self.expect_keyword(Keyword::Set)?;

        // Parse assignments
        let mut assignments = Vec::new();
        loop {
            // Parse column name (support regular, delimited identifiers, and rowid keyword)
            let column = match self.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let c = col.clone();
                    self.advance();
                    c
                }
                // SQLite allows updating the virtual rowid column
                Token::Keyword { keyword: Keyword::Rowid, .. } => {
                    self.advance();
                    "rowid".to_string()
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected column name in SET clause".to_string(),
                    })
                }
            };

            // Expect =
            self.expect_token(Token::Symbol('='))?;

            // Parse value expression
            let value = self.parse_expression()?;

            assignments.push(vibesql_ast::Assignment { column, value });

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        // Parse optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.consume_keyword(Keyword::Where)?;
            // Check for WHERE CURRENT OF cursor_name
            if self.try_consume_keyword(Keyword::Current) {
                self.expect_keyword(Keyword::Of)?;
                let cursor_name = self.parse_identifier()?;
                Some(vibesql_ast::WhereClause::CurrentOf(cursor_name))
            } else {
                Some(vibesql_ast::WhereClause::Condition(self.parse_expression()?))
            }
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::UpdateStmt {
            table_name,
            quoted,
            assignments,
            where_clause,
            conflict_clause,
        })
    }
}
