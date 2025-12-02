use super::*;

impl Parser {
    /// Parse DELETE statement
    pub(super) fn parse_delete_statement(&mut self) -> Result<vibesql_ast::DeleteStmt, ParseError> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;

        // Check for optional ONLY keyword
        let only = self.try_consume_keyword(Keyword::Only);

        // Check for optional left parenthesis
        let has_paren = matches!(self.peek(), Token::LParen);
        if has_paren {
            self.advance(); // consume '('
        }

        // Parse table name
        let table_name = match self.peek() {
            Token::Identifier(sym) => {
                let table = self.resolve(*sym);
                self.advance();
                table
            }
            _ => {
                return Err(ParseError {
                    message: "Expected table name after DELETE FROM".to_string(),
                })
            }
        };

        // If we had opening paren, expect closing paren
        if has_paren {
            if !matches!(self.peek(), Token::RParen) {
                return Err(ParseError { message: "Expected ')' after table name".to_string() });
            }
            self.advance(); // consume ')'
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

        Ok(vibesql_ast::DeleteStmt { only, table_name, where_clause })
    }
}
