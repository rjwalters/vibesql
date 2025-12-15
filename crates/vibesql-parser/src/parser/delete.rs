use super::*;

impl Parser {
    /// Parse DELETE statement
    pub(super) fn parse_delete_statement(&mut self) -> Result<vibesql_ast::DeleteStmt, ParseError> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;

        // Check for optional ONLY keyword
        let only = self.try_consume_keyword(Keyword::Only);

        // Check for optional left parenthesis (for DELETE FROM ONLY (table_name) syntax)
        let has_paren = matches!(self.peek(), Token::LParen);
        if has_paren {
            self.advance(); // consume '('
        }

        // Parse table name with optional schema qualifier and quoted flag
        // Supports: tablename, "TableName", schema.table, "schema"."table"
        let table_ref = self.parse_table_ref()?;
        // Use full_name() to get combined schema.table format for backward compatibility
        let table_name = table_ref.full_name();
        let quoted = table_ref.is_any_quoted();

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

        Ok(vibesql_ast::DeleteStmt { only, table_name, quoted, where_clause })
    }
}
