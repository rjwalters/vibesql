use super::*;

impl Parser {
    /// Parse a subquery: (SELECT ...)
    /// Used for scalar subqueries and IN operator
    #[allow(dead_code)]
    pub(super) fn parse_subquery(&mut self) -> Result<vibesql_ast::SelectStmt, ParseError> {
        // Expect opening paren
        self.expect_token(Token::LParen)?;

        // Parse SELECT statement
        let select_stmt = self.parse_select_statement()?;

        // Expect closing paren
        self.expect_token(Token::RParen)?;

        Ok(select_stmt)
    }

    /// Parse scalar subquery, parenthesized expression, or row value constructor
    pub(super) fn parse_parenthesized(
        &mut self,
    ) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        match self.peek() {
            Token::LParen => {
                self.advance(); // consume '('

                // Check if this is a scalar subquery by peeking at next token
                if self.peek_keyword(Keyword::Select) {
                    // It's a scalar subquery: (SELECT ...)
                    let select_stmt = self.parse_select_statement()?;
                    self.expect_token(Token::RParen)?;
                    Ok(Some(vibesql_ast::Expression::ScalarSubquery(Box::new(select_stmt))))
                } else {
                    // Parse first expression
                    let first_expr = self.parse_expression()?;

                    // Check if this is a row value constructor (tuple): (expr, expr, ...)
                    if matches!(self.peek(), Token::Comma) {
                        let mut values = vec![first_expr];
                        while matches!(self.peek(), Token::Comma) {
                            self.advance(); // consume ','
                            values.push(self.parse_expression()?);
                        }
                        self.expect_token(Token::RParen)?;
                        Ok(Some(vibesql_ast::Expression::RowValueConstructor(values)))
                    } else {
                        // Regular parenthesized expression: (expr)
                        self.expect_token(Token::RParen)?;
                        Ok(Some(first_expr))
                    }
                }
            }
            _ => Ok(None),
        }
    }
}
