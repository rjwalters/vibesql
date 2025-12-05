use super::*;

impl Parser {
    /// Parse OR expression (lowest precedence)
    pub(super) fn parse_or_expression(&mut self) -> Result<vibesql_ast::Expression, ParseError> {
        let mut left = self.parse_and_expression()?;

        while self.peek_keyword(Keyword::Or) {
            self.consume_keyword(Keyword::Or)?;
            let right = self.parse_and_expression()?;
            left = vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse AND expression
    pub(super) fn parse_and_expression(&mut self) -> Result<vibesql_ast::Expression, ParseError> {
        let mut left = self.parse_not_expression()?;

        while self.peek_keyword(Keyword::And) {
            self.consume_keyword(Keyword::And)?;
            let right = self.parse_not_expression()?;
            left = vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse NOT expression
    /// NOT has precedence between AND and comparison operators
    /// This ensures "NOT col IS NULL" parses as "NOT (col IS NULL)" not "(NOT col) IS NULL"
    pub(super) fn parse_not_expression(&mut self) -> Result<vibesql_ast::Expression, ParseError> {
        // Check for NOT keyword (but not NOT IN, NOT BETWEEN, NOT LIKE, NOT EXISTS)
        // Those are handled in parse_comparison_expression and parse_primary_expression
        if self.peek_keyword(Keyword::Not) {
            // Peek ahead to see if it's a special case
            let saved_pos = self.position;
            self.advance(); // consume NOT

            // Check for special cases that are NOT unary NOT
            if self.peek_keyword(Keyword::In)
                || self.peek_keyword(Keyword::Between)
                || self.peek_keyword(Keyword::Like)
                || self.peek_keyword(Keyword::Exists)
            {
                // Restore position and let the other parsers handle it
                self.position = saved_pos;
                return self.parse_comparison_expression();
            }

            // It's a unary NOT - parse the expression it applies to
            // Recursively call parse_not_expression to handle multiple NOTs
            let expr = self.parse_not_expression()?;

            Ok(vibesql_ast::Expression::UnaryOp {
                op: vibesql_ast::UnaryOperator::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison_expression()
        }
    }

    /// Parse additive expression (handles +, -, and ||)
    pub(super) fn parse_additive_expression(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let mut left = self.parse_multiplicative_expression()?;

        loop {
            let op = match self.peek() {
                Token::Symbol('+') => vibesql_ast::BinaryOperator::Plus,
                Token::Symbol('-') => vibesql_ast::BinaryOperator::Minus,
                Token::Operator(crate::token::MultiCharOperator::Concat) => {
                    vibesql_ast::BinaryOperator::Concat
                }
                _ => break,
            };
            self.advance();

            let right = self.parse_multiplicative_expression()?;
            left = vibesql_ast::Expression::BinaryOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse multiplicative expression (handles *, /, DIV, %)
    pub(super) fn parse_multiplicative_expression(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let mut left = self.parse_unary_expression()?;

        loop {
            let op = match self.peek() {
                Token::Symbol('*') => vibesql_ast::BinaryOperator::Multiply,
                Token::Symbol('/') => vibesql_ast::BinaryOperator::Divide,
                Token::Symbol('%') => vibesql_ast::BinaryOperator::Modulo,
                Token::Keyword(Keyword::Div) => vibesql_ast::BinaryOperator::IntegerDivide,
                _ => break,
            };
            self.advance();

            let right = self.parse_unary_expression()?;
            left = vibesql_ast::Expression::BinaryOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse comparison expression (handles =, <, >, <=, >=, !=, <>, IN, BETWEEN, LIKE, IS NULL)
    /// These operators have lower precedence than arithmetic operators
    pub(super) fn parse_comparison_expression(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let mut left = self.parse_additive_expression()?;

        // Check for IN operator (including NOT IN) and BETWEEN (including NOT BETWEEN)
        if self.peek_keyword(Keyword::Not) {
            // Peek ahead to see if it's "NOT IN" or "NOT BETWEEN"
            let saved_pos = self.position;
            self.advance(); // consume NOT

            if self.peek_keyword(Keyword::In) {
                // It's NOT IN
                self.consume_keyword(Keyword::In)?;

                // Expect opening paren
                self.expect_token(Token::LParen)?;

                // Check if it's a subquery (SELECT ...) or a value list
                if self.peek_keyword(Keyword::Select) {
                    // It's a subquery: NOT IN (SELECT ...)
                    let subquery = self.parse_select_statement()?;
                    self.expect_token(Token::RParen)?;

                    return Ok(vibesql_ast::Expression::In {
                        expr: Box::new(left),
                        subquery: Box::new(subquery),
                        negated: true,
                    });
                } else {
                    // It's a value list: NOT IN (val1, val2, ...)
                    let values = self.parse_expression_list()?;
                    self.expect_token(Token::RParen)?;

                    // Empty IN lists are allowed per SQL:1999 (evaluates to TRUE for NOT IN)
                    return Ok(vibesql_ast::Expression::InList {
                        expr: Box::new(left),
                        values,
                        negated: true,
                    });
                }
            } else if self.peek_keyword(Keyword::Between) {
                // It's NOT BETWEEN
                self.consume_keyword(Keyword::Between)?;

                // Check for optional ASYMMETRIC or SYMMETRIC
                let symmetric = if self.peek_keyword(Keyword::Symmetric) {
                    self.consume_keyword(Keyword::Symmetric)?;
                    true
                } else {
                    // ASYMMETRIC is default, but can be explicitly specified
                    if self.peek_keyword(Keyword::Asymmetric) {
                        self.consume_keyword(Keyword::Asymmetric)?;
                    }
                    false
                };

                // Parse low AND high
                let low = self.parse_additive_expression()?;
                self.consume_keyword(Keyword::And)?;
                let high = self.parse_additive_expression()?;

                return Ok(vibesql_ast::Expression::Between {
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: true,
                    symmetric,
                });
            } else if self.peek_keyword(Keyword::Like) {
                // It's NOT LIKE
                self.consume_keyword(Keyword::Like)?;

                // Parse pattern expression
                let pattern = self.parse_additive_expression()?;

                return Ok(vibesql_ast::Expression::Like {
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated: true,
                });
            } else {
                // Not "NOT IN", "NOT BETWEEN", or "NOT LIKE", restore position and continue
                // Note: NOT EXISTS is handled in parse_primary_expression()
                self.position = saved_pos;
            }
        } else if self.peek_keyword(Keyword::In) {
            // It's IN (not negated)
            self.consume_keyword(Keyword::In)?;

            // Expect opening paren
            self.expect_token(Token::LParen)?;

            // Check if it's a subquery (SELECT ...) or a value list
            if self.peek_keyword(Keyword::Select) {
                // It's a subquery: IN (SELECT ...)
                let subquery = self.parse_select_statement()?;
                self.expect_token(Token::RParen)?;

                return Ok(vibesql_ast::Expression::In {
                    expr: Box::new(left),
                    subquery: Box::new(subquery),
                    negated: false,
                });
            } else {
                // It's a value list: IN (val1, val2, ...)
                let values = self.parse_expression_list()?;
                self.expect_token(Token::RParen)?;

                // Empty IN lists are allowed per SQL:1999 (evaluates to FALSE)
                return Ok(vibesql_ast::Expression::InList {
                    expr: Box::new(left),
                    values,
                    negated: false,
                });
            }
        } else if self.peek_keyword(Keyword::Between) {
            // It's BETWEEN (not negated)
            self.consume_keyword(Keyword::Between)?;

            // Check for optional ASYMMETRIC or SYMMETRIC
            let symmetric = if self.peek_keyword(Keyword::Symmetric) {
                self.consume_keyword(Keyword::Symmetric)?;
                true
            } else {
                // ASYMMETRIC is default, but can be explicitly specified
                if self.peek_keyword(Keyword::Asymmetric) {
                    self.consume_keyword(Keyword::Asymmetric)?;
                }
                false
            };

            // Parse low AND high
            let low = self.parse_additive_expression()?;
            self.consume_keyword(Keyword::And)?;
            let high = self.parse_additive_expression()?;

            return Ok(vibesql_ast::Expression::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated: false,
                symmetric,
            });
        } else if self.peek_keyword(Keyword::Like) {
            // It's LIKE (not negated)
            self.consume_keyword(Keyword::Like)?;

            // Parse pattern expression
            let pattern = self.parse_additive_expression()?;

            return Ok(vibesql_ast::Expression::Like {
                expr: Box::new(left),
                pattern: Box::new(pattern),
                negated: false,
            });
        }

        // Check for comparison operators (both single-char and multi-char)
        // Note: Exclude || (concat) operator which should be handled in additive expression
        let is_comparison = match self.peek() {
            Token::Symbol('=') | Token::Symbol('<') | Token::Symbol('>') => true,
            Token::Operator(op) => !matches!(op, crate::token::MultiCharOperator::Concat),
            _ => false,
        };

        if is_comparison {
            let op = match self.peek() {
                Token::Symbol('=') => vibesql_ast::BinaryOperator::Equal,
                Token::Symbol('<') => vibesql_ast::BinaryOperator::LessThan,
                Token::Symbol('>') => vibesql_ast::BinaryOperator::GreaterThan,
                Token::Operator(op) => {
                    use crate::token::MultiCharOperator;
                    match op {
                        MultiCharOperator::LessEqual => {
                            vibesql_ast::BinaryOperator::LessThanOrEqual
                        }
                        MultiCharOperator::GreaterEqual => {
                            vibesql_ast::BinaryOperator::GreaterThanOrEqual
                        }
                        MultiCharOperator::NotEqual | MultiCharOperator::NotEqualAlt => {
                            vibesql_ast::BinaryOperator::NotEqual
                        }
                        // Vector distance operators (pgvector compatible)
                        MultiCharOperator::CosineDistance => {
                            vibesql_ast::BinaryOperator::CosineDistance
                        }
                        MultiCharOperator::NegativeInnerProduct => {
                            vibesql_ast::BinaryOperator::NegativeInnerProduct
                        }
                        MultiCharOperator::L2Distance => vibesql_ast::BinaryOperator::L2Distance,
                        MultiCharOperator::Concat => {
                            return Err(ParseError {
                                message: format!("Unexpected operator: {}", op),
                            })
                        }
                    }
                }
                _ => unreachable!(),
            };
            self.advance();

            // Check for quantified comparison (ALL, ANY, SOME)
            if self.peek_keyword(Keyword::All)
                || self.peek_keyword(Keyword::Any)
                || self.peek_keyword(Keyword::Some)
            {
                let quantifier = if self.peek_keyword(Keyword::All) {
                    self.consume_keyword(Keyword::All)?;
                    vibesql_ast::Quantifier::All
                } else if self.peek_keyword(Keyword::Any) {
                    self.consume_keyword(Keyword::Any)?;
                    vibesql_ast::Quantifier::Any
                } else {
                    self.consume_keyword(Keyword::Some)?;
                    vibesql_ast::Quantifier::Some
                };

                // Expect opening paren
                self.expect_token(Token::LParen)?;

                // Parse subquery
                let subquery = self.parse_select_statement()?;

                // Expect closing paren
                self.expect_token(Token::RParen)?;

                return Ok(vibesql_ast::Expression::QuantifiedComparison {
                    expr: Box::new(left),
                    op,
                    quantifier,
                    subquery: Box::new(subquery),
                });
            }

            let right = self.parse_additive_expression()?;
            left = vibesql_ast::Expression::BinaryOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        // Check for IS NULL / IS NOT NULL
        if self.peek_keyword(Keyword::Is) {
            self.consume_keyword(Keyword::Is)?;

            // Check for NOT
            let negated = if self.peek_keyword(Keyword::Not) {
                self.consume_keyword(Keyword::Not)?;
                true
            } else {
                false
            };

            // Expect NULL
            self.expect_keyword(Keyword::Null)?;

            left = vibesql_ast::Expression::IsNull { expr: Box::new(left), negated };
        }

        Ok(left)
    }

    /// Parse unary expression (handles unary +/- operators)
    pub(super) fn parse_unary_expression(&mut self) -> Result<vibesql_ast::Expression, ParseError> {
        // Check for unary + or -
        match self.peek() {
            Token::Symbol('+') => {
                self.advance();
                let expr = self.parse_unary_expression()?;
                Ok(vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Plus,
                    expr: Box::new(expr),
                })
            }
            Token::Symbol('-') => {
                self.advance();
                let expr = self.parse_unary_expression()?;
                Ok(vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Minus,
                    expr: Box::new(expr),
                })
            }
            _ => self.parse_primary_expression(),
        }
    }

    /// Parse a comma-separated list of expressions
    /// Used for IN (val1, val2, ...) and function arguments
    /// Does NOT consume the opening or closing parentheses
    pub fn parse_expression_list(&mut self) -> Result<Vec<vibesql_ast::Expression>, ParseError> {
        let mut expressions = Vec::new();

        // Check for empty list (SQLite compatibility)
        if matches!(self.peek(), Token::RParen) {
            return Ok(expressions);
        }

        // Parse first expression
        expressions.push(self.parse_expression()?);

        // Parse remaining expressions
        while matches!(self.peek(), Token::Comma) {
            self.advance(); // consume comma
            expressions.push(self.parse_expression()?);
        }

        Ok(expressions)
    }
}
