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
                || self.peek_keyword(Keyword::Glob)
                || self.peek_keyword(Keyword::Exists)
            {
                // Restore position and let the other parsers handle it
                self.position = saved_pos;
                return self.parse_bitwise_or_expression();
            }

            // It's a unary NOT - parse the expression it applies to
            // Recursively call parse_not_expression to handle multiple NOTs
            let expr = self.parse_not_expression()?;

            Ok(vibesql_ast::Expression::UnaryOp {
                op: vibesql_ast::UnaryOperator::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_bitwise_or_expression()
        }
    }

    /// Parse bitwise OR expression (handles |)
    /// Precedence: between NOT and bitwise AND
    pub(super) fn parse_bitwise_or_expression(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let mut left = self.parse_bitwise_and_expression()?;

        while self.peek() == &Token::Symbol('|') {
            self.advance();
            let right = self.parse_bitwise_and_expression()?;
            left = vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::BitwiseOr,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse bitwise AND expression (handles &)
    /// Precedence: between bitwise OR and comparison
    pub(super) fn parse_bitwise_and_expression(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let mut left = self.parse_comparison_expression()?;

        while self.peek() == &Token::Symbol('&') {
            self.advance();
            let right = self.parse_comparison_expression()?;
            left = vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::BitwiseAnd,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse shift expression (handles <<, >>)
    /// Precedence: between comparison and additive
    pub(super) fn parse_shift_expression(&mut self) -> Result<vibesql_ast::Expression, ParseError> {
        let mut left = self.parse_additive_expression()?;

        loop {
            let op = match self.peek() {
                Token::Operator(crate::token::MultiCharOperator::LeftShift) => {
                    vibesql_ast::BinaryOperator::LeftShift
                }
                Token::Operator(crate::token::MultiCharOperator::RightShift) => {
                    vibesql_ast::BinaryOperator::RightShift
                }
                _ => break,
            };
            self.advance();

            let right = self.parse_additive_expression()?;
            left = vibesql_ast::Expression::BinaryOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
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
                Token::Keyword { keyword: Keyword::Div, .. } => {
                    vibesql_ast::BinaryOperator::IntegerDivide
                }
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
        let mut left = self.parse_shift_expression()?;

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
                let low = self.parse_shift_expression()?;
                self.consume_keyword(Keyword::And)?;
                let high = self.parse_shift_expression()?;

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
                let pattern = self.parse_shift_expression()?;

                return Ok(vibesql_ast::Expression::Like {
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated: true,
                });
            } else if self.peek_keyword(Keyword::Glob) {
                // It's NOT GLOB (SQLite)
                self.consume_keyword(Keyword::Glob)?;

                // Parse pattern expression
                let pattern = self.parse_shift_expression()?;

                return Ok(vibesql_ast::Expression::Glob {
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated: true,
                });
            } else {
                // Not "NOT IN", "NOT BETWEEN", "NOT LIKE", or "NOT GLOB", restore position and continue
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
        } else if self.peek_keyword(Keyword::Glob) {
            // It's GLOB (SQLite) (not negated)
            self.consume_keyword(Keyword::Glob)?;

            // Parse pattern expression
            let pattern = self.parse_additive_expression()?;

            return Ok(vibesql_ast::Expression::Glob {
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
                        // SQLite compatibility: == is a synonym for =
                        MultiCharOperator::DoubleEqual => vibesql_ast::BinaryOperator::Equal,
                        // Vector distance operators (pgvector compatible)
                        MultiCharOperator::CosineDistance => {
                            vibesql_ast::BinaryOperator::CosineDistance
                        }
                        MultiCharOperator::NegativeInnerProduct => {
                            vibesql_ast::BinaryOperator::NegativeInnerProduct
                        }
                        MultiCharOperator::L2Distance => vibesql_ast::BinaryOperator::L2Distance,
                        MultiCharOperator::Concat
                        | MultiCharOperator::LeftShift
                        | MultiCharOperator::RightShift => {
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

            let right = self.parse_shift_expression()?;
            left = vibesql_ast::Expression::BinaryOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        // Check for IS NULL / IS NOT NULL / IS [NOT] DISTINCT FROM / IS [NOT] TRUE/FALSE/UNKNOWN
        if self.peek_keyword(Keyword::Is) {
            self.consume_keyword(Keyword::Is)?;

            // Check for NOT
            let negated = if self.peek_keyword(Keyword::Not) {
                self.consume_keyword(Keyword::Not)?;
                true
            } else {
                false
            };

            // Check for DISTINCT FROM (SQL:1999), NULL, TRUE, FALSE, or UNKNOWN
            if self.peek_keyword(Keyword::Distinct) {
                self.consume_keyword(Keyword::Distinct)?;
                self.expect_keyword(Keyword::From)?;
                let right = self.parse_shift_expression()?;
                left = vibesql_ast::Expression::IsDistinctFrom {
                    left: Box::new(left),
                    right: Box::new(right),
                    negated,
                };
            } else if self.peek_keyword(Keyword::True) {
                self.consume_keyword(Keyword::True)?;
                left = vibesql_ast::Expression::IsTruthValue {
                    expr: Box::new(left),
                    truth_value: vibesql_ast::TruthValue::True,
                    negated,
                };
            } else if self.peek_keyword(Keyword::False) {
                self.consume_keyword(Keyword::False)?;
                left = vibesql_ast::Expression::IsTruthValue {
                    expr: Box::new(left),
                    truth_value: vibesql_ast::TruthValue::False,
                    negated,
                };
            } else if self.peek_keyword(Keyword::Unknown) {
                self.consume_keyword(Keyword::Unknown)?;
                left = vibesql_ast::Expression::IsTruthValue {
                    expr: Box::new(left),
                    truth_value: vibesql_ast::TruthValue::Unknown,
                    negated,
                };
            } else if self.peek_keyword(Keyword::Null) {
                // IS NULL / IS NOT NULL
                self.consume_keyword(Keyword::Null)?;
                left = vibesql_ast::Expression::IsNull { expr: Box::new(left), negated };
            } else {
                // SQLite compatibility: IS <expr> - compare using IS semantics (NULL-safe equals)
                // This handles cases like `expr IS 0` or `expr IS 1`
                let right = self.parse_shift_expression()?;
                left = vibesql_ast::Expression::IsDistinctFrom {
                    left: Box::new(left),
                    right: Box::new(right),
                    // IS is equivalent to IS NOT DISTINCT FROM (NULL-safe equals)
                    // IS NOT is equivalent to IS DISTINCT FROM (NULL-safe not equals)
                    negated: !negated,
                };
            }
        }

        Ok(left)
    }

    /// Parse unary expression (handles unary +, -, ~ operators)
    pub(super) fn parse_unary_expression(&mut self) -> Result<vibesql_ast::Expression, ParseError> {
        // Check for unary +, -, or ~
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
                // Special case: handle i64::MIN (-9223372036854775808)
                // The positive value 9223372036854775808 overflows i64, but when
                // negated it becomes i64::MIN which is valid. We need to detect
                // this case and parse the combined negative number as i64.
                if let Token::Number(n) = self.peek() {
                    if n == "9223372036854775808" {
                        // This is the only number that overflows i64 but is valid when negated
                        self.advance();
                        return Ok(vibesql_ast::Expression::Literal(
                            vibesql_types::SqlValue::Integer(i64::MIN),
                        ));
                    }
                }
                let expr = self.parse_unary_expression()?;
                Ok(vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Minus,
                    expr: Box::new(expr),
                })
            }
            Token::Symbol('~') => {
                self.advance();
                let expr = self.parse_unary_expression()?;
                Ok(vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::BitwiseNot,
                    expr: Box::new(expr),
                })
            }
            _ => self.parse_postfix_expression(),
        }
    }

    /// Parse postfix expressions (COLLATE)
    /// COLLATE has high precedence, binding tighter than most operators
    pub(super) fn parse_postfix_expression(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let mut expr = self.parse_primary_expression()?;

        // Handle COLLATE postfix operator
        while self.peek_keyword(Keyword::Collate) {
            self.consume_keyword(Keyword::Collate)?;
            // Parse collation name (identifier)
            let collation = match self.peek() {
                Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                    let name = name.clone();
                    self.advance();
                    name
                }
                Token::Keyword { keyword: kw, .. } => {
                    // Allow keywords like BINARY, NOCASE, RTRIM as collation names
                    let name = kw.to_string();
                    self.advance();
                    name
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected collation name after COLLATE".to_string(),
                    })
                }
            };
            expr = vibesql_ast::Expression::Collate { expr: Box::new(expr), collation };
        }

        Ok(expr)
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
