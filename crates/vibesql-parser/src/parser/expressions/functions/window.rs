//! Window function parsing
//!
//! This module handles parsing of window functions including:
//! - Window specifications (OVER clause)
//! - Frame clauses (ROWS/RANGE)
//! - Frame boundaries (PRECEDING/FOLLOWING/CURRENT ROW)
//! - Window function classification

use super::super::*;

impl Parser {
    /// Classify a function as aggregate, ranking, or value window function
    pub(super) fn classify_window_function(
        &self,
        name: &str,
        args: Vec<vibesql_ast::Expression>,
    ) -> vibesql_ast::WindowFunctionSpec {
        let name_upper = name.to_uppercase();

        match name_upper.as_str() {
            // Ranking functions
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" => {
                vibesql_ast::WindowFunctionSpec::Ranking { name: name_upper, args }
            }

            // Value functions
            "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" => {
                vibesql_ast::WindowFunctionSpec::Value { name: name_upper, args }
            }

            // Aggregate functions (SUM, AVG, COUNT, MIN, MAX, etc.)
            _ => vibesql_ast::WindowFunctionSpec::Aggregate { name: name_upper, args },
        }
    }

    /// Parse window specification (OVER clause contents)
    pub(super) fn parse_window_spec(&mut self) -> Result<vibesql_ast::WindowSpec, ParseError> {
        // OVER ( [PARTITION BY expr_list] [ORDER BY order_list] [frame_clause] )
        self.expect_token(Token::LParen)?;

        let mut partition_by = None;
        let mut order_by = None;
        let mut frame = None;

        // Check for empty OVER()
        if matches!(self.peek(), Token::RParen) {
            self.advance();
            return Ok(vibesql_ast::WindowSpec { partition_by, order_by, frame });
        }

        // Parse PARTITION BY clause
        if matches!(self.peek(), Token::Keyword(Keyword::Partition)) {
            self.advance(); // consume PARTITION
            self.expect_keyword(Keyword::By)?;

            let mut expressions = vec![self.parse_expression()?];

            while matches!(self.peek(), Token::Comma) {
                self.advance();
                expressions.push(self.parse_expression()?);
            }

            partition_by = Some(expressions);
        }

        // Parse ORDER BY clause
        if matches!(self.peek(), Token::Keyword(Keyword::Order)) {
            self.advance(); // consume ORDER
            self.expect_keyword(Keyword::By)?;

            let mut order_items = Vec::new();
            loop {
                let expr = self.parse_expression()?;

                // Check for optional ASC/DESC
                let direction = if matches!(self.peek(), Token::Keyword(Keyword::Asc)) {
                    self.advance();
                    vibesql_ast::OrderDirection::Asc
                } else if matches!(self.peek(), Token::Keyword(Keyword::Desc)) {
                    self.advance();
                    vibesql_ast::OrderDirection::Desc
                } else {
                    vibesql_ast::OrderDirection::Asc // Default
                };

                order_items.push(vibesql_ast::OrderByItem { expr, direction });

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }

            order_by = Some(order_items);
        }

        // Parse frame clause (ROWS/RANGE)
        if matches!(self.peek(), Token::Keyword(Keyword::Rows) | Token::Keyword(Keyword::Range)) {
            frame = Some(self.parse_frame_clause()?);
        }

        self.expect_token(Token::RParen)?;

        Ok(vibesql_ast::WindowSpec { partition_by, order_by, frame })
    }

    /// Parse frame clause (ROWS/RANGE BETWEEN ... AND ...)
    pub(super) fn parse_frame_clause(&mut self) -> Result<vibesql_ast::WindowFrame, ParseError> {
        // Parse frame unit (ROWS or RANGE)
        let unit = match self.peek() {
            Token::Keyword(Keyword::Rows) => {
                self.advance();
                vibesql_ast::FrameUnit::Rows
            }
            Token::Keyword(Keyword::Range) => {
                self.advance();
                vibesql_ast::FrameUnit::Range
            }
            _ => {
                return Err(ParseError {
                    message: format!(
                        "Expected ROWS or RANGE in frame clause, found {:?}",
                        self.peek()
                    ),
                })
            }
        };

        // Parse BETWEEN ... AND ... or single bound
        if matches!(self.peek(), Token::Keyword(Keyword::Between)) {
            self.advance(); // consume BETWEEN

            let start = self.parse_frame_bound()?;

            self.expect_keyword(Keyword::And)?;

            let end = self.parse_frame_bound()?;

            Ok(vibesql_ast::WindowFrame { unit, start, end: Some(end) })
        } else {
            // Single bound (defaults to CURRENT ROW as end)
            let start = self.parse_frame_bound()?;

            Ok(vibesql_ast::WindowFrame { unit, start, end: None })
        }
    }

    /// Parse a single frame boundary
    pub(super) fn parse_frame_bound(&mut self) -> Result<vibesql_ast::FrameBound, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Unbounded) => {
                self.advance(); // consume UNBOUNDED

                match self.peek() {
                    Token::Keyword(Keyword::Preceding) => {
                        self.advance();
                        Ok(vibesql_ast::FrameBound::UnboundedPreceding)
                    }
                    Token::Keyword(Keyword::Following) => {
                        self.advance();
                        Ok(vibesql_ast::FrameBound::UnboundedFollowing)
                    }
                    _ => Err(ParseError {
                        message: format!(
                            "Expected PRECEDING or FOLLOWING after UNBOUNDED, found {:?}",
                            self.peek()
                        ),
                    }),
                }
            }

            Token::Keyword(Keyword::Current) => {
                self.advance(); // consume CURRENT
                                // Expect ROW (note: not ROWS, this is "CURRENT ROW" singular)
                                // Accept ROW as either keyword or identifier for compatibility
                match self.peek() {
                    Token::Keyword(Keyword::Row) => {
                        self.advance();
                        Ok(vibesql_ast::FrameBound::CurrentRow)
                    }
                    Token::Identifier(ref id) if self.resolve_str(*id).to_uppercase() == "ROW" => {
                        self.advance();
                        Ok(vibesql_ast::FrameBound::CurrentRow)
                    }
                    _ => Err(ParseError {
                        message: format!(
                            "Expected ROW after CURRENT in frame bound, found {:?}",
                            self.peek()
                        ),
                    }),
                }
            }

            // N PRECEDING or N FOLLOWING
            _ => {
                let offset = self.parse_primary_expression()?;

                match self.peek() {
                    Token::Keyword(Keyword::Preceding) => {
                        self.advance();
                        Ok(vibesql_ast::FrameBound::Preceding(Box::new(offset)))
                    }
                    Token::Keyword(Keyword::Following) => {
                        self.advance();
                        Ok(vibesql_ast::FrameBound::Following(Box::new(offset)))
                    }
                    _ => Err(ParseError {
                        message: format!(
                            "Expected PRECEDING or FOLLOWING in frame bound, found {:?}",
                            self.peek()
                        ),
                    }),
                }
            }
        }
    }
}
