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
        filter: Option<Box<vibesql_ast::Expression>>,
    ) -> vibesql_ast::WindowFunctionSpec {
        // Use uppercase for matching, FunctionIdentifier preserves original case for display
        let name_upper = name.to_uppercase();
        let func_id = vibesql_ast::FunctionIdentifier::new(name);

        match name_upper.as_str() {
            // Ranking functions (FILTER not applicable to ranking functions)
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" | "PERCENT_RANK" | "CUME_DIST" => {
                vibesql_ast::WindowFunctionSpec::Ranking { name: func_id, args }
            }

            // Value functions (FILTER not applicable to value functions)
            "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {
                vibesql_ast::WindowFunctionSpec::Value { name: func_id, args }
            }

            // Aggregate functions (SUM, AVG, COUNT, MIN, MAX, etc.)
            // FILTER clause is supported for aggregate window functions
            _ => vibesql_ast::WindowFunctionSpec::Aggregate { name: func_id, args, filter },
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
        if matches!(self.peek(), Token::Keyword { keyword: Keyword::Partition, .. }) {
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
        if matches!(self.peek(), Token::Keyword { keyword: Keyword::Order, .. }) {
            self.advance(); // consume ORDER
            self.expect_keyword(Keyword::By)?;

            let mut order_items = Vec::new();
            loop {
                let expr = self.parse_expression()?;

                // Check for optional ASC/DESC
                let direction =
                    if matches!(self.peek(), Token::Keyword { keyword: Keyword::Asc, .. }) {
                        self.advance();
                        vibesql_ast::OrderDirection::Asc
                    } else if matches!(self.peek(), Token::Keyword { keyword: Keyword::Desc, .. }) {
                        self.advance();
                        vibesql_ast::OrderDirection::Desc
                    } else {
                        vibesql_ast::OrderDirection::Asc // Default
                    };

                order_items.push(vibesql_ast::OrderByItem { expr, direction, nulls_order: None });

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }

            order_by = Some(order_items);
        }

        // Parse frame clause (ROWS/RANGE/GROUPS)
        if matches!(
            self.peek(),
            Token::Keyword { keyword: Keyword::Rows, .. }
                | Token::Keyword { keyword: Keyword::Range, .. }
                | Token::Keyword { keyword: Keyword::Groups, .. }
        ) {
            frame = Some(self.parse_frame_clause()?);
        }

        self.expect_token(Token::RParen)?;

        Ok(vibesql_ast::WindowSpec { partition_by, order_by, frame })
    }

    /// Parse frame clause (ROWS/RANGE/GROUPS BETWEEN ... AND ... [EXCLUDE ...])
    pub(super) fn parse_frame_clause(&mut self) -> Result<vibesql_ast::WindowFrame, ParseError> {
        // Parse frame unit (ROWS, RANGE, or GROUPS)
        let unit = match self.peek() {
            Token::Keyword { keyword: Keyword::Rows, .. } => {
                self.advance();
                vibesql_ast::FrameUnit::Rows
            }
            Token::Keyword { keyword: Keyword::Range, .. } => {
                self.advance();
                vibesql_ast::FrameUnit::Range
            }
            Token::Keyword { keyword: Keyword::Groups, .. } => {
                self.advance();
                vibesql_ast::FrameUnit::Groups
            }
            _ => {
                return Err(ParseError {
                    message: format!(
                        "Expected ROWS, RANGE, or GROUPS in frame clause, found {:?}",
                        self.peek()
                    ),
                })
            }
        };

        // Parse BETWEEN ... AND ... or single bound
        let (start, end) = if matches!(self.peek(), Token::Keyword { keyword: Keyword::Between, .. })
        {
            self.advance(); // consume BETWEEN

            let start = self.parse_frame_bound()?;

            self.expect_keyword(Keyword::And)?;

            let end = self.parse_frame_bound()?;

            (start, Some(end))
        } else {
            // Single bound (defaults to CURRENT ROW as end)
            let start = self.parse_frame_bound()?;

            (start, None)
        };

        // Parse optional EXCLUDE clause
        let exclude = self.parse_frame_exclude()?;

        Ok(vibesql_ast::WindowFrame { unit, start, end, exclude })
    }

    /// Parse optional EXCLUDE clause for window frames
    fn parse_frame_exclude(&mut self) -> Result<Option<vibesql_ast::FrameExclude>, ParseError> {
        if !matches!(self.peek(), Token::Keyword { keyword: Keyword::Exclude, .. }) {
            return Ok(None);
        }

        self.advance(); // consume EXCLUDE

        match self.peek() {
            // EXCLUDE NO OTHERS
            Token::Keyword { keyword: Keyword::No, .. } => {
                self.advance(); // consume NO
                self.expect_keyword(Keyword::Others)?;
                Ok(Some(vibesql_ast::FrameExclude::NoOthers))
            }

            // EXCLUDE CURRENT ROW
            Token::Keyword { keyword: Keyword::Current, .. } => {
                self.advance(); // consume CURRENT
                // Accept ROW as either keyword or identifier for compatibility
                match self.peek() {
                    Token::Keyword { keyword: Keyword::Row, .. } => {
                        self.advance();
                        Ok(Some(vibesql_ast::FrameExclude::CurrentRow))
                    }
                    Token::Identifier(ref id) if id.to_uppercase() == "ROW" => {
                        self.advance();
                        Ok(Some(vibesql_ast::FrameExclude::CurrentRow))
                    }
                    _ => Err(ParseError {
                        message: format!(
                            "Expected ROW after EXCLUDE CURRENT, found {:?}",
                            self.peek()
                        ),
                    }),
                }
            }

            // EXCLUDE GROUP
            Token::Keyword { keyword: Keyword::Group, .. } => {
                self.advance();
                Ok(Some(vibesql_ast::FrameExclude::Group))
            }

            // EXCLUDE TIES
            Token::Keyword { keyword: Keyword::Ties, .. } => {
                self.advance();
                Ok(Some(vibesql_ast::FrameExclude::Ties))
            }

            _ => Err(ParseError {
                message: format!(
                    "Expected NO OTHERS, CURRENT ROW, GROUP, or TIES after EXCLUDE, found {:?}",
                    self.peek()
                ),
            }),
        }
    }

    /// Parse a single frame boundary
    pub(super) fn parse_frame_bound(&mut self) -> Result<vibesql_ast::FrameBound, ParseError> {
        match self.peek() {
            Token::Keyword { keyword: Keyword::Unbounded, .. } => {
                self.advance(); // consume UNBOUNDED

                match self.peek() {
                    Token::Keyword { keyword: Keyword::Preceding, .. } => {
                        self.advance();
                        Ok(vibesql_ast::FrameBound::UnboundedPreceding)
                    }
                    Token::Keyword { keyword: Keyword::Following, .. } => {
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

            Token::Keyword { keyword: Keyword::Current, .. } => {
                self.advance(); // consume CURRENT
                                // Expect ROW (note: not ROWS, this is "CURRENT ROW" singular)
                                // Accept ROW as either keyword or identifier for compatibility
                match self.peek() {
                    Token::Keyword { keyword: Keyword::Row, .. } => {
                        self.advance();
                        Ok(vibesql_ast::FrameBound::CurrentRow)
                    }
                    Token::Identifier(ref id) if id.to_uppercase() == "ROW" => {
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

            // N PRECEDING or N FOLLOWING (including negative numbers like -1 PRECEDING)
            _ => {
                let offset = self.parse_unary_expression()?;

                match self.peek() {
                    Token::Keyword { keyword: Keyword::Preceding, .. } => {
                        self.advance();
                        Ok(vibesql_ast::FrameBound::Preceding(Box::new(offset)))
                    }
                    Token::Keyword { keyword: Keyword::Following, .. } => {
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
