//! Window function parsing
//!
//! This module handles parsing of window functions including:
//! - Window specifications (OVER clause)
//! - Frame clauses (ROWS/RANGE)
//! - Frame boundaries (PRECEDING/FOLLOWING/CURRENT ROW)
//! - Window function classification

use super::super::*;

impl Parser {
    /// Check if a function is a window-only function that requires an OVER clause.
    /// These functions cannot be used as scalar functions - they MUST have an OVER clause.
    ///
    /// Returns true for:
    /// - Ranking functions: ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST
    /// - Value functions: LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
    pub(super) fn is_window_only_function(&self, name: &str) -> bool {
        matches!(
            name,
            // Ranking functions
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" | "PERCENT_RANK" | "CUME_DIST" |
            // Value functions
            "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE"
        )
    }

    /// Validate argument count for window functions.
    ///
    /// Window function signatures:
    /// - 0 args: ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST
    /// - 1 arg: NTILE, FIRST_VALUE, LAST_VALUE
    /// - 1-3 args: LAG, LEAD
    /// - 2 args: NTH_VALUE
    pub(super) fn validate_window_function_args(
        &self,
        name: &str,
        arg_count: usize,
        original_name: &str,
    ) -> Result<(), ParseError> {
        let (min_args, max_args) = match name {
            // Zero-argument ranking functions
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST" => (0, 0),
            // Single-argument functions
            "NTILE" | "FIRST_VALUE" | "LAST_VALUE" => (1, 1),
            // Variable-argument value functions
            "LAG" | "LEAD" => (1, 3),
            // Two-argument function
            "NTH_VALUE" => (2, 2),
            // Not a window-only function, no validation needed
            _ => return Ok(()),
        };

        if arg_count < min_args || arg_count > max_args {
            return Err(ParseError {
                message: format!(
                    "wrong number of arguments to function {}()",
                    original_name.to_lowercase()
                ),
            });
        }

        Ok(())
    }

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

    /// Returns true when the token immediately after `OVER` can begin a window
    /// specification: a `(` for an inline spec, or a window-name reference. A
    /// window name may be a plain identifier or a keyword that is usable as an
    /// identifier (SQLite fallback), e.g. `OVER over` / `OVER window` (window6
    /// 5.1-5.4). When this returns false, `OVER` is being used as a fallback
    /// identifier (a column alias), not a window clause.
    pub(super) fn over_starts_window_spec(&self) -> bool {
        match self.peek_next() {
            Token::LParen | Token::Identifier(_) | Token::DelimitedIdentifier(_) => true,
            Token::Keyword { keyword, .. } => keyword.can_be_identifier(),
            _ => false,
        }
    }

    /// Parse window specification (OVER clause contents)
    ///
    /// Supports:
    /// - `OVER ()` - empty window
    /// - `OVER (PARTITION BY ... ORDER BY ...)` - full specification
    /// - `OVER window_name` - reference to named window (no parens)
    /// - `OVER (window_name)` - reference to named window
    /// - `OVER (window_name ORDER BY ...)` - inherit from named window with additions
    pub(super) fn parse_window_spec(&mut self) -> Result<vibesql_ast::WindowSpec, ParseError> {
        // Check for OVER window_name (bare identifier, no parentheses)
        // This handles: OVER win
        match self.peek() {
            Token::Identifier(name) => {
                let base_name = name.clone();
                self.advance();
                return Ok(vibesql_ast::WindowSpec {
                    base_window_name: Some(base_name),
                    partition_by: None,
                    order_by: None,
                    frame: None,
                });
            }
            // A window-name reference may also be a keyword used as a fallback
            // identifier, e.g. `OVER over` / `OVER window` (window6 5.1-5.4).
            Token::Keyword { keyword, original, .. } if keyword.can_be_identifier() => {
                let base_name = original.clone();
                self.advance();
                return Ok(vibesql_ast::WindowSpec {
                    base_window_name: Some(base_name),
                    partition_by: None,
                    order_by: None,
                    frame: None,
                });
            }
            Token::LParen => {
                // Fall through to parenthesized window spec parsing
            }
            other => {
                return Err(ParseError {
                    message: format!(
                        "Expected window specification (identifier or '('), found {:?}",
                        other
                    ),
                });
            }
        }

        // OVER ( [window_name] [PARTITION BY expr_list] [ORDER BY order_list] [frame_clause] )
        self.expect_token(Token::LParen)?;

        let mut base_window_name = None;
        let mut partition_by = None;
        let mut order_by = None;
        let mut frame = None;

        // Check for empty OVER()
        if matches!(self.peek(), Token::RParen) {
            self.advance();
            return Ok(vibesql_ast::WindowSpec { base_window_name, partition_by, order_by, frame });
        }

        // Check for base window name (inheriting from a named window)
        // This handles: OVER (win ...) or OVER (win)
        // The identifier must be followed by a clause keyword (PARTITION, ORDER, ROWS, RANGE,
        // GROUPS) or closing paren - not followed by something that would make it an
        // expression
        if let Token::Identifier(name) = self.peek() {
            // Peek ahead to see what follows
            let name_clone = name.clone();

            // Look at the next token after the identifier
            // Save position and advance to check
            self.advance(); // consume identifier

            // Check if this looks like a window name reference
            match self.peek() {
                Token::RParen
                | Token::Keyword { keyword: Keyword::Partition, .. }
                | Token::Keyword { keyword: Keyword::Order, .. }
                | Token::Keyword { keyword: Keyword::Rows, .. }
                | Token::Keyword { keyword: Keyword::Range, .. }
                | Token::Keyword { keyword: Keyword::Groups, .. } => {
                    // This is a window name reference
                    base_window_name = Some(name_clone);
                }
                _ => {
                    // Not a window name, backtrack and parse normally as expression
                    // This case handles things like OVER (1) which isn't valid anyway
                    // but we'll let it fall through to PARTITION BY parsing
                    // Actually, we need to restore - but we can't easily backtrack
                    // So let's just error if we're in this ambiguous state
                    return Err(ParseError {
                        message: format!(
                            "Expected PARTITION BY, ORDER BY, frame clause, or ')' after window name '{}', found {:?}",
                            name_clone,
                            self.peek()
                        ),
                    });
                }
            }
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

                // Window ORDER BY supports NULLS FIRST/LAST (SQL:2003).
                let nulls_order =
                    if matches!(self.peek(), Token::Keyword { keyword: Keyword::Nulls, .. }) {
                        self.advance(); // consume NULLS
                        if matches!(self.peek(), Token::Keyword { keyword: Keyword::First, .. }) {
                            self.advance();
                            Some(vibesql_ast::NullsOrder::First)
                        } else if matches!(
                            self.peek(),
                            Token::Keyword { keyword: Keyword::Last, .. }
                        ) {
                            self.advance();
                            Some(vibesql_ast::NullsOrder::Last)
                        } else {
                            return Err(ParseError {
                                message: "Expected FIRST or LAST after NULLS".to_string(),
                            });
                        }
                    } else {
                        None
                    };

                order_items.push(vibesql_ast::OrderByItem { expr, direction, nulls_order });

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

        Ok(vibesql_ast::WindowSpec { base_window_name, partition_by, order_by, frame })
    }

    /// Parse frame clause (ROWS/RANGE/GROUPS BETWEEN ... AND ... [EXCLUDE ...])
    /// Reject UNBOUNDED FOLLOWING used as a frame *start* bound.
    ///
    /// SQLite's grammar (`frame_bound_s`) does not allow UNBOUNDED FOLLOWING at
    /// the start of a frame; it reports `near "FOLLOWING": syntax error`.
    fn reject_unbounded_start(bound: &vibesql_ast::FrameBound) -> Result<(), ParseError> {
        if matches!(bound, vibesql_ast::FrameBound::UnboundedFollowing) {
            return Err(ParseError { message: "near \"FOLLOWING\": syntax error".to_string() });
        }
        Ok(())
    }

    /// Reject UNBOUNDED PRECEDING used as a frame *end* bound.
    ///
    /// SQLite's grammar (`frame_bound_e`) does not allow UNBOUNDED PRECEDING at
    /// the end of a frame; it reports `near "PRECEDING": syntax error`.
    fn reject_unbounded_end(bound: &vibesql_ast::FrameBound) -> Result<(), ParseError> {
        if matches!(bound, vibesql_ast::FrameBound::UnboundedPreceding) {
            return Err(ParseError { message: "near \"PRECEDING\": syntax error".to_string() });
        }
        Ok(())
    }

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
        let (start, end) =
            if matches!(self.peek(), Token::Keyword { keyword: Keyword::Between, .. }) {
                self.advance(); // consume BETWEEN

                let start = self.parse_frame_bound()?;
                // A start bound may not be UNBOUNDED FOLLOWING (SQLite grammar:
                // frame_bound_s excludes UNBOUNDED FOLLOWING). SQLite reports a
                // syntax error at the FOLLOWING token.
                Self::reject_unbounded_start(&start)?;

                self.expect_keyword(Keyword::And)?;

                let end = self.parse_frame_bound()?;
                // An end bound may not be UNBOUNDED PRECEDING (SQLite grammar:
                // frame_bound_e excludes UNBOUNDED PRECEDING). SQLite reports a
                // syntax error at the PRECEDING token.
                Self::reject_unbounded_end(&end)?;

                (start, Some(end))
            } else {
                // Single bound (defaults to CURRENT ROW as end). A single start
                // bound is subject to the same restriction as a BETWEEN start.
                let start = self.parse_frame_bound()?;
                Self::reject_unbounded_start(&start)?;

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

    /// Parse WINDOW clause definitions
    ///
    /// Syntax: WINDOW name AS (window_spec) [, name AS (window_spec) ...]
    ///
    /// Example: WINDOW win AS (PARTITION BY x ORDER BY y), win2 AS (ORDER BY z)
    pub(crate) fn parse_window_definitions(
        &mut self,
    ) -> Result<Vec<vibesql_ast::WindowDefinition>, ParseError> {
        let mut definitions = Vec::new();

        loop {
            // Parse window name
            let name = match self.peek() {
                Token::Identifier(id) => {
                    let name = id.clone();
                    self.advance();
                    name
                }
                Token::Keyword { original, .. } => {
                    // Allow keywords as window names (for compatibility)
                    let name = original.clone();
                    self.advance();
                    name
                }
                _ => {
                    return Err(ParseError {
                        message: format!(
                            "Expected window name identifier, found {:?}",
                            self.peek()
                        ),
                    })
                }
            };

            // Expect AS keyword
            self.expect_keyword(Keyword::As)?;

            // Parse the window specification (including parentheses)
            self.expect_token(Token::LParen)?;

            let mut base_window_name = None;
            let mut partition_by = None;
            let mut order_by = None;
            let mut frame = None;

            // Check for empty window spec
            if !matches!(self.peek(), Token::RParen) {
                // Check for base window name
                if let Token::Identifier(base_name) = self.peek() {
                    let base_name_clone = base_name.clone();
                    self.advance();

                    // Check if this looks like a window name reference
                    match self.peek() {
                        Token::RParen
                        | Token::Keyword { keyword: Keyword::Partition, .. }
                        | Token::Keyword { keyword: Keyword::Order, .. }
                        | Token::Keyword { keyword: Keyword::Rows, .. }
                        | Token::Keyword { keyword: Keyword::Range, .. }
                        | Token::Keyword { keyword: Keyword::Groups, .. } => {
                            base_window_name = Some(base_name_clone);
                        }
                        _ => {
                            return Err(ParseError {
                                message: format!(
                                    "Expected window specification clause after '{}', found {:?}",
                                    base_name_clone,
                                    self.peek()
                                ),
                            });
                        }
                    }
                }

                // Parse PARTITION BY clause
                if matches!(self.peek(), Token::Keyword { keyword: Keyword::Partition, .. }) {
                    self.advance();
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
                    self.advance();
                    self.expect_keyword(Keyword::By)?;

                    let mut order_items = Vec::new();
                    loop {
                        let expr = self.parse_expression()?;
                        let direction = if matches!(
                            self.peek(),
                            Token::Keyword { keyword: Keyword::Asc, .. }
                        ) {
                            self.advance();
                            vibesql_ast::OrderDirection::Asc
                        } else if matches!(
                            self.peek(),
                            Token::Keyword { keyword: Keyword::Desc, .. }
                        ) {
                            self.advance();
                            vibesql_ast::OrderDirection::Desc
                        } else {
                            vibesql_ast::OrderDirection::Asc
                        };

                        // Window ORDER BY supports NULLS FIRST/LAST (SQL:2003).
                        let nulls_order = if matches!(
                            self.peek(),
                            Token::Keyword { keyword: Keyword::Nulls, .. }
                        ) {
                            self.advance(); // consume NULLS
                            if matches!(self.peek(), Token::Keyword { keyword: Keyword::First, .. })
                            {
                                self.advance();
                                Some(vibesql_ast::NullsOrder::First)
                            } else if matches!(
                                self.peek(),
                                Token::Keyword { keyword: Keyword::Last, .. }
                            ) {
                                self.advance();
                                Some(vibesql_ast::NullsOrder::Last)
                            } else {
                                return Err(ParseError {
                                    message: "Expected FIRST or LAST after NULLS".to_string(),
                                });
                            }
                        } else {
                            None
                        };

                        order_items.push(vibesql_ast::OrderByItem { expr, direction, nulls_order });

                        if matches!(self.peek(), Token::Comma) {
                            self.advance();
                        } else {
                            break;
                        }
                    }
                    order_by = Some(order_items);
                }

                // Parse frame clause
                if matches!(
                    self.peek(),
                    Token::Keyword { keyword: Keyword::Rows, .. }
                        | Token::Keyword { keyword: Keyword::Range, .. }
                        | Token::Keyword { keyword: Keyword::Groups, .. }
                ) {
                    frame = Some(self.parse_frame_clause()?);
                }
            }

            self.expect_token(Token::RParen)?;

            definitions.push(vibesql_ast::WindowDefinition {
                name,
                spec: vibesql_ast::WindowSpec { base_window_name, partition_by, order_by, frame },
            });

            // Check for more window definitions
            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(definitions)
    }
}
