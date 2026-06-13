//! Function parsing module
//!
//! This module handles parsing of all SQL function types:
//! - Regular functions (e.g., UPPER, LOWER, CONCAT)
//! - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//! - Window functions (with OVER clause)
//! - Special SQL:1999 syntax (POSITION, TRIM, SUBSTRING)

use super::*;

mod string_special;
mod window;

impl Parser {
    /// Parse function call expressions (including window functions)
    pub(super) fn parse_function_call(
        &mut self,
    ) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        // Try to match either an identifier or specific keywords that can be function names
        let function_name = match self.peek() {
            Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                let name = id.clone();
                self.advance();
                // Check if followed by '('
                if !matches!(self.peek(), Token::LParen) {
                    // Not a function call - rewind
                    self.position -= 1;
                    return Ok(None);
                }
                name
            }
            // Allow LEFT, RIGHT, REPLACE, SCHEMA, GROUPING, GROUPING_ID, GLOB, LIKE, MATCH,
            // DATE, and TIME keywords as function names. These are reserved keywords but
            // can also be functions (DATE/TIME: SQLite date('now')/time('now'), #5307).
            Token::Keyword { keyword: Keyword::Left, .. }
            | Token::Keyword { keyword: Keyword::Right, .. }
            | Token::Keyword { keyword: Keyword::Replace, .. }
            | Token::Keyword { keyword: Keyword::Schema, .. }
            | Token::Keyword { keyword: Keyword::Grouping, .. }
            | Token::Keyword { keyword: Keyword::GroupingId, .. }
            | Token::Keyword { keyword: Keyword::Glob, .. }
            | Token::Keyword { keyword: Keyword::Like, .. }
            | Token::Keyword { keyword: Keyword::Match, .. }
            | Token::Keyword { keyword: Keyword::Date, .. }
            | Token::Keyword { keyword: Keyword::Time, .. } => {
                // Peek ahead to see if this is followed by '('
                // Don't consume the keyword unless we're sure it's a function
                // SQL:1999 normalizes unquoted identifiers (including function names) to lowercase
                let keyword_name = match self.peek() {
                    Token::Keyword { keyword: Keyword::Left, .. } => "left",
                    Token::Keyword { keyword: Keyword::Right, .. } => "right",
                    Token::Keyword { keyword: Keyword::Replace, .. } => "replace",
                    Token::Keyword { keyword: Keyword::Schema, .. } => "schema",
                    Token::Keyword { keyword: Keyword::Grouping, .. } => "grouping",
                    Token::Keyword { keyword: Keyword::GroupingId, .. } => "grouping_id",
                    Token::Keyword { keyword: Keyword::Glob, .. } => "glob",
                    Token::Keyword { keyword: Keyword::Like, .. } => "like",
                    Token::Keyword { keyword: Keyword::Match, .. } => "match",
                    Token::Keyword { keyword: Keyword::Date, .. } => "date",
                    Token::Keyword { keyword: Keyword::Time, .. } => "time",
                    _ => unreachable!(),
                };

                // Look ahead to next token
                if self.position + 1 < self.tokens.len() {
                    if matches!(self.tokens[self.position + 1], Token::LParen) {
                        // Yes, it's a function call
                        self.advance(); // consume keyword
                        keyword_name.to_string()
                    } else {
                        // Not a function call, don't consume
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        };

        self.advance(); // consume '('
        let first = function_name;

        // Special case for RAISE(ABORT|FAIL|ROLLBACK, msg) and RAISE(IGNORE).
        // SQLite's trigger-program error/abort expression. The first argument is
        // one of the conflict-resolution keywords (not a normal expression), so
        // it cannot go through the generic argument loop.
        if first.to_uppercase() == "RAISE" {
            return Ok(Some(self.parse_raise_expression()?));
        }

        // Special case for VALUES(column) - MySQL ON DUPLICATE KEY UPDATE
        // Returns the value that would have been inserted
        if first.to_uppercase() == "VALUES" {
            // Expect a single column name as argument
            let column = match self.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let column_name = col.clone();
                    self.advance();
                    column_name
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected column name in VALUES() function".to_string(),
                    })
                }
            };
            self.expect_token(Token::RParen)?;
            return Ok(Some(vibesql_ast::Expression::DuplicateKeyValue { column }));
        }

        // Special case for POSITION(substring IN string [USING unit])
        // SQL:1999 standard syntax
        if first.to_uppercase() == "POSITION" {
            return Ok(Some(self.parse_position_function()?));
        }

        // Special case for TRIM([position] [removal_char FROM] string)
        // SQL:1999 standard syntax
        if first.to_uppercase() == "TRIM" {
            return Ok(Some(self.parse_trim_function()?));
        }

        // Special case for SUBSTRING(string FROM start [FOR length] [USING unit])
        // SQL:1999 standard syntax - alternative to comma syntax
        if first.to_uppercase() == "SUBSTRING" {
            return Ok(Some(self.parse_substring_function(first)?));
        }

        // Special case for EXTRACT(field FROM expr)
        // SQL:1999 standard syntax for date/time extraction
        if first.to_uppercase() == "EXTRACT" {
            return Ok(Some(self.parse_extract_function()?));
        }

        // Check if this might be an aggregate function (before we know argument count)
        // Note: MIN/MAX with multiple arguments are scalar functions (like LEAST/GREATEST)
        // while MIN/MAX with a single argument are aggregate functions
        let function_name_upper = first.to_uppercase();
        let might_be_aggregate = matches!(
            function_name_upper.as_str(),
            "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "GROUP_CONCAT"
                | "STRING_AGG"
                | "TOTAL"
                | "JSON_GROUP_ARRAY"
        );

        // Parse optional DISTINCT or ALL for potential aggregate functions
        let distinct = if might_be_aggregate {
            if matches!(self.peek(), Token::Keyword { keyword: Keyword::Distinct, .. }) {
                self.advance(); // consume DISTINCT
                true
            } else if matches!(self.peek(), Token::Keyword { keyword: Keyword::All, .. }) {
                self.advance(); // consume ALL
                false
            } else {
                false // default is ALL
            }
        } else {
            false
        };

        // Parse function arguments
        let mut args = Vec::new();
        let mut character_unit = None;
        let mut order_by = None;

        // Check for empty argument list or '*'
        if matches!(self.peek(), Token::RParen) {
            // No arguments: func()
            self.advance();
        } else if matches!(self.peek(), Token::Symbol('*')) {
            // Special case for COUNT(*)
            self.advance(); // consume '*'
            self.expect_token(Token::RParen)?;
            // Represent * as a special wildcard expression
            args.push(vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "*", false,
            )));
        } else if might_be_aggregate
            && matches!(self.peek(), Token::Keyword { keyword: Keyword::Order, .. })
        {
            // Zero-arg aggregate with ORDER BY: count(ORDER BY a)
            // This is a special SQL extension allowing ORDER BY without arguments
            self.advance(); // consume ORDER
            self.expect_keyword(Keyword::By)?;

            let mut order_items = Vec::new();
            loop {
                // parse_expression already handles COLLATE as a postfix operator
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

                // Check for optional NULLS FIRST/LAST
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

            // Check for too many ORDER BY terms (SQLite compatibility)
            if order_items.len() > super::super::MAX_ORDER_BY_TERMS {
                return Err(ParseError {
                    message: "too many terms in ORDER BY clause".to_string(),
                });
            }

            order_by = Some(order_items);
            self.expect_token(Token::RParen)?;
        } else {
            // Parse comma-separated argument list
            loop {
                let arg = self.parse_expression()?;
                args.push(arg);

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }

            // Parse optional ORDER BY clause for aggregate functions
            // SQL:2003 syntax: aggregate(expr ORDER BY order_list)
            // Example: GROUP_CONCAT(name ORDER BY name ASC)
            if might_be_aggregate
                && matches!(self.peek(), Token::Keyword { keyword: Keyword::Order, .. })
            {
                self.advance(); // consume ORDER
                self.expect_keyword(Keyword::By)?;

                let mut order_items = Vec::new();
                loop {
                    // parse_expression already handles COLLATE as a postfix operator
                    let expr = self.parse_expression()?;

                    // Check for optional ASC/DESC
                    let direction =
                        if matches!(self.peek(), Token::Keyword { keyword: Keyword::Asc, .. }) {
                            self.advance();
                            vibesql_ast::OrderDirection::Asc
                        } else if matches!(
                            self.peek(),
                            Token::Keyword { keyword: Keyword::Desc, .. }
                        ) {
                            self.advance();
                            vibesql_ast::OrderDirection::Desc
                        } else {
                            vibesql_ast::OrderDirection::Asc // Default
                        };

                    // Check for optional NULLS FIRST/LAST
                    let nulls_order =
                        if matches!(self.peek(), Token::Keyword { keyword: Keyword::Nulls, .. }) {
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

                // Check for too many ORDER BY terms (SQLite compatibility)
                if order_items.len() > super::super::MAX_ORDER_BY_TERMS {
                    return Err(ParseError {
                        message: "too many terms in ORDER BY clause".to_string(),
                    });
                }

                order_by = Some(order_items);
            }

            // Parse optional USING clause for string functions BEFORE closing paren
            if matches!(function_name_upper.as_str(), "CHARACTER_LENGTH" | "CHAR_LENGTH")
                && matches!(self.peek(), Token::Keyword { keyword: Keyword::Using, .. })
            {
                self.advance(); // consume USING
                character_unit = Some(self.parse_character_unit()?);
            }

            self.expect_token(Token::RParen)?;
        }

        // Parse optional FILTER clause (SQL:2003)
        // Syntax: aggregate(...) FILTER (WHERE condition) [OVER (...)]
        let filter = if matches!(self.peek(), Token::Keyword { keyword: Keyword::Filter, .. }) {
            self.advance(); // consume FILTER
            self.expect_token(Token::LParen)?;
            self.expect_keyword(Keyword::Where)?;
            let condition = self.parse_expression()?;
            self.expect_token(Token::RParen)?;
            Some(Box::new(condition))
        } else {
            None
        };

        // Check for OVER clause (window function)
        if matches!(self.peek(), Token::Keyword { keyword: Keyword::Over, .. }) {
            self.advance(); // consume OVER

            // Validate argument count for window functions
            self.validate_window_function_args(&function_name_upper, args.len(), &first)?;

            // Parse window specification
            let window_spec = self.parse_window_spec()?;

            // Determine window function type based on function name
            let function_spec = self.classify_window_function(&first, args, filter);

            return Ok(Some(vibesql_ast::Expression::WindowFunction {
                function: function_spec,
                over: window_spec,
            }));
        }

        // Check if this is a window-only function used without OVER clause
        // These functions REQUIRE an OVER clause - they cannot be used as scalar functions
        if self.is_window_only_function(&function_name_upper) {
            return Err(ParseError {
                message: format!("misuse of window function {}()", first.to_lowercase()),
            });
        }

        // Determine if this is truly an aggregate function
        // MIN/MAX with >1 argument are scalar functions (SQLite compatibility)
        // GROUP_CONCAT accepts 1 or 2 arguments (expr, separator)
        let is_aggregate = match function_name_upper.as_str() {
            "COUNT" | "SUM" | "AVG" | "TOTAL" => true,
            "GROUP_CONCAT" | "STRING_AGG" => args.len() <= 2, // 1 or 2 args
            "JSON_GROUP_ARRAY" => true,                       // JSON aggregate function
            "MIN" | "MAX" => args.len() <= 1 && !distinct, // multi-arg or DISTINCT with >1 arg = scalar
            _ => false,
        };

        // Return appropriate expression type
        // FunctionIdentifier handles SQL:1999 case normalization internally
        // (canonical lowercase for comparison, display preserves original case)
        let func_id = vibesql_ast::FunctionIdentifier::new(&first);
        if is_aggregate {
            // DISTINCT aggregates must have exactly one argument (SQLite compatibility)
            if distinct && args.len() != 1 {
                return Err(ParseError {
                    message: "DISTINCT aggregates must have exactly one argument".to_string(),
                });
            }
            Ok(Some(vibesql_ast::Expression::AggregateFunction {
                name: func_id,
                distinct,
                args,
                order_by,
                filter,
            }))
        } else {
            // ORDER BY and FILTER are only allowed in aggregate functions
            if order_by.is_some() {
                return Err(ParseError {
                    message: format!(
                        "ORDER BY may not be used with non-aggregate {}()",
                        first.to_uppercase()
                    ),
                });
            }
            if filter.is_some() {
                return Err(ParseError {
                    message: format!(
                        "FILTER may not be used with non-aggregate {}()",
                        first.to_uppercase()
                    ),
                });
            }
            Ok(Some(vibesql_ast::Expression::Function { name: func_id, args, character_unit }))
        }
    }

    /// Parse a `RAISE(...)` trigger-program expression.
    ///
    /// The opening `RAISE` identifier and the `(` have already been consumed.
    /// Accepts SQLite's four forms:
    /// - `RAISE(ABORT, error-message)`
    /// - `RAISE(FAIL, error-message)`
    /// - `RAISE(ROLLBACK, error-message)`
    /// - `RAISE(IGNORE)`
    ///
    /// The error-message is required for ABORT/FAIL/ROLLBACK and forbidden for
    /// IGNORE (matching SQLite, which reports a `near ","`/`near ")"` syntax
    /// error for the mismatched forms).
    fn parse_raise_expression(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        use vibesql_ast::RaiseAction;

        // SQLite only permits RAISE() inside a trigger-program (a
        // `CREATE TRIGGER` body) and rejects it at prepare/parse time
        // everywhere else, e.g. `SELECT raise(ABORT, 'x')` ->
        // `RAISE() may only be used within a trigger-program`. Match that:
        // reject at parse time unless we are parsing a trigger body.
        if !self.in_trigger_body {
            return Err(ParseError {
                message: "RAISE() may only be used within a trigger-program".to_string(),
            });
        }

        // First argument: one of the conflict-resolution keywords.
        let action = match self.peek() {
            Token::Keyword { keyword: Keyword::Abort, .. } => RaiseAction::Abort,
            Token::Keyword { keyword: Keyword::Fail, .. } => RaiseAction::Fail,
            Token::Keyword { keyword: Keyword::Rollback, .. } => RaiseAction::Rollback,
            Token::Keyword { keyword: Keyword::Ignore, .. } => RaiseAction::Ignore,
            other => {
                return Err(ParseError {
                    message: format!(
                        "near \"{}\": syntax error (expected ABORT, FAIL, ROLLBACK, or IGNORE in RAISE())",
                        other
                    ),
                });
            }
        };
        self.advance(); // consume the action keyword

        let error_message = if matches!(action, RaiseAction::Ignore) {
            // RAISE(IGNORE) takes no message.
            self.expect_token(Token::RParen)?;
            None
        } else {
            // RAISE(ABORT|FAIL|ROLLBACK, error-message) requires a message.
            self.expect_token(Token::Comma)?;
            let message = self.parse_expression()?;
            self.expect_token(Token::RParen)?;
            Some(Box::new(message))
        };

        Ok(vibesql_ast::Expression::Raise { action, error_message })
    }
}
