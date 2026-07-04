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

    // ------------------------------------------------------------------
    // Arithmetic tier functions (shift -> additive -> concat -> multiplicative)
    //
    // Each tier comes in two forms:
    // - `parse_<tier>_expression()` parses a fresh operand (starting at the
    //   unary tier) and then climbs from that seed.
    // - `parse_<tier>_expression_from(left)` takes an already-parsed left
    //   operand, first runs it through all tighter tiers, then applies this
    //   tier's operator loop. Each operator loop exists exactly once, in the
    //   `_from` variant.
    //
    // Parallel-structure invariant (see arena_parser/expression.rs): the
    // arena parser mirrors this decomposition minus the shift and bitwise
    // tiers, which it deliberately lacks. The arena parser must produce ASTs
    // equivalent to this parser for everything it accepts; anything it cannot
    // express must fail arena parsing (triggering parse_with_arena_fallback),
    // never silently truncate.
    // ------------------------------------------------------------------

    /// Parse shift expression (handles <<, >>)
    /// Precedence: between comparison and additive
    pub(super) fn parse_shift_expression(&mut self) -> Result<vibesql_ast::Expression, ParseError> {
        let seed = self.parse_unary_expression()?;
        self.parse_shift_expression_from(seed)
    }

    /// Shift tier (`<<`, `>>`) with an already-parsed left operand.
    ///
    /// The seed is first climbed through the tighter tiers
    /// (multiplicative -> concat -> additive), so this doubles as the seeded
    /// precedence-climbing entry point used by `continue_higher_precedence_ops`.
    fn parse_shift_expression_from(
        &mut self,
        left: vibesql_ast::Expression,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let mut left = self.parse_additive_expression_from(left)?;

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

    /// Parse additive expression (handles +, -)
    /// Per SQLite, || has higher precedence than + and -, so it's handled in parse_concat_expression
    pub(super) fn parse_additive_expression(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let seed = self.parse_unary_expression()?;
        self.parse_additive_expression_from(seed)
    }

    /// Additive tier (`+`, `-`) with an already-parsed left operand.
    fn parse_additive_expression_from(
        &mut self,
        left: vibesql_ast::Expression,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let mut left = self.parse_concat_expression_from(left)?;

        loop {
            let op = match self.peek() {
                Token::Symbol('+') => vibesql_ast::BinaryOperator::Plus,
                Token::Symbol('-') => vibesql_ast::BinaryOperator::Minus,
                _ => break,
            };
            self.advance();

            let right = self.parse_concat_expression()?;
            left = vibesql_ast::Expression::BinaryOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse string concatenation expression (handles ||)
    /// Per SQLite, || has higher precedence than + and -, but lower than * / %
    pub(super) fn parse_concat_expression(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let seed = self.parse_unary_expression()?;
        self.parse_concat_expression_from(seed)
    }

    /// Concat tier (`||`) with an already-parsed left operand.
    fn parse_concat_expression_from(
        &mut self,
        left: vibesql_ast::Expression,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let mut left = self.parse_multiplicative_expression_from(left)?;

        while self.peek() == &Token::Operator(crate::token::MultiCharOperator::Concat) {
            self.advance();
            let right = self.parse_multiplicative_expression()?;
            left = vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Concat,
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
        let seed = self.parse_unary_expression()?;
        self.parse_multiplicative_expression_from(seed)
    }

    /// Multiplicative tier (`*`, `/`, `%`, `DIV`) with an already-parsed left operand.
    fn parse_multiplicative_expression_from(
        &mut self,
        mut left: vibesql_ast::Expression,
    ) -> Result<vibesql_ast::Expression, ParseError> {
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
    ///
    /// Per SQLite, all operators in this tier (=, <, >, IS, IN, BETWEEN, LIKE, GLOB,
    /// ISNULL, NOTNULL, ...) are left-associative and composable, so they chain:
    /// `1 IN (1) NOT IN (SELECT 2)` parses as `(1 IN (1)) NOT IN (SELECT 2)`.
    /// Additionally, IN's right operand is syntactically closed (a parenthesized
    /// list/subquery or a table name), so a tighter-binding operator that follows an
    /// IN node takes the whole IN expression as its left operand:
    /// `1 IN (SELECT 1) + 1` parses as `(1 IN (SELECT 1)) + 1`.
    pub(super) fn parse_comparison_expression(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let mut left = self.parse_shift_expression()?;

        loop {
            // Check for IN operator (including NOT IN) and BETWEEN (including NOT BETWEEN)
            if self.peek_keyword(Keyword::Not) {
                // Peek ahead to see if it's "NOT IN" or "NOT BETWEEN"
                let saved_pos = self.position;
                self.advance(); // consume NOT

                if self.peek_keyword(Keyword::In) {
                    // It's NOT IN
                    self.consume_keyword(Keyword::In)?;

                    // Check if it's NOT IN table_name (SQLite syntax) or NOT IN (...)
                    if self.peek() != &Token::LParen {
                        // SQLite compatibility: NOT IN table_name is equivalent to NOT IN (SELECT *
                        // FROM table_name) Parse the table name and expand to a
                        // subquery
                        let table_ref = self.parse_table_ref()?;
                        let table_name = table_ref.full_name();
                        let quoted = table_ref.is_any_quoted();

                        // Create a SELECT * FROM table_name subquery
                        let subquery = vibesql_ast::SelectStmt {
                            with_clause: None,
                            distinct: false,
                            select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
                            into_table: None,
                            into_variables: None,
                            from: Some(vibesql_ast::FromClause::Table {
                                index_hint: None,
                                name: table_name,
                                alias: None,
                                column_aliases: None,
                                quoted,
                            }),
                            where_clause: None,
                            group_by: None,
                            having: None,
                            window_definitions: None,
                            order_by: None,
                            limit: None,
                            offset: None,
                            set_operation: None,
                            values: None,
                        };

                        left = vibesql_ast::Expression::In {
                            expr: Box::new(left),
                            subquery: Box::new(subquery),
                            negated: true,
                        };
                    } else {
                        // Standard syntax: NOT IN (...)
                        self.expect_token(Token::LParen)?;

                        // Check if it's a subquery (SELECT ...) or a value list
                        // Also check for parenthesized subqueries like NOT IN ((SELECT ...))
                        let (is_subquery_through_parens, extra_paren_depth) =
                            self.peek_select_through_parens();

                        if self.peek_keyword(Keyword::Select)
                            || self.peek_keyword(Keyword::Values)
                            || self.peek_keyword(Keyword::With)
                            || is_subquery_through_parens
                        {
                            // It's a subquery: NOT IN (SELECT ...) or NOT IN ((SELECT ...))
                            // Consume any extra opening parentheses
                            for _ in 0..extra_paren_depth {
                                self.expect_token(Token::LParen)?;
                            }

                            // NOT IN (VALUES(...), ...) — a table value constructor
                            // subquery (rowvalue.test 17.1 / 21.0).
                            let subquery = if self.peek_keyword(Keyword::Values) {
                                self.parse_embedded_select_statement()?
                            } else {
                                self.parse_select_statement()?
                            };

                            // Consume matching closing parentheses
                            for _ in 0..extra_paren_depth {
                                self.expect_token(Token::RParen)?;
                            }
                            self.expect_token(Token::RParen)?;

                            // Don't return - assign to left and continue to check for IS NULL
                            left = vibesql_ast::Expression::In {
                                expr: Box::new(left),
                                subquery: Box::new(subquery),
                                negated: true,
                            };
                        } else {
                            // It's a value list: NOT IN (val1, val2, ...)
                            let values = self.parse_expression_list()?;
                            self.expect_token(Token::RParen)?;

                            // Empty IN lists are allowed per SQL:1999 (evaluates to TRUE for NOT IN)
                            // Don't return - assign to left and continue to check for IS NULL
                            left = vibesql_ast::Expression::InList {
                                expr: Box::new(left),
                                values,
                                negated: true,
                            };
                        }
                    }

                    // IN's right operand is syntactically closed, so tighter-binding
                    // operators that follow take the whole IN node as their left
                    // operand (SQLite: `1 NOT IN (SELECT 1) << 2`).
                    left = self.continue_higher_precedence_ops(left)?;
                    continue;
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

                    left = vibesql_ast::Expression::Between {
                        expr: Box::new(left),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated: true,
                        symmetric,
                    };
                    continue;
                } else if self.peek_keyword(Keyword::Like) {
                    // It's NOT LIKE
                    self.consume_keyword(Keyword::Like)?;

                    // Parse pattern expression
                    let pattern = self.parse_shift_expression()?;

                    // Check for optional ESCAPE clause
                    let escape = if self.peek_keyword(Keyword::Escape) {
                        self.consume_keyword(Keyword::Escape)?;
                        Some(Box::new(self.parse_shift_expression()?))
                    } else {
                        None
                    };

                    left = vibesql_ast::Expression::Like {
                        expr: Box::new(left),
                        pattern: Box::new(pattern),
                        negated: true,
                        escape,
                    };
                    continue;
                } else if self.peek_keyword(Keyword::Glob) {
                    // It's NOT GLOB (SQLite)
                    self.consume_keyword(Keyword::Glob)?;

                    // Parse pattern expression
                    let pattern = self.parse_shift_expression()?;

                    // Check for optional ESCAPE clause
                    let escape = if self.peek_keyword(Keyword::Escape) {
                        self.consume_keyword(Keyword::Escape)?;
                        Some(Box::new(self.parse_shift_expression()?))
                    } else {
                        None
                    };

                    left = vibesql_ast::Expression::Glob {
                        expr: Box::new(left),
                        pattern: Box::new(pattern),
                        negated: true,
                        escape,
                    };
                    continue;
                } else if self.peek_keyword(Keyword::Null) {
                    // SQLite compatibility: "expr NOT NULL" (without IS) is equivalent to "expr IS NOT
                    // NULL" BUT: In column definition context, "DEFAULT expr NOT NULL"
                    // should parse NOT NULL as a column constraint, not as part of the
                    // expression.
                    //
                    // Heuristic: If the left expression is a simple literal and what follows NULL
                    // could be a column constraint context (`,` `)` or constraint keyword), then
                    // treat NOT NULL as a column constraint, not an operator.
                    // This handles: DEFAULT 'table' NOT NULL
                    // But allows: WHERE col NOT NULL (col is not a literal)
                    self.consume_keyword(Keyword::Null)?;

                    // Check if left is a literal (in which case NOT NULL as an operator is semantically
                    // odd)
                    let left_is_literal = matches!(&left, vibesql_ast::Expression::Literal(_));

                    // Check what comes after NULL
                    let next_could_be_column_constraint = match self.peek() {
                        Token::Comma | Token::RParen => true,
                        Token::Keyword { keyword: kw, .. } => matches!(
                            kw,
                            // Column constraint keywords that can follow NOT NULL
                            Keyword::Check
                                | Keyword::Unique
                                | Keyword::Primary
                                | Keyword::References
                                | Keyword::Collate
                                | Keyword::Default
                                | Keyword::On
                                | Keyword::Generated
                                | Keyword::AutoIncrement
                                | Keyword::Constraint
                        ),
                        Token::Semicolon | Token::Eof => false, // At end of query, treat as operator
                        _ => false,
                    };

                    if left_is_literal && next_could_be_column_constraint {
                        // Likely in column definition: DEFAULT 'value' NOT NULL
                        // Restore position - NOT NULL should be parsed as column constraint
                        self.position = saved_pos;
                        break;
                    } else {
                        // General case: expr NOT NULL is an operator
                        left =
                            vibesql_ast::Expression::IsNull { expr: Box::new(left), negated: true };
                        continue;
                    }
                } else {
                    // Not "NOT IN", "NOT BETWEEN", "NOT LIKE", "NOT GLOB", or "NOT NULL", restore
                    // position and stop scanning this tier. Note: NOT EXISTS is handled in
                    // parse_primary_expression()
                    self.position = saved_pos;
                    break;
                }
            } else if self.peek_keyword(Keyword::In) {
                // It's IN (not negated)
                self.consume_keyword(Keyword::In)?;

                // Check if it's IN table_name (SQLite syntax) or IN (...)
                if self.peek() != &Token::LParen {
                    // SQLite compatibility: IN table_name is equivalent to IN (SELECT * FROM
                    // table_name) Parse the table name and expand to a subquery
                    let table_ref = self.parse_table_ref()?;
                    let table_name = table_ref.full_name();
                    let quoted = table_ref.is_any_quoted();

                    // Create a SELECT * FROM table_name subquery
                    let subquery = vibesql_ast::SelectStmt {
                        with_clause: None,
                        distinct: false,
                        select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
                        into_table: None,
                        into_variables: None,
                        from: Some(vibesql_ast::FromClause::Table {
                            index_hint: None,
                            name: table_name,
                            alias: None,
                            column_aliases: None,
                            quoted,
                        }),
                        where_clause: None,
                        group_by: None,
                        having: None,
                        window_definitions: None,
                        order_by: None,
                        limit: None,
                        offset: None,
                        set_operation: None,
                        values: None,
                    };

                    left = vibesql_ast::Expression::In {
                        expr: Box::new(left),
                        subquery: Box::new(subquery),
                        negated: false,
                    };
                } else {
                    // Standard syntax: IN (...)
                    self.expect_token(Token::LParen)?;

                    // Check if it's a subquery (SELECT ...) or a value list
                    // Also check for parenthesized subqueries like IN ((SELECT ...))
                    let (is_subquery_through_parens, extra_paren_depth) =
                        self.peek_select_through_parens();

                    if self.peek_keyword(Keyword::Select)
                        || self.peek_keyword(Keyword::Values)
                        || self.peek_keyword(Keyword::With)
                        || is_subquery_through_parens
                    {
                        // It's a subquery: IN (SELECT ...) or IN ((SELECT ...))
                        // Consume any extra opening parentheses
                        for _ in 0..extra_paren_depth {
                            self.expect_token(Token::LParen)?;
                        }

                        // IN (VALUES(...), ...) — a table value constructor
                        // subquery (rowvalue.test 17.1 / 21.0).
                        let subquery = if self.peek_keyword(Keyword::Values) {
                            self.parse_embedded_select_statement()?
                        } else {
                            self.parse_select_statement()?
                        };

                        // Consume matching closing parentheses
                        for _ in 0..extra_paren_depth {
                            self.expect_token(Token::RParen)?;
                        }
                        self.expect_token(Token::RParen)?;

                        // Don't return - assign to left and continue to check for IS NULL
                        left = vibesql_ast::Expression::In {
                            expr: Box::new(left),
                            subquery: Box::new(subquery),
                            negated: false,
                        };
                    } else {
                        // It's a value list: IN (val1, val2, ...)
                        let values = self.parse_expression_list()?;
                        self.expect_token(Token::RParen)?;

                        // Empty IN lists are allowed per SQL:1999 (evaluates to FALSE)
                        // Don't return - assign to left and continue to check for IS NULL
                        left = vibesql_ast::Expression::InList {
                            expr: Box::new(left),
                            values,
                            negated: false,
                        };
                    }
                }

                // IN's right operand is syntactically closed, so tighter-binding
                // operators that follow take the whole IN node as their left
                // operand (SQLite: `1 IN (SELECT 1) + 1` = `(1 IN (SELECT 1)) + 1`).
                left = self.continue_higher_precedence_ops(left)?;
                continue;
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
                // Bounds parse at the shift tier (same as NOT BETWEEN): everything
                // tighter than the comparison tier is allowed; only the boolean AND
                // separating low/high must not be consumed.
                let low = self.parse_shift_expression()?;
                self.consume_keyword(Keyword::And)?;
                let high = self.parse_shift_expression()?;

                left = vibesql_ast::Expression::Between {
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: false,
                    symmetric,
                };
                continue;
            } else if self.peek_keyword(Keyword::Like) {
                // It's LIKE (not negated)
                self.consume_keyword(Keyword::Like)?;

                // Parse pattern expression at the shift tier (same as NOT LIKE)
                let pattern = self.parse_shift_expression()?;

                // Check for optional ESCAPE clause
                let escape = if self.peek_keyword(Keyword::Escape) {
                    self.consume_keyword(Keyword::Escape)?;
                    Some(Box::new(self.parse_shift_expression()?))
                } else {
                    None
                };

                left = vibesql_ast::Expression::Like {
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated: false,
                    escape,
                };
                continue;
            } else if self.peek_keyword(Keyword::Glob) {
                // It's GLOB (SQLite) (not negated)
                self.consume_keyword(Keyword::Glob)?;

                // Parse pattern expression at the shift tier (same as NOT GLOB)
                let pattern = self.parse_shift_expression()?;

                // Check for optional ESCAPE clause
                let escape = if self.peek_keyword(Keyword::Escape) {
                    self.consume_keyword(Keyword::Escape)?;
                    Some(Box::new(self.parse_shift_expression()?))
                } else {
                    None
                };

                left = vibesql_ast::Expression::Glob {
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated: false,
                    escape,
                };
                continue;
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
                            MultiCharOperator::L2Distance => {
                                vibesql_ast::BinaryOperator::L2Distance
                            }
                            MultiCharOperator::Concat
                            | MultiCharOperator::LeftShift
                            | MultiCharOperator::RightShift
                            | MultiCharOperator::JsonExtract
                            | MultiCharOperator::JsonExtractText => {
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

                    left = vibesql_ast::Expression::QuantifiedComparison {
                        expr: Box::new(left),
                        op,
                        quantifier,
                        subquery: Box::new(subquery),
                    };
                    continue;
                }

                let right = self.parse_shift_expression()?;
                left = vibesql_ast::Expression::BinaryOp {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                };
                continue;
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
                continue;
            }

            // SQLite compatibility: ISNULL and NOTNULL as postfix operators
            // These are equivalent to IS NULL and IS NOT NULL respectively
            // The enclosing loop supports chaining: `x NOTNULL NOTNULL`
            if self.peek_keyword(Keyword::Isnull) {
                self.consume_keyword(Keyword::Isnull)?;
                left = vibesql_ast::Expression::IsNull { expr: Box::new(left), negated: false };
                continue;
            }
            if self.peek_keyword(Keyword::Notnull) {
                self.consume_keyword(Keyword::Notnull)?;
                left = vibesql_ast::Expression::IsNull { expr: Box::new(left), negated: true };
                continue;
            }

            // No more operators at this precedence tier
            break;
        }

        Ok(left)
    }

    /// Continue parsing tighter-binding binary operators (multiplicative, concat,
    /// additive, shift) with an already-parsed left operand.
    ///
    /// Used after an IN/NOT IN node: its right operand is syntactically closed
    /// (a parenthesized list/subquery or a table name), so per SQLite a
    /// tighter-binding operator that follows takes the entire IN expression as
    /// its left operand: `1 IN (SELECT 1) + 1` parses as `(1 IN (SELECT 1)) + 1`
    /// and `1 NOT IN (SELECT 1) << 2` as `(1 NOT IN (SELECT 1)) << 2`.
    ///
    /// Each operator's right operand is parsed at the next-tighter tier, so
    /// relative precedence among these operators is preserved
    /// (e.g. `x IN (1) + 2 * 3` parses as `(x IN (1)) + (2 * 3)`).
    ///
    /// Delegates to `parse_shift_expression_from`, which climbs the seeded
    /// left operand through the multiplicative, concat, additive, and shift
    /// tiers using the canonical per-tier operator loops. The arena parser
    /// has the same helper (minus the shift tier) in arena_parser/expression.rs.
    fn continue_higher_precedence_ops(
        &mut self,
        left: vibesql_ast::Expression,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        self.parse_shift_expression_from(left)
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

    /// Parse postfix expressions (JSON operators ->, ->>, and COLLATE)
    /// These have high precedence, binding tighter than most operators
    pub(super) fn parse_postfix_expression(
        &mut self,
    ) -> Result<vibesql_ast::Expression, ParseError> {
        let mut expr = self.parse_primary_expression()?;

        // Handle JSON operators -> and ->> (high precedence, left-associative)
        loop {
            let op = match self.peek() {
                Token::Operator(crate::token::MultiCharOperator::JsonExtract) => {
                    vibesql_ast::BinaryOperator::JsonExtract
                }
                Token::Operator(crate::token::MultiCharOperator::JsonExtractText) => {
                    vibesql_ast::BinaryOperator::JsonExtractText
                }
                _ => break,
            };
            self.advance();

            // Parse the key/index expression (usually a string or integer)
            let right = self.parse_primary_expression()?;
            expr = vibesql_ast::Expression::BinaryOp {
                op,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }

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
