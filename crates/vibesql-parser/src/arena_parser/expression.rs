//! Arena-allocated expression parsing.

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::{
    arena::{
        CaseWhen, Expression, ExtendedExpr, NullsOrder, OrderByItem, OrderDirection, Quantifier,
        Symbol, TruthValue, WindowFunctionSpec, WindowSpec,
    },
    BinaryOperator, UnaryOperator,
};
use vibesql_types::SqlValue;

use super::ArenaParser;
use crate::{
    keywords::Keyword,
    token::{MultiCharOperator, Token},
    ParseError,
};

impl<'arena> ArenaParser<'arena> {
    /// Parse an expression (entry point).
    pub(crate) fn parse_expression(&mut self) -> Result<Expression<'arena>, ParseError> {
        self.parse_or_expression()
    }

    /// Parse OR expression (lowest precedence).
    /// Produces a flat Disjunction for chains of 2+ OR operands.
    fn parse_or_expression(&mut self) -> Result<Expression<'arena>, ParseError> {
        let first = self.parse_and_expression()?;

        // Check if there's at least one OR
        if !self.peek_keyword(Keyword::Or) {
            return Ok(first);
        }

        // Collect all OR operands into a flat vector
        let mut terms = BumpVec::new_in(self.arena);
        terms.push(first);

        while self.peek_keyword(Keyword::Or) {
            self.consume_keyword(Keyword::Or)?;
            terms.push(self.parse_and_expression()?);
        }

        Ok(Expression::Disjunction(terms))
    }

    /// Parse AND expression.
    /// Produces a flat Conjunction for chains of 2+ AND operands.
    fn parse_and_expression(&mut self) -> Result<Expression<'arena>, ParseError> {
        let first = self.parse_not_expression()?;

        // Check if there's at least one AND
        if !self.peek_keyword(Keyword::And) {
            return Ok(first);
        }

        // Collect all AND operands into a flat vector
        let mut terms = BumpVec::new_in(self.arena);
        terms.push(first);

        while self.peek_keyword(Keyword::And) {
            self.consume_keyword(Keyword::And)?;
            terms.push(self.parse_not_expression()?);
        }

        Ok(Expression::Conjunction(terms))
    }

    /// Parse NOT expression.
    fn parse_not_expression(&mut self) -> Result<Expression<'arena>, ParseError> {
        if self.peek_keyword(Keyword::Not) {
            let saved_pos = self.position;
            self.advance();

            if self.peek_keyword(Keyword::In)
                || self.peek_keyword(Keyword::Between)
                || self.peek_keyword(Keyword::Like)
                || self.peek_keyword(Keyword::Exists)
            {
                self.position = saved_pos;
                return self.parse_comparison_expression();
            }

            let expr = self.parse_not_expression()?;
            let expr_ref = self.arena.alloc(expr);

            Ok(Expression::UnaryOp { op: UnaryOperator::Not, expr: expr_ref })
        } else {
            self.parse_comparison_expression()
        }
    }

    /// Parse comparison expression.
    fn parse_comparison_expression(&mut self) -> Result<Expression<'arena>, ParseError> {
        let mut left = self.parse_additive_expression()?;

        // Check for IN, BETWEEN, LIKE operators
        if self.peek_keyword(Keyword::Not) {
            let saved_pos = self.position;
            self.advance();

            if self.peek_keyword(Keyword::In) {
                self.consume_keyword(Keyword::In)?;
                self.expect_token(Token::LParen)?;

                // Check for parenthesized subqueries like NOT IN ((SELECT ...))
                let (is_subquery_through_parens, extra_paren_depth) =
                    self.peek_select_through_parens();

                if self.peek_keyword(Keyword::Select)
                    || self.peek_keyword(Keyword::Values)
                    || is_subquery_through_parens
                {
                    // Consume any extra opening parentheses
                    for _ in 0..extra_paren_depth {
                        self.expect_token(Token::LParen)?;
                    }

                    let subquery = self.parse_select_statement()?;

                    // Consume matching closing parentheses
                    for _ in 0..extra_paren_depth {
                        self.expect_token(Token::RParen)?;
                    }
                    self.expect_token(Token::RParen)?;
                    let left_ref = self.arena.alloc(left);
                    return Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::In {
                        expr: left_ref,
                        subquery,
                        negated: true,
                    })));
                } else {
                    let values = self.parse_expression_list()?;
                    self.expect_token(Token::RParen)?;
                    let left_ref = self.arena.alloc(left);
                    return Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::InList {
                        expr: left_ref,
                        values,
                        negated: true,
                    })));
                }
            } else if self.peek_keyword(Keyword::Between) {
                self.consume_keyword(Keyword::Between)?;
                let symmetric = self.try_consume_keyword(Keyword::Symmetric);
                if !symmetric {
                    self.try_consume_keyword(Keyword::Asymmetric);
                }

                let low = self.parse_additive_expression()?;
                self.consume_keyword(Keyword::And)?;
                let high = self.parse_additive_expression()?;

                let left_ref = self.arena.alloc(left);
                let low_ref = self.arena.alloc(low);
                let high_ref = self.arena.alloc(high);

                return Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::Between {
                    expr: left_ref,
                    low: low_ref,
                    high: high_ref,
                    negated: true,
                    symmetric,
                })));
            } else if self.peek_keyword(Keyword::Like) {
                self.consume_keyword(Keyword::Like)?;
                let pattern = self.parse_additive_expression()?;
                let escape: Option<&Expression<'arena>> = if self.peek_keyword(Keyword::Escape) {
                    self.consume_keyword(Keyword::Escape)?;
                    Some(self.arena.alloc(self.parse_additive_expression()?))
                } else {
                    None
                };
                let left_ref = self.arena.alloc(left);
                let pattern_ref = self.arena.alloc(pattern);
                return Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::Like {
                    expr: left_ref,
                    pattern: pattern_ref,
                    negated: true,
                    escape,
                })));
            } else if self.peek_keyword(Keyword::Null) {
                // SQLite compatibility: "expr NOT NULL" (without IS) is equivalent to "expr IS NOT NULL"
                self.consume_keyword(Keyword::Null)?;
                let left_ref = self.arena.alloc(left);
                return Ok(Expression::IsNull { expr: left_ref, negated: true });
            } else {
                self.position = saved_pos;
            }
        } else if self.peek_keyword(Keyword::In) {
            self.consume_keyword(Keyword::In)?;
            self.expect_token(Token::LParen)?;

            // Check for parenthesized subqueries like IN ((SELECT ...))
            let (is_subquery_through_parens, extra_paren_depth) = self.peek_select_through_parens();

            if self.peek_keyword(Keyword::Select)
                || self.peek_keyword(Keyword::Values)
                || is_subquery_through_parens
            {
                // Consume any extra opening parentheses
                for _ in 0..extra_paren_depth {
                    self.expect_token(Token::LParen)?;
                }

                let subquery = self.parse_select_statement()?;

                // Consume matching closing parentheses
                for _ in 0..extra_paren_depth {
                    self.expect_token(Token::RParen)?;
                }
                self.expect_token(Token::RParen)?;
                let left_ref = self.arena.alloc(left);
                return Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::In {
                    expr: left_ref,
                    subquery,
                    negated: false,
                })));
            } else {
                let values = self.parse_expression_list()?;
                self.expect_token(Token::RParen)?;
                let left_ref = self.arena.alloc(left);
                return Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::InList {
                    expr: left_ref,
                    values,
                    negated: false,
                })));
            }
        } else if self.peek_keyword(Keyword::Between) {
            self.consume_keyword(Keyword::Between)?;
            let symmetric = self.try_consume_keyword(Keyword::Symmetric);
            if !symmetric {
                self.try_consume_keyword(Keyword::Asymmetric);
            }

            let low = self.parse_additive_expression()?;
            self.consume_keyword(Keyword::And)?;
            let high = self.parse_additive_expression()?;

            let left_ref = self.arena.alloc(left);
            let low_ref = self.arena.alloc(low);
            let high_ref = self.arena.alloc(high);

            return Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::Between {
                expr: left_ref,
                low: low_ref,
                high: high_ref,
                negated: false,
                symmetric,
            })));
        } else if self.peek_keyword(Keyword::Like) {
            self.consume_keyword(Keyword::Like)?;
            let pattern = self.parse_additive_expression()?;
            let escape: Option<&Expression<'arena>> = if self.peek_keyword(Keyword::Escape) {
                self.consume_keyword(Keyword::Escape)?;
                Some(self.arena.alloc(self.parse_additive_expression()?))
            } else {
                None
            };
            let left_ref = self.arena.alloc(left);
            let pattern_ref = self.arena.alloc(pattern);
            return Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::Like {
                expr: left_ref,
                pattern: pattern_ref,
                negated: false,
                escape,
            })));
        }

        // Check for comparison operators
        let is_comparison = match self.peek() {
            Token::Symbol('=') | Token::Symbol('<') | Token::Symbol('>') => true,
            Token::Operator(s) => matches!(
                s,
                MultiCharOperator::LessEqual
                    | MultiCharOperator::GreaterEqual
                    | MultiCharOperator::NotEqual
                    | MultiCharOperator::NotEqualAlt
                    | MultiCharOperator::DoubleEqual
                    | MultiCharOperator::CosineDistance
                    | MultiCharOperator::NegativeInnerProduct
                    | MultiCharOperator::L2Distance
            ),
            _ => false,
        };

        if is_comparison {
            let op = match self.peek() {
                Token::Symbol('=') => BinaryOperator::Equal,
                Token::Symbol('<') => BinaryOperator::LessThan,
                Token::Symbol('>') => BinaryOperator::GreaterThan,
                Token::Operator(s) => match s {
                    MultiCharOperator::LessEqual => BinaryOperator::LessThanOrEqual,
                    MultiCharOperator::GreaterEqual => BinaryOperator::GreaterThanOrEqual,
                    MultiCharOperator::NotEqual | MultiCharOperator::NotEqualAlt => {
                        BinaryOperator::NotEqual
                    }
                    // SQLite compatibility: == is a synonym for =
                    MultiCharOperator::DoubleEqual => BinaryOperator::Equal,
                    // Vector distance operators (pgvector compatible)
                    MultiCharOperator::CosineDistance => BinaryOperator::CosineDistance,
                    MultiCharOperator::NegativeInnerProduct => BinaryOperator::NegativeInnerProduct,
                    MultiCharOperator::L2Distance => BinaryOperator::L2Distance,
                    _ => {
                        return Err(ParseError {
                            message: "Unexpected || operator in comparison".to_string(),
                        })
                    }
                },
                _ => unreachable!(),
            };
            self.advance();

            // Check for quantified comparison (ALL, ANY, SOME)
            if self.peek_keyword(Keyword::All)
                || self.peek_keyword(Keyword::Any)
                || self.peek_keyword(Keyword::Some)
            {
                let quantifier = if self.try_consume_keyword(Keyword::All) {
                    Quantifier::All
                } else if self.try_consume_keyword(Keyword::Any) {
                    Quantifier::Any
                } else {
                    self.consume_keyword(Keyword::Some)?;
                    Quantifier::Some
                };

                self.expect_token(Token::LParen)?;
                let subquery = self.parse_select_statement()?;
                self.expect_token(Token::RParen)?;

                let left_ref = self.arena.alloc(left);
                return Ok(Expression::Extended(self.arena.alloc(
                    ExtendedExpr::QuantifiedComparison { expr: left_ref, op, quantifier, subquery },
                )));
            }

            let right = self.parse_additive_expression()?;
            let left_ref = self.arena.alloc(left);
            let right_ref = self.arena.alloc(right);
            left = Expression::BinaryOp { op, left: left_ref, right: right_ref };
        }

        // Check for IS NULL / IS NOT NULL / IS [NOT] DISTINCT FROM / IS [NOT] TRUE/FALSE/UNKNOWN
        if self.peek_keyword(Keyword::Is) {
            self.consume_keyword(Keyword::Is)?;
            let negated = self.try_consume_keyword(Keyword::Not);

            // Check for DISTINCT FROM (SQL:1999), NULL, TRUE, FALSE, or UNKNOWN
            if self.peek_keyword(Keyword::Distinct) {
                self.consume_keyword(Keyword::Distinct)?;
                self.expect_keyword(Keyword::From)?;
                let right = self.parse_additive_expression()?;
                let left_ref = self.arena.alloc(left);
                let right_ref = self.arena.alloc(right);
                left = Expression::IsDistinctFrom { left: left_ref, right: right_ref, negated };
            } else if self.peek_keyword(Keyword::True) {
                self.consume_keyword(Keyword::True)?;
                let left_ref = self.arena.alloc(left);
                left = Expression::IsTruthValue {
                    expr: left_ref,
                    truth_value: TruthValue::True,
                    negated,
                };
            } else if self.peek_keyword(Keyword::False) {
                self.consume_keyword(Keyword::False)?;
                let left_ref = self.arena.alloc(left);
                left = Expression::IsTruthValue {
                    expr: left_ref,
                    truth_value: TruthValue::False,
                    negated,
                };
            } else if self.peek_keyword(Keyword::Unknown) {
                self.consume_keyword(Keyword::Unknown)?;
                let left_ref = self.arena.alloc(left);
                left = Expression::IsTruthValue {
                    expr: left_ref,
                    truth_value: TruthValue::Unknown,
                    negated,
                };
            } else if self.peek_keyword(Keyword::Null) {
                // IS NULL / IS NOT NULL
                self.consume_keyword(Keyword::Null)?;
                let left_ref = self.arena.alloc(left);
                left = Expression::IsNull { expr: left_ref, negated };
            } else {
                // SQLite compatibility: IS <expr> - compare using IS semantics (NULL-safe equals)
                // This handles cases like `expr IS 0` or `expr IS 1`
                let right = self.parse_additive_expression()?;
                let left_ref = self.arena.alloc(left);
                let right_ref = self.arena.alloc(right);
                // IS is equivalent to IS NOT DISTINCT FROM (NULL-safe equals)
                // IS NOT is equivalent to IS DISTINCT FROM (NULL-safe not equals)
                left = Expression::IsDistinctFrom {
                    left: left_ref,
                    right: right_ref,
                    negated: !negated,
                };
            }
        }

        // SQLite compatibility: ISNULL and NOTNULL as postfix operators
        // These are equivalent to IS NULL and IS NOT NULL respectively
        // Loop to support chaining: `x NOTNULL NOTNULL` → `(x IS NOT NULL) IS NOT NULL`
        loop {
            if self.peek_keyword(Keyword::Isnull) {
                self.consume_keyword(Keyword::Isnull)?;
                let left_ref = self.arena.alloc(left);
                left = Expression::IsNull { expr: left_ref, negated: false };
            } else if self.peek_keyword(Keyword::Notnull) {
                self.consume_keyword(Keyword::Notnull)?;
                let left_ref = self.arena.alloc(left);
                left = Expression::IsNull { expr: left_ref, negated: true };
            } else {
                break;
            }
        }

        Ok(left)
    }

    /// Parse additive expression (handles +, -).
    /// Per SQLite, || has higher precedence than + and -, so it's handled in parse_concat_expression.
    fn parse_additive_expression(&mut self) -> Result<Expression<'arena>, ParseError> {
        let mut left = self.parse_concat_expression()?;

        loop {
            let op = match self.peek() {
                Token::Symbol('+') => BinaryOperator::Plus,
                Token::Symbol('-') => BinaryOperator::Minus,
                _ => break,
            };
            self.advance();

            let right = self.parse_concat_expression()?;
            let left_ref = self.arena.alloc(left);
            let right_ref = self.arena.alloc(right);
            left = Expression::BinaryOp { op, left: left_ref, right: right_ref };
        }

        Ok(left)
    }

    /// Parse string concatenation expression (handles ||).
    /// Per SQLite, || has higher precedence than + and -, but lower than * / %.
    fn parse_concat_expression(&mut self) -> Result<Expression<'arena>, ParseError> {
        let mut left = self.parse_multiplicative_expression()?;

        while self.peek() == &Token::Operator(MultiCharOperator::Concat) {
            self.advance();
            let right = self.parse_multiplicative_expression()?;
            let left_ref = self.arena.alloc(left);
            let right_ref = self.arena.alloc(right);
            left = Expression::BinaryOp {
                op: BinaryOperator::Concat,
                left: left_ref,
                right: right_ref,
            };
        }

        Ok(left)
    }

    /// Parse multiplicative expression.
    fn parse_multiplicative_expression(&mut self) -> Result<Expression<'arena>, ParseError> {
        let mut left = self.parse_unary_expression()?;

        loop {
            let op = match self.peek() {
                Token::Symbol('*') => BinaryOperator::Multiply,
                Token::Symbol('/') => BinaryOperator::Divide,
                Token::Symbol('%') => BinaryOperator::Modulo,
                Token::Keyword { keyword: Keyword::Div, .. } => BinaryOperator::IntegerDivide,
                _ => break,
            };
            self.advance();

            let right = self.parse_unary_expression()?;
            let left_ref = self.arena.alloc(left);
            let right_ref = self.arena.alloc(right);
            left = Expression::BinaryOp { op, left: left_ref, right: right_ref };
        }

        Ok(left)
    }

    /// Parse unary expression.
    fn parse_unary_expression(&mut self) -> Result<Expression<'arena>, ParseError> {
        match self.peek() {
            Token::Symbol('+') => {
                self.advance();
                let expr = self.parse_unary_expression()?;
                let expr_ref = self.arena.alloc(expr);
                Ok(Expression::UnaryOp { op: UnaryOperator::Plus, expr: expr_ref })
            }
            Token::Symbol('-') => {
                self.advance();
                // Special case: handle i64::MIN (-9223372036854775808)
                // The positive value 9223372036854775808 overflows i64, but when
                // negated it becomes i64::MIN which is valid.
                if let Token::Number(n) = self.peek() {
                    if n == "9223372036854775808" {
                        self.advance();
                        return Ok(Expression::Literal(SqlValue::Integer(i64::MIN)));
                    }
                }
                let expr = self.parse_unary_expression()?;
                let expr_ref = self.arena.alloc(expr);
                Ok(Expression::UnaryOp { op: UnaryOperator::Minus, expr: expr_ref })
            }
            _ => self.parse_primary_expression(),
        }
    }

    /// Parse primary expression.
    fn parse_primary_expression(&mut self) -> Result<Expression<'arena>, ParseError> {
        // Placeholder (?)
        if matches!(self.peek(), Token::Placeholder) {
            self.advance();
            let index = self.next_placeholder();
            return Ok(Expression::Placeholder(index));
        }

        // Numbered placeholder ($1, $2, etc.)
        if let Token::NumberedPlaceholder(n) = self.peek() {
            let index = *n;
            self.advance();
            return Ok(Expression::NumberedPlaceholder(index));
        }

        // Named placeholder (:name)
        if let Token::NamedPlaceholder(name) = self.peek() {
            let name = name.clone();
            self.advance();
            let name = self.intern(&name);
            return Ok(Expression::NamedPlaceholder(name));
        }

        // Session variable (@@...)
        if let Token::SessionVariable(name) = self.peek() {
            let name = name.clone();
            self.advance();
            let name = self.intern(&name);
            return Ok(Expression::Extended(
                self.arena.alloc(ExtendedExpr::SessionVariable { name }),
            ));
        }

        // Literals
        if let Some(expr) = self.parse_literal()? {
            return Ok(expr);
        }

        // EXISTS / NOT EXISTS
        if self.peek_keyword(Keyword::Exists) || self.peek_keyword(Keyword::Not) {
            if let Some(expr) = self.parse_exists()? {
                return Ok(expr);
            }
        }

        // CASE expression
        if self.peek_keyword(Keyword::Case) {
            return self.parse_case_expression();
        }

        // CAST expression
        if self.peek_keyword(Keyword::Cast) {
            return self.parse_cast_expression();
        }

        // Current date/time
        if let Some(expr) = self.parse_current_datetime()? {
            return Ok(expr);
        }

        // Function call or identifier
        // Extract identifier from token (handles both regular identifiers and keywords-as-identifiers)
        let first_ident = match self.peek() {
            Token::Identifier(name) => Some((name.clone(), false)),
            Token::DelimitedIdentifier(name) => Some((name.clone(), true)),
            Token::Keyword { keyword: kw, .. } if kw.can_be_identifier() => {
                // Keywords like TEMP, YEAR, MONTH, etc. can be used as identifiers
                Some((kw.to_string().to_lowercase(), false))
            }
            _ => None,
        };

        if let Some((name, first_quoted)) = first_ident {
            // Check if it's a function call
            if matches!(self.tokens.get(self.position + 1), Some(Token::LParen)) {
                return self.parse_function_call(&name);
            }

            // Otherwise it's a column reference
            self.advance();
            let name_sym = self.intern(&name);

            // Check for qualified name (table.column or schema.table.column)
            if self.try_consume(&Token::Symbol('.')) {
                // Second part: identifier or any keyword (keywords allowed after .)
                let second_ident = match self.peek() {
                    Token::Identifier(id) => Some((id.clone(), false)),
                    Token::DelimitedIdentifier(id) => Some((id.clone(), true)),
                    Token::Keyword { keyword: kw, .. } => {
                        // Any keyword can be used as table/column name when qualified
                        Some((kw.to_string().to_lowercase(), false))
                    }
                    _ => None,
                };

                if let Some((second, second_quoted)) = second_ident {
                    self.advance();
                    let second_sym = self.intern(&second);

                    // Check for three-part name (schema.table.column)
                    if self.try_consume(&Token::Symbol('.')) {
                        // Third part: identifier or any keyword
                        let third_ident = match self.peek() {
                            Token::Identifier(id) => Some((id.clone(), false)),
                            Token::DelimitedIdentifier(id) => Some((id.clone(), true)),
                            Token::Keyword { keyword: kw, .. } => {
                                Some((kw.to_string().to_lowercase(), false))
                            }
                            _ => None,
                        };

                        if let Some((third, third_quoted)) = third_ident {
                            self.advance();
                            let third_sym = self.intern(&third);
                            return Ok(Expression::ColumnRef {
                                schema: Some(name_sym),
                                table: Some(second_sym),
                                column: third_sym,
                                schema_quoted: first_quoted,
                                table_quoted: second_quoted,
                                column_quoted: third_quoted,
                            });
                        } else if matches!(self.peek(), Token::Symbol('*')) {
                            // schema.table.* - not supported, treat as error
                            self.advance();
                            return Ok(Expression::Wildcard);
                        }
                    }

                    // Two-part name (table.column)
                    return Ok(Expression::ColumnRef {
                        schema: None,
                        table: Some(name_sym),
                        column: second_sym,
                        schema_quoted: false,
                        table_quoted: first_quoted,
                        column_quoted: second_quoted,
                    });
                } else if matches!(self.peek(), Token::Symbol('*')) {
                    self.advance();
                    return Ok(Expression::Wildcard);
                }
            }

            return Ok(Expression::ColumnRef {
                schema: None,
                table: None,
                column: name_sym,
                schema_quoted: false,
                table_quoted: false,
                column_quoted: first_quoted,
            });
        }

        // Wildcard (*)
        if matches!(self.peek(), Token::Symbol('*')) {
            self.advance();
            return Ok(Expression::Wildcard);
        }

        // Parenthesized expression, subquery, or row value constructor (tuple)
        if matches!(self.peek(), Token::LParen) {
            self.advance();

            // Check for subquery (SELECT or WITH-prefixed SELECT)
            if self.peek_keyword(Keyword::Select) || self.peek_keyword(Keyword::With) {
                let subquery = self.parse_select_statement()?;
                self.expect_token(Token::RParen)?;
                return Ok(Expression::Extended(
                    self.arena.alloc(ExtendedExpr::ScalarSubquery(subquery)),
                ));
            }

            // Parse first expression
            let first_expr = self.parse_expression()?;

            // Check if this is a row value constructor (tuple): (expr, expr, ...)
            if matches!(self.peek(), Token::Comma) {
                let mut values = BumpVec::new_in(self.arena);
                values.push(first_expr);
                while matches!(self.peek(), Token::Comma) {
                    self.advance(); // consume ','
                    values.push(self.parse_expression()?);
                }
                self.expect_token(Token::RParen)?;
                return Ok(Expression::Extended(
                    self.arena.alloc(ExtendedExpr::RowValueConstructor(values)),
                ));
            }

            // Regular parenthesized expression
            self.expect_token(Token::RParen)?;
            return Ok(first_expr);
        }

        Err(ParseError { message: self.peek().syntax_error() })
    }

    /// Parse literal values.
    fn parse_literal(&mut self) -> Result<Option<Expression<'arena>>, ParseError> {
        let expr = match self.peek() {
            Token::Number(n) => {
                let n = n.clone();
                self.advance();
                // Try to parse as integer first, then as float
                if let Ok(i) = n.parse::<i64>() {
                    Some(Expression::Literal(SqlValue::Integer(i)))
                } else if let Ok(f) = n.parse::<f64>() {
                    Some(Expression::Literal(SqlValue::Double(f)))
                } else {
                    return Err(ParseError { message: format!("Invalid number: {}", n) });
                }
            }
            Token::String(s) => {
                let s = arcstr::ArcStr::from(s.as_str());
                self.advance();
                Some(Expression::Literal(SqlValue::Varchar(s)))
            }
            Token::BlobLiteral(bytes) => {
                let blob = bytes.clone();
                self.advance();
                Some(Expression::Literal(SqlValue::Blob(blob)))
            }
            Token::Keyword { keyword: Keyword::True, .. } => {
                self.advance();
                Some(Expression::Literal(SqlValue::Boolean(true)))
            }
            Token::Keyword { keyword: Keyword::False, .. } => {
                self.advance();
                Some(Expression::Literal(SqlValue::Boolean(false)))
            }
            Token::Keyword { keyword: Keyword::Null, .. } => {
                self.advance();
                Some(Expression::Literal(SqlValue::Null))
            }
            Token::Keyword { keyword: Keyword::Default, .. } => {
                self.advance();
                Some(Expression::Default)
            }
            // Typed literals: DATE 'string', TIME 'string', TIMESTAMP 'string'
            // If not followed by a string literal, treat as column name (SQLite compatibility)
            Token::Keyword { keyword: Keyword::Date, .. } => {
                self.advance();
                match self.peek() {
                    Token::String(s) => {
                        let date_str = s.clone();
                        self.advance();
                        match date_str.parse::<vibesql_types::Date>() {
                            Ok(date) => Some(Expression::Literal(SqlValue::Date(date))),
                            Err(e) => {
                                return Err(ParseError {
                                    message: format!("Invalid DATE literal: {}", e),
                                })
                            }
                        }
                    }
                    _ => {
                        // Treat DATE as column name when not followed by string literal
                        Some(Expression::ColumnRef {
                            schema: None,
                            table: None,
                            column: self.interner.intern("DATE"),
                            schema_quoted: false,
                            table_quoted: false,
                            column_quoted: false,
                        })
                    }
                }
            }
            Token::Keyword { keyword: Keyword::Time, .. } => {
                self.advance();
                match self.peek() {
                    Token::String(s) => {
                        let time_str = s.clone();
                        self.advance();
                        match time_str.parse::<vibesql_types::Time>() {
                            Ok(time) => Some(Expression::Literal(SqlValue::Time(time))),
                            Err(e) => {
                                return Err(ParseError {
                                    message: format!("Invalid TIME literal: {}", e),
                                })
                            }
                        }
                    }
                    _ => {
                        // Treat TIME as column name when not followed by string literal
                        Some(Expression::ColumnRef {
                            schema: None,
                            table: None,
                            column: self.interner.intern("TIME"),
                            schema_quoted: false,
                            table_quoted: false,
                            column_quoted: false,
                        })
                    }
                }
            }
            Token::Keyword { keyword: Keyword::Timestamp, .. } => {
                self.advance();
                match self.peek() {
                    Token::String(s) => {
                        let timestamp_str = s.clone();
                        self.advance();
                        match timestamp_str.parse::<vibesql_types::Timestamp>() {
                            Ok(timestamp) => {
                                Some(Expression::Literal(SqlValue::Timestamp(timestamp)))
                            }
                            Err(e) => {
                                return Err(ParseError {
                                    message: format!("Invalid TIMESTAMP literal: {}", e),
                                })
                            }
                        }
                    }
                    _ => {
                        // Treat TIMESTAMP as column name when not followed by string literal
                        Some(Expression::ColumnRef {
                            schema: None,
                            table: None,
                            column: self.interner.intern("TIMESTAMP"),
                            schema_quoted: false,
                            table_quoted: false,
                            column_quoted: false,
                        })
                    }
                }
            }
            Token::Keyword { keyword: Keyword::Interval, .. } => {
                return self.parse_interval_literal();
            }
            _ => None,
        };
        Ok(expr)
    }

    /// Parse INTERVAL literal: INTERVAL 'value' field [TO field]
    fn parse_interval_literal(&mut self) -> Result<Option<Expression<'arena>>, ParseError> {
        self.advance(); // consume INTERVAL keyword
        match self.peek() {
            Token::String(interval_str) => {
                let value_str = interval_str.clone();
                self.advance();

                // Parse interval field (YEAR, MONTH, DAY, etc.)
                let start_field = self.parse_interval_field()?;

                // Check for TO (multi-field interval)
                let interval_spec = if self.try_consume_keyword(Keyword::To) {
                    let end_field = self.parse_interval_field()?;
                    format!("{} {} TO {}", value_str, start_field, end_field)
                } else {
                    format!("{} {}", value_str, start_field)
                };

                // Parse the interval string into an Interval type
                match interval_spec.parse::<vibesql_types::Interval>() {
                    Ok(interval) => Ok(Some(Expression::Literal(SqlValue::Interval(interval)))),
                    Err(e) => {
                        Err(ParseError { message: format!("Invalid INTERVAL literal: {}", e) })
                    }
                }
            }
            _ => Err(ParseError {
                message: "Expected string literal after INTERVAL keyword".to_string(),
            }),
        }
    }

    /// Parse an interval field name (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND).
    fn parse_interval_field(&mut self) -> Result<String, ParseError> {
        let field = match self.peek() {
            Token::Identifier(field) => field.to_uppercase(),
            Token::Keyword { keyword: Keyword::Year, .. } => "YEAR".to_string(),
            Token::Keyword { keyword: Keyword::Month, .. } => "MONTH".to_string(),
            Token::Keyword { keyword: Keyword::Day, .. } => "DAY".to_string(),
            Token::Keyword { keyword: Keyword::Hour, .. } => "HOUR".to_string(),
            Token::Keyword { keyword: Keyword::Minute, .. } => "MINUTE".to_string(),
            Token::Keyword { keyword: Keyword::Second, .. } => "SECOND".to_string(),
            _ => {
                return Err(ParseError {
                    message: "Expected interval field (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)"
                        .to_string(),
                })
            }
        };
        self.advance();
        Ok(field)
    }

    /// Parse EXISTS / NOT EXISTS.
    fn parse_exists(&mut self) -> Result<Option<Expression<'arena>>, ParseError> {
        let negated = if self.peek_keyword(Keyword::Not) {
            self.advance();
            if !self.peek_keyword(Keyword::Exists) {
                self.position -= 1;
                return Ok(None);
            }
            true
        } else {
            false
        };

        if self.peek_keyword(Keyword::Exists) {
            self.advance();
            self.expect_token(Token::LParen)?;
            let subquery = self.parse_select_statement()?;
            self.expect_token(Token::RParen)?;
            return Ok(Some(Expression::Extended(
                self.arena.alloc(ExtendedExpr::Exists { subquery, negated }),
            )));
        }

        Ok(None)
    }

    /// Parse CASE expression.
    fn parse_case_expression(&mut self) -> Result<Expression<'arena>, ParseError> {
        self.consume_keyword(Keyword::Case)?;

        // Check for simple CASE (CASE expr WHEN ...)
        let operand: Option<&'arena Expression<'arena>> = if !self.peek_keyword(Keyword::When) {
            let expr = self.parse_expression()?;
            Some(self.arena.alloc(expr))
        } else {
            None
        };

        // Parse WHEN clauses
        let mut when_clauses = BumpVec::new_in(self.arena);
        while self.peek_keyword(Keyword::When) {
            self.consume_keyword(Keyword::When)?;

            let mut conditions = BumpVec::new_in(self.arena);
            conditions.push(self.parse_expression()?);

            // Handle multiple conditions (WHEN a, b, c THEN ...)
            while self.try_consume(&Token::Comma) {
                conditions.push(self.parse_expression()?);
            }

            self.consume_keyword(Keyword::Then)?;
            let result = self.parse_expression()?;

            when_clauses.push(CaseWhen { conditions, result });
        }

        // Parse optional ELSE
        let else_result: Option<&'arena Expression<'arena>> =
            if self.try_consume_keyword(Keyword::Else) {
                let expr = self.parse_expression()?;
                Some(self.arena.alloc(expr))
            } else {
                None
            };

        self.consume_keyword(Keyword::End)?;

        Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::Case {
            operand,
            when_clauses,
            else_result,
        })))
    }

    /// Parse CAST expression.
    fn parse_cast_expression(&mut self) -> Result<Expression<'arena>, ParseError> {
        self.consume_keyword(Keyword::Cast)?;
        self.expect_token(Token::LParen)?;

        let expr = self.parse_expression()?;
        let expr_ref = self.arena.alloc(expr);

        self.consume_keyword(Keyword::As)?;
        // SQLite tolerates a missing type name: CAST(x AS ) parses with
        // no affinity (treated like CAST(x AS ANY) — value passes through).
        let data_type = if matches!(self.peek(), Token::RParen) {
            vibesql_types::DataType::BinaryLargeObject
        } else {
            self.parse_data_type()?
        };

        self.expect_token(Token::RParen)?;

        Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::Cast { expr: expr_ref, data_type })))
    }

    /// Parse current date/time functions.
    fn parse_current_datetime(&mut self) -> Result<Option<Expression<'arena>>, ParseError> {
        if self.peek_keyword(Keyword::CurrentDate) {
            self.advance();
            return Ok(Some(Expression::CurrentDate));
        }

        if self.peek_keyword(Keyword::CurrentTime) {
            self.advance();
            let precision = self.parse_optional_precision()?;
            return Ok(Some(Expression::CurrentTime { precision }));
        }

        if self.peek_keyword(Keyword::CurrentTimestamp) {
            self.advance();
            let precision = self.parse_optional_precision()?;
            return Ok(Some(Expression::CurrentTimestamp { precision }));
        }

        Ok(None)
    }

    /// Parse optional precision (e.g., CURRENT_TIME(3)).
    fn parse_optional_precision(&mut self) -> Result<Option<u32>, ParseError> {
        if self.try_consume(&Token::LParen) {
            if let Token::Number(n) = self.peek() {
                let n = n
                    .parse::<u32>()
                    .map_err(|_| ParseError { message: "Invalid precision".to_string() })?;
                self.advance();
                self.expect_token(Token::RParen)?;
                return Ok(Some(n));
            }
            self.expect_token(Token::RParen)?;
        }
        Ok(None)
    }

    /// Parse function call.
    fn parse_function_call(&mut self, name: &str) -> Result<Expression<'arena>, ParseError> {
        let name_upper = name.to_uppercase();
        let name_sym = self.intern(name);
        self.advance(); // consume function name
        self.expect_token(Token::LParen)?;

        // Check if this might be an aggregate function (before we know argument count)
        // Note: MIN/MAX with multiple arguments are scalar functions (like LEAST/GREATEST)
        // while MIN/MAX with a single argument are aggregate functions
        let might_be_aggregate = matches!(
            name_upper.as_str(),
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "STRING_AGG" | "TOTAL"
        );

        if might_be_aggregate {
            let distinct = self.try_consume_keyword(Keyword::Distinct);
            self.try_consume_keyword(Keyword::All); // ALL is default

            let args = if matches!(self.peek(), Token::RParen) {
                BumpVec::new_in(self.arena)
            } else {
                self.parse_expression_list()?
            };

            self.expect_token(Token::RParen)?;

            // Check for OVER clause (window function)
            if self.peek_keyword(Keyword::Over) {
                // Validate argument count for window functions
                Self::validate_window_function_args(&name_upper, args.len(), name)?;
                return self.parse_window_function(name_sym, &name_upper, args);
            }

            // Determine if this is truly an aggregate function
            // MIN/MAX with >1 argument are scalar functions (SQLite compatibility)
            // GROUP_CONCAT accepts 1 or 2 arguments (expr, separator)
            let is_aggregate = match name_upper.as_str() {
                "COUNT" | "SUM" | "AVG" | "TOTAL" => true,
                "GROUP_CONCAT" | "STRING_AGG" => args.len() <= 2, // 1 or 2 args
                "MIN" | "MAX" => args.len() <= 1 && !distinct,
                _ => false,
            };

            if is_aggregate {
                // DISTINCT aggregates must have exactly one argument (SQLite compatibility)
                if distinct && args.len() != 1 {
                    return Err(ParseError {
                        message: "DISTINCT aggregates must have exactly one argument".to_string(),
                    });
                }
                return Ok(Expression::Extended(
                    self.arena.alloc(ExtendedExpr::AggregateFunction {
                        name: name_sym,
                        distinct,
                        args,
                    }),
                ));
            } else {
                // Multi-argument MIN/MAX - treat as scalar function
                return Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::Function {
                    name: name_sym,
                    args,
                    character_unit: None,
                })));
            }
        }

        // Regular function
        let args = if matches!(self.peek(), Token::RParen) {
            BumpVec::new_in(self.arena)
        } else {
            self.parse_expression_list()?
        };

        self.expect_token(Token::RParen)?;

        // Check for OVER clause
        if self.peek_keyword(Keyword::Over) {
            // Validate argument count for window functions
            Self::validate_window_function_args(&name_upper, args.len(), name)?;
            return self.parse_window_function(name_sym, &name_upper, args);
        }

        // Check if this is a window-only function used without OVER clause
        // These functions REQUIRE an OVER clause - they cannot be used as scalar functions
        if Self::is_window_only_function(&name_upper) {
            return Err(ParseError {
                message: format!("misuse of window function {}()", name.to_lowercase()),
            });
        }

        Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::Function {
            name: name_sym,
            args,
            character_unit: None,
        })))
    }

    /// Check if a function is a window-only function that requires an OVER clause.
    /// These functions cannot be used as scalar functions - they MUST have an OVER clause.
    fn is_window_only_function(name: &str) -> bool {
        matches!(
            name,
            // Ranking functions
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" | "PERCENT_RANK" | "CUME_DIST" |
            // Value functions
            "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE"
        )
    }

    /// Validate argument count for window functions.
    fn validate_window_function_args(
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

    /// Classify a window function as aggregate, ranking, or value.
    ///
    /// This mirrors the classification in the regular parser to ensure
    /// consistent behavior between parsing modes.
    fn classify_window_function(
        name_upper: &str,
        name: Symbol,
        args: BumpVec<'arena, Expression<'arena>>,
    ) -> WindowFunctionSpec<'arena> {
        match name_upper {
            // Ranking functions
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" | "PERCENT_RANK" | "CUME_DIST" => {
                WindowFunctionSpec::Ranking { name, args }
            }
            // Value functions
            "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {
                WindowFunctionSpec::Value { name, args }
            }
            // Everything else is an aggregate function
            _ => WindowFunctionSpec::Aggregate { name, args },
        }
    }

    /// Parse window function (OVER clause).
    fn parse_window_function(
        &mut self,
        name: Symbol,
        name_upper: &str,
        args: BumpVec<'arena, Expression<'arena>>,
    ) -> Result<Expression<'arena>, ParseError> {
        self.consume_keyword(Keyword::Over)?;
        self.expect_token(Token::LParen)?;

        // Parse PARTITION BY
        let partition_by = if self.try_consume_keyword(Keyword::Partition) {
            self.consume_keyword(Keyword::By)?;
            Some(self.parse_expression_list()?)
        } else {
            None
        };

        // Parse ORDER BY
        let order_by = if self.try_consume_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::By)?;
            Some(self.parse_order_by_list()?)
        } else {
            None
        };

        // Parse frame clause (simplified)
        let frame = None; // TODO: implement frame parsing

        self.expect_token(Token::RParen)?;

        // Classify the window function based on its name
        let function = Self::classify_window_function(name_upper, name, args);
        let over = WindowSpec { partition_by, order_by, frame };

        Ok(Expression::Extended(self.arena.alloc(ExtendedExpr::WindowFunction { function, over })))
    }

    /// Parse comma-separated expression list.
    pub(crate) fn parse_expression_list(
        &mut self,
    ) -> Result<BumpVec<'arena, Expression<'arena>>, ParseError> {
        let mut exprs = BumpVec::new_in(self.arena);

        if matches!(self.peek(), Token::RParen) {
            return Ok(exprs);
        }

        exprs.push(self.parse_expression()?);

        while self.try_consume(&Token::Comma) {
            exprs.push(self.parse_expression()?);
        }

        Ok(exprs)
    }

    /// Parse ORDER BY list.
    fn parse_order_by_list(&mut self) -> Result<BumpVec<'arena, OrderByItem<'arena>>, ParseError> {
        let mut items = BumpVec::new_in(self.arena);

        loop {
            let expr = self.parse_expression()?;
            let direction = if self.try_consume_keyword(Keyword::Desc) {
                OrderDirection::Desc
            } else {
                self.try_consume_keyword(Keyword::Asc);
                OrderDirection::Asc
            };

            // Parse optional NULLS FIRST/LAST (SQL:2003 extension)
            let nulls_order = if self.try_consume_keyword(Keyword::Nulls) {
                if self.try_consume_keyword(Keyword::First) {
                    Some(NullsOrder::First)
                } else if self.try_consume_keyword(Keyword::Last) {
                    Some(NullsOrder::Last)
                } else {
                    return Err(crate::ParseError {
                        message: format!(
                            "Expected FIRST or LAST after NULLS, found {:?}",
                            self.peek()
                        ),
                    });
                }
            } else {
                None
            };

            items.push(OrderByItem { expr, direction, nulls_order });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(items)
    }

    /// Parse data type (simplified).
    pub(crate) fn parse_data_type(&mut self) -> Result<vibesql_types::DataType, ParseError> {
        // Get type name as uppercase string
        let type_upper = match self.peek() {
            Token::Identifier(type_name) => type_name.to_uppercase(),
            Token::Keyword { keyword: Keyword::Date, .. } => "DATE".to_string(),
            Token::Keyword { keyword: Keyword::Time, .. } => "TIME".to_string(),
            Token::Keyword { keyword: Keyword::Timestamp, .. } => "TIMESTAMP".to_string(),
            Token::Keyword { keyword: Keyword::Interval, .. } => "INTERVAL".to_string(),
            Token::Keyword { keyword: Keyword::Character, .. } => "CHARACTER".to_string(),
            Token::Keyword { keyword: Keyword::Boolean, .. } => "BOOLEAN".to_string(),
            Token::Keyword { keyword: Keyword::Any, .. } => "ANY".to_string(),
            _ => return Err(ParseError { message: self.peek().syntax_error() }),
        };
        self.advance();

        match type_upper.as_str() {
            "INTEGER" | "INT" => Ok(vibesql_types::DataType::Integer),
            "SMALLINT" => Ok(vibesql_types::DataType::Smallint),
            "BIGINT" => Ok(vibesql_types::DataType::Bigint),
            "BOOLEAN" | "BOOL" => Ok(vibesql_types::DataType::Boolean),
            "FLOAT" => {
                // Parse optional precision
                if self.try_consume(&Token::LParen) {
                    if let Token::Number(_) = self.peek() {
                        self.advance();
                    }
                    self.expect_token(Token::RParen)?;
                }
                Ok(vibesql_types::DataType::Float { precision: 53 })
            }
            "REAL" => Ok(vibesql_types::DataType::Real),
            "DOUBLE" => {
                // Check for DOUBLE PRECISION
                if let Token::Identifier(next) = self.peek() {
                    if next.to_uppercase() == "PRECISION" {
                        self.advance();
                    }
                }
                Ok(vibesql_types::DataType::DoublePrecision)
            }
            "VARCHAR" | "CHARACTER" => {
                // Parse optional length
                if self.try_consume(&Token::LParen) {
                    if let Token::Number(n) = self.peek() {
                        let len = n.parse::<usize>().ok();
                        self.advance();
                        self.expect_token(Token::RParen)?;
                        return Ok(vibesql_types::DataType::Varchar { max_length: len });
                    }
                    self.expect_token(Token::RParen)?;
                }
                Ok(vibesql_types::DataType::Varchar { max_length: None })
            }
            "TEXT" => {
                // SQLite allows TEXT(n) for compatibility but ignores the size
                if self.try_consume(&Token::LParen) {
                    // Parse and ignore the size parameter
                    if let Token::Number(_) = self.peek() {
                        self.advance();
                    }
                    self.expect_token(Token::RParen)?;
                }
                Ok(vibesql_types::DataType::Varchar { max_length: None })
            }
            "DATE" => Ok(vibesql_types::DataType::Date),
            "TIME" => Ok(vibesql_types::DataType::Time { with_timezone: false }),
            "TIMESTAMP" => Ok(vibesql_types::DataType::Timestamp { with_timezone: false }),
            "NUMERIC" | "DECIMAL" | "DEC" => {
                // Parse optional precision and scale
                if self.try_consume(&Token::LParen) {
                    let precision = if let Token::Number(n) = self.peek() {
                        let p = n.parse::<u8>().unwrap_or(38);
                        self.advance();
                        p
                    } else {
                        38
                    };
                    let scale = if self.try_consume(&Token::Comma) {
                        if let Token::Number(n) = self.peek() {
                            let s = n.parse::<u8>().unwrap_or(0);
                            self.advance();
                            s
                        } else {
                            0
                        }
                    } else {
                        0
                    };
                    self.expect_token(Token::RParen)?;
                    Ok(vibesql_types::DataType::Numeric { precision, scale })
                } else {
                    Ok(vibesql_types::DataType::Numeric { precision: 38, scale: 0 })
                }
            }
            "UNSIGNED" => Ok(vibesql_types::DataType::Unsigned),
            "SIGNED" => Ok(vibesql_types::DataType::Integer),
            "BLOB" | "ANY" => Ok(vibesql_types::DataType::BinaryLargeObject),
            "CHAR" => {
                // Parse optional length
                let length = if self.try_consume(&Token::LParen) {
                    let len = if let Token::Number(n) = self.peek() {
                        let l = n.parse::<usize>().unwrap_or(1);
                        self.advance();
                        l
                    } else {
                        1
                    };
                    self.expect_token(Token::RParen)?;
                    len
                } else {
                    1
                };
                Ok(vibesql_types::DataType::Character { length })
            }
            _ => {
                // Unknown type - default to varchar
                Ok(vibesql_types::DataType::Varchar { max_length: None })
            }
        }
    }
}
