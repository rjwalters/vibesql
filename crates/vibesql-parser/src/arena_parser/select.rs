//! Arena-allocated SELECT statement parsing.

use std::sync::atomic::{AtomicU64, Ordering};

use bumpalo::collections::Vec as BumpVec;
use vibesql_ast::arena::{
    CommonTableExpr, Expression, FromClause, GroupByClause, GroupingElement, GroupingSet, JoinType,
    NullsOrder, OrderByItem, OrderDirection, SelectItem, SelectStmt, SetOperation, SetOperator,
    Symbol,
};

use super::ArenaParser;
use crate::{keywords::Keyword, token::Token, ParseError};

/// Counter for generating unique derived table aliases when none is provided.
/// SQLite allows derived tables without aliases, unlike SQL:1999 which requires them.
static ARENA_DERIVED_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

impl<'arena> ArenaParser<'arena> {
    /// Parse a SELECT statement.
    pub(crate) fn parse_select_statement(
        &mut self,
    ) -> Result<&'arena SelectStmt<'arena>, ParseError> {
        // Parse optional WITH clause
        let with_clause = if self.try_consume_keyword(Keyword::With) {
            // Check for optional RECURSIVE keyword (SQL:1999, SQLite)
            let recursive = self.try_consume_keyword(Keyword::Recursive);
            Some(self.parse_with_clause(recursive)?)
        } else {
            None
        };

        self.consume_keyword(Keyword::Select)?;

        // Parse DISTINCT
        let distinct = self.try_consume_keyword(Keyword::Distinct);
        self.try_consume_keyword(Keyword::All);

        // Parse select list
        let select_list = self.parse_select_list()?;

        // Parse optional INTO
        let (into_table, into_variables) = self.parse_into_clause()?;

        // Parse FROM
        let from = if self.try_consume_keyword(Keyword::From) {
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        // Parse WHERE
        let where_clause = if self.try_consume_keyword(Keyword::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse GROUP BY
        let group_by = if self.try_consume_keyword(Keyword::Group) {
            self.consume_keyword(Keyword::By)?;
            Some(self.parse_group_by_clause()?)
        } else {
            None
        };

        // Parse HAVING
        let having = if self.try_consume_keyword(Keyword::Having) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse set operations (UNION, INTERSECT, EXCEPT) BEFORE ORDER BY/LIMIT
        // This ensures ORDER BY/LIMIT apply to the entire set operation result
        let set_operation = self.parse_set_operation_internal()?;

        // Parse ORDER BY (only after set operations, so it applies to combined result)
        let order_by = if self.try_consume_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::By)?;
            Some(self.parse_order_by_clause()?)
        } else {
            None
        };

        // Check for ORDER BY appearing before a set operation (SQLite-compatible error)
        // This catches: SELECT ... ORDER BY ... UNION SELECT ...
        if order_by.is_some() {
            let op_name = if self.peek_keyword(Keyword::Union) {
                // Check for UNION ALL vs UNION
                let saved = self.position;
                self.advance(); // consume UNION
                let all = self.try_consume_keyword(Keyword::All);
                self.position = saved; // restore position
                if all {
                    Some("UNION ALL")
                } else {
                    Some("UNION")
                }
            } else if self.peek_keyword(Keyword::Intersect) {
                Some("INTERSECT")
            } else if self.peek_keyword(Keyword::Except) {
                Some("EXCEPT")
            } else {
                None
            };

            if let Some(op) = op_name {
                return Err(ParseError {
                    message: format!("ORDER BY clause should come after {} not before", op),
                });
            }
        }

        // Parse LIMIT (supports comma syntax: LIMIT offset,count)
        let (limit, offset_from_limit) = if self.try_consume_keyword(Keyword::Limit) {
            let first_expr = self.parse_expression()?;

            // Check for comma syntax: LIMIT offset,count
            if self.try_consume(&Token::Comma) {
                let second_expr = self.parse_expression()?;
                // In comma syntax, first is offset, second is count
                (Some(second_expr), Some(first_expr))
            } else {
                (Some(first_expr), None)
            }
        } else {
            (None, None)
        };

        // Parse OFFSET (only if not already set via comma syntax)
        let offset = if offset_from_limit.is_some() {
            offset_from_limit
        } else if self.try_consume_keyword(Keyword::Offset) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Check for LIMIT appearing before a set operation (SQLite-compatible error)
        // This catches: SELECT ... LIMIT ... UNION SELECT ...
        if limit.is_some() || offset.is_some() {
            let op_name = if self.peek_keyword(Keyword::Union) {
                // Check for UNION ALL vs UNION
                let saved = self.position;
                self.advance(); // consume UNION
                let all = self.try_consume_keyword(Keyword::All);
                self.position = saved; // restore position
                if all {
                    Some("UNION ALL")
                } else {
                    Some("UNION")
                }
            } else if self.peek_keyword(Keyword::Intersect) {
                Some("INTERSECT")
            } else if self.peek_keyword(Keyword::Except) {
                Some("EXCEPT")
            } else {
                None
            };

            if let Some(op) = op_name {
                return Err(ParseError {
                    message: format!("LIMIT clause should come after {} not before", op),
                });
            }
        }

        // Issue #4448: Reject ORDER BY after LIMIT/OFFSET
        // SQLite rejects: SELECT f1 FROM test1 LIMIT 5 OFFSET 1 ORDER BY f2
        if (limit.is_some() || offset.is_some()) && self.peek_keyword(Keyword::Order) {
            return Err(ParseError { message: self.peek().syntax_error() });
        }

        // Issue #4448: Validate no unexpected tokens before semicolon/EOF
        // This catches incomplete input like: SELECT f1 FROM test1 AS 'hi', test2 AS
        // and unexpected keywords like: SELECT f1 FROM test1 ORDER BY f1 desc, f2 where
        // Note: RParen is valid because SELECT can appear in subqueries/CTEs
        match self.peek() {
            Token::Semicolon | Token::Eof | Token::RParen => {}
            _ => return Err(ParseError { message: self.peek().syntax_error() }),
        }

        let stmt = SelectStmt {
            with_clause,
            distinct,
            select_list,
            into_table,
            into_variables,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            set_operation,
            values: None,
        };

        Ok(self.arena.alloc(stmt))
    }

    /// Parse a VALUES statement (standalone or as part of a view definition).
    ///
    /// Syntax: VALUES(expr, ...) [, (expr, ...), ...] [ORDER BY] [LIMIT] [OFFSET]
    ///
    /// Used for:
    /// - Standalone VALUES statements: VALUES(1);
    /// - View definitions: CREATE VIEW dual AS VALUES('x');
    /// - CTEs: WITH t AS (VALUES(1)) SELECT * FROM t;
    pub(crate) fn parse_values_statement(
        &mut self,
    ) -> Result<&'arena SelectStmt<'arena>, ParseError> {
        self.consume_keyword(Keyword::Values)?;

        // Parse VALUES rows: (row1), (row2), ...
        let mut rows = BumpVec::new_in(self.arena);
        loop {
            self.expect_token(Token::LParen)?;
            let row = self.parse_expression_list()?;
            self.expect_token(Token::RParen)?;
            rows.push(row);

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        // Parse set operations (UNION, INTERSECT, EXCEPT)
        let set_operation = self.parse_set_operation_internal()?;

        // Parse ORDER BY
        let order_by = if self.try_consume_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::By)?;
            Some(self.parse_order_by_clause()?)
        } else {
            None
        };

        // Parse LIMIT (supports comma syntax: LIMIT offset,count)
        let (limit, offset_from_limit) = if self.try_consume_keyword(Keyword::Limit) {
            let first_expr = self.parse_expression()?;
            if self.try_consume(&Token::Comma) {
                let second_expr = self.parse_expression()?;
                (Some(second_expr), Some(first_expr))
            } else {
                (Some(first_expr), None)
            }
        } else {
            (None, None)
        };

        // Parse OFFSET (only if not already set via comma syntax)
        let offset = if offset_from_limit.is_some() {
            offset_from_limit
        } else if self.try_consume_keyword(Keyword::Offset) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: BumpVec::new_in(self.arena),
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by,
            limit,
            offset,
            set_operation,
            values: Some(rows),
        };

        Ok(self.arena.alloc(stmt))
    }

    /// Parse WITH clause (CTEs).
    ///
    /// If `recursive` is true, all CTEs in this list are marked as recursive.
    /// In SQL:1999/SQLite, the RECURSIVE keyword applies to all CTEs in the WITH clause.
    fn parse_with_clause(
        &mut self,
        recursive: bool,
    ) -> Result<BumpVec<'arena, CommonTableExpr<'arena>>, ParseError> {
        let mut ctes = BumpVec::new_in(self.arena);

        loop {
            let cte = self.parse_cte(recursive)?;
            ctes.push(cte);

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(ctes)
    }

    /// Parse a single CTE.
    ///
    /// The `recursive` parameter indicates whether this CTE was declared in a
    /// WITH RECURSIVE clause. In SQL:1999/SQLite, RECURSIVE applies to all CTEs
    /// in the WITH clause, even if they don't actually recurse.
    fn parse_cte(&mut self, recursive: bool) -> Result<CommonTableExpr<'arena>, ParseError> {
        // Parse CTE name
        let name = match self.peek() {
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                self.intern(&name)
            }
            // Allow unreserved keywords (like NULLS, TIMESTAMP, etc.) and SQLite
            // fallback keywords (like ROWS) as CTE names.
            Token::Keyword { keyword: kw, .. }
                if kw.can_be_identifier() || kw.is_sqlite_fallback_keyword() =>
            {
                let name = format!("{}", kw).to_lowercase();
                self.advance();
                self.intern(&name)
            }
            _ => return Err(ParseError { message: "Expected CTE name".to_string() }),
        };

        // Parse optional column list
        let columns = if self.try_consume(&Token::LParen) {
            let mut cols = BumpVec::new_in(self.arena);
            while let Token::Identifier(col) = self.peek() {
                let col = col.clone();
                self.advance();
                cols.push(self.intern(&col));
                if !self.try_consume(&Token::Comma) {
                    break;
                }
            }
            self.expect_token(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        self.consume_keyword(Keyword::As)?;

        // Parse optional materialization hint: MATERIALIZED or NOT MATERIALIZED
        let materialization = if self.try_consume_keyword(Keyword::Not) {
            self.consume_keyword(Keyword::Materialized)?;
            vibesql_ast::CteMaterialization::NotMaterialized
        } else if self.try_consume_keyword(Keyword::Materialized) {
            vibesql_ast::CteMaterialization::Materialized
        } else {
            vibesql_ast::CteMaterialization::Default
        };

        self.expect_token(Token::LParen)?;
        let query = self.parse_select_statement()?;
        self.expect_token(Token::RParen)?;

        Ok(CommonTableExpr { name, columns, query, recursive, materialization })
    }

    /// Parse select list.
    /// Also used for RETURNING clauses on DML statements (same item grammar).
    pub(crate) fn parse_select_list(
        &mut self,
    ) -> Result<BumpVec<'arena, SelectItem<'arena>>, ParseError> {
        let mut items = BumpVec::new_in(self.arena);

        loop {
            let item = self.parse_select_item()?;
            items.push(item);

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(items)
    }

    /// Parse a single select item.
    fn parse_select_item(&mut self) -> Result<SelectItem<'arena>, ParseError> {
        // Check for * wildcard
        if matches!(self.peek(), Token::Symbol('*')) {
            self.advance();
            return Ok(SelectItem::Wildcard { alias: None });
        }

        // Check for qualified wildcard (table.* or alias.*)
        // Must check BEFORE parsing as expression to avoid losing the qualifier.
        // The expression parser would parse "table.*" as Expression::Wildcard, losing the table
        // name.
        let saved_position = self.position;
        let qualifier = match self.peek() {
            Token::Identifier(qualifier) | Token::DelimitedIdentifier(qualifier) => {
                Some(qualifier.clone())
            }
            // Allow contextual keywords (M, YEAR, WINDOW, ...) as wildcard qualifiers,
            // matching the keyword-as-identifier handling in expression parsing. Without
            // this, `m.*` would fall through to parse_expression and silently degrade to a
            // bare Wildcard, losing the qualifier.
            // SQL:1999 normalizes unquoted identifiers to lowercase.
            Token::Keyword { keyword: kw, .. } if kw.can_be_identifier() => {
                Some(format!("{}", kw).to_lowercase())
            }
            _ => None,
        };
        if let Some(qualifier) = qualifier {
            self.advance();

            if matches!(self.peek(), Token::Symbol('.')) {
                self.advance(); // consume dot

                if matches!(self.peek(), Token::Symbol('*')) {
                    self.advance(); // consume asterisk
                    let qualifier_sym = self.intern(&qualifier);
                    return Ok(SelectItem::QualifiedWildcard {
                        qualifier: qualifier_sym,
                        alias: None,
                    });
                }
            }

            // Not a qualified wildcard, backtrack
            self.position = saved_position;
        }

        // Record start position before parsing expression
        let start_pos = self.position;

        // Parse expression
        let expr = self.parse_expression()?;

        // Record end position after parsing expression
        let end_pos = self.position;

        // Reconstruct source text from tokens consumed during expression parsing.
        // This preserves the original identifier case and operator style (e.g., `f1+F2`
        // instead of `(F1 + F2)`) for use as column names when no alias is provided.
        let source_text = self.reconstruct_source_text(start_pos, end_pos);

        // Check for qualified wildcard (table.*)
        // Note: We compare the symbol directly now, which means we need to intern "*" for
        // comparison But actually, the Wildcard expression is separate - column reference
        // with "*" becomes Expression::Wildcard So this check shouldn't match anymore since
        // Expression::ColumnRef won't have "*" as column. The wildcard case is handled in
        // expression parsing.
        if let Expression::ColumnRef { schema: _, table: Some(t), column: _, .. } = &expr {
            // Check if it's a qualified wildcard - but this won't happen since
            // we parse table.* as Wildcard expression, not as ColumnRef with "*" column
            // Keep this code path but it likely won't be hit
            let _ = t; // silence warning
        }

        // Check for alias - supports identifiers, delimited identifiers, keywords, and
        // single-quoted strings (SQLite compatibility: SELECT 1 AS 'a')
        let alias = if self.try_consume_keyword(Keyword::As) {
            Some(self.parse_alias_name_symbol()?)
        } else if let Token::Identifier(name) = self.peek() {
            // Implicit alias (no AS keyword)
            let name = name.clone();
            self.advance();
            Some(self.intern(&name))
        } else {
            None
        };

        Ok(SelectItem::Expression { expr, alias, source_text })
    }

    /// Parse INTO clause.
    fn parse_into_clause(
        &mut self,
    ) -> Result<(Option<Symbol>, Option<BumpVec<'arena, Symbol>>), ParseError> {
        if !self.try_consume_keyword(Keyword::Into) {
            return Ok((None, None));
        }

        // Check if it's SELECT INTO table_name or SELECT INTO @var1, @var2
        if let Token::Identifier(name) = self.peek() {
            let name = name.clone();
            self.advance();
            let name_sym = self.intern(&name);
            return Ok((Some(name_sym), None));
        }

        // Parse variable list
        let mut vars = BumpVec::new_in(self.arena);
        while let Token::Identifier(var) = self.peek() {
            let var = var.clone();
            self.advance();
            vars.push(self.intern(&var));
            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        if vars.is_empty() {
            return Err(ParseError {
                message: "Expected table name or variables after INTO".to_string(),
            });
        }

        Ok((None, Some(vars)))
    }

    /// Parse FROM clause.
    pub(crate) fn parse_from_clause(&mut self) -> Result<FromClause<'arena>, ParseError> {
        let mut from = self.parse_table_reference()?;

        // Parse JOINs
        loop {
            let join_type = if self.try_consume_keyword(Keyword::Inner) {
                self.consume_keyword(Keyword::Join)?;
                Some(JoinType::Inner)
            } else if self.try_consume_keyword(Keyword::Left) {
                self.try_consume_keyword(Keyword::Outer);
                self.consume_keyword(Keyword::Join)?;
                Some(JoinType::LeftOuter)
            } else if self.try_consume_keyword(Keyword::Right) {
                self.try_consume_keyword(Keyword::Outer);
                self.consume_keyword(Keyword::Join)?;
                Some(JoinType::RightOuter)
            } else if self.try_consume_keyword(Keyword::Full) {
                self.try_consume_keyword(Keyword::Outer);
                self.consume_keyword(Keyword::Join)?;
                Some(JoinType::FullOuter)
            } else if self.try_consume_keyword(Keyword::Cross) {
                self.consume_keyword(Keyword::Join)?;
                Some(JoinType::Cross)
            } else if self.try_consume_keyword(Keyword::Join) {
                Some(JoinType::Inner)
            } else if self.try_consume(&Token::Comma) {
                // Comma normally represents CROSS JOIN, but we need to check for
                // SQLite's legacy syntax: FROM t1, t2 ON condition (treated as INNER JOIN)
                Some(JoinType::Cross)
            } else {
                None
            };

            // Track if this was originally a comma-join (for legacy ON support)
            let was_comma_join = matches!(join_type, Some(JoinType::Cross))
                && !matches!(self.peek(), Token::Keyword { keyword: Keyword::Natural, .. });

            if let Some(mut jt) = join_type {
                let natural = self.try_consume_keyword(Keyword::Natural);
                let right = self.parse_table_reference()?;

                // For comma-joins, check for legacy ON clause (SQLite compatibility)
                // If found, treat as INNER JOIN instead of CROSS JOIN
                let (condition, using_columns) = if was_comma_join && !natural {
                    if self.try_consume_keyword(Keyword::On) {
                        jt = JoinType::Inner; // Convert to INNER JOIN
                        (Some(self.parse_expression()?), None)
                    } else {
                        (None, None)
                    }
                } else if jt != JoinType::Cross && !natural {
                    if self.try_consume_keyword(Keyword::On) {
                        (Some(self.parse_expression()?), None)
                    } else if self.try_consume_keyword(Keyword::Using) {
                        self.expect_token(Token::LParen)?;
                        let mut columns = BumpVec::new_in(self.arena);
                        loop {
                            let name = match self.peek() {
                                Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                                    let n = name.clone();
                                    self.advance();
                                    n
                                }
                                // Allow unreserved keywords as column names
                                Token::Keyword { keyword: kw, .. } if kw.can_be_identifier() => {
                                    let n = format!("{}", kw).to_lowercase();
                                    self.advance();
                                    n
                                }
                                _ => {
                                    return Err(ParseError {
                                        message: "Expected column name in USING clause".to_string(),
                                    });
                                }
                            };
                            columns.push(self.intern(&name));
                            if !self.try_consume(&Token::Comma) {
                                break;
                            }
                        }
                        self.expect_token(Token::RParen)?;
                        (None, Some(columns))
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

                let left_ref = self.arena.alloc(from);
                let right_ref = self.arena.alloc(right);

                from = FromClause::Join {
                    left: left_ref,
                    right: right_ref,
                    join_type: jt,
                    condition,
                    using_columns,
                    natural,
                    alias: None,
                };
            } else {
                break;
            }
        }

        Ok(from)
    }

    /// Parse a table reference.
    fn parse_table_reference(&mut self) -> Result<FromClause<'arena>, ParseError> {
        // Check for subquery
        if self.try_consume(&Token::LParen) {
            let query = self.parse_select_statement()?;
            self.expect_token(Token::RParen)?;

            // Parse optional alias - SQLite allows derived tables without aliases
            // Issue #4448: If AS is present, an alias MUST follow
            let has_as = self.try_consume_keyword(Keyword::As);
            let alias = if let Token::Identifier(name) = self.peek() {
                let name = name.clone();
                self.advance();
                self.intern(&name)
            } else if has_as {
                // AS was present but no identifier follows - call parse_alias_name
                // to get proper alias handling (keywords, strings, etc.) or error
                self.parse_alias_name_symbol()?
            } else {
                // Auto-generate unique alias for SQLite compatibility
                let generated = format!(
                    "(subquery-{})",
                    ARENA_DERIVED_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
                );
                self.intern(&generated)
            };

            // Parse optional column aliases: (col1, col2, ...)
            let column_aliases = self.parse_column_alias_list()?;

            return Ok(FromClause::Subquery { query, alias, column_aliases });
        }

        // Table-valued function in FROM position (JSON1 TVFs: json_each,
        // json_tree). An unquoted identifier immediately followed by `(` is a TVF
        // call only if the name is allow-listed; any other `ident(` in FROM remains
        // a parse error (preserving prior behavior). Mirrors the standard parser.
        if let Token::Identifier(name) = self.peek() {
            if matches!(self.peek_next(), Token::LParen)
                && crate::table_functions::is_table_valued_function(name)
            {
                return self.parse_table_function();
            }
        }

        // Regular table reference - check for both regular and delimited identifiers
        let (name, quoted) = match self.peek() {
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                (self.intern(&name), false)
            }
            Token::DelimitedIdentifier(name) => {
                let name = name.clone();
                self.advance();
                (self.intern(&name), true)
            }
            _ => return Err(ParseError { message: self.peek().syntax_error() }),
        };

        // Check for alias
        // Issue #4448: If AS is present, an alias MUST follow
        let has_as = self.try_consume_keyword(Keyword::As);
        let alias = if self.peek_index_hint() && !has_as {
            // SQLite index hint (INDEXED BY x / NOT INDEXED), not an alias
            None
        } else if let Token::Identifier(alias) = self.peek() {
            // Make sure it's not a keyword that would start a new clause
            if !matches!(
                self.peek(),
                Token::Keyword { keyword: Keyword::Where, .. }
                    | Token::Keyword { keyword: Keyword::Join, .. }
                    | Token::Keyword { keyword: Keyword::Inner, .. }
                    | Token::Keyword { keyword: Keyword::Left, .. }
                    | Token::Keyword { keyword: Keyword::Right, .. }
                    | Token::Keyword { keyword: Keyword::Full, .. }
                    | Token::Keyword { keyword: Keyword::Cross, .. }
                    | Token::Keyword { keyword: Keyword::On, .. }
                    | Token::Keyword { keyword: Keyword::Group, .. }
                    | Token::Keyword { keyword: Keyword::Order, .. }
                    | Token::Keyword { keyword: Keyword::Having, .. }
                    | Token::Keyword { keyword: Keyword::Limit, .. }
                    | Token::Keyword { keyword: Keyword::Union, .. }
                    | Token::Keyword { keyword: Keyword::Intersect, .. }
                    | Token::Keyword { keyword: Keyword::Except, .. }
            ) {
                let alias = alias.clone();
                self.advance();
                Some(self.intern(&alias))
            } else if has_as {
                // AS was present but followed by a clause keyword - error
                return Err(ParseError { message: self.peek().syntax_error() });
            } else {
                None
            }
        } else if has_as {
            // AS was present but no valid alias follows - must call parse_alias_name
            // to get proper alias handling (keywords, strings, etc.) or error
            Some(self.parse_alias_name_symbol()?)
        } else {
            None
        };

        // Parse optional column aliases: (col1, col2, ...)
        let column_aliases = if alias.is_some() { self.parse_column_alias_list()? } else { None };

        // Parse SQLite index hints: INDEXED BY <name> / NOT INDEXED
        let index_hint = self.parse_index_hint()?;

        Ok(FromClause::Table { name, alias, column_aliases, quoted, index_hint })
    }

    /// Parse a table-valued function reference in FROM position, e.g.
    /// `json_each('[1,2,3]')`, `json_tree(x, '$.a')`, or
    /// `json_each(x) AS je(k, v)`.
    ///
    /// The caller has already verified (via lookahead) that the current token is
    /// an allow-listed identifier immediately followed by `(`. This mirrors the
    /// standard parser's `parse_table_function`, producing
    /// [`vibesql_ast::arena::FromClause::TableFunction`] with the name normalized
    /// to lowercase. No executor support exists yet.
    fn parse_table_function(&mut self) -> Result<FromClause<'arena>, ParseError> {
        // Consume the function name (already known to be an allow-listed identifier).
        let name = match self.peek() {
            Token::Identifier(raw) => {
                let raw = raw.clone();
                let normalized = crate::table_functions::normalized_table_valued_function(&raw)
                    .unwrap_or_else(|| raw.to_lowercase());
                self.advance();
                self.intern(&normalized)
            }
            // Unreachable in practice: caller only dispatches here after confirming
            // an allow-listed identifier. Guard defensively anyway.
            _ => return Err(ParseError { message: self.peek().syntax_error() }),
        };

        // Parse the parenthesized, comma-separated argument list.
        self.expect_token(Token::LParen)?;
        let args = self.parse_expression_list()?;
        self.expect_token(Token::RParen)?;

        // Optional alias: `AS je` or bare `je`.
        let has_as = self.try_consume_keyword(Keyword::As);
        let alias = if let Token::Identifier(a) = self.peek() {
            let a = a.clone();
            self.advance();
            Some(self.intern(&a))
        } else if has_as {
            // AS was present but no plain identifier follows - use the shared
            // alias helper to handle keywords / quoted strings, or error.
            Some(self.parse_alias_name_symbol()?)
        } else {
            None
        };

        // Optional column-alias list: `AS je(k, v)`. Only meaningful with an alias.
        let column_aliases = if alias.is_some() { self.parse_column_alias_list()? } else { None };

        Ok(FromClause::TableFunction { name, args, alias, column_aliases })
    }

    /// Check if the upcoming tokens form a SQLite index hint:
    /// `INDEXED BY <name>` or `NOT INDEXED`.
    ///
    /// Note: INDEXED is not a keyword in our lexer, so it arrives as an
    /// identifier token (lowercased by the lexer).
    fn peek_index_hint(&self) -> bool {
        match self.peek() {
            Token::Identifier(id) if id.eq_ignore_ascii_case("indexed") => {
                self.peek_next_keyword(Keyword::By)
            }
            Token::Keyword { keyword: Keyword::Not, .. } => {
                matches!(self.peek_next(), Token::Identifier(id) if id.eq_ignore_ascii_case("indexed"))
            }
            _ => false,
        }
    }

    /// Parse SQLite index hints: `INDEXED BY <index-name>` or `NOT INDEXED`.
    ///
    /// The hint is carried through the AST so the executor can validate that
    /// an `INDEXED BY` index exists on the table (SQLite: `no such index: X`).
    /// VibeSQL's planner chooses indexes independently, so the hint has no
    /// effect on planning.
    fn parse_index_hint(&mut self) -> Result<Option<vibesql_ast::arena::IndexHint>, ParseError> {
        if !self.peek_index_hint() {
            return Ok(None);
        }
        if self.peek_keyword(Keyword::Not) {
            self.advance(); // consume NOT
            self.advance(); // consume INDEXED
            return Ok(Some(vibesql_ast::arena::IndexHint::NotIndexed));
        }
        self.advance(); // consume INDEXED
        self.advance(); // consume BY
        match self.peek() {
            Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                let name = name.clone();
                self.advance();
                Ok(Some(vibesql_ast::arena::IndexHint::IndexedBy(self.intern(&name))))
            }
            // SQLite fallback keywords are legal index names here too
            // (keyword1.test: `SELECT b FROM t1 INDEXED BY abort WHERE a=2`).
            // Lowercased like any unquoted identifier so catalog lookup matches
            // the name stored by `CREATE INDEX abort ...`.
            Token::Keyword { keyword: kw, .. }
                if kw.can_be_identifier() || kw.is_sqlite_fallback_keyword() =>
            {
                let name = kw.to_string().to_lowercase();
                self.advance();
                Ok(Some(vibesql_ast::arena::IndexHint::IndexedBy(self.intern(&name))))
            }
            _ => Err(ParseError { message: "Expected index name after INDEXED BY".to_string() }),
        }
    }

    /// Parse GROUP BY clause.
    fn parse_group_by_clause(&mut self) -> Result<GroupByClause<'arena>, ParseError> {
        // Check for ROLLUP, CUBE, GROUPING SETS
        if self.peek_keyword(Keyword::Rollup) {
            self.advance();
            self.expect_token(Token::LParen)?;
            let elements = self.parse_grouping_elements()?;
            self.expect_token(Token::RParen)?;
            return Ok(GroupByClause::Rollup(elements));
        }

        if self.peek_keyword(Keyword::Cube) {
            self.advance();
            self.expect_token(Token::LParen)?;
            let elements = self.parse_grouping_elements()?;
            self.expect_token(Token::RParen)?;
            return Ok(GroupByClause::Cube(elements));
        }

        if self.peek_keyword(Keyword::Grouping) {
            self.advance();
            self.consume_keyword(Keyword::Sets)?;
            self.expect_token(Token::LParen)?;
            let sets = self.parse_grouping_sets()?;
            self.expect_token(Token::RParen)?;
            return Ok(GroupByClause::GroupingSets(sets));
        }

        // Simple GROUP BY
        let mut exprs = BumpVec::new_in(self.arena);
        loop {
            exprs.push(self.parse_expression()?);
            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(GroupByClause::Simple(exprs))
    }

    /// Parse grouping elements.
    fn parse_grouping_elements(
        &mut self,
    ) -> Result<BumpVec<'arena, GroupingElement<'arena>>, ParseError> {
        let mut elements = BumpVec::new_in(self.arena);

        loop {
            if self.try_consume(&Token::LParen) {
                // Composite element
                let mut exprs = BumpVec::new_in(self.arena);
                loop {
                    exprs.push(self.parse_expression()?);
                    if !self.try_consume(&Token::Comma) {
                        break;
                    }
                }
                self.expect_token(Token::RParen)?;
                elements.push(GroupingElement::Composite(exprs));
            } else {
                // Single element
                let expr = self.parse_expression()?;
                elements.push(GroupingElement::Single(expr));
            }

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(elements)
    }

    /// Parse grouping sets.
    fn parse_grouping_sets(&mut self) -> Result<BumpVec<'arena, GroupingSet<'arena>>, ParseError> {
        let mut sets = BumpVec::new_in(self.arena);

        loop {
            self.expect_token(Token::LParen)?;
            let mut columns = BumpVec::new_in(self.arena);

            if !matches!(self.peek(), Token::RParen) {
                loop {
                    columns.push(self.parse_expression()?);
                    if !self.try_consume(&Token::Comma) {
                        break;
                    }
                }
            }

            self.expect_token(Token::RParen)?;
            sets.push(GroupingSet { columns });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(sets)
    }

    /// Parse ORDER BY clause.
    fn parse_order_by_clause(
        &mut self,
    ) -> Result<BumpVec<'arena, OrderByItem<'arena>>, ParseError> {
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

    /// Parse set operation (UNION/INTERSECT/EXCEPT) - internal implementation.
    /// The right-hand side is parsed without ORDER BY/LIMIT since those apply to
    /// the combined result, not individual SELECT statements.
    fn parse_set_operation_internal(&mut self) -> Result<Option<SetOperation<'arena>>, ParseError> {
        let op = if self.try_consume_keyword(Keyword::Union) {
            SetOperator::Union
        } else if self.try_consume_keyword(Keyword::Intersect) {
            SetOperator::Intersect
        } else if self.try_consume_keyword(Keyword::Except) {
            SetOperator::Except
        } else {
            return Ok(None);
        };

        let all = self.try_consume_keyword(Keyword::All);
        self.try_consume_keyword(Keyword::Distinct);

        // Parse the right side without ORDER BY/LIMIT - those apply to the combined result
        let right = self.parse_select_for_set_operation()?;

        Ok(Some(SetOperation { op, all, right }))
    }

    /// Parse a SELECT statement for the right side of a set operation.
    /// This does NOT parse ORDER BY/LIMIT since those should apply to the combined result.
    fn parse_select_for_set_operation(&mut self) -> Result<&'arena SelectStmt<'arena>, ParseError> {
        // Handle parenthesized subqueries
        if self.try_consume(&Token::LParen) {
            let stmt = self.parse_select_statement()?;
            if !self.try_consume(&Token::RParen) {
                return Err(ParseError {
                    message: "Expected ')' after parenthesized SELECT in set operation".to_string(),
                });
            }
            return Ok(stmt);
        }

        self.consume_keyword(Keyword::Select)?;

        // Parse DISTINCT
        let distinct = self.try_consume_keyword(Keyword::Distinct);
        self.try_consume_keyword(Keyword::All);

        // Parse select list
        let select_list = self.parse_select_list()?;

        // Parse optional INTO
        let (into_table, into_variables) = self.parse_into_clause()?;

        // Parse FROM
        let from = if self.try_consume_keyword(Keyword::From) {
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        // Parse WHERE
        let where_clause = if self.try_consume_keyword(Keyword::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse GROUP BY
        let group_by = if self.try_consume_keyword(Keyword::Group) {
            self.consume_keyword(Keyword::By)?;
            Some(self.parse_group_by_clause()?)
        } else {
            None
        };

        // Parse HAVING
        let having = if self.try_consume_keyword(Keyword::Having) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse nested set operations (for chains like A UNION B UNION C)
        // But NOT ORDER BY/LIMIT - those are parsed by the outer parse_select_statement
        let set_operation = self.parse_set_operation_internal()?;

        // Note: ORDER BY, LIMIT, OFFSET are NOT parsed here
        // They apply to the combined result and are handled by the outer statement

        let stmt = SelectStmt {
            with_clause: None, // CTEs are on the outer statement
            distinct,
            select_list,
            into_table,
            into_variables,
            from,
            where_clause,
            group_by,
            having,
            order_by: None, // ORDER BY applies to combined result
            limit: None,    // LIMIT applies to combined result
            offset: None,   // OFFSET applies to combined result
            set_operation,
            values: None,
        };

        Ok(self.arena.alloc(stmt))
    }
}
