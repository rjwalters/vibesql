use std::sync::atomic::{AtomicU64, Ordering};

use super::*;

/// Counter for generating unique derived table aliases when none is provided.
/// SQLite allows derived tables without aliases, unlike SQL:1999 which requires them.
static DERIVED_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

impl Parser {
    /// Parse FROM clause
    pub(crate) fn parse_from_clause(&mut self) -> Result<vibesql_ast::FromClause, ParseError> {
        // Parse the first table reference
        let mut left = self.parse_table_reference()?;

        // Check for a bare ON / USING with no preceding JOIN - SQLite error
        // compatibility (tkt3935). A join constraint may only appear after a
        // join operator (a JOIN keyword or a legacy comma-join), so when one
        // directly follows the first table term SQLite emits a descriptive
        // error rather than a bare token syntax error.
        if self.peek_keyword(Keyword::On) {
            // Consume "ON <expr>" first: SQLite parses the ON constraint before
            // deciding what went wrong. If a USING clause then follows the ON
            // expression (`... ON b USING(a)`), that trailing USING is a
            // token-level syntax error (`near "USING": syntax error`); a bare
            // `... ON b` instead reports the missing-JOIN diagnostic.
            self.consume_keyword(Keyword::On)?;
            let _ = self.parse_expression()?;
            if self.peek_keyword(Keyword::Using) {
                return Err(ParseError { message: self.peek().syntax_error() });
            }
            return Err(ParseError { message: "a JOIN clause is required before ON".to_string() });
        }
        if self.peek_keyword(Keyword::Using) {
            return Err(ParseError {
                message: "a JOIN clause is required before USING".to_string(),
            });
        }

        // Check for JOINs or commas (left-associative)
        while self.is_join_keyword() || self.peek() == &Token::Comma {
            let (join_type, right, condition, using_columns, natural) =
                if self.peek() == &Token::Comma {
                    // Comma normally represents CROSS JOIN, but SQLite's legacy syntax
                    // allows "FROM t1, t2 ON condition" which behaves like INNER JOIN
                    self.advance(); // Consume comma
                    let right = self.parse_table_reference()?;

                    // Check for a legacy ON or USING clause after a comma-join.
                    // SQLite treats "," exactly like CROSS/INNER JOIN, so the
                    // ON/USING filtering clauses are legal after it and turn the
                    // cartesian product into an inner join:
                    //   FROM t1, t2 ON t1.a=t2.b
                    //   FROM t1, t2 USING (a)
                    // (e_select-0.1.2 / 1.4 / 1.5 / 1.6 / 1.7 comma variants).
                    if self.peek_keyword(Keyword::On) {
                        self.consume_keyword(Keyword::On)?;
                        let condition = self.parse_expression()?;
                        (vibesql_ast::JoinType::Inner, right, Some(condition), None, false)
                    } else if self.peek_keyword(Keyword::Using) {
                        self.consume_keyword(Keyword::Using)?;
                        self.expect_token(Token::LParen)?;
                        let columns =
                            self.parse_comma_separated_list(|p| p.parse_column_name())?;
                        self.expect_token(Token::RParen)?;
                        (vibesql_ast::JoinType::Inner, right, None, Some(columns), false)
                    } else {
                        (vibesql_ast::JoinType::Cross, right, None, None, false)
                    }
                } else {
                    let (join_type, natural) = self.parse_join_type()?;

                    // Parse right table reference
                    let right = self.parse_table_reference()?;

                    // Parse ON condition or USING clause (comes after table reference)
                    // NATURAL JOIN should not have an ON or USING clause
                    let (condition, using_columns) = if self.peek_keyword(Keyword::On) {
                        if natural {
                            return Err(ParseError {
                                message: "a NATURAL join may not have an ON or USING clause"
                                    .to_string(),
                            });
                        }
                        self.consume_keyword(Keyword::On)?;
                        (Some(self.parse_expression()?), None)
                    } else if self.peek_keyword(Keyword::Using) {
                        if natural {
                            return Err(ParseError {
                                message: "a NATURAL join may not have an ON or USING clause"
                                    .to_string(),
                            });
                        }
                        self.consume_keyword(Keyword::Using)?;
                        self.expect_token(Token::LParen)?;
                        let columns = self.parse_comma_separated_list(|p| p.parse_column_name())?;
                        self.expect_token(Token::RParen)?;
                        (None, Some(columns))
                    } else {
                        (None, None)
                    };
                    (join_type, right, condition, using_columns, natural)
                };

            // Build JOIN node
            left = vibesql_ast::FromClause::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                condition,
                using_columns,
                natural,
                alias: None,
            };
        }

        Ok(left)
    }

    /// Parse VALUES clause rows: VALUES(1,2), (3,4), ...
    /// Returns Vec<Vec<Expression>> where each inner vec is a row
    pub(crate) fn parse_values_rows(
        &mut self,
    ) -> Result<Vec<Vec<vibesql_ast::Expression>>, ParseError> {
        self.expect_keyword(Keyword::Values)?;
        let mut rows = Vec::new();
        loop {
            self.expect_token(Token::LParen)?;
            let row = self.parse_comma_separated_list(|p| p.parse_expression())?;
            self.expect_token(Token::RParen)?;
            rows.push(row);

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(rows)
    }

    /// Parse a single table reference (table name, subquery, or derived table with alias)
    pub(crate) fn parse_table_reference(&mut self) -> Result<vibesql_ast::FromClause, ParseError> {
        match self.peek() {
            Token::LParen => {
                // Parenthesized expression: could be a subquery, VALUES, or a JOIN expression
                self.advance(); // Consume '('

                // Check if this is a subquery (starts with SELECT or WITH), VALUES, or a table
                // reference/JOIN
                let result =
                    if self.peek_keyword(Keyword::Select) || self.peek_keyword(Keyword::With) {
                        // Parse the SELECT statement (subquery, possibly with CTE)
                        let query = Box::new(self.parse_select_statement()?);

                        // Expect closing ')'
                        match self.peek() {
                            Token::RParen => {
                                self.advance();
                            }
                            _ => {
                                return Err(ParseError {
                                    message: "Expected ')' after subquery".to_string(),
                                })
                            }
                        }

                        // Support both SQL:1999 (AS required) and MySQL (AS optional) modes
                        // Parse optional AS keyword
                        if self.peek_keyword(Keyword::As) {
                            self.consume_keyword(Keyword::As)?;
                        }

                        // Parse alias - keywords allowed as aliases (except clause keywords)
                        // SQLite allows derived tables without aliases; auto-generate if not provided
                        // SQLite also allows single-quoted strings as aliases (non-standard but common)
                        let alias = match self.peek() {
                            Token::Identifier(id)
                            | Token::DelimitedIdentifier(id)
                            | Token::String(id) => {
                                let alias = id.clone();
                                self.advance();
                                alias
                            }
                            Token::Keyword { keyword: kw, .. } => {
                                // Only consume keyword if it looks like it could be an alias
                                // (not a SQL keyword that starts a new clause)
                                if !matches!(
                                    kw,
                                    Keyword::Where
                                        | Keyword::Order
                                        | Keyword::Limit
                                        | Keyword::Offset
                                        | Keyword::Group
                                        | Keyword::Having
                                        | Keyword::Union
                                        | Keyword::Intersect
                                        | Keyword::Except
                                        | Keyword::Join
                                        | Keyword::Inner
                                        | Keyword::Left
                                        | Keyword::Right
                                        | Keyword::Full
                                        | Keyword::Cross
                                        | Keyword::Natural
                                        | Keyword::On
                                        | Keyword::Using
                                ) {
                                    let alias = kw.to_string();
                                    self.advance();
                                    alias
                                } else {
                                    // Generate default alias for clause keywords
                                    format!(
                                        "(subquery-{})",
                                        DERIVED_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
                                    )
                                }
                            }
                            _ => {
                                // Auto-generate unique alias for SQLite compatibility
                                format!(
                                    "(subquery-{})",
                                    DERIVED_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed)
                                )
                            }
                        };

                        // Parse optional column aliases: AS alias (col1, col2, ...)
                        // SQL:1999 Feature E051-09
                        let column_aliases = self.parse_column_alias_list()?;

                        vibesql_ast::FromClause::Subquery { query, alias, column_aliases }
                    } else if self.peek_keyword(Keyword::Values) {
                        // Parse VALUES clause as table constructor
                        // Example: (VALUES(1,'a'), (2,'b')) AS t(x, y)
                        let rows = self.parse_values_rows()?;

                        // Expect closing ')'
                        match self.peek() {
                            Token::RParen => {
                                self.advance();
                            }
                            _ => {
                                return Err(ParseError {
                                    message: "Expected ')' after VALUES clause".to_string(),
                                })
                            }
                        }

                        // Parse optional AS keyword
                        if self.peek_keyword(Keyword::As) {
                            self.consume_keyword(Keyword::As)?;
                        }

                        // Parse alias (optional for VALUES tables) - keywords allowed as aliases
                        // If no alias is provided, generate a default one
                        // SQLite also allows single-quoted strings as aliases (non-standard but common)
                        let alias = match self.peek() {
                            Token::Identifier(id)
                            | Token::DelimitedIdentifier(id)
                            | Token::String(id) => {
                                let alias = id.clone();
                                self.advance();
                                alias
                            }
                            Token::Keyword { keyword: kw, .. } => {
                                // Only consume keyword if it looks like it could be an alias
                                // (not a SQL keyword that starts a new clause)
                                if !matches!(
                                    kw,
                                    Keyword::Where
                                        | Keyword::Order
                                        | Keyword::Limit
                                        | Keyword::Offset
                                        | Keyword::Group
                                        | Keyword::Having
                                        | Keyword::Union
                                        | Keyword::Intersect
                                        | Keyword::Except
                                        | Keyword::Join
                                        | Keyword::Inner
                                        | Keyword::Left
                                        | Keyword::Right
                                        | Keyword::Full
                                        | Keyword::Cross
                                        | Keyword::Natural
                                        | Keyword::On
                                        | Keyword::Using
                                ) {
                                    let alias = kw.to_string();
                                    self.advance();
                                    alias
                                } else {
                                    // Generate default alias when no explicit alias provided
                                    "_values_".to_string()
                                }
                            }
                            _ => {
                                // Generate default alias when no explicit alias provided
                                "_values_".to_string()
                            }
                        };

                        // Parse optional column aliases: AS alias (col1, col2, ...)
                        let column_aliases = self.parse_column_alias_list()?;

                        vibesql_ast::FromClause::Values { rows, alias, column_aliases }
                    } else {
                        // Parenthesized table reference or JOIN expression
                        // Parse as a FROM clause (which handles JOINs)
                        let mut from_clause = self.parse_from_clause()?;

                        // Expect closing ')'
                        match self.peek() {
                            Token::RParen => {
                                self.advance();
                            }
                            _ => {
                                return Err(ParseError {
                                    message: "Expected ')' after parenthesized table reference"
                                        .to_string(),
                                })
                            }
                        }

                        // Parse optional alias for a parenthesized FROM term.
                        // This covers not only parenthesized JOIN expressions
                        // (`FROM t1 JOIN (t2 JOIN t3 USING(id)) AS j1 ON ...`) but
                        // also a *single* parenthesized term reached via this
                        // fallback branch — a plain table (`FROM (t1) AS xyz`) or a
                        // table-valued function (`FROM (json_each(...)) AS xyz`).
                        // The alias must be reattached to whichever variant the
                        // inner content parsed into; previously only `Join` was
                        // handled, so `(t1) AS xyz` / `(json_each(...)) AS xyz`
                        // silently dropped the alias and later failed to resolve
                        // `xyz.*` (issue #6051).
                        if self.peek_keyword(Keyword::As) {
                            self.consume_keyword(Keyword::As)?;
                            // Parse alias - must be an identifier or keyword usable as identifier
                            let alias = self.parse_alias_name()?;
                            match &mut from_clause {
                                vibesql_ast::FromClause::Join { alias: a, .. } => {
                                    *a = Some(alias);
                                }
                                vibesql_ast::FromClause::Table { alias: a, .. } => {
                                    *a = Some(alias);
                                }
                                vibesql_ast::FromClause::TableFunction { alias: a, .. } => {
                                    *a = Some(alias);
                                }
                                // Subquery/Values require and parse their alias
                                // inline in their own branches above, so they are
                                // not reachable via this fallback path.
                                vibesql_ast::FromClause::Subquery { .. }
                                | vibesql_ast::FromClause::Values { .. } => {}
                            }
                        }

                        from_clause
                    };

                Ok(result)
            }
            // Table-valued function in FROM position (JSON1 TVFs: json_each,
            // json_tree). An unquoted identifier immediately followed by `(` is a
            // TVF call only if the name is allow-listed; any other `ident(` in FROM
            // remains a parse error (preserving prior behavior). Delimited/quoted
            // identifiers are never TVFs — a quoted `"json_each"` is a table name.
            Token::Identifier(name)
                if matches!(self.peek_next(), Token::LParen)
                    && crate::table_functions::is_table_valued_function(name) =>
            {
                self.parse_table_function()
            }
            // SQLite compatibility: single-quoted strings can be used as table names
            Token::Identifier(_) | Token::DelimitedIdentifier(_) | Token::String(_) => {
                let table = self.parse_table_ref()?;

                // Check for optional alias
                // Parse optional table alias - keywords allowed after AS (e.g., FROM t AS year)
                let alias = self.parse_optional_table_alias()?;

                // Parse optional column aliases: AS alias (col1, col2, ...)
                // SQL:1999 Feature E051-09
                // Note: column_aliases requires an alias to be present
                let column_aliases =
                    if alias.is_some() { self.parse_column_alias_list()? } else { None };

                // Parse SQLite index hints: INDEXED BY <name> / NOT INDEXED
                let index_hint = self.parse_index_hint()?;

                Ok(vibesql_ast::FromClause::Table {
                    name: table.full_name(),
                    alias,
                    column_aliases,
                    quoted: table.is_any_quoted(),
                    index_hint,
                })
            }
            // Allow keywords as table names in FROM position. After FROM a keyword
            // that is not a clause/operator word is an unquoted table name, mirroring
            // the column-reference handling in `parse_identifier_expression` and the
            // CREATE TABLE side (so `FROM savepoint` resolves the table created by
            // `CREATE TABLE savepoint(...)` — see table.test table-7.3). Clause and
            // operator keywords (WHERE, JOIN, ON, ...) are excluded by
            // `can_be_identifier_in_table_position()` and continue to terminate the
            // clause; unlike expression position, CAST and the CURRENT_* words are
            // accepted here because no special primary form can start after FROM
            // (keyword1.test: `SELECT * FROM cast`).
            Token::Keyword { keyword: kw, .. } if kw.can_be_identifier_in_table_position() => {
                let table = self.parse_table_ref()?;

                // Check for optional alias. Uses the shared helper so a keyword
                // may serve as the alias of a keyword table name too — e.g.
                // `FROM over over` / `FROM window window` (window6 5.2/5.3/5.4).
                let alias = self.parse_optional_table_alias()?;

                let column_aliases =
                    if alias.is_some() { self.parse_column_alias_list()? } else { None };

                // Parse SQLite index hints: INDEXED BY <name> / NOT INDEXED
                let index_hint = self.parse_index_hint()?;

                Ok(vibesql_ast::FromClause::Table {
                    name: table.full_name(),
                    alias,
                    column_aliases,
                    quoted: table.is_any_quoted(),
                    index_hint,
                })
            }
            // No table name / subquery where one was required (e.g. `SELECT * FROM;`).
            // SQLite reports this as a token-level syntax error — `near ";": syntax
            // error` — rather than a descriptive message, so mirror that. This also
            // lets CREATE TRIGGER bodies surface the same syntax error at create time
            // (trigger1-2.1 / 2.2).
            _ => Err(ParseError { message: self.peek().syntax_error() }),
        }
    }

    /// Parse a table-valued function reference in FROM position, e.g.
    /// `json_each('[1,2,3]')`, `json_tree(x, '$.a')`, or
    /// `json_each(x) AS je(k, v)`.
    ///
    /// The caller has already verified (via lookahead) that the current token is
    /// an allow-listed identifier immediately followed by `(`. This method
    /// consumes the name, the parenthesized comma-separated argument list, and an
    /// optional `AS`-alias with an optional column-alias list. It produces
    /// [`vibesql_ast::FromClause::TableFunction`] with the name normalized to
    /// lowercase. No executor support exists yet — a parsed TableFunction errors
    /// or no-ops at execution until the executor phase lands.
    pub(crate) fn parse_table_function(&mut self) -> Result<vibesql_ast::FromClause, ParseError> {
        // Consume the function name (already known to be an allow-listed identifier).
        let name = match self.peek() {
            Token::Identifier(name) => {
                let normalized = crate::table_functions::normalized_table_valued_function(name)
                    .unwrap_or_else(|| name.to_lowercase());
                self.advance();
                normalized
            }
            // Unreachable in practice: the caller only dispatches here after
            // confirming an allow-listed identifier. Guard defensively anyway.
            _ => return Err(ParseError { message: self.peek().syntax_error() }),
        };

        // Parse the parenthesized, comma-separated argument list.
        self.expect_token(Token::LParen)?;
        let args = if self.peek() == &Token::RParen {
            Vec::new()
        } else {
            self.parse_comma_separated_list(|p| p.parse_expression())?
        };
        self.expect_token(Token::RParen)?;

        // Optional alias: `AS je` or bare `je`, reusing the shared alias helper.
        let alias = if self.peek_keyword(Keyword::As) {
            self.consume_keyword(Keyword::As)?;
            Some(self.parse_alias_name()?)
        } else if matches!(self.peek(), Token::Identifier(_) | Token::DelimitedIdentifier(_))
            && !self.is_join_keyword()
        {
            Some(self.parse_alias_name()?)
        } else {
            None
        };

        // Optional column-alias list: `AS je(k, v)`. Only meaningful with an alias.
        let column_aliases = if alias.is_some() { self.parse_column_alias_list()? } else { None };

        Ok(vibesql_ast::FromClause::TableFunction { name, args, alias, column_aliases })
    }

    /// Check if the upcoming tokens form a SQLite index hint:
    /// `INDEXED BY <name>` or `NOT INDEXED`.
    ///
    /// Note: INDEXED is not a keyword in our lexer, so it arrives as an
    /// identifier token (lowercased by the lexer).
    pub(crate) fn peek_index_hint(&self) -> bool {
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
    pub(crate) fn parse_index_hint(
        &mut self,
    ) -> Result<Option<vibesql_ast::IndexHint>, ParseError> {
        if !self.peek_index_hint() {
            return Ok(None);
        }
        if self.peek_keyword(Keyword::Not) {
            self.advance(); // consume NOT
            self.advance(); // consume INDEXED
            return Ok(Some(vibesql_ast::IndexHint::NotIndexed));
        }
        self.advance(); // consume INDEXED
        self.advance(); // consume BY
        match self.peek() {
            Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                let name = name.clone();
                self.advance();
                Ok(Some(vibesql_ast::IndexHint::IndexedBy(name)))
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
                Ok(Some(vibesql_ast::IndexHint::IndexedBy(name)))
            }
            _ => Err(ParseError { message: "Expected index name after INDEXED BY".to_string() }),
        }
    }

    /// Check if current token is a JOIN keyword
    pub(crate) fn is_join_keyword(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword { keyword: Keyword::Join, .. }
                | Token::Keyword { keyword: Keyword::Inner, .. }
                | Token::Keyword { keyword: Keyword::Left, .. }
                | Token::Keyword { keyword: Keyword::Right, .. }
                | Token::Keyword { keyword: Keyword::Cross, .. }
                | Token::Keyword { keyword: Keyword::Full, .. }
                | Token::Keyword { keyword: Keyword::Natural, .. }
                | Token::Keyword { keyword: Keyword::Outer, .. }
        )
    }

    /// Returns true when the current `WINDOW` keyword begins a real WINDOW clause,
    /// i.e. it is followed by `<name> AS`. SQLite treats `WINDOW` as a fallback
    /// identifier otherwise: `SELECT * FROM t4 window, t4` uses `window` as the
    /// table alias of `t4` (window6 4.1), not as the start of a WINDOW clause.
    pub(crate) fn peek_window_starts_clause(&self) -> bool {
        if !matches!(self.peek(), Token::Keyword { keyword: Keyword::Window, .. }) {
            return false;
        }
        let name_follows = matches!(
            self.peek_next(),
            Token::Identifier(_) | Token::DelimitedIdentifier(_) | Token::Keyword { .. }
        );
        name_follows
            && matches!(self.peek_at_offset(2), Token::Keyword { keyword: Keyword::As, .. })
    }

    /// Returns true when the current token is `WINDOW` used as an identifier (table
    /// alias) rather than as the start of a WINDOW clause. See
    /// `peek_window_starts_clause` for the disambiguation rule.
    pub(crate) fn window_keyword_used_as_alias(&self) -> bool {
        matches!(self.peek(), Token::Keyword { keyword: Keyword::Window, .. })
            && !self.peek_window_starts_clause()
    }

    /// Check if current token is a clause keyword that cannot be used as implicit alias
    /// These keywords start new clauses in SELECT statements
    pub(crate) fn is_clause_keyword(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword { keyword: Keyword::On, .. }
                | Token::Keyword { keyword: Keyword::Where, .. }
                | Token::Keyword { keyword: Keyword::Group, .. }
                | Token::Keyword { keyword: Keyword::Having, .. }
                | Token::Keyword { keyword: Keyword::Window, .. }
                | Token::Keyword { keyword: Keyword::Order, .. }
                | Token::Keyword { keyword: Keyword::Limit, .. }
                | Token::Keyword { keyword: Keyword::Offset, .. }
                | Token::Keyword { keyword: Keyword::Union, .. }
                | Token::Keyword { keyword: Keyword::Intersect, .. }
                | Token::Keyword { keyword: Keyword::Except, .. }
                | Token::Keyword { keyword: Keyword::Using, .. }
                | Token::Keyword { keyword: Keyword::For, .. }
                // RETURNING terminates the SELECT embedded in INSERT ... SELECT
                // ... RETURNING; SQLite likewise refuses RETURNING as an
                // implicit (AS-less) table alias.
                | Token::Keyword { keyword: Keyword::Returning, .. }
        )
    }

    /// Parse an optional table alias in FROM position.
    ///
    /// Handles three forms, mirroring SQLite:
    /// - `AS <name>` (explicit; keywords allowed after AS)
    /// - `<identifier>` (implicit, no AS) — but not a JOIN keyword / index hint
    /// - `<keyword>` used as an implicit alias (e.g. `FROM t m`, and the
    ///   fallback-identifier keywords `FROM over over` / `FROM t4 window`),
    ///   excluding JOIN keywords, clause keywords (unless `WINDOW` is being used
    ///   as an alias rather than starting a real WINDOW clause), and index hints.
    ///
    /// Shared by both the identifier-table-name and keyword-table-name branches
    /// so keyword aliases (`OVER`, `WINDOW`, ...) work after a keyword table name
    /// too (window6 5.2/5.3/5.4: `FROM over over`, `FROM window window`).
    pub(crate) fn parse_optional_table_alias(&mut self) -> Result<Option<String>, ParseError> {
        let alias = if self.peek_keyword(Keyword::As) {
            self.consume_keyword(Keyword::As)?;
            Some(self.parse_alias_name()?)
        } else if matches!(
            self.peek(),
            Token::Identifier(_) | Token::DelimitedIdentifier(_)
        ) && !self.is_join_keyword()
            && !self.peek_index_hint()
        {
            // Implicit alias (no AS keyword) - but not a JOIN keyword
            match self.peek() {
                Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                    let alias = id.clone();
                    self.advance();
                    Some(alias)
                }
                _ => None,
            }
        } else if matches!(self.peek(), Token::Keyword { keyword: _, .. })
            && !self.is_join_keyword()
            && (!self.is_clause_keyword() || self.window_keyword_used_as_alias())
            && !self.peek_index_hint()
        {
            // Allow non-reserved keywords as implicit aliases (e.g., FROM t m).
            // Keywords like M, YEAR, etc. can be used as aliases. WINDOW is
            // normally a clause keyword, but `FROM t4 window, t4` uses it as a
            // table alias (window6 4.1); `window_keyword_used_as_alias()`
            // permits that only when WINDOW does not begin a real
            // `WINDOW <name> AS (...)` clause.
            match self.peek() {
                Token::Keyword { keyword: kw, .. } => {
                    let alias = kw.to_string();
                    self.advance();
                    Some(alias)
                }
                _ => None,
            }
        } else {
            None
        };
        Ok(alias)
    }

    /// Parse JOIN type (INNER JOIN, LEFT JOIN, NATURAL JOIN, etc.)
    /// Returns (JoinType, is_natural)
    ///
    /// SQLite supports NATURAL in various positions:
    /// - NATURAL JOIN, NATURAL LEFT JOIN, NATURAL LEFT OUTER JOIN
    /// - LEFT NATURAL JOIN, LEFT OUTER NATURAL JOIN
    /// - OUTER LEFT NATURAL JOIN
    ///
    /// Invalid combinations like "INNER OUTER" or "LEFT BOGUS" produce:
    /// "unknown join type: INNER OUTER" (SQLite-compatible error message)
    pub(crate) fn parse_join_type(&mut self) -> Result<(vibesql_ast::JoinType, bool), ParseError> {
        // Track consumed join type keywords for error messages
        let mut consumed_keywords: Vec<String> = Vec::new();

        // Check for optional NATURAL keyword first
        let mut is_natural = if self.peek_keyword(Keyword::Natural) {
            self.consume_keyword(Keyword::Natural)?;
            consumed_keywords.push("NATURAL".to_string());
            true
        } else {
            false
        };

        let join_type = match self.peek() {
            Token::Keyword { keyword: Keyword::Join, .. } => {
                self.advance();
                vibesql_ast::JoinType::Inner // Default JOIN is INNER JOIN
            }
            Token::Keyword { keyword: Keyword::Inner, .. } => {
                self.advance();
                consumed_keywords.push("INNER".to_string());
                // Check for NATURAL after INNER (INNER NATURAL JOIN)
                if self.peek_keyword(Keyword::Natural) {
                    self.consume_keyword(Keyword::Natural)?;
                    is_natural = true;
                }
                // Check for invalid combinations like "INNER OUTER" or "INNER BOGUS"
                if let Token::Keyword { keyword: kw, .. } = self.peek() {
                    if *kw != Keyword::Join && *kw != Keyword::Natural {
                        // Consume remaining non-JOIN keywords to build error message
                        while let Token::Keyword { keyword: next_kw, .. } = self.peek() {
                            if *next_kw == Keyword::Join {
                                break;
                            }
                            consumed_keywords.push(next_kw.to_string().to_uppercase());
                            self.advance();
                        }
                        // If we ended on JOIN, that's part of an invalid combination
                        if self.peek_keyword(Keyword::Join) {
                            // We have something like "INNER OUTER JOIN" - invalid
                            return Err(ParseError {
                                message: format!(
                                    "unknown join type: {}",
                                    consumed_keywords.join(" ")
                                ),
                            });
                        }
                        return Err(ParseError {
                            message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                        });
                    }
                } else if let Token::Identifier(id) = self.peek() {
                    // Handle "INNER BOGUS" where BOGUS is an identifier
                    consumed_keywords.push(id.to_uppercase());
                    self.advance();
                    // Consume any more keywords/identifiers before JOIN
                    loop {
                        match self.peek() {
                            Token::Keyword { keyword: next_kw, .. }
                                if *next_kw != Keyword::Join =>
                            {
                                consumed_keywords.push(next_kw.to_string().to_uppercase());
                                self.advance();
                            }
                            Token::Identifier(id2) => {
                                consumed_keywords.push(id2.to_uppercase());
                                self.advance();
                            }
                            _ => break,
                        }
                    }
                    return Err(ParseError {
                        message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                    });
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::Inner
            }
            Token::Keyword { keyword: Keyword::Left, .. } => {
                self.advance();
                consumed_keywords.push("LEFT".to_string());
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                // Check for NATURAL after LEFT [OUTER] (LEFT NATURAL JOIN, LEFT OUTER NATURAL JOIN)
                if self.peek_keyword(Keyword::Natural) {
                    self.consume_keyword(Keyword::Natural)?;
                    is_natural = true;
                }
                // Check for invalid combinations like "LEFT BOGUS"
                if let Token::Keyword { keyword: kw, .. } = self.peek() {
                    if *kw != Keyword::Join && *kw != Keyword::Natural && *kw != Keyword::Outer {
                        // Consume remaining non-JOIN keywords to build error message
                        while let Token::Keyword { keyword: next_kw, .. } = self.peek() {
                            if *next_kw == Keyword::Join {
                                break;
                            }
                            consumed_keywords.push(next_kw.to_string().to_uppercase());
                            self.advance();
                        }
                        return Err(ParseError {
                            message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                        });
                    }
                } else if let Token::Identifier(id) = self.peek() {
                    // Handle "LEFT BOGUS" where BOGUS is an identifier
                    consumed_keywords.push(id.to_uppercase());
                    self.advance();
                    // Consume any more keywords before JOIN
                    while let Token::Keyword { keyword: next_kw, .. } = self.peek() {
                        if *next_kw == Keyword::Join {
                            break;
                        }
                        consumed_keywords.push(next_kw.to_string().to_uppercase());
                        self.advance();
                    }
                    return Err(ParseError {
                        message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                    });
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::LeftOuter
            }
            Token::Keyword { keyword: Keyword::Right, .. } => {
                self.advance();
                consumed_keywords.push("RIGHT".to_string());
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                // Check for NATURAL after RIGHT [OUTER]
                if self.peek_keyword(Keyword::Natural) {
                    self.consume_keyword(Keyword::Natural)?;
                    is_natural = true;
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::RightOuter
            }
            Token::Keyword { keyword: Keyword::Cross, .. } => {
                self.advance();
                consumed_keywords.push("CROSS".to_string());
                // Check for NATURAL after CROSS
                if self.peek_keyword(Keyword::Natural) {
                    self.consume_keyword(Keyword::Natural)?;
                    is_natural = true;
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::Cross
            }
            Token::Keyword { keyword: Keyword::Full, .. } => {
                self.advance();
                consumed_keywords.push("FULL".to_string());
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                // Check for NATURAL after FULL [OUTER]
                if self.peek_keyword(Keyword::Natural) {
                    self.consume_keyword(Keyword::Natural)?;
                    is_natural = true;
                }
                self.expect_keyword(Keyword::Join)?;
                vibesql_ast::JoinType::FullOuter
            }
            // Support "OUTER LEFT/RIGHT/FULL JOIN" syntax (SQLite compatibility)
            Token::Keyword { keyword: Keyword::Outer, .. } => {
                self.advance(); // Consume OUTER
                consumed_keywords.push("OUTER".to_string());
                match self.peek() {
                    Token::Keyword { keyword: Keyword::Left, .. } => {
                        self.advance();
                        // Check for NATURAL after OUTER LEFT
                        if self.peek_keyword(Keyword::Natural) {
                            self.consume_keyword(Keyword::Natural)?;
                            is_natural = true;
                        }
                        self.expect_keyword(Keyword::Join)?;
                        vibesql_ast::JoinType::LeftOuter
                    }
                    Token::Keyword { keyword: Keyword::Right, .. } => {
                        self.advance();
                        // Check for NATURAL after OUTER RIGHT
                        if self.peek_keyword(Keyword::Natural) {
                            self.consume_keyword(Keyword::Natural)?;
                            is_natural = true;
                        }
                        self.expect_keyword(Keyword::Join)?;
                        vibesql_ast::JoinType::RightOuter
                    }
                    Token::Keyword { keyword: Keyword::Full, .. } => {
                        self.advance();
                        // Check for NATURAL after OUTER FULL
                        if self.peek_keyword(Keyword::Natural) {
                            self.consume_keyword(Keyword::Natural)?;
                            is_natural = true;
                        }
                        self.expect_keyword(Keyword::Join)?;
                        vibesql_ast::JoinType::FullOuter
                    }
                    Token::Keyword { keyword: Keyword::Join, .. } => {
                        // "OUTER JOIN" without LEFT/RIGHT/FULL defaults to LEFT OUTER JOIN
                        self.advance();
                        vibesql_ast::JoinType::LeftOuter
                    }
                    Token::Keyword { keyword: kw, .. } => {
                        // Invalid combination like "OUTER NATURAL INNER"
                        consumed_keywords.push(kw.to_string().to_uppercase());
                        self.advance();
                        // Consume any more keywords before JOIN
                        while let Token::Keyword { keyword: next_kw, .. } = self.peek() {
                            if *next_kw == Keyword::Join {
                                break;
                            }
                            consumed_keywords.push(next_kw.to_string().to_uppercase());
                            self.advance();
                        }
                        return Err(ParseError {
                            message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                        });
                    }
                    _ => {
                        return Err(ParseError {
                            message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                        })
                    }
                }
            }
            _ => {
                // Handle "NATURAL AWK SED JOIN" and similar
                if !consumed_keywords.is_empty() {
                    // We already consumed NATURAL, now we see something unexpected
                    while let Token::Keyword { keyword: next_kw, .. } = self.peek() {
                        if *next_kw == Keyword::Join {
                            break;
                        }
                        consumed_keywords.push(next_kw.to_string().to_uppercase());
                        self.advance();
                    }
                    while let Token::Identifier(id) = self.peek() {
                        consumed_keywords.push(id.to_uppercase());
                        self.advance();
                    }
                    // Keep consuming keywords until JOIN
                    while let Token::Keyword { keyword: next_kw, .. } = self.peek() {
                        if *next_kw == Keyword::Join {
                            break;
                        }
                        consumed_keywords.push(next_kw.to_string().to_uppercase());
                        self.advance();
                    }
                    return Err(ParseError {
                        message: format!("unknown join type: {}", consumed_keywords.join(" ")),
                    });
                }
                return Err(ParseError { message: "Expected JOIN keyword".to_string() });
            }
        };

        // NATURAL CROSS JOIN is valid SQL and should apply the natural join condition
        // (matching on common column names). The executor handles this by treating
        // NATURAL CROSS JOIN as an inner join with the natural join condition.
        Ok((join_type, is_natural))
    }
}
