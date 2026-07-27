//! CREATE TABLE parsing

use vibesql_types::DataType;

use super::super::*;

impl Parser {
    /// Parse CREATE TABLE statement
    /// Supports CREATE [TEMP|TEMPORARY] TABLE syntax (SQLite compatibility)
    pub(in crate::parser) fn parse_create_table_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateTableStmt, ParseError> {
        self.expect_keyword(Keyword::Create)?;

        // Check for TEMP or TEMPORARY keyword (SQLite compatibility)
        // Temporary tables are stored in a separate "temp" schema
        let temporary = if self.peek_keyword(Keyword::Temp) || self.peek_keyword(Keyword::Temporary)
        {
            self.advance();
            true
        } else {
            false
        };

        self.expect_keyword(Keyword::Table)?;

        // Check for optional IF NOT EXISTS clause
        let if_not_exists = if self.peek_keyword(Keyword::If) {
            self.advance(); // consume IF
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse table name with quoted flag (supports schema.table).
        // Capture the verbatim source spelling of the table-name token before
        // `parse_table_ref` normalizes (de-quotes) it. SQLite echoes the table
        // name exactly as written — including its quoting form and casing — in
        // the "table ... already exists" error, whereas `table_name` below holds
        // the normalized identifier used for catalog lookups. For a
        // `schema.name` qualified reference the table-name token is the one
        // following the `.` separator, so we record the cursor position of the
        // *final* identifier token consumed by the table ref. (Mirrors the
        // CREATE TRIGGER mechanism from #5538.)
        let name_start_pos = self.current_position();
        let table = self.parse_table_ref()?;
        let name_token_pos = self.current_position().saturating_sub(1).max(name_start_pos);
        let name_source = self.token_source_at(name_token_pos).map(str::to_string);

        // Check for CREATE TABLE ... AS SELECT syntax
        if self.peek_keyword(Keyword::As) {
            self.advance(); // consume AS
                            // Parse the SELECT statement
            let select_stmt = self.parse_select_statement()?;

            // Expect semicolon or EOF
            if matches!(self.peek(), Token::Semicolon) {
                self.advance();
            }

            return Ok(vibesql_ast::CreateTableStmt {
                temporary,
                if_not_exists,
                table_name: table.full_name(),
                columns: Vec::new(),
                table_constraints: Vec::new(),
                table_options: Vec::new(),
                quoted: table.is_any_quoted(),
                name_source,
                as_query: Some(Box::new(select_stmt)),
                without_rowid: false, // CREATE TABLE AS SELECT cannot be WITHOUT ROWID
                strict: false,        // CREATE TABLE AS SELECT cannot be STRICT
            });
        }

        // Parse column definitions and table constraints
        self.expect_token(Token::LParen)?;
        let mut columns = Vec::new();
        let mut table_constraints = Vec::new();

        loop {
            // Check if this is a table-level constraint (including CONSTRAINT keyword)
            if self.peek_keyword(Keyword::Constraint)
                || self.peek_keyword(Keyword::Primary)
                || self.peek_keyword(Keyword::Foreign)
                || self.peek_keyword(Keyword::Unique)
                || self.peek_keyword(Keyword::Check)
                || self.peek_keyword(Keyword::Fulltext)
            {
                table_constraints.push(self.parse_table_constraint()?);
                // Absorb any trailing, body-less `CONSTRAINT <name>` clauses that
                // SQLite accepts and ignores after a table constraint (#6173).
                self.skip_trailing_constraint_names();
                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                    continue;
                } else {
                    break;
                }
            }

            // Parse column name
            let name = match self.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let c = col.clone();
                    self.advance();
                    c
                }
                // SQLite compatibility: at the start of a column definition, a keyword
                // can only be a column name. Table-level constraint keywords (CONSTRAINT,
                // PRIMARY, FOREIGN, UNIQUE, CHECK, FULLTEXT) are already consumed above,
                // so any remaining keyword here is an unquoted column name. SQLite reserves
                // very few words and accepts the rest as identifiers in this position,
                // including DESC, ASC, KEY, BEGIN, END (see table.test table-7.x).
                //
                // SQL:1999: normalize to lowercase when used as an unquoted identifier.
                Token::Keyword { keyword: kw, .. } => {
                    let col_name = format!("{}", kw).to_lowercase();
                    self.advance();
                    col_name
                }
                // SQLite compatibility: a single-quoted string literal is accepted
                // as a column name in a column-definition position (quote.test
                // quote-1.0: `CREATE TABLE '@abc'('#xyz' int, '!pqr' text)`).
                // In DDL name position there is no string-literal-vs-identifier
                // ambiguity, so we take the string verbatim as the column name.
                Token::String(col) => {
                    let c = col.clone();
                    self.advance();
                    c
                }
                _ => return Err(ParseError { message: "Expected column name".to_string() }),
            };

            // Parse data type (optional for SQLite compatibility)
            // SQLite allows typeless columns with BLOB affinity
            // Also check for generated column syntax: AS(expression) or GENERATED ALWAYS AS(expression)
            // Record the token index at the start of the (optional) datatype so
            // we can capture its verbatim source spelling for STRICT-table
            // validation (`type_source`).
            let type_start = self.current_position();
            let (data_type, is_exact_integer_type, mut generated_expr, type_source) =
                if self.peek_keyword(Keyword::As) {
                    // Generated column (short form): AS(expression)
                    self.advance(); // consume AS
                    self.expect_token(Token::LParen)?;
                    let expr = self.parse_expression()?;
                    self.expect_token(Token::RParen)?;
                    // Skip optional STORED or VIRTUAL keyword
                    if self.peek_keyword(Keyword::Stored) || self.peek_keyword(Keyword::Virtual) {
                        self.advance();
                    }
                    // Generated columns have the type inferred from the expression
                    // For now, use BLOB affinity (will be refined by type inference)
                    (DataType::BinaryLargeObject, false, Some(Box::new(expr)), None)
                } else if self.is_column_constraint_or_separator() {
                    // No data type specified - use BLOB affinity (SQLite default)
                    (DataType::BinaryLargeObject, false, None, None)
                } else {
                    let (dt, is_int) = self.parse_data_type_with_integer_flag()?;
                    let src = self.source_between(type_start, self.current_position());
                    (dt, is_int, None, src)
                };

            // Check for GENERATED ALWAYS AS (expression) [STORED|VIRTUAL] after data type
            if generated_expr.is_none() && self.peek_keyword(Keyword::Generated) {
                self.advance(); // consume GENERATED
                self.expect_keyword(Keyword::Always)?;
                self.expect_keyword(Keyword::As)?;
                self.expect_token(Token::LParen)?;
                let expr = self.parse_expression()?;
                self.expect_token(Token::RParen)?;
                // Skip optional STORED or VIRTUAL keyword
                if self.peek_keyword(Keyword::Stored) || self.peek_keyword(Keyword::Virtual) {
                    self.advance();
                }
                generated_expr = Some(Box::new(expr));
            }

            // Also check for short form AS (expression) after data type
            if generated_expr.is_none() && self.peek_keyword(Keyword::As) {
                self.advance(); // consume AS
                self.expect_token(Token::LParen)?;
                let expr = self.parse_expression()?;
                self.expect_token(Token::RParen)?;
                // Skip optional STORED or VIRTUAL keyword
                if self.peek_keyword(Keyword::Stored) || self.peek_keyword(Keyword::Virtual) {
                    self.advance();
                }
                generated_expr = Some(Box::new(expr));
            }

            // Parse optional DEFAULT clause (before COMMENT, per MySQL standard).
            // SQLite also allows DEFAULT to appear interleaved with column constraints
            // (e.g. `idx UNIQUE DEFAULT NULL`), so `parse_column_constraints` below may
            // also consume a DEFAULT clause and return it via its second tuple element.
            // Verbatim source text of the DEFAULT expression, captured so
            // PRAGMA table_info can echo the original spelling (#6175). A DEFAULT
            // may appear here (before constraints) or interleaved with them
            // (captured as `mid_default_source` below).
            let mut default_source: Option<String> = None;
            let mut default_value = if self.peek_keyword(Keyword::Default) {
                self.advance(); // consume DEFAULT
                let default_start = self.current_position();
                let expr = self.parse_expression()?;
                default_source = self.source_between(default_start, self.current_position());
                Some(Box::new(expr))
            } else {
                None
            };

            // Parse optional COMMENT clause (after DEFAULT, per MySQL standard)
            let comment = if self.peek_keyword(Keyword::Comment) {
                self.advance(); // consume COMMENT
                match self.peek() {
                    Token::String(s) => {
                        let c = s.clone();
                        self.advance();
                        Some(c)
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected string literal after COMMENT".to_string(),
                        })
                    }
                }
            } else {
                None
            };

            // Parse column constraints (which may include NOT NULL, an
            // interleaved DEFAULT, and — in any order — a generated-column
            // `[GENERATED ALWAYS] AS (expr)` clause).
            let (constraints, mid_default, mid_generated, mid_default_source) =
                self.parse_column_constraints()?;
            if let Some(d) = mid_default {
                if default_value.is_some() {
                    return Err(ParseError {
                        message: "Multiple DEFAULT clauses for the same column".to_string(),
                    });
                }
                default_value = Some(d);
                default_source = mid_default_source;
            }
            if let Some(g) = mid_generated {
                if generated_expr.is_some() {
                    return Err(ParseError {
                        message: "duplicate generated column clause".to_string(),
                    });
                }
                generated_expr = Some(g);
            }

            // SQLite rejects a generated column that also declares DEFAULT with
            // `cannot use DEFAULT on a generated column` (verified against
            // sqlite3 3.51.0). The DEFAULT may be written either adjacent to the
            // generated clause or interleaved with the column constraints
            // (captured above as `mid_default`), so this guard runs after the
            // constraint scan to cover both spellings — matching the same guard
            // in the ALTER TABLE ADD COLUMN path.
            if generated_expr.is_some() && default_value.is_some() {
                return Err(ParseError {
                    message: "cannot use DEFAULT on a generated column".to_string(),
                });
            }

            // Determine nullability based on constraints
            let nullable = !constraints
                .iter()
                .any(|c| matches!(&c.kind, vibesql_ast::ColumnConstraintKind::NotNull));

            // Record the verbatim DEFAULT source for PRAGMA table_info (#6175),
            // keyed by lowercase column name. Only recorded when both a default
            // and its captured source text are present (source capture requires
            // the parser to have been built with source/span info).
            if let Some(src) = default_source {
                self.column_default_sources.push((name.to_lowercase(), src));
            }

            columns.push(vibesql_ast::ColumnDef {
                name,
                data_type,
                nullable,
                constraints,
                default_value,
                comment,
                generated_expr,
                is_exact_integer_type,
                type_source,
            });

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        self.expect_token(Token::RParen)?;

        // Parse optional table options (MySQL extensions)
        let table_options = self.parse_table_options()?;

        // Parse optional trailing table-option clauses:
        //   WITH OIDS / WITHOUT OIDS (PostgreSQL), WITHOUT ROWID (SQLite),
        //   and STRICT (SQLite). SQLite accepts STRICT and WITHOUT ROWID in
        //   either order, separated by commas:
        //     CREATE TABLE t(a INTEGER) STRICT;
        //     CREATE TABLE t(a INTEGER) STRICT, WITHOUT ROWID;
        //     CREATE TABLE t(a INTEGER) WITHOUT ROWID, STRICT;
        let mut without_rowid = false;
        let mut strict = false;
        loop {
            if self.peek_keyword(Keyword::With) {
                self.advance(); // consume WITH
                self.expect_keyword(Keyword::Oids)?;
            } else if self.peek_keyword(Keyword::Without) {
                self.advance(); // consume WITHOUT
                if self.peek_keyword(Keyword::Oids) {
                    self.advance(); // consume OIDS
                } else if self.peek_keyword(Keyword::Rowid) {
                    self.advance(); // consume ROWID (SQLite WITHOUT ROWID tables)
                    without_rowid = true;
                } else {
                    return Err(ParseError {
                        message: "Expected OIDS or ROWID after WITHOUT".to_string(),
                    });
                }
            } else if self.peek_keyword(Keyword::Strict) {
                self.advance(); // consume STRICT (SQLite STRICT tables)
                strict = true;
            } else {
                break;
            }

            // Table-option clauses are comma-separated; stop when the comma is
            // absent so a trailing semicolon / EOF ends the statement.
            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::CreateTableStmt {
            temporary,
            if_not_exists,
            table_name: table.full_name(),
            columns,
            table_constraints,
            table_options,
            quoted: table.is_any_quoted(),
            name_source,
            as_query: None,
            without_rowid,
            strict,
        })
    }

    /// Check if the current token indicates a column constraint or column separator
    /// rather than a data type. This enables SQLite-style typeless column definitions.
    ///
    /// Returns true if the next token is:
    /// - A column separator: `,` or `)`
    /// - A column constraint keyword: `NULL`, `NOT`, `PRIMARY`, `UNIQUE`, `CHECK`, `REFERENCES`,
    ///   `AUTO_INCREMENT`, `AUTOINCREMENT`, `KEY`, `CONSTRAINT`
    /// - A column modifier: `DEFAULT`, `COMMENT`
    fn is_column_constraint_or_separator(&self) -> bool {
        match self.peek() {
            // Column separators
            Token::Comma | Token::RParen => true,

            // Column constraint keywords
            Token::Keyword { keyword: Keyword::Null, .. } => true,
            Token::Keyword { keyword: Keyword::Not, .. } => true,
            Token::Keyword { keyword: Keyword::Primary, .. } => true,
            Token::Keyword { keyword: Keyword::Unique, .. } => true,
            Token::Keyword { keyword: Keyword::Check, .. } => true,
            Token::Keyword { keyword: Keyword::References, .. } => true,
            Token::Keyword { keyword: Keyword::AutoIncrement, .. } => true,
            Token::Keyword { keyword: Keyword::Key, .. } => true,
            Token::Keyword { keyword: Keyword::Constraint, .. } => true,

            // Column modifiers (appear after data type normally)
            Token::Keyword { keyword: Keyword::Default, .. } => true,
            Token::Keyword { keyword: Keyword::Comment, .. } => true,
            Token::Keyword { keyword: Keyword::Collate, .. } => true,

            // Generated column keywords (SQLite)
            Token::Keyword { keyword: Keyword::Generated, .. } => true,
            Token::Keyword { keyword: Keyword::As, .. } => true,

            // Not a constraint/separator - likely a data type
            _ => false,
        }
    }
}
