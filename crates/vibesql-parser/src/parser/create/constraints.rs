//! Constraint parsing for CREATE TABLE

use super::super::*;

/// Validate that constraint columns do not contain expressions.
/// SQLite prohibits expressions in PRIMARY KEY and UNIQUE constraints.
fn validate_no_expressions_in_constraint(
    columns: &[vibesql_ast::IndexColumn],
) -> Result<(), ParseError> {
    for col in columns {
        if matches!(col, vibesql_ast::IndexColumn::Expression { .. }) {
            return Err(ParseError {
                message: "expressions prohibited in PRIMARY KEY and UNIQUE constraints".to_string(),
            });
        }
    }
    Ok(())
}

impl Parser {
    /// Parse an optional ON CONFLICT clause for constraints
    /// Returns None if no ON CONFLICT clause is present
    /// Syntax: ON CONFLICT ROLLBACK|ABORT|FAIL|IGNORE|REPLACE
    pub(in crate::parser) fn parse_constraint_conflict_clause(
        &mut self,
    ) -> Result<Option<vibesql_ast::ConflictClause>, ParseError> {
        // Check for ON keyword
        if !self.peek_keyword(Keyword::On) {
            return Ok(None);
        }

        // Peek ahead to see if next is CONFLICT (vs ON DELETE/UPDATE for foreign keys)
        if self.position + 1 < self.tokens.len() {
            if let Token::Keyword { keyword: Keyword::Conflict, .. } =
                &self.tokens[self.position + 1]
            {
                self.advance(); // consume ON
                self.advance(); // consume CONFLICT

                // Parse the conflict resolution algorithm
                if self.peek_keyword(Keyword::Rollback) {
                    self.advance();
                    return Ok(Some(vibesql_ast::ConflictClause::Rollback));
                } else if self.peek_keyword(Keyword::Abort) {
                    self.advance();
                    return Ok(Some(vibesql_ast::ConflictClause::Abort));
                } else if self.peek_keyword(Keyword::Fail) {
                    self.advance();
                    return Ok(Some(vibesql_ast::ConflictClause::Fail));
                } else if self.peek_keyword(Keyword::Ignore) {
                    self.advance();
                    return Ok(Some(vibesql_ast::ConflictClause::Ignore));
                } else if self.peek_keyword(Keyword::Replace) {
                    self.advance();
                    return Ok(Some(vibesql_ast::ConflictClause::Replace));
                } else {
                    return Err(ParseError {
                        message:
                            "Expected ROLLBACK, ABORT, FAIL, IGNORE, or REPLACE after ON CONFLICT"
                                .to_string(),
                    });
                }
            }
        }

        Ok(None)
    }

    /// Parse an index column specification with optional prefix length or expression
    ///
    /// Supports both simple column references and expression indexes:
    /// - Column: `col1`, `col1(10)` (with prefix length)
    /// - Expression: `(lower(name))`, `(a + b)`
    ///
    /// Syntax: column_name [ ( integer ) ] | ( expression )
    fn parse_index_column_spec(&mut self) -> Result<vibesql_ast::IndexColumn, ParseError> {
        // Check if this is an expression index (starts with parenthesis)
        if self.peek() == &Token::LParen {
            self.advance(); // consume LParen

            // Parse the expression
            let expr = self.parse_expression()?;

            self.expect_token(Token::RParen)?;

            // Check for optional ASC/DESC
            let direction = if self.peek_keyword(crate::keywords::Keyword::Asc) {
                self.advance();
                vibesql_ast::OrderDirection::Asc
            } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
                self.advance();
                vibesql_ast::OrderDirection::Desc
            } else {
                vibesql_ast::OrderDirection::Asc
            };

            self.reject_nulls_in_index_position()?;

            return Ok(vibesql_ast::IndexColumn::new_expression(expr, direction));
        }

        // Save position before parsing identifier - we may need to backtrack
        // if this turns out to be an expression like abs(b) rather than a column name
        let saved_position = self.position;

        let column_name = self.parse_column_name()?;

        // Check for optional prefix length: column_name(length)
        // BUT: if the token after ( is not a number, this is likely a function call
        // like abs(b), not a prefix length like name(10). In that case, backtrack
        // and parse as an expression.
        let prefix_length = if self.peek() == &Token::LParen {
            // Peek ahead to check if this is a prefix length (number) or function args
            if matches!(self.peek_at_offset(1), Token::Number(_)) {
                self.advance(); // consume LParen

                // Parse the integer length
                let length = match self.peek() {
                    Token::Number(n) => {
                        let value = n.parse::<i64>().map_err(|_| ParseError {
                            message: "Invalid integer for column prefix length".to_string(),
                        })?;
                        self.advance();

                        // Validate prefix length range
                        if value < 1 {
                            return Err(ParseError {
                                message: "Prefix length must be at least 1".to_string(),
                            });
                        }
                        if value > 10000 {
                            return Err(ParseError {
                                message: "Prefix length must not exceed 10000".to_string(),
                            });
                        }

                        value
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected integer for column prefix length".to_string(),
                        })
                    }
                };

                self.expect_token(Token::RParen)?;
                Some(length as u64)
            } else {
                // Not a prefix length - this is a function call like abs(b)
                // Backtrack and parse as an expression
                self.position = saved_position;
                let expr = self.parse_expression()?;

                // Check for optional ASC/DESC
                let direction = if self.peek_keyword(crate::keywords::Keyword::Asc) {
                    self.advance();
                    vibesql_ast::OrderDirection::Asc
                } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
                    self.advance();
                    vibesql_ast::OrderDirection::Desc
                } else {
                    vibesql_ast::OrderDirection::Asc
                };

                self.reject_nulls_in_index_position()?;

                return Ok(vibesql_ast::IndexColumn::new_expression(expr, direction));
            }
        } else {
            None
        };

        // Optional COLLATE clause (SQLite grammar allows a per-column collation
        // inside table-level PRIMARY KEY / UNIQUE column lists, e.g.
        // `PRIMARY KEY(a COLLATE nocase, a)` — verified against sqlite3 3.51.0).
        // Carried into `IndexColumn` so upsert conflict-target matching can
        // compare a target column's explicit COLLATE against this key-part's
        // collation (issue #5921), mirroring the CREATE INDEX column-list path.
        let collation = if self.peek_keyword(crate::keywords::Keyword::Collate) {
            self.advance(); // consume COLLATE
            let name = match self.peek() {
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
            Some(name)
        } else {
            None
        };

        // Check for optional ASC/DESC
        let direction = if self.peek_keyword(crate::keywords::Keyword::Asc) {
            self.advance();
            vibesql_ast::OrderDirection::Asc
        } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
            self.advance();
            vibesql_ast::OrderDirection::Desc
        } else {
            vibesql_ast::OrderDirection::Asc // Default for constraints
        };

        // Reject `NULLS FIRST` / `NULLS LAST` here too — same SQLite rule as in
        // CREATE INDEX column specs (see `parse_index_column_list`).
        self.reject_nulls_in_index_position()?;

        Ok(vibesql_ast::IndexColumn::Column {
            column_name,
            direction,
            prefix_length,
            collation,
            // Table-constraint column lists (PRIMARY KEY/UNIQUE) are a
            // separate grammar position from `CREATE INDEX`'s column list;
            // SQLite's quoted-identifier hint (issue #6560) only applies to
            // CREATE INDEX, so this is unconditionally unquoted.
            is_quoted: false,
        })
    }
    /// Parse the ON DELETE/UPDATE actions and MATCH clauses of a
    /// foreign-key-clause.
    ///
    /// SQLite's grammar allows `ON DELETE/UPDATE action` and `MATCH name`
    /// clauses to appear in any order and interleaved, e.g.
    /// `REFERENCES p MATCH SIMPLE ON DELETE CASCADE`. MATCH clauses are parsed
    /// but ignored: every SQLite foreign key behaves as if MATCH SIMPLE were
    /// specified (see e_fkey-62). Rejecting MATCH as a syntax error made every
    /// schema that spells one out fail to create.
    fn parse_referential_actions(
        &mut self,
    ) -> Result<
        (Option<vibesql_ast::ReferentialAction>, Option<vibesql_ast::ReferentialAction>),
        ParseError,
    > {
        let mut on_delete = None;
        let mut on_update = None;

        loop {
            if self.peek_keyword(Keyword::On) {
                self.advance(); // consume ON
                if self.peek_keyword(Keyword::Delete) {
                    self.advance(); // consume DELETE
                    on_delete = Some(self.parse_referential_action()?);
                } else if self.peek_keyword(Keyword::Update) {
                    self.advance(); // consume UPDATE
                    on_update = Some(self.parse_referential_action()?);
                } else {
                    return Err(ParseError {
                        message: "Expected DELETE or UPDATE after ON".to_string(),
                    });
                }
            } else if self.peek_keyword(Keyword::Match) {
                self.advance(); // consume MATCH
                self.consume_match_name()?; // MATCH <name> — parsed and ignored
            } else {
                break;
            }
        }

        Ok((on_delete, on_update))
    }

    /// Consume the name argument of a foreign-key `MATCH` clause and discard it.
    ///
    /// The name is one of SIMPLE / FULL / PARTIAL / NONE per the SQL standard,
    /// but SQLite (and thus VibeSQL) accepts any identifier-like token here and
    /// ignores it. FULL and NONE tokenize as keywords while SIMPLE and PARTIAL
    /// tokenize as identifiers, so both token classes are accepted (mirroring
    /// how a COLLATE name is consumed above).
    fn consume_match_name(&mut self) -> Result<(), ParseError> {
        match self.peek() {
            Token::Identifier(_) | Token::DelimitedIdentifier(_) | Token::Keyword { .. } => {
                self.advance();
                Ok(())
            }
            _ => Err(ParseError { message: "Expected name after MATCH".to_string() }),
        }
    }

    /// Parse constraint deferral mode: [NOT] DEFERRABLE [INITIALLY {DEFERRED | IMMEDIATE}]
    ///
    /// Returns None if no deferral clause is present.
    fn parse_constraint_deferral(
        &mut self,
    ) -> Result<Option<vibesql_ast::ConstraintDeferral>, ParseError> {
        let is_deferrable = if self.peek_keyword(Keyword::Not) {
            // Check if followed by DEFERRABLE
            if self.position + 1 < self.tokens.len() {
                if let Token::Keyword { keyword: Keyword::Deferrable, .. } =
                    &self.tokens[self.position + 1]
                {
                    self.advance(); // consume NOT
                    self.advance(); // consume DEFERRABLE
                    false
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        } else if self.peek_keyword(Keyword::Deferrable) {
            self.advance(); // consume DEFERRABLE
            true
        } else {
            return Ok(None);
        };

        // Parse optional INITIALLY clause
        let initially_deferred = if self.peek_keyword(Keyword::Initially) {
            self.advance(); // consume INITIALLY
            if self.peek_keyword(Keyword::Deferred) {
                self.advance(); // consume DEFERRED
                true
            } else if self.peek_keyword(Keyword::Immediate) {
                self.advance(); // consume IMMEDIATE
                false
            } else {
                return Err(ParseError {
                    message: "Expected DEFERRED or IMMEDIATE after INITIALLY".to_string(),
                });
            }
        } else {
            false // Default is INITIALLY IMMEDIATE
        };

        Ok(Some(vibesql_ast::ConstraintDeferral { is_deferrable, initially_deferred }))
    }

    /// Parse a single referential action (NO ACTION, CASCADE, RESTRICT, SET NULL, SET DEFAULT)
    fn parse_referential_action(&mut self) -> Result<vibesql_ast::ReferentialAction, ParseError> {
        if self.peek_keyword(Keyword::No) {
            self.advance(); // consume NO
            self.expect_keyword(Keyword::Action)?;
            Ok(vibesql_ast::ReferentialAction::NoAction)
        } else if self.peek_keyword(Keyword::Cascade) {
            self.advance(); // consume CASCADE
            Ok(vibesql_ast::ReferentialAction::Cascade)
        } else if self.peek_keyword(Keyword::Restrict) {
            self.advance(); // consume RESTRICT
            Ok(vibesql_ast::ReferentialAction::Restrict)
        } else if self.peek_keyword(Keyword::Set) {
            self.advance(); // consume SET
            if self.peek_keyword(Keyword::Null) {
                self.advance(); // consume NULL
                Ok(vibesql_ast::ReferentialAction::SetNull)
            } else if self.peek_keyword(Keyword::Default) {
                self.advance(); // consume DEFAULT
                Ok(vibesql_ast::ReferentialAction::SetDefault)
            } else {
                Err(ParseError { message: "Expected NULL or DEFAULT after SET".to_string() })
            }
        } else {
            Err(ParseError {
                message: "Expected NO ACTION, CASCADE, RESTRICT, SET NULL, or SET DEFAULT"
                    .to_string(),
            })
        }
    }

    /// Parse column-level constraints (PRIMARY KEY, UNIQUE, CHECK, REFERENCES)
    ///
    /// Returns the list of parsed constraints plus an optional `DEFAULT` expression
    /// encountered mid-stream. SQLite grammar allows DEFAULT to appear interleaved
    /// with column constraints in any order (e.g., `idx UNIQUE DEFAULT NULL`), so
    /// callers should treat the returned default as additional output and merge it
    /// with any DEFAULT clause parsed before the constraint list.
    #[allow(clippy::type_complexity)]
    pub(in crate::parser) fn parse_column_constraints(
        &mut self,
        column_name: &str,
    ) -> Result<
        (
            Vec<vibesql_ast::ColumnConstraint>,
            Option<Box<vibesql_ast::Expression>>,
            Option<Box<vibesql_ast::Expression>>,
            Option<String>,
        ),
        ParseError,
    > {
        let mut constraints = Vec::new();
        let mut default_value: Option<Box<vibesql_ast::Expression>> = None;
        let mut generated_expr: Option<Box<vibesql_ast::Expression>> = None;
        // Verbatim source text of an interleaved DEFAULT expression, captured so
        // PRAGMA table_info can echo the original spelling (#6175). See
        // `Parser::parse_sql_with_default_sources`.
        let mut default_source: Option<String> = None;

        loop {
            // Check for optional CONSTRAINT keyword
            let name = if self.peek_keyword(Keyword::Constraint) {
                self.advance(); // consume CONSTRAINT
                match self.peek() {
                    // SQLite accepts identifiers, double-quoted identifiers, and
                    // single-quoted string literals as constraint names
                    // (e.g. `CONSTRAINT 'c1' NOT NULL`).
                    Token::Identifier(n) | Token::DelimitedIdentifier(n) | Token::String(n) => {
                        let constraint_name = n.clone();
                        self.advance();
                        Some(constraint_name)
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected constraint name after CONSTRAINT".to_string(),
                        })
                    }
                }
            } else {
                None
            };

            match self.peek() {
                Token::Keyword { keyword: Keyword::Default, .. } => {
                    // DEFAULT clause may appear interleaved with column constraints
                    // (SQLite grammar), and SQLite also permits a preceding
                    // `CONSTRAINT <name>` on a DEFAULT clause (e.g.
                    // `c1 text CONSTRAINT "1 2" DEFAULT (1+1)` —
                    // e_createtable-0.2.1.5.6 / #6406). VibeSQL does not
                    // currently model a named DEFAULT constraint downstream
                    // (the default expression is stored unconditionally, not
                    // as a `ColumnConstraint`), so the name is accepted
                    // syntactically and discarded here, matching how an
                    // unnamed DEFAULT is handled.
                    self.advance(); // consume DEFAULT
                    let default_start = self.current_position();
                    let expr = self.parse_expression()?;
                    if default_value.is_some() {
                        return Err(ParseError {
                            message: "Multiple DEFAULT clauses for the same column".to_string(),
                        });
                    }
                    default_source = self.source_between(default_start, self.current_position());
                    default_value = Some(Box::new(expr));
                }
                Token::Keyword { keyword: Keyword::Null, .. } => {
                    // MySQL allows standalone NULL keyword to explicitly indicate nullable column
                    // (which is the default anyway), so we just consume it and skip it
                    self.advance(); // consume NULL
                                    // This is a no-op - nullable is the default
                }
                Token::Keyword { keyword: Keyword::Not, .. } => {
                    // Disambiguate NOT NULL from the `NOT DEFERRABLE` deferral
                    // clause, which SQLite also permits as a standalone
                    // column-level constraint (it is ignored for non-FK
                    // columns but must parse without error).
                    let next_is_deferrable = matches!(
                        self.tokens.get(self.position + 1),
                        Some(Token::Keyword { keyword: Keyword::Deferrable, .. })
                    );
                    if next_is_deferrable {
                        // NOT DEFERRABLE [INITIALLY ...] — parsed and ignored.
                        let _ = self.parse_constraint_deferral()?;
                    } else {
                        self.advance(); // consume NOT
                        self.expect_keyword(Keyword::Null)?;
                        // Parse optional ON CONFLICT clause (SQLite extension):
                        // `col NOT NULL ON CONFLICT ROLLBACK|ABORT|FAIL|IGNORE|REPLACE`.
                        let on_conflict = self.parse_constraint_conflict_clause()?;
                        let kind = match on_conflict {
                            Some(_) => vibesql_ast::ColumnConstraintKind::NotNullWithConflict {
                                on_conflict,
                            },
                            None => vibesql_ast::ColumnConstraintKind::NotNull,
                        };
                        constraints.push(vibesql_ast::ColumnConstraint { name, kind });
                    }
                }
                Token::Keyword { keyword: Keyword::Deferrable, .. } => {
                    // Standalone column-level DEFERRABLE [INITIALLY DEFERRED |
                    // IMMEDIATE]. SQLite ignores deferral on non-FK columns but
                    // accepts the syntax; parse it and drop the result.
                    let _ = self.parse_constraint_deferral()?;
                }
                Token::Keyword { keyword: Keyword::Primary, .. } => {
                    self.advance(); // consume PRIMARY
                    self.expect_keyword(Keyword::Key)?;
                    // SQLite allows optional ASC or DESC after PRIMARY KEY
                    if self.peek_keyword(Keyword::Asc) || self.peek_keyword(Keyword::Desc) {
                        self.advance(); // consume ASC or DESC (ignored - SQLite ignores it too)
                    }
                    // Parse optional ON CONFLICT clause
                    let on_conflict = self.parse_constraint_conflict_clause()?;
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::PrimaryKey { on_conflict },
                    });
                }
                Token::Keyword { keyword: Keyword::Unique, .. } => {
                    self.advance(); // consume UNIQUE
                                    // MySQL allows optional KEY keyword after UNIQUE
                    if self.peek_keyword(Keyword::Key) {
                        self.advance(); // consume KEY
                    }
                    // Parse optional ON CONFLICT clause
                    let on_conflict = self.parse_constraint_conflict_clause()?;
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::Unique { on_conflict },
                    });
                }
                Token::Keyword { keyword: Keyword::Check, .. } => {
                    self.advance(); // consume CHECK
                    let lparen_pos = self.position;
                    self.expect_token(Token::LParen)?;
                    let expr = self.parse_expression()?;
                    let rparen_pos = self.position;
                    self.expect_token(Token::RParen)?;
                    // Preserve the verbatim CHECK expression text so the
                    // "CHECK constraint failed: <expr>" message echoes the
                    // original operator spacing (SQLite-compatible), not the
                    // whitespace-stripped `expr.to_sql()` re-render.
                    let source_text = self.source_inside_delimiters(lparen_pos, rparen_pos);
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::Check {
                            expr: Box::new(expr),
                            source_text,
                        },
                    });
                }
                Token::Keyword { keyword: Keyword::References, .. } => {
                    self.advance(); // consume REFERENCES
                                    // Accept both regular and quoted identifiers (e.g., REFERENCES t1, REFERENCES
                                    // "t1")
                    let table = self.parse_identifier().map_err(|_| ParseError {
                        message: "Expected table name after REFERENCES".to_string(),
                    })?;

                    // Column specification is optional in SQLite - defaults to PK
                    let column = if self.peek() == &Token::LParen {
                        self.advance(); // consume LParen
                                        // Accept both regular and quoted identifiers for column names.
                                        // Do NOT override the error on failure: an empty list
                                        // (`REFERENCES p()`) must surface SQLite's own generic
                                        // "near \")\": syntax error" (e_fkey-28.2, Part of #6170)
                                        // rather than a VibeSQL-specific message -- SQLite's grammar
                                        // has no production for an empty column-name-list, so it
                                        // reports a plain LALR syntax error naming the unexpected
                                        // token, not a custom REFERENCES-aware message.
                        let col_name = self.parse_column_name()?;
                        // An inline column-constraint REFERENCES clause defines a
                        // single-column foreign key, so the parent column list must
                        // name exactly one column. SQLite still parses a longer list
                        // grammatically but rejects it as a semantic error naming the
                        // child column and parent table (table.test table-10.11):
                        //   CREATE TABLE t6(a,b, c REFERENCES t4(x,y));
                        //   -> "foreign key on c should reference only one column of table t4"
                        // (issue #6173). Consume the rest of the list so the paren is
                        // balanced before reporting the error.
                        let mut extra = false;
                        while matches!(self.peek(), Token::Comma) {
                            self.advance();
                            self.parse_column_name().map_err(|_| ParseError {
                                message: "Expected column name in REFERENCES".to_string(),
                            })?;
                            extra = true;
                        }
                        self.expect_token(Token::RParen)?;
                        if extra {
                            return Err(ParseError {
                                message: format!(
                                    "foreign key on {} should reference only one column of table {}",
                                    column_name, table
                                ),
                            });
                        }
                        Some(col_name)
                    } else {
                        None // Column not specified, defaults to PK
                    };

                    let (on_delete, on_update) = self.parse_referential_actions()?;
                    let deferral = self.parse_constraint_deferral()?;

                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::References {
                            table,
                            column,
                            on_delete,
                            on_update,
                            deferral,
                        },
                    });
                }
                Token::Keyword { keyword: Keyword::AutoIncrement, .. } => {
                    self.advance(); // consume AUTO_INCREMENT or AUTOINCREMENT
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::AutoIncrement,
                    });
                }
                Token::Keyword { keyword: Keyword::Key, .. } => {
                    self.advance(); // consume KEY
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::Key,
                    });
                }
                Token::Keyword { keyword: Keyword::Collate, .. } => {
                    self.advance(); // consume COLLATE
                    let collation_name = match self.peek() {
                        Token::Identifier(n) | Token::DelimitedIdentifier(n) => {
                            let coll = n.clone();
                            self.advance();
                            coll
                        }
                        Token::Keyword { original, .. } => {
                            // Allow keyword-like collation names (e.g., NOCASE, BINARY)
                            let coll = original.clone();
                            self.advance();
                            coll
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected collation name after COLLATE".to_string(),
                            })
                        }
                    };
                    constraints.push(vibesql_ast::ColumnConstraint {
                        name,
                        kind: vibesql_ast::ColumnConstraintKind::Collate(collation_name),
                    });
                }
                Token::Keyword { keyword: Keyword::Generated, .. } => {
                    // Generated column constraint (full form):
                    //   [CONSTRAINT name] GENERATED ALWAYS AS (expr) [STORED|VIRTUAL]
                    // In SQLite the generated-column clause is itself a
                    // column-constraint alternative, so it may appear interleaved
                    // with UNIQUE / NOT NULL / etc. in any order (e.g.
                    // `b UNIQUE AS(a)`, `b INT NOT NULL AS(a==0)`). Parsing it here
                    // — rather than only at the fixed post-type position — lets
                    // those orderings parse (#6173: gencol1 `near "AS"` failures).
                    self.advance(); // consume GENERATED
                    self.expect_keyword(Keyword::Always)?;
                    self.expect_keyword(Keyword::As)?;
                    self.expect_token(Token::LParen)?;
                    let expr = self.parse_expression()?;
                    self.expect_token(Token::RParen)?;
                    if self.peek_keyword(Keyword::Stored) || self.peek_keyword(Keyword::Virtual) {
                        self.advance();
                    }
                    if generated_expr.is_some() {
                        return Err(ParseError {
                            message: "duplicate generated column clause".to_string(),
                        });
                    }
                    generated_expr = Some(Box::new(expr));
                }
                Token::Keyword { keyword: Keyword::As, .. } => {
                    // Generated column constraint (short form):
                    //   [CONSTRAINT name] AS (expr) [STORED|VIRTUAL]
                    self.advance(); // consume AS
                    self.expect_token(Token::LParen)?;
                    let expr = self.parse_expression()?;
                    self.expect_token(Token::RParen)?;
                    if self.peek_keyword(Keyword::Stored) || self.peek_keyword(Keyword::Virtual) {
                        self.advance();
                    }
                    if generated_expr.is_some() {
                        return Err(ParseError {
                            message: "duplicate generated column clause".to_string(),
                        });
                    }
                    generated_expr = Some(Box::new(expr));
                }
                _ => {
                    // A `CONSTRAINT name` clause with no constraint body following
                    // it is accepted and ignored by SQLite for backwards
                    // compatibility ("The CONSTRAINT name clause can follow a
                    // constraint. Such a clause is ignored." — check.test 2.10).
                    // This also covers back-to-back names such as
                    // `CONSTRAINT x_one CONSTRAINT x_two CHECK(...)`, where only the
                    // final name binds to the constraint body. We consumed the
                    // CONSTRAINT + name tokens above, so continue the loop to pick
                    // up whatever follows (another name, a real constraint, or the
                    // column terminator) rather than erroring (#6173).
                    if name.is_some() {
                        continue;
                    }
                    break;
                }
            }
        }

        Ok((constraints, default_value, generated_expr, default_source))
    }

    /// Absorb trailing, body-less `CONSTRAINT <name>` clauses that SQLite accepts
    /// and ignores after a table-level constraint (e.g.
    /// `CONSTRAINT u_one UNIQUE(x,y,z) CONSTRAINT u_two` — check.test 2.12). Only
    /// a `CONSTRAINT <name>` immediately followed by a `,` or `)` terminator is
    /// treated as such a trailing name; a `CONSTRAINT <name> <TYPE>` sequence is
    /// left untouched so a genuine next constraint still parses.
    pub(in crate::parser) fn skip_trailing_constraint_names(&mut self) {
        while self.peek_keyword(Keyword::Constraint) {
            let name_is_ident = matches!(
                self.tokens.get(self.position + 1),
                Some(Token::Identifier(_) | Token::DelimitedIdentifier(_) | Token::String(_))
            );
            let terminated = matches!(
                self.tokens.get(self.position + 2),
                Some(Token::Comma) | Some(Token::RParen)
            );
            if name_is_ident && terminated {
                self.advance(); // consume CONSTRAINT
                self.advance(); // consume the ignored name
            } else {
                break;
            }
        }
    }

    /// Parse table-level constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK)
    pub(in crate::parser) fn parse_table_constraint(
        &mut self,
    ) -> Result<vibesql_ast::TableConstraint, ParseError> {
        // Check for optional CONSTRAINT keyword
        let name = if self.peek_keyword(Keyword::Constraint) {
            self.advance(); // consume CONSTRAINT
            match self.peek() {
                // SQLite accepts identifiers, double-quoted identifiers, and
                // single-quoted string literals as constraint names
                // (e.g. `CONSTRAINT 'c1' CHECK(...)`).
                Token::Identifier(n) | Token::DelimitedIdentifier(n) | Token::String(n) => {
                    let constraint_name = n.clone();
                    self.advance();
                    Some(constraint_name)
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected constraint name after CONSTRAINT".to_string(),
                    })
                }
            }
        } else {
            None
        };

        let kind = match self.peek() {
            Token::Keyword { keyword: Keyword::Primary, .. } => {
                self.advance(); // consume PRIMARY
                self.expect_keyword(Keyword::Key)?;
                self.expect_token(Token::LParen)?;

                let mut columns = Vec::new();
                loop {
                    columns.push(self.parse_index_column_spec()?);

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                // SQLite's alternate AUTOINCREMENT syntax: the keyword may
                // appear directly inside the PRIMARY KEY parentheses, after
                // the column list, e.g. `PRIMARY KEY(x AUTOINCREMENT)`
                // (autoinc-7.1, issue #6173) — equivalent to declaring `x
                // INTEGER PRIMARY KEY AUTOINCREMENT` as a column constraint.
                // Only meaningful for a single-column PK; record the column
                // name so the CREATE TABLE column loop (table.rs) can fold it
                // into that column's own constraint list. A multi-column PK
                // followed by AUTOINCREMENT has no documented meaning in
                // SQLite and no test exercises it, so the keyword is simply
                // left unconsumed in that case (surfacing as a syntax error,
                // same as before this change).
                if columns.len() == 1 && self.peek_keyword(Keyword::AutoIncrement) {
                    self.advance(); // consume AUTOINCREMENT
                    if let Some(name) = columns[0].column_name() {
                        self.pending_table_pk_autoincrement_column = Some(name.to_string());
                    }
                }
                self.expect_token(Token::RParen)?;
                // Validate no expressions in PRIMARY KEY constraint
                validate_no_expressions_in_constraint(&columns)?;
                // Parse optional ON CONFLICT clause
                let on_conflict = self.parse_constraint_conflict_clause()?;
                vibesql_ast::TableConstraintKind::PrimaryKey { columns, on_conflict }
            }
            Token::Keyword { keyword: Keyword::Foreign, .. } => {
                self.advance(); // consume FOREIGN
                self.expect_keyword(Keyword::Key)?;
                self.expect_token(Token::LParen)?;

                let mut columns = Vec::new();
                loop {
                    // Accept both regular and quoted identifiers for column names
                    let col = self.parse_column_name().map_err(|_| ParseError {
                        message: "Expected column name in FOREIGN KEY".to_string(),
                    })?;
                    columns.push(col);

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.expect_token(Token::RParen)?;
                self.expect_keyword(Keyword::References)?;

                // Accept both regular and quoted identifiers for table names
                let references_table = self.parse_identifier().map_err(|_| ParseError {
                    message: "Expected table name after REFERENCES".to_string(),
                })?;

                // Column specification is optional - if omitted, defaults to PK
                let mut references_columns = Vec::new();
                if self.peek() == &Token::LParen {
                    self.advance(); // consume LParen

                    loop {
                        // Accept both regular and quoted identifiers for column names.
                        // Do NOT override the error on failure: an empty list
                        // (`FOREIGN KEY(...) REFERENCES p()`) must surface SQLite's own
                        // generic "near \")\": syntax error" (e_fkey-28.4/28.5, Part of
                        // #6170) rather than a VibeSQL-specific message -- see the
                        // matching column-constraint REFERENCES parse above.
                        let col = self.parse_column_name()?;
                        references_columns.push(col);

                        if matches!(self.peek(), Token::Comma) {
                            self.advance();
                        } else {
                            break;
                        }
                    }

                    self.expect_token(Token::RParen)?;
                }

                let (on_delete, on_update) = self.parse_referential_actions()?;
                let deferral = self.parse_constraint_deferral()?;

                vibesql_ast::TableConstraintKind::ForeignKey {
                    columns,
                    references_table,
                    references_columns,
                    on_delete,
                    on_update,
                    deferral,
                }
            }
            Token::Keyword { keyword: Keyword::Unique, .. } => {
                self.advance(); // consume UNIQUE
                self.expect_token(Token::LParen)?;

                let mut columns = Vec::new();
                loop {
                    columns.push(self.parse_index_column_spec()?);

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.expect_token(Token::RParen)?;
                // Validate no expressions in UNIQUE constraint
                validate_no_expressions_in_constraint(&columns)?;
                // Parse optional ON CONFLICT clause
                let on_conflict = self.parse_constraint_conflict_clause()?;
                vibesql_ast::TableConstraintKind::Unique { columns, on_conflict }
            }
            Token::Keyword { keyword: Keyword::Check, .. } => {
                self.advance(); // consume CHECK
                let lparen_pos = self.position;
                self.expect_token(Token::LParen)?;
                let expr = self.parse_expression()?;
                let rparen_pos = self.position;
                self.expect_token(Token::RParen)?;
                // Preserve the verbatim CHECK expression text (see the
                // column-level CHECK arm) so the violation message echoes the
                // original operator spacing.
                let source_text = self.source_inside_delimiters(lparen_pos, rparen_pos);
                vibesql_ast::TableConstraintKind::Check { expr: Box::new(expr), source_text }
            }
            Token::Keyword { keyword: Keyword::Fulltext, .. } => {
                self.advance(); // consume FULLTEXT

                // Optional INDEX keyword
                if self.peek_keyword(Keyword::Index) {
                    self.advance(); // consume INDEX
                }

                // Optional index name
                let index_name = if matches!(self.peek(), Token::Identifier(_)) {
                    // Look ahead to see if next token is LParen
                    if self.position + 1 < self.tokens.len()
                        && matches!(self.tokens[self.position + 1], Token::LParen)
                    {
                        let name = match self.peek() {
                            Token::Identifier(n) => Some(n.clone()),
                            _ => None,
                        };
                        if name.is_some() {
                            self.advance(); // consume index name
                        }
                        name
                    } else {
                        None
                    }
                } else {
                    None
                };

                self.expect_token(Token::LParen)?;

                let mut columns = Vec::new();
                loop {
                    columns.push(self.parse_index_column_spec()?);

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.expect_token(Token::RParen)?;
                vibesql_ast::TableConstraintKind::Fulltext { index_name, columns }
            }
            _ => return Err(ParseError {
                message:
                    "Expected table constraint keyword (PRIMARY, FOREIGN, UNIQUE, CHECK, FULLTEXT)"
                        .to_string(),
            }),
        };

        Ok(vibesql_ast::TableConstraint { name, kind })
    }
}
