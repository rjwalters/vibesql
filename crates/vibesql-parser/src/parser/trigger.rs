//! CREATE TRIGGER and DROP TRIGGER statement parsers

use super::{ParseError, Parser};
use crate::{keywords::Keyword, token::Token};

impl Parser {
    /// Parse CREATE TRIGGER statement
    ///
    /// Syntax:
    ///   CREATE [TEMP | TEMPORARY] TRIGGER trigger_name
    ///   [{BEFORE | AFTER | INSTEAD OF}] {INSERT | UPDATE | DELETE}
    ///   ON table_name
    ///   [FOR EACH {ROW | STATEMENT}]
    ///   [WHEN (condition)]
    ///   triggered_action
    ///
    /// Note: BEFORE/AFTER/INSTEAD OF is optional; defaults to BEFORE (SQLite compatibility)
    pub(super) fn parse_create_trigger_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateTriggerStmt, ParseError> {
        // Expect CREATE keyword
        self.expect_keyword(Keyword::Create)?;

        // Optional TEMP/TEMPORARY modifier, accepted for SQLite compatibility and
        // ignored: SQLite places TEMP triggers in a session-scoped `temp` schema,
        // but VibeSQL has no multi-session or ATTACH support, so the trigger is
        // created as a regular trigger.
        let _ =
            self.try_consume_keyword(Keyword::Temp) || self.try_consume_keyword(Keyword::Temporary);

        // Expect TRIGGER keyword
        self.expect_keyword(Keyword::Trigger)?;

        // Parse trigger name
        let trigger_name = self.parse_identifier()?;

        // Parse timing: BEFORE | AFTER | INSTEAD OF (optional, defaults to BEFORE per SQLite)
        let timing = if self.try_consume_keyword(Keyword::Before) {
            vibesql_ast::TriggerTiming::Before
        } else if self.try_consume_keyword(Keyword::After) {
            vibesql_ast::TriggerTiming::After
        } else if self.try_consume_keyword(Keyword::Instead) {
            self.expect_keyword(Keyword::Of)?;
            vibesql_ast::TriggerTiming::InsteadOf
        } else {
            // SQLite allows omitting the timing, defaulting to BEFORE
            // Check if next token is an event keyword (INSERT, UPDATE, DELETE)
            if self.peek_keyword(Keyword::Insert)
                || self.peek_keyword(Keyword::Update)
                || self.peek_keyword(Keyword::Delete)
            {
                vibesql_ast::TriggerTiming::Before
            } else {
                return Err(ParseError {
                    message: "Expected BEFORE, AFTER, INSTEAD OF, or event (INSERT/UPDATE/DELETE) after trigger name".to_string(),
                });
            }
        };

        // Parse event: INSERT | UPDATE [OF columns] | DELETE
        let event = if self.try_consume_keyword(Keyword::Insert) {
            vibesql_ast::TriggerEvent::Insert
        } else if self.try_consume_keyword(Keyword::Update) {
            // Check for optional OF column_list
            let columns = if self.try_consume_keyword(Keyword::Of) {
                let mut cols = Vec::new();
                self.expect_token(Token::LParen)?;
                loop {
                    let col = self.parse_identifier()?;
                    cols.push(col);

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }
                self.expect_token(Token::RParen)?;
                Some(cols)
            } else {
                None
            };
            vibesql_ast::TriggerEvent::Update(columns)
        } else if self.try_consume_keyword(Keyword::Delete) {
            vibesql_ast::TriggerEvent::Delete
        } else {
            return Err(ParseError {
                message: "Expected INSERT, UPDATE, or DELETE after trigger timing".to_string(),
            });
        };

        // Expect ON keyword
        self.expect_keyword(Keyword::On)?;

        // Parse table name
        let table_name = self.parse_identifier()?;

        // Parse optional FOR EACH ROW/STATEMENT
        let granularity = if self.try_consume_keyword(Keyword::For) {
            self.expect_keyword(Keyword::Each)?;
            if self.try_consume_keyword(Keyword::Row) {
                vibesql_ast::TriggerGranularity::Row
            } else if self.try_consume_keyword(Keyword::Statement) {
                vibesql_ast::TriggerGranularity::Statement
            } else {
                return Err(ParseError {
                    message: "Expected ROW or STATEMENT after FOR EACH".to_string(),
                });
            }
        } else {
            // Default to ROW for SQLite compatibility
            // SQLite doesn't support STATEMENT-level triggers - all triggers are FOR EACH ROW
            vibesql_ast::TriggerGranularity::Row
        };

        // Parse optional WHEN condition
        // SQLite syntax: WHEN expression (no parens required)
        // PostgreSQL syntax: WHEN (expression) (parens required)
        // We support both for compatibility
        let when_condition = if self.try_consume_keyword(Keyword::When) {
            let has_paren = if self.peek() == &Token::LParen {
                self.advance(); // consume optional (
                true
            } else {
                false
            };
            // The WHEN condition is part of the trigger-program, so SQLite
            // permits RAISE() there too. Mark the parser as inside a trigger
            // body for the duration of the WHEN expression so RAISE() is
            // admitted (it is rejected at parse time everywhere else).
            let prev_in_trigger_body = self.in_trigger_body;
            self.in_trigger_body = true;
            let expr_result = self.parse_expression();
            self.in_trigger_body = prev_in_trigger_body;
            let expr = expr_result?;
            if has_paren {
                self.expect_token(Token::RParen)?;
            }
            Some(Box::new(expr))
        } else {
            None
        };

        // Parse triggered action
        // For now, we'll store the action as raw SQL
        // We expect BEGIN...END block or a simple statement
        let triggered_action = self.parse_trigger_action()?;

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::CreateTriggerStmt {
            trigger_name,
            timing,
            event,
            table_name,
            granularity,
            when_condition,
            triggered_action,
        })
    }

    /// Parse triggered action (simplified: just collect tokens until semicolon or EOF)
    fn parse_trigger_action(&mut self) -> Result<vibesql_ast::TriggerAction, ParseError> {
        // Simplified implementation: store raw SQL as a string
        // A full implementation would parse procedural SQL (BEGIN...END blocks)

        // Expect BEGIN keyword
        self.expect_keyword(Keyword::Begin)?;

        let mut sql_parts = vec!["BEGIN".to_string()];
        let mut depth = 1; // Track BEGIN/END nesting

        // Collect tokens until matching END
        loop {
            match self.peek() {
                Token::Keyword { keyword: Keyword::Begin, .. } => {
                    sql_parts.push("BEGIN".to_string());
                    depth += 1;
                    self.advance();
                }
                Token::Keyword { keyword: Keyword::End, .. } => {
                    sql_parts.push("END".to_string());
                    depth -= 1;
                    self.advance();
                    if depth == 0 {
                        break;
                    }
                }
                Token::Eof => {
                    return Err(ParseError {
                        message: "Unexpected end of input in trigger action".to_string(),
                    });
                }
                token => {
                    // Convert token back to valid SQL using to_sql()
                    sql_parts.push(token.to_sql());
                    self.advance();
                }
            }
        }

        let raw_sql = sql_parts.join(" ");

        // Validate the body statements at CREATE TRIGGER time. SQLite
        // parses every trigger-body statement when the trigger is created
        // and rejects create-time errors then — e.g. `NULLS FIRST/LAST` in
        // a conflict target / index position (nulls1.test 3.1.12) — rather
        // than deferring them to fire time. VibeSQL stores the body as raw
        // SQL (`RawSql`) and previously surfaced no error until fire time.
        //
        // We re-parse each body statement with the same `parse_sql` entry
        // point the executor uses when the trigger fires
        // (`TriggerFirer::parse_trigger_sql`) and surface create-time
        // *validation* errors (the class SQLite rejects at create time).
        //
        // We deliberately do NOT make a body-statement parse failure a hard
        // gate: VibeSQL's parser does not yet support every construct that
        // is valid inside a SQLite trigger body (e.g. `RAISE(ABORT, …)`),
        // and SQLite accepts those at create time. Hard-rejecting any
        // unparseable body would regress such triggers (which today are
        // stored raw and only re-parsed at fire time). So a parse failure
        // is only propagated when it is one of the create-time rejection
        // classes SQLite also enforces (see [`Parser::is_create_time_rejection`]);
        // otherwise the body is preserved as `RawSql` as before.
        //
        // This is a *validate* pass only — no semantic / name resolution
        // (SQLite allows a body referencing a not-yet-created table), so it
        // matches SQLite's create-time strictness.
        Self::validate_trigger_body(&raw_sql)?;

        Ok(vibesql_ast::TriggerAction::RawSql(raw_sql))
    }

    /// Parse-and-validate each statement of a `BEGIN ... END` trigger body,
    /// surfacing the create-time validation errors SQLite enforces.
    ///
    /// Splits the body into statements with
    /// [`crate::split_trigger_body_statements`] — the same string-literal- and
    /// comment-aware splitter `TriggerFirer::parse_trigger_sql` in
    /// `vibesql-executor` uses at fire time, so the two paths cannot drift and
    /// neither mishandles a `;` inside a string literal — and parses each
    /// statement with [`Parser::parse_sql`]. A parse error is only propagated
    /// when it is a create-time rejection class SQLite also enforces (see
    /// [`Parser::is_create_time_rejection`]); other parse failures are
    /// tolerated so that bodies using constructs VibeSQL cannot yet parse
    /// (but SQLite accepts at create time) are preserved as before.
    fn validate_trigger_body(raw_sql: &str) -> Result<(), ParseError> {
        for stmt_sql in crate::split_trigger_body_statements(raw_sql) {
            // Parse each body statement as a trigger-program statement so
            // `RAISE()` is admitted (SQLite only permits RAISE() within a
            // trigger-program; `parse_sql` rejects it at parse time, but a
            // trigger body legitimately contains it).
            if let Err(e) = Self::parse_sql_in_trigger_body(&stmt_sql) {
                if Self::is_create_time_rejection(&e) {
                    // Propagate verbatim so the message (e.g.
                    // `unsupported use of NULLS FIRST`) matches the
                    // direct-statement form and SQLite.
                    return Err(e);
                }
                // Otherwise tolerate: a construct VibeSQL does not yet
                // parse. Preserve the prior behavior (stored raw, re-parsed
                // at fire time) so we don't reject bodies SQLite accepts.
            }
        }

        Ok(())
    }

    /// Is this body-statement parse error one that SQLite also rejects at
    /// `CREATE TRIGGER` time (rather than a VibeSQL parser gap)?
    ///
    /// Scoped to the `NULLS FIRST/LAST`-in-index-position error class
    /// (nulls1.test 3.1.12), whose message is emitted verbatim by
    /// `reject_nulls_in_index_position`. Additional create-time rejection
    /// classes can be added here as they are identified.
    fn is_create_time_rejection(error: &ParseError) -> bool {
        error.message.starts_with("unsupported use of NULLS ")
    }

    /// Parse ALTER TRIGGER statement
    ///
    /// Syntax:
    ///   ALTER TRIGGER trigger_name {ENABLE | DISABLE}
    pub(super) fn parse_alter_trigger_statement(
        &mut self,
    ) -> Result<vibesql_ast::AlterTriggerStmt, ParseError> {
        // Expect ALTER keyword
        self.expect_keyword(Keyword::Alter)?;

        // Expect TRIGGER keyword
        self.expect_keyword(Keyword::Trigger)?;

        // Parse trigger name
        let trigger_name = self.parse_identifier()?;

        // Parse action: ENABLE or DISABLE
        let action = if self.try_consume_keyword(Keyword::Enable) {
            vibesql_ast::AlterTriggerAction::Enable
        } else if self.try_consume_keyword(Keyword::Disable) {
            vibesql_ast::AlterTriggerAction::Disable
        } else {
            return Err(ParseError {
                message: "Expected ENABLE or DISABLE after trigger name".to_string(),
            });
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::AlterTriggerStmt { trigger_name, action })
    }

    /// Parse DROP TRIGGER statement
    ///
    /// Syntax:
    ///   DROP TRIGGER trigger_name [CASCADE | RESTRICT]
    pub(super) fn parse_drop_trigger_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropTriggerStmt, ParseError> {
        // Expect DROP keyword
        self.expect_keyword(Keyword::Drop)?;

        // Expect TRIGGER keyword
        self.expect_keyword(Keyword::Trigger)?;

        // Parse trigger name
        let trigger_name = self.parse_identifier()?;

        // Parse optional CASCADE or RESTRICT
        let cascade = if self.try_consume_keyword(Keyword::Cascade) {
            true
        } else if self.try_consume_keyword(Keyword::Restrict) {
            false
        } else {
            // Default to RESTRICT per SQL:1999
            false
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::DropTriggerStmt { trigger_name, cascade })
    }
}
