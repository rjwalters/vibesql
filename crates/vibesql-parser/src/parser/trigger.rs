//! CREATE TRIGGER and DROP TRIGGER statement parsers

use super::{ParseError, Parser};
use crate::{keywords::Keyword, lexer::Lexer, token::Token};

/// SQLite's verbatim error when a `CREATE TRIGGER` body (or its WHEN clause)
/// references a bound parameter / variable. A trigger program is compiled once
/// when the trigger is created and has no bind context at fire time, so SQLite
/// rejects any variable reference at create time (see triggerE.test).
const TRIGGER_CANNOT_USE_VARIABLES: &str = "trigger cannot use variables";

/// SQLite's verbatim error when a trigger-body INSERT/UPDATE/DELETE targets a
/// schema-qualified table name (e.g. `INSERT INTO main.t VALUES(...)` inside a
/// trigger body). The trigger grammar never allowed a qualifier on the DML
/// target; SQLite rejects it at create time with this message rather than a
/// bare "syntax error" (ticket #3947, trigger1-16.1..16.3).
const TRIGGER_QUALIFIED_TABLE: &str =
    "qualified table names are not allowed on INSERT, UPDATE, and DELETE statements within triggers";

/// SQLite's verbatim error when a trigger-body UPDATE/DELETE carries a
/// `NOT INDEXED` clause (trigger1-16.4/16.6).
const TRIGGER_NOT_INDEXED: &str =
    "the NOT INDEXED clause is not allowed on UPDATE or DELETE statements within triggers";

/// SQLite's verbatim error when a trigger-body UPDATE/DELETE carries an
/// `INDEXED BY <index>` clause (trigger1-16.5/16.7).
const TRIGGER_INDEXED_BY: &str =
    "the INDEXED BY clause is not allowed on UPDATE or DELETE statements within triggers";

/// Pick the verbatim SQLite rejection message for an index hint on a
/// trigger-body UPDATE/DELETE (`NOT INDEXED` vs `INDEXED BY <index>`).
fn index_hint_rejection_message(hint: &vibesql_ast::IndexHint) -> String {
    match hint {
        vibesql_ast::IndexHint::NotIndexed => TRIGGER_NOT_INDEXED.to_string(),
        vibesql_ast::IndexHint::IndexedBy(_) => TRIGGER_INDEXED_BY.to_string(),
    }
}

/// SQLite rejects an `ORDER BY` or `LIMIT` clause on an UPDATE/DELETE statement
/// inside a trigger body as a plain grammar syntax error — the trigger-program
/// grammar simply has no ORDER BY / LIMIT productions for its DML statements,
/// and this holds "regardless of the compilation options used to build SQLite"
/// (R-57359-59558 for UPDATE, R-64942-06615 for DELETE; e_update-2.5 /
/// e_delete-2.5). The offending token reported is `ORDER` when an ORDER BY is
/// present and otherwise `LIMIT` (an OFFSET can only follow a LIMIT, so a bare
/// OFFSET never occurs). Top-level UPDATE/DELETE ... LIMIT keeps working because
/// this check only runs on trigger-body statements.
fn trigger_body_limit_order_rejection(
    order_by_present: bool,
    limit_present: bool,
) -> Option<ParseError> {
    if order_by_present {
        Some(ParseError { message: "near \"ORDER\": syntax error".to_string() })
    } else if limit_present {
        Some(ParseError { message: "near \"LIMIT\": syntax error".to_string() })
    } else {
        None
    }
}

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

        // Optional TEMP/TEMPORARY modifier (SQLite places such a trigger in the
        // session `temp` schema). We record that as `schema = Some("temp")` so
        // the trigger is registered against the temp schema, mirroring how a
        // temp table/index shadows its main-schema namesake (triggerD-3.x).
        let is_temp =
            self.try_consume_keyword(Keyword::Temp) || self.try_consume_keyword(Keyword::Temporary);

        // Expect TRIGGER keyword
        self.expect_keyword(Keyword::Trigger)?;

        // Optional IF NOT EXISTS: when present, creating a trigger whose name
        // already exists is a no-op success instead of an error (SQLite).
        let if_not_exists = if self.peek_keyword(Keyword::If) {
            self.advance(); // consume IF
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse trigger name, allowing an optional schema qualifier
        // (`CREATE TRIGGER main.r1 ...` / `CREATE TRIGGER temp.r1 ...`). The
        // table-ref helper also accepts a keyword schema (`temp`, `main`).
        // Capture the verbatim source spelling of the trigger-name token before
        // `parse_table_ref` normalizes (de-quotes) it. SQLite echoes the trigger
        // name exactly as written — including its quoting form — in the
        // "trigger ... already exists" error (trigger1-1.2.2/1.2.3), whereas
        // `trigger_name` below holds the de-quoted identifier used for catalog
        // lookups. For a `schema.name` qualified reference, the trigger-name
        // token is the one following the `.` separator, so we record the cursor
        // position of the *final* identifier token consumed by the table ref.
        let name_start_pos = self.current_position();
        let name_ref = self.parse_table_ref()?;
        let trigger_name = name_ref.name;
        // The trigger-name identifier is the last token before the cursor now.
        // (`schema . name` consumes three tokens; `name` consumes one.)
        let name_token_pos = self.current_position().saturating_sub(1).max(name_start_pos);
        let name_source = self.token_source_at(name_token_pos).map(str::to_string);

        // Resolve the trigger's schema. An explicit `schema.` prefix wins; if
        // absent, the `TEMP`/`TEMPORARY` modifier implies the `temp` schema.
        // SQLite forbids combining `CREATE TEMP TRIGGER` with an explicit
        // non-temp schema (e.g. `CREATE TEMP TRIGGER main.r1`); reject that.
        let schema = match (is_temp, name_ref.schema_name) {
            (_, Some(schema_name)) => {
                if is_temp && !schema_name.eq_ignore_ascii_case("temp") {
                    // SQLite emits this exact message (no name suffix) for
                    // `CREATE TEMP TRIGGER main.r1 ...` (trigger7-1.1).
                    return Err(ParseError {
                        message: "temporary trigger may not have qualified name".to_string(),
                    });
                }
                // VibeSQL exposes only the `main` and `temp` schemas (no ATTACH),
                // so a trigger name qualified with any other database is an
                // "unknown database <name>" prepare-time error, matching SQLite
                // (trigger7-1.2). `temporary` is accepted as an alias of `temp`.
                if !schema_name.eq_ignore_ascii_case("main")
                    && !schema_name.eq_ignore_ascii_case("temp")
                    && !schema_name.eq_ignore_ascii_case("temporary")
                {
                    return Err(ParseError {
                        message: format!("unknown database {}", schema_name),
                    });
                }
                Some(schema_name)
            }
            (true, None) => Some("temp".to_string()),
            (false, None) => None,
        };

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
            //
            // Standard SQLite (3.51.0) uses an UNPARENTHESIZED comma-separated
            // column list terminated by ON:
            //   CREATE TRIGGER tr AFTER UPDATE OF c, d ON tbl ...
            // SQLite itself rejects the parenthesized form `UPDATE OF (c, d)`.
            // VibeSQL accepts BOTH: the standard unparenthesized form for
            // conformance (trigger2-3.x), and the parenthesized form as a
            // lenient extension (it was previously the only accepted form).
            let columns = if self.try_consume_keyword(Keyword::Of) {
                let mut cols = Vec::new();
                // Optional opening paren (lenient extension; not valid SQLite).
                let has_paren = if matches!(self.peek(), Token::LParen) {
                    self.advance();
                    true
                } else {
                    false
                };
                loop {
                    let col = self.parse_identifier()?;
                    cols.push(col);

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }
                if has_paren {
                    self.expect_token(Token::RParen)?;
                }
                // For the unparenthesized form the column list runs until ON,
                // which is consumed by `expect_keyword(On)` further below.
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

        // Parse table name, allowing an optional schema qualifier
        // (`CREATE TRIGGER tr1 ... ON main.t1 ...` / `... ON temp.t1 ...`).
        // SQLite accepts a schema-qualified trigger target (e_delete-2.2.1.1 /
        // e_update-2.1.1: `CREATE TRIGGER main.tr1 AFTER INSERT ON main.t7 ...`);
        // previously this used `parse_identifier()`, which cannot consume the
        // `schema.` prefix and hit a bare `near ".": syntax error` on any
        // qualified ON target. `table_name` downstream is looked up via
        // `Database::get_table`/`catalog.table_exists`, which already
        // special-cases an explicit `temp.` prefix (mapping it to the session's
        // temp schema) but has no separate "main" schema namespace — main
        // tables are stored unqualified — so a `main.` qualifier is stripped
        // here (dropping it is a no-op for resolution) while a `temp.`
        // qualifier and any other schema name (e.g. `aux.` — unsupported,
        // ATTACH-blocked) are preserved verbatim so a missing/foreign-schema
        // target still resolves (or fails) using its own qualified name rather
        // than silently colliding with a same-named main-schema table.
        let on_table_ref = self.parse_table_ref()?;
        let table_name = match &on_table_ref.schema_name {
            Some(schema) if schema.eq_ignore_ascii_case("main") => on_table_ref.name.clone(),
            // Normalize to lowercase "temp." — the `temp` keyword renders as
            // "TEMP" via `Keyword::to_string()`, but the rest of the codebase
            // (e.g. `CreateTriggerStmt::schema`, `Database::get_table`'s own
            // `temp.` special-case) conventionally spells it lowercase.
            // `get_table` compares case-insensitively either way, so this only
            // affects the round-tripped spelling, not resolution.
            Some(schema) if schema.eq_ignore_ascii_case("temp") => {
                format!("temp.{}", on_table_ref.name)
            }
            Some(_) => on_table_ref.full_name(),
            None => on_table_ref.name,
        };

        // Parse optional FOR EACH ROW.
        //
        // SQLite's grammar only accepts `FOR EACH ROW`; there are no
        // statement-level triggers. Anything else after `FOR EACH` (most
        // notably `STATEMENT`) is a syntax error in SQLite — e.g.
        // `near "STATEMENT": syntax error` (trigger1-1.1.3). We surface the
        // same `near "<tok>": syntax error` so the rejection matches SQLite
        // rather than silently accepting an unsupported granularity.
        let granularity = if self.try_consume_keyword(Keyword::For) {
            self.expect_keyword(Keyword::Each)?;
            if self.try_consume_keyword(Keyword::Row) {
                vibesql_ast::TriggerGranularity::Row
            } else {
                return Err(ParseError { message: self.peek().syntax_error() });
            }
        } else {
            // Default to ROW for SQLite compatibility (all SQLite triggers are
            // FOR EACH ROW, whether or not the clause is written).
            vibesql_ast::TriggerGranularity::Row
        };

        // Parse optional WHEN condition
        // SQLite syntax: WHEN expression (no parens required)
        // PostgreSQL syntax: WHEN (expression) (parens required)
        //
        // We delegate to the full expression parser, which natively handles
        // both forms: a bare condition (`WHEN new.a > 20`), a parenthesized
        // condition (`WHEN (new.a > 20)`), AND a leading subquery
        // (`WHEN (SELECT count(*) FROM tbl) = 0`, trigger2-3.2). Consuming the
        // opening paren ourselves as an optional PostgreSQL-style wrapper would
        // mis-handle the subquery case — the `(` belongs to the subquery, so
        // stripping it leaves `SELECT ... ) = 0`, which the expression parser
        // (rightly) rejects. Letting `parse_expression` see the `(` lets its
        // scalar-subquery / parenthesized-expression handling do the right thing.
        let when_condition = if self.try_consume_keyword(Keyword::When) {
            // The WHEN condition is part of the trigger-program, so SQLite
            // permits RAISE() there too. Mark the parser as inside a trigger
            // body for the duration of the WHEN expression so RAISE() is
            // admitted (it is rejected at parse time everywhere else).
            let prev_in_trigger_body = self.in_trigger_body;
            self.in_trigger_body = true;
            let expr_result = self.parse_expression();
            self.in_trigger_body = prev_in_trigger_body;
            let expr = expr_result?;
            // SQLite rejects a bound parameter / variable in the WHEN clause at
            // create time (triggerE-1.1.1: `WHEN new.a = ?`). NEW/OLD column
            // references are *not* variables and must still be allowed.
            if Self::expression_references_variable(&expr) {
                return Err(ParseError { message: TRIGGER_CANNOT_USE_VARIABLES.to_string() });
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
            if_not_exists,
            schema,
            trigger_name,
            name_source,
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
        // Track block nesting. Both `BEGIN ... END` and `CASE ... END` open a
        // block that `END` closes. The trigger body's terminating `END` is the
        // one that brings the depth back to 0; a `CASE ... END` *expression*
        // inside a body statement (e.g.
        // `SELECT CASE WHEN NEW.id = 1 THEN raise(IGNORE) END;`) must not be
        // mistaken for it, or the body is truncated at the CASE's `END`.
        let mut depth = 1;

        // Collect tokens until the matching body-terminator END.
        loop {
            match self.peek() {
                Token::Keyword { keyword: Keyword::Begin, .. } => {
                    sql_parts.push("BEGIN".to_string());
                    depth += 1;
                    self.advance();
                }
                Token::Keyword { keyword: Keyword::Case, .. } => {
                    // A CASE expression opens a block its own END closes.
                    sql_parts.push("CASE".to_string());
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
        // Reject any bound parameter / variable anywhere in the body. SQLite
        // rejects `CREATE TRIGGER` at create time when a body statement
        // references a bound parameter (`?`, `?NNN`, `:name`, `@name`, `$name`,
        // `$NNN`) — in INSERT VALUES, UPDATE SET, WHERE, SELECT, GROUP BY,
        // ORDER BY, LIMIT, function args, window ORDER BY, etc. — because a
        // trigger program is compiled once and has no bind context (triggerE.test
        // 1.1.x / 1.2.x). We scan tokens rather than the parsed AST so the check
        // is independent of whether VibeSQL can fully parse the body statement
        // (some valid SQLite body constructs are tolerated as `RawSql`), and so
        // every position is covered uniformly. NEW/OLD references tokenize as
        // ordinary identifiers, never as variable tokens, so they are not caught.
        if Self::trigger_body_text_references_variable(raw_sql) {
            return Err(ParseError { message: TRIGGER_CANNOT_USE_VARIABLES.to_string() });
        }

        for stmt_sql in crate::split_trigger_body_statements(raw_sql) {
            // Re-append the statement terminator the splitter stripped. SQLite
            // parses the whole body in one pass, so a statement that ends
            // prematurely (e.g. `SELECT * FROM`) reports the *following* `;` as
            // the offending token (`near ";": syntax error`, trigger1-2.1 /
            // 2.2). Parsing the split statement alone would instead hit EOF and
            // report `incomplete input`; restoring the `;` makes the offending
            // token — and thus the message — match SQLite. A `;` after a valid
            // statement is harmless (parsing stops at the statement boundary).
            let stmt_with_terminator = format!("{stmt_sql};");

            // Parse each body statement as a trigger-program statement so
            // `RAISE()` is admitted (SQLite only permits RAISE() within a
            // trigger-program; `parse_sql` rejects it at parse time, but a
            // trigger body legitimately contains it).
            match Self::parse_sql_in_trigger_body(&stmt_with_terminator) {
                Ok(stmt) => Self::validate_trigger_body_statement(&stmt)?,
                Err(e) => {
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
        }

        Ok(())
    }

    /// Reject the create-time-invalid constructs SQLite disallows on a
    /// trigger-body INSERT/UPDATE/DELETE statement (ticket #3947, trigger1-16.x):
    ///
    /// 1. A schema-qualified DML target (`INSERT INTO main.t ...`,
    ///    `UPDATE main.t ...`, `DELETE FROM main.t ...`). The trigger grammar
    ///    never permitted a schema qualifier on the DML target — the whole
    ///    trigger program runs in the schema of the trigger's own table — so
    ///    SQLite rejects it with a full message rather than a bare syntax error.
    /// 2. An `INDEXED BY <index>` / `NOT INDEXED` clause on a body UPDATE/DELETE,
    ///    which SQLite likewise forbids inside a trigger program.
    ///
    /// Only runs on statements VibeSQL parsed successfully, so bodies using
    /// constructs VibeSQL cannot yet parse are still tolerated as before.
    fn validate_trigger_body_statement(stmt: &vibesql_ast::Statement) -> Result<(), ParseError> {
        use vibesql_ast::Statement;
        match stmt {
            Statement::Insert(insert) => {
                // INSERT keeps the schema qualifier in a dedicated field.
                if insert.schema_name.is_some() {
                    return Err(ParseError { message: TRIGGER_QUALIFIED_TABLE.to_string() });
                }
                // The "DEFAULT VALUES" form of INSERT is supported for top-level
                // INSERT statements only, never inside a trigger program — the
                // trigger-body grammar has no DEFAULT VALUES production, so SQLite
                // reports a plain syntax error at the `DEFAULT` token regardless of
                // compilation options (R-15888-36326; e_insert-5.2.1). Top-level
                // `INSERT INTO t DEFAULT VALUES` keeps working because this check
                // only runs on trigger-body statements.
                if matches!(insert.source, vibesql_ast::InsertSource::DefaultValues) {
                    return Err(ParseError {
                        message: "near \"DEFAULT\": syntax error".to_string(),
                    });
                }
            }
            Statement::Update(update) => {
                if Self::dml_target_is_schema_qualified(&update.table_name, update.quoted) {
                    return Err(ParseError { message: TRIGGER_QUALIFIED_TABLE.to_string() });
                }
                if let Some(hint) = &update.index_hint {
                    return Err(ParseError { message: index_hint_rejection_message(hint) });
                }
                if let Some(e) = trigger_body_limit_order_rejection(
                    update.order_by.is_some(),
                    update.limit.is_some(),
                ) {
                    return Err(e);
                }
            }
            Statement::Delete(delete) => {
                if Self::dml_target_is_schema_qualified(&delete.table_name, delete.quoted) {
                    return Err(ParseError { message: TRIGGER_QUALIFIED_TABLE.to_string() });
                }
                if let Some(hint) = &delete.index_hint {
                    return Err(ParseError { message: index_hint_rejection_message(hint) });
                }
                if let Some(e) = trigger_body_limit_order_rejection(
                    delete.order_by.is_some(),
                    delete.limit.is_some(),
                ) {
                    return Err(e);
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Is an UPDATE/DELETE target name schema-qualified (`schema.table`)?
    ///
    /// UPDATE/DELETE flatten the qualifier into `table_name` as `schema.table`
    /// (see [`vibesql_ast::TableRef::full_name`]). A dot only signals a schema
    /// qualifier for an *unquoted* name — a quoted identifier may legitimately
    /// contain a dot (`"weird.name"`) and is not a schema reference — so the
    /// quoted form is deliberately left alone to avoid false positives.
    fn dml_target_is_schema_qualified(table_name: &str, quoted: bool) -> bool {
        !quoted && table_name.contains('.')
    }

    /// Is this body-statement parse error one that SQLite also rejects at
    /// `CREATE TRIGGER` time (rather than a VibeSQL parser gap)?
    ///
    /// Two classes are propagated:
    ///
    /// 1. The `NULLS FIRST/LAST`-in-index-position error class (nulls1.test
    ///    3.1.12), whose message is emitted verbatim by
    ///    `reject_nulls_in_index_position`.
    /// 2. Token-level syntax errors (`near "X": syntax error`). A genuine
    ///    syntax error in a body statement — e.g. `SELECT * FROM;`
    ///    (trigger1-2.1 / 2.2) — is rejected by SQLite at create time, so we
    ///    surface it too. This is deliberately distinguished from VibeSQL
    ///    *parser gaps*, which produce descriptive messages (e.g. "Expected
    ///    ...") rather than the `near "X": syntax error` form; those are still
    ///    tolerated so we don't reject bodies SQLite accepts but VibeSQL
    ///    cannot yet parse.
    ///
    /// Additional create-time rejection classes can be added here as they are
    /// identified.
    fn is_create_time_rejection(error: &ParseError) -> bool {
        error.message.starts_with("unsupported use of NULLS ")
            || (error.message.starts_with("near \"") && error.message.ends_with("\": syntax error"))
    }

    /// Does the trigger-body SQL text contain a bound-parameter / variable token?
    ///
    /// Lexes the raw body (the `BEGIN ... END` block as a string) and returns
    /// `true` if any token is a placeholder or variable: `?` ([`Token::Placeholder`]),
    /// `?NNN` / `$NNN` ([`Token::NumberedPlaceholder`]), `:name` / `$name`
    /// ([`Token::NamedPlaceholder`]), `@name` ([`Token::UserVariable`]) or
    /// `@@name` ([`Token::SessionVariable`]).
    ///
    /// A lexer error is treated as "no variable found": we are only adding a
    /// create-time rejection for the variable class here, and an unlexable body
    /// is handled (and reported, if appropriate) by the per-statement parse pass
    /// below — we must not turn a lex failure into the variable-specific error.
    fn trigger_body_text_references_variable(raw_sql: &str) -> bool {
        let mut lexer = Lexer::new(raw_sql);
        match lexer.tokenize() {
            Ok(tokens) => tokens.iter().any(Self::token_is_variable),
            Err(_) => false,
        }
    }

    /// Is this token a bound parameter / variable reference (not a NEW/OLD
    /// pseudo-column or any other identifier)?
    fn token_is_variable(token: &Token) -> bool {
        matches!(
            token,
            Token::Placeholder
                | Token::NumberedPlaceholder(_)
                | Token::NamedPlaceholder(_)
                | Token::UserVariable(_)
                | Token::SessionVariable(_)
        )
    }

    /// Does this (already-parsed) expression reference a bound parameter /
    /// variable anywhere in its tree? Used for the WHEN clause, which is parsed
    /// into an AST before the body is collected as raw SQL. NEW/OLD references
    /// are [`vibesql_ast::Expression::PseudoVariable`] and deliberately ignored.
    fn expression_references_variable(expr: &vibesql_ast::Expression) -> bool {
        let mut found = false;
        vibesql_ast::visitor::walk_expression(
            &mut VariableExpressionFinder { found: &mut found },
            expr,
        );
        found
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
    ///   DROP TRIGGER [IF EXISTS] trigger_name [CASCADE | RESTRICT]
    pub(super) fn parse_drop_trigger_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropTriggerStmt, ParseError> {
        // Expect DROP keyword
        self.expect_keyword(Keyword::Drop)?;

        // Expect TRIGGER keyword
        self.expect_keyword(Keyword::Trigger)?;

        // Optional IF EXISTS (SQLite / SQL:2008): dropping a missing trigger
        // becomes a no-op instead of an error.
        let if_exists = if self.peek_keyword(Keyword::If) {
            self.advance(); // consume IF
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse trigger name. SQLite accepts a single-quoted string literal as
        // the trigger name in DROP TRIGGER (e.g. `DROP TRIGGER 'tr'`), so accept
        // a string token in addition to bare / delimited identifiers.
        let trigger_name = if let Token::String(s) = self.peek() {
            let name = s.clone();
            self.advance();
            name
        } else {
            self.parse_identifier()?
        };

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

        Ok(vibesql_ast::DropTriggerStmt { trigger_name, cascade, if_exists })
    }
}

/// Expression visitor that records whether any bound-parameter / variable node
/// is present. Used to validate the WHEN clause of a `CREATE TRIGGER`. NEW/OLD
/// references are `Expression::PseudoVariable` and are intentionally not matched.
struct VariableExpressionFinder<'a> {
    found: &'a mut bool,
}

impl vibesql_ast::visitor::ExpressionVisitor for VariableExpressionFinder<'_> {
    fn pre_visit_expression(
        &mut self,
        expr: &vibesql_ast::Expression,
    ) -> vibesql_ast::visitor::VisitResult {
        use vibesql_ast::Expression;
        if matches!(
            expr,
            Expression::Placeholder(_)
                | Expression::NumberedPlaceholder(_)
                | Expression::NamedPlaceholder(_)
                | Expression::SessionVariable { .. }
        ) {
            *self.found = true;
            return vibesql_ast::visitor::VisitResult::Stop;
        }
        vibesql_ast::visitor::VisitResult::Continue
    }
}
