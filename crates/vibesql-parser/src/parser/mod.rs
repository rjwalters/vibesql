use std::fmt;

use crate::{
    keywords::Keyword,
    lexer::{Lexer, Span},
    token::Token,
};

/// Maximum number of terms allowed in an ORDER BY clause.
/// This matches SQLite's SQLITE_MAX_COLUMN default of 2000.
/// See: https://www.sqlite.org/limits.html
pub const MAX_ORDER_BY_TERMS: usize = 2000;

mod advanced_objects;
mod alter;
mod attach;
mod create;
mod cursor;
mod delete;
mod domain;
mod drop;
mod expressions;
mod grant;
mod helpers;
mod index;
mod insert;
mod introspection;
mod prepared;
mod revoke;
mod role;
mod schema;
mod select;
mod table_options;
mod transaction;
mod trigger;
mod truncate;
mod update;
mod view;

/// Parser error
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Parse error: {}", self.message)
    }
}

/// SQL Parser - converts tokens into AST
pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
    /// Original SQL source, retained so that selected constructs can recover the
    /// *verbatim* source spelling of a token (e.g. a delimited identifier with
    /// its original quoting). Empty when the parser was built directly from a
    /// token vector via [`Parser::new`] (in which case source recovery is
    /// unavailable and callers fall back to the normalized token text).
    source: String,
    /// Byte spans into `source`, one per entry in `tokens`. Empty (and ignored)
    /// when `source` is empty.
    spans: Vec<Span>,
    /// Counter for placeholder parameters (?)
    /// Incremented each time a placeholder is parsed, providing 0-indexed parameter positions
    placeholder_count: usize,
    /// Whether the parser is currently inside a `CREATE TRIGGER` body
    /// (a trigger-program). SQLite only permits the `RAISE()` expression
    /// within a trigger-program and rejects it at prepare/parse time
    /// elsewhere; this flag lets [`Parser::parse_raise_expression`] accept
    /// `RAISE()` inside a trigger body and reject it everywhere else,
    /// matching sqlite3 (`RAISE() may only be used within a trigger-program`).
    in_trigger_body: bool,
    /// Verbatim source text of each CREATE TABLE column's `DEFAULT` expression,
    /// as `(lowercase column name, source text)` pairs in declaration order.
    /// Populated only when the parser was built with source/span info
    /// ([`Parser::new_with_source`]). `PRAGMA table_info` reads this (via
    /// [`Parser::parse_sql_with_default_sources`]) to echo the original default
    /// spelling — e.g. `X'abcdef'`, `'abcde'`, `-1` — rather than a lossy
    /// `ToSql` re-render (which uppercases blob hex and drops operator spacing).
    column_default_sources: Vec<(String, String)>,
    /// Set by [`Parser::parse_table_constraint`] when it parses a table-level
    /// `PRIMARY KEY (col-name [, ...] AUTOINCREMENT)` constraint whose column
    /// list has exactly one column — SQLite's alternate syntax for declaring
    /// AUTOINCREMENT "inside the parentheses on a separate PRIMARY KEY
    /// designation" (autoinc-7.1), equivalent to `col-name INTEGER PRIMARY KEY
    /// AUTOINCREMENT` as a column constraint. Consumed by the CREATE TABLE
    /// column loop (issue #6173), which folds it into that column's
    /// constraint list so the rest of the pipeline (rowid-alias detection,
    /// AUTOINCREMENT validation in vibesql-executor) sees a single uniform
    /// shape regardless of which syntax the user wrote. Cleared to `None`
    /// after being read; left `None` when AUTOINCREMENT was absent or the
    /// column list had more than one column (in which case the AUTOINCREMENT
    /// keyword is simply not consumed here and parsing proceeds normally,
    /// surfacing as a syntax error — a composite-key AUTOINCREMENT has no
    /// documented meaning and no test exercises it).
    pending_table_pk_autoincrement_column: Option<String>,
}

impl Parser {
    /// Create a new parser from tokens.
    ///
    /// Source recovery (verbatim token text via [`Parser::current_token_source`])
    /// is unavailable on parsers built this way; use [`Parser::new_with_source`]
    /// when the original SQL must be recoverable.
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser {
            tokens,
            position: 0,
            placeholder_count: 0,
            in_trigger_body: false,
            source: String::new(),
            spans: Vec::new(),
            column_default_sources: Vec::new(),
            pending_table_pk_autoincrement_column: None,
        }
    }

    /// Create a new parser from tokens together with their byte spans and the
    /// original SQL source. This enables [`Parser::current_token_source`] to
    /// return the verbatim source text of a token (e.g. a delimited identifier
    /// with its original quoting), which SQLite preserves in some error
    /// messages.
    pub fn new_with_source(tokens: Vec<Token>, spans: Vec<Span>, source: String) -> Self {
        Parser {
            tokens,
            position: 0,
            placeholder_count: 0,
            in_trigger_body: false,
            source,
            spans,
            column_default_sources: Vec::new(),
            pending_table_pk_autoincrement_column: None,
        }
    }

    /// Current token index (cursor position within the token stream).
    pub(crate) fn current_position(&self) -> usize {
        self.position
    }

    /// Return the verbatim source text of the token at index `pos`, including
    /// any surrounding delimiters. Returns `None` when span/source information
    /// is not available (parsers built via [`Parser::new`]) or `pos` is out of
    /// range.
    pub(crate) fn token_source_at(&self, pos: usize) -> Option<&str> {
        if self.source.is_empty() {
            return None;
        }
        let span = self.spans.get(pos)?;
        self.source.get(span.start..span.end)
    }

    /// Return the verbatim source text spanning the tokens in the half-open
    /// index range `[start, end)`, preserving the original spelling, casing, and
    /// any embedded whitespace (e.g. `TEXT(50)`, `DOUBLE PRECISION`). Returns
    /// `None` when span/source information is unavailable (parsers built via
    /// [`Parser::new`]) or the range is empty/out of bounds.
    pub(crate) fn source_between(&self, start: usize, end: usize) -> Option<String> {
        if self.source.is_empty() || end <= start {
            return None;
        }
        let start_span = self.spans.get(start)?;
        let end_span = self.spans.get(end - 1)?;
        self.source.get(start_span.start..end_span.end).map(str::to_string)
    }

    /// Return the verbatim source text that sits *between* the two delimiter
    /// tokens at indices `open` and `close` (exclusive of both), trimmed of
    /// leading and trailing ASCII whitespace but otherwise byte-for-byte from
    /// the original source.
    ///
    /// This is used to recover a CHECK constraint expression's original
    /// spelling from `CHECK ( <here> )`: SQLite echoes exactly these bytes
    /// (interior spacing and comments preserved, outer whitespace trimmed) in
    /// its "CHECK constraint failed: <expr>" message. Returns `None` when
    /// span/source information is unavailable (parsers built via
    /// [`Parser::new`]) or the delimiter indices are out of range.
    pub(crate) fn source_inside_delimiters(&self, open: usize, close: usize) -> Option<String> {
        if self.source.is_empty() || close <= open {
            return None;
        }
        let open_span = self.spans.get(open)?;
        let close_span = self.spans.get(close)?;
        self.source.get(open_span.end..close_span.start).map(|s| s.trim().to_string())
    }

    /// Parse a comma-separated list of items using a provided parser function
    ///
    /// This is a generic helper that consolidates the common pattern of parsing
    /// comma-separated lists throughout the parser (e.g., GROUP BY expressions,
    /// ORDER BY items, identifier lists, etc.)
    ///
    /// # Arguments
    /// * `parse_item` - Closure that parses a single item of type T
    ///
    /// # Returns
    /// * `Ok(Vec<T>)` - Successfully parsed list of items
    /// * `Err(ParseError)` - Error parsing an item
    ///
    /// # Example
    /// ```text
    /// // Parse comma-separated expressions (for GROUP BY)
    /// let exprs = self.parse_comma_separated_list(|p| p.parse_expression())?;
    ///
    /// // Parse comma-separated identifiers
    /// let ids = self.parse_comma_separated_list(|p| p.parse_identifier())?;
    /// ```
    pub fn parse_comma_separated_list<T, F>(&mut self, parse_item: F) -> Result<Vec<T>, ParseError>
    where
        F: Fn(&mut Self) -> Result<T, ParseError>,
    {
        let mut items = Vec::new();

        // Parse first item
        items.push(parse_item(self)?);

        // Parse remaining items (preceded by commas)
        while matches!(self.peek(), Token::Comma) {
            self.advance(); // consume comma
            items.push(parse_item(self)?);
        }

        Ok(items)
    }

    /// Parse SQL input string into a Statement
    pub fn parse_sql(input: &str) -> Result<vibesql_ast::Statement, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens_with_spans = lexer
            .tokenize_with_spans()
            .map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;
        let hints = lexer.take_hints();
        let (tokens, spans): (Vec<Token>, Vec<Span>) = tokens_with_spans.into_iter().unzip();
        let leading_hints = crate::leading_select_hints(&tokens, &spans, &hints);

        let mut parser = Parser::new_with_source(tokens, spans, input.to_string());
        let mut stmt = parser.parse_statement()?;
        if let vibesql_ast::Statement::Select(ref mut select_stmt) = stmt {
            select_stmt.hints = leading_hints;
        }
        Ok(stmt)
    }

    /// Parse SQL and, for a `CREATE TABLE`, also return the verbatim source text
    /// of each column's `DEFAULT` expression, keyed by lowercase column name.
    ///
    /// `PRAGMA table_info` uses this so the `dflt_value` column echoes the
    /// original default spelling (e.g. `X'abcdef'`, `'abcde'`, `-1`,
    /// `CURRENT_TIME`) exactly as written in the CREATE TABLE statement, which
    /// SQLite does. A plain `expr.to_sql()` re-render is lossy here: it
    /// uppercases blob-literal hex (`x'ABCDEF'`) and normalizes operator
    /// spacing. The map is empty for non-`CREATE TABLE` input or columns without
    /// a DEFAULT.
    pub fn parse_sql_with_default_sources(
        input: &str,
    ) -> Result<(vibesql_ast::Statement, std::collections::HashMap<String, String>), ParseError>
    {
        let mut lexer = Lexer::new(input);
        let tokens_with_spans = lexer
            .tokenize_with_spans()
            .map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;
        let (tokens, spans): (Vec<Token>, Vec<Span>) = tokens_with_spans.into_iter().unzip();

        let mut parser = Parser::new_with_source(tokens, spans, input.to_string());
        let stmt = parser.parse_statement()?;
        let map = parser.column_default_sources.iter().cloned().collect();
        Ok((stmt, map))
    }

    /// Parse a single SQL statement that originates from a `CREATE TRIGGER`
    /// body (a trigger-program).
    ///
    /// This is identical to [`Parser::parse_sql`] except that the parser is
    /// marked as being inside a trigger body, so the `RAISE()` expression is
    /// accepted (SQLite only permits `RAISE()` within a trigger-program and
    /// rejects it at parse time everywhere else). Both the create-time
    /// validation pass ([`Parser::validate_trigger_body`]) and the executor's
    /// fire-time re-parse (`TriggerFirer::parse_trigger_sql`) must use this
    /// entry point so a trigger body containing `RAISE()` is admitted on both
    /// paths.
    pub fn parse_sql_in_trigger_body(input: &str) -> Result<vibesql_ast::Statement, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens_with_spans = lexer
            .tokenize_with_spans()
            .map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;
        let (tokens, spans): (Vec<Token>, Vec<Span>) = tokens_with_spans.into_iter().unzip();

        let mut parser = Parser::new_with_source(tokens, spans, input.to_string());
        parser.in_trigger_body = true;
        parser.parse_statement()
    }

    /// Parse a standalone SQL expression using the full main-parser grammar.
    ///
    /// Reload paths (binary catalog expression indexes, partial-index WHERE
    /// clauses) persist expressions as `ToSql`-rendered text and must re-parse
    /// them on load. The arena parser's `parse_expression_to_owned` covers a
    /// smaller grammar (e.g. it rejects `COLLATE`), so anything the main
    /// parser accepted at DDL time — and therefore anything `ToSql` can render
    /// — must be re-parsed through this entry point, not the arena one
    /// (issue #5833).
    ///
    /// Trailing tokens after the expression are an error: a partially-consumed
    /// input means the rendered text did not round-trip as a single
    /// expression.
    pub fn parse_expression_sql(input: &str) -> Result<vibesql_ast::Expression, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens_with_spans = lexer
            .tokenize_with_spans()
            .map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;
        let (tokens, spans): (Vec<Token>, Vec<Span>) = tokens_with_spans.into_iter().unzip();

        let mut parser = Parser::new_with_source(tokens, spans, input.to_string());
        let expr = parser.parse_expression()?;
        // A single trailing `;` is a benign terminator, but anything after it
        // means the rendered text did not round-trip as one expression (e.g. a
        // corrupt/hand-crafted catalog blob smuggling `x; DROP TABLE t`). Fail
        // closed: consume one semicolon, then require EOF (issue #5866).
        match parser.peek() {
            Token::Eof => Ok(expr),
            Token::Semicolon => {
                parser.advance(); // consume the terminating semicolon
                match parser.peek() {
                    Token::Eof => Ok(expr),
                    other => Err(ParseError {
                        message: format!(
                            "Unexpected trailing token {:?} after expression in '{}'",
                            other, input
                        ),
                    }),
                }
            }
            other => Err(ParseError {
                message: format!(
                    "Unexpected trailing token {:?} after expression in '{}'",
                    other, input
                ),
            }),
        }
    }

    /// Parse a statement
    pub fn parse_statement(&mut self) -> Result<vibesql_ast::Statement, ParseError> {
        match self.peek() {
            Token::Keyword { keyword: Keyword::Select, .. } => {
                let select_stmt = self.parse_select_statement()?;
                Ok(vibesql_ast::Statement::Select(Box::new(select_stmt)))
            }
            Token::Keyword { keyword: Keyword::With, .. } => {
                // WITH can precede SELECT, INSERT, UPDATE, or DELETE
                // Parse the CTE list first, then check what statement follows
                self.parse_with_statement()
            }
            Token::Keyword { keyword: Keyword::Values, .. } => {
                let select_stmt = self.parse_values_statement()?;
                Ok(vibesql_ast::Statement::Select(Box::new(select_stmt)))
            }
            Token::Keyword { keyword: Keyword::Insert, .. } => {
                let insert_stmt = self.parse_insert_statement()?;
                Ok(vibesql_ast::Statement::Insert(insert_stmt))
            }
            Token::Keyword { keyword: Keyword::Replace, .. } => {
                let insert_stmt = self.parse_replace_statement()?;
                Ok(vibesql_ast::Statement::Insert(insert_stmt))
            }
            Token::Keyword { keyword: Keyword::Update, .. } => {
                let update_stmt = self.parse_update_statement()?;
                Ok(vibesql_ast::Statement::Update(update_stmt))
            }
            Token::Keyword { keyword: Keyword::Delete, .. } => {
                let delete_stmt = self.parse_delete_statement()?;
                Ok(vibesql_ast::Statement::Delete(delete_stmt))
            }
            Token::Keyword { keyword: Keyword::Create, .. } => {
                // Check for CREATE OR REPLACE VIEW and CREATE OR REPLACE TEMP/TEMPORARY VIEW
                if self.peek_next_keyword(Keyword::Or)
                    && matches!(
                        self.peek_at_offset(2),
                        Token::Keyword { keyword: Keyword::Replace, .. }
                    )
                {
                    // Could be CREATE OR REPLACE VIEW or CREATE OR REPLACE TEMP/TEMPORARY VIEW
                    if matches!(
                        self.peek_at_offset(3),
                        Token::Keyword { keyword: Keyword::View, .. }
                    ) || matches!(
                        self.peek_at_offset(3),
                        Token::Keyword { keyword: Keyword::Temp, .. }
                    ) || matches!(
                        self.peek_at_offset(3),
                        Token::Keyword { keyword: Keyword::Temporary, .. }
                    ) {
                        return Ok(vibesql_ast::Statement::CreateView(
                            self.parse_create_view_statement()?,
                        ));
                    }
                }
                if self.peek_next_keyword(Keyword::Table) {
                    Ok(vibesql_ast::Statement::CreateTable(self.parse_create_table_statement()?))
                } else if self.peek_next_keyword(Keyword::Schema) {
                    Ok(vibesql_ast::Statement::CreateSchema(self.parse_create_schema_statement()?))
                } else if self.peek_next_keyword(Keyword::Role) {
                    Ok(vibesql_ast::Statement::CreateRole(self.parse_create_role_statement()?))
                } else if self.peek_next_keyword(Keyword::Domain) {
                    Ok(vibesql_ast::Statement::CreateDomain(self.parse_create_domain_statement()?))
                } else if self.peek_next_keyword(Keyword::Sequence) {
                    Ok(vibesql_ast::Statement::CreateSequence(
                        self.parse_create_sequence_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Type) {
                    Ok(vibesql_ast::Statement::CreateType(self.parse_create_type_statement()?))
                } else if self.peek_next_keyword(Keyword::Collation) {
                    Ok(vibesql_ast::Statement::CreateCollation(
                        self.parse_create_collation_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Character) {
                    Ok(vibesql_ast::Statement::CreateCharacterSet(
                        self.parse_create_character_set_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Translation) {
                    Ok(vibesql_ast::Statement::CreateTranslation(
                        self.parse_create_translation_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::View) {
                    Ok(vibesql_ast::Statement::CreateView(self.parse_create_view_statement()?))
                } else if self.peek_next_keyword(Keyword::Temp)
                    || self.peek_next_keyword(Keyword::Temporary)
                {
                    // CREATE TEMP/TEMPORARY followed by TABLE, TRIGGER, or VIEW
                    // Check offset 2 to determine which object kind follows the modifier
                    if matches!(
                        self.peek_at_offset(2),
                        Token::Keyword { keyword: Keyword::Table, .. }
                    ) {
                        Ok(vibesql_ast::Statement::CreateTable(
                            self.parse_create_table_statement()?,
                        ))
                    } else if matches!(
                        self.peek_at_offset(2),
                        Token::Keyword { keyword: Keyword::Trigger, .. }
                    ) {
                        Ok(vibesql_ast::Statement::CreateTrigger(
                            self.parse_create_trigger_statement()?,
                        ))
                    } else {
                        Ok(vibesql_ast::Statement::CreateView(self.parse_create_view_statement()?))
                    }
                } else if self.peek_next_keyword(Keyword::Trigger) {
                    Ok(vibesql_ast::Statement::CreateTrigger(
                        self.parse_create_trigger_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Index)
                    || self.peek_next_keyword(Keyword::Unique)
                    || self.peek_next_keyword(Keyword::Fulltext)
                    || self.peek_next_keyword(Keyword::Spatial)
                {
                    Ok(vibesql_ast::Statement::CreateIndex(self.parse_create_index_statement()?))
                } else if self.peek_next_keyword(Keyword::Assertion) {
                    Ok(vibesql_ast::Statement::CreateAssertion(
                        self.parse_create_assertion_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Procedure) {
                    Ok(vibesql_ast::Statement::CreateProcedure(
                        self.parse_create_procedure_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Function) {
                    Ok(vibesql_ast::Statement::CreateFunction(
                        self.parse_create_function_statement()?,
                    ))
                } else {
                    Err(ParseError {
                        message:
                            "Expected TABLE, SCHEMA, ROLE, DOMAIN, SEQUENCE, TYPE, COLLATION, CHARACTER, TRANSLATION, VIEW, TRIGGER, INDEX, ASSERTION, PROCEDURE, or FUNCTION after CREATE"
                                .to_string(),
                    })
                }
            }
            Token::Keyword { keyword: Keyword::Drop, .. } => {
                if self.peek_next_keyword(Keyword::Table) {
                    Ok(vibesql_ast::Statement::DropTable(self.parse_drop_table_statement()?))
                } else if self.peek_next_keyword(Keyword::Schema) {
                    Ok(vibesql_ast::Statement::DropSchema(self.parse_drop_schema_statement()?))
                } else if self.peek_next_keyword(Keyword::Role) {
                    Ok(vibesql_ast::Statement::DropRole(self.parse_drop_role_statement()?))
                } else if self.peek_next_keyword(Keyword::Domain) {
                    Ok(vibesql_ast::Statement::DropDomain(self.parse_drop_domain_statement()?))
                } else if self.peek_next_keyword(Keyword::Sequence) {
                    Ok(vibesql_ast::Statement::DropSequence(self.parse_drop_sequence_statement()?))
                } else if self.peek_next_keyword(Keyword::Type) {
                    Ok(vibesql_ast::Statement::DropType(self.parse_drop_type_statement()?))
                } else if self.peek_next_keyword(Keyword::Collation) {
                    Ok(vibesql_ast::Statement::DropCollation(
                        self.parse_drop_collation_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Character) {
                    Ok(vibesql_ast::Statement::DropCharacterSet(
                        self.parse_drop_character_set_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Translation) {
                    Ok(vibesql_ast::Statement::DropTranslation(
                        self.parse_drop_translation_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::View) {
                    Ok(vibesql_ast::Statement::DropView(self.parse_drop_view_statement()?))
                } else if self.peek_next_keyword(Keyword::Trigger) {
                    Ok(vibesql_ast::Statement::DropTrigger(self.parse_drop_trigger_statement()?))
                } else if self.peek_next_keyword(Keyword::Index) {
                    Ok(vibesql_ast::Statement::DropIndex(self.parse_drop_index_statement()?))
                } else if self.peek_next_keyword(Keyword::Assertion) {
                    Ok(vibesql_ast::Statement::DropAssertion(
                        self.parse_drop_assertion_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Procedure) {
                    Ok(vibesql_ast::Statement::DropProcedure(
                        self.parse_drop_procedure_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Function) {
                    Ok(vibesql_ast::Statement::DropFunction(self.parse_drop_function_statement()?))
                } else {
                    Err(ParseError {
                        message:
                            "Expected TABLE, SCHEMA, ROLE, DOMAIN, SEQUENCE, TYPE, COLLATION, CHARACTER, TRANSLATION, VIEW, TRIGGER, INDEX, ASSERTION, PROCEDURE, or FUNCTION after DROP"
                                .to_string(),
                    })
                }
            }
            Token::Keyword { keyword: Keyword::Truncate, .. } => {
                let truncate_stmt = self.parse_truncate_table_statement()?;
                Ok(vibesql_ast::Statement::TruncateTable(truncate_stmt))
            }
            Token::Keyword { keyword: Keyword::Alter, .. } => {
                if self.peek_next_keyword(Keyword::Table) {
                    let alter_stmt = self.parse_alter_table_statement()?;
                    Ok(vibesql_ast::Statement::AlterTable(alter_stmt))
                } else if self.peek_next_keyword(Keyword::Sequence) {
                    let alter_stmt = self.parse_alter_sequence_statement()?;
                    Ok(vibesql_ast::Statement::AlterSequence(alter_stmt))
                } else if self.peek_next_keyword(Keyword::Trigger) {
                    let alter_stmt = self.parse_alter_trigger_statement()?;
                    Ok(vibesql_ast::Statement::AlterTrigger(alter_stmt))
                } else {
                    Err(ParseError {
                        message: "Expected TABLE, SEQUENCE, or TRIGGER after ALTER".to_string(),
                    })
                }
            }
            Token::Keyword { keyword: Keyword::Reindex, .. } => {
                let reindex_stmt = self.parse_reindex_statement()?;
                Ok(vibesql_ast::Statement::Reindex(reindex_stmt))
            }
            Token::Keyword { keyword: Keyword::Vacuum, .. } => {
                let vacuum_stmt = self.parse_vacuum_statement()?;
                Ok(vibesql_ast::Statement::Vacuum(vacuum_stmt))
            }
            Token::Keyword { keyword: Keyword::Analyze, .. } => {
                let analyze_stmt = self.parse_analyze_statement()?;
                Ok(vibesql_ast::Statement::Analyze(analyze_stmt))
            }
            Token::Keyword { keyword: Keyword::Explain, .. } => {
                let explain_stmt = self.parse_explain_statement()?;
                Ok(vibesql_ast::Statement::Explain(explain_stmt))
            }
            Token::Keyword { keyword: Keyword::Begin, .. }
            | Token::Keyword { keyword: Keyword::Start, .. } => {
                let begin_stmt = self.parse_begin_statement()?;
                Ok(vibesql_ast::Statement::BeginTransaction(begin_stmt))
            }
            Token::Keyword { keyword: Keyword::Commit, .. }
            | Token::Keyword { keyword: Keyword::End, .. } => {
                // END is a SQLite alias for COMMIT in transaction context
                let commit_stmt = self.parse_commit_statement()?;
                Ok(vibesql_ast::Statement::Commit(commit_stmt))
            }
            Token::Keyword { keyword: Keyword::Rollback, .. } => {
                // Check if this is ROLLBACK TO SAVEPOINT by looking ahead
                let saved_position = self.position;
                self.advance(); // consume ROLLBACK
                if self.peek_keyword(Keyword::To) {
                    // Reset and parse as ROLLBACK TO SAVEPOINT
                    self.position = saved_position;
                    let rollback_to_stmt = self.parse_rollback_to_savepoint_statement()?;
                    Ok(vibesql_ast::Statement::RollbackToSavepoint(rollback_to_stmt))
                } else {
                    // Reset and parse as regular ROLLBACK
                    self.position = saved_position;
                    let rollback_stmt = self.parse_rollback_statement()?;
                    Ok(vibesql_ast::Statement::Rollback(rollback_stmt))
                }
            }
            Token::Keyword { keyword: Keyword::Savepoint, .. } => {
                let savepoint_stmt = self.parse_savepoint_statement()?;
                Ok(vibesql_ast::Statement::Savepoint(savepoint_stmt))
            }
            Token::Keyword { keyword: Keyword::Release, .. } => {
                let release_stmt = self.parse_release_savepoint_statement()?;
                Ok(vibesql_ast::Statement::ReleaseSavepoint(release_stmt))
            }
            Token::Keyword { keyword: Keyword::Set, .. } => {
                // Look ahead to determine which SET statement this is
                if self.peek_next_keyword(Keyword::Schema) {
                    let set_stmt = self.parse_set_schema_statement()?;
                    Ok(vibesql_ast::Statement::SetSchema(set_stmt))
                } else if self.peek_next_keyword(Keyword::Catalog) {
                    let set_stmt = schema::parse_set_catalog(self)?;
                    Ok(vibesql_ast::Statement::SetCatalog(set_stmt))
                } else if self.peek_next_keyword(Keyword::Names) {
                    let set_stmt = schema::parse_set_names(self)?;
                    Ok(vibesql_ast::Statement::SetNames(set_stmt))
                } else if self.peek_next_keyword(Keyword::Time) {
                    let set_stmt = schema::parse_set_time_zone(self)?;
                    Ok(vibesql_ast::Statement::SetTimeZone(set_stmt))
                } else if self.peek_next_keyword(Keyword::Transaction) {
                    let set_stmt = self.parse_set_transaction_statement()?;
                    Ok(vibesql_ast::Statement::SetTransaction(set_stmt))
                } else if self.peek_next_keyword(Keyword::Local) {
                    // SET LOCAL TRANSACTION
                    let set_stmt = self.parse_set_transaction_statement()?;
                    Ok(vibesql_ast::Statement::SetTransaction(set_stmt))
                } else {
                    // Try to parse as SET variable statement (SESSION/GLOBAL or direct variable)
                    let set_stmt = schema::parse_set_variable(self)?;
                    Ok(vibesql_ast::Statement::SetVariable(set_stmt))
                }
            }
            Token::Keyword { keyword: Keyword::Grant, .. } => {
                let grant_stmt = self.parse_grant_statement()?;
                Ok(vibesql_ast::Statement::Grant(grant_stmt))
            }
            Token::Keyword { keyword: Keyword::Revoke, .. } => {
                let revoke_stmt = self.parse_revoke_statement()?;
                Ok(vibesql_ast::Statement::Revoke(revoke_stmt))
            }
            Token::Keyword { keyword: Keyword::Declare, .. } => {
                let declare_cursor_stmt = self.parse_declare_cursor_statement()?;
                Ok(vibesql_ast::Statement::DeclareCursor(declare_cursor_stmt))
            }
            Token::Keyword { keyword: Keyword::Open, .. } => {
                let open_cursor_stmt = self.parse_open_cursor_statement()?;
                Ok(vibesql_ast::Statement::OpenCursor(open_cursor_stmt))
            }
            Token::Keyword { keyword: Keyword::Fetch, .. } => {
                let fetch_stmt = self.parse_fetch_statement()?;
                Ok(vibesql_ast::Statement::Fetch(fetch_stmt))
            }
            Token::Keyword { keyword: Keyword::Close, .. } => {
                let close_cursor_stmt = self.parse_close_cursor_statement()?;
                Ok(vibesql_ast::Statement::CloseCursor(close_cursor_stmt))
            }
            Token::Keyword { keyword: Keyword::Call, .. } => {
                let call_stmt = self.parse_call_statement()?;
                Ok(vibesql_ast::Statement::Call(call_stmt))
            }
            Token::Keyword { keyword: Keyword::Show, .. } => self.parse_show_statement(),
            Token::Keyword { keyword: Keyword::Describe, .. } => {
                let describe_stmt = self.parse_describe_statement()?;
                Ok(vibesql_ast::Statement::Describe(describe_stmt))
            }
            Token::Keyword { keyword: Keyword::Prepare, .. } => {
                let prepare_stmt = self.parse_prepare_statement()?;
                Ok(vibesql_ast::Statement::Prepare(prepare_stmt))
            }
            Token::Keyword { keyword: Keyword::Execute, .. } => {
                let execute_stmt = self.parse_execute_statement()?;
                Ok(vibesql_ast::Statement::Execute(execute_stmt))
            }
            Token::Keyword { keyword: Keyword::Deallocate, .. } => {
                let deallocate_stmt = self.parse_deallocate_statement()?;
                Ok(vibesql_ast::Statement::Deallocate(deallocate_stmt))
            }
            Token::Keyword { keyword: Keyword::Pragma, .. } => {
                let pragma_stmt = self.parse_pragma_statement()?;
                Ok(vibesql_ast::Statement::Pragma(pragma_stmt))
            }
            // ATTACH / DETACH are dispatched on a leading identifier rather
            // than a lexer keyword so that `attach` / `detach` remain usable
            // as ordinary identifiers everywhere else (see parser/attach.rs).
            Token::Identifier(word) if word.eq_ignore_ascii_case("ATTACH") => {
                let attach_stmt = self.parse_attach_statement()?;
                Ok(vibesql_ast::Statement::Attach(attach_stmt))
            }
            Token::Identifier(word) if word.eq_ignore_ascii_case("DETACH") => {
                let detach_stmt = self.parse_detach_statement()?;
                Ok(vibesql_ast::Statement::Detach(detach_stmt))
            }
            _ => Err(ParseError { message: self.peek().syntax_error() }),
        }
    }

    /// Parse BEGIN [TRANSACTION] statement
    pub fn parse_begin_statement(&mut self) -> Result<vibesql_ast::BeginStmt, ParseError> {
        transaction::parse_begin_statement(self)
    }

    /// Parse COMMIT statement
    pub fn parse_commit_statement(&mut self) -> Result<vibesql_ast::CommitStmt, ParseError> {
        transaction::parse_commit_statement(self)
    }

    /// Parse ROLLBACK statement
    pub fn parse_rollback_statement(&mut self) -> Result<vibesql_ast::RollbackStmt, ParseError> {
        transaction::parse_rollback_statement(self)
    }

    /// Parse ALTER TABLE statement
    pub fn parse_alter_table_statement(
        &mut self,
    ) -> Result<vibesql_ast::AlterTableStmt, ParseError> {
        alter::parse_alter_table(self)
    }

    /// Parse SAVEPOINT statement
    pub fn parse_savepoint_statement(&mut self) -> Result<vibesql_ast::SavepointStmt, ParseError> {
        transaction::parse_savepoint_statement(self)
    }

    /// Parse ROLLBACK TO SAVEPOINT statement
    pub fn parse_rollback_to_savepoint_statement(
        &mut self,
    ) -> Result<vibesql_ast::RollbackToSavepointStmt, ParseError> {
        transaction::parse_rollback_to_savepoint_statement(self)
    }

    /// Parse RELEASE SAVEPOINT statement
    pub fn parse_release_savepoint_statement(
        &mut self,
    ) -> Result<vibesql_ast::ReleaseSavepointStmt, ParseError> {
        transaction::parse_release_savepoint_statement(self)
    }

    /// Parse CREATE SCHEMA statement
    pub fn parse_create_schema_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateSchemaStmt, ParseError> {
        schema::parse_create_schema(self)
    }

    /// Parse DROP SCHEMA statement
    pub fn parse_drop_schema_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropSchemaStmt, ParseError> {
        schema::parse_drop_schema(self)
    }

    /// Parse SET SCHEMA statement
    pub fn parse_set_schema_statement(&mut self) -> Result<vibesql_ast::SetSchemaStmt, ParseError> {
        schema::parse_set_schema(self)
    }

    /// Parse GRANT statement
    pub fn parse_grant_statement(&mut self) -> Result<vibesql_ast::GrantStmt, ParseError> {
        grant::parse_grant(self)
    }

    /// Parse REVOKE statement
    pub fn parse_revoke_statement(&mut self) -> Result<vibesql_ast::RevokeStmt, ParseError> {
        revoke::parse_revoke(self)
    }

    /// Parse CREATE ROLE statement
    pub fn parse_create_role_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateRoleStmt, ParseError> {
        role::parse_create_role(self)
    }

    /// Parse DROP ROLE statement
    pub fn parse_drop_role_statement(&mut self) -> Result<vibesql_ast::DropRoleStmt, ParseError> {
        role::parse_drop_role(self)
    }

    // ========================================================================
    // Advanced SQL Object Parsers (SQL:1999)
    // ========================================================================

    /// Parse CREATE DOMAIN statement (uses full implementation from domain module)
    pub fn parse_create_domain_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateDomainStmt, ParseError> {
        domain::parse_create_domain(self)
    }

    /// Parse DROP DOMAIN statement (uses full implementation from domain module)
    pub fn parse_drop_domain_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropDomainStmt, ParseError> {
        domain::parse_drop_domain(self)
    }

    /// Parse CREATE SEQUENCE statement
    pub fn parse_create_sequence_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateSequenceStmt, ParseError> {
        advanced_objects::parse_create_sequence(self)
    }

    /// Parse DROP SEQUENCE statement
    pub fn parse_drop_sequence_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropSequenceStmt, ParseError> {
        advanced_objects::parse_drop_sequence(self)
    }

    /// Parse ALTER SEQUENCE statement
    pub fn parse_alter_sequence_statement(
        &mut self,
    ) -> Result<vibesql_ast::AlterSequenceStmt, ParseError> {
        advanced_objects::parse_alter_sequence(self)
    }

    /// Parse CREATE TYPE statement
    pub fn parse_create_type_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateTypeStmt, ParseError> {
        advanced_objects::parse_create_type(self)
    }

    /// Parse SET TRANSACTION statement
    pub fn parse_set_transaction_statement(
        &mut self,
    ) -> Result<vibesql_ast::SetTransactionStmt, ParseError> {
        // SET keyword
        self.expect_keyword(Keyword::Set)?;

        // Optional LOCAL keyword
        let local = self.try_consume_keyword(Keyword::Local);

        // TRANSACTION keyword
        self.expect_keyword(Keyword::Transaction)?;

        // Parse optional characteristics
        let mut isolation_level = None;
        let mut access_mode = None;

        loop {
            if self.try_consume_keyword(Keyword::Serializable) {
                isolation_level = Some(vibesql_ast::IsolationLevel::Serializable);
            } else if self.try_consume_keyword(Keyword::Read) {
                if self.try_consume_keyword(Keyword::Only) {
                    access_mode = Some(vibesql_ast::TransactionAccessMode::ReadOnly);
                } else if self.try_consume_keyword(Keyword::Write) {
                    access_mode = Some(vibesql_ast::TransactionAccessMode::ReadWrite);
                } else {
                    return Err(ParseError {
                        message: "Expected ONLY or WRITE after READ".to_string(),
                    });
                }
            } else if self.try_consume_keyword(Keyword::Isolation) {
                self.expect_keyword(Keyword::Level)?;
                if self.try_consume_keyword(Keyword::Serializable) {
                    isolation_level = Some(vibesql_ast::IsolationLevel::Serializable);
                } else {
                    return Err(ParseError {
                        message: "Expected SERIALIZABLE after ISOLATION LEVEL".to_string(),
                    });
                }
            } else {
                break;
            }

            // Check for comma (more characteristics)
            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(vibesql_ast::SetTransactionStmt { local, isolation_level, access_mode })
    }

    /// Parse DROP TYPE statement
    pub fn parse_drop_type_statement(&mut self) -> Result<vibesql_ast::DropTypeStmt, ParseError> {
        advanced_objects::parse_drop_type(self)
    }

    /// Parse CREATE COLLATION statement
    pub fn parse_create_collation_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateCollationStmt, ParseError> {
        advanced_objects::parse_create_collation(self)
    }

    /// Parse DROP COLLATION statement
    pub fn parse_drop_collation_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropCollationStmt, ParseError> {
        advanced_objects::parse_drop_collation(self)
    }

    /// Parse CREATE CHARACTER SET statement
    pub fn parse_create_character_set_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateCharacterSetStmt, ParseError> {
        advanced_objects::parse_create_character_set(self)
    }

    /// Parse DROP CHARACTER SET statement
    pub fn parse_drop_character_set_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropCharacterSetStmt, ParseError> {
        advanced_objects::parse_drop_character_set(self)
    }

    /// Parse CREATE TRANSLATION statement
    pub fn parse_create_translation_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateTranslationStmt, ParseError> {
        advanced_objects::parse_create_translation(self)
    }

    /// Parse DROP TRANSLATION statement
    pub fn parse_drop_translation_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropTranslationStmt, ParseError> {
        advanced_objects::parse_drop_translation(self)
    }

    /// Parse CREATE ASSERTION statement
    pub fn parse_create_assertion_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateAssertionStmt, ParseError> {
        advanced_objects::parse_create_assertion(self)
    }

    /// Parse DROP ASSERTION statement
    pub fn parse_drop_assertion_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropAssertionStmt, ParseError> {
        advanced_objects::parse_drop_assertion(self)
    }

    /// Parse CREATE PROCEDURE statement
    pub fn parse_create_procedure_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateProcedureStmt, ParseError> {
        self.advance(); // consume CREATE
        self.advance(); // consume PROCEDURE
        self.parse_create_procedure()
    }

    /// Parse DROP PROCEDURE statement
    pub fn parse_drop_procedure_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropProcedureStmt, ParseError> {
        self.advance(); // consume DROP
        self.advance(); // consume PROCEDURE
        self.parse_drop_procedure()
    }

    /// Parse CREATE FUNCTION statement
    pub fn parse_create_function_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateFunctionStmt, ParseError> {
        self.advance(); // consume CREATE
        self.advance(); // consume FUNCTION
        self.parse_create_function()
    }

    /// Parse DROP FUNCTION statement
    pub fn parse_drop_function_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropFunctionStmt, ParseError> {
        self.advance(); // consume DROP
        self.advance(); // consume FUNCTION
        self.parse_drop_function()
    }

    /// Parse CALL statement
    pub fn parse_call_statement(&mut self) -> Result<vibesql_ast::CallStmt, ParseError> {
        self.advance(); // consume CALL
        self.parse_call()
    }

    /// Parse statement starting with WITH clause (CTEs)
    ///
    /// WITH can precede SELECT, VALUES, INSERT, UPDATE, or DELETE statements.
    /// This method parses the CTE list first, then dispatches to the appropriate
    /// statement parser based on the following keyword.
    fn parse_with_statement(&mut self) -> Result<vibesql_ast::Statement, ParseError> {
        self.consume_keyword(Keyword::With)?;

        // Check for optional RECURSIVE keyword
        let recursive = if self.peek_keyword(Keyword::Recursive) {
            self.consume_keyword(Keyword::Recursive)?;
            true
        } else {
            false
        };

        // Parse CTE list
        let cte_list = self.parse_cte_list(recursive)?;

        // Check what statement follows the CTEs
        match self.peek() {
            Token::Keyword { keyword: Keyword::Select, .. } => {
                // For SELECT, we need to reconstruct the statement with the CTEs
                // The parse_select_statement expects to parse WITH itself, so we need
                // to create a SelectStmt with the CTEs manually
                let mut select_stmt = self.parse_select_statement_after_with()?;
                select_stmt.with_clause = Some(cte_list);
                Ok(vibesql_ast::Statement::Select(Box::new(select_stmt)))
            }
            Token::Keyword { keyword: Keyword::Values, .. } => {
                // SQLite treats a standalone VALUES as a SELECT form, so a WITH
                // clause may precede a bare VALUES statement (issue #5353).
                // parse_values_statement already returns a SelectStmt with the
                // `values` body set; attach the CTEs to it.
                let mut select_stmt = self.parse_values_statement()?;
                select_stmt.with_clause = Some(cte_list);
                Ok(vibesql_ast::Statement::Select(Box::new(select_stmt)))
            }
            Token::Keyword { keyword: Keyword::Insert, .. } => {
                // Parse INSERT with pre-parsed CTEs
                let insert_stmt = self.parse_insert_statement_with_cte(cte_list)?;
                Ok(vibesql_ast::Statement::Insert(insert_stmt))
            }
            Token::Keyword { keyword: Keyword::Update, .. } => {
                // Parse UPDATE with pre-parsed CTEs
                let update_stmt = self.parse_update_statement_with_cte(cte_list)?;
                Ok(vibesql_ast::Statement::Update(update_stmt))
            }
            Token::Keyword { keyword: Keyword::Delete, .. } => {
                // Parse DELETE with pre-parsed CTEs
                let delete_stmt = self.parse_delete_statement_with_cte(cte_list)?;
                Ok(vibesql_ast::Statement::Delete(delete_stmt))
            }
            _ => Err(ParseError {
                message: format!(
                    "Expected SELECT, VALUES, INSERT, UPDATE, or DELETE after WITH clause, found {}",
                    self.peek().syntax_error()
                ),
            }),
        }
    }

    /// Parse SELECT statement after WITH clause has been consumed
    /// This is similar to parse_select_statement but expects SELECT (not WITH)
    fn parse_select_statement_after_with(&mut self) -> Result<vibesql_ast::SelectStmt, ParseError> {
        self.expect_keyword(Keyword::Select)?;

        // Parse optional set quantifier (DISTINCT or ALL)
        let distinct = if self.peek_keyword(Keyword::Distinct) {
            self.consume_keyword(Keyword::Distinct)?;
            true
        } else if self.peek_keyword(Keyword::All) {
            self.consume_keyword(Keyword::All)?;
            false
        } else {
            false
        };

        // Parse SELECT list
        let select_list = self.parse_select_list()?;

        // Parse optional INTO clause
        let (into_table, into_variables) = if self.peek_keyword(Keyword::Into) {
            self.consume_keyword(Keyword::Into)?;
            if matches!(self.peek(), Token::UserVariable(_)) {
                let variables = self.parse_comma_separated_list(|p| match p.peek() {
                    Token::UserVariable(var_name) => {
                        let name = var_name.clone();
                        p.advance();
                        Ok(name)
                    }
                    _ => Err(ParseError {
                        message: "Expected user variable (@var) in procedural SELECT INTO"
                            .to_string(),
                    }),
                })?;
                (None, Some(variables))
            } else {
                (Some(self.parse_identifier()?), None)
            }
        } else {
            (None, None)
        };

        // Parse optional FROM clause
        let from = if self.peek_keyword(Keyword::From) {
            self.consume_keyword(Keyword::From)?;
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        // Parse optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.consume_keyword(Keyword::Where)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse optional GROUP BY clause
        let group_by = if self.peek_keyword(Keyword::Group) {
            self.consume_keyword(Keyword::Group)?;
            self.expect_keyword(Keyword::By)?;
            Some(self.parse_group_by_clause()?)
        } else {
            None
        };

        // Parse optional HAVING clause
        let having = if self.peek_keyword(Keyword::Having) {
            self.consume_keyword(Keyword::Having)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse optional WINDOW clause (named window definitions).
        // Mirrors `parse_select_statement_internal`; without this a WITH-prefixed
        // query silently drops its named windows so `OVER win` fails to resolve
        // ("no such window") — window6.test 10.0.
        let window_definitions = if self.peek_keyword(Keyword::Window) {
            self.consume_keyword(Keyword::Window)?;
            Some(self.parse_window_definitions()?)
        } else {
            None
        };

        // Parse set operations (UNION, INTERSECT, EXCEPT)
        let set_operation = if self.peek_keyword(Keyword::Union)
            || self.peek_keyword(Keyword::Intersect)
            || self.peek_keyword(Keyword::Except)
        {
            let op = if self.peek_keyword(Keyword::Union) {
                self.consume_keyword(Keyword::Union)?;
                vibesql_ast::SetOperator::Union
            } else if self.peek_keyword(Keyword::Intersect) {
                self.consume_keyword(Keyword::Intersect)?;
                vibesql_ast::SetOperator::Intersect
            } else {
                self.consume_keyword(Keyword::Except)?;
                vibesql_ast::SetOperator::Except
            };

            let all = if self.peek_keyword(Keyword::All) {
                self.consume_keyword(Keyword::All)?;
                true
            } else if self.peek_keyword(Keyword::Distinct) {
                self.consume_keyword(Keyword::Distinct)?;
                false
            } else {
                false
            };

            let right = if matches!(self.peek(), Token::LParen) {
                self.advance();
                let stmt = if self.peek_keyword(Keyword::Values) {
                    self.parse_values_statement()?
                } else {
                    self.parse_select_statement()?
                };
                if !matches!(self.peek(), Token::RParen) {
                    return Err(ParseError {
                        message: "Expected ')' after parenthesized statement in set operation"
                            .to_string(),
                    });
                }
                self.advance();
                Box::new(stmt)
            } else if self.peek_keyword(Keyword::Values) {
                Box::new(self.parse_values_statement()?)
            } else {
                Box::new(self.parse_select_statement()?)
            };

            Some(vibesql_ast::SetOperation { op, all, right })
        } else {
            None
        };

        // Parse ORDER BY
        let order_by = if self.peek_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::Order)?;
            self.expect_keyword(Keyword::By)?;

            let order_items = self.parse_comma_separated_list(|p| {
                let expr = p.parse_expression()?;
                let direction = if p.peek_keyword(Keyword::Asc) {
                    p.consume_keyword(Keyword::Asc)?;
                    vibesql_ast::OrderDirection::Asc
                } else if p.peek_keyword(Keyword::Desc) {
                    p.consume_keyword(Keyword::Desc)?;
                    vibesql_ast::OrderDirection::Desc
                } else {
                    vibesql_ast::OrderDirection::Asc
                };

                let nulls_order = if p.peek_keyword(Keyword::Nulls) {
                    p.consume_keyword(Keyword::Nulls)?;
                    if p.peek_keyword(Keyword::First) {
                        p.consume_keyword(Keyword::First)?;
                        Some(vibesql_ast::NullsOrder::First)
                    } else if p.peek_keyword(Keyword::Last) {
                        p.consume_keyword(Keyword::Last)?;
                        Some(vibesql_ast::NullsOrder::Last)
                    } else {
                        return Err(ParseError {
                            message: format!(
                                "Expected FIRST or LAST after NULLS, found {}",
                                p.peek().syntax_error()
                            ),
                        });
                    }
                } else {
                    None
                };

                Ok(vibesql_ast::OrderByItem { expr, direction, nulls_order })
            })?;

            // Check for too many ORDER BY terms (SQLite compatibility)
            if order_items.len() > MAX_ORDER_BY_TERMS {
                return Err(ParseError {
                    message: "too many terms in ORDER BY clause".to_string(),
                });
            }

            Some(order_items)
        } else {
            None
        };

        // Parse LIMIT (supports comma syntax)
        let (limit, offset_from_limit) = if self.peek_keyword(Keyword::Limit) {
            self.consume_keyword(Keyword::Limit)?;
            let first_expr = self.parse_expression()?;

            if matches!(self.peek(), Token::Comma) {
                self.advance();
                let second_expr = self.parse_expression()?;
                (Some(second_expr), Some(first_expr))
            } else {
                (Some(first_expr), None)
            }
        } else {
            (None, None)
        };

        // Parse OFFSET
        let offset = if offset_from_limit.is_some() {
            offset_from_limit
        } else if self.peek_keyword(Keyword::Offset) {
            self.consume_keyword(Keyword::Offset)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Consume optional semicolon
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::SelectStmt {
            hints: Vec::new(),
            with_clause: None, // Will be set by caller
            distinct,
            select_list,
            into_table,
            into_variables,
            from,
            where_clause,
            group_by,
            having,
            window_definitions,
            order_by,
            limit,
            offset,
            set_operation,
            values: None,
        })
    }
}
