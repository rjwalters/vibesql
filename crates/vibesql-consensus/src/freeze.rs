//! Freeze-at-propose: deterministic replication of non-deterministic SQL
//! (#5377).
//!
//! A [`TxnEntry`](crate::TxnEntry) replicates a transaction as a batch of
//! SQL statement strings, re-executed through the executor at apply time.
//! That is deterministic only if the statements are: `random()`,
//! `CURRENT_TIMESTAMP`, `datetime('now')`, … are evaluated **at apply
//! time**, so each replica would compute different values and silently
//! diverge.
//!
//! This module makes the statement-batch form deterministic by
//! **freezing non-deterministic expressions on the proposer**:
//!
//! 1. [`freeze_statement`] (propose side, leader): parse the statement, validate that every
//!    non-deterministic call site can be made deterministic, evaluate each site once, and return
//!    the drawn values in a deterministic traversal order. The entry carries the **original SQL
//!    text** plus this value list (no SQL re-printing — the DML surface has no round-trip
//!    serializer).
//! 2. [`substitute_statement`] (apply side, every replica): parse the same text, repeat the **same
//!    traversal**, and splice the frozen values in as literals before execution. Identical text +
//!    identical code ⇒ identical traversal ⇒ identical statements everywhere.
//!
//! # Which call sites can be frozen?
//!
//! Classification comes from the central [`vibesql_ast::volatility`]
//! list (shared with the optimizer's push-down guard). Per category:
//!
//! - **Clock readings** (`CURRENT_TIMESTAMP`/`CURRENT_DATE`/ `CURRENT_TIME` keywords, `now()`,
//!   `current_*()` call forms and their MySQL aliases `curdate()`/`curtime()`, the SQLite
//!   date/time functions when they reference `'now'` — explicitly, implicitly by omitting the
//!   time value, or via the timezone-dependent `'localtime'`/`'utc'` modifiers — the one-argument
//!   PostgreSQL `age(x)` (which compares against the current date; the two-argument form is
//!   pure), and `CAST(<TIME literal> AS TIMESTAMP)` (SQL:1999 stamps the current date)): frozen
//!   **anywhere** in a write statement. SQLite already fixes `'now'` per statement, so a single
//!   per-statement value is the correct semantics even in per-row contexts like
//!   `SET ts = CURRENT_TIMESTAMP`. `CAST(<TIME column> AS TIMESTAMP)` is a per-row clock read
//!   whose operand type is only known against the schema; it is rejected at apply by
//!   [`time_cast_violation`] (see #5381 for operand shapes beyond literals and target-table
//!   columns).
//! - **Per-row volatile functions** (`random()`, `randomblob()`, `rand()`): frozen only where the
//!   call site evaluates **exactly once** — expressions in `INSERT … VALUES` rows (and `DELETE`'s
//!   `LIMIT`/`OFFSET`). In per-row contexts (`SET`, `WHERE`, `ORDER BY`) a single frozen value
//!   would change semantics (every row would get the *same* draw), so the statement is rejected at
//!   propose with an explanatory error.
//! - **Session-state functions** (`last_insert_rowid()`, `changes()`, `total_changes()`, and the
//!   identity/server-information functions `user()`/`current_user()`/`database()`/`schema()`/
//!   `version()`): always rejected. Their value depends on session/server state that is not part
//!   of replicated database state (e.g. snapshots do not carry the rowid counter, so replay paths
//!   would disagree; node versions differ during rolling upgrades).
//! - **Deterministic date/time usage** (`strftime('%s', col)`, `datetime(col, '+1 day')`, …): pure
//!   functions of their arguments — left untouched.
//!
//! Volatile calls inside **query-bearing parts** (subqueries, CTEs,
//! `INSERT … SELECT`, `UPDATE … FROM`, `CREATE VIEW` bodies,
//! `CREATE INDEX` expressions, `ON CONFLICT` targets) are rejected
//! outright: their evaluation cardinality is data-dependent, and view /
//! index definitions would smuggle apply-time nondeterminism into later
//! statements. `RETURNING` clauses are exempt — their output is
//! computed and discarded at apply time and never feeds replicated
//! state.
//!
//! # Defense in depth at apply
//!
//! [`substitute_statement`] re-runs the full validation, so the state
//! machine **rejects** (deterministically, on every replica) any entry
//! containing an unfrozen non-deterministic site — entries proposed
//! around the freeze pass cannot diverge replicas. A frozen-site /
//! value-count mismatch, on the other hand, indicates version skew
//! between proposer and applier and is surfaced as
//! [`SubstituteError::FrozenSiteMismatch`], which the apply path treats
//! as **fatal** (halt the node, do not record a rejection — see
//! [`ConsensusError::FatalApply`](crate::ConsensusError::FatalApply)).
//!
//! # Non-deterministic DEFAULT clauses (frozen through the schema, #5381)
//!
//! `CREATE TABLE t (… ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)` stores
//! the expression in the schema; an INSERT that *fires* such a default
//! (column omitted, explicit `DEFAULT`, explicit `NULL` — VibeSQL
//! applies defaults to NULL **values** — or `DEFAULT VALUES`) would
//! evaluate the clock at apply time. These sites are invisible to the
//! positional freeze pass (they live in the schema, not the statement
//! text), so they get their own freeze/substitute pair keyed off the
//! target table's schema:
//!
//! - [`freeze_statement_with_schema`] (propose, on the leader — whose applied state *is* the
//!   replicated schema): after the positional pass, enumerate the DEFAULT-firing sites in a
//!   deterministic order and evaluate each one with the **executor's own DEFAULT evaluator**
//!   ([`vibesql_executor::evaluate_default_expression`]), **once per firing row** — exactly the
//!   per-row evaluation `apply_default_values` performs at execution time. (sqlite3 stamps one
//!   statement-stable `CURRENT_TIMESTAMP` across a multi-row INSERT; VibeSQL's executor has no
//!   per-statement 'now' cache, so per-row draws are the faithful semantics — see the #5382
//!   review notes.) The drawn values travel in `TxnEntry::frozen_defaults`.
//! - [`substitute_volatile_defaults`] (apply, every replica): repeat the same enumeration against
//!   the same (replicated, hence identical) schema and splice the values in as literals — the
//!   executor then never fires the volatile DEFAULT. A site/value count mismatch means the schema
//!   changed between propose and apply (a propose-time race, not version skew): the mismatch is a
//!   *deterministic* function of the entry and the replicated schema, so it rejects identically
//!   on every replica. An entry with firing sites and **no** frozen values (produced by pre-#5381
//!   propose code, or proposed around the freeze pass) is rejected the same way — the #5377
//!   defense in depth is preserved.
//!
//! Sites that cannot be frozen are rejected at propose: `INSERT … SELECT` into a table with a
//! volatile DEFAULT (row count and NULL-ness are not statically known), a non-literal VALUES
//! cell for a volatile-DEFAULT column (VibeSQL applies the DEFAULT to an evaluated NULL, so the
//! proposer cannot prove whether it fires), and DEFAULTs that read session state.
//! [`volatile_default_violation`] / [`volatile_default_violation_update`] remain the audit
//! primitives for statements that are *not* rewritten — trigger bodies.
//!
//! # Trigger DDL (#5381)
//!
//! `CREATE TRIGGER` joins the replicated surface with strict validation: a trigger body
//! re-executes through the executor **at apply time, on every replica, data-dependently** — no
//! positional freeze can cover it — so [`validate_statement`] admits the definition only if the
//! WHEN clause and every body statement are volatile-free (the same strict detector that guards
//! `CREATE VIEW` bodies). Firing is then deterministic by induction: the triggering statement is
//! in the entry, the trigger exists identically in every replica's catalog, and its body draws on
//! nothing but replicated state. Two schema-dependent hazards remain beyond the static check,
//! both handled against the replicated catalog: at `CREATE TRIGGER` apply time,
//! [`trigger_body_schema_violation`] rejects bodies whose DML would fire a volatile DEFAULT or a
//! TIME→TIMESTAMP column cast; and because later DDL can invalidate that audit (drop + recreate a
//! body-referenced table with a volatile DEFAULT), every replicated DML statement re-checks the
//! trigger bodies it could fire — transitively through trigger cascades — via
//! [`dml_trigger_violation`].

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use vibesql_ast::{
    visitor::{
        transform_expression, walk_expression, walk_select, ExpressionMutVisitor,
        ExpressionVisitor, VisitResult,
    },
    volatility, Assignment, CommonTableExpr, ConflictTargetItem, CreateTriggerStmt, DeleteStmt,
    Expression, FromClause, IndexColumn, InsertSource, InsertStmt, OnConflictAction, SelectItem,
    SelectStmt, Statement, TriggerAction, UpdateStmt, WhereClause,
};
use vibesql_types::{DataType, Date, SqlValue, StringValue, Time, Timestamp};

// ---------------------------------------------------------------------------
// Frozen values: the serializable closed set freezing can produce
// ---------------------------------------------------------------------------

/// One frozen (proposer-evaluated) value, carried in the replicated
/// entry alongside the statement text and spliced back in as a literal
/// at apply time.
///
/// This is a deliberate mirror of the [`SqlValue`] variants that
/// evaluating a volatile expression can produce, kept separate so the
/// wire encoding is owned by the consensus crate and cannot drift with
/// unrelated `SqlValue` changes. `Interval` and `Vector` values are not
/// representable — freezing such a result is a propose-time error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FrozenValue {
    /// SQL NULL.
    Null,
    /// BOOLEAN.
    Boolean(bool),
    /// INTEGER (i64) — e.g. `random()`.
    Integer(i64),
    /// SMALLINT.
    Smallint(i16),
    /// BIGINT.
    Bigint(i64),
    /// Unsigned 64-bit (MySQL compatibility).
    Unsigned(u64),
    /// NUMERIC.
    Numeric(f64),
    /// FLOAT (f32).
    Float(f32),
    /// REAL — e.g. `julianday('now')`.
    Real(f64),
    /// DOUBLE PRECISION.
    Double(f64),
    /// CHARACTER.
    Character(String),
    /// VARCHAR / TEXT — e.g. `datetime('now')`.
    Varchar(String),
    /// DATE — e.g. `CURRENT_DATE`.
    Date {
        /// Year (signed; proleptic Gregorian).
        year: i32,
        /// Month, 1–12.
        month: u8,
        /// Day of month, 1–31.
        day: u8,
    },
    /// TIME — e.g. `CURRENT_TIME`.
    Time {
        /// Hour, 0–23.
        hour: u8,
        /// Minute, 0–59.
        minute: u8,
        /// Second, 0–59.
        second: u8,
        /// Nanoseconds, 0–999 999 999.
        nanosecond: u32,
    },
    /// TIMESTAMP — e.g. `CURRENT_TIMESTAMP`.
    Timestamp {
        /// Year (signed; proleptic Gregorian).
        year: i32,
        /// Month, 1–12.
        month: u8,
        /// Day of month, 1–31.
        day: u8,
        /// Hour, 0–23.
        hour: u8,
        /// Minute, 0–59.
        minute: u8,
        /// Second, 0–59.
        second: u8,
        /// Nanoseconds, 0–999 999 999.
        nanosecond: u32,
    },
    /// BLOB — e.g. `randomblob(16)`.
    Blob(Vec<u8>),
}

impl TryFrom<SqlValue> for FrozenValue {
    type Error = String;

    fn try_from(value: SqlValue) -> Result<Self, Self::Error> {
        Ok(match value {
            SqlValue::Null => FrozenValue::Null,
            SqlValue::Boolean(b) => FrozenValue::Boolean(b),
            SqlValue::Integer(i) => FrozenValue::Integer(i),
            SqlValue::Smallint(i) => FrozenValue::Smallint(i),
            SqlValue::Bigint(i) => FrozenValue::Bigint(i),
            SqlValue::Unsigned(u) => FrozenValue::Unsigned(u),
            SqlValue::Numeric(f) => FrozenValue::Numeric(f),
            SqlValue::Float(f) => FrozenValue::Float(f),
            SqlValue::Real(f) => FrozenValue::Real(f),
            SqlValue::Double(f) => FrozenValue::Double(f),
            SqlValue::Character(s) => FrozenValue::Character(s.to_string()),
            SqlValue::Varchar(s) => FrozenValue::Varchar(s.to_string()),
            SqlValue::Date(d) => FrozenValue::Date { year: d.year, month: d.month, day: d.day },
            SqlValue::Time(t) => FrozenValue::Time {
                hour: t.hour,
                minute: t.minute,
                second: t.second,
                nanosecond: t.nanosecond,
            },
            SqlValue::Timestamp(ts) => FrozenValue::Timestamp {
                year: ts.date.year,
                month: ts.date.month,
                day: ts.date.day,
                hour: ts.time.hour,
                minute: ts.time.minute,
                second: ts.time.second,
                nanosecond: ts.time.nanosecond,
            },
            SqlValue::Blob(b) => FrozenValue::Blob(b),
            other @ (SqlValue::Interval(_) | SqlValue::Vector(_)) => {
                return Err(format!(
                    "value of type {} cannot be frozen into a replicated entry",
                    other.type_name()
                ));
            }
        })
    }
}

impl From<FrozenValue> for SqlValue {
    fn from(value: FrozenValue) -> Self {
        match value {
            FrozenValue::Null => SqlValue::Null,
            FrozenValue::Boolean(b) => SqlValue::Boolean(b),
            FrozenValue::Integer(i) => SqlValue::Integer(i),
            FrozenValue::Smallint(i) => SqlValue::Smallint(i),
            FrozenValue::Bigint(i) => SqlValue::Bigint(i),
            FrozenValue::Unsigned(u) => SqlValue::Unsigned(u),
            FrozenValue::Numeric(f) => SqlValue::Numeric(f),
            FrozenValue::Float(f) => SqlValue::Float(f),
            FrozenValue::Real(f) => SqlValue::Real(f),
            FrozenValue::Double(f) => SqlValue::Double(f),
            FrozenValue::Character(s) => SqlValue::Character(StringValue::from(s)),
            FrozenValue::Varchar(s) => SqlValue::Varchar(StringValue::from(s)),
            FrozenValue::Date { year, month, day } => SqlValue::Date(Date { year, month, day }),
            FrozenValue::Time { hour, minute, second, nanosecond } => {
                SqlValue::Time(Time { hour, minute, second, nanosecond })
            }
            FrozenValue::Timestamp { year, month, day, hour, minute, second, nanosecond } => {
                SqlValue::Timestamp(Timestamp {
                    date: Date { year, month, day },
                    time: Time { hour, minute, second, nanosecond },
                })
            }
            FrozenValue::Blob(b) => SqlValue::Blob(b),
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Propose-side failure: the statement cannot be made deterministic, so
/// it must not enter the replicated log. Surfaced to the proposing
/// client before consensus (no log index is consumed).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FreezeError {
    /// The statement contains nondeterminism that freezing cannot make
    /// deterministic (per-row volatile functions, session-state
    /// functions, volatile calls in query-bearing positions, …).
    #[error("statement cannot be replicated deterministically: {0}")]
    NotReplicable(String),
    /// Evaluating a volatile call site at propose time failed.
    #[error("failed to evaluate non-deterministic expression at propose time: {0}")]
    Eval(String),
}

/// Apply-side failure from [`substitute_statement`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubstituteError {
    /// The entry contains a non-deterministic site with no frozen value
    /// (it was proposed without the freeze pass). Deterministic: every
    /// replica parses the same text and rejects identically — the apply
    /// path records this as a rejected entry.
    UnfrozenVolatile(String),
    /// The number of frozen values does not match the number of
    /// volatile sites found at apply time. With identical code this
    /// cannot happen for entries produced by [`freeze_statement`]; it
    /// indicates proposer/applier version skew (different volatility
    /// classifications), which silently diverges replicas if treated as
    /// a rejection. The apply path must treat this as **fatal**.
    FrozenSiteMismatch(String),
}

impl std::fmt::Display for SubstituteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubstituteError::UnfrozenVolatile(m) => write!(
                f,
                "non-deterministic SQL must be frozen at propose time (propose through the \
                 replication layer): {m}"
            ),
            SubstituteError::FrozenSiteMismatch(m) => {
                write!(
                    f,
                    "frozen-value/volatile-site mismatch (proposer/applier version skew?): {m}"
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Propose side: validate and freeze one write statement.
///
/// Returns the frozen values for the statement's non-deterministic call
/// sites, in the deterministic traversal order that
/// [`substitute_statement`] will repeat at apply time (empty if the
/// statement is already deterministic).
///
/// Unparseable statements return `Ok(vec![])`: the apply path rejects
/// them with the same parse error on every replica, which is already
/// deterministic (and matches the pre-freeze behavior of consuming a
/// log index for malformed entries).
pub fn freeze_statement(sql: &str) -> Result<Vec<FrozenValue>, FreezeError> {
    freeze_statement_with_schema(sql, None).map(|(frozen, _)| frozen)
}

/// Propose side with schema access (#5381): freeze the statement's
/// positional non-deterministic sites **and** the non-deterministic
/// column DEFAULTs it would fire.
///
/// `schema` is the target table's schema from the proposer's applied
/// state — on the leader that *is* the replicated schema. `None` (table
/// not found, statement has no target table, or the caller has no
/// schema access) skips DEFAULT freezing; the apply path then rejects
/// any statement that fires a volatile DEFAULT, exactly as before
/// #5381.
///
/// Returns `(positional_values, default_values)` — the first list is
/// what [`substitute_statement`] consumes, the second what
/// [`substitute_volatile_defaults`] consumes. DEFAULT sites are
/// enumerated on the statement *after* positional substitution (so a
/// frozen `CURRENT_TIMESTAMP` cell counts as the literal it will be at
/// apply time), in the deterministic order the apply side repeats:
/// schema column order, then row order within each column, then
/// `ON CONFLICT DO UPDATE` / `ON DUPLICATE KEY UPDATE` / `SET`
/// assignments in statement order.
pub fn freeze_statement_with_schema(
    sql: &str,
    schema: Option<&vibesql_catalog::TableSchema>,
) -> Result<(Vec<FrozenValue>, Vec<FrozenValue>), FreezeError> {
    let Ok(statement) = vibesql_parser::parse_with_arena_fallback(sql) else {
        return Ok((Vec::new(), Vec::new()));
    };
    validate_statement(&statement).map_err(FreezeError::NotReplicable)?;

    let mut visitor = SiteTransformer::capture();
    let mut statement = transform_statement_sites(statement, &mut visitor);
    let frozen = match visitor.mode {
        Mode::Capture { error: Some(e), .. } => return Err(e),
        Mode::Capture { values, .. } => values,
        Mode::Substitute { .. } => unreachable!("capture visitor"),
    };

    let mut defaults = Vec::new();
    if let Some(schema) = schema {
        let mut mode = DefaultMode::Capture { values: Vec::new() };
        transform_default_sites(&mut statement, schema, &mut mode).map_err(|e| match e {
            DefaultSiteError::Policy(m) => FreezeError::NotReplicable(m),
            DefaultSiteError::Eval(m) => FreezeError::Eval(m),
        })?;
        let DefaultMode::Capture { values } = mode else { unreachable!("capture mode") };
        defaults = values;
    }
    Ok((frozen, defaults))
}

/// Apply side: validate the statement and splice the frozen values back
/// in as literals.
///
/// Re-runs the same validation as [`freeze_statement`] (defense in
/// depth: entries proposed around the freeze pass are rejected
/// deterministically), then repeats the same traversal substituting
/// `frozen[i]` for the `i`-th non-deterministic site.
pub fn substitute_statement(
    statement: Statement,
    frozen: &[FrozenValue],
) -> Result<Statement, SubstituteError> {
    if let Err(reason) = validate_statement(&statement) {
        return Err(SubstituteError::UnfrozenVolatile(reason));
    }

    let mut visitor = SiteTransformer::substitute(frozen);
    let statement = transform_statement_sites(statement, &mut visitor);
    let Mode::Substitute { cursor, .. } = visitor.mode else {
        unreachable!("substitute visitor");
    };
    match cursor {
        sites if sites == frozen.len() => Ok(statement),
        sites if frozen.is_empty() => Err(SubstituteError::UnfrozenVolatile(format!(
            "statement has {sites} non-deterministic call site(s) but no frozen values"
        ))),
        sites => Err(SubstituteError::FrozenSiteMismatch(format!(
            "statement has {sites} non-deterministic call site(s) but {} frozen value(s)",
            frozen.len()
        ))),
    }
}

/// Apply side of DEFAULT freezing (#5381): repeat the propose-side
/// enumeration of [`freeze_statement_with_schema`] against the
/// (replicated, hence identical) target-table schema and splice
/// `frozen_defaults` in as literals, so the executor never fires the
/// volatile DEFAULT.
///
/// The statement must already have gone through
/// [`substitute_statement`] (positional sites spliced to literals) —
/// the enumeration classifies VALUES cells by literal-ness, and both
/// sides must see the same shapes.
///
/// Errors are **deterministic rejections** (every replica computes the
/// same site list from the same entry and the same replicated schema):
///
/// - firing sites with no frozen values — the entry predates DEFAULT freezing or was proposed
///   around it (the #5377 defense-in-depth rejection, preserved);
/// - a site/value count mismatch — the schema changed between propose and apply (a propose-time
///   race, not version skew; retrying the statement re-freezes against the current schema);
/// - sites that can never be frozen (non-literal cell for a volatile-DEFAULT column,
///   `INSERT … SELECT`, session-state DEFAULTs).
pub fn substitute_volatile_defaults(
    mut statement: Statement,
    schema: &vibesql_catalog::TableSchema,
    frozen_defaults: &[FrozenValue],
) -> Result<Statement, String> {
    let mut mode = DefaultMode::Substitute { values: frozen_defaults, cursor: 0 };
    transform_default_sites(&mut statement, schema, &mut mode).map_err(|e| match e {
        DefaultSiteError::Policy(m) | DefaultSiteError::Eval(m) => m,
    })?;
    let DefaultMode::Substitute { cursor, .. } = mode else { unreachable!("substitute mode") };
    match cursor {
        sites if sites == frozen_defaults.len() => Ok(statement),
        sites if frozen_defaults.is_empty() => Err(format!(
            "this statement fires {sites} non-deterministic column DEFAULT site(s) of table \
             '{}' that would be evaluated at apply time; propose it through the replication \
             layer so the DEFAULT values can be frozen (#5381)",
            schema.name
        )),
        sites => Err(format!(
            "statement fires {sites} non-deterministic DEFAULT site(s) of table '{}' but the \
             entry carries {} frozen DEFAULT value(s); the table's schema changed between \
             propose and apply — retry the statement (#5381)",
            schema.name,
            frozen_defaults.len()
        )),
    }
}

// ---------------------------------------------------------------------------
// Volatile DEFAULT sites (shared capture / substitution walker, #5381)
// ---------------------------------------------------------------------------

/// One pass over the DEFAULT-firing sites: capture (evaluate on the
/// proposer) or substitute (splice the entry's values at apply). The
/// crux is the same as the positional pass: **one** walker, so the site
/// order cannot drift between propose and apply.
enum DefaultMode<'a> {
    Capture { values: Vec<FrozenValue> },
    Substitute { values: &'a [FrozenValue], cursor: usize },
}

enum DefaultSiteError {
    /// The statement cannot be made deterministic (policy rejection).
    Policy(String),
    /// Evaluating a DEFAULT at propose time failed.
    Eval(String),
}

/// Walk every DEFAULT-firing site of the statement in the fixed shared
/// order: INSERT source sites in schema-column order (rows in order
/// within each column), then `ON CONFLICT DO UPDATE`, then
/// `ON DUPLICATE KEY UPDATE`, then UPDATE `SET` assignments in
/// statement order. Only INSERT and UPDATE have DEFAULT sites.
fn transform_default_sites(
    statement: &mut Statement,
    schema: &vibesql_catalog::TableSchema,
    mode: &mut DefaultMode,
) -> Result<(), DefaultSiteError> {
    match statement {
        Statement::Insert(stmt) => transform_insert_default_sites(stmt, schema, mode),
        Statement::Update(stmt) => {
            transform_assignment_default_sites(&mut stmt.assignments, schema, mode)
        }
        _ => Ok(()),
    }
}

fn transform_insert_default_sites(
    stmt: &mut InsertStmt,
    schema: &vibesql_catalog::TableSchema,
    mode: &mut DefaultMode,
) -> Result<(), DefaultSiteError> {
    let volatile_columns: Vec<(usize, &vibesql_catalog::ColumnSchema)> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.default_value.as_ref().is_some_and(default_expr_is_volatile))
        .collect();

    if !volatile_columns.is_empty() {
        match &mut stmt.source {
            InsertSource::DefaultValues => {
                // Rewrite `DEFAULT VALUES` into an explicit column list
                // carrying the frozen values; the executor fills the
                // remaining columns from their (deterministic) DEFAULTs
                // exactly as `DEFAULT VALUES` would have.
                let mut columns = Vec::new();
                let mut row = Vec::new();
                for (_, column) in &volatile_columns {
                    let default_expr = column.default_value.as_ref().expect("filtered above");
                    reject_unfreezable_default(default_expr, &column.name, &schema.name)?;
                    columns.push(column.name.clone());
                    row.push(default_site_literal(default_expr, &column.name, mode)?);
                }
                stmt.columns = columns;
                stmt.source = InsertSource::Values(vec![row]);
            }
            InsertSource::Values(rows) => {
                for (column_idx, column) in &volatile_columns {
                    let default_expr = column.default_value.as_ref().expect("filtered above");
                    let position = if stmt.columns.is_empty() {
                        // Positional insert: the column sits at its
                        // schema index. (Rows shorter than the schema
                        // are a deterministic executor error.)
                        Some(*column_idx)
                    } else {
                        stmt.columns.iter().position(|c| c.eq_ignore_ascii_case(&column.name))
                    };
                    match position {
                        Some(position) => {
                            for row in rows.iter_mut() {
                                let Some(cell) = row.get_mut(position) else { continue };
                                match cell {
                                    Expression::Default
                                    | Expression::Literal(SqlValue::Null) => {
                                        reject_unfreezable_default(
                                            default_expr,
                                            &column.name,
                                            &schema.name,
                                        )?;
                                        *cell = default_site_literal(
                                            default_expr,
                                            &column.name,
                                            mode,
                                        )?;
                                    }
                                    Expression::Literal(_) => {}
                                    _ => {
                                        // VibeSQL applies the DEFAULT to an
                                        // evaluated NULL, so a non-literal
                                        // cell may fire it at apply time —
                                        // unprovable at propose.
                                        return Err(DefaultSiteError::Policy(format!(
                                            "column '{}' of table '{}' has a non-deterministic \
                                             DEFAULT, and VibeSQL applies DEFAULTs to NULL \
                                             values: the value supplied for it in a replicated \
                                             INSERT must be a literal or the DEFAULT keyword so \
                                             the proposer can prove whether the DEFAULT fires \
                                             (#5381)",
                                            column.name, schema.name
                                        )));
                                    }
                                }
                            }
                        }
                        None => {
                            // Omitted from an explicit column list: the
                            // DEFAULT fires for every row — one drawn
                            // value per row (matching the executor's
                            // per-row `apply_default_values`). Append
                            // the column and the per-row literals.
                            reject_unfreezable_default(default_expr, &column.name, &schema.name)?;
                            for row in rows.iter_mut() {
                                let literal =
                                    default_site_literal(default_expr, &column.name, mode)?;
                                row.push(literal);
                            }
                            stmt.columns.push(column.name.clone());
                        }
                    }
                }
            }
            InsertSource::Select(_) => {
                let names: Vec<&str> =
                    volatile_columns.iter().map(|(_, c)| c.name.as_str()).collect();
                return Err(DefaultSiteError::Policy(format!(
                    "table '{}' has non-deterministic DEFAULT(s) on column(s) {} and the rows \
                     of an INSERT … SELECT are not statically known (a NULL output fires the \
                     DEFAULT at apply time); insert explicit VALUES instead (#5381)",
                    schema.name,
                    names.join(", ")
                )));
            }
        }
    }

    for on_conflict in &mut stmt.on_conflict {
        if let OnConflictAction::DoUpdate { assignments, .. } = &mut on_conflict.action {
            transform_assignment_default_sites(assignments, schema, mode)?;
        }
    }
    if let Some(assignments) = &mut stmt.on_duplicate_key_update {
        transform_assignment_default_sites(assignments, schema, mode)?;
    }
    Ok(())
}

/// `SET col = DEFAULT` sites (UPDATE, `ON CONFLICT DO UPDATE`,
/// `ON DUPLICATE KEY UPDATE`): one frozen value per assignment. The
/// only evaluable volatile DEFAULTs are clock readings
/// (`CURRENT_DATE`/`CURRENT_TIME`/`CURRENT_TIMESTAMP` — see the
/// executor's `evaluate_default_expression`), and a single
/// per-statement clock value in a per-row context is the established
/// freeze policy (SQLite fixes `'now'` per statement).
fn transform_assignment_default_sites(
    assignments: &mut [Assignment],
    schema: &vibesql_catalog::TableSchema,
    mode: &mut DefaultMode,
) -> Result<(), DefaultSiteError> {
    for assignment in assignments.iter_mut() {
        if !matches!(assignment.value, Expression::Default) {
            continue;
        }
        let Some(column) =
            schema.columns.iter().find(|c| c.name.eq_ignore_ascii_case(&assignment.column))
        else {
            continue;
        };
        let Some(default_expr) = &column.default_value else { continue };
        if !default_expr_is_volatile(default_expr) {
            continue;
        }
        reject_unfreezable_default(default_expr, &column.name, &schema.name)?;
        assignment.value = default_site_literal(default_expr, &column.name, mode)?;
    }
    Ok(())
}

/// DEFAULTs that read session state can never be frozen (consistent
/// with the top-level session-state rejection).
fn reject_unfreezable_default(
    default_expr: &Expression,
    column: &str,
    table: &str,
) -> Result<(), DefaultSiteError> {
    if let Expression::Function { name, .. } = default_expr {
        if volatility::is_session_state_function(name.canonical()) {
            return Err(DefaultSiteError::Policy(format!(
                "column '{column}' of table '{table}' has a DEFAULT that reads session state \
                 ({}), which is not part of replicated database state; supply an explicit value \
                 (#5381)",
                name.canonical()
            )));
        }
    }
    Ok(())
}

/// Produce the literal for one DEFAULT-firing site: capture evaluates
/// the DEFAULT with the executor's own evaluator (identical semantics
/// to the apply-time fire it replaces); substitute splices the entry's
/// next frozen value. A missing value still advances the cursor — the
/// caller reports the count mismatch and the statement never executes.
fn default_site_literal(
    default_expr: &Expression,
    column: &str,
    mode: &mut DefaultMode,
) -> Result<Expression, DefaultSiteError> {
    match mode {
        DefaultMode::Capture { values } => {
            let value =
                vibesql_executor::evaluate_default_expression(default_expr).map_err(|e| {
                    DefaultSiteError::Eval(format!(
                        "failed to evaluate the DEFAULT for column '{column}' at propose time: \
                         {e}"
                    ))
                })?;
            let frozen = FrozenValue::try_from(value).map_err(DefaultSiteError::Eval)?;
            let literal = Expression::Literal(SqlValue::from(frozen.clone()));
            values.push(frozen);
            Ok(literal)
        }
        DefaultMode::Substitute { values, cursor } => {
            let site = *cursor;
            *cursor += 1;
            Ok(match values.get(site) {
                Some(frozen) => Expression::Literal(SqlValue::from(frozen.clone())),
                // Mismatch: splice a placeholder; the caller reports the
                // count mismatch and the statement is rejected unexecuted.
                None => Expression::Literal(SqlValue::Null),
            })
        }
    }
}

/// Does this INSERT fire a non-deterministic column DEFAULT?
///
/// `schema` must be the (replicated, hence identical on every node)
/// schema of the statement's target table. Returns an explanatory
/// reason if a column whose DEFAULT reads the clock (or is otherwise
/// volatile) would be filled in at execution time: the column is
/// omitted from the column list, given the `DEFAULT` keyword, given an
/// explicit `NULL` (VibeSQL applies defaults to NULL values), the
/// statement is `INSERT … DEFAULT VALUES`, or the rows come from a
/// SELECT (values not statically known).
///
/// Used for statements that are **not** rewritten by the DEFAULT
/// freeze pass — trigger body DML, audited by
/// [`trigger_body_schema_violation`] / [`dml_trigger_violation`].
pub fn volatile_default_violation(
    stmt: &InsertStmt,
    schema: &vibesql_catalog::TableSchema,
) -> Option<String> {
    for (idx, column) in schema.columns.iter().enumerate() {
        let Some(default_expr) = &column.default_value else { continue };
        if !default_expr_is_volatile(default_expr) {
            continue;
        }
        if insert_fires_default(stmt, &column.name, idx) {
            return Some(format!(
                "column '{}' of table '{}' has a non-deterministic DEFAULT that this INSERT \
                 would evaluate at apply time; supply an explicit value for it (#5377)",
                column.name, schema.name
            ));
        }
    }
    None
}

/// Does this UPDATE assign `DEFAULT` to a column with a
/// non-deterministic DEFAULT? (`SET col = DEFAULT` re-evaluates the
/// default expression at execution time.)
pub fn volatile_default_violation_update(
    stmt: &UpdateStmt,
    schema: &vibesql_catalog::TableSchema,
) -> Option<String> {
    for assignment in &stmt.assignments {
        if !matches!(assignment.value, Expression::Default) {
            continue;
        }
        let volatile = schema.columns.iter().any(|c| {
            c.name.eq_ignore_ascii_case(&assignment.column)
                && c.default_value.as_ref().is_some_and(default_expr_is_volatile)
        });
        if volatile {
            return Some(format!(
                "SET {} = DEFAULT would evaluate a non-deterministic DEFAULT at apply time; \
                 supply an explicit value (#5377)",
                assignment.column
            ));
        }
    }
    None
}

/// Schema-dependent audit of a `CREATE TRIGGER` body at apply time
/// (#5381). The static validation in [`validate_statement`] guarantees
/// the body is volatile-free, but two hazards only resolve against the
/// (replicated, hence identical) catalog:
///
/// - body DML that would **fire a non-deterministic DEFAULT** of its own target table (the
///   classic audit-table pattern `audit(…, ts DEFAULT CURRENT_TIMESTAMP)` — the trigger fires at
///   apply, the body INSERT fires the DEFAULT, every replica stamps its own clock);
/// - body DML containing `CAST(<TIME column> AS TIMESTAMP)` (per-row clock read).
///
/// Body DML targeting a table that does not exist is rejected too: the
/// body cannot be audited against a schema that is not there yet, and
/// admitting it would let a later `CREATE TABLE` introduce a volatile
/// DEFAULT behind the trigger's back (the fire-time backstop
/// [`dml_trigger_violation`] would still catch it, but rejecting at
/// CREATE is the actionable error).
pub fn trigger_body_schema_violation(
    stmt: &CreateTriggerStmt,
    db: &vibesql_storage::Database,
) -> Option<String> {
    let body = match parse_trigger_body(stmt) {
        Ok(body) => body,
        Err(reason) => return Some(reason),
    };
    for statement in &body {
        if let Some(reason) =
            trigger_body_statement_schema_violation(statement, db, &stmt.trigger_name)
        {
            return Some(reason);
        }
    }
    None
}

/// Fire-time backstop for trigger-body nondeterminism (#5381): before a
/// replicated DML statement on `table_name` executes, re-audit the
/// bodies of every enabled trigger it could fire — transitively through
/// trigger cascades (a body INSERT can fire the target table's own
/// triggers) — against the **current** replicated catalog.
///
/// [`trigger_body_schema_violation`] already ran when each trigger was
/// created, but later DDL can rot that audit: drop + recreate a
/// body-referenced table *with* a volatile DEFAULT and the body's
/// INSERT would fire the apply-time clock. The catalog is replicated
/// state, so this rejection is identical on every replica. Triggers are
/// matched by table only (not by event) — a deliberate
/// over-approximation that keeps the walk simple; it can only reject
/// statements on tables whose trigger graph has rotted, where a loud
/// deterministic error is the desired behavior.
pub fn dml_trigger_violation(
    db: &vibesql_storage::Database,
    table_name: &str,
) -> Option<String> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut pending = vec![table_name.to_ascii_lowercase()];
    while let Some(table) = pending.pop() {
        if !visited.insert(table.clone()) {
            continue;
        }
        for trigger in db.catalog.get_triggers_for_table(&table, None).filter(|t| t.enabled) {
            let TriggerAction::RawSql(sql) = &trigger.triggered_action;
            let body = match vibesql_executor::TriggerFirer::parse_trigger_sql(sql) {
                Ok(body) => body,
                Err(e) => {
                    return Some(format!(
                        "body of trigger '{}' (on table '{}') cannot be parsed, so it cannot \
                         be audited for deterministic replication: {e} (#5381)",
                        trigger.name, table
                    ));
                }
            };
            for statement in &body {
                if let Some(reason) =
                    trigger_body_statement_schema_violation(statement, db, &trigger.name)
                {
                    return Some(reason);
                }
                // Body DML can fire the *next* table's triggers.
                match statement {
                    Statement::Insert(s) => pending.push(s.table_name.to_ascii_lowercase()),
                    Statement::Update(s) => pending.push(s.table_name.to_ascii_lowercase()),
                    Statement::Delete(s) => pending.push(s.table_name.to_ascii_lowercase()),
                    _ => {}
                }
            }
        }
    }
    None
}

/// Audit one trigger-body statement against the replicated catalog:
/// volatile DEFAULTs its DML would fire, and TIME→TIMESTAMP column
/// casts. Shared by the CREATE-time audit and the fire-time backstop.
fn trigger_body_statement_schema_violation(
    statement: &Statement,
    db: &vibesql_storage::Database,
    trigger_name: &str,
) -> Option<String> {
    let table_name = match statement {
        Statement::Insert(s) => &s.table_name,
        Statement::Update(s) => &s.table_name,
        Statement::Delete(s) => &s.table_name,
        // SELECT (and anything else, which the static validation
        // rejects) writes nothing.
        _ => return None,
    };
    let Some(table) = db.get_table(table_name) else {
        return Some(format!(
            "body of trigger '{trigger_name}' references table '{table_name}', which does not \
             exist in the replicated catalog; create it first so the body can be audited for \
             deterministic replication (#5381)"
        ));
    };
    let schema = &table.schema;
    let violation = match statement {
        Statement::Insert(s) => volatile_default_violation(s, schema),
        Statement::Update(s) => volatile_default_violation_update(s, schema),
        _ => None,
    };
    if let Some(reason) = violation {
        return Some(format!("body of trigger '{trigger_name}': {reason}"));
    }
    if let Some(reason) = time_cast_violation(statement, schema) {
        return Some(format!("body of trigger '{trigger_name}': {reason}"));
    }
    None
}

/// Parse a trigger body with **exactly** the parsing the executor uses
/// when the trigger fires
/// ([`vibesql_executor::TriggerFirer::parse_trigger_sql`]) — validating
/// against a different parse would let nondeterminism through.
fn parse_trigger_body(stmt: &CreateTriggerStmt) -> Result<Vec<Statement>, String> {
    let TriggerAction::RawSql(sql) = &stmt.triggered_action;
    vibesql_executor::TriggerFirer::parse_trigger_sql(sql).map_err(|e| {
        format!(
            "body of trigger '{}' cannot be parsed, so it cannot be validated for \
             deterministic replication: {e} (#5381)",
            stmt.trigger_name
        )
    })
}

/// Does this write statement cast a TIME column of its target table to
/// TIMESTAMP? SQL:1999 semantics stamp the **current date** onto the
/// time value, so `CAST(time_col AS TIMESTAMP)` is a *per-row clock
/// read* at apply time — invisible to the positional freeze pass (the
/// operand's type is not statically known at propose) and unfixable by
/// a single frozen value. Like the DEFAULT audit, this is checked at
/// apply against the (replicated, hence identical) target-table schema,
/// so the rejection is deterministic on every replica.
///
/// Scope: column references that bind to the target table — `SET` /
/// `WHERE` / `ORDER BY` roots of UPDATE and DELETE, and the
/// `ON CONFLICT DO UPDATE` / `ON DUPLICATE KEY UPDATE` parts of INSERT
/// (including the `excluded.` alias, which carries the same schema).
/// The literal-operand form is frozen at propose instead
/// ([`is_time_to_timestamp_cast`]); TIME-typed operands that are
/// neither literals nor direct columns of the target table (e.g.
/// columns of other tables inside `INSERT … SELECT`) are tracked in
/// #5381.
pub fn time_cast_violation(
    statement: &Statement,
    schema: &vibesql_catalog::TableSchema,
) -> Option<String> {
    let mut detector = TimeCastDetector { schema, error: None };
    let mut check = |expr: &Expression| {
        if detector.error.is_none() {
            let _ = walk_expression(&mut detector, expr);
        }
    };
    match statement {
        Statement::Insert(stmt) => {
            for on_conflict in &stmt.on_conflict {
                if let OnConflictAction::DoUpdate { assignments, where_clause } =
                    &on_conflict.action
                {
                    for assignment in assignments {
                        check(&assignment.value);
                    }
                    if let Some(where_clause) = where_clause {
                        check(where_clause);
                    }
                }
            }
            if let Some(assignments) = &stmt.on_duplicate_key_update {
                for assignment in assignments {
                    check(&assignment.value);
                }
            }
        }
        Statement::Update(stmt) => {
            for assignment in &stmt.assignments {
                check(&assignment.value);
            }
            if let Some(WhereClause::Condition(expr)) = &stmt.where_clause {
                check(expr);
            }
        }
        Statement::Delete(stmt) => {
            if let Some(WhereClause::Condition(expr)) = &stmt.where_clause {
                check(expr);
            }
            if let Some(order_by) = &stmt.order_by {
                for item in order_by {
                    check(&item.expr);
                }
            }
            if let Some(limit) = &stmt.limit {
                check(limit);
            }
            if let Some(offset) = &stmt.offset {
                check(offset);
            }
        }
        _ => {}
    }
    detector.error
}

/// Precise apply-side audit for the #5398 residual, **resolved against
/// the replicated catalog** (#5406).
///
/// The earlier schema-free guard was sound but with no
/// catalog in scope it treats *every* column reference inside a query
/// body as possibly-TIME, so it over-rejects always-safe casts such as
/// `INSERT INTO t SELECT CAST(s.text_col AS TIMESTAMP) FROM s` (an
/// other-table column that is actually TEXT). At apply time the whole
/// replicated [`Database`](vibesql_storage::Database) is available — and
/// it is identical on every replica — so we can resolve each in-scope
/// table/alias to its schema and type column references precisely:
///
/// * a `CAST(<provably-non-TIME expression> AS TIMESTAMP)` is **accepted**
///   (no over-rejection), and
/// * only a genuinely-TIME (or unresolvable) operand is rejected — the
///   same deterministic rejection on every replica.
///
/// Falls back to the conservative schema-free classifier wherever the
/// scope cannot be resolved precisely (a derived table / VALUES / CTE
/// whose output column types are not in the catalog, or a referenced
/// table absent from the catalog), so soundness is never traded for
/// precision: an operand that cannot be *proven* non-TIME is still
/// rejected.
pub fn time_cast_query_violation_with_db(
    statement: &Statement,
    db: &vibesql_storage::Database,
) -> Option<String> {
    match statement {
        Statement::Insert(stmt) => {
            if let InsertSource::Select(query) = &stmt.source {
                return query_body_time_cast_violation(query, db);
            }
            None
        }
        Statement::Update(stmt) => {
            // The SET/WHERE roots bind to the target table and are
            // audited with that schema by [`time_cast_violation`]; here we
            // only descend into nested subqueries, each resolved against
            // its own FROM scope.
            for assignment in &stmt.assignments {
                if let Some(reason) = subqueries_time_cast_violation(&assignment.value, db) {
                    return Some(reason);
                }
            }
            if let Some(WhereClause::Condition(expr)) = &stmt.where_clause {
                if let Some(reason) = subqueries_time_cast_violation(expr, db) {
                    return Some(reason);
                }
            }
            None
        }
        Statement::Delete(stmt) => {
            if let Some(WhereClause::Condition(expr)) = &stmt.where_clause {
                return subqueries_time_cast_violation(expr, db);
            }
            None
        }
        _ => None,
    }
}

/// A column-resolution scope: the tables/aliases visible to expressions
/// at one query level, each resolved to its catalog schema. A scope is
/// *opaque* when any FROM item cannot be resolved to a concrete schema
/// (a derived table, VALUES constructor, or CTE whose per-column types
/// are not in the catalog); inside an opaque scope an unqualified column
/// reference could bind to the un-typed source, so it is treated
/// conservatively as possibly-TIME.
struct Scope<'a> {
    /// `(alias_or_table_name, schema)` for each base table in scope.
    tables: Vec<(String, &'a vibesql_catalog::TableSchema)>,
    /// True when some FROM item (derived table / VALUES / CTE) has no
    /// catalog schema, so unqualified references cannot be proven
    /// non-TIME.
    opaque: bool,
}

impl<'a> Scope<'a> {
    fn from_select(query: &SelectStmt, db: &'a vibesql_storage::Database) -> Self {
        let mut scope = Scope { tables: Vec::new(), opaque: false };
        // A WITH clause introduces names whose column types are not in the
        // catalog; any unqualified reference might bind to one, so be
        // conservative for the whole level.
        if query.with_clause.is_some() {
            scope.opaque = true;
        }
        if let Some(from) = &query.from {
            scope.add_from(from, db);
        }
        scope
    }

    fn add_from(&mut self, from: &FromClause, db: &'a vibesql_storage::Database) {
        match from {
            FromClause::Table { name, alias, .. } => {
                match db.get_table(name) {
                    Some(table) => {
                        let key = alias.clone().unwrap_or_else(|| name.clone());
                        self.tables.push((key, &table.schema));
                    }
                    // Unknown table: the executor will error
                    // deterministically, but until then any reference to it
                    // is unresolvable.
                    None => self.opaque = true,
                }
            }
            FromClause::Join { left, right, .. } => {
                self.add_from(left, db);
                self.add_from(right, db);
            }
            // Derived tables, VALUES constructors, and table-valued functions
            // expose columns whose types are not in the catalog.
            FromClause::Subquery { .. }
            | FromClause::Values { .. }
            | FromClause::TableFunction { .. } => self.opaque = true,
        }
    }

    /// Resolve a column reference's data type, or `None` when it cannot be
    /// pinned to a concrete catalog column (unknown / ambiguous / opaque
    /// scope).
    fn column_type(&self, col: &vibesql_ast::ColumnIdentifier) -> Option<&DataType> {
        let name = col.column_canonical();
        match col.table_canonical() {
            Some(qualifier) => self
                .tables
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(qualifier))
                .and_then(|(_, schema)| {
                    schema.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
                })
                .map(|c| &c.data_type),
            None => {
                if self.opaque {
                    return None;
                }
                let mut found: Option<&DataType> = None;
                for (_, schema) in &self.tables {
                    if let Some(c) =
                        schema.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
                    {
                        if found.is_some() {
                            // Ambiguous across tables: cannot pin a type.
                            return None;
                        }
                        found = Some(&c.data_type);
                    }
                }
                found
            }
        }
    }
}

/// Walk a query body (and its subqueries), flagging any
/// `CAST(<could-be-TIME> AS TIMESTAMP)` resolved against the per-level
/// FROM scope. Returns the first violation, or `None` if every such cast
/// is provably non-TIME.
fn query_body_time_cast_violation(
    query: &SelectStmt,
    db: &vibesql_storage::Database,
) -> Option<String> {
    let scope = Scope::from_select(query, db);
    // SELECT-list, WHERE, HAVING, ORDER BY, GROUP BY expressions all bind
    // against this level's FROM scope.
    for item in &query.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if let Some(reason) = scoped_expr_time_cast_violation(expr, &scope, db) {
                return Some(reason);
            }
        }
    }
    if let Some(expr) = &query.where_clause {
        if let Some(reason) = scoped_expr_time_cast_violation(expr, &scope, db) {
            return Some(reason);
        }
    }
    if let Some(expr) = &query.having {
        if let Some(reason) = scoped_expr_time_cast_violation(expr, &scope, db) {
            return Some(reason);
        }
    }
    if let Some(group_by) = &query.group_by {
        for expr in group_by.all_expressions() {
            if let Some(reason) = scoped_expr_time_cast_violation(expr, &scope, db) {
                return Some(reason);
            }
        }
    }
    if let Some(order_by) = &query.order_by {
        for item in order_by {
            if let Some(reason) = scoped_expr_time_cast_violation(&item.expr, &scope, db) {
                return Some(reason);
            }
        }
    }
    // Derived tables in FROM are their own query bodies with their own
    // scope; CTEs likewise.
    if let Some(ctes) = &query.with_clause {
        for cte in ctes {
            if let Some(reason) = query_body_time_cast_violation(&cte.query, db) {
                return Some(reason);
            }
        }
    }
    if let Some(from) = &query.from {
        if let Some(reason) = from_subquery_time_cast_violation(from, db) {
            return Some(reason);
        }
    }
    if let Some(set_op) = &query.set_operation {
        if let Some(reason) = query_body_time_cast_violation(&set_op.right, db) {
            return Some(reason);
        }
    }
    None
}

/// Descend the derived tables of a FROM clause (each its own query body).
fn from_subquery_time_cast_violation(
    from: &FromClause,
    db: &vibesql_storage::Database,
) -> Option<String> {
    match from {
        FromClause::Table { .. }
        | FromClause::Values { .. }
        | FromClause::TableFunction { .. } => None,
        FromClause::Subquery { query, .. } => query_body_time_cast_violation(query, db),
        FromClause::Join { left, right, condition, .. } => from_subquery_time_cast_violation(left, db)
            .or_else(|| from_subquery_time_cast_violation(right, db))
            .or_else(|| {
                // A join condition binds against the surrounding scope; it
                // is checked at that level by the caller via the WHERE/
                // SELECT walk only if it surfaces there. ON conditions are
                // their own expressions, so walk them with no scope-precise
                // typing of correlated refs would be unsound — but they
                // rarely carry casts; resolve them against the empty scope
                // (conservative).
                condition.as_ref().and_then(|cond| {
                    scoped_expr_time_cast_violation(
                        cond,
                        &Scope { tables: Vec::new(), opaque: true },
                        db,
                    )
                })
            }),
    }
}

/// Audit only the *subqueries* nested inside an UPDATE/DELETE SET/WHERE
/// root expression. The root casts themselves bind to the target table
/// and are already audited with that schema by [`time_cast_violation`];
/// re-checking them here would need the target scope. Each nested
/// subquery is resolved against its own FROM scope (#5406).
fn subqueries_time_cast_violation(
    expr: &Expression,
    db: &vibesql_storage::Database,
) -> Option<String> {
    let mut visitor = SubqueryTimeCastVisitor { db, error: None };
    let _ = walk_expression(&mut visitor, expr);
    visitor.error
}

struct SubqueryTimeCastVisitor<'a> {
    db: &'a vibesql_storage::Database,
    error: Option<String>,
}

impl ExpressionVisitor for SubqueryTimeCastVisitor<'_> {
    fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
        if self.error.is_some() {
            return VisitResult::Stop;
        }
        match expr {
            Expression::ScalarSubquery(query) | Expression::Exists { subquery: query, .. } => {
                if let Some(reason) = query_body_time_cast_violation(query, self.db) {
                    self.error = Some(reason);
                    return VisitResult::Stop;
                }
                VisitResult::Skip
            }
            Expression::In { expr: operand, subquery, .. }
            | Expression::QuantifiedComparison { expr: operand, subquery, .. } => {
                if walk_expression(self, operand).should_stop() {
                    return VisitResult::Stop;
                }
                if let Some(reason) = query_body_time_cast_violation(subquery, self.db) {
                    self.error = Some(reason);
                    return VisitResult::Stop;
                }
                VisitResult::Skip
            }
            _ => VisitResult::Continue,
        }
    }
}

/// Walk one expression at a known scope, flagging TIME→TIMESTAMP casts by
/// precise type and descending into any nested subqueries (each resolved
/// against its own FROM scope).
fn scoped_expr_time_cast_violation(
    expr: &Expression,
    scope: &Scope<'_>,
    db: &vibesql_storage::Database,
) -> Option<String> {
    // Reuse the shared expression walker for the in-scope sub-tree, but
    // stop it at every subquery boundary (returning `Skip`) and re-enter
    // each subquery with its own FROM scope via
    // [`query_body_time_cast_violation`]. This keeps the precise scope
    // associated with the level the column reference actually binds to.
    let mut visitor = ScopedTimeCastVisitor { scope, db, error: None };
    let _ = walk_expression(&mut visitor, expr);
    visitor.error
}

struct ScopedTimeCastVisitor<'a, 'b> {
    scope: &'a Scope<'b>,
    db: &'a vibesql_storage::Database,
    error: Option<String>,
}

impl ExpressionVisitor for ScopedTimeCastVisitor<'_, '_> {
    fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
        if self.error.is_some() {
            return VisitResult::Stop;
        }
        match expr {
            Expression::Cast { expr: operand, data_type: DataType::Timestamp { .. } }
                if cast_operand_could_be_time_scoped(operand, self.scope) =>
            {
                self.error = Some(query_time_cast_message());
                VisitResult::Stop
            }
            // A nested subquery opens a fresh FROM scope; audit it there
            // and stop the generic walker from descending (it would lose
            // the scope).
            Expression::ScalarSubquery(query) | Expression::Exists { subquery: query, .. } => {
                if let Some(reason) = query_body_time_cast_violation(query, self.db) {
                    self.error = Some(reason);
                    return VisitResult::Stop;
                }
                VisitResult::Skip
            }
            Expression::In { expr: operand, subquery, .. }
            | Expression::QuantifiedComparison { expr: operand, subquery, .. } => {
                if walk_expression(self, operand).should_stop() {
                    return VisitResult::Stop;
                }
                if let Some(reason) = query_body_time_cast_violation(subquery, self.db) {
                    self.error = Some(reason);
                    return VisitResult::Stop;
                }
                VisitResult::Skip
            }
            _ => VisitResult::Continue,
        }
    }
}

/// Scope-precise variant of [`cast_operand_could_be_time_inner`]: a
/// column reference is provably non-TIME when the scope resolves it to a
/// concrete non-TIME catalog column; an unresolvable reference (opaque
/// scope, unknown / ambiguous column) is conservatively possibly-TIME.
fn cast_operand_could_be_time_scoped(operand: &Expression, scope: &Scope<'_>) -> bool {
    match operand {
        Expression::Literal(v) => matches!(v, SqlValue::Time(_)),
        Expression::CurrentTime { .. } => true,
        Expression::CurrentDate | Expression::CurrentTimestamp { .. } => false,
        Expression::ColumnRef(col) => match scope.column_type(col) {
            Some(data_type) => matches!(data_type, DataType::Time { .. }),
            None => true,
        },
        Expression::Cast { data_type, .. } => matches!(data_type, DataType::Time { .. }),
        Expression::Case { when_clauses, else_result, .. } => {
            when_clauses.iter().any(|w| cast_operand_could_be_time_scoped(&w.result, scope))
                || else_result
                    .as_ref()
                    .is_some_and(|e| cast_operand_could_be_time_scoped(e, scope))
        }
        Expression::BinaryOp { .. }
        | Expression::UnaryOp { .. }
        | Expression::Conjunction(_)
        | Expression::Disjunction(_)
        | Expression::IsNull { .. }
        | Expression::IsDistinctFrom { .. }
        | Expression::IsTruthValue { .. }
        | Expression::In { .. }
        | Expression::InList { .. }
        | Expression::Between { .. }
        | Expression::Like { .. }
        | Expression::Glob { .. }
        | Expression::Exists { .. }
        | Expression::QuantifiedComparison { .. }
        | Expression::Position { .. }
        | Expression::Trim { .. }
        | Expression::Extract { .. } => false,
        // Function calls (may return TIME), window functions, scalar
        // subqueries, sequences, etc. — not provably non-TIME.
        _ => true,
    }
}

fn query_time_cast_message() -> String {
    "CAST(<TIME-typed expression> AS TIMESTAMP) inside a query body stamps the apply-time \
     current date per row (SQL:1999) and cannot be replicated deterministically; the operand \
     resolves to a TIME value (or cannot be proven non-TIME) against the replicated catalog \
     (#5406)"
        .to_string()
}

/// Flags `CAST(<target-table TIME column> AS TIMESTAMP)`. Subqueries
/// are skipped: their columns bind to other tables, whose schemas this
/// audit does not resolve (#5381).
struct TimeCastDetector<'a> {
    schema: &'a vibesql_catalog::TableSchema,
    error: Option<String>,
}

impl ExpressionVisitor for TimeCastDetector<'_> {
    fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
        if self.error.is_some() {
            return VisitResult::Stop;
        }
        match expr {
            Expression::ScalarSubquery(_) | Expression::Exists { .. } => VisitResult::Skip,
            Expression::In { expr: operand, .. }
            | Expression::QuantifiedComparison { expr: operand, .. } => {
                // Check the non-subquery operand; skip the subquery.
                if walk_expression(self, operand).should_stop() {
                    return VisitResult::Stop;
                }
                VisitResult::Skip
            }
            Expression::Cast { expr: operand, data_type: DataType::Timestamp { .. } } => {
                // The direct target-table TIME column gets a precise,
                // schema-named diagnostic (the #5377/#5382 case).
                if let Expression::ColumnRef(col) = operand.as_ref() {
                    // Unqualified references and references qualified with
                    // the target table (or the ON CONFLICT `excluded`
                    // alias, which shares its schema) bind to the target
                    // table.
                    let binds_to_target = col.table_canonical().is_none_or(|t| {
                        t.eq_ignore_ascii_case(&self.schema.name)
                            || t.eq_ignore_ascii_case("excluded")
                    });
                    let is_time_column = binds_to_target
                        && self.schema.columns.iter().any(|c| {
                            c.name.eq_ignore_ascii_case(col.column_canonical())
                                && matches!(c.data_type, DataType::Time { .. })
                        });
                    if is_time_column {
                        self.error = Some(format!(
                            "CAST({} AS TIMESTAMP) on TIME column '{}' of table '{}' stamps the \
                             current date per row at apply time (SQL:1999) and cannot be \
                             replicated deterministically; combine the column with an explicit \
                             date instead (#5377)",
                            col.column_display(),
                            col.column_display(),
                            self.schema.name
                        ));
                        return VisitResult::Stop;
                    }
                }
                // Residual (#5398): any other operand that could yield a
                // TIME value — a column of *another* table (unresolvable
                // here, so treated conservatively), or a computed TIME
                // expression (`CASE … THEN time_col END`, `time(col)`,
                // `CAST(x AS TIME)`, `CURRENT_TIME`). SQL:1999 stamps the
                // apply-time current date onto any TIME→TIMESTAMP cast, so
                // these are per-row clock reads too. We have no general
                // multi-table expression typer, so we reject
                // conservatively: any operand not *provably* non-TIME is
                // refused (sound — no silent divergence — at the cost of
                // over-rejecting some always-safe casts; precise typing is
                // tracked as a follow-on). Literal TIME operands never
                // reach here: they freeze at propose and arrive
                // substituted to a TIMESTAMP literal.
                if cast_operand_could_be_time(operand, self.schema) {
                    self.error = Some(
                        "CAST(<expression> AS TIMESTAMP) where the operand may be a TIME value \
                         stamps the apply-time current date per row (SQL:1999) and cannot be \
                         replicated deterministically; freeze the value at propose (use a TIME \
                         literal) or combine it with an explicit date instead. The replicated \
                         path conservatively rejects any TIME→TIMESTAMP cast whose operand is \
                         not provably non-TIME (#5398)"
                            .to_string(),
                    );
                    return VisitResult::Stop;
                }
                VisitResult::Continue
            }
            _ => VisitResult::Continue,
        }
    }
}

/// Conservative static type guard for `CAST(<operand> AS TIMESTAMP)`
/// (#5398): could `operand` evaluate to a `TIME` value?
///
/// This is the residual companion to the precise target-table-column
/// check above. There is no general multi-table expression typer in
/// scope here (the apply path resolves only the target-table schema, the
/// propose-side freeze pass no schema at all), so we cannot prove the
/// type of an arbitrary operand. To stay **sound** — never silently
/// accept a per-row clock read that would diverge across replicas — this
/// returns `true` (reject) unless the operand is *provably non-TIME*:
///
/// * a non-TIME literal — the only literal that could be TIME is
///   `Time`, but those freeze at propose and arrive substituted, so a
///   literal operand here is non-TIME;
/// * a target-table column whose schema type is known and not TIME;
/// * an expression whose result type is structurally non-TIME (string /
///   numeric / boolean operators and the obvious non-TIME-returning
///   value functions);
/// * a nested `CAST(… AS <non-TIME>)` — the cast fixes the output type.
///
/// Everything else — a column of another table (unresolvable), a `time(…)`
/// call, `CURRENT_TIME`, `CAST(… AS TIME)`, a `CASE` with any TIME-or-
/// unknown branch, or any expression shape not enumerated as safe — is
/// treated as possibly-TIME and rejected. Over-rejection is sound (it
/// only refuses some always-safe casts); silent acceptance is not.
fn cast_operand_could_be_time(
    operand: &Expression,
    schema: &vibesql_catalog::TableSchema,
) -> bool {
    cast_operand_could_be_time_inner(operand, schema)
}

/// Is this operand **provably TIME without any schema** — i.e. known to
/// be a TIME value from its syntax alone (#5406)?
///
/// This is the schema-free dual of [`cast_operand_could_be_time`] used by
/// the *propose-side* query-body validator ([`StrictDetector`]), which
/// has no catalog access. The propose pass should reject a
/// `CAST(... AS TIMESTAMP)` in a query body only when the operand is
/// *certainly* a TIME value with no schema needed to prove it — a TIME
/// literal, `CURRENT_TIME`, `CAST(... AS TIME)`, or a `CASE` all of whose
/// branches are provably TIME. Schema-*dependent* operands (column
/// references, function calls) are left to the precise, catalog-resolved
/// apply-side audit ([`time_cast_query_violation_with_db`]), so that an
/// `INSERT … SELECT CAST(<other-table non-TIME column> AS TIMESTAMP)` is
/// no longer over-rejected at propose. Soundness is preserved: a
/// genuinely-TIME column operand that slips past propose is rejected
/// deterministically at apply.
fn cast_operand_is_provably_time(operand: &Expression) -> bool {
    match operand {
        Expression::Literal(SqlValue::Time(_)) => true,
        Expression::CurrentTime { .. } => true,
        Expression::Cast { data_type: DataType::Time { .. }, .. } => true,
        Expression::Case { when_clauses, else_result, .. } => {
            // Provably TIME only if *every* possible result is provably
            // TIME (including the implicit NULL else, which is not TIME) —
            // so an else-less CASE is never provably TIME.
            else_result
                .as_ref()
                .is_some_and(|e| cast_operand_is_provably_time(e))
                && when_clauses.iter().all(|w| cast_operand_is_provably_time(&w.result))
        }
        _ => false,
    }
}

fn cast_operand_could_be_time_inner(
    operand: &Expression,
    schema: &vibesql_catalog::TableSchema,
) -> bool {
    match operand {
        // Literals: only a TIME literal is TIME, and those are frozen at
        // propose, so a literal reaching apply is non-TIME. NULL casts to
        // a NULL timestamp deterministically.
        Expression::Literal(v) => matches!(v, SqlValue::Time(_)),

        // The apply-time clock reads that *produce* TIME.
        Expression::CurrentTime { .. } => true,

        // A column reference: provably non-TIME only when it binds to the
        // target table and that column's type is known and not TIME.
        // Other-table / unqualified-but-absent columns are unresolvable →
        // conservatively possibly-TIME.
        Expression::ColumnRef(col) => {
            let binds_to_target = col.table_canonical().is_none_or(|t| {
                t.eq_ignore_ascii_case(&schema.name) || t.eq_ignore_ascii_case("excluded")
            });
            if !binds_to_target {
                return true;
            }
            match schema
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(col.column_canonical()))
            {
                Some(c) => matches!(c.data_type, DataType::Time { .. }),
                // Unknown column on the target table: be conservative.
                None => true,
            }
        }

        // A nested cast fixes the output type: TIME-producing only when
        // the target type is TIME.
        Expression::Cast { data_type, .. } => matches!(data_type, DataType::Time { .. }),

        // CASE yields one of its branch results: possibly-TIME if any
        // result (or the implicit NULL else) could be TIME.
        Expression::Case { when_clauses, else_result, .. } => {
            when_clauses.iter().any(|w| cast_operand_could_be_time_inner(&w.result, schema))
                || else_result
                    .as_ref()
                    .is_some_and(|e| cast_operand_could_be_time_inner(e, schema))
        }

        // Structurally non-TIME result shapes: arithmetic / string
        // concatenation produce numbers or strings; comparison and
        // logical operators produce booleans; string/position value
        // functions produce strings or integers. EXTRACT yields a
        // numeric field. None of these can be TIME.
        Expression::BinaryOp { .. }
        | Expression::UnaryOp { .. }
        | Expression::Conjunction(_)
        | Expression::Disjunction(_)
        | Expression::IsNull { .. }
        | Expression::IsDistinctFrom { .. }
        | Expression::IsTruthValue { .. }
        | Expression::In { .. }
        | Expression::InList { .. }
        | Expression::Between { .. }
        | Expression::Like { .. }
        | Expression::Glob { .. }
        | Expression::Exists { .. }
        | Expression::QuantifiedComparison { .. }
        | Expression::Position { .. }
        | Expression::Trim { .. }
        | Expression::Extract { .. } => false,

        // CURRENT_DATE / CURRENT_TIMESTAMP are not TIME-typed (and freeze
        // at propose anyway).
        Expression::CurrentDate | Expression::CurrentTimestamp { .. } => false,

        // Everything else — function calls (which may return TIME, e.g.
        // `time(col)`), subqueries, window functions, sequences, etc. —
        // is not provably non-TIME, so reject conservatively.
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// Site classification (shared by validation, capture and substitution)
// ---------------------------------------------------------------------------

/// Is this node a freezable non-deterministic call site?
///
/// Evaluated **post-order** (children already frozen to literals), so
/// "arguments are literals" is the right const-ness test on both the
/// capture and the substitute path. Must stay in lockstep with
/// [`validate_node`]: anything the validator lets through to a
/// transformable root either matches here (and gets a frozen value) or
/// is deterministic.
fn is_freeze_site(expr: &Expression) -> bool {
    if is_time_to_timestamp_cast(expr) {
        return true;
    }
    match expr {
        Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. } => true,
        Expression::Function { name, args, .. } => {
            let n = name.canonical();
            let all_literal = args.iter().all(|a| matches!(a, Expression::Literal(_)));
            if volatility::is_random_function(n) || volatility::is_clock_function(n) {
                all_literal
            } else if volatility::is_datetime_family_function(n) {
                all_literal && datetime_uses_clock(n, args)
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Does a date/time-family call read the wall clock (or the node-local
/// timezone)? True when the time value is implicitly `'now'` (omitted)
/// or any literal string argument is `'now'`, `'localtime'`, or
/// `'utc'`.
fn datetime_uses_clock(canonical_name: &str, args: &[Expression]) -> bool {
    // PostgreSQL `age(x)` compares its argument against the current
    // date; the two-argument form is a pure function of its arguments.
    // age() takes DATE/TIMESTAMP values (string arguments are a
    // deterministic executor error), so the SQLite 'now'/'localtime'
    // string-modifier scan below does not apply to it.
    if canonical_name == "age" {
        return args.len() == 1;
    }
    let implicit_now = match canonical_name {
        // strftime(format[, time-value, ...]): one argument means the
        // time value defaults to 'now'.
        "strftime" => args.len() <= 1,
        // timediff(a, b) requires both time values; fewer arguments is
        // a (deterministic) executor error, not a clock read.
        "timediff" => false,
        _ => args.is_empty(),
    };
    implicit_now
        || args.iter().any(|a| {
            let (Expression::Literal(SqlValue::Varchar(s))
            | Expression::Literal(SqlValue::Character(s))) = a
            else {
                return false;
            };
            let t = s.as_str().trim().to_ascii_lowercase();
            t == "now" || t == "localtime" || t == "utc"
        })
}

/// Is this `CAST(<TIME literal> AS TIMESTAMP)`? SQL:1999 semantics
/// stamp the **current date** onto the time value (see the executor's
/// `evaluator/casting.rs`), so this cast is a clock read. With a
/// literal operand it freezes like the other clock readings (post-order
/// literalization also covers `CAST(CURRENT_TIME AS TIMESTAMP)`). The
/// column-operand form is a *per-row* clock read whose operand type is
/// only known against the schema, so it is rejected at apply by
/// [`time_cast_violation`]; TIME-typed operands that are neither
/// literals nor direct columns of the target table are tracked in
/// #5381.
fn is_time_to_timestamp_cast(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Cast { expr: operand, data_type: DataType::Timestamp { .. } }
            if matches!(**operand, Expression::Literal(SqlValue::Time(_)))
    )
}

/// Will this expression be a literal after the freeze pass replaces
/// nested freezable sites? (Const-ness check used by validation, which
/// runs *before* the transform.)
fn will_be_literal(expr: &Expression) -> bool {
    if is_time_to_timestamp_cast(expr) {
        return true;
    }
    match expr {
        Expression::Literal(_) => true,
        Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. } => true,
        Expression::Function { name, args, .. } => {
            let n = name.canonical();
            let freezable_name = volatility::is_random_function(n)
                || volatility::is_clock_function(n)
                || (volatility::is_datetime_family_function(n) && datetime_uses_clock(n, args));
            freezable_name && args.iter().all(will_be_literal)
        }
        _ => false,
    }
}

/// Is a stored DEFAULT expression non-deterministic? Matches both the
/// keyword forms (`Expression::CurrentTimestamp`, …) and the
/// function-call forms the parser stores for DEFAULT clauses
/// (`Expression::Function { name: "CURRENT_TIMESTAMP" }`).
fn default_expr_is_volatile(expr: &Expression) -> bool {
    if is_time_to_timestamp_cast(expr) {
        return true;
    }
    match expr {
        Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. } => true,
        Expression::Function { name, args, .. } => {
            let n = name.canonical();
            volatility::is_random_function(n)
                || volatility::is_clock_function(n)
                || volatility::is_session_state_function(n)
                || (volatility::is_datetime_family_function(n) && datetime_uses_clock(n, args))
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Validation (immutable pass; shared by propose and apply)
// ---------------------------------------------------------------------------

/// Evaluation cardinality of an expression root within its statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ctx {
    /// Evaluated exactly once per statement execution (an `INSERT …
    /// VALUES` cell, `LIMIT`/`OFFSET`): any volatile site can be frozen.
    Once,
    /// Evaluated once per affected row (`SET`, `WHERE`, `ORDER BY`):
    /// clock readings can be frozen (SQLite fixes `'now'` per statement
    /// anyway); per-row volatile functions cannot.
    PerRow,
}

/// Validate one write statement for deterministic replication. Returns
/// an explanatory reason if it cannot be frozen.
fn validate_statement(statement: &Statement) -> Result<(), String> {
    match statement {
        Statement::Insert(stmt) => validate_insert(stmt),
        Statement::Update(stmt) => validate_update(stmt),
        Statement::Delete(stmt) => validate_delete(stmt),
        Statement::CreateView(stmt) => {
            // A view body with volatile calls would smuggle apply-time
            // nondeterminism into every later statement that writes
            // through / selects from it. Reject at the definition.
            check_query_volatile_free(&stmt.query, "CREATE VIEW body")
        }
        Statement::CreateIndex(stmt) => {
            for column in &stmt.columns {
                if let IndexColumn::Expression { expr, .. } = column {
                    check_expr_volatile_free(expr, "index expression")?;
                }
            }
            if let Some(where_clause) = &stmt.where_clause {
                check_expr_volatile_free(where_clause, "partial index WHERE")?;
            }
            Ok(())
        }
        // A trigger body re-executes at apply time on every replica,
        // data-dependently — strict validation, like CREATE VIEW
        // (#5381). DROP TRIGGER carries no expressions.
        Statement::CreateTrigger(stmt) => validate_create_trigger(stmt),
        // CREATE TABLE stores DEFAULT expressions without evaluating
        // them — deterministic as DDL. INSERTs that would *fire* a
        // volatile default are frozen through the schema (or rejected)
        // by the DEFAULT freeze pass. Other statements either carry no
        // expressions or are rejected by the state machine's statement
        // dispatch.
        _ => Ok(()),
    }
}

/// Strict validation of a `CREATE TRIGGER` definition (#5381): the WHEN
/// clause and every body statement must be volatile-free, because the
/// body re-executes through the executor at apply time — per firing
/// row, data-dependently — where nothing can be frozen. Runs at propose
/// (actionable error before a log index is consumed) **and** at apply
/// (via [`substitute_statement`]'s re-validation: defense in depth for
/// entries proposed around the freeze pass). Schema-dependent hazards
/// (volatile DEFAULTs fired by body DML, TIME column casts) need
/// catalog access and are audited separately by
/// [`trigger_body_schema_violation`].
fn validate_create_trigger(stmt: &CreateTriggerStmt) -> Result<(), String> {
    if let Some(when_condition) = &stmt.when_condition {
        check_expr_volatile_free(when_condition, "trigger WHEN clause")?;
    }
    for statement in parse_trigger_body(stmt)? {
        check_trigger_body_statement(&statement)?;
    }
    Ok(())
}

/// Strict volatile-free check over one trigger-body statement. The
/// statement kinds mirror what the executor's trigger firing supports
/// (`TriggerFirer::execute_statement`): INSERT/UPDATE/DELETE/SELECT —
/// anything else is rejected here with a clearer error than the
/// fire-time one. `RETURNING` clauses are exempt for the same reason as
/// at top level (computed and discarded; never feeds replicated state).
fn check_trigger_body_statement(statement: &Statement) -> Result<(), String> {
    const CTX: &str = "trigger body";
    match statement {
        Statement::Insert(stmt) => {
            check_ctes_volatile_free(&stmt.with_clause)?;
            match &stmt.source {
                InsertSource::Values(rows) => {
                    for row in rows {
                        for expr in row {
                            check_expr_volatile_free(expr, CTX)?;
                        }
                    }
                }
                InsertSource::Select(query) => {
                    check_query_volatile_free(query, "trigger body INSERT … SELECT")?;
                }
                InsertSource::DefaultValues => {}
            }
            for on_conflict in &stmt.on_conflict {
                if let Some(items) = &on_conflict.conflict_target {
                    for item in items {
                        if let ConflictTargetItem::Expression(expr) = item {
                            check_expr_volatile_free(expr, CTX)?;
                        }
                    }
                }
                if let Some(target_where) = &on_conflict.target_where {
                    check_expr_volatile_free(target_where, CTX)?;
                }
                if let OnConflictAction::DoUpdate { assignments, where_clause } =
                    &on_conflict.action
                {
                    for assignment in assignments {
                        check_expr_volatile_free(&assignment.value, CTX)?;
                    }
                    if let Some(where_clause) = where_clause {
                        check_expr_volatile_free(where_clause, CTX)?;
                    }
                }
            }
            if let Some(assignments) = &stmt.on_duplicate_key_update {
                for assignment in assignments {
                    check_expr_volatile_free(&assignment.value, CTX)?;
                }
            }
            Ok(())
        }
        Statement::Update(stmt) => {
            check_ctes_volatile_free(&stmt.with_clause)?;
            if let Some(from_clauses) = &stmt.from_clause {
                for from in from_clauses {
                    check_from_volatile_free(from)?;
                }
            }
            for assignment in &stmt.assignments {
                check_expr_volatile_free(&assignment.value, CTX)?;
            }
            if let Some(WhereClause::Condition(expr)) = &stmt.where_clause {
                check_expr_volatile_free(expr, CTX)?;
            }
            Ok(())
        }
        Statement::Delete(stmt) => {
            check_ctes_volatile_free(&stmt.with_clause)?;
            if let Some(WhereClause::Condition(expr)) = &stmt.where_clause {
                check_expr_volatile_free(expr, CTX)?;
            }
            if let Some(order_by) = &stmt.order_by {
                for item in order_by {
                    check_expr_volatile_free(&item.expr, CTX)?;
                }
            }
            if let Some(limit) = &stmt.limit {
                check_expr_volatile_free(limit, CTX)?;
            }
            if let Some(offset) = &stmt.offset {
                check_expr_volatile_free(offset, CTX)?;
            }
            Ok(())
        }
        Statement::Select(query) => check_query_volatile_free(query, "trigger body SELECT"),
        other => Err(format!(
            "statement is not supported in a replicated trigger body (the apply-time executor \
             fires INSERT/UPDATE/DELETE/SELECT only): {other:?}"
        )),
    }
}

fn validate_insert(stmt: &InsertStmt) -> Result<(), String> {
    check_ctes_volatile_free(&stmt.with_clause)?;
    match &stmt.source {
        InsertSource::Values(rows) => {
            for row in rows {
                for expr in row {
                    validate_root(expr, Ctx::Once)?;
                }
            }
        }
        InsertSource::Select(query) => check_query_volatile_free(query, "INSERT … SELECT")?,
        InsertSource::DefaultValues => {}
    }
    for on_conflict in &stmt.on_conflict {
        // Conflict targets are matched *structurally* against index
        // definitions — they must never be rewritten, so volatile calls
        // there are rejected rather than frozen.
        if let Some(items) = &on_conflict.conflict_target {
            for item in items {
                if let ConflictTargetItem::Expression(expr) = item {
                    check_expr_volatile_free(expr, "ON CONFLICT target")?;
                }
            }
        }
        if let Some(target_where) = &on_conflict.target_where {
            check_expr_volatile_free(target_where, "ON CONFLICT target WHERE")?;
        }
        if let OnConflictAction::DoUpdate { assignments, where_clause } = &on_conflict.action {
            validate_assignments(assignments, Ctx::PerRow)?;
            if let Some(where_clause) = where_clause {
                validate_root(where_clause, Ctx::PerRow)?;
            }
        }
    }
    if let Some(assignments) = &stmt.on_duplicate_key_update {
        validate_assignments(assignments, Ctx::PerRow)?;
    }
    // RETURNING is intentionally exempt: its output is computed and
    // discarded at apply time and never feeds replicated state.
    Ok(())
}

fn validate_update(stmt: &UpdateStmt) -> Result<(), String> {
    check_ctes_volatile_free(&stmt.with_clause)?;
    if let Some(from_clauses) = &stmt.from_clause {
        for from in from_clauses {
            check_from_volatile_free(from)?;
        }
    }
    validate_assignments(&stmt.assignments, Ctx::PerRow)?;
    if let Some(WhereClause::Condition(expr)) = &stmt.where_clause {
        validate_root(expr, Ctx::PerRow)?;
    }
    Ok(())
}

fn validate_delete(stmt: &DeleteStmt) -> Result<(), String> {
    check_ctes_volatile_free(&stmt.with_clause)?;
    if let Some(WhereClause::Condition(expr)) = &stmt.where_clause {
        validate_root(expr, Ctx::PerRow)?;
    }
    if let Some(order_by) = &stmt.order_by {
        for item in order_by {
            validate_root(&item.expr, Ctx::PerRow)?;
        }
    }
    if let Some(limit) = &stmt.limit {
        validate_root(limit, Ctx::Once)?;
    }
    if let Some(offset) = &stmt.offset {
        validate_root(offset, Ctx::Once)?;
    }
    Ok(())
}

fn validate_assignments(assignments: &[Assignment], ctx: Ctx) -> Result<(), String> {
    for assignment in assignments {
        validate_root(&assignment.value, ctx)?;
    }
    Ok(())
}

/// Validate one transformable expression root in context `ctx`.
fn validate_root(expr: &Expression, ctx: Ctx) -> Result<(), String> {
    let mut validator = RootValidator { ctx, error: None };
    let _ = walk_expression(&mut validator, expr);
    match validator.error {
        Some(reason) => Err(reason),
        None => Ok(()),
    }
}

struct RootValidator {
    ctx: Ctx,
    error: Option<String>,
}

impl RootValidator {
    fn reject(&mut self, reason: String) -> VisitResult {
        self.error = Some(reason);
        VisitResult::Stop
    }

    fn check_subquery(&mut self, query: &SelectStmt) -> VisitResult {
        if let Err(reason) = check_query_volatile_free(query, "subquery") {
            return self.reject(reason);
        }
        VisitResult::Skip
    }
}

impl ExpressionVisitor for RootValidator {
    fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
        if self.error.is_some() {
            return VisitResult::Stop;
        }
        match expr {
            // Subqueries evaluate data-dependently; volatile calls
            // inside them cannot be frozen to a single value. The
            // non-subquery operand (e.g. the left side of IN) is still
            // validated in this root's context.
            Expression::ScalarSubquery(query) => self.check_subquery(query),
            Expression::Exists { subquery, .. } => self.check_subquery(subquery),
            Expression::In { expr: operand, subquery, .. }
            | Expression::QuantifiedComparison { expr: operand, subquery, .. } => {
                if walk_expression(self, operand).should_stop() {
                    return VisitResult::Stop;
                }
                self.check_subquery(subquery)
            }
            Expression::CurrentDate
            | Expression::CurrentTime { .. }
            | Expression::CurrentTimestamp { .. } => VisitResult::Continue,
            Expression::Function { name, args, .. } => {
                let n = name.canonical();
                if volatility::is_session_state_function(n) {
                    return self.reject(format!(
                        "{n}() reads session state that is not part of replicated database \
                         state; it cannot be replicated (#5377)"
                    ));
                }
                if volatility::is_random_function(n) {
                    if self.ctx == Ctx::PerRow {
                        return self.reject(format!(
                            "{n}() in a per-row context (SET/WHERE/ORDER BY) cannot be frozen \
                             to a single value without changing its semantics; use explicit \
                             values instead (#5377)"
                        ));
                    }
                    if !args.iter().all(will_be_literal) {
                        return self.reject(format!(
                            "{n}() with non-constant arguments cannot be evaluated at propose \
                             time (#5377)"
                        ));
                    }
                    return VisitResult::Continue;
                }
                if volatility::is_clock_function(n)
                    || (volatility::is_datetime_family_function(n) && datetime_uses_clock(n, args))
                {
                    if !args.iter().all(will_be_literal) {
                        return self.reject(format!(
                            "{n}() reads the clock but has non-constant arguments, so it \
                             cannot be evaluated at propose time (#5377)"
                        ));
                    }
                    return VisitResult::Continue;
                }
                VisitResult::Continue
            }
            _ => VisitResult::Continue,
        }
    }
}

/// Reject any non-deterministic call anywhere in `query` (used for
/// query-bearing statement parts that are never transformed).
fn check_query_volatile_free(query: &SelectStmt, context: &str) -> Result<(), String> {
    let mut detector = StrictDetector { context, error: None };
    walk_select(&mut detector, query);
    match detector.error {
        Some(reason) => Err(reason),
        None => Ok(()),
    }
}

/// Reject any non-deterministic call anywhere in `expr`.
fn check_expr_volatile_free(expr: &Expression, context: &str) -> Result<(), String> {
    let mut detector = StrictDetector { context, error: None };
    let _ = walk_expression(&mut detector, expr);
    match detector.error {
        Some(reason) => Err(reason),
        None => Ok(()),
    }
}

/// Reject any non-deterministic call anywhere in an `UPDATE … FROM`
/// clause (derived tables, join conditions, VALUES rows).
fn check_from_volatile_free(from: &FromClause) -> Result<(), String> {
    match from {
        FromClause::Table { .. } => Ok(()),
        FromClause::Subquery { query, .. } => check_query_volatile_free(query, "FROM subquery"),
        FromClause::Join { left, right, condition, .. } => {
            check_from_volatile_free(left)?;
            check_from_volatile_free(right)?;
            if let Some(condition) = condition {
                check_expr_volatile_free(condition, "join condition")?;
            }
            Ok(())
        }
        FromClause::Values { rows, .. } => {
            for row in rows {
                for expr in row {
                    check_expr_volatile_free(expr, "FROM VALUES")?;
                }
            }
            Ok(())
        }
        FromClause::TableFunction { args, .. } => {
            for expr in args {
                check_expr_volatile_free(expr, "FROM table function")?;
            }
            Ok(())
        }
    }
}

fn check_ctes_volatile_free(ctes: &Option<Vec<CommonTableExpr>>) -> Result<(), String> {
    if let Some(ctes) = ctes {
        for cte in ctes {
            check_query_volatile_free(&cte.query, "WITH clause")?;
        }
    }
    Ok(())
}

/// Rejects every non-deterministic call site, including deterministic-
/// looking clock reads — used for query-bearing parts where evaluation
/// cardinality is data-dependent.
struct StrictDetector<'a> {
    context: &'a str,
    error: Option<String>,
}

impl ExpressionVisitor for StrictDetector<'_> {
    fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
        if self.error.is_some() {
            return VisitResult::Stop;
        }
        let volatile = match expr {
            // A TIME→TIMESTAMP cast stamps the apply-time current date
            // (SQL:1999). In a query-bearing part the evaluation count is
            // data-dependent, so it cannot be frozen at propose. With no
            // catalog in scope here, reject only the operands that are
            // *provably* TIME from their syntax alone (TIME literal,
            // CURRENT_TIME, CAST(... AS TIME)); schema-dependent operands
            // (column refs, function calls) are resolved precisely against
            // the replicated catalog by the apply-side audit
            // ([`time_cast_query_violation_with_db`], #5406), so a cast
            // over a provably-non-TIME other-table column is no longer
            // over-rejected at propose. A genuinely-TIME column operand
            // that slips past here is rejected deterministically at apply.
            Expression::Cast { expr: operand, data_type: DataType::Timestamp { .. } }
                if cast_operand_is_provably_time(operand) =>
            {
                Some("CAST(<TIME-typed expression> AS TIMESTAMP) clock reading (stamps the current date)".to_string())
            }
            Expression::CurrentDate
            | Expression::CurrentTime { .. }
            | Expression::CurrentTimestamp { .. } => Some("CURRENT_* clock reading".to_string()),
            Expression::Function { name, args, .. } => {
                let n = name.canonical();
                let hit = volatility::is_random_function(n)
                    || volatility::is_session_state_function(n)
                    || volatility::is_clock_function(n)
                    || (volatility::is_datetime_family_function(n) && datetime_uses_clock(n, args));
                hit.then(|| format!("{n}()"))
            }
            _ => None,
        };
        if let Some(what) = volatile {
            self.error = Some(format!(
                "non-deterministic {what} inside a {} cannot be replicated: its evaluation \
                 count is data-dependent, so it cannot be frozen to a single value (#5377)",
                self.context
            ));
            return VisitResult::Stop;
        }
        VisitResult::Continue
    }
}

// ---------------------------------------------------------------------------
// Capture / substitution (the shared deterministic transform)
// ---------------------------------------------------------------------------

enum Mode<'a> {
    Capture { values: Vec<FrozenValue>, error: Option<FreezeError> },
    Substitute { values: &'a [FrozenValue], cursor: usize },
}

struct SiteTransformer<'a> {
    mode: Mode<'a>,
}

impl<'a> SiteTransformer<'a> {
    fn capture() -> Self {
        Self { mode: Mode::Capture { values: Vec::new(), error: None } }
    }

    fn substitute(values: &'a [FrozenValue]) -> Self {
        Self { mode: Mode::Substitute { values, cursor: 0 } }
    }
}

impl ExpressionMutVisitor for SiteTransformer<'_> {
    fn post_visit_expression(&mut self, expr: Expression) -> Expression {
        if !is_freeze_site(&expr) {
            return expr;
        }
        match &mut self.mode {
            Mode::Capture { error: Some(_), .. } => expr,
            Mode::Capture { values, error } => match evaluate_site(&expr) {
                Ok(frozen) => {
                    let literal = SqlValue::from(frozen.clone());
                    values.push(frozen);
                    Expression::Literal(literal)
                }
                Err(e) => {
                    *error = Some(e);
                    expr
                }
            },
            Mode::Substitute { values, cursor } => {
                let site = *cursor;
                *cursor += 1;
                match values.get(site) {
                    Some(frozen) => Expression::Literal(SqlValue::from(frozen.clone())),
                    // Mismatch: leave the node; substitute_statement
                    // reports it from the final cursor count.
                    None => expr,
                }
            }
        }
    }
}

/// Evaluate one freezable site on the proposer. The expression is
/// constant by construction (validation + post-order literalization of
/// nested sites), so an empty schema/row/database context suffices.
fn evaluate_site(expr: &Expression) -> Result<FrozenValue, FreezeError> {
    let schema = vibesql_catalog::TableSchema::new("__freeze__".to_string(), vec![]);
    let database = vibesql_storage::Database::new();
    let row = vibesql_storage::Row::new(vec![]);
    let value = vibesql_executor::ExpressionEvaluator::with_database(&schema, &database)
        .eval(expr, &row)
        .map_err(|e| FreezeError::Eval(e.to_string()))?;
    FrozenValue::try_from(value).map_err(FreezeError::Eval)
}

/// Run the transform over every freezable expression root of the
/// statement, in a fixed order shared by capture and substitution.
/// Query-bearing parts (validated volatile-free) and RETURNING clauses
/// (apply-time output only) are deliberately not traversed.
fn transform_statement_sites(statement: Statement, visitor: &mut SiteTransformer) -> Statement {
    match statement {
        Statement::Insert(mut stmt) => {
            if let InsertSource::Values(rows) = stmt.source {
                stmt.source = InsertSource::Values(
                    rows.into_iter()
                        .map(|row| {
                            row.into_iter().map(|e| transform_expression(visitor, e)).collect()
                        })
                        .collect(),
                );
            }
            stmt.on_conflict = stmt
                .on_conflict
                .into_iter()
                .map(|mut on_conflict| {
                    if let OnConflictAction::DoUpdate { assignments, where_clause } =
                        on_conflict.action
                    {
                        on_conflict.action = OnConflictAction::DoUpdate {
                            assignments: transform_assignments(assignments, visitor),
                            where_clause: where_clause.map(|e| transform_expression(visitor, e)),
                        };
                    }
                    on_conflict
                })
                .collect();
            if let Some(assignments) = stmt.on_duplicate_key_update.take() {
                stmt.on_duplicate_key_update = Some(transform_assignments(assignments, visitor));
            }
            Statement::Insert(stmt)
        }
        Statement::Update(mut stmt) => {
            stmt.assignments = transform_assignments(stmt.assignments, visitor);
            if let Some(WhereClause::Condition(expr)) = stmt.where_clause.take() {
                stmt.where_clause =
                    Some(WhereClause::Condition(transform_expression(visitor, expr)));
            }
            Statement::Update(stmt)
        }
        Statement::Delete(mut stmt) => {
            if let Some(WhereClause::Condition(expr)) = stmt.where_clause.take() {
                stmt.where_clause =
                    Some(WhereClause::Condition(transform_expression(visitor, expr)));
            }
            if let Some(order_by) = stmt.order_by.take() {
                stmt.order_by = Some(
                    order_by
                        .into_iter()
                        .map(|mut item| {
                            item.expr = transform_expression(visitor, item.expr);
                            item
                        })
                        .collect(),
                );
            }
            if let Some(limit) = stmt.limit.take() {
                stmt.limit = Some(transform_expression(visitor, limit));
            }
            if let Some(offset) = stmt.offset.take() {
                stmt.offset = Some(transform_expression(visitor, offset));
            }
            Statement::Delete(stmt)
        }
        other => other,
    }
}

fn transform_assignments(
    assignments: Vec<Assignment>,
    visitor: &mut SiteTransformer,
) -> Vec<Assignment> {
    assignments
        .into_iter()
        .map(|mut a| {
            // `SET col = DEFAULT` is audited against the schema by
            // `volatile_default_violation_update`, not rewritten here.
            a.value = transform_expression(visitor, a.value);
            a
        })
        .collect()
}

// ---------------------------------------------------------------------------
// DEFAULT-clause audit helpers
// ---------------------------------------------------------------------------

/// Would executing `stmt` fill in the DEFAULT for `column_name` (at
/// schema position `column_idx`) in any row?
fn insert_fires_default(stmt: &InsertStmt, column_name: &str, column_idx: usize) -> bool {
    let rows = match &stmt.source {
        InsertSource::DefaultValues => return true,
        // Values produced by a SELECT are not statically known, and a
        // NULL output also fires the default — conservatively treat the
        // default as fired unless every column is explicitly... it
        // cannot be verified, so: fired.
        InsertSource::Select(_) => return true,
        InsertSource::Values(rows) => rows,
    };

    let position = if stmt.columns.is_empty() {
        // Positional insert: the column sits at its schema index. Rows
        // shorter than the schema are a (deterministic) executor error.
        Some(column_idx)
    } else {
        stmt.columns.iter().position(|c| c.eq_ignore_ascii_case(column_name))
    };
    let Some(position) = position else {
        // Column omitted from an explicit column list: default fires.
        return true;
    };
    rows.iter().any(|row| {
        match row.get(position) {
            // Explicit DEFAULT keyword, or explicit NULL (VibeSQL
            // applies column defaults to NULL values).
            Some(Expression::Default) | Some(Expression::Literal(SqlValue::Null)) => true,
            // A literal value cannot evaluate to NULL: never fires.
            Some(Expression::Literal(_)) => false,
            // Any other expression *may* evaluate to NULL (e.g.
            // `nullif(1, 1)`, a scalar subquery, `NEW.col` in a trigger
            // body), which fires the default — unprovable statically, so
            // conservatively treat it as a fire (#5381).
            Some(_) => true,
            // Row too short: deterministic executor error, not a
            // default fire.
            None => false,
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> Statement {
        vibesql_parser::parse_with_arena_fallback(sql).unwrap()
    }

    /// Freeze + substitute roundtrip: the substituted statement carries
    /// exactly the frozen values as literals.
    #[test]
    fn freeze_then_substitute_replaces_sites_with_the_frozen_literals() {
        let sql = "INSERT INTO t VALUES (random(), datetime('now'), randomblob(8))";
        let frozen = freeze_statement(sql).unwrap();
        assert_eq!(frozen.len(), 3);
        assert!(matches!(frozen[0], FrozenValue::Integer(_)), "random() -> {:?}", frozen[0]);
        // VibeSQL's datetime() evaluates to a structured TIMESTAMP value.
        assert!(
            matches!(&frozen[1], FrozenValue::Timestamp { .. }),
            "datetime('now') -> {:?}",
            frozen[1]
        );
        assert!(
            matches!(&frozen[2], FrozenValue::Blob(b) if b.len() == 8),
            "randomblob(8) -> {:?}",
            frozen[2]
        );

        let substituted = substitute_statement(parse(sql), &frozen).unwrap();
        let Statement::Insert(stmt) = substituted else { panic!("not an insert") };
        let InsertSource::Values(rows) = &stmt.source else { panic!("not values") };
        let literals: Vec<&Expression> = rows[0].iter().collect();
        for (expr, frozen) in literals.iter().zip(&frozen) {
            assert_eq!(
                **expr,
                Expression::Literal(SqlValue::from(frozen.clone())),
                "substituted literal must equal the frozen value"
            );
        }
    }

    #[test]
    fn deterministic_statements_freeze_to_nothing() {
        for sql in [
            "INSERT INTO t VALUES (1, 'a')",
            "UPDATE t SET x = strftime('%s', created_at) WHERE id = 1",
            "DELETE FROM t WHERE x = datetime(created_at, '+1 day')",
            "CREATE TABLE t (id INTEGER PRIMARY KEY, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        ] {
            assert_eq!(freeze_statement(sql).unwrap(), vec![], "{sql}");
        }
    }

    #[test]
    fn clock_readings_freeze_in_per_row_contexts() {
        let frozen =
            freeze_statement("UPDATE t SET updated_at = CURRENT_TIMESTAMP WHERE id > 3").unwrap();
        assert_eq!(frozen.len(), 1);
        assert!(matches!(frozen[0], FrozenValue::Timestamp { .. }), "got {:?}", frozen[0]);

        let frozen = freeze_statement("DELETE FROM t WHERE ts < datetime('now')").unwrap();
        assert_eq!(frozen.len(), 1);
    }

    /// The MySQL clock aliases dispatch to the same executor
    /// implementations as `CURRENT_DATE`/`CURRENT_TIME` (PR #5382 review:
    /// they previously froze to zero sites and diverged replicas).
    #[test]
    fn mysql_clock_aliases_freeze() {
        let frozen = freeze_statement("INSERT INTO t VALUES (curdate(), curtime())").unwrap();
        assert_eq!(frozen.len(), 2);
        assert!(matches!(frozen[0], FrozenValue::Date { .. }), "curdate() -> {:?}", frozen[0]);
        assert!(matches!(frozen[1], FrozenValue::Time { .. }), "curtime() -> {:?}", frozen[1]);

        // ... including in per-row contexts (clock semantics).
        let frozen = freeze_statement("UPDATE t SET d = curdate() WHERE id = 1").unwrap();
        assert_eq!(frozen.len(), 1);
    }

    /// PostgreSQL `age(x)` compares against the current date — a clock
    /// read; the two-argument form is a pure function of its arguments.
    #[test]
    fn one_arg_age_freezes_and_two_arg_age_is_deterministic() {
        let sql = "INSERT INTO t VALUES (age(DATE '2020-01-02'))";
        let frozen = freeze_statement(sql).unwrap();
        assert_eq!(frozen.len(), 1);
        assert!(matches!(&frozen[0], FrozenValue::Varchar(_)), "age(x) -> {:?}", frozen[0]);
        substitute_statement(parse(sql), &frozen).unwrap();

        let frozen =
            freeze_statement("INSERT INTO t VALUES (age(DATE '2024-06-11', DATE '2020-01-02'))")
                .unwrap();
        assert_eq!(frozen, vec![], "two-argument age() is deterministic");

        // age(col): a per-row clock read whose result depends on the
        // row — cannot be frozen to one value.
        let err = freeze_statement("UPDATE t SET x = age(d)").unwrap_err();
        assert!(matches!(&err, FreezeError::NotReplicable(m) if m.contains("clock")), "{err:?}");
    }

    /// SQL:1999 `CAST(TIME AS TIMESTAMP)` stamps the *current date*: the
    /// literal-operand form freezes like other clock reads (including
    /// the nested `CAST(CURRENT_TIME AS TIMESTAMP)` via post-order
    /// literalization); inside query-bearing parts it is rejected.
    #[test]
    fn time_literal_cast_to_timestamp_freezes() {
        let sql = "INSERT INTO t VALUES (CAST(TIME '12:34:56' AS TIMESTAMP))";
        let frozen = freeze_statement(sql).unwrap();
        assert_eq!(frozen.len(), 1);
        assert!(
            matches!(frozen[0], FrozenValue::Timestamp { hour: 12, minute: 34, second: 56, .. }),
            "the frozen timestamp must carry the literal's time part: {:?}",
            frozen[0]
        );
        substitute_statement(parse(sql), &frozen).unwrap();

        // Deterministic casts to TIMESTAMP are untouched.
        let frozen =
            freeze_statement("INSERT INTO t VALUES (CAST('2026-06-11 12:00:00' AS TIMESTAMP))")
                .unwrap();
        assert_eq!(frozen, vec![]);

        // Post-order: CURRENT_TIME freezes to a TIME literal first, which
        // then makes the cast itself a (second) freeze site.
        let sql = "INSERT INTO t VALUES (CAST(CURRENT_TIME AS TIMESTAMP))";
        let frozen = freeze_statement(sql).unwrap();
        assert_eq!(frozen.len(), 2, "inner clock read + the cast's date stamp");
        assert!(matches!(frozen[0], FrozenValue::Time { .. }), "got {:?}", frozen[0]);
        assert!(matches!(frozen[1], FrozenValue::Timestamp { .. }), "got {:?}", frozen[1]);
        substitute_statement(parse(sql), &frozen).unwrap();

        // Inside query-bearing parts the evaluation count is
        // data-dependent: rejected, like the other clock reads.
        let err =
            freeze_statement("INSERT INTO t SELECT CAST(TIME '12:34:56' AS TIMESTAMP) FROM s")
                .unwrap_err();
        assert!(matches!(err, FreezeError::NotReplicable(_)), "{err:?}");
    }

    #[test]
    fn per_row_random_is_rejected() {
        for sql in [
            "UPDATE t SET x = random()",
            "UPDATE t SET x = 1 WHERE random() > 0",
            "DELETE FROM t WHERE random() % 2 = 0",
        ] {
            let err = freeze_statement(sql).unwrap_err();
            assert!(
                matches!(&err, FreezeError::NotReplicable(m) if m.contains("per-row")),
                "{sql}: {err:?}"
            );
        }
    }

    #[test]
    fn session_state_functions_are_rejected() {
        for sql in [
            "INSERT INTO t VALUES (last_insert_rowid())",
            "UPDATE t SET x = changes()",
            "INSERT INTO t VALUES (total_changes())",
            // Identity / server-information functions: deterministic
            // today only via the session-defaults invariant; rejected so
            // the freeze pass stays robust if that invariant weakens.
            "INSERT INTO t VALUES (user())",
            "INSERT INTO t VALUES (current_user())",
            "UPDATE t SET x = database()",
            "INSERT INTO t VALUES (version())",
        ] {
            let err = freeze_statement(sql).unwrap_err();
            assert!(
                matches!(&err, FreezeError::NotReplicable(m) if m.contains("session state")),
                "{sql}: {err:?}"
            );
        }
    }

    #[test]
    fn volatile_inside_query_bearing_parts_is_rejected() {
        for sql in [
            "INSERT INTO t SELECT random() FROM s",
            "INSERT INTO t VALUES ((SELECT random()))",
            "INSERT INTO t VALUES ((SELECT max(x) FROM s WHERE y = random()))",
            "UPDATE t SET x = 1 WHERE id IN (SELECT id FROM s WHERE r = random())",
            "WITH c AS (SELECT random() AS r FROM s) INSERT INTO t SELECT r FROM c",
            "CREATE VIEW v AS SELECT random() AS r",
            "CREATE INDEX i ON t (datetime('now'))",
        ] {
            let err = freeze_statement(sql).unwrap_err();
            assert!(matches!(err, FreezeError::NotReplicable(_)), "{sql}: {err:?}");
        }
    }

    /// Subqueries without volatile calls pass through untouched, even
    /// next to a frozen site in the same statement.
    #[test]
    fn deterministic_subqueries_are_allowed() {
        let frozen =
            freeze_statement("INSERT INTO t VALUES ((SELECT max(id) FROM s), random())").unwrap();
        assert_eq!(frozen.len(), 1);
    }

    /// Nested freezable sites freeze inside-out (post-order): the inner
    /// site becomes a literal, which can make the outer call
    /// deterministic — or freezable in turn.
    #[test]
    fn nested_volatile_arguments_freeze_inside_out() {
        // unixepoch() freezes (inner site), but the arithmetic around it
        // is not folded to a literal, so randomblob()'s argument is not
        // constant at propose time. Validation rejects it.
        let err =
            freeze_statement("INSERT INTO t VALUES (randomblob(unixepoch() % 8 + 1))").unwrap_err();
        assert!(matches!(err, FreezeError::NotReplicable(_)), "{err:?}");

        // datetime('now') freezes first (post-order); the outer
        // unixepoch(<frozen literal>) no longer reads the clock and is
        // left for deterministic apply-time evaluation.
        let sql = "INSERT INTO t VALUES (unixepoch(datetime('now')))";
        let frozen = freeze_statement(sql).unwrap();
        assert_eq!(frozen.len(), 1, "only the inner clock read freezes");
        assert!(matches!(frozen[0], FrozenValue::Timestamp { .. }), "got {:?}", frozen[0]);
        substitute_statement(parse(sql), &frozen).unwrap();
    }

    #[test]
    fn substitute_rejects_unfrozen_volatile_sites() {
        let err = substitute_statement(parse("INSERT INTO t VALUES (random())"), &[]).unwrap_err();
        assert!(matches!(err, SubstituteError::UnfrozenVolatile(_)), "{err:?}");

        // ... including in query-bearing parts (validation re-runs).
        let err =
            substitute_statement(parse("INSERT INTO t SELECT random() FROM s"), &[]).unwrap_err();
        assert!(matches!(err, SubstituteError::UnfrozenVolatile(_)), "{err:?}");

        // ... and session-state functions, which are never freezable.
        let err = substitute_statement(
            parse("INSERT INTO t VALUES (last_insert_rowid())"),
            &[FrozenValue::Integer(7)],
        )
        .unwrap_err();
        assert!(matches!(err, SubstituteError::UnfrozenVolatile(_)), "{err:?}");
    }

    #[test]
    fn substitute_count_mismatch_is_flagged_as_version_skew() {
        let err = substitute_statement(
            parse("INSERT INTO t VALUES (random())"),
            &[FrozenValue::Integer(1), FrozenValue::Integer(2)],
        )
        .unwrap_err();
        assert!(matches!(err, SubstituteError::FrozenSiteMismatch(_)), "{err:?}");

        let err = substitute_statement(
            parse("INSERT INTO t VALUES (random(), random())"),
            &[FrozenValue::Integer(1)],
        )
        .unwrap_err();
        assert!(matches!(err, SubstituteError::FrozenSiteMismatch(_)), "{err:?}");
    }

    #[test]
    fn unparseable_sql_freezes_to_nothing() {
        // The apply path rejects it with the same parse error on every
        // replica — already deterministic.
        assert_eq!(freeze_statement("NOT VALID SQL AT ALL").unwrap(), vec![]);
    }

    #[test]
    fn frozen_value_roundtrips_through_sqlvalue_and_serde() {
        let values = vec![
            FrozenValue::Null,
            FrozenValue::Boolean(true),
            FrozenValue::Integer(-42),
            FrozenValue::Real(1.5),
            FrozenValue::Varchar("2026-06-11 12:00:00".to_string()),
            FrozenValue::Date { year: 2026, month: 6, day: 11 },
            FrozenValue::Time { hour: 1, minute: 2, second: 3, nanosecond: 4 },
            FrozenValue::Timestamp {
                year: 2026,
                month: 6,
                day: 11,
                hour: 12,
                minute: 0,
                second: 0,
                nanosecond: 0,
            },
            FrozenValue::Blob(vec![1, 2, 3]),
        ];
        for v in values {
            let through_sql = FrozenValue::try_from(SqlValue::from(v.clone())).unwrap();
            assert_eq!(through_sql, v);
            let json = serde_json::to_vec(&v).unwrap();
            assert_eq!(serde_json::from_slice::<FrozenValue>(&json).unwrap(), v);
        }
    }

    #[test]
    fn interval_values_cannot_be_frozen() {
        let err =
            FrozenValue::try_from(SqlValue::Interval(vibesql_types::Interval::new(String::new())))
                .unwrap_err();
        assert!(err.contains("cannot be frozen"), "{err}");
    }

    // -- DEFAULT-clause audit ------------------------------------------------

    fn users_schema_with_volatile_default() -> vibesql_catalog::TableSchema {
        let Statement::CreateTable(stmt) = parse(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, \
             ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        ) else {
            panic!("not a create table");
        };
        let mut db = vibesql_storage::Database::new();
        vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        db.get_table("users").unwrap().schema.clone()
    }

    #[test]
    fn volatile_default_fires_when_column_omitted_or_defaulted() {
        let schema = users_schema_with_volatile_default();
        for sql in [
            "INSERT INTO users (id, name) VALUES (1, 'a')",
            "INSERT INTO users (id, name, ts) VALUES (1, 'a', DEFAULT)",
            "INSERT INTO users (id, name, ts) VALUES (1, 'a', NULL)",
            "INSERT INTO users VALUES (1, 'a', NULL)",
            "INSERT INTO users DEFAULT VALUES",
            "INSERT INTO users SELECT id, name, ts FROM staging",
        ] {
            let Statement::Insert(stmt) = parse(sql) else { panic!("not an insert: {sql}") };
            assert!(
                volatile_default_violation(&stmt, &schema).is_some(),
                "{sql} must fire the volatile default"
            );
        }
    }

    #[test]
    fn volatile_default_quiet_when_value_supplied() {
        let schema = users_schema_with_volatile_default();
        for sql in [
            "INSERT INTO users (id, name, ts) VALUES (1, 'a', '2026-06-11 12:00:00')",
            "INSERT INTO users VALUES (1, 'a', '2026-06-11 12:00:00')",
        ] {
            let Statement::Insert(stmt) = parse(sql) else { panic!("not an insert: {sql}") };
            assert_eq!(volatile_default_violation(&stmt, &schema), None, "{sql}");
        }
    }

    #[test]
    fn update_set_default_with_volatile_default_is_flagged() {
        let schema = users_schema_with_volatile_default();
        let Statement::Update(stmt) = parse("UPDATE users SET ts = DEFAULT WHERE id = 1") else {
            panic!("not an update");
        };
        assert!(volatile_default_violation_update(&stmt, &schema).is_some());

        let Statement::Update(stmt) = parse("UPDATE users SET name = DEFAULT WHERE id = 1") else {
            panic!("not an update");
        };
        assert_eq!(volatile_default_violation_update(&stmt, &schema), None);
    }

    // -- TIME-column cast audit ----------------------------------------------

    fn sched_schema_with_time_column() -> vibesql_catalog::TableSchema {
        let Statement::CreateTable(stmt) =
            parse("CREATE TABLE sched (id INTEGER PRIMARY KEY, t TIME, s TEXT)")
        else {
            panic!("not a create table");
        };
        let mut db = vibesql_storage::Database::new();
        vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        db.get_table("sched").unwrap().schema.clone()
    }

    #[test]
    fn time_column_cast_to_timestamp_is_flagged() {
        let schema = sched_schema_with_time_column();
        for sql in [
            "UPDATE sched SET s = CAST(t AS TIMESTAMP) WHERE id = 1",
            "UPDATE sched SET s = 'x' WHERE CAST(t AS TIMESTAMP) > '2026-01-01 00:00:00'",
            "UPDATE sched SET s = CAST(sched.t AS TIMESTAMP)",
            "DELETE FROM sched WHERE CAST(t AS TIMESTAMP) < '2026-01-01 00:00:00'",
            "INSERT INTO sched VALUES (1, '12:00:00', 'x') \
             ON CONFLICT (id) DO UPDATE SET s = CAST(excluded.t AS TIMESTAMP)",
        ] {
            let violation = time_cast_violation(&parse(sql), &schema);
            assert!(
                violation.as_deref().is_some_and(|m| m.contains("TIME column")),
                "{sql} must be flagged, got: {violation:?}"
            );
        }
    }

    /// False-positive guard (#5398): TIME→TIMESTAMP casts whose operand
    /// is *provably* non-TIME must keep passing. The conservative
    /// residual rejection (below) must not catch these.
    #[test]
    fn provably_non_time_casts_are_not_flagged() {
        let schema = sched_schema_with_time_column();
        for sql in [
            // Target TEXT column: CAST to TIMESTAMP is a deterministic
            // parse, no clock read.
            "UPDATE sched SET s = CAST(s AS TIMESTAMP)",
            // Casting the TIME column to something other than TIMESTAMP.
            "UPDATE sched SET s = CAST(t AS TEXT)",
            // Non-TIME literal operands (post-freeze, literals are what a
            // frozen TIME literal becomes too).
            "UPDATE sched SET s = CAST('2026-06-11 12:00:00' AS TIMESTAMP)",
            "UPDATE sched SET s = CAST(1700000000 AS TIMESTAMP)",
            "UPDATE sched SET s = CAST(NULL AS TIMESTAMP)",
            // Nested cast fixes a non-TIME output type.
            "UPDATE sched SET s = CAST(CAST(t AS DATE) AS TIMESTAMP)",
            // CURRENT_TIMESTAMP / CURRENT_DATE are not TIME-typed.
            "UPDATE sched SET s = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)",
            // Arithmetic / string shapes are structurally non-TIME.
            "UPDATE sched SET s = CAST(id + 1 AS TIMESTAMP)",
            "UPDATE sched SET s = CAST(EXTRACT(HOUR FROM t) AS TIMESTAMP)",
            // CASE all of whose results are provably non-TIME.
            "UPDATE sched SET s = CAST(CASE WHEN id > 0 THEN s ELSE 'x' END AS TIMESTAMP)",
            // Subqueries are skipped (their columns bind to other tables;
            // the DML-root residual does not descend into them — #5398
            // covers the root operands, not subquery interiors).
            "DELETE FROM sched WHERE id IN (SELECT id FROM x WHERE CAST(t AS TIMESTAMP) > y)",
        ] {
            assert_eq!(time_cast_violation(&parse(sql), &schema), None, "{sql}");
        }
    }

    /// The residual (#5398): TIME→TIMESTAMP casts over operands that are
    /// neither literals nor the target-table TIME column, but could still
    /// yield a TIME value — an other-table column or a computed TIME
    /// expression. These were silently accepted before; now they are
    /// rejected deterministically (sound conservative over-rejection).
    #[test]
    fn residual_time_casts_are_rejected() {
        let schema = sched_schema_with_time_column();
        for sql in [
            // Column of *another* table (unresolvable here): conservative.
            "UPDATE sched SET s = CAST(other.t AS TIMESTAMP)",
            "INSERT INTO sched (id, s) VALUES (1, 'x') \
             ON CONFLICT (id) DO UPDATE SET s = CAST(other.col AS TIMESTAMP)",
            // Unqualified column absent from the target schema.
            "UPDATE sched SET s = CAST(unknown_col AS TIMESTAMP)",
            // Computed TIME expressions.
            "UPDATE sched SET s = CAST(time(t) AS TIMESTAMP)",
            "UPDATE sched SET s = CAST(CAST(s AS TIME) AS TIMESTAMP)",
            "UPDATE sched SET s = CAST(CURRENT_TIME AS TIMESTAMP)",
            "UPDATE sched SET s = CAST(CASE WHEN id > 0 THEN t ELSE t END AS TIMESTAMP)",
        ] {
            let violation = time_cast_violation(&parse(sql), &schema);
            assert!(
                violation.as_deref().is_some_and(|m| m.contains("#5398")),
                "{sql} must be rejected as a residual TIME cast, got: {violation:?}"
            );
        }
    }

    // -- Precise query-body cast disposition (#5406) -------------------------

    /// A catalog with the target `sched(id, t TIME, s TEXT)` plus a
    /// source table `src(id, txt TEXT, tm TIME)` used to exercise
    /// schema-resolved (multi-table) cast disposition.
    fn db_with_target_and_source() -> vibesql_storage::Database {
        let mut db = vibesql_storage::Database::new();
        for sql in [
            "CREATE TABLE sched (id INTEGER PRIMARY KEY, t TIME, s TEXT)",
            "CREATE TABLE src (id INTEGER PRIMARY KEY, txt TEXT, tm TIME)",
        ] {
            let Statement::CreateTable(stmt) = parse(sql) else { panic!("not create table") };
            vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        db
    }

    /// #5406: TIME→TIMESTAMP casts in a query body whose operand is
    /// **provably non-TIME** once resolved against every in-scope table
    /// are now *accepted* — no over-rejection. These were all rejected by
    /// the old schema-free guard.
    #[test]
    fn provably_non_time_query_body_casts_are_accepted() {
        let db = db_with_target_and_source();
        for sql in [
            // Other-table TEXT column inside INSERT … SELECT (the headline
            // #5406 case): resolvable to TEXT → accepted.
            "INSERT INTO sched (id, s) SELECT id, CAST(src.txt AS TIMESTAMP) FROM src",
            // Same, via an alias.
            "INSERT INTO sched (id, s) SELECT id, CAST(x.txt AS TIMESTAMP) FROM src AS x",
            // Unqualified, unambiguous non-TIME column resolved across the
            // single FROM table.
            "INSERT INTO sched (id, s) SELECT id, CAST(txt AS TIMESTAMP) FROM src",
            // Non-TIME column inside a nested subquery.
            "UPDATE sched SET s = 'x' WHERE id IN \
             (SELECT id FROM src WHERE CAST(src.txt AS TIMESTAMP) > '2026-01-01 00:00:00')",
            // WHERE of the SELECT body.
            "INSERT INTO sched (id, s) SELECT id, txt FROM src \
             WHERE CAST(txt AS TIMESTAMP) IS NOT NULL",
            // Structurally non-TIME computed expression over a TIME column.
            "INSERT INTO sched (id, s) SELECT id, CAST(EXTRACT(HOUR FROM tm) AS TIMESTAMP) FROM src",
        ] {
            assert_eq!(
                time_cast_query_violation_with_db(&parse(sql), &db),
                None,
                "{sql} must be accepted (operand is provably non-TIME)"
            );
        }
    }

    /// #5406: genuinely-TIME query-body cast operands stay rejected, now
    /// resolved precisely against the catalog (not over-broadly).
    #[test]
    fn genuinely_time_query_body_casts_are_rejected() {
        let db = db_with_target_and_source();
        for sql in [
            // Other-table TIME column resolved to TIME → rejected.
            "INSERT INTO sched (id, s) SELECT id, CAST(src.tm AS TIMESTAMP) FROM src",
            "INSERT INTO sched (id, s) SELECT id, CAST(x.tm AS TIMESTAMP) FROM src AS x",
            // Unqualified TIME column resolved across the FROM table.
            "INSERT INTO sched (id, s) SELECT id, CAST(tm AS TIMESTAMP) FROM src",
            // CURRENT_TIME in the body (provably TIME, schema-free).
            "INSERT INTO sched (id, s) SELECT id, CAST(CURRENT_TIME AS TIMESTAMP) FROM src",
            // TIME column inside a nested subquery.
            "UPDATE sched SET s = 'x' WHERE id IN \
             (SELECT id FROM src WHERE CAST(src.tm AS TIMESTAMP) > '2026-01-01 00:00:00')",
            // time(...) call: not provably non-TIME, unresolvable → rejected.
            "INSERT INTO sched (id, s) SELECT id, CAST(time(tm) AS TIMESTAMP) FROM src",
        ] {
            let violation = time_cast_query_violation_with_db(&parse(sql), &db);
            assert!(
                violation.as_deref().is_some_and(|m| m.contains("#5406")),
                "{sql} must be rejected, got: {violation:?}"
            );
        }
    }

    /// #5406: when the scope cannot be resolved precisely (an unknown
    /// table, a derived table / VALUES / CTE whose column types are not in
    /// the catalog), the audit falls back to conservative rejection so
    /// soundness is never traded for precision.
    #[test]
    fn unresolvable_scopes_fall_back_to_conservative_rejection() {
        let db = db_with_target_and_source();
        for sql in [
            // Unknown source table: cannot prove the operand non-TIME.
            "INSERT INTO sched (id, s) SELECT id, CAST(c AS TIMESTAMP) FROM unknown_tbl",
            // Derived table: its column types are not in the catalog.
            "INSERT INTO sched (id, s) SELECT id, CAST(d.c AS TIMESTAMP) \
             FROM (SELECT id, tm AS c FROM src) AS d",
            // CTE: column types not in the catalog.
            "INSERT INTO sched (id, s) WITH c AS (SELECT id, tm AS v FROM src) \
             SELECT id, CAST(v AS TIMESTAMP) FROM c",
            // Ambiguous unqualified column across a join.
            "INSERT INTO sched (id, s) SELECT a.id, CAST(id AS TIMESTAMP) \
             FROM src a JOIN src b ON a.id = b.id",
        ] {
            let violation = time_cast_query_violation_with_db(&parse(sql), &db);
            assert!(
                violation.is_some(),
                "{sql} must fall back to conservative rejection, got: {violation:?}"
            );
        }
    }

    // -- DEFAULT freeze / substitute round-trip (#5381) ----------------------

    /// The propose-side capture and the apply-side substitution walk the
    /// firing sites in the same order, so a frozen value round-trips back
    /// into the statement as a literal — and apply repeating the walk
    /// against the same schema reproduces the same site count.
    #[test]
    fn default_freeze_and_substitute_round_trip() {
        let schema = users_schema_with_volatile_default();
        let (positional, defaults) = freeze_statement_with_schema(
            "INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b')",
            Some(&schema),
        )
        .unwrap();
        assert!(positional.is_empty(), "no positional volatile sites");
        assert_eq!(defaults.len(), 2, "one frozen CURRENT_TIMESTAMP per row");

        // Apply side: splice the frozen values; the statement now carries
        // them as literals and fires no DEFAULT.
        let stmt = parse("INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b')");
        let spliced = substitute_volatile_defaults(stmt, &schema, &defaults).unwrap();
        let Statement::Insert(insert) = &spliced else { panic!("not an insert") };
        // The omitted `ts` column was appended with per-row literals.
        assert!(insert.columns.iter().any(|c| c.eq_ignore_ascii_case("ts")));
        assert!(volatile_default_violation(insert, &schema).is_none(), "no DEFAULT fires anymore");
    }

    /// An entry that fires DEFAULT sites but carries no frozen values
    /// (pre-#5381 / proposed around the freeze pass) is rejected — the
    /// preserved #5377 defense in depth.
    #[test]
    fn substitute_with_no_frozen_values_rejects_firing_sites() {
        let schema = users_schema_with_volatile_default();
        let stmt = parse("INSERT INTO users (id, name) VALUES (1, 'a')");
        let err = substitute_volatile_defaults(stmt, &schema, &[]).unwrap_err();
        assert!(err.contains("DEFAULT") && err.contains("#5381"), "got: {err}");
    }

    // -- CREATE TRIGGER validation (#5381) -----------------------------------

    #[test]
    fn deterministic_trigger_body_is_admitted() {
        for sql in [
            "CREATE TRIGGER trg AFTER INSERT ON t \
             BEGIN INSERT INTO audit (k, v) VALUES (NEW.id, NEW.v); END",
            "CREATE TRIGGER trg AFTER UPDATE ON t \
             BEGIN UPDATE audit SET v = NEW.v WHERE k = OLD.id; END",
            "CREATE TRIGGER trg AFTER DELETE ON t \
             BEGIN DELETE FROM audit WHERE k = OLD.id; END",
        ] {
            validate_statement(&parse(sql)).unwrap_or_else(|e| panic!("{sql} must be admitted: {e}"));
        }
    }

    #[test]
    fn volatile_trigger_body_is_rejected() {
        for sql in [
            // random() in the body INSERT.
            "CREATE TRIGGER trg AFTER INSERT ON t \
             BEGIN INSERT INTO audit (k, v) VALUES (NEW.id, random()); END",
            // datetime('now') in the body.
            "CREATE TRIGGER trg AFTER INSERT ON t \
             BEGIN INSERT INTO audit (k, v) VALUES (NEW.id, datetime('now')); END",
            // Volatile WHEN clause.
            "CREATE TRIGGER trg AFTER INSERT ON t WHEN random() > 0 \
             BEGIN INSERT INTO audit (k, v) VALUES (NEW.id, NEW.v); END",
        ] {
            assert!(validate_statement(&parse(sql)).is_err(), "{sql} must be rejected");
        }
    }

    #[test]
    fn raise_trigger_body_is_admitted() {
        // RAISE() is deterministic (no clock / RNG / session state), so a
        // RAISE-bearing trigger body must pass the replication volatility
        // validator and replicate verbatim (#5409). The abort is identical on
        // every replica.
        for sql in [
            "CREATE TRIGGER trg BEFORE UPDATE ON t WHEN NEW.v > 100 \
             BEGIN SELECT raise(ABORT, 'value too big'); END",
            "CREATE TRIGGER trg BEFORE INSERT ON t WHEN NEW.v < 0 \
             BEGIN SELECT raise(FAIL, 'no negatives'); END",
            "CREATE TRIGGER trg BEFORE DELETE ON t WHEN OLD.v = 0 \
             BEGIN SELECT raise(ROLLBACK, 'protected'); END",
            "CREATE TRIGGER trg BEFORE UPDATE ON t WHEN NEW.v = 0 \
             BEGIN SELECT raise(IGNORE); END",
            // Message built from a non-volatile expression over NEW is still
            // deterministic.
            "CREATE TRIGGER trg BEFORE UPDATE ON t \
             BEGIN SELECT raise(ABORT, 'bad: ' || NEW.v); END",
        ] {
            validate_statement(&parse(sql))
                .unwrap_or_else(|e| panic!("RAISE trigger body must be admitted: {sql}: {e}"));
        }
    }

    #[test]
    fn raise_message_with_volatile_call_is_rejected() {
        // A volatile call *inside* the RAISE message must still be caught by
        // the walker (RAISE itself is non-volatile, but its message subtree is
        // validated like any other expression).
        let sql = "CREATE TRIGGER trg BEFORE UPDATE ON t \
                   BEGIN SELECT raise(ABORT, 'r=' || random()); END";
        assert!(
            validate_statement(&parse(sql)).is_err(),
            "RAISE with a volatile message must be rejected"
        );
    }
}
