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
//! # Non-deterministic DEFAULT clauses
//!
//! `CREATE TABLE t (… ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)` stores
//! the expression in the schema; an INSERT that *fires* such a default
//! (column omitted, explicit `DEFAULT`, explicit `NULL` — VibeSQL
//! applies defaults to NULL values — or `DEFAULT VALUES`) would
//! evaluate the clock at apply time. [`volatile_default_violation`]
//! detects this against the (replicated, hence identical) schema and
//! the apply path rejects the entry deterministically. The fix for
//! users is to supply the column value explicitly; freezing through the
//! schema is left to the effects-form follow-up.

use serde::{Deserialize, Serialize};
use vibesql_ast::{
    visitor::{
        transform_expression, walk_expression, walk_select, ExpressionMutVisitor,
        ExpressionVisitor, VisitResult,
    },
    volatility, Assignment, CommonTableExpr, ConflictTargetItem, DeleteStmt, Expression,
    FromClause, IndexColumn, InsertSource, InsertStmt, OnConflictAction, SelectStmt, Statement,
    UpdateStmt, WhereClause,
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
    let Ok(statement) = vibesql_parser::parse_with_arena_fallback(sql) else {
        return Ok(Vec::new());
    };
    validate_statement(&statement).map_err(FreezeError::NotReplicable)?;

    let mut visitor = SiteTransformer::capture();
    let _ = transform_statement_sites(statement, &mut visitor);
    match visitor.mode {
        Mode::Capture { error: Some(e), .. } => Err(e),
        Mode::Capture { values, .. } => Ok(values),
        Mode::Substitute { .. } => unreachable!("capture visitor"),
    }
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
            if let Some(on_conflict) = &stmt.on_conflict {
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
                let Expression::ColumnRef(col) = operand.as_ref() else {
                    return VisitResult::Continue;
                };
                // Unqualified references and references qualified with
                // the target table (or the ON CONFLICT `excluded` alias,
                // which shares its schema) bind to the target table.
                let binds_to_target = col.table_canonical().is_none_or(|t| {
                    t.eq_ignore_ascii_case(&self.schema.name) || t.eq_ignore_ascii_case("excluded")
                });
                let is_time_column = binds_to_target
                    && self.schema.columns.iter().any(|c| {
                        c.name.eq_ignore_ascii_case(col.column_canonical())
                            && matches!(c.data_type, DataType::Time { .. })
                    });
                if is_time_column {
                    self.error = Some(format!(
                        "CAST({} AS TIMESTAMP) on TIME column '{}' of table '{}' stamps the \
                         current date per row at apply time (SQL:1999) and cannot be replicated \
                         deterministically; combine the column with an explicit date instead \
                         (#5377)",
                        col.column_display(),
                        col.column_display(),
                        self.schema.name
                    ));
                    return VisitResult::Stop;
                }
                VisitResult::Continue
            }
            _ => VisitResult::Continue,
        }
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
        // CREATE TABLE stores DEFAULT expressions without evaluating
        // them — deterministic as DDL. INSERTs that would *fire* a
        // volatile default are caught by `volatile_default_violation`.
        // Other statements either carry no expressions or are rejected
        // by the state machine's statement dispatch.
        _ => Ok(()),
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
    if let Some(on_conflict) = &stmt.on_conflict {
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
            e if is_time_to_timestamp_cast(e) => {
                Some("CAST(TIME AS TIMESTAMP) clock reading (stamps the current date)".to_string())
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
            if let Some(mut on_conflict) = stmt.on_conflict.take() {
                if let OnConflictAction::DoUpdate { assignments, where_clause } = on_conflict.action
                {
                    on_conflict.action = OnConflictAction::DoUpdate {
                        assignments: transform_assignments(assignments, visitor),
                        where_clause: where_clause.map(|e| transform_expression(visitor, e)),
                    };
                }
                stmt.on_conflict = Some(on_conflict);
            }
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
            Some(_) => false,
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

    #[test]
    fn non_time_casts_and_other_tables_are_not_flagged() {
        let schema = sched_schema_with_time_column();
        for sql in [
            // TEXT column: CAST to TIMESTAMP is a deterministic parse.
            "UPDATE sched SET s = CAST(s AS TIMESTAMP)",
            // Casting the TIME column to something other than TIMESTAMP.
            "UPDATE sched SET s = CAST(t AS TEXT)",
            // Qualified with a different table: binds elsewhere.
            "UPDATE sched SET s = CAST(other.t AS TIMESTAMP)",
            // Subqueries are skipped (their columns bind to other tables).
            "DELETE FROM sched WHERE id IN (SELECT id FROM x WHERE CAST(t AS TIMESTAMP) > y)",
        ] {
            assert_eq!(time_cast_violation(&parse(sql), &schema), None, "{sql}");
        }
    }
}
