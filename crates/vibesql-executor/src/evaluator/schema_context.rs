//! Schema-expression evaluation context
//!
//! SQLite rejects non-deterministic uses of the date/time functions (a call
//! that resolves its time-value to the current time — `'now'` or a
//! zero-argument default like `date()` — or that applies a `'localtime'` /
//! `'utc'` modifier) when the call is evaluated inside a schema-attached
//! expression: a CHECK constraint, a generated column, or an index expression
//! (including a partial index's WHERE predicate).
//!
//! Crucially this is an EVALUATION-time check, not a DDL-time one:
//! `CREATE TABLE t(a CHECK(a < julianday('now')))` succeeds, and the error
//! fires when the expression is first evaluated (INSERT/UPDATE/CREATE INDEX
//! build). The trigger can come from row data (`date(x)` with `x = 'now'`),
//! which no static schema scan can detect.
//!
//! `SchemaExprContext` is carried by `ExpressionEvaluator` and threaded into
//! `eval_scalar_function`, where the date/time functions consult it.
//!
//! SQLite reference: date.c `setDateTimeToCurrent`/`toLocaltime` calling
//! `sqlite3NotPureFunc` for expressions compiled with `OP_PureFunc`
//! (`NC_IsCheck` / `NC_GenCol` / `NC_IdxExpr` / `NC_PartIdx`).

use crate::errors::ExecutorError;

/// The schema-attached expression context an expression is being evaluated
/// in, if any. `None` (the default) means an ordinary query context where
/// non-deterministic date/time functions are allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchemaExprContext {
    /// Ordinary evaluation (SELECT, VALUES, WHERE, ...): no restriction
    #[default]
    None,
    /// A CHECK constraint expression
    CheckConstraint,
    /// A generated (computed) column expression
    GeneratedColumn,
    /// An index expression or a partial index's WHERE predicate
    Index,
}

impl SchemaExprContext {
    /// SQLite's human-readable name for the context, used in the
    /// "non-deterministic use of f() in {ctx}" error message.
    /// Returns `None` for the unrestricted context.
    pub fn description(self) -> Option<&'static str> {
        match self {
            SchemaExprContext::None => None,
            SchemaExprContext::CheckConstraint => Some("a CHECK constraint"),
            SchemaExprContext::GeneratedColumn => Some("a generated column"),
            // SQLite reports partial-index WHERE clauses as "an index" too
            SchemaExprContext::Index => Some("an index"),
        }
    }

    /// Build the SQLite-compatible rejection error for a non-deterministic
    /// use of `func_name` in this context, or `None` when evaluation is
    /// unrestricted. `func_name` may be uppercase (internal convention);
    /// SQLite prints the lowercase function name.
    pub fn non_deterministic_error(self, func_name: &str) -> Option<ExecutorError> {
        self.description().map(|ctx| {
            ExecutorError::SqliteCompatError(format!(
                "non-deterministic use of {}() in {}",
                func_name.to_ascii_lowercase(),
                ctx
            ))
        })
    }
}
