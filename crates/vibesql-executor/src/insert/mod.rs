mod bulk_transfer;
pub(crate) mod constraints;
pub mod defaults;
mod duplicate_key_update;
mod execution;
mod foreign_keys;
mod on_conflict_update;
mod replace;
mod row_validator;
pub mod validation;

use crate::errors::ExecutorError;

pub use execution::InsertOutcome;
pub(crate) use execution::{coerce_rowid_affinity, RowidAffinity};

/// Executor for INSERT statements
pub struct InsertExecutor;

impl InsertExecutor {
    /// Execute an INSERT statement
    /// Returns number of rows inserted
    pub fn execute(
        db: &mut vibesql_storage::Database,
        stmt: &vibesql_ast::InsertStmt,
    ) -> Result<usize, ExecutorError> {
        // Wrap the top-level statement in SQLite's per-variant RAISE scope
        // handling (#5417): inside an explicit transaction, RAISE(ABORT) undoes
        // just this statement, RAISE(FAIL) keeps its partial changes, and
        // RAISE(ROLLBACK) rolls back the whole transaction.
        let may_fire = crate::raise_scope::table_may_fire_trigger(db, &stmt.table_name);
        crate::raise_scope::run_top_level_dml_with_conflict_scope(
            db,
            may_fire,
            stmt.conflict_clause,
            |db| execution::execute_insert(db, stmt),
        )
    }

    /// Execute an INSERT statement, capturing RETURNING rows (SQLite 3.35.0+)
    ///
    /// Returns an [`InsertOutcome`] carrying the number of affected rows, the
    /// number of rows handled via the upsert `ON CONFLICT DO UPDATE` arm, and,
    /// when the statement carries a RETURNING clause, the projected NEW rows
    /// (values as actually inserted, including defaults, generated columns,
    /// and auto INTEGER PRIMARY KEY). Rows skipped by `OR IGNORE` /
    /// `ON CONFLICT DO NOTHING` are omitted; `ON DUPLICATE KEY UPDATE`
    /// contributes the post-UPDATE row.
    ///
    /// When the statement has no RETURNING clause `returning` is `None`.
    pub fn execute_returning(
        db: &mut vibesql_storage::Database,
        stmt: &vibesql_ast::InsertStmt,
    ) -> Result<InsertOutcome, ExecutorError> {
        // Per-variant RAISE scope for RETURNING DML (#5432, follow-on to #5417):
        // a RAISE fired from a trigger during a RETURNING INSERT gets the same
        // statement-savepoint scope as the bare `execute` path. When the RAISE
        // aborts the statement the error propagates (no rows returned) and the
        // chosen variant's scope is applied.
        let may_fire = crate::raise_scope::table_may_fire_trigger(db, &stmt.table_name);
        crate::raise_scope::run_top_level_dml_with_conflict_scope(
            db,
            may_fire,
            stmt.conflict_clause,
            |db| execution::execute_insert_returning(db, stmt),
        )
    }

    /// Execute an INSERT statement with procedural context
    /// Supports procedural variables in VALUES clause
    /// Returns number of rows inserted
    pub fn execute_with_procedural_context(
        db: &mut vibesql_storage::Database,
        stmt: &vibesql_ast::InsertStmt,
        procedural_context: &crate::procedural::ExecutionContext,
    ) -> Result<usize, ExecutorError> {
        // Per-variant RAISE scope for procedural-context DML (#5432): a RAISE
        // fired from a trigger inside a procedure/script gets the same
        // statement-savepoint scope as the bare `execute` path.
        let may_fire = crate::raise_scope::table_may_fire_trigger(db, &stmt.table_name);
        crate::raise_scope::run_top_level_dml_with_conflict_scope(
            db,
            may_fire,
            stmt.conflict_clause,
            |db| execution::execute_insert_with_procedural_context(db, stmt, procedural_context),
        )
    }
}

/// Execute an INSERT statement with trigger context
/// This function is used when executing INSERT statements within trigger bodies
/// to support OLD/NEW pseudo-variable references
pub fn execute_insert_with_trigger_context(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
    trigger_context: &crate::trigger_execution::TriggerContext,
) -> Result<usize, ExecutorError> {
    execution::execute_insert_with_trigger_context(db, stmt, trigger_context)
}
