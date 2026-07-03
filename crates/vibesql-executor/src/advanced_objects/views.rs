//! Executor for VIEW objects (SQL:1999)

use vibesql_ast::*;
use vibesql_catalog::ViewDropBehavior;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute CREATE VIEW statement
pub fn execute_create_view(stmt: &CreateViewStmt, db: &mut Database) -> Result<(), ExecutorError> {
    use vibesql_catalog::ViewDefinition;

    // Check if view already exists
    let view_exists = db.catalog.get_view(&stmt.view_name).is_some();

    // If IF NOT EXISTS and view already exists, just return success
    if stmt.if_not_exists && view_exists {
        return Ok(());
    }

    // Guard against exponentially self-referential view nests before we (eagerly)
    // execute the view body to derive its columns. VibeSQL materializes views by
    // executing their queries, so a doubling nest (v2=v1∪v1, v4=v2∪v2, …) would
    // expand exponentially and hang here. Match SQLite (ticket [d58ccbb3f1b],
    // view3.test / #5394) by raising `too many references to "<view>": max 65535`
    // at CREATE VIEW time instead. See crate::select::view_reference_guard.
    crate::select::view_reference_guard::check_view_reference_limit(&stmt.query, db)?;

    // If no explicit column list is provided, derive column names from the query.
    // This ensures views with SELECT * preserve original column names. Use simple
    // column names (without table prefix) for view schema compatibility.
    //
    // VibeSQL materializes views by executing them, so a deeply nested (doubling)
    // view nest expands exponentially and would hang while deriving columns here
    // (#5394, view3.test). When the body would re-materialize many views, derive
    // the columns statically from the catalog instead. Ordinary views keep the
    // eager execution path so all existing CREATE-time validations
    // (set-operation arity, ORDER BY term resolution, etc.) surface as before.
    const STATIC_COLUMN_DERIVATION_THRESHOLD: u64 = 64;
    let columns = if stmt.columns.is_some() {
        stmt.columns.clone()
    } else {
        let body_is_expensive =
            crate::select::view_reference_guard::max_view_reference_count(&stmt.query, db)
                > STATIC_COLUMN_DERIVATION_THRESHOLD;

        // Static fast path for expensive bodies only (avoids exponential
        // re-materialization of deep view nests).
        let static_cols = if body_is_expensive {
            crate::select::cte::try_static_select_columns(&stmt.query, db)
        } else {
            None
        };

        if let Some(cols) = static_cols {
            Some(cols)
        } else {
            // Execute the query once to derive column names.
            //
            // SQLite-compatible error prefixing: in SQLite the "error in view <name>: "
            // prefix is exclusively an ALTER-time schema-reparse phenomenon (alter.c);
            // query-time view expansion errors are bare. VibeSQL compiles views eagerly,
            // so both error classes surface here at CREATE VIEW time. We allow-list the
            // prefix to OrderByTermNotInResultSet only — the sole class the TCL suite
            // expects prefixed (window1.test 32.10, altertab.test). All other variants
            // (notably SetOperationColumnMismatch, see select7.test 8.2) pass through
            // bare, matching SQLite's query-time messages.
            use crate::select::SelectExecutor;
            let executor = SelectExecutor::new(db);
            match executor.execute_with_simple_columns(&stmt.query) {
                Ok(result) => Some(result.columns),
                Err(e @ ExecutorError::OrderByTermNotInResultSet { .. }) => {
                    return Err(ExecutorError::SqliteCompatError(format!(
                        "error in view {}: {}",
                        stmt.view_name, e
                    )));
                }
                // Lax view creation (SQLite semantics, issue #5795 /
                // alterdropcol.test 5.2.1): a view whose SELECT names a column
                // that does not (yet) resolve is still created — SQLite defers
                // column resolution to query time. The view is stored with no
                // derived column list; querying it re-runs the SELECT and
                // surfaces the resolution error then, and ALTER-time schema
                // re-parses report it as `error in view <name>: ...`.
                Err(ExecutorError::ColumnNotFound { .. }) => None,
                Err(other) => return Err(other),
            }
        }
    };

    // Tag temp views with the `temp` schema so they surface via
    // sqlite_temp_master and are excluded from sqlite_master (#5541), mirroring
    // the temp-trigger schema tag (#5532) and temp-index tag (#5513).
    let schema = if stmt.temporary { Some("temp".to_string()) } else { None };

    let view = if let Some(ref sql) = stmt.sql_definition {
        ViewDefinition::new_with_sql(
            stmt.view_name.clone(),
            columns,
            (*stmt.query).clone(),
            stmt.with_check_option,
            sql.clone(),
        )
    } else {
        ViewDefinition::new(
            stmt.view_name.clone(),
            columns,
            (*stmt.query).clone(),
            stmt.with_check_option,
        )
    }
    .with_schema(schema);

    if stmt.or_replace || (stmt.if_not_exists && !view_exists) {
        // For OR REPLACE, drop the view if it exists, then CREATE
        if view_exists && stmt.or_replace {
            let _ = db.catalog.drop_view(&stmt.view_name, false);
        }
        db.catalog.create_view(view)?;
    } else {
        // Regular CREATE VIEW (will fail if view already exists)
        db.catalog.create_view(view)?;
    }
    Ok(())
}

/// Execute DROP VIEW statement
pub fn execute_drop_view(stmt: &DropViewStmt, db: &mut Database) -> Result<(), ExecutorError> {
    // Check if view exists
    let view_exists = db.catalog.get_view(&stmt.view_name).is_some();

    // If IF EXISTS is specified and view doesn't exist, succeed silently
    if stmt.if_exists && !view_exists {
        return Ok(());
    }

    // Determine drop behavior:
    // - CASCADE: drop dependent views recursively
    // - RESTRICT (explicit): fail if dependents exist
    // - Neither: SQLite-compatible behavior (just drop, ignore dependents)
    let drop_behavior = if stmt.cascade {
        ViewDropBehavior::Cascade
    } else if stmt.restrict {
        ViewDropBehavior::Restrict
    } else {
        ViewDropBehavior::Silent // SQLite-compatible: allow dropping even with dependents
    };

    // Resolve the view's temp/main schema tag *before* the catalog removes it so
    // the INSTEAD OF trigger cascade can apply the temp-shadows-main filter.
    let view_is_temp = db.catalog.get_view(&stmt.view_name).is_some_and(|v| v.is_temp());

    db.catalog.drop_view_with_behavior(&stmt.view_name, drop_behavior)?;

    // Cascade-drop the INSTEAD OF triggers defined ON this view, matching sqlite3
    // 3.51.0: an INSTEAD OF trigger cannot outlive the view it is attached to, so
    // `DROP VIEW v` removes every trigger whose `ON v` target is the dropped view
    // (leaving triggers on a *different* view alone). See
    // `Catalog::drop_view_triggers` (view analogue of the table path in #5597).
    db.catalog.drop_view_triggers(&stmt.view_name, view_is_temp);
    Ok(())
}
