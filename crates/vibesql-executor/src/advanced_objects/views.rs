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

    // If no explicit column list is provided, derive column names from the query
    // This ensures views with SELECT * preserve original column names
    // Use simple column names (without table prefix) for view schema compatibility
    let columns = if stmt.columns.is_none() {
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
        let result = executor.execute_with_simple_columns(&stmt.query).map_err(|e| match e {
            e @ ExecutorError::OrderByTermNotInResultSet { .. } => {
                ExecutorError::SqliteCompatError(format!("error in view {}: {}", stmt.view_name, e))
            }
            other => other,
        })?;
        Some(result.columns)
    } else {
        stmt.columns.clone()
    };

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
    };

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

    db.catalog.drop_view_with_behavior(&stmt.view_name, drop_behavior)?;
    Ok(())
}
