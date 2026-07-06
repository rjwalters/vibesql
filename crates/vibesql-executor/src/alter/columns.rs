//! Column operation executors for ALTER TABLE

use vibesql_ast::*;
use vibesql_catalog::ColumnSchema;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use super::validation::{convert_value, evaluate_simple_default, is_type_conversion_safe};
use crate::errors::ExecutorError;

/// Whether `name` is an implicit unique index auto-created for a PRIMARY KEY or
/// UNIQUE constraint (SQLite names these `sqlite_autoindex_<table>_<n>`). Such
/// indexes are reported through the PRIMARY KEY / UNIQUE drop-column messages,
/// not the generic `error in index` path.
fn is_auto_index(name: &str) -> bool {
    name.to_ascii_lowercase().starts_with("sqlite_autoindex_")
}

/// Execute ADD COLUMN
pub(super) fn execute_add_column(
    stmt: &AddColumnStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    let table = database
        .get_table_mut(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Check if column already exists
    if table.schema.has_column(&stmt.column_def.name) {
        return Err(ExecutorError::ColumnAlreadyExists(stmt.column_def.name.clone()));
    }

    // STRICT tables (issue #5837) reject a non-strict datatype on the added
    // column, wrapping the strict error with SQLite's ALTER-context prefix:
    // `error in table <t> after add column: <strict error>`.
    let added_strict_type = if table.schema.strict {
        match crate::strict::classify_strict_column(
            &stmt.table_name,
            &stmt.column_def.name,
            stmt.column_def.type_source.as_deref(),
        ) {
            Ok(st) => Some(st),
            Err(ExecutorError::SqliteCompatError(msg)) => {
                return Err(ExecutorError::SqliteCompatError(format!(
                    "error in table {} after add column: {}",
                    stmt.table_name, msg
                )));
            }
            Err(e) => return Err(e),
        }
    } else {
        None
    };

    // Add column to schema
    let mut new_column = ColumnSchema::new(
        stmt.column_def.name.clone(),
        stmt.column_def.data_type.clone(),
        stmt.column_def.nullable,
    );

    // Set the default value if provided
    if let Some(ref default_expr) = stmt.column_def.default_value {
        new_column.set_default(*default_expr.clone());
    }

    // Carry the generated-column expression onto the catalog column so the new
    // column computes its value instead of storing a plain NULL. Without this
    // the parsed `GENERATED ALWAYS AS (expr)` clause was dropped and the column
    // read back as NULL (issue #5861). VibeSQL materializes generated columns at
    // write time, so STORED and VIRTUAL behave identically here.
    if let Some(ref gen_expr) = stmt.column_def.generated_expr {
        new_column.generated_expr = Some(*gen_expr.clone());
    }

    table.schema_mut().add_column(new_column)?;

    // Keep the parallel STRICT type vector aligned with the new column set.
    if let Some(st) = added_strict_type {
        table.schema_mut().strict_types.push(st);
    }

    // Backfill existing rows. For a generated column, evaluate the expression
    // per-row against each row's current (pre-existing) values, mirroring the
    // INSERT-time materialization in `insert::defaults::apply_generated_columns`
    // (issue #5861). The new column was appended at the end of the schema, so
    // the indices of the pre-existing columns the expression references are
    // unchanged, and evaluation resolves exactly as it does at INSERT time.
    // Non-generated columns keep the previous behavior: a static default or NULL.
    if let Some(ref gen_expr) = stmt.column_def.generated_expr {
        let gen_expr = *gen_expr.clone();
        let col_type = stmt.column_def.data_type.clone();
        let schema_snapshot = table.schema.clone();
        let evaluator = crate::ExpressionEvaluator::new(&schema_snapshot)
            .with_schema_context(crate::evaluator::SchemaExprContext::GeneratedColumn);
        for row in table.rows_mut() {
            let value = evaluator.eval(&gen_expr, row)?;
            let coerced = crate::insert::validation::coerce_value(value, &col_type)?;
            row.add_value(coerced);
        }
    } else {
        // Add default value (or NULL) to all existing rows
        let default_value = if let Some(ref default_expr) = stmt.column_def.default_value {
            // Evaluate the default expression for simple cases (literals)
            evaluate_simple_default(default_expr)?
        } else {
            SqlValue::Null
        };

        for row in table.rows_mut() {
            row.add_value(default_value.clone());
        }
    }

    // Invalidate the database-level columnar cache since table structure changed.
    database.invalidate_columnar_cache(&stmt.table_name);

    Ok(format!("Column '{}' added to table '{}'", stmt.column_def.name, stmt.table_name))
}

/// Execute DROP COLUMN.
///
/// Validations run in SQLite's order so the same error surfaces as sqlite3
/// (verified against `alterdropcol.test`, sqlite3 3.51.0). Every rejection uses
/// SQLite's verbatim message via [`ExecutorError::Other`] so the TCL harness
/// (which asserts exact error text) matches. See issue #5784.
pub(super) fn execute_drop_column(
    stmt: &DropColumnStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    let table_name = &stmt.table_name;
    let col = &stmt.column_name;

    // A view is not a table: SQLite rejects DROP COLUMN on a view with a
    // view-specific message, before any table/column resolution.
    if database.catalog.get_view(table_name).is_some() {
        return Err(ExecutorError::Other(format!(
            "cannot drop column from view \"{}\"",
            table_name
        )));
    }

    // The schema table itself may not be altered.
    if super::validation::is_sqlite_schema_table(table_name) {
        return Err(ExecutorError::Other("table sqlite_master may not be altered".to_string()));
    }

    // Resolve the table. A missing table keeps `TableNotFound`, which the
    // harness normalizes to SQLite's `no such table: <name>`.
    let schema = match database.get_table(table_name) {
        Some(table) => &table.schema,
        None => return Err(ExecutorError::TableNotFound(table_name.clone())),
    };

    // Column existence. `IF EXISTS` makes a missing column a no-op.
    if !schema.has_column(col) {
        if stmt.if_exists {
            return Ok(format!("Column '{}' does not exist in table '{}'", col, table_name));
        }
        return Err(ExecutorError::Other(format!("no such column: \"{}\"", col)));
    }

    // PRIMARY KEY / UNIQUE columns cannot be dropped (PRIMARY KEY wins when a
    // column is both, matching SQLite).
    if schema.is_column_in_primary_key(col)
        || schema.get_integer_primary_key_index() == schema.get_column_index(col)
    {
        return Err(ExecutorError::Other(format!("cannot drop PRIMARY KEY column: \"{}\"", col)));
    }

    // A UNIQUE column cannot be dropped. VibeSQL records a column-level UNIQUE
    // constraint as an implicit unique auto-index (`sqlite_autoindex_*`) rather
    // than in `unique_constraints`, so check both representations. This must be
    // decided before the generic index-reference check below, otherwise the
    // auto-index would surface as `error in index sqlite_autoindex_...` instead
    // of SQLite's `cannot drop UNIQUE column`.
    let in_unique = super::validation::column_in_unique_constraint(schema, col)
        || database.catalog.get_table_indexes(table_name).iter().any(|idx| {
            idx.is_unique
                && is_auto_index(&idx.name)
                && super::validation::index_references_column(idx, col)
        });
    if in_unique {
        return Err(ExecutorError::Other(format!("cannot drop UNIQUE column: \"{}\"", col)));
    }

    // Cannot drop the only remaining column.
    if schema.columns.len() <= 1 {
        return Err(ExecutorError::Other(format!(
            "cannot drop column \"{}\": no other columns exist",
            col
        )));
    }

    // Pre-drop schema re-parse: a view or trigger that is *already* broken —
    // even one unrelated to this table — aborts the ALTER before anything is
    // touched, matching SQLite's first schema re-parse (no "after drop column"
    // suffix; the object was broken before the drop). See issue #5795.
    super::drop_column_checks::precheck_schema_objects(database)?;

    // Dependent-object validation: dropping the column must not leave an
    // explicit index (plain or expression/partial) dangling. SQLite re-parses
    // the schema after the drop and rolls back with this message when an index
    // still references the gone column. Implicit unique auto-indexes are handled
    // by the UNIQUE check above and skipped here.
    for index in database.catalog.get_table_indexes(table_name) {
        if !is_auto_index(&index.name) && super::validation::index_references_column(index, col) {
            return Err(ExecutorError::Other(format!(
                "error in index {} after drop column: no such column: {}",
                index.name, col
            )));
        }
    }

    // Post-drop schema re-parse: a table-level CHECK, dependent view, or
    // trigger that would reference the gone column aborts the ALTER, matching
    // SQLite's second schema re-parse (`error in <type> <name> after drop
    // column: ...`). Validation runs against a simulated post-drop schema, so
    // nothing has been mutated yet and no rollback is needed. See issue #5795.
    super::drop_column_checks::postcheck_schema_objects(database, table_name, col)?;

    let table = database
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

    // Get column index
    let col_index = table.schema.get_column_index(&stmt.column_name).ok_or_else(|| {
        ExecutorError::ColumnNotFound {
            column_name: stmt.column_name.clone(),
            table_name: stmt.table_name.clone(),
            searched_tables: vec![stmt.table_name.clone()],
            available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
        }
    })?;

    // Remove column from schema
    table.schema_mut().remove_column(col_index)?;

    // Remove column data from all rows
    for row in table.rows_mut() {
        let _ = row.remove_value(col_index);
    }

    // Invalidate the database-level columnar cache since table structure changed.
    database.invalidate_columnar_cache(&stmt.table_name);

    Ok(format!("Column '{}' dropped from table '{}'", stmt.column_name, stmt.table_name))
}

/// Execute ALTER COLUMN
pub(super) fn execute_alter_column(
    stmt: &AlterColumnStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    match stmt {
        AlterColumnStmt::SetDefault { table_name, column_name, default } => {
            let table = database
                .get_table_mut(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

            let col_index = table.schema.get_column_index(column_name).ok_or_else(|| {
                ExecutorError::ColumnNotFound {
                    column_name: column_name.clone(),
                    table_name: table_name.clone(),
                    searched_tables: vec![table_name.clone()],
                    available_columns: table
                        .schema
                        .columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect(),
                }
            })?;

            // Set the default value in the schema
            table.schema_mut().set_column_default(col_index, default.clone())?;

            Ok(format!("Default set for column '{}' in table '{}'", column_name, table_name))
        }
        AlterColumnStmt::DropDefault { table_name, column_name } => {
            let table = database
                .get_table_mut(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

            let col_index = table.schema.get_column_index(column_name).ok_or_else(|| {
                ExecutorError::ColumnNotFound {
                    column_name: column_name.clone(),
                    table_name: table_name.clone(),
                    searched_tables: vec![table_name.clone()],
                    available_columns: table
                        .schema
                        .columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect(),
                }
            })?;

            // Drop the default value from the schema
            table.schema_mut().drop_column_default(col_index)?;

            Ok(format!("Default dropped for column '{}' in table '{}'", column_name, table_name))
        }
        AlterColumnStmt::SetNotNull { table_name, column_name } => {
            let table = database
                .get_table_mut(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

            let col_index = table.schema.get_column_index(column_name).ok_or_else(|| {
                ExecutorError::ColumnNotFound {
                    column_name: column_name.clone(),
                    table_name: table_name.clone(),
                    searched_tables: vec![table_name.clone()],
                    available_columns: table
                        .schema
                        .columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect(),
                }
            })?;

            // Check if any existing rows have NULL in this column
            for row in table.scan() {
                if let SqlValue::Null = &row.values[col_index] {
                    return Err(ExecutorError::ConstraintViolation(
                        "Cannot set NOT NULL: column contains NULL values".to_string(),
                    ));
                }
            }

            // Set column as NOT NULL
            table.schema_mut().set_column_nullable(col_index, false)?;

            Ok(format!("Column '{}' set to NOT NULL in table '{}'", column_name, table_name))
        }
        AlterColumnStmt::DropNotNull { table_name, column_name } => {
            let table = database
                .get_table_mut(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

            let col_index = table.schema.get_column_index(column_name).ok_or_else(|| {
                ExecutorError::ColumnNotFound {
                    column_name: column_name.clone(),
                    table_name: table_name.clone(),
                    searched_tables: vec![table_name.clone()],
                    available_columns: table
                        .schema
                        .columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect(),
                }
            })?;

            // Set column as nullable
            table.schema_mut().set_column_nullable(col_index, true)?;

            Ok(format!("Column '{}' set to nullable in table '{}'", column_name, table_name))
        }
    }
}

/// Execute RENAME COLUMN (`ALTER TABLE t RENAME [COLUMN] old TO new`).
///
/// Renames the column in the table schema and propagates the rename into any
/// trigger bodies that reference `<table>.<old_column>`, matching SQLite's
/// `legacy_alter_table=OFF` behavior (see `crate::trigger_rename`).
pub(super) fn execute_rename_column(
    stmt: &RenameColumnStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    let table = database
        .get_table_mut(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // The old column must exist.
    let col_index = table.schema.get_column_index(&stmt.old_column_name).ok_or_else(|| {
        ExecutorError::ColumnNotFound {
            column_name: stmt.old_column_name.clone(),
            table_name: stmt.table_name.clone(),
            searched_tables: vec![stmt.table_name.clone()],
            available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
        }
    })?;

    // Renaming to an existing (different) column name is a conflict. Allow a
    // case-only rename of the same column.
    if !stmt.new_column_name.eq_ignore_ascii_case(&stmt.old_column_name)
        && table.schema.has_column(&stmt.new_column_name)
    {
        return Err(ExecutorError::ColumnAlreadyExists(stmt.new_column_name.clone()));
    }

    // Rename in the schema (keeps the column-index cache consistent).
    table.schema_mut().rename_column(col_index, &stmt.new_column_name)?;

    // Invalidate the database-level columnar cache since the schema changed.
    database.invalidate_columnar_cache(&stmt.table_name);

    // Propagate the rename into trigger bodies that reference this column. If an
    // unqualified reference to the renamed column is ambiguous in a trigger
    // body, SQLite (legacy_alter_table=OFF) aborts the whole ALTER and leaves
    // the schema unchanged. Roll back the schema rename on that error so the
    // ALTER is atomic, matching SQLite.
    if let Err(err) = super::table_options::rewrite_triggers_for_column_rename(
        database,
        &stmt.table_name,
        &stmt.old_column_name,
        &stmt.new_column_name,
    ) {
        if let Some(table) = database.get_table_mut(&stmt.table_name) {
            if let Some(idx) = table.schema.get_column_index(&stmt.new_column_name) {
                // Best-effort rollback; the column was just renamed so this
                // restores the original name.
                let _ = table.schema_mut().rename_column(idx, &stmt.old_column_name);
            }
        }
        database.invalidate_columnar_cache(&stmt.table_name);
        return Err(err);
    }

    // Propagate the rename into dependent index metadata, in BOTH copies:
    // the catalog copy (drives sqlite_master rendering and planner/FK
    // checks) and the storage copy (drives index maintenance and binary
    // persistence). This covers plain column lists, expression-index ASTs,
    // and partial-index WHERE predicates — SQLite rewrites all index
    // references on RENAME COLUMN. Without this the next checkpoint
    // persists index metadata naming a column that no longer exists in the
    // table, and the fail-closed open policy makes the database unopenable
    // (issue #5877). Runs after the trigger rewrite so a trigger-side abort
    // leaves the index metadata untouched.
    database.catalog.rename_column_in_table_indexes(
        &stmt.table_name,
        &stmt.old_column_name,
        &stmt.new_column_name,
    );
    database.rename_column_in_table_indexes(
        &stmt.table_name,
        &stmt.old_column_name,
        &stmt.new_column_name,
    );

    Ok(format!(
        "Column '{}' renamed to '{}' in table '{}'",
        stmt.old_column_name, stmt.new_column_name, stmt.table_name
    ))
}

/// Execute MODIFY COLUMN
pub(super) fn execute_modify_column(
    stmt: &ModifyColumnStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    let table = database
        .get_table_mut(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Get column index
    let col_index = table.schema.get_column_index(&stmt.column_name).ok_or_else(|| {
        ExecutorError::ColumnNotFound {
            column_name: stmt.column_name.clone(),
            table_name: stmt.table_name.clone(),
            searched_tables: vec![stmt.table_name.clone()],
            available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
        }
    })?;

    let old_type = &table.schema.columns[col_index].data_type;
    let new_type = &stmt.new_column_def.data_type;

    // Type conversion validation - be strict about compatibility
    let is_compatible = is_type_conversion_safe(old_type, new_type);

    if !is_compatible {
        return Err(ExecutorError::TypeMismatch {
            left: SqlValue::Null, // Placeholder
            op: format!("Cannot convert column from {:?} to {:?}", old_type, new_type),
            right: SqlValue::Null,
        });
    }

    // Convert existing data
    for row in table.rows_mut() {
        if let Some(value) = row.values.get_mut(col_index) {
            *value = convert_value(value.clone(), new_type)?;
        }
    }

    // Update schema
    table.schema_mut().columns[col_index].data_type = new_type.clone();
    table.schema_mut().columns[col_index].nullable = stmt.new_column_def.nullable;

    // Update default value if provided
    if let Some(ref default_expr) = stmt.new_column_def.default_value {
        table.schema_mut().set_column_default(col_index, *default_expr.clone())?;
    }

    // Invalidate the database-level columnar cache since table structure changed.
    database.invalidate_columnar_cache(&stmt.table_name);

    Ok(format!("Column '{}' modified in table '{}'", stmt.column_name, stmt.table_name))
}

/// Execute CHANGE COLUMN (rename + modify)
pub(super) fn execute_change_column(
    stmt: &ChangeColumnStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    let table = database
        .get_table_mut(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Get column index
    let col_index = table.schema.get_column_index(&stmt.old_column_name).ok_or_else(|| {
        ExecutorError::ColumnNotFound {
            column_name: stmt.old_column_name.clone(),
            table_name: stmt.table_name.clone(),
            searched_tables: vec![stmt.table_name.clone()],
            available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
        }
    })?;

    let old_type = &table.schema.columns[col_index].data_type;
    let new_type = &stmt.new_column_def.data_type;

    // Type conversion validation
    let is_compatible = is_type_conversion_safe(old_type, new_type);

    if !is_compatible {
        return Err(ExecutorError::TypeMismatch {
            left: SqlValue::Null,
            op: format!("Cannot convert column from {:?} to {:?}", old_type, new_type),
            right: SqlValue::Null,
        });
    }

    // Convert existing data
    for row in table.rows_mut() {
        if let Some(value) = row.values.get_mut(col_index) {
            *value = convert_value(value.clone(), new_type)?;
        }
    }

    // Update schema - rename and modify. Use `rename_column` so the
    // column-index cache stays consistent with the new name.
    table.schema_mut().rename_column(col_index, &stmt.new_column_def.name)?;
    table.schema_mut().columns[col_index].data_type = new_type.clone();
    table.schema_mut().columns[col_index].nullable = stmt.new_column_def.nullable;

    // Update default value if provided
    if let Some(ref default_expr) = stmt.new_column_def.default_value {
        table.schema_mut().set_column_default(col_index, *default_expr.clone())?;
    }

    // Invalidate the database-level columnar cache since table structure changed.
    database.invalidate_columnar_cache(&stmt.table_name);

    Ok(format!(
        "Column '{}' changed to '{}' in table '{}'",
        stmt.old_column_name, stmt.new_column_def.name, stmt.table_name
    ))
}
