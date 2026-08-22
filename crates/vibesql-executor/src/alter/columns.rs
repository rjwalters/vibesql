//! Column operation executors for ALTER TABLE

use vibesql_ast::{pretty_print::ToSql, *};
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

/// Whether a DEFAULT expression is a compile-time constant, mirroring SQLite's
/// `sqlite3ExprIsConstant` gate in `sqlite3AlterFinishAddColumn`. SQLite rejects
/// `ALTER TABLE ... ADD COLUMN` with a non-constant default (e.g. `CURRENT_TIME`,
/// a function call, or a column reference) because the new column's value must be
/// materializable without a row context. Literals and arithmetic/unary
/// combinations of literals are constant; everything else (CURRENT_*, functions,
/// column refs, subqueries) is not.
fn is_constant_default(expr: &Expression) -> bool {
    match expr {
        Expression::Literal(_) => true,
        Expression::UnaryOp { expr, .. } => is_constant_default(expr),
        Expression::BinaryOp { left, right, .. } => {
            is_constant_default(left) && is_constant_default(right)
        }
        _ => false,
    }
}

/// Whether a DEFAULT expression is (or folds to) SQL NULL. SQLite treats
/// `DEFAULT NULL` as equivalent to no default when deciding whether a NOT NULL
/// column can be added.
fn is_null_default(expr: &Expression) -> bool {
    matches!(expr, Expression::Literal(SqlValue::Null))
}

/// Execute ADD COLUMN
pub(super) fn execute_add_column(
    stmt: &AddColumnStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    // A view is not a table: SQLite rejects `ALTER TABLE <view> ADD COLUMN` with
    // a view-specific message, before any table/column resolution (alter3-2.5).
    if database.catalog.get_view(&stmt.table_name).is_some() {
        return Err(ExecutorError::Other("Cannot add a column to a view".to_string()));
    }

    // Read `PRAGMA foreign_keys` up front: the REFERENCES/non-NULL-default
    // restriction below is gated on it, but by that point `table` holds a
    // mutable borrow of `database`, so the pragma cannot be read from
    // `database` there (E0502).
    let foreign_keys_enabled = database.foreign_keys_enabled();

    let table = database
        .get_table_mut(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Check if column already exists
    if table.schema.has_column(&stmt.column_def.name) {
        return Err(ExecutorError::ColumnAlreadyExists(stmt.column_def.name.clone()));
    }

    // SQLite's ADD COLUMN restrictions (`sqlite3AlterFinishAddColumn`), checked in
    // SQLite's order and *before* any schema mutation so a rejected ALTER leaves
    // the table untouched (alter3-2.*, verified against sqlite3 3.51.0):
    //   1. A PRIMARY KEY column cannot be added.
    //   2. A UNIQUE column cannot be added.
    //   3. A NOT NULL column with a NULL (or absent) default cannot be added.
    //   4. A column with a non-constant default cannot be added.
    let has_primary_key = stmt
        .column_def
        .constraints
        .iter()
        .any(|c| matches!(c.kind, ColumnConstraintKind::PrimaryKey { .. }));
    if has_primary_key {
        return Err(ExecutorError::Other("Cannot add a PRIMARY KEY column".to_string()));
    }
    let has_unique = stmt
        .column_def
        .constraints
        .iter()
        .any(|c| matches!(c.kind, ColumnConstraintKind::Unique { .. }));
    if has_unique {
        return Err(ExecutorError::Other("Cannot add a UNIQUE column".to_string()));
    }
    // A DEFAULT NULL is equivalent to no default for the NOT NULL check.
    // Generated columns are exempt: their value comes from the generation
    // expression (not a default), so SQLite permits `ADD COLUMN x AS (expr)
    // NOT NULL` and instead validates the computed value of every existing row
    // against the NOT NULL constraint (alter3-9.5..9.7). That per-row check runs
    // after the backfill below.
    let default_is_null_or_absent =
        stmt.column_def.default_value.as_deref().is_none_or(is_null_default);
    if !stmt.column_def.nullable
        && default_is_null_or_absent
        && stmt.column_def.generated_expr.is_none()
    {
        return Err(ExecutorError::Other(
            "Cannot add a NOT NULL column with default value NULL".to_string(),
        ));
    }
    // A REFERENCES column cannot carry a non-NULL default (SQLite's
    // `sqlite3AlterFinishAddColumn`, fkey2-14.1.4/1.5, e_fkey-61.1.1): the
    // added column's default would need to satisfy the FK constraint for
    // every existing row without an FK existence check ever running, which
    // SQLite refuses to do. `DEFAULT NULL` (or no default) is fine because
    // NULL never participates in FK matching. This check runs *before* the
    // non-constant-default check below: SQLite's own restriction ordering
    // reports the REFERENCES-specific message even when the default is also
    // non-constant (e.g. `f REFERENCES t1 DEFAULT CURRENT_TIME`,
    // fkey2-14.1.5) -- both restrictions apply, but SQLite surfaces this one
    // first.
    //
    // Gated on `PRAGMA foreign_keys` being ON (fkey2-14.1.6): SQLite's own
    // restriction only fires when FK enforcement is active — with
    // `foreign_keys=OFF` the same ALTER (e.g. `ADD COLUMN h DEFAULT 'text'
    // REFERENCES t1`) succeeds verbatim (verified against sqlite3 3.51.0).
    let has_references = stmt
        .column_def
        .constraints
        .iter()
        .any(|c| matches!(c.kind, ColumnConstraintKind::References { .. }));
    if has_references && !default_is_null_or_absent && foreign_keys_enabled {
        return Err(ExecutorError::Other(
            "Cannot add a REFERENCES column with non-NULL default value".to_string(),
        ));
    }
    if let Some(default_expr) = stmt.column_def.default_value.as_deref() {
        if !is_constant_default(default_expr) {
            return Err(ExecutorError::Other(
                "Cannot add a column with non-constant default".to_string(),
            ));
        }
    }

    // A STORED generated column may only be added while the table is empty.
    // sqlite3 3.51.0 rejects `ADD COLUMN ... GENERATED ALWAYS AS (expr) STORED`
    // on a populated table with `cannot add a STORED column` (a STORED column
    // would require rewriting persisted row data). A VIRTUAL generated column
    // (the default when neither keyword is given) is computed at read time and
    // is backfilled below, so it stays permitted on a populated table. Checked
    // before any schema mutation so the ALTER has no side effects on rejection.
    if stmt.generated_stored && stmt.column_def.generated_expr.is_some() && table.row_count() > 0 {
        return Err(ExecutorError::Other("cannot add a STORED column".to_string()));
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

    // SQLite validates the constraints declared on an added column against the
    // *existing* table contents and aborts the whole ALTER (leaving the table
    // untouched) if any current row would violate them (alter3-9.*). We check
    // the added column's:
    //   - column-level CHECK constraints, and
    //   - NOT NULL, but only for a generated column (a non-generated column is already guaranteed
    //     non-NULL by the constant-default rule enforced above; a generated column's value is
    //     computed per row and may be NULL).
    // Rows are scanned in order and CHECK is evaluated before NOT NULL within a
    // row, matching sqlite3 3.51.0 (alter3-9.6 reports the CHECK failure even
    // though a later row would also fail NOT NULL).
    // Carry the constraint name alongside each added CHECK expression so it can
    // be persisted into `schema.check_constraints` below, using the same naming
    // convention CREATE TABLE's column-level CHECK uses (explicit name, else
    // the verbatim CHECK source text, else the re-rendered expression) --
    // see `ConstraintValidator::process_constraints` in `constraint_validator.rs`.
    let added_checks: Vec<(String, Expression)> = stmt
        .column_def
        .constraints
        .iter()
        .filter_map(|c| match &c.kind {
            ColumnConstraintKind::Check { expr, source_text } => {
                let name =
                    c.name.clone().or_else(|| source_text.clone()).unwrap_or_else(|| expr.to_sql());
                Some((name, (**expr).clone()))
            }
            _ => None,
        })
        .collect();
    let needs_not_null_check =
        !stmt.column_def.nullable && stmt.column_def.generated_expr.is_some();

    if !added_checks.is_empty() || needs_not_null_check {
        let schema_snapshot = table.schema.clone();
        let new_col_index = schema_snapshot.columns.len() - 1;
        let evaluator = crate::ExpressionEvaluator::new(&schema_snapshot)
            .with_schema_context(crate::evaluator::SchemaExprContext::CheckConstraint);

        let mut violation: Option<ExecutorError> = None;
        for row in table.rows_mut() {
            for (_, expr) in &added_checks {
                if evaluator.eval(expr, row)? == SqlValue::Boolean(false) {
                    // SQLite emits the bare message (no constraint name) for the
                    // ADD COLUMN existing-row validation path.
                    violation = Some(ExecutorError::SqliteCompatError(
                        "CHECK constraint failed".to_string(),
                    ));
                    break;
                }
            }
            if violation.is_some() {
                break;
            }
            if needs_not_null_check && matches!(row.values.get(new_col_index), Some(SqlValue::Null))
            {
                violation = Some(ExecutorError::SqliteCompatError(
                    "NOT NULL constraint failed".to_string(),
                ));
                break;
            }
        }

        // Existing rows all satisfy the added CHECK(s): persist them into the
        // schema so later INSERT/UPDATE enforce them too, matching CREATE
        // TABLE column-CHECK behavior (issue #6241). Previously the CHECK was
        // only ever evaluated once here, against the rows present at ALTER
        // time, and a subsequent violating INSERT went unrejected.
        //
        // Track which constraint names were actually persisted so a failure
        // partway through (e.g. a duplicate name on a later CHECK in the same
        // `ADD COLUMN` clause) can be rolled back precisely below -- a CHECK
        // need not reference the newly added column at all (e.g. `ADD COLUMN
        // c CHECK(a!=1)`), so `remove_column`'s column-reference filter alone
        // cannot be relied on to strip it.
        let mut persisted_check_names: Vec<String> = Vec::new();
        if violation.is_none() {
            for (name, expr) in &added_checks {
                match table.schema_mut().add_check_constraint(name.clone(), expr.clone()) {
                    Ok(()) => persisted_check_names.push(name.clone()),
                    Err(e) => {
                        violation = Some(ExecutorError::from(e));
                        break;
                    }
                }
            }
        }

        if let Some(err) = violation {
            // Roll back the schema + row mutations so the rejected ALTER leaves
            // the table exactly as it was (SQLite is atomic here).
            for row in table.rows_mut() {
                let _ = row.remove_value(new_col_index);
            }
            let _ = table.schema_mut().remove_column(new_col_index);
            for name in &persisted_check_names {
                let _ = table.schema_mut().drop_check_constraint(name);
            }
            if added_strict_type.is_some() {
                table.schema_mut().strict_types.pop();
            }
            database.invalidate_columnar_cache(&stmt.table_name);
            return Err(err);
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

    // (The schema/statistics-table guard — `table <name> may not be altered` —
    // is now checked centrally in `alter::mod::execute_with_source` before
    // dispatch, uniformly across every ALTER sub-command; see that check for
    // rationale. Unreachable here.)

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
    // Renaming a column of a VIEW is rejected with a dedicated message
    // (`cannot rename columns of view "<name>"`), matching SQLite. Checked
    // before the table lookup so a view does not fall through to the generic
    // `no such table` path (altercol-12.2.2/12.2.3).
    if let Some(view) = database.catalog.get_view(&stmt.table_name) {
        return Err(ExecutorError::Other(format!(
            "cannot rename columns of view \"{}\"",
            view.name
        )));
    }

    // Resolve the table + column and check for a name conflict with an
    // immutable borrow so the whole-schema precheck below can also borrow the
    // database immutably. The mutable borrow for the actual rename is taken
    // afterwards (once these checks and the precheck have passed).
    let col_index = {
        let table = database
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // The old column must exist. SQLite reports the SQL-standard
        // `no such column: "<col>"` here (altercol-12.4.2), the same wording
        // the DROP COLUMN path uses.
        let col_index = table.schema.get_column_index(&stmt.old_column_name).ok_or_else(|| {
            ExecutorError::Other(format!("no such column: \"{}\"", stmt.old_column_name))
        })?;

        // Renaming to an existing (different) column name is a conflict. Allow a
        // case-only rename of the same column.
        if !stmt.new_column_name.eq_ignore_ascii_case(&stmt.old_column_name)
            && table.schema.has_column(&stmt.new_column_name)
        {
            return Err(ExecutorError::ColumnAlreadyExists(stmt.new_column_name.clone()));
        }

        col_index
    };

    // Whole-schema dependent-object re-validation, matching SQLite's schema
    // re-parse on ALTER TABLE RENAME COLUMN (the same check DROP COLUMN runs):
    // a view or trigger that is *already* broken — e.g. a trigger body that
    // inserts into a table that was never created, or a view that references a
    // column that does not exist — aborts the ALTER with
    // `error in <type> <name>: <inner error>`, leaving the schema untouched.
    // Runs before any mutation so a failed RENAME COLUMN is atomic.
    super::drop_column_checks::precheck_schema_objects(database)?;

    let table = database
        .get_table_mut(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Rename in the schema (keeps the column-index cache consistent).
    table.schema_mut().rename_column(col_index, &stmt.new_column_name)?;

    // Invalidate the database-level columnar cache since the schema changed.
    database.invalidate_columnar_cache(&stmt.table_name);

    // Propagate the rename into trigger bodies that reference this column. If an
    // unqualified reference to the renamed column is ambiguous in a trigger
    // body, SQLite (legacy_alter_table=OFF) aborts the whole ALTER and leaves
    // the schema unchanged. Roll back the schema rename on that error so the
    // ALTER is atomic, matching SQLite.
    // Rolls back the just-applied schema rename and returns the error, keeping the
    // ALTER atomic when a dependent-object rewrite (trigger or view) aborts on a
    // genuine ambiguity — matching SQLite, which leaves the schema unchanged.
    let rollback = |database: &mut Database, err: ExecutorError| -> ExecutorError {
        if let Some(table) = database.get_table_mut(&stmt.table_name) {
            if let Some(idx) = table.schema.get_column_index(&stmt.new_column_name) {
                let _ = table.schema_mut().rename_column(idx, &stmt.old_column_name);
            }
        }
        database.invalidate_columnar_cache(&stmt.table_name);
        err
    };

    if let Err(err) = super::table_options::rewrite_triggers_for_column_rename(
        database,
        &stmt.table_name,
        &stmt.old_column_name,
        &stmt.new_column_name,
    ) {
        return Err(rollback(database, err));
    }

    // Propagate the rename into dependent VIEW definitions (verbatim
    // `sql_definition` text + parsed `query` AST). Runs after the trigger rewrite
    // so an ambiguous view aborts the whole ALTER before index/FK bookkeeping.
    if let Err(err) = super::table_options::rewrite_views_for_column_rename(
        database,
        &stmt.table_name,
        &stmt.old_column_name,
        &stmt.new_column_name,
    ) {
        return Err(rollback(database, err));
    }

    // Propagate the rename into any child table's foreign key that references the
    // renamed column of THIS (parent) table: rewrite the child's verbatim
    // `REFERENCES <table>(<col_list>)` text and its in-memory FK parent-column
    // metadata (altercol.test 4.1/4.4).
    super::table_options::rewrite_child_foreign_keys_for_column_rename(
        database,
        &stmt.table_name,
        &stmt.old_column_name,
        &stmt.new_column_name,
    );

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
