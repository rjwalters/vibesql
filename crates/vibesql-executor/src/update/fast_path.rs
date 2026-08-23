//! Fast path optimizations for single-row UPDATE operations
//!
//! This module provides optimized execution paths for simple single-row updates
//! that bypass the two-phase update semantics when it's safe to do so.
//!
//! ## Optimization Tiers
//!
//! 1. **Super-fast path**: Direct in-place column updates for literal assignments to non-indexed,
//!    non-PK, non-unique columns. No row cloning required.
//!
//! 2. **Fast path**: Single-row PK updates with minimal validation. Skips trigger checks, schema
//!    cloning, and two-phase execution.

use std::collections::{HashMap, HashSet};

use vibesql_ast::{BinaryOperator, Expression, UpdateStmt};
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::{
    errors::ExecutorError,
    evaluator::{coercion::coerce_value_to_column_type, ExpressionEvaluator},
    expression_index_maintenance,
    insert::validation::coerce_value,
    partial_index_maintenance,
};

/// Try to execute UPDATE via fast path for simple single-row PK updates.
/// Returns Some(count) if fast path succeeded, None if we should use normal path.
///
/// Fast path conditions:
/// - WHERE clause is simple equality on single-column primary key
/// - No foreign keys to validate
/// - Table has a primary key index
pub(super) fn try_fast_path_update(
    stmt: &UpdateStmt,
    database: &mut Database,
    schema: &vibesql_catalog::TableSchema,
) -> Result<Option<usize>, ExecutorError> {
    // Use canonical table name from schema for all storage operations
    let table_name = &schema.name;

    // Tuple/row assignments (`SET (a, b) = ...`) need the row-value unpacking
    // in the main path; the fast paths evaluate each assignment as a scalar.
    if stmt.assignments.iter().any(|a| a.is_tuple()) {
        return Ok(None);
    }

    // Check if we have a simple PK lookup in WHERE clause
    let where_clause = match &stmt.where_clause {
        Some(vibesql_ast::WhereClause::Condition(expr)) => expr,
        _ => return Ok(None), // No WHERE or CURRENT OF - use normal path
    };

    // Extract PK value from WHERE clause
    let pk_value = match extract_pk_equality(where_clause, schema) {
        Some(val) => val,
        None => return Ok(None), // Not a simple PK equality
    };

    // Coerce extracted WHERE-clause literals to match PK column affinities.
    // The PK index HashMap is keyed on stored (affinity-coerced) values, so
    // a raw literal can silently miss when types differ — e.g. `WHERE p=1200`
    // on a TEXT PRIMARY KEY storing "1200". See issue #5145.
    let pk_value: Vec<SqlValue> = {
        let pk_indices = match schema.get_primary_key_indices() {
            Some(indices) => indices,
            None => return Ok(None),
        };
        pk_value
            .into_iter()
            .zip(pk_indices.iter())
            .map(|(val, &idx)| coerce_value_to_column_type(val, &schema.columns[idx].data_type))
            .collect()
    };

    // Get table and check for PK index, look up row index
    let row_index = {
        let table = database
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        let pk_index = match table.primary_key_index() {
            Some(idx) => idx,
            None => return Ok(None), // No PK index
        };

        // Look up row by PK
        match pk_index.get(&pk_value) {
            Some(&idx) => idx,
            None => return Ok(Some(0)), // Row not found - 0 rows updated
        }
    }; // table borrow ends here

    // SUPER-FAST PATH: All literal assignments to non-indexed, non-PK, non-unique columns
    // This path avoids ALL row cloning by updating columns in-place
    // Extended from single-assignment to support multiple assignments (ONEPASS optimization)
    if let Some(result) = try_super_fast_path(stmt, database, schema, row_index)? {
        return Ok(Some(result));
    }

    // Skip fast path if table has foreign keys (need validation)
    if !schema.foreign_keys.is_empty() {
        return Ok(None);
    }

    // Skip fast path if table has unique constraints (need validation)
    if !schema.unique_constraints.is_empty() {
        return Ok(None);
    }

    // Check if we're updating PK columns - if so, check for CASCADE requirements
    if let Some(ref pk_idx) = schema.get_primary_key_indices() {
        let updates_pk = stmt.assignments.iter().any(|a| {
            schema.get_column_index(&a.column).map(|idx| pk_idx.contains(&idx)).unwrap_or(false)
        });
        if updates_pk {
            // Check if ANY table in database has foreign keys (might need CASCADE)
            let has_any_fks = database.catalog.list_tables().iter().any(|table_name| {
                database
                    .catalog
                    .get_table(table_name)
                    .map(|s| !s.foreign_keys.is_empty())
                    .unwrap_or(false)
            });
            if has_any_fks {
                return Ok(None); // Use normal path for CASCADE handling
            }
        }
    }

    // The PK-only check above misses a real referential-integrity hazard:
    // another table's FOREIGN KEY can target THIS table's parent key on a
    // column set that is NOT this table's own PRIMARY KEY -- e.g.
    // `FOREIGN KEY(x,y) REFERENCES tce71(a,b)` where tce71's actual PRIMARY
    // KEY is only `a` and the composite parent key `(a,b)` is backed by a
    // separate UNIQUE INDEX or table-level UNIQUE constraint (fkey2-
    // ce7c13.1.2/1.3/1.5/1.6). Without this check, `UPDATE tce71 SET b=201`
    // touches no PK column, `updates_pk` is false, and the fast path applied
    // the write directly with ZERO foreign-key enforcement -- silently
    // orphaning `tce72`'s reference. Fall back to the normal path whenever
    // an assigned column is part of any OTHER table's FK parent key that
    // references this table, using the same lazy parent-index resolution
    // the normal path already relies on (`resolved_parent_indices_for_fk`)
    // so a FK declared before its parent table existed still resolves
    // correctly here.
    {
        let assigned_indices: HashSet<usize> =
            stmt.assignments.iter().filter_map(|a| schema.get_column_index(&a.column)).collect();
        let touches_incoming_fk_parent_key = !assigned_indices.is_empty()
            && database.catalog.list_tables().iter().any(|other_table_name| {
                database
                    .catalog
                    .get_table(other_table_name)
                    .map(|other_schema| {
                        other_schema.foreign_keys.iter().any(|fk| {
                            fk.parent_table.eq_ignore_ascii_case(table_name)
                                && crate::foreign_key_check::resolved_parent_indices_for_fk(
                                    database, fk,
                                )
                                .iter()
                                .any(|idx| assigned_indices.contains(idx))
                        })
                    })
                    .unwrap_or(false)
            });
        if touches_incoming_fk_parent_key {
            return Ok(None); // Use normal path for FK enforcement
        }
    }

    // Re-borrow table to get the old row
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;
    let old_row = table.scan()[row_index].clone();

    // Create evaluator for expression evaluation
    let evaluator = ExpressionEvaluator::with_database(schema, database);

    // Apply assignments
    let mut new_row = old_row.clone();
    let mut changed_columns = HashSet::new();

    for assignment in &stmt.assignments {
        // Check if this is a rowid assignment - fall back to normal path
        let col_name_lower = assignment.column.to_lowercase();
        let is_rowid =
            col_name_lower == "rowid" || col_name_lower == "_rowid_" || col_name_lower == "oid";
        if is_rowid {
            return Ok(None); // rowid update needs normal path for proper handling
        }

        let col_index = schema.get_column_index(&assignment.column).ok_or_else(|| {
            ExecutorError::ColumnNotFound {
                column_name: assignment.column.clone(),
                table_name: stmt.table_name.clone(),
                searched_tables: vec![stmt.table_name.clone()],
                available_columns: schema.columns.iter().map(|c| c.name.clone()).collect(),
            }
        })?;

        // Assigning the INTEGER PRIMARY KEY (rowid alias) column relocates the
        // rowid and applies rowid affinity (`datatype mismatch` on non-integer
        // values, trigger1-15.1) — fall back to the normal path, same as a
        // `SET rowid = ...` assignment above.
        if schema.rowid_alias_column == Some(col_index) {
            return Ok(None);
        }

        let new_value = match &assignment.value {
            vibesql_ast::Expression::Default => {
                let column = &schema.columns[col_index];
                if let Some(default_expr) = &column.default_value {
                    match default_expr {
                        vibesql_ast::Expression::Literal(lit) => lit.clone(),
                        _ => return Ok(None), // Complex default - use normal path
                    }
                } else {
                    SqlValue::Null
                }
            }
            _ => evaluator.eval(&assignment.value, &old_row)?,
        };

        // Apply type affinity coercion (SQLite compatibility)
        let column = &schema.columns[col_index];
        let coerced_value = coerce_value(new_value, &column.data_type)?;

        new_row
            .set(col_index, coerced_value)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        changed_columns.insert(col_index);
    }

    // Quick constraint validation (NOT NULL only for changed columns)
    for &col_idx in &changed_columns {
        let column = &schema.columns[col_idx];
        if !column.nullable && new_row.values[col_idx] == SqlValue::Null {
            // SQLite-compatible format: "NOT NULL constraint failed: <table>.<column>"
            return Err(ExecutorError::SqliteCompatError(format!(
                "NOT NULL constraint failed: {}.{}",
                table_name, column.name
            )));
        }
    }

    // Enforce CHECK constraints against the fully-materialized new row. The
    // fast path previously validated NOT NULL only, silently bypassing CHECK
    // for single-row PK-equality UPDATEs (check.test 9.2/9.3). `new_row` is a
    // full clone of the old row with the assignments applied, so a CHECK that
    // references untouched columns or the rowid evaluates correctly here.
    if !schema.check_constraints.is_empty() {
        super::constraints::ConstraintValidator::new(schema)
            .with_check_constraints_ignored(database.ignore_check_constraints())
            .validate_check_constraints(&new_row)?;
    }

    // Check PK uniqueness if updating PK columns
    let pk_indices = schema.get_primary_key_indices();
    if let Some(ref pk_idx) = pk_indices {
        let updates_pk = changed_columns.iter().any(|c| pk_idx.contains(c));
        if updates_pk {
            // PK is being updated - need to check uniqueness
            let new_pk: Vec<_> = pk_idx.iter().map(|&i| new_row.values[i].clone()).collect();
            if let Some(pk_index) = table.primary_key_index() {
                if let Some(&existing_idx) = pk_index.get(&new_pk) {
                    if existing_idx != row_index {
                        return Err(ExecutorError::ConstraintViolation(format!(
                            "PRIMARY KEY constraint violation: duplicate key {:?} on {}",
                            new_pk, stmt.table_name
                        )));
                    }
                }
            }
        }
    }

    // Update user-defined indexes FIRST (while we still have both row references)
    // Pass changed_columns to skip indexes that don't involve any modified columns
    database.update_indexes_for_update(
        table_name,
        &old_row,
        &new_row,
        row_index,
        Some(&changed_columns),
    );

    // Maintain expression indexes for this update
    expression_index_maintenance::maintain_expression_indexes_for_update(
        database, table_name, &old_row, &new_row, row_index,
    );

    // Maintain partial indexes for this update (predicate evaluated per row).
    partial_index_maintenance::maintain_partial_indexes_for_update(
        database, table_name, &old_row, &new_row, row_index,
    );

    // Phase 1c (Issue #5150 / #5136): stamp the new row's xmin with the
    // active txn id when the `mvcc_enabled` feature is on. Off-state is a
    // no-op, preserving pre-MVCC behavior bit-for-bit.
    let txn_id = database.transaction_id();
    vibesql_storage::stamp_xmin_for_write(&mut new_row, txn_id);
    new_row.xmax = None;

    // Apply the update directly (transfers ownership of new_row, no clone needed)
    let table_mut = database
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Use unchecked variant - row is already validated above
    table_mut.update_row_unchecked(row_index, new_row, &old_row, &changed_columns);

    Ok(Some(1))
}

/// Try SUPER-FAST path: direct in-place column updates for literal assignments
/// to non-indexed, non-PK, non-unique columns.
///
/// This is the ONEPASS optimization for single-row updates:
/// - Supports multiple assignments (not just single)
/// - Validates all columns can be updated in-place
/// - No row cloning required
///
/// Returns Some(1) if all updates were applied in-place, None if should use normal path.
fn try_super_fast_path(
    stmt: &UpdateStmt,
    database: &mut Database,
    schema: &vibesql_catalog::TableSchema,
    row_index: usize,
) -> Result<Option<usize>, ExecutorError> {
    // Use canonical table name from schema for all storage operations
    let table_name = &schema.name;

    // CHECK constraints may reference columns the UPDATE does not touch (e.g.
    // `CHECK(b > a)` with only `b` assigned) or the rowid, so they can only be
    // evaluated against a fully-materialized row. The super-fast path mutates
    // columns in place without building such a row, so defer any table carrying
    // CHECK constraints to the row-materializing fast/normal path where the
    // check is actually enforced (regression: single-row PK UPDATEs silently
    // bypassed CHECK — see check.test 9.2/9.3).
    if !schema.check_constraints.is_empty() {
        return Ok(None);
    }

    // Collect all literal updates that can be done in-place
    let mut inplace_updates: Vec<(usize, SqlValue)> = Vec::new();

    let pk_indices = schema.get_primary_key_indices();

    for assignment in &stmt.assignments {
        // Check if this is a rowid assignment - fall back to normal path
        let col_name_lower = assignment.column.to_lowercase();
        let is_rowid =
            col_name_lower == "rowid" || col_name_lower == "_rowid_" || col_name_lower == "oid";
        if is_rowid {
            return Ok(None); // rowid update needs normal path for proper handling
        }

        // Check if value is a literal (no expression evaluation needed)
        let new_value = match &assignment.value {
            vibesql_ast::Expression::Literal(val) => val.clone(),
            _ => return Ok(None), // Non-literal expression - use normal path
        };

        let col_index = match schema.get_column_index(&assignment.column) {
            Some(idx) => idx,
            None => return Ok(None), // Column not found - let normal path handle error
        };

        // Check column is not in PK
        let is_pk_col = pk_indices.as_ref().map(|pk| pk.contains(&col_index)).unwrap_or(false);
        if is_pk_col {
            return Ok(None); // PK update needs full validation
        }

        // Check column is not in any unique constraint
        let col_name_upper = assignment.column.to_uppercase();
        let is_unique_col = schema
            .unique_constraints
            .iter()
            .any(|uc| uc.iter().any(|name| name.to_uppercase() == col_name_upper));
        if is_unique_col {
            return Ok(None); // Unique constraint needs validation
        }

        // Check column is not a FOREIGN KEY child-side column. This in-place
        // path writes the new value directly without running
        // `ForeignKeyValidator::collect_constraints_with_old`, so an FK
        // column update landing here silently skipped referential-integrity
        // enforcement whenever the column was not *also* independently
        // indexed/unique/PK (fkey3-3.6.5: `UPDATE t SET parent_id=1000
        // WHERE id=2` on a self-referential composite-key FK reached this
        // path — `parent_id` alone carries no index — and wrote the
        // dangling value with zero FK validation). Any assignment touching
        // an FK column must fall back to the row-materializing path where
        // the check actually runs.
        let is_fk_col = schema.foreign_keys.iter().any(|fk| fk.column_indices.contains(&col_index));
        if is_fk_col {
            return Ok(None); // FK column update needs full validation
        }

        // Apply type affinity coercion (SQLite compatibility)
        let column = &schema.columns[col_index];
        let coerced_value = coerce_value(new_value, &column.data_type)?;

        // Check NOT NULL constraint (after coercion)
        if !column.nullable && coerced_value == SqlValue::Null {
            // SQLite-compatible format: "NOT NULL constraint failed: <table>.<column>"
            return Err(ExecutorError::SqliteCompatError(format!(
                "NOT NULL constraint failed: {}.{}",
                table_name, column.name
            )));
        }

        // Check no user-defined indexes on this column
        if database.has_index_on_column(table_name, &assignment.column) {
            return Ok(None); // Index update needs normal path
        }

        // Partial indexes may reference any column in their WHERE predicate
        // (not just the indexed column). Skip the super-fast in-place path
        // when any partial index exists so the partial-index maintenance
        // helper can re-evaluate predicates.
        if database.has_partial_indexes(table_name) {
            return Ok(None);
        }

        inplace_updates.push((col_index, coerced_value));
    }

    // All checks passed - apply updates in-place
    if inplace_updates.is_empty() {
        return Ok(None); // No updates to apply
    }

    // Phase 1c (Issue #5150 / #5136): the super-fast in-place path doesn't
    // produce a "new row" object — it mutates column values directly. To
    // record the MVCC version transition we stamp xmin on the same physical
    // row after applying the column writes. Off-state: no stamping.
    #[cfg(feature = "mvcc_enabled")]
    let txn_id = database.transaction_id();

    let table_mut = database
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Apply all column updates in-place (no row cloning!)
    for (col_index, new_value) in inplace_updates {
        table_mut.update_column_inplace(row_index, col_index, new_value);
    }

    // Stamp xmin on the in-place row. The row also remains live, so xmax
    // is left at its current value (typically None; if it had previously
    // been stamped by a rolled-back delete we'd be losing that, but the
    // rollback path restores tables wholesale, so this can't actually
    // arise — see TransactionManager::rollback_transaction).
    #[cfg(feature = "mvcc_enabled")]
    if let Some(id) = txn_id {
        table_mut.stamp_row_xmin_inplace(row_index, id);
    }

    Ok(Some(1))
}

/// Extract primary key values from WHERE expression.
///
/// Supports:
/// - Single-column PK: `pk = value` or `value = pk`
/// - Composite PK: `pk1 = val1 AND pk2 = val2` (any order)
///
/// Returns Some(pk_values) in PK column order if all PK columns are matched,
/// None otherwise.
pub(super) fn extract_pk_equality(
    expr: &Expression,
    schema: &vibesql_catalog::TableSchema,
) -> Option<Vec<SqlValue>> {
    let pk_indices = schema.get_primary_key_indices()?;
    if pk_indices.is_empty() {
        return None;
    }

    // Collect all column = literal equalities from the expression
    let mut equalities: HashMap<usize, SqlValue> = HashMap::new();
    collect_pk_equalities(expr, schema, &mut equalities);

    // Check if we have all PK columns and build result in PK order
    let mut pk_values = Vec::with_capacity(pk_indices.len());
    for &pk_col in &pk_indices {
        match equalities.get(&pk_col) {
            Some(value) => pk_values.push(value.clone()),
            None => return None, // Missing PK column
        }
    }

    Some(pk_values)
}

/// Recursively collect column = literal equalities from WHERE expression
fn collect_pk_equalities(
    expr: &Expression,
    schema: &vibesql_catalog::TableSchema,
    equalities: &mut HashMap<usize, SqlValue>,
) {
    match expr {
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            // Recurse into AND branches
            collect_pk_equalities(left, schema, equalities);
            collect_pk_equalities(right, schema, equalities);
        }
        Expression::Conjunction(exprs) => {
            // Handle flattened AND chains
            for e in exprs {
                collect_pk_equalities(e, schema, equalities);
            }
        }
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Check: column = literal
            if let (Expression::ColumnRef(col_id), Expression::Literal(value)) =
                (left.as_ref(), right.as_ref())
            {
                if let Some(col_index) = schema.get_column_index(col_id.column_canonical()) {
                    equalities.insert(col_index, value.clone());
                }
            }
            // Check: literal = column
            else if let (Expression::Literal(value), Expression::ColumnRef(col_id)) =
                (left.as_ref(), right.as_ref())
            {
                if let Some(col_index) = schema.get_column_index(col_id.column_canonical()) {
                    equalities.insert(col_index, value.clone());
                }
            }
        }
        _ => {} // Ignore other expressions
    }
}
