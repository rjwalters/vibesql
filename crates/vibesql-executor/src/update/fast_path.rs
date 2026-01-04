//! Fast path optimizations for single-row UPDATE operations
//!
//! This module provides optimized execution paths for simple single-row updates
//! that bypass the two-phase update semantics when it's safe to do so.
//!
//! ## Optimization Tiers
//!
//! 1. **Super-fast path**: Direct in-place column updates for literal assignments
//!    to non-indexed, non-PK, non-unique columns. No row cloning required.
//!
//! 2. **Fast path**: Single-row PK updates with minimal validation. Skips
//!    trigger checks, schema cloning, and two-phase execution.

use std::collections::{HashMap, HashSet};

use vibesql_ast::{BinaryOperator, Expression, UpdateStmt};
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::{
    errors::ExecutorError, evaluator::ExpressionEvaluator,
    expression_index_maintenance, insert::validation::coerce_value,
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
            return Err(ExecutorError::ConstraintViolation(format!(
                "NOT NULL constraint violation: column '{}' cannot be NULL",
                column.name
            )));
        }
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
        database,
        table_name,
        &old_row,
        &new_row,
        row_index,
    );

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

        // Apply type affinity coercion (SQLite compatibility)
        let column = &schema.columns[col_index];
        let coerced_value = coerce_value(new_value, &column.data_type)?;

        // Check NOT NULL constraint (after coercion)
        if !column.nullable && coerced_value == SqlValue::Null {
            return Err(ExecutorError::ConstraintViolation(format!(
                "NOT NULL constraint violation: column '{}' cannot be NULL",
                column.name
            )));
        }

        // Check no user-defined indexes on this column
        if database.has_index_on_column(table_name, &assignment.column) {
            return Ok(None); // Index update needs normal path
        }

        inplace_updates.push((col_index, coerced_value));
    }

    // All checks passed - apply updates in-place
    if inplace_updates.is_empty() {
        return Ok(None); // No updates to apply
    }

    let table_mut = database
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Apply all column updates in-place (no row cloning!)
    for (col_index, new_value) in inplace_updates {
        table_mut.update_column_inplace(row_index, col_index, new_value);
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
