//! UPDATE FROM clause handling
//!
//! This module implements SQLite's UPDATE FROM syntax (added in SQLite 3.33.0),
//! which allows multi-table UPDATE statements:
//!
//! ```sql
//! UPDATE t1 SET col = t2.val FROM t2 WHERE t1.id = t2.id;
//! ```
//!
//! The FROM clause specifies additional tables to join with the target table.
//! Values from these tables can be used in both SET expressions and WHERE clause.

use std::collections::{HashMap, HashSet};

use vibesql_ast::{
    Assignment, ColumnIdentifier, Expression, FromClause, JoinType, SelectItem, SelectStmt,
    UpdateStmt, WhereClause,
};
use vibesql_catalog::TableSchema;
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Result of executing the join between target table and FROM tables
pub struct UpdateFromJoinResult {
    /// Target table row index and the computed SET values for each matched row
    pub matched_rows: Vec<UpdateFromMatch>,
}

/// A single matched row from the UPDATE FROM join
pub struct UpdateFromMatch {
    /// Physical row index in the target table
    pub row_index: usize,
    /// Original row from target table
    pub target_row: Row,
    /// Computed values for each SET assignment (in order)
    pub set_values: Vec<SqlValue>,
}

/// Execute the join for UPDATE FROM and return matched rows with computed SET values
///
/// This builds and executes a synthetic SELECT that:
/// 1. Joins the target table with all FROM tables
/// 2. Computes SET expression values as part of the SELECT
/// 3. Returns the target row index and computed values for each match
pub fn execute_update_from_join(
    stmt: &UpdateStmt,
    from_clauses: &[FromClause],
    database: &Database,
    target_schema: &TableSchema,
) -> Result<UpdateFromJoinResult, ExecutorError> {
    let target_table_name = &target_schema.name;
    let target_alias = stmt.alias.clone();
    let target_prefix = target_alias.clone().unwrap_or_else(|| target_table_name.clone());

    // Determine how to identify target rows:
    // - For WITHOUT ROWID tables: use PRIMARY KEY columns
    // - For regular tables: use rowid
    let pk_columns = get_pk_column_names(target_schema);
    let use_rowid = !target_schema.without_rowid && pk_columns.is_empty();

    // Build SELECT list:
    // 1. Target table identifier columns (rowid or PK columns)
    // 2. Each SET expression value (computed using joined context)
    let mut select_list = Vec::new();
    let num_id_columns: usize;

    if use_rowid {
        // Use rowid for regular tables
        select_list.push(SelectItem::Expression {
            expr: Expression::ColumnRef(ColumnIdentifier::qualified(
                &target_prefix,
                false,
                "rowid",
                false,
            )),
            alias: Some("__target_rowid__".to_string()),
            source_text: None,
        });
        num_id_columns = 1;
    } else {
        // Use PRIMARY KEY columns for WITHOUT ROWID tables or tables with explicit PK
        for (i, pk_col) in pk_columns.iter().enumerate() {
            select_list.push(SelectItem::Expression {
                expr: Expression::ColumnRef(ColumnIdentifier::qualified(
                    &target_prefix,
                    false,
                    pk_col,
                    false,
                )),
                alias: Some(format!("__target_pk_{}__", i)),
                source_text: None,
            });
        }
        num_id_columns = pk_columns.len();
    }

    // Add each SET expression to be computed in the join context
    for (i, assignment) in stmt.assignments.iter().enumerate() {
        select_list.push(SelectItem::Expression {
            expr: assignment.value.clone(),
            alias: Some(format!("__set_{}__", i)),
            source_text: None,
        });
    }

    // Build FROM clause: target_table [alias], from_clause1, from_clause2, ...
    let target_from = FromClause::Table {
        name: target_table_name.clone(),
        alias: target_alias,
        column_aliases: None,
        quoted: stmt.quoted,
    };

    // Combine the target table with FROM clauses
    // For JOIN-type FROM clauses, we need to extract the leftmost table and
    // cross-join with target, then reattach the rest of the join structure.
    // This ensures `t5, m1 LEFT JOIN m2` behaves as `(t5 CROSS JOIN m1) LEFT JOIN m2`
    let mut combined_from = target_from;
    for from_clause in from_clauses {
        combined_from = combine_with_from_clause(combined_from, from_clause.clone());
    }

    // Build WHERE clause from UPDATE's WHERE clause
    let where_clause = stmt.where_clause.as_ref().and_then(|wc| match wc {
        WhereClause::Condition(expr) => Some(expr.clone()),
        WhereClause::CurrentOf(_) => None, // Not supported with UPDATE FROM
    });

    // Build the synthetic SELECT statement
    let select_stmt = SelectStmt {
        with_clause: stmt.with_clause.clone(),
        select_list,
        distinct: false,
        into_table: None,
        into_variables: None,
        from: Some(combined_from),
        where_clause,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
        values: None,
    };

    // Execute the SELECT
    let executor = crate::SelectExecutor::new(database);
    let rows = executor.execute(&select_stmt)?;

    // Build a map from identifier values to SET values
    // Key: either rowid (as single-element vec) or PK column values
    let num_assignments = stmt.assignments.len();
    let mut id_to_set_values: HashMap<Vec<SqlValue>, Vec<SqlValue>> = HashMap::new();

    for row in rows {
        // Extract identifier values (first num_id_columns values)
        let id_values: Vec<SqlValue> = row.values[..num_id_columns].to_vec();

        // Skip NULL identifiers
        if id_values.iter().any(|v| matches!(v, SqlValue::Null)) {
            continue;
        }

        // Only keep first match per identifier (SQLite semantics)
        if id_to_set_values.contains_key(&id_values) {
            continue;
        }

        // Extract SET values
        let set_values: Vec<SqlValue> = (0..num_assignments)
            .map(|i| {
                row.values
                    .get(num_id_columns + i)
                    .cloned()
                    .unwrap_or(SqlValue::Null)
            })
            .collect();

        id_to_set_values.insert(id_values, set_values);
    }

    // Now scan the target table to find matching rows
    let target_table = database
        .get_table(target_table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(target_table_name.clone()))?;

    let mut matched_rows = Vec::new();

    // Get PK column indices for lookup
    let pk_indices: Vec<usize> = if use_rowid {
        vec![] // Not used when using rowid
    } else {
        pk_columns
            .iter()
            .filter_map(|name| target_schema.get_column_index(name))
            .collect()
    };

    for (row_index, target_row) in target_table.scan().iter().enumerate() {
        // Build identifier for this row
        let id_values: Vec<SqlValue> = if use_rowid {
            // Use the row's rowid
            let rowid = target_row
                .row_id
                .map(|id| SqlValue::Integer(id as i64))
                .unwrap_or_else(|| SqlValue::Integer((row_index + 1) as i64));
            vec![rowid]
        } else {
            // Use PK column values
            pk_indices
                .iter()
                .map(|&idx| target_row.get(idx).cloned().unwrap_or(SqlValue::Null))
                .collect()
        };

        // Check if this row matches any from the join result
        if let Some(set_values) = id_to_set_values.remove(&id_values) {
            matched_rows.push(UpdateFromMatch {
                row_index,
                target_row: target_row.clone(),
                set_values,
            });
        }
    }

    Ok(UpdateFromJoinResult { matched_rows })
}

/// Get the PRIMARY KEY column names for a table
fn get_pk_column_names(schema: &TableSchema) -> Vec<String> {
    // Primary key is stored as Option<Vec<String>> in TableSchema
    schema.primary_key.clone().unwrap_or_default()
}

/// Combine the accumulated FROM clause with a new FROM clause
///
/// For simple tables, this creates a CROSS JOIN.
/// For JOIN-type FROM clauses, this extracts the leftmost table, cross-joins with it,
/// then reattaches the rest of the join structure. This matches SQLite's parsing
/// of comma-separated FROM items: `t1, t2 LEFT JOIN t3` = `(t1 CROSS JOIN t2) LEFT JOIN t3`
fn combine_with_from_clause(accumulated: FromClause, from_clause: FromClause) -> FromClause {
    match from_clause {
        // For a simple table, just cross join
        FromClause::Table { .. } | FromClause::Subquery { .. } | FromClause::Values { .. } => {
            FromClause::Join {
                left: Box::new(accumulated),
                right: Box::new(from_clause),
                join_type: JoinType::Cross,
                condition: None,
                using_columns: None,
                natural: false,
                alias: None,
            }
        }
        // For a JOIN, we need to inject the accumulated clause on the left side
        FromClause::Join {
            left,
            right,
            join_type,
            condition,
            using_columns,
            natural,
            alias,
        } => {
            // Recursively combine with the left side of the join
            let new_left = combine_with_from_clause(accumulated, *left);
            FromClause::Join {
                left: Box::new(new_left),
                right,
                join_type,
                condition,
                using_columns,
                natural,
                alias,
            }
        }
    }
}

/// Apply UPDATE FROM matches to build update operations
///
/// Takes the matched rows with pre-computed SET values and creates
/// (row_index, old_row, new_row, changed_columns) tuples for the executor.
pub fn apply_update_from_matches(
    matches: &[UpdateFromMatch],
    assignments: &[Assignment],
    target_schema: &TableSchema,
) -> Result<Vec<(usize, Row, Row, HashSet<usize>, bool)>, ExecutorError> {
    let pk_indices = target_schema.get_primary_key_indices();
    let mut updates = Vec::with_capacity(matches.len());

    for m in matches {
        let mut new_row = m.target_row.clone();
        let mut changed_columns = HashSet::new();
        let mut updates_pk = false;

        for (i, assignment) in assignments.iter().enumerate() {
            // Handle rowid assignment specially
            let col_name_lower = assignment.column.to_lowercase();
            let is_rowid =
                col_name_lower == "rowid" || col_name_lower == "_rowid_" || col_name_lower == "oid";

            if is_rowid {
                // Handle rowid update
                if let Some(ipk_col_idx) = target_schema.rowid_alias_column {
                    new_row
                        .set(ipk_col_idx, m.set_values[i].clone())
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                    changed_columns.insert(ipk_col_idx);
                    if pk_indices.as_ref().is_some_and(|pk| pk.contains(&ipk_col_idx)) {
                        updates_pk = true;
                    }
                } else {
                    // Update virtual rowid
                    let new_rowid = match &m.set_values[i] {
                        SqlValue::Integer(id) => *id as u64,
                        SqlValue::Bigint(id) => *id as u64,
                        other => {
                            return Err(ExecutorError::UnsupportedExpression(format!(
                                "ROWID must be an integer, got {:?}",
                                other
                            )));
                        }
                    };
                    new_row.row_id = Some(new_rowid);
                }
                continue;
            }

            // Find column index
            let col_index = target_schema.get_column_index(&assignment.column).ok_or_else(|| {
                ExecutorError::NoSuchColumn { column_ref: assignment.column.clone() }
            })?;

            // Coerce value to column type
            let coerced_value = crate::insert::validation::coerce_value(
                m.set_values[i].clone(),
                &target_schema.columns[col_index].data_type,
            )?;

            new_row
                .set(col_index, coerced_value)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            changed_columns.insert(col_index);

            // Check if this column is part of primary key
            if let Some(ref pk) = pk_indices {
                if pk.contains(&col_index) {
                    updates_pk = true;
                }
            }
        }

        updates.push((m.row_index, m.target_row.clone(), new_row, changed_columns, updates_pk));
    }

    Ok(updates)
}
