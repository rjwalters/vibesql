//! Shared helper functions for fast path execution
//!
//! This module provides common utilities used by multiple fast path strategies:
//! - Projection (applying SELECT list to rows)
//! - Filtering (applying WHERE clause)
//! - Sorting (applying ORDER BY)
//! - Value extraction (parsing expressions for literals and columns)

use std::collections::{HashMap, HashSet};

use vibesql_ast::{Expression, OrderByItem, OrderDirection, SelectItem};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use crate::select::executor::builder::SelectExecutor;

/// Result of extracting equality predicate values from WHERE clause
///
/// Distinguishes between:
/// - `Values(map)`: Successfully extracted equality values
/// - `Contradiction`: Multiple equality predicates on same column with different values
///   (e.g., col = 1 AND col = 2 is always false)
pub(crate) enum EqualityResult {
    Values(HashMap<String, SqlValue>),
    Contradiction,
}

impl SelectExecutor<'_> {
    /// Apply WHERE filter in fast path (simplified, no CSE)
    pub(crate) fn apply_where_filter_fast(
        &self,
        where_clause: &Expression,
        rows: Vec<Row>,
        schema: &CombinedSchema,
    ) -> Result<Vec<Row>, ExecutorError> {
        use crate::evaluator::compiled::CompiledPredicate;

        // Try to use compiled predicate for fast evaluation
        let compiled = CompiledPredicate::compile(where_clause, schema);

        if compiled.is_fully_compiled() {
            // Fast path: use compiled predicate
            let filtered: Vec<Row> =
                rows.into_iter().filter(|row| compiled.evaluate(row).unwrap_or(false)).collect();
            Ok(filtered)
        } else {
            // Fall back to standard evaluator
            use crate::evaluator::CombinedExpressionEvaluator;
            let evaluator = CombinedExpressionEvaluator::with_database(schema, self.database);

            let mut filtered = Vec::new();
            for row in rows {
                let result = evaluator.eval(where_clause, &row)?;
                if matches!(result, SqlValue::Boolean(true)) {
                    filtered.push(row);
                }
                evaluator.clear_cse_cache();
            }
            Ok(filtered)
        }
    }

    /// Apply projection in fast path
    pub(crate) fn apply_projection_fast(
        &self,
        select_list: &[SelectItem],
        rows: Vec<Row>,
        schema: &CombinedSchema,
    ) -> Result<Vec<Row>, ExecutorError> {
        use crate::evaluator::CombinedExpressionEvaluator;
        use crate::select::projection::project_row_combined;

        // Check if this is SELECT * - no projection needed
        if select_list.len() == 1 && matches!(&select_list[0], SelectItem::Wildcard { .. }) {
            return Ok(rows);
        }

        // Validate that all column references exist in the schema.
        // This is important even when there are no rows to return a proper error.
        self.validate_select_columns(select_list, schema)?;

        let evaluator = CombinedExpressionEvaluator::with_database(schema, self.database);
        let buffer_pool = self.query_buffer_pool();

        let mut projected = Vec::with_capacity(rows.len());
        for row in &rows {
            let projected_row =
                project_row_combined(row, select_list, &evaluator, schema, &None, buffer_pool)?;
            projected.push(projected_row);
        }

        Ok(projected)
    }

    /// Validate that all column references in the SELECT list exist in the schema
    pub(crate) fn validate_select_columns(
        &self,
        select_list: &[SelectItem],
        schema: &CombinedSchema,
    ) -> Result<(), ExecutorError> {
        for item in select_list {
            if let SelectItem::Expression { expr, .. } = item {
                Self::validate_expression_columns(expr, schema)?;
            }
        }
        Ok(())
    }

    /// Recursively validate column references in an expression
    pub(crate) fn validate_expression_columns(
        expr: &Expression,
        schema: &CombinedSchema,
    ) -> Result<(), ExecutorError> {
        match expr {
            Expression::ColumnRef { table, column } => {
                if schema.get_column_index(table.as_deref(), column).is_none() {
                    // Collect available column names for the error message
                    let available_columns: Vec<String> = schema
                        .table_schemas
                        .values()
                        .flat_map(|(_, s)| s.columns.iter().map(|c| c.name.clone()))
                        .collect();
                    return Err(ExecutorError::ColumnNotFound {
                        column_name: column.clone(),
                        table_name: table.clone().unwrap_or_else(|| "unknown".to_string()),
                        searched_tables: schema.table_names(),
                        available_columns,
                    });
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                Self::validate_expression_columns(left, schema)?;
                Self::validate_expression_columns(right, schema)?;
            }
            Expression::UnaryOp { expr, .. } => {
                Self::validate_expression_columns(expr, schema)?;
            }
            Expression::Cast { expr, .. } => {
                Self::validate_expression_columns(expr, schema)?;
            }
            // Literals and other expressions don't need column validation
            _ => {}
        }
        Ok(())
    }

    /// Apply ORDER BY sorting in fast path
    ///
    /// Uses simple column-based sorting for the fast path.
    /// ORDER BY expressions must be simple column references (validated by is_simple_order_by).
    /// ORDER BY with aliases is excluded at detection time by uses_select_alias().
    pub(crate) fn apply_order_by_fast(
        &self,
        order_by: &[OrderByItem],
        mut rows: Vec<Row>,
        schema: &CombinedSchema,
    ) -> Result<Vec<Row>, ExecutorError> {
        use crate::select::grouping::compare_sql_values;
        use std::cmp::Ordering;

        // Pre-compute column indices for ORDER BY columns
        let mut sort_indices: Vec<(usize, OrderDirection)> = Vec::with_capacity(order_by.len());

        for item in order_by {
            let col_idx = match &item.expr {
                Expression::ColumnRef { table, column } => schema
                    .get_column_index(table.as_deref(), column)
                    .ok_or_else(|| ExecutorError::ColumnNotFound {
                        column_name: column.clone(),
                        table_name: table.clone().unwrap_or_default(),
                        searched_tables: schema.table_names(),
                        available_columns: vec![],
                    })?,
                _ => {
                    return Err(ExecutorError::Other(
                        "Fast path ORDER BY requires simple column references".to_string(),
                    ));
                }
            };
            sort_indices.push((col_idx, item.direction.clone()));
        }

        // Sort rows by the specified columns
        rows.sort_by(|a, b| {
            for (col_idx, dir) in &sort_indices {
                let val_a = &a.values[*col_idx];
                let val_b = &b.values[*col_idx];

                // Handle NULLs: always sort last regardless of ASC/DESC
                let cmp = match (val_a.is_null(), val_b.is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => return Ordering::Greater, // NULL always sorts last
                    (false, true) => return Ordering::Less,    // non-NULL always sorts first
                    (false, false) => {
                        // Compare non-NULL values, respecting direction
                        match dir {
                            OrderDirection::Asc => compare_sql_values(val_a, val_b),
                            OrderDirection::Desc => compare_sql_values(val_a, val_b).reverse(),
                        }
                    }
                };

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });

        Ok(rows)
    }

    /// Extract equality predicate values for given columns from WHERE clause
    ///
    /// Returns `EqualityResult::Contradiction` if multiple equality predicates on the
    /// same column have different values (e.g., col = 1 AND col = 2), which means
    /// the WHERE clause is always false and no rows can match.
    pub(crate) fn extract_pk_values(
        &self,
        expr: &Expression,
        pk_columns: &[&str],
    ) -> EqualityResult {
        let mut values = HashMap::new();
        if self.collect_pk_equality_values(expr, pk_columns, &mut values) {
            EqualityResult::Values(values)
        } else {
            EqualityResult::Contradiction
        }
    }

    /// Recursively collect equality values for PK columns
    ///
    /// Returns `false` if a contradiction is detected (multiple equalities on same
    /// column with different values), `true` otherwise.
    pub(crate) fn collect_pk_equality_values(
        &self,
        expr: &Expression,
        pk_columns: &[&str],
        values: &mut HashMap<String, SqlValue>,
    ) -> bool {
        if let Expression::BinaryOp { left, op, right } = expr {
            match op {
                vibesql_ast::BinaryOperator::And => {
                    // Recurse into both sides of AND
                    // Short-circuit if contradiction found
                    if !self.collect_pk_equality_values(left, pk_columns, values) {
                        return false;
                    }
                    if !self.collect_pk_equality_values(right, pk_columns, values) {
                        return false;
                    }
                }
                vibesql_ast::BinaryOperator::Equal => {
                    // Check for column = literal pattern
                    if let Some((col_name, value)) = self.extract_column_literal_pair(left, right) {
                        // Case-insensitive comparison for SQL identifiers
                        // Parser uppercases identifiers but schema may have lowercase column names
                        if pk_columns.iter().any(|pk| pk.eq_ignore_ascii_case(&col_name)) {
                            let key = col_name.to_ascii_lowercase();
                            // Check for contradiction: multiple equalities with different values
                            if let Some(existing) = values.get(&key) {
                                if existing != &value {
                                    // Contradiction: col = X AND col = Y where X != Y
                                    return false;
                                }
                                // Same value, no need to insert again
                            } else {
                                values.insert(key, value);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        true
    }

    /// Extract column name and literal value from an equality expression
    pub(crate) fn extract_column_literal_pair(
        &self,
        left: &Expression,
        right: &Expression,
    ) -> Option<(String, SqlValue)> {
        // Try left = column, right = literal
        if let Expression::ColumnRef { column, .. } = left {
            if let Some(value) = self.literal_to_value(right) {
                return Some((column.clone(), value));
            }
        }
        // Try left = literal, right = column
        if let Expression::ColumnRef { column, .. } = right {
            if let Some(value) = self.literal_to_value(left) {
                return Some((column.clone(), value));
            }
        }
        None
    }

    /// Extract SqlValue from a literal expression
    pub(crate) fn literal_to_value(&self, expr: &Expression) -> Option<SqlValue> {
        match expr {
            Expression::Literal(val) => Some(val.clone()),
            _ => None,
        }
    }

    /// Check if a WHERE clause is fully satisfied by equality predicates on the given columns.
    ///
    /// Returns true ONLY if the WHERE clause contains ONLY equality predicates
    /// on the specified columns (connected by AND). Any other predicates (non-equality
    /// comparisons, predicates on other columns, OR, etc.) will cause this to return false.
    ///
    /// This is used to determine if additional filtering is needed after an index lookup.
    pub(crate) fn where_fully_satisfied_by_equality_columns(
        &self,
        expr: &Expression,
        covered_columns: &HashSet<String>,
    ) -> bool {
        match expr {
            // Equality predicate: col = literal
            Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::Equal, right } => {
                // Check if this is an equality on a covered column
                if let Some((col_name, _)) = self.extract_column_literal_pair(left, right) {
                    covered_columns.contains(&col_name.to_ascii_lowercase())
                } else {
                    false // Not a simple column = literal pattern
                }
            }
            // AND: both sides must be fully satisfied
            Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
                self.where_fully_satisfied_by_equality_columns(left, covered_columns)
                    && self.where_fully_satisfied_by_equality_columns(right, covered_columns)
            }
            // Any other expression type is not satisfied by the index lookup
            _ => false,
        }
    }

    /// Extract IN list values for a column from WHERE clause
    /// Returns None if no IN predicate found for the column
    pub(crate) fn extract_in_values(expr: &Expression, column_name: &str) -> Option<Vec<SqlValue>> {
        match expr {
            Expression::InList { expr: col_expr, values, negated } => {
                if *negated {
                    return None; // NOT IN is not a contradiction detector
                }
                // Check if the IN expression is for our target column
                if let Expression::ColumnRef { column, .. } = col_expr.as_ref() {
                    if column.eq_ignore_ascii_case(column_name) {
                        // Extract all literal values from the IN list
                        let mut result = Vec::new();
                        for v in values {
                            if let Expression::Literal(val) = v {
                                result.push(val.clone());
                            }
                        }
                        if !result.is_empty() {
                            return Some(result);
                        }
                    }
                }
                None
            }
            Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
                // Recursively search both sides of AND
                Self::extract_in_values(left, column_name)
                    .or_else(|| Self::extract_in_values(right, column_name))
            }
            _ => None,
        }
    }

    /// Extract simple column names from SELECT list
    ///
    /// Returns Some(column_names) if SELECT list contains only simple column references,
    /// None otherwise (e.g., SELECT *, SELECT col + 1, SELECT func(col), etc.)
    pub(crate) fn extract_select_columns(
        &self,
        select_list: &[SelectItem],
        _table_schema: &vibesql_catalog::TableSchema,
    ) -> Option<Vec<String>> {
        let mut columns = Vec::new();

        for item in select_list {
            match item {
                SelectItem::Expression { expr: Expression::ColumnRef { column, .. }, .. } => {
                    columns.push(column.clone());
                }
                // Wildcards or complex expressions can't use covering scan
                _ => return None,
            }
        }

        if columns.is_empty() {
            None
        } else {
            Some(columns)
        }
    }

    /// Try to extract simple column indices from a SELECT list
    ///
    /// Returns Some(indices) if all SELECT items are simple column references,
    /// None otherwise (indicating fallback to full evaluator path is needed).
    ///
    /// This is an optimization for TPC-C style queries where the SELECT list
    /// contains only column references like `SELECT c_id, c_first, c_middle ...`
    pub(crate) fn try_extract_simple_column_indices(
        &self,
        select_list: &[SelectItem],
        table_schema: &vibesql_catalog::TableSchema,
    ) -> Option<Vec<usize>> {
        let mut indices = Vec::with_capacity(select_list.len());

        for item in select_list {
            match item {
                SelectItem::Expression {
                    expr: Expression::ColumnRef { table: _, column }, ..
                } => {
                    // Find column index by name (case-insensitive)
                    let idx = table_schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(column))?;
                    indices.push(idx);
                }
                _ => return None, // Not a simple column reference
            }
        }

        Some(indices)
    }

    /// Project rows by direct column indices (ultra-fast path)
    ///
    /// This avoids:
    /// - Creating CombinedSchema (which clones TableSchema)
    /// - Creating CombinedExpressionEvaluator
    /// - Going through the full evaluator machinery
    ///
    /// For simple column projections, this is 10-100x faster than the full path.
    pub(crate) fn project_by_indices_fast(&self, rows: Vec<Row>, col_indices: &[usize]) -> Vec<Row> {
        rows.into_iter()
            .map(|row| {
                let projected_values: Vec<SqlValue> =
                    col_indices.iter().map(|&idx| row.values[idx].clone()).collect();
                Row::from_vec(projected_values)
            })
            .collect()
    }

    /// Extract BETWEEN bounds from a WHERE clause for a target column
    ///
    /// Returns Some((low, high)) if the expression contains `column BETWEEN low AND high`,
    /// None otherwise. Handles nested ANDs to find the BETWEEN clause.
    pub(crate) fn extract_between_bounds(
        &self,
        expr: &Expression,
        target_column: &str,
    ) -> Option<(SqlValue, SqlValue)> {
        match expr {
            Expression::Between { expr: col_expr, low, high, negated, .. } => {
                if *negated {
                    return None;
                }
                if let Expression::ColumnRef { column, .. } = col_expr.as_ref() {
                    if column.eq_ignore_ascii_case(target_column) {
                        let low_val = self.literal_to_value(low)?;
                        let high_val = self.literal_to_value(high)?;
                        return Some((low_val, high_val));
                    }
                }
                None
            }
            Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => self
                .extract_between_bounds(left, target_column)
                .or_else(|| self.extract_between_bounds(right, target_column)),
            _ => None,
        }
    }
}
