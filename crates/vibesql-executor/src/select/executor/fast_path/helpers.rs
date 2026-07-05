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

use crate::{
    errors::ExecutorError, schema::CombinedSchema, select::executor::builder::SelectExecutor,
};

/// Result of extracting equality predicate values from WHERE clause
///
/// Distinguishes between:
/// - `Values(map)`: Successfully extracted equality values
/// - `Contradiction`: Multiple equality predicates on same column with different values (e.g., col
///   = 1 AND col = 2 is always false)
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
        use crate::{
            evaluator::CombinedExpressionEvaluator, select::projection::project_row_combined,
        };

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
            Expression::ColumnRef(col_id) => {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                if schema.get_column_index(table, column).is_none() {
                    // SQLite compatibility: Allow ROWID pseudo-column references
                    // Only if there's no actual column with that name (real columns take precedence)
                    // WITHOUT ROWID tables do NOT have the rowid pseudo-column (Issue #4953)
                    let lower = column.to_lowercase();
                    let is_rowid_alias = lower == "rowid" || lower == "_rowid_" || lower == "oid";
                    if is_rowid_alias {
                        // Verify the qualifier matches a table in the schema (if qualified)
                        if let Some(qualifier) = table {
                            let qualifier_lower = qualifier.to_lowercase();
                            // Check if the qualified table is a WITHOUT ROWID table
                            for (table_id, (_, table_schema)) in &schema.table_schemas {
                                if table_id.canonical() == qualifier_lower
                                    && table_schema.without_rowid
                                {
                                    // WITHOUT ROWID tables do not have rowid pseudo-column
                                    return Err(ExecutorError::ColumnNotFound {
                                        column_name: column.to_string(),
                                        table_name: qualifier.to_string(),
                                        searched_tables: vec![qualifier.to_string()],
                                        available_columns: table_schema
                                            .columns
                                            .iter()
                                            .map(|c| c.name.clone())
                                            .collect(),
                                    });
                                }
                            }
                            let table_exists = schema
                                .table_schemas
                                .keys()
                                .any(|k| k.canonical() == qualifier_lower);
                            if table_exists {
                                return Ok(());
                            }
                        } else {
                            // Unqualified ROWID is valid if there's at least one table in scope
                            // AND none of the tables are WITHOUT ROWID
                            if !schema.table_schemas.is_empty() {
                                // Check if any table in scope is WITHOUT ROWID
                                for (table_id, (_, table_schema)) in &schema.table_schemas {
                                    if table_schema.without_rowid {
                                        return Err(ExecutorError::ColumnNotFound {
                                            column_name: column.to_string(),
                                            table_name: table_id.display().to_string(),
                                            searched_tables: schema.table_names(),
                                            available_columns: table_schema
                                                .columns
                                                .iter()
                                                .map(|c| c.name.clone())
                                                .collect(),
                                        });
                                    }
                                }
                                return Ok(());
                            }
                        }
                    }

                    // Collect available column names for the error message
                    let available_columns: Vec<String> = schema
                        .table_schemas
                        .values()
                        .flat_map(|(_, s)| s.columns.iter().map(|c| c.name.clone()))
                        .collect();
                    return Err(ExecutorError::ColumnNotFound {
                        column_name: column.to_string(),
                        table_name: table
                            .map(|t| t.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
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
        use std::cmp::Ordering;

        use crate::select::grouping::compare_sql_values;

        // Sort key can be either a column index or rowid pseudo-column
        // None = sort by rowid, Some(idx) = sort by column at index
        #[derive(Clone)]
        enum SortKey {
            Column(usize),
            Rowid(Option<String>), // table qualifier for JOINs
        }

        // Pre-compute sort keys for ORDER BY columns
        // (sort_key, direction, nulls_first)
        let mut sort_keys: Vec<(SortKey, OrderDirection, bool)> =
            Vec::with_capacity(order_by.len());

        for item in order_by {
            let sort_key = match &item.expr {
                Expression::ColumnRef(col_id) => {
                    let table = col_id.table_canonical();
                    let column = col_id.column_canonical();

                    // First try to get column index from schema
                    if let Some(col_idx) = schema.get_column_index(table, column) {
                        SortKey::Column(col_idx)
                    } else {
                        // Check if this is a rowid pseudo-column
                        let column_lower = column.to_lowercase();
                        let is_rowid = column_lower == "rowid"
                            || column_lower == "_rowid_"
                            || column_lower == "oid";

                        if is_rowid {
                            // Issue #4538: For tables with INTEGER PRIMARY KEY, use the IPK column
                            // instead of row metadata. The IPK column IS the rowid.
                            // Check if any table in the schema has a rowid_alias_column
                            let ipk_col_idx = if let Some(table_qualifier) = table {
                                // Qualified rowid (e.g., t1.rowid)
                                let table_id =
                                    vibesql_catalog::TableIdentifier::unquoted(table_qualifier);
                                schema.table_schemas.get(&table_id).and_then(
                                    |(start_idx, table_schema)| {
                                        table_schema
                                            .rowid_alias_column
                                            .map(|col_idx| start_idx + col_idx)
                                    },
                                )
                            } else {
                                // Unqualified rowid - find first table with IPK
                                schema.table_schemas.values().find_map(
                                    |(start_idx, table_schema)| {
                                        table_schema
                                            .rowid_alias_column
                                            .map(|col_idx| start_idx + col_idx)
                                    },
                                )
                            };

                            // If we found an IPK column, use it for sorting
                            if let Some(col_idx) = ipk_col_idx {
                                sort_keys.push((
                                    SortKey::Column(col_idx),
                                    item.direction.clone(),
                                    item.nulls_order.is_some_and(|no| {
                                        matches!(no, vibesql_ast::NullsOrder::First)
                                    }),
                                ));
                                continue; // Skip the rest of this loop iteration
                            }

                            // No IPK found, fall back to row metadata rowid
                            // Verify table qualifier if present
                            if let Some(qualifier) = table {
                                let qualifier_lower = qualifier.to_lowercase();
                                let table_exists = schema
                                    .table_schemas
                                    .keys()
                                    .any(|k| k.canonical() == qualifier_lower);
                                if !table_exists {
                                    return Err(ExecutorError::ColumnNotFound {
                                        column_name: column.to_string(),
                                        table_name: qualifier.to_string(),
                                        searched_tables: schema.table_names(),
                                        available_columns: vec![],
                                    });
                                }
                                SortKey::Rowid(Some(qualifier.to_string()))
                            } else {
                                // Unqualified rowid - valid if there's at least one table
                                if schema.table_schemas.is_empty() {
                                    return Err(ExecutorError::ColumnNotFound {
                                        column_name: column.to_string(),
                                        table_name: String::new(),
                                        searched_tables: schema.table_names(),
                                        available_columns: vec![],
                                    });
                                }
                                SortKey::Rowid(None)
                            }
                        } else {
                            return Err(ExecutorError::ColumnNotFound {
                                column_name: column.to_string(),
                                table_name: table.map(|t| t.to_string()).unwrap_or_default(),
                                searched_tables: schema.table_names(),
                                available_columns: vec![],
                            });
                        }
                    }
                }
                _ => {
                    return Err(ExecutorError::Other(
                        "Fast path ORDER BY requires simple column references".to_string(),
                    ));
                }
            };
            // Determine NULL ordering:
            // - If explicitly specified via NULLS FIRST/LAST, use that
            // - Default: SQLite treats NULL as smallest value, so:
            //   - ASC: NULL comes first (smallest first)
            //   - DESC: NULL comes last (smallest last)
            let nulls_first = match item.nulls_order {
                Some(vibesql_ast::NullsOrder::First) => true,
                Some(vibesql_ast::NullsOrder::Last) => false,
                None => matches!(item.direction, vibesql_ast::OrderDirection::Asc),
            };
            sort_keys.push((sort_key, item.direction.clone(), nulls_first));
        }

        // Sort rows by the specified columns/rowid
        rows.sort_by(|a, b| {
            for (sort_key, dir, nulls_first) in &sort_keys {
                let (val_a, val_b) = match sort_key {
                    SortKey::Column(col_idx) => (&a.values[*col_idx], &b.values[*col_idx]),
                    SortKey::Rowid(table_qualifier) => {
                        // Get rowid from row metadata
                        let rowid_a = a.get_row_id_for_table(table_qualifier.as_deref());
                        let rowid_b = b.get_row_id_for_table(table_qualifier.as_deref());

                        // Compare rowid values with proper NULL handling
                        let cmp = match (rowid_a, rowid_b) {
                            (None, None) => Ordering::Equal,
                            (None, Some(_)) => {
                                if *nulls_first {
                                    Ordering::Less
                                } else {
                                    Ordering::Greater
                                }
                            }
                            (Some(_), None) => {
                                if *nulls_first {
                                    Ordering::Greater
                                } else {
                                    Ordering::Less
                                }
                            }
                            (Some(id_a), Some(id_b)) => {
                                // Rowids are SIGNED (issue #5835): row_id is
                                // the two's-complement bit pattern of an i64,
                                // so compare in signed space — rowid -1
                                // (u64::MAX) sorts before 0. Identical to
                                // u64 order for non-negative rowids.
                                let raw_cmp = (id_a as i64).cmp(&(id_b as i64));
                                match dir {
                                    OrderDirection::Asc => raw_cmp,
                                    OrderDirection::Desc => raw_cmp.reverse(),
                                }
                            }
                        };
                        if cmp != Ordering::Equal {
                            return cmp;
                        }
                        continue; // Move to next sort key
                    }
                };

                // Handle NULLs according to nulls_first setting
                let cmp = match (val_a.is_null(), val_b.is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => {
                        if *nulls_first {
                            return Ordering::Less; // NULL sorts before non-NULL
                        } else {
                            return Ordering::Greater; // NULL sorts after non-NULL
                        }
                    }
                    (false, true) => {
                        if *nulls_first {
                            return Ordering::Greater; // non-NULL sorts after NULL
                        } else {
                            return Ordering::Less; // non-NULL sorts before NULL
                        }
                    }
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
        if let Expression::ColumnRef(col_id) = left {
            if let Some(value) = self.literal_to_value(right) {
                return Some((col_id.column_canonical().to_string(), value));
            }
        }
        // Try left = literal, right = column
        if let Expression::ColumnRef(col_id) = right {
            if let Some(value) = self.literal_to_value(left) {
                return Some((col_id.column_canonical().to_string(), value));
            }
        }
        None
    }

    /// Extract SqlValue from a literal expression
    ///
    /// Returns None for NULL literals because `col = NULL` can never match any row
    /// in SQL semantics (use `IS NULL` instead). NULL = NULL is NULL, not TRUE.
    pub(crate) fn literal_to_value(&self, expr: &Expression) -> Option<SqlValue> {
        match expr {
            // Exclude NULL - col = NULL can never match any row
            Expression::Literal(SqlValue::Null) => None,
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
                if let Expression::ColumnRef(col_id) = col_expr.as_ref() {
                    if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
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
                SelectItem::Expression { expr: Expression::ColumnRef(col_id), .. } => {
                    columns.push(col_id.column_canonical().to_string());
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
                SelectItem::Expression { expr: Expression::ColumnRef(col_id), .. } => {
                    // Find column index by name (case-insensitive)
                    let idx = table_schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(col_id.column_canonical()))?;
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
    pub(crate) fn project_by_indices_fast(
        &self,
        rows: Vec<Row>,
        col_indices: &[usize],
    ) -> Vec<Row> {
        rows.into_iter()
            .map(|row| {
                let projected_values: Vec<SqlValue> =
                    col_indices.iter().map(|&idx| row.values[idx].clone()).collect();
                // Issue #4954: Preserve row_id when projecting for rowid support
                let mut result = Row::from_vec(projected_values);
                if let Some(ref row_ids) = row.row_ids {
                    result.row_ids = Some(row_ids.clone());
                } else if let Some(row_id) = row.row_id {
                    result.row_id = Some(row_id);
                }
                result
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
                if let Expression::ColumnRef(col_id) = col_expr.as_ref() {
                    if col_id.column_canonical().eq_ignore_ascii_case(target_column) {
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

#[cfg(test)]
mod tests {
    use vibesql_ast::Expression;
    use vibesql_storage::Database;
    use vibesql_types::SqlValue;

    use crate::select::executor::builder::SelectExecutor;

    /// Regression test for issue where `col = NULL` incorrectly matched rows
    /// with NULL values when a secondary index existed on the column.
    ///
    /// In SQL, `col = NULL` should NEVER match any row (use `IS NULL` instead),
    /// because NULL = NULL evaluates to NULL (unknown), not TRUE.
    ///
    /// The bug was that `literal_to_value` extracted NULL as a valid value,
    /// causing the secondary index lookup to find rows with NULL values.
    #[test]
    fn test_literal_to_value_excludes_null() {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);

        // NULL literal should return None (cannot be used for index lookup)
        let null_literal = Expression::Literal(SqlValue::Null);
        assert!(
            executor.literal_to_value(&null_literal).is_none(),
            "literal_to_value should return None for NULL literals"
        );

        // Integer literal should return Some
        let int_literal = Expression::Literal(SqlValue::Integer(42));
        assert_eq!(executor.literal_to_value(&int_literal), Some(SqlValue::Integer(42)));

        // String literal should return Some
        let str_literal = Expression::Literal(SqlValue::Varchar("test".into()));
        assert_eq!(executor.literal_to_value(&str_literal), Some(SqlValue::Varchar("test".into())));

        // Non-literal expression should return None
        let column_ref = Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("col", false));
        assert!(executor.literal_to_value(&column_ref).is_none());
    }
}
