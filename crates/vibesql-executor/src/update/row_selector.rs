//! Row selection logic for UPDATE operations

use vibesql_ast::{BinaryOperator, Expression};

use crate::{errors::ExecutorError, evaluator::ExpressionEvaluator};

/// Row selector for UPDATE statements
///
/// Handles WHERE clause evaluation and primary key index optimization
pub struct RowSelector<'a> {
    schema: &'a vibesql_catalog::TableSchema,
    evaluator: &'a ExpressionEvaluator<'a>,
}

impl<'a> RowSelector<'a> {
    /// Create a new row selector
    pub fn new(
        schema: &'a vibesql_catalog::TableSchema,
        evaluator: &'a ExpressionEvaluator<'a>,
    ) -> Self {
        Self { schema, evaluator }
    }

    /// Select rows to update based on WHERE clause
    ///
    /// Uses primary key index optimization when possible, otherwise falls back to table scan.
    pub fn select_rows(
        &self,
        table: &vibesql_storage::Table,
        where_clause: &Option<vibesql_ast::WhereClause>,
    ) -> Result<Vec<(usize, vibesql_storage::Row)>, ExecutorError> {
        // Try to use primary key index for fast lookup
        if let Some(vibesql_ast::WhereClause::Condition(where_expr)) = where_clause {
            if let Some(pk_values) = Self::extract_primary_key_lookup(where_expr, self.schema) {
                // Use primary key index for O(1) lookup
                if let Some(pk_index) = table.primary_key_index() {
                    if let Some(&row_index) = pk_index.get(&pk_values) {
                        // Found the row via index - single row to update
                        return Ok(vec![(row_index, table.scan()[row_index].clone())]);
                    } else {
                        // Primary key not found - no rows to update
                        return Ok(vec![]);
                    }
                }
                // No primary key index available, fall back to table scan below
            }
        }

        // Fall back to table scan
        Self::collect_candidate_rows(table, where_clause, self.evaluator)
    }

    /// Analyze WHERE expression to see if it can use primary key index for fast lookup
    ///
    /// Returns the primary key values if the expression is an equality (or conjunction of equalities)
    /// on all primary key columns, otherwise returns None.
    ///
    /// Supports:
    /// - Single-column PK: `WHERE pk = value`
    /// - Composite PK: `WHERE pk1 = val1 AND pk2 = val2`
    fn extract_primary_key_lookup(
        where_expr: &Expression,
        schema: &vibesql_catalog::TableSchema,
    ) -> Option<Vec<vibesql_types::SqlValue>> {
        let pk_indices = schema.get_primary_key_indices()?;
        if pk_indices.is_empty() {
            return None;
        }

        // For single-column PK, use simple extraction
        if pk_indices.len() == 1 {
            return Self::extract_single_pk_equality(where_expr, schema, pk_indices[0]);
        }

        // For composite PK, extract all equalities from AND expressions
        Self::extract_composite_pk_equalities(where_expr, schema, &pk_indices)
    }

    /// Extract single PK column equality from expression
    fn extract_single_pk_equality(
        where_expr: &Expression,
        schema: &vibesql_catalog::TableSchema,
        pk_col_index: usize,
    ) -> Option<Vec<vibesql_types::SqlValue>> {
        if let Expression::BinaryOp { left, op: BinaryOperator::Equal, right } = where_expr {
            // Check: column = literal
            if let (Expression::ColumnRef { column, .. }, Expression::Literal(value)) =
                (left.as_ref(), right.as_ref())
            {
                if let Some(col_index) = schema.get_column_index(column) {
                    if col_index == pk_col_index {
                        return Some(vec![value.clone()]);
                    }
                }
            }

            // Check: literal = column
            if let (Expression::Literal(value), Expression::ColumnRef { column, .. }) =
                (left.as_ref(), right.as_ref())
            {
                if let Some(col_index) = schema.get_column_index(column) {
                    if col_index == pk_col_index {
                        return Some(vec![value.clone()]);
                    }
                }
            }
        }
        None
    }

    /// Extract composite PK values from AND expressions
    ///
    /// For a composite PK (pk1, pk2), matches expressions like:
    /// - `pk1 = val1 AND pk2 = val2`
    /// - `pk2 = val2 AND pk1 = val1` (order doesn't matter)
    /// - Nested ANDs: `(pk1 = val1 AND pk2 = val2) AND pk3 = val3`
    fn extract_composite_pk_equalities(
        where_expr: &Expression,
        schema: &vibesql_catalog::TableSchema,
        pk_indices: &[usize],
    ) -> Option<Vec<vibesql_types::SqlValue>> {
        // Collect all equalities from the expression
        let mut equalities: std::collections::HashMap<usize, vibesql_types::SqlValue> =
            std::collections::HashMap::new();
        Self::collect_equalities(where_expr, schema, &mut equalities);

        // Check if we have all PK columns
        let mut pk_values = Vec::with_capacity(pk_indices.len());
        for &pk_col in pk_indices {
            match equalities.get(&pk_col) {
                Some(value) => pk_values.push(value.clone()),
                None => return None, // Missing PK column equality
            }
        }

        Some(pk_values)
    }

    /// Recursively collect column = literal equalities from expression
    fn collect_equalities(
        expr: &Expression,
        schema: &vibesql_catalog::TableSchema,
        equalities: &mut std::collections::HashMap<usize, vibesql_types::SqlValue>,
    ) {
        match expr {
            Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
                // Recurse into AND branches
                Self::collect_equalities(left, schema, equalities);
                Self::collect_equalities(right, schema, equalities);
            }
            Expression::Conjunction(exprs) => {
                // Handle flattened AND chains
                for e in exprs {
                    Self::collect_equalities(e, schema, equalities);
                }
            }
            Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
                // Check: column = literal
                if let (Expression::ColumnRef { column, .. }, Expression::Literal(value)) =
                    (left.as_ref(), right.as_ref())
                {
                    if let Some(col_index) = schema.get_column_index(column) {
                        equalities.insert(col_index, value.clone());
                    }
                }
                // Check: literal = column
                else if let (Expression::Literal(value), Expression::ColumnRef { column, .. }) =
                    (left.as_ref(), right.as_ref())
                {
                    if let Some(col_index) = schema.get_column_index(column) {
                        equalities.insert(col_index, value.clone());
                    }
                }
            }
            _ => {} // Ignore other expressions (OR, comparisons, etc.)
        }
    }

    /// Collect candidate rows that match the WHERE clause (fallback for non-indexed queries)
    fn collect_candidate_rows(
        table: &vibesql_storage::Table,
        where_clause: &Option<vibesql_ast::WhereClause>,
        evaluator: &ExpressionEvaluator,
    ) -> Result<Vec<(usize, vibesql_storage::Row)>, ExecutorError> {
        let mut candidate_rows = Vec::new();

        // Use scan_live() to skip deleted rows and get correct physical indices
        for (row_index, row) in table.scan_live() {
            // Clear CSE cache before evaluating each row to prevent column values
            // from being incorrectly cached across different rows
            evaluator.clear_cse_cache();

            // Check WHERE clause
            let should_update = if let Some(ref where_clause) = where_clause {
                match where_clause {
                    vibesql_ast::WhereClause::Condition(where_expr) => {
                        let result = evaluator.eval(where_expr, row)?;
                        // SQL semantics: only TRUE (not NULL) causes update
                        matches!(result, vibesql_types::SqlValue::Boolean(true))
                    }
                    vibesql_ast::WhereClause::CurrentOf(cursor_name) => {
                        // TODO: Implement cursor support for positioned UPDATE/DELETE
                        //
                        // Requirements for implementation:
                        // 1. Cursor registry/manager in Database to track declared cursors
                        // 2. Cursor state tracking (current position, result set, etc.)
                        // 3. Executor support for DECLARE/OPEN/FETCH/CLOSE statements
                        // 4. Integration with UPDATE/DELETE to access cursor position
                        //
                        // Note: Parser and AST support for cursors already exists in:
                        // - crates/ast/src/ddl/cursor.rs (DeclareCursorStmt, etc.)
                        // - crates/parser/src/parser/cursor.rs (parsing logic)
                        //
                        // See SQL:1999 Feature E121 for cursor specification.
                        return Err(ExecutorError::UnsupportedFeature(format!(
                            "WHERE CURRENT OF {} - Positioned UPDATE/DELETE not yet implemented. \
                            Requires cursor infrastructure (DECLARE/OPEN/FETCH/CLOSE execution, \
                            cursor state management, and position tracking). \
                            Use a standard WHERE clause instead.",
                            cursor_name
                        )));
                    }
                }
            } else {
                true // No WHERE clause = update all rows
            };

            if should_update {
                candidate_rows.push((row_index, row.clone()));
            }
        }

        Ok(candidate_rows)
    }
}
