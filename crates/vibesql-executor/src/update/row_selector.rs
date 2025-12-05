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
    /// Returns the primary key value if the expression is a simple equality on the primary key,
    /// otherwise returns None.
    #[allow(clippy::single_match)]
    fn extract_primary_key_lookup(
        where_expr: &Expression,
        schema: &vibesql_catalog::TableSchema,
    ) -> Option<Vec<vibesql_types::SqlValue>> {
        // Only handle simple binary equality operations
        match where_expr {
            Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
                // Check if left side is a column reference and right side is a literal
                if let (Expression::ColumnRef { column, .. }, Expression::Literal(value)) =
                    (left.as_ref(), right.as_ref())
                {
                    // Check if this column is the primary key
                    if let Some(pk_indices) = schema.get_primary_key_indices() {
                        if let Some(col_index) = schema.get_column_index(column) {
                            // Only handle single-column primary keys for now
                            if pk_indices.len() == 1 && pk_indices[0] == col_index {
                                return Some(vec![value.clone()]);
                            }
                        }
                    }
                }

                // Also check the reverse: literal = column
                if let (Expression::Literal(value), Expression::ColumnRef { column, .. }) =
                    (left.as_ref(), right.as_ref())
                {
                    if let Some(pk_indices) = schema.get_primary_key_indices() {
                        if let Some(col_index) = schema.get_column_index(column) {
                            if pk_indices.len() == 1 && pk_indices[0] == col_index {
                                return Some(vec![value.clone()]);
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        None
    }

    /// Collect candidate rows that match the WHERE clause (fallback for non-indexed queries)
    fn collect_candidate_rows(
        table: &vibesql_storage::Table,
        where_clause: &Option<vibesql_ast::WhereClause>,
        evaluator: &ExpressionEvaluator,
    ) -> Result<Vec<(usize, vibesql_storage::Row)>, ExecutorError> {
        let mut candidate_rows = Vec::new();

        for (row_index, row) in table.scan().iter().enumerate() {
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
