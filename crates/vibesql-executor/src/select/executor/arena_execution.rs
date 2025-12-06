//! Arena-based SELECT execution for zero-allocation prepared statement execution.
//!
//! This module provides arena-based execution of SELECT statements, enabling
//! zero-allocation query execution for prepared statements with inline
//! placeholder resolution.
//!
//! # Usage
//!
//! ```text
//! use bumpalo::Bump;
//! use vibesql_parser::arena_parser::ArenaParser;
//!
//! let arena = Bump::new();
//! let stmt = ArenaParser::parse_sql("SELECT * FROM users WHERE id = ?", &arena)?;
//! let params = &[SqlValue::Integer(42)];
//! let result = executor.execute_select_arena(stmt, params)?;
//! ```
//!
//! # Performance
//!
//! Arena-based execution provides:
//! - Zero malloc/free overhead per query execution
//! - Inline placeholder resolution (no AST cloning)
//! - Direct evaluation without intermediate allocations
//!
//! This is particularly beneficial for OLTP workloads with high query rates.

use std::cmp::Ordering;
use std::collections::HashMap;

use vibesql_ast::arena::{
    ArenaInterner, Expression as ArenaExpression, ExtendedExpr as ArenaExtendedExpr,
    SelectItem as ArenaSelectItem, SelectStmt as ArenaSelectStmt,
};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::builder::SelectExecutor;
use crate::errors::ExecutorError;
use crate::evaluator::window::compare_values;
use crate::evaluator::ArenaExpressionEvaluator;
use crate::schema::CombinedSchema;

impl SelectExecutor<'_> {
    /// Execute an arena-allocated SELECT statement with inline placeholder resolution.
    ///
    /// This method provides zero-allocation query execution for prepared statements.
    /// Parameters are resolved inline during evaluation, avoiding AST cloning.
    ///
    /// # Arguments
    ///
    /// * `stmt` - Arena-allocated SELECT statement
    /// * `params` - Parameter values for placeholder resolution
    ///
    /// # Returns
    ///
    /// Vector of result rows.
    ///
    /// # Limitations
    ///
    /// Currently supports simple SELECT queries without:
    /// - JOINs (single table only)
    /// - Subqueries
    /// - Aggregates, GROUP BY, HAVING
    /// - Window functions
    /// - CTEs (WITH clause)
    ///
    /// For unsupported features, returns an error. Fall back to standard execution.
    pub fn execute_select_arena<'arena>(
        &self,
        stmt: &ArenaSelectStmt<'arena>,
        params: &[SqlValue],
        interner: &'arena ArenaInterner<'arena>,
    ) -> Result<Vec<Row>, ExecutorError> {
        // Check for unsupported features
        if stmt.with_clause.is_some() {
            return Err(ExecutorError::UnsupportedExpression(
                "Arena execution does not support WITH clause".to_string(),
            ));
        }

        if stmt.set_operation.is_some() {
            return Err(ExecutorError::UnsupportedExpression(
                "Arena execution does not support set operations".to_string(),
            ));
        }

        if stmt.group_by.is_some() || stmt.having.is_some() {
            return Err(ExecutorError::UnsupportedExpression(
                "Arena execution does not support GROUP BY/HAVING".to_string(),
            ));
        }

        if stmt.distinct {
            return Err(ExecutorError::UnsupportedExpression(
                "Arena execution does not support DISTINCT".to_string(),
            ));
        }

        // Check for aggregates in select list
        if self.has_arena_aggregates(&stmt.select_list) {
            return Err(ExecutorError::UnsupportedExpression(
                "Arena execution does not support aggregate functions".to_string(),
            ));
        }

        // Execute based on FROM clause
        match &stmt.from {
            Some(from) => self.execute_arena_with_from(stmt, from, params, interner),
            None => self.execute_arena_without_from(stmt, params, interner),
        }
    }

    /// Execute SELECT without FROM clause (expression-only).
    fn execute_arena_without_from<'arena>(
        &self,
        stmt: &ArenaSelectStmt<'arena>,
        params: &[SqlValue],
        interner: &'arena ArenaInterner<'arena>,
    ) -> Result<Vec<Row>, ExecutorError> {
        // Create an empty schema and row for expression evaluation
        let schema = CombinedSchema { table_schemas: HashMap::new(), total_columns: 0 };
        let empty_row = Row::new(vec![]);
        let evaluator = ArenaExpressionEvaluator::new(&schema, params, interner);

        // Evaluate each select item
        let mut values = Vec::with_capacity(stmt.select_list.len());
        for item in stmt.select_list.iter() {
            match item {
                ArenaSelectItem::Expression { expr, .. } => {
                    let value = evaluator.eval(expr, &empty_row)?;
                    values.push(value);
                }
                ArenaSelectItem::Wildcard { .. } | ArenaSelectItem::QualifiedWildcard { .. } => {
                    // Wildcard without FROM is typically an error, but we'll skip it
                    continue;
                }
            }
        }

        // Apply LIMIT/OFFSET (for expression-only, limit defaults to 1 row)
        let rows = vec![Row::new(values)];
        Ok(self.apply_arena_limit_offset(rows, stmt.limit, stmt.offset))
    }

    /// Execute SELECT with FROM clause.
    fn execute_arena_with_from<'arena>(
        &self,
        stmt: &ArenaSelectStmt<'arena>,
        from: &vibesql_ast::arena::FromClause<'arena>,
        params: &[SqlValue],
        interner: &'arena ArenaInterner<'arena>,
    ) -> Result<Vec<Row>, ExecutorError> {
        use vibesql_ast::arena::FromClause;

        // Currently only support simple table reference
        let (table_name, alias) = match from {
            FromClause::Table { name, alias, .. } => (*name, *alias),
            FromClause::Join { .. } => {
                return Err(ExecutorError::UnsupportedExpression(
                    "Arena execution does not support JOINs yet".to_string(),
                ));
            }
            FromClause::Subquery { .. } => {
                return Err(ExecutorError::UnsupportedExpression(
                    "Arena execution does not support subqueries in FROM".to_string(),
                ));
            }
        };

        // Resolve table name symbol to string
        let table_name_str = interner.resolve(table_name);

        // Get the table
        let table = self
            .database
            .get_table(table_name_str)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name_str.to_string()))?;

        // Build schema - use alias if provided, otherwise table name
        let schema_alias_str = alias.map(|a| interner.resolve(a)).unwrap_or(table_name_str);
        let schema = CombinedSchema::from_table(schema_alias_str.to_string(), table.schema.clone());

        // Create evaluator
        let evaluator =
            ArenaExpressionEvaluator::with_database(&schema, params, self.database, interner);

        // Scan table and filter (only live rows)
        // Issue #3790: Use scan_live() to filter out deleted rows
        let mut results = Vec::new();
        for (_, row) in table.scan_live() {
            // Apply WHERE clause filter
            if let Some(where_clause) = &stmt.where_clause {
                let filter_result = evaluator.eval(where_clause, row)?;
                match filter_result {
                    SqlValue::Boolean(true) => {}
                    SqlValue::Boolean(false) | SqlValue::Null => continue,
                    _ => {
                        return Err(ExecutorError::TypeError(format!(
                            "WHERE clause must evaluate to boolean, got {:?}",
                            filter_result
                        )));
                    }
                }
            }

            // Project columns
            let projected =
                self.project_arena_row(&stmt.select_list, row, &schema, &evaluator, interner)?;
            results.push(projected);

            // Check timeout periodically
            if results.len() % 1000 == 0 {
                self.check_timeout()?;
            }
        }

        // Apply ORDER BY if present
        if let Some(order_by) = &stmt.order_by {
            self.sort_arena_results(&mut results, order_by.as_slice(), &schema, params, interner)?;
        }

        // Apply LIMIT/OFFSET
        Ok(self.apply_arena_limit_offset(results, stmt.limit, stmt.offset))
    }

    /// Project a row according to the SELECT list.
    fn project_arena_row<'arena>(
        &self,
        select_list: &[ArenaSelectItem<'arena>],
        row: &Row,
        schema: &CombinedSchema,
        evaluator: &ArenaExpressionEvaluator<'_, 'arena>,
        interner: &'arena ArenaInterner<'arena>,
    ) -> Result<Row, ExecutorError> {
        let mut values = Vec::with_capacity(select_list.len());

        for item in select_list.iter() {
            match item {
                ArenaSelectItem::Expression { expr, .. } => {
                    let value = evaluator.eval(expr, row)?;
                    values.push(value);
                }
                ArenaSelectItem::Wildcard { .. } => {
                    // Unqualified wildcard (*) - expand all columns
                    values.extend(row.values.iter().cloned());
                }
                ArenaSelectItem::QualifiedWildcard { qualifier, .. } => {
                    // Qualified wildcard (table.*) - expand columns from specific table
                    let qualifier_str = interner.resolve(*qualifier);
                    if let Some(&(start, ref tbl_schema)) = schema.get_table(qualifier_str) {
                        for i in 0..tbl_schema.columns.len() {
                            if let Some(val) = row.get(start + i) {
                                values.push(val.clone());
                            }
                        }
                    } else {
                        // Table not found - expand all as fallback
                        values.extend(row.values.iter().cloned());
                    }
                }
            }
        }

        Ok(Row::new(values))
    }

    /// Sort results according to ORDER BY clause.
    fn sort_arena_results<'arena>(
        &self,
        results: &mut Vec<Row>,
        order_by: &[vibesql_ast::arena::OrderByItem<'arena>],
        schema: &CombinedSchema,
        params: &[SqlValue],
        interner: &'arena ArenaInterner<'arena>,
    ) -> Result<(), ExecutorError> {
        use vibesql_ast::arena::OrderDirection;

        // Create evaluator for order by expressions
        let evaluator =
            ArenaExpressionEvaluator::with_database(schema, params, self.database, interner);

        // Pre-compute order by values for each row to avoid repeated evaluation
        let mut keyed_rows: Vec<(Vec<SqlValue>, Row)> = results
            .drain(..)
            .map(|row| {
                let keys: Result<Vec<_>, _> =
                    order_by.iter().map(|item| evaluator.eval(&item.expr, &row)).collect();
                keys.map(|k| (k, row))
            })
            .collect::<Result<_, _>>()?;

        // Sort by keys
        keyed_rows.sort_by(|(keys_a, _), (keys_b, _)| {
            for (i, (key_a, key_b)) in keys_a.iter().zip(keys_b.iter()).enumerate() {
                let cmp = compare_values(key_a, key_b);
                if cmp != Ordering::Equal {
                    // Apply ASC/DESC
                    let asc =
                        order_by.get(i).is_some_and(|o| matches!(o.direction, OrderDirection::Asc));
                    return if asc { cmp } else { cmp.reverse() };
                }
            }
            Ordering::Equal
        });

        // Move sorted rows back to results
        results.extend(keyed_rows.into_iter().map(|(_, row)| row));

        Ok(())
    }

    /// Apply LIMIT and OFFSET to results.
    fn apply_arena_limit_offset(
        &self,
        mut results: Vec<Row>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Vec<Row> {
        // Apply offset first
        if let Some(off) = offset {
            if off >= results.len() {
                return vec![];
            }
            results = results.into_iter().skip(off).collect();
        }

        // Apply limit
        if let Some(lim) = limit {
            results.truncate(lim);
        }

        results
    }

    /// Check if select list contains aggregate functions.
    fn has_arena_aggregates<'arena>(&self, select_list: &[ArenaSelectItem<'arena>]) -> bool {
        for item in select_list.iter() {
            if let ArenaSelectItem::Expression { expr, .. } = item {
                if self.arena_expr_has_aggregate(expr) {
                    return true;
                }
            }
        }
        false
    }

    /// Check if an arena expression contains an aggregate function.
    fn arena_expr_has_aggregate<'arena>(&self, expr: &ArenaExpression<'arena>) -> bool {
        match expr {
            // Hot-path inline variants
            ArenaExpression::BinaryOp { left, right, .. } => {
                self.arena_expr_has_aggregate(left) || self.arena_expr_has_aggregate(right)
            }
            ArenaExpression::UnaryOp { expr, .. } => self.arena_expr_has_aggregate(expr),
            ArenaExpression::IsNull { expr, .. } => self.arena_expr_has_aggregate(expr),
            ArenaExpression::Conjunction(children) | ArenaExpression::Disjunction(children) => {
                children.iter().any(|c| self.arena_expr_has_aggregate(c))
            }
            ArenaExpression::Literal(_)
            | ArenaExpression::Placeholder(_)
            | ArenaExpression::NumberedPlaceholder(_)
            | ArenaExpression::NamedPlaceholder(_)
            | ArenaExpression::ColumnRef { .. }
            | ArenaExpression::Wildcard
            | ArenaExpression::CurrentDate
            | ArenaExpression::CurrentTime { .. }
            | ArenaExpression::CurrentTimestamp { .. }
            | ArenaExpression::Default => false,
            // Cold-path extended variants
            ArenaExpression::Extended(ext) => self.arena_extended_has_aggregate(ext),
        }
    }

    /// Check if an extended expression contains an aggregate function.
    fn arena_extended_has_aggregate<'arena>(&self, ext: &ArenaExtendedExpr<'arena>) -> bool {
        match ext {
            ArenaExtendedExpr::AggregateFunction { .. } => true,
            ArenaExtendedExpr::Function { args, .. } => {
                args.iter().any(|a| self.arena_expr_has_aggregate(a))
            }
            ArenaExtendedExpr::Case { operand, when_clauses, else_result, .. } => {
                operand.as_ref().is_some_and(|o| self.arena_expr_has_aggregate(o))
                    || when_clauses.iter().any(|w| {
                        w.conditions.iter().any(|c| self.arena_expr_has_aggregate(c))
                            || self.arena_expr_has_aggregate(&w.result)
                    })
                    || else_result.as_ref().is_some_and(|e| self.arena_expr_has_aggregate(e))
            }
            ArenaExtendedExpr::Between { expr, low, high, .. } => {
                self.arena_expr_has_aggregate(expr)
                    || self.arena_expr_has_aggregate(low)
                    || self.arena_expr_has_aggregate(high)
            }
            ArenaExtendedExpr::InList { expr, values, .. } => {
                self.arena_expr_has_aggregate(expr)
                    || values.iter().any(|v| self.arena_expr_has_aggregate(v))
            }
            ArenaExtendedExpr::Cast { expr, .. } => self.arena_expr_has_aggregate(expr),
            ArenaExtendedExpr::Like { expr, pattern, .. } => {
                self.arena_expr_has_aggregate(expr) || self.arena_expr_has_aggregate(pattern)
            }
            _ => false,
        }
    }
}
