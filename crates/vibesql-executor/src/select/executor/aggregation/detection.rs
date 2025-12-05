//! Aggregate detection helpers for SelectExecutor

use super::super::builder::SelectExecutor;

impl SelectExecutor<'_> {
    /// Check if SELECT list contains aggregate functions
    pub(in crate::select::executor) fn has_aggregates(
        &self,
        select_list: &[vibesql_ast::SelectItem],
    ) -> bool {
        select_list.iter().any(|item| match item {
            vibesql_ast::SelectItem::Expression { expr, .. } => self.expression_has_aggregate(expr),
            _ => false,
        })
    }

    /// Check if an expression contains aggregate functions
    #[allow(clippy::only_used_in_recursion)]
    pub(in crate::select::executor) fn expression_has_aggregate(
        &self,
        expr: &vibesql_ast::Expression,
    ) -> bool {
        match expr {
            // New AggregateFunction variant
            vibesql_ast::Expression::AggregateFunction { .. } => true,
            // Old Function variant (backwards compatibility for aggregates)
            vibesql_ast::Expression::Function { name, args, .. } => {
                // Check if this is an aggregate function name
                if matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                    return true;
                }
                // Otherwise, check if any arguments contain aggregates
                args.iter().any(|arg| self.expression_has_aggregate(arg))
            }
            vibesql_ast::Expression::BinaryOp { left, right, .. } => {
                self.expression_has_aggregate(left) || self.expression_has_aggregate(right)
            }
            // Unary operations - check if inner expression contains aggregate
            vibesql_ast::Expression::UnaryOp { expr, .. } => self.expression_has_aggregate(expr),
            vibesql_ast::Expression::Cast { expr, .. } => self.expression_has_aggregate(expr),
            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                operand.as_ref().is_some_and(|e| self.expression_has_aggregate(e))
                    || when_clauses.iter().any(|when_clause| {
                        when_clause.conditions.iter().any(|c| self.expression_has_aggregate(c))
                            || self.expression_has_aggregate(&when_clause.result)
                    })
                    || else_result.as_ref().is_some_and(|e| self.expression_has_aggregate(e))
            }
            // BETWEEN: check all three sub-expressions
            vibesql_ast::Expression::Between { expr, low, high, .. } => {
                self.expression_has_aggregate(expr)
                    || self.expression_has_aggregate(low)
                    || self.expression_has_aggregate(high)
            }
            // IN list: check test expression and all values
            vibesql_ast::Expression::InList { expr, values, .. } => {
                self.expression_has_aggregate(expr)
                    || values.iter().any(|v| self.expression_has_aggregate(v))
            }
            // IN subquery: check test expression
            vibesql_ast::Expression::In { expr, .. } => self.expression_has_aggregate(expr),
            // LIKE: check both expression and pattern
            vibesql_ast::Expression::Like { expr, pattern, .. } => {
                self.expression_has_aggregate(expr) || self.expression_has_aggregate(pattern)
            }
            // IS NULL: check inner expression
            vibesql_ast::Expression::IsNull { expr, .. } => self.expression_has_aggregate(expr),
            // Position: check both substring and string
            vibesql_ast::Expression::Position { substring, string, .. } => {
                self.expression_has_aggregate(substring) || self.expression_has_aggregate(string)
            }
            // Trim: check removal char and string
            vibesql_ast::Expression::Trim { removal_char, string, .. } => {
                removal_char.as_ref().is_some_and(|e| self.expression_has_aggregate(e))
                    || self.expression_has_aggregate(string)
            }
            // Interval: check the value expression
            vibesql_ast::Expression::Interval { value, .. } => self.expression_has_aggregate(value),
            // Quantified comparison: check left-hand expression
            vibesql_ast::Expression::QuantifiedComparison { expr, .. } => {
                self.expression_has_aggregate(expr)
            }
            // Scalar subquery and EXISTS: subqueries can contain aggregates, but we don't check inside them
            // The aggregates inside subqueries are in their own scope
            vibesql_ast::Expression::ScalarSubquery(_) | vibesql_ast::Expression::Exists { .. } => {
                false
            }
            // Window functions already contain aggregate-like logic
            vibesql_ast::Expression::WindowFunction { .. } => false,
            // DuplicateKeyValue references a column from INSERT VALUES
            vibesql_ast::Expression::DuplicateKeyValue { .. } => false,
            // Literals, column refs, wildcards, current date/time, defaults, sequences, etc. don't contain aggregates
            _ => false,
        }
    }

    /// Check if statement is a simple COUNT(*) query that can use fast path
    ///
    /// Fast path conditions:
    /// - Single SELECT item: COUNT(*)
    /// - No WHERE clause
    /// - No GROUP BY clause
    /// - No HAVING clause
    /// - No DISTINCT
    /// - No JOIN (single table reference)
    /// - No set operations (UNION, INTERSECT, EXCEPT)
    /// - FROM clause contains single table (not subquery/CTE)
    pub(in crate::select::executor) fn is_simple_count_star(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Option<String> {
        // Must have exactly one select item
        if stmt.select_list.len() != 1 {
            return None;
        }

        // Check if it's COUNT(*)
        let is_count_star = match &stmt.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => {
                match expr {
                    vibesql_ast::Expression::AggregateFunction { name, distinct, args } => {
                        // Must be COUNT, not DISTINCT, with single wildcard argument
                        if name.to_uppercase() != "COUNT" || *distinct || args.len() != 1 {
                            return None;
                        }
                        matches!(args[0], vibesql_ast::Expression::Wildcard)
                            || matches!(
                                &args[0],
                                vibesql_ast::Expression::ColumnRef { table: None, column } if column == "*"
                            )
                    }
                    vibesql_ast::Expression::Function { name, args, .. } => {
                        // Old Function variant (backwards compatibility)
                        if name.to_uppercase() != "COUNT" || args.len() != 1 {
                            return None;
                        }
                        matches!(args[0], vibesql_ast::Expression::Wildcard)
                            || matches!(
                                &args[0],
                                vibesql_ast::Expression::ColumnRef { table: None, column } if column == "*"
                            )
                    }
                    _ => false,
                }
            }
            _ => false,
        };

        if !is_count_star {
            return None;
        }

        // Must not have WHERE, GROUP BY, HAVING, DISTINCT, or set operations
        if stmt.where_clause.is_some()
            || stmt.group_by.is_some()
            || stmt.having.is_some()
            || stmt.distinct
            || stmt.set_operation.is_some()
        {
            return None;
        }

        // Must have a FROM clause with a single table
        let table_name = match &stmt.from {
            Some(vibesql_ast::FromClause::Table { name, .. }) => name.clone(),
            Some(vibesql_ast::FromClause::Join { .. }) => return None, // JOIN not allowed
            Some(vibesql_ast::FromClause::Subquery { .. }) => return None, // Subquery not allowed
            None => return None,                                       // No FROM clause
        };

        Some(table_name)
    }
}
