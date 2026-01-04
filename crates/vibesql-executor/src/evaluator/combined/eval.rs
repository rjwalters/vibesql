//! Main evaluation entry point for combined expressions

use vibesql_types::{SqlValue, TypeAffinity};

use super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::{errors::ExecutorError, select::WindowFunctionKey};

impl CombinedExpressionEvaluator<'_> {
    /// Get the SQLite type affinity of an expression if it's a column reference.
    ///
    /// Returns Some(affinity) if the expression is a column reference and we can
    /// determine its declared type from the schema. Returns None for literals,
    /// function calls, and other non-column expressions.
    fn get_expression_affinity(&self, expr: &vibesql_ast::Expression) -> Option<TypeAffinity> {
        match expr {
            vibesql_ast::Expression::ColumnRef(col_id) => {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();

                // Look up the column in the combined schema to get its declared type
                for (_start_idx, table_schema) in self.schema.table_schemas.values() {
                    // If table qualifier is specified, check if this is the right table
                    if let Some(t) = table {
                        let table_name_lower = table_schema.name.to_lowercase();
                        if t.to_lowercase() != table_name_lower {
                            continue;
                        }
                    }

                    // Look up the column in this table
                    if let Some(col_offset) = table_schema.get_column_index(column) {
                        let col_schema = &table_schema.columns[col_offset];
                        return Some(col_schema.data_type.sqlite_affinity());
                    }
                }

                // Column not found - check if it's a rowid pseudo-column
                // ROWID, _rowid_, and oid have INTEGER affinity
                let column_lower = column.to_lowercase();
                if column_lower == "rowid" || column_lower == "_rowid_" || column_lower == "oid" {
                    return Some(TypeAffinity::Integer);
                }
                // Otherwise treat as NONE affinity
                Some(TypeAffinity::None)
            }
            // For COLLATE expressions, get affinity of the inner expression
            vibesql_ast::Expression::Collate { expr, .. } => self.get_expression_affinity(expr),
            // Literals, functions, and other expressions don't have column affinity
            _ => None,
        }
    }

    /// Get the effective collation for an expression.
    ///
    /// Returns the collation from:
    /// 1. Explicit COLLATE clause (highest priority)
    /// 2. Column-level collation from CREATE TABLE definition
    /// 3. None (use default binary collation)
    pub(crate) fn get_expression_collation(
        &self,
        expr: &vibesql_ast::Expression,
    ) -> Option<String> {
        match expr {
            // Explicit COLLATE has highest priority
            vibesql_ast::Expression::Collate { collation, .. } => Some(collation.clone()),
            // Column reference - look up column's declared collation
            vibesql_ast::Expression::ColumnRef(col_id) => {
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();

                // Look up the column in the combined schema
                for (_start_idx, table_schema) in self.schema.table_schemas.values() {
                    // If table qualifier is specified, check if this is the right table
                    if let Some(t) = table {
                        let table_name_lower = table_schema.name.to_lowercase();
                        if t.to_lowercase() != table_name_lower {
                            continue;
                        }
                    }

                    // Look up the column in this table
                    if let Some(col_offset) = table_schema.get_column_index(column) {
                        return table_schema.columns[col_offset].collation.clone();
                    }
                }
                None
            }
            // Other expressions don't have intrinsic collation
            _ => None,
        }
    }

    /// Check if an expression is a numeric literal (INTEGER or REAL).
    fn is_numeric_literal(&self, expr: &vibesql_ast::Expression) -> bool {
        match expr {
            vibesql_ast::Expression::Literal(val) => {
                matches!(
                    val,
                    SqlValue::Integer(_)
                        | SqlValue::Smallint(_)
                        | SqlValue::Bigint(_)
                        | SqlValue::Unsigned(_)
                        | SqlValue::Float(_)
                        | SqlValue::Real(_)
                        | SqlValue::Double(_)
                        | SqlValue::Numeric(_)
                )
            }
            _ => false,
        }
    }

    /// Check if an expression is a string literal (VARCHAR or CHAR).
    fn is_string_literal(&self, expr: &vibesql_ast::Expression) -> bool {
        match expr {
            vibesql_ast::Expression::Literal(val) => {
                matches!(val, SqlValue::Varchar(_) | SqlValue::Character(_))
            }
            _ => false,
        }
    }

    /// Try to convert a string SqlValue to a numeric SqlValue.
    /// Returns the original value if the string doesn't look like a number.
    /// This implements SQLite's NUMERIC affinity coercion rules.
    fn try_coerce_string_to_numeric(val: &SqlValue) -> SqlValue {
        match val {
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                let trimmed = s.trim();
                // Try parsing as integer first
                if let Ok(n) = trimmed.parse::<i64>() {
                    return SqlValue::Integer(n);
                }
                // Try parsing as float (use Double for higher precision)
                if let Ok(n) = trimmed.parse::<f64>() {
                    return SqlValue::Double(n);
                }
                // Not a number - return original value
                val.clone()
            }
            _ => val.clone(),
        }
    }

    /// Apply SQLite affinity rules for comparisons.
    ///
    /// When comparing a TEXT-affinity column to an INTEGER literal, SQLite:
    /// 1. Converts the INTEGER to TEXT
    /// 2. Performs string comparison
    ///
    /// This method is public for use in GROUP BY expression evaluation.
    pub fn apply_affinity_for_comparison(
        &self,
        left_expr: &vibesql_ast::Expression,
        left_val: SqlValue,
        right_expr: &vibesql_ast::Expression,
        right_val: SqlValue,
    ) -> (SqlValue, SqlValue) {
        let left_affinity = self.get_expression_affinity(left_expr);
        let right_affinity = self.get_expression_affinity(right_expr);

        // Case 1: Left is TEXT column, right is numeric literal
        if left_affinity == Some(TypeAffinity::Text) && self.is_numeric_literal(right_expr) {
            let right_as_text = match &right_val {
                SqlValue::Integer(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Smallint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Bigint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Unsigned(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Float(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Real(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Double(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Numeric(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                _ => right_val,
            };
            return (left_val, right_as_text);
        }

        // Case 2: Right is TEXT column, left is numeric literal
        if right_affinity == Some(TypeAffinity::Text) && self.is_numeric_literal(left_expr) {
            let left_as_text = match &left_val {
                SqlValue::Integer(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Smallint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Bigint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Unsigned(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Float(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Real(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Double(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Numeric(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                _ => left_val,
            };
            return (left_as_text, right_val);
        }

        // Case 3: Left is NUMERIC/INTEGER/REAL column, right is string literal
        // Try to convert the string literal to a number for numeric comparison
        // Per SQLite: NUMERIC affinity tries to convert strings to numbers if possible
        let is_left_numeric_affinity = matches!(
            left_affinity,
            Some(TypeAffinity::Numeric) | Some(TypeAffinity::Integer) | Some(TypeAffinity::Real)
        );
        if is_left_numeric_affinity && self.is_string_literal(right_expr) {
            let right_coerced = Self::try_coerce_string_to_numeric(&right_val);
            return (left_val, right_coerced);
        }

        // Case 4: Right is NUMERIC/INTEGER/REAL column, left is string literal
        // Try to convert the string literal to a number for numeric comparison
        let is_right_numeric_affinity = matches!(
            right_affinity,
            Some(TypeAffinity::Numeric) | Some(TypeAffinity::Integer) | Some(TypeAffinity::Real)
        );
        if is_right_numeric_affinity && self.is_string_literal(left_expr) {
            let left_coerced = Self::try_coerce_string_to_numeric(&left_val);
            return (left_coerced, right_val);
        }

        // Case 5: Left is NUMERIC/INTEGER/REAL column, right is NONE/TEXT column with TEXT value
        // Per SQLite: when one operand has NUMERIC affinity and the other has TEXT or NONE affinity,
        // try to convert the TEXT value to a number for comparison.
        // This handles column-to-column comparisons like: NUMERIC_col = NONE_col
        let is_right_non_numeric_affinity = matches!(
            right_affinity,
            Some(TypeAffinity::None) | Some(TypeAffinity::Text) | None
        );
        if is_left_numeric_affinity
            && is_right_non_numeric_affinity
            && matches!(right_val, SqlValue::Varchar(_) | SqlValue::Character(_))
        {
            let right_coerced = Self::try_coerce_string_to_numeric(&right_val);
            return (left_val, right_coerced);
        }

        // Case 6: Right is NUMERIC/INTEGER/REAL column, left is NONE/TEXT column with TEXT value
        // Symmetric case of Case 5
        let is_left_non_numeric_affinity = matches!(
            left_affinity,
            Some(TypeAffinity::None) | Some(TypeAffinity::Text) | None
        );
        if is_right_numeric_affinity
            && is_left_non_numeric_affinity
            && matches!(left_val, SqlValue::Varchar(_) | SqlValue::Character(_))
        {
            let left_coerced = Self::try_coerce_string_to_numeric(&left_val);
            return (left_coerced, right_val);
        }

        // No affinity conversion needed - use original values
        // This includes:
        // - Bare columns (NONE affinity) vs bare columns (NONE affinity) → type ordering
        // - TEXT column vs NONE column → type ordering (no numeric affinity involved)
        // - Literal vs literal → type ordering (handled in compare)
        // - Same-type comparisons → direct comparison
        (left_val, right_val)
    }

    /// Evaluate an expression in the context of a combined row
    /// This is the main entry point for expression evaluation
    pub(crate) fn eval(
        &self,
        expr: &vibesql_ast::Expression,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow from deeply nested expressions
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        // CSE: Check cache if enabled and expression is deterministic
        if self.enable_cse
            && super::super::expression_hash::ExpressionHasher::is_deterministic(expr)
        {
            let hash = super::super::expression_hash::ExpressionHasher::hash(expr);

            // Check cache (get requires mut borrow to update LRU order)
            if let Some(cached) = self.cse_cache.borrow_mut().get(&hash) {
                return Ok(cached.clone());
            }

            // Evaluate with depth increment and cache result
            let result = self.with_incremented_depth(|evaluator| evaluator.eval_impl(expr, row))?;
            self.cse_cache.borrow_mut().put(hash, result.clone());
            return Ok(result);
        }

        // Non-cached path: increment depth and evaluate
        self.with_incremented_depth(|evaluator| evaluator.eval_impl(expr, row))
    }

    /// Internal implementation of eval with depth already incremented
    fn eval_impl(
        &self,
        expr: &vibesql_ast::Expression,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        match expr {
            // Literals - just return the value
            vibesql_ast::Expression::Literal(val) => Ok(val.clone()),

            // DEFAULT keyword - not allowed in UPDATE/SELECT expressions
            // DEFAULT is only valid in INSERT VALUES and UPDATE SET
            // This evaluator is used for SELECT and WHERE clauses where DEFAULT is invalid
            vibesql_ast::Expression::Default => Err(ExecutorError::UnsupportedExpression(
                "DEFAULT keyword is only valid in INSERT VALUES and UPDATE SET clauses".to_string(),
            )),

            // Column reference - look up column index (with optional table qualifier)
            vibesql_ast::Expression::ColumnRef(col_id) => {
                let schema = col_id.schema_canonical();
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                // Display forms preserve original case for error messages (SQLite compatibility)
                let table_display = col_id.table_display();
                let column_display = col_id.column_display();

                // Handle schema qualifier (three-part names like schema.table.column)
                // SQLite schemas: "main" (default), "temp" (temporary tables), or attached database names
                // For our single-database implementation:
                // - "main" is silently accepted (treated as default)
                // - Other schemas return "no such column" error to match SQLite behavior
                if let Some(schema_name) = schema {
                    let schema_lower = schema_name.to_lowercase();
                    if schema_lower != "main" {
                        // SQLite returns "no such column: schema.table.column" for unknown schemas
                        // Use display forms to preserve original case
                        let schema_display = col_id.schema_display().unwrap_or(schema_name);
                        return Err(ExecutorError::NoSuchColumn {
                            column_ref: format!(
                                "{}.{}.{}",
                                schema_display,
                                table_display.unwrap_or(table.unwrap_or("")),
                                column_display
                            ),
                        });
                    }
                }

                // Special case: "*" is a wildcard used in COUNT(*) and is not a real column
                // Return NULL here - the actual COUNT(*) logic handles this specially
                if column == "*" {
                    return Ok(vibesql_types::SqlValue::Null);
                }

                // Check for ambiguous qualified column references (SQLite compatibility - issue #4507)
                // This must be checked BEFORE resolving the column, as SQLite requires
                // an error when a table alias appears multiple times in the FROM clause.
                // Example: SELECT A.f1 FROM test1 AS A, test1 AS A => "ambiguous column name: A.f1"
                // Use display forms to preserve original case in error messages
                if let Some(table_disp) = table_display {
                    self.schema.validate_qualified_reference(table_disp, column_display)?;
                }

                // Check if table qualifier is valid (SQLite compatibility - issue #4510)
                // If a qualified column reference like "t3.a" has a table qualifier that doesn't
                // exist in the query, return "no such column: t3.a" error
                if let Some(table_name) = table {
                    // Create TableIdentifier for lookup (handles case-insensitive comparison)
                    let table_id = vibesql_catalog::TableIdentifier::from(table_name);
                    let inner_has_table = self.schema.table_schemas.contains_key(&table_id);
                    let outer_has_table = self
                        .outer_schema
                        .is_some_and(|outer| outer.table_schemas.contains_key(&table_id));
                    if !inner_has_table && !outer_has_table {
                        // Table qualifier doesn't exist in query - return "no such column" error
                        return Err(ExecutorError::NoSuchColumn {
                            column_ref: format!(
                                "{}.{}",
                                table_display.unwrap_or(table_name),
                                column_display
                            ),
                        });
                    }
                }

                // SQLite compatibility: Handle ROWID pseudo-column
                // ROWID, _rowid_, and oid are aliases that return the row's unique identifier
                let column_lower = column.to_lowercase();
                if column_lower == "rowid" || column_lower == "_rowid_" || column_lower == "oid" {
                    // First check if schema has a real column with this name
                    if self.get_column_index_cached(table, column).is_none() {
                        // Issue #4536: Check for INTEGER PRIMARY KEY alias column
                        // If the table has an INTEGER PRIMARY KEY, it acts as an alias for rowid.
                        // The column's value IS the rowid, so return that column's value.
                        // Look up the table's schema to find rowid_alias_column
                        let table_id = table.map(vibesql_catalog::TableIdentifier::from);
                        if let Some(table_id) = table_id {
                            if let Some((start_idx, table_schema)) =
                                self.schema.table_schemas.get(&table_id)
                            {
                                if let Some(ipk_col_idx) = table_schema.rowid_alias_column {
                                    // Return the IPK column value (offset by start_idx in combined row)
                                    let combined_idx = start_idx + ipk_col_idx;
                                    return row.get(combined_idx).cloned().ok_or(
                                        ExecutorError::ColumnIndexOutOfBounds {
                                            index: combined_idx,
                                        },
                                    );
                                }
                            }
                        } else {
                            // Unqualified rowid - find the first table with rowid_alias_column
                            // (for single-table queries this will be the only table)
                            for (start_idx, table_schema) in self.schema.table_schemas.values() {
                                if let Some(ipk_col_idx) = table_schema.rowid_alias_column {
                                    let combined_idx = start_idx + ipk_col_idx;
                                    return row.get(combined_idx).cloned().ok_or(
                                        ExecutorError::ColumnIndexOutOfBounds {
                                            index: combined_idx,
                                        },
                                    );
                                }
                            }
                        }

                        // No IPK alias - fall back to row_id tracking
                        // Use the new get_row_id_for_table method that handles both single-table
                        // and multi-table (JOIN) rows (issue #4370)
                        if let Some(row_id) = row.get_row_id_for_table(table) {
                            return Ok(vibesql_types::SqlValue::Bigint(row_id as i64));
                        }
                        // ROWID not available - return NULL (matches SQLite behavior for derived tables)
                        return Ok(vibesql_types::SqlValue::Null);
                    }
                }

                // Check procedural context first (variables/parameters take precedence over table
                // columns) This is only checked when there's no table qualifier, as
                // variables don't have table prefixes
                if table.is_none() {
                    if let Some(proc_ctx) = self.procedural_context {
                        // Try to get value from procedural context (checks variables then
                        // parameters)
                        if let Some(value) = proc_ctx.get_value(column) {
                            return Ok(value.clone());
                        }
                    }

                    // Check for ambiguous unqualified column references (SQLite compatibility)
                    // This must be checked BEFORE resolving the column, as SQLite requires
                    // an error when a column name exists in multiple joined tables.
                    if self.schema.is_column_ambiguous(column) {
                        return Err(ExecutorError::AmbiguousColumnName {
                            column_name: column.to_string(),
                        });
                    }
                }

                // Try to resolve in inner schema first
                if let Some(col_index) = self.get_column_index_cached(table, column) {
                    // Issue #4783: USING column semantics differ from SQLite in OUTER JOINs
                    // For unqualified references to USING columns in RIGHT/FULL OUTER JOINs,
                    // apply COALESCE(left.col, right.col) semantics.
                    // Qualified references (t1.col) should NOT use coalesce - they return the raw value.
                    if table.is_none() {
                        if let Some((left_idx, right_idx)) =
                            self.schema.get_using_coalesce_pair(column)
                        {
                            // Get left value (at col_index, which equals left_idx for USING columns)
                            let left_val = row
                                .get(left_idx)
                                .cloned()
                                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: left_idx })?;

                            // Apply COALESCE: if left is NULL, use right value
                            if left_val == vibesql_types::SqlValue::Null {
                                return row.get(right_idx).cloned().ok_or(
                                    ExecutorError::ColumnIndexOutOfBounds { index: right_idx },
                                );
                            }
                            return Ok(left_val);
                        }
                    }

                    return row
                        .get(col_index)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                }

                // If not found in inner schema and outer context exists, try outer schema
                // FIX for issue #4493: Support chained context resolution for deeply nested subqueries
                // First try immediate parent (outer_row + outer_schema)
                if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
                    if let Some(col_index) = outer_schema.get_column_index(table, column) {
                        return outer_row
                            .get(col_index)
                            .cloned()
                            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                    }
                }

                // If still not found, try chaining through outer_context for grandparent and beyond
                // This implements SQLite-style NameContext chaining for arbitrary nesting depth
                if let Some(outer_context) = self.outer_context {
                    // Recursively resolve through the context chain
                    // The outer_context will search its own schema, then its outer schema, etc.
                    if let (Some(outer_row), Some(_)) =
                        (outer_context.outer_row, outer_context.outer_schema)
                    {
                        if let Some(col_index) =
                            outer_context.schema.get_column_index(table, column)
                        {
                            return outer_row
                                .get(col_index)
                                .cloned()
                                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                        }
                        // Continue chaining recursively
                        if let Some(grandparent_context) = outer_context.outer_context {
                            // TODO: This should be a recursive call, but we need to restructure
                            // For now, this provides 3-level nesting support
                            if let (Some(grandparent_row), Some(_)) =
                                (grandparent_context.outer_row, grandparent_context.outer_schema)
                            {
                                if let Some(col_index) =
                                    grandparent_context.schema.get_column_index(table, column)
                                {
                                    return grandparent_row.get(col_index).cloned().ok_or(
                                        ExecutorError::ColumnIndexOutOfBounds { index: col_index },
                                    );
                                }
                            }
                        }
                    }
                }

                // Column not found in either schema - collect diagnostic info
                let searched_tables: Vec<String> = self.schema.table_names();
                let mut available_columns = Vec::new();
                for (_start, schema) in self.schema.table_schemas.values() {
                    available_columns.extend(schema.columns.iter().map(|c| c.name.clone()));
                }
                if let Some(outer_schema) = self.outer_schema {
                    for (_start, schema) in outer_schema.table_schemas.values() {
                        available_columns.extend(schema.columns.iter().map(|c| c.name.clone()));
                    }
                }

                Err(ExecutorError::ColumnNotFound {
                    column_name: column.to_string(),
                    table_name: table
                        .map(|t| t.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    searched_tables,
                    available_columns,
                })
            }

            // Binary operations
            vibesql_ast::Expression::BinaryOp { left, op, right } => {
                use vibesql_types::SqlValue;

                // Short-circuit evaluation for AND/OR operators
                match op {
                    vibesql_ast::BinaryOperator::And => {
                        let left_val = self.eval(left, row)?;
                        // Short-circuit: if left is false, return false immediately
                        match left_val {
                            SqlValue::Boolean(false) => Ok(SqlValue::Boolean(false)),
                            // For NULL and TRUE, must evaluate right side
                            // SQL three-valued logic:
                            // - NULL AND FALSE = FALSE (not NULL!)
                            // - NULL AND TRUE = NULL
                            // - TRUE AND x = x
                            _ => {
                                let right_val = self.eval(right, row)?;

                                // Special case: NULL AND FALSE = FALSE
                                if matches!(left_val, SqlValue::Null)
                                    && matches!(right_val, SqlValue::Boolean(false))
                                {
                                    return Ok(SqlValue::Boolean(false));
                                }

                                let sql_mode =
                                    self.database.map(|db| db.sql_mode()).unwrap_or_default();
                                ExpressionEvaluator::eval_binary_op_static(
                                    &left_val, op, &right_val, sql_mode,
                                )
                            }
                        }
                    }
                    vibesql_ast::BinaryOperator::Or => {
                        let left_val = self.eval(left, row)?;
                        // Short-circuit: if left is true, return true immediately
                        match left_val {
                            SqlValue::Boolean(true) => Ok(SqlValue::Boolean(true)),
                            // For NULL and FALSE, must evaluate right side
                            // SQL three-valued logic:
                            // - NULL OR TRUE = TRUE (not NULL!)
                            // - NULL OR FALSE = NULL
                            // - FALSE OR x = x
                            _ => {
                                let right_val = self.eval(right, row)?;

                                // Special case: NULL OR TRUE = TRUE
                                if matches!(left_val, SqlValue::Null)
                                    && matches!(right_val, SqlValue::Boolean(true))
                                {
                                    return Ok(SqlValue::Boolean(true));
                                }

                                let sql_mode =
                                    self.database.map(|db| db.sql_mode()).unwrap_or_default();
                                ExpressionEvaluator::eval_binary_op_static(
                                    &left_val, op, &right_val, sql_mode,
                                )
                            }
                        }
                    }
                    // For all other operators, evaluate both sides as before
                    _ => {
                        // Check if this is a comparison operator
                        let is_comparison = matches!(
                            op,
                            vibesql_ast::BinaryOperator::Equal
                                | vibesql_ast::BinaryOperator::NotEqual
                                | vibesql_ast::BinaryOperator::LessThan
                                | vibesql_ast::BinaryOperator::LessThanOrEqual
                                | vibesql_ast::BinaryOperator::GreaterThan
                                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                        );

                        // Handle row value constructor comparisons
                        // SQL:1999 Section 7.1: Row value constructor comparison
                        // (a, b) op (c, d) uses lexicographic ordering
                        if is_comparison {
                            if let (
                                vibesql_ast::Expression::RowValueConstructor(left_values),
                                vibesql_ast::Expression::RowValueConstructor(right_values),
                            ) = (left.as_ref(), right.as_ref())
                            {
                                return self.eval_row_value_comparison(left_values, op, right_values, row);
                            }
                        }

                        // Get effective collation from either side
                        // Priority: explicit COLLATE > column-level collation
                        let collation = if is_comparison {
                            self.get_expression_collation(left)
                                .or_else(|| self.get_expression_collation(right))
                        } else {
                            None
                        };

                        let left_val = self.eval(left, row)?;
                        let right_val = self.eval(right, row)?;

                        // Apply collation to string values if needed
                        let (left_val, right_val) = if let Some(ref collation_name) = collation {
                            let collation_lower = collation_name.to_lowercase();
                            if collation_lower == "nocase" {
                                // For NOCASE collation, uppercase both string values
                                let left_transformed = match &left_val {
                                    SqlValue::Varchar(s) => {
                                        SqlValue::Varchar(arcstr::ArcStr::from(s.to_uppercase()))
                                    }
                                    SqlValue::Character(s) => {
                                        SqlValue::Character(arcstr::ArcStr::from(s.to_uppercase()))
                                    }
                                    other => other.clone(),
                                };
                                let right_transformed = match &right_val {
                                    SqlValue::Varchar(s) => {
                                        SqlValue::Varchar(arcstr::ArcStr::from(s.to_uppercase()))
                                    }
                                    SqlValue::Character(s) => {
                                        SqlValue::Character(arcstr::ArcStr::from(s.to_uppercase()))
                                    }
                                    other => other.clone(),
                                };
                                (left_transformed, right_transformed)
                            } else {
                                // For BINARY or other collations, use values as-is
                                (left_val, right_val)
                            }
                        } else {
                            (left_val, right_val)
                        };

                        // Apply SQLite type affinity rules for comparisons
                        // TEXT column vs INTEGER literal → convert INTEGER to TEXT, string compare
                        let (left_val, right_val) = if is_comparison {
                            self.apply_affinity_for_comparison(left, left_val, right, right_val)
                        } else {
                            (left_val, right_val)
                        };

                        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                        ExpressionEvaluator::eval_binary_op_static(
                            &left_val, op, &right_val, sql_mode,
                        )
                    }
                }
            }

            // CASE expression
            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                self.eval_case(operand, when_clauses, else_result, row)
            }

            // IN operator with subquery
            vibesql_ast::Expression::In { expr, subquery, negated } => {
                self.eval_in_subquery(expr, subquery, *negated, row)
            }

            // Scalar subquery - must return exactly one row and one column
            vibesql_ast::Expression::ScalarSubquery(subquery) => {
                self.eval_scalar_subquery(subquery, row)
            }

            // BETWEEN predicate: expr BETWEEN low AND high
            vibesql_ast::Expression::Between { expr, low, high, negated, symmetric } => {
                self.eval_between(expr, low, high, *negated, *symmetric, row)
            }

            // CAST expression: CAST(expr AS data_type)
            vibesql_ast::Expression::Cast { expr, data_type } => {
                self.eval_cast(expr, data_type, row)
            }

            // POSITION expression: POSITION(substring IN string)
            vibesql_ast::Expression::Position { substring, string, character_unit: _ } => {
                self.eval_position(substring, string, row)
            }

            // TRIM expression: TRIM([position] [removal_char FROM] string)
            vibesql_ast::Expression::Trim { position, removal_char, string } => {
                self.eval_trim(position, removal_char, string, row)
            }

            // EXTRACT expression: EXTRACT(field FROM expr)
            vibesql_ast::Expression::Extract { field, expr } => self.eval_extract(field, expr, row),

            // LIKE pattern matching: expr LIKE pattern
            vibesql_ast::Expression::Like { expr, pattern, negated, .. } => {
                self.eval_like(expr, pattern, *negated, row)
            }

            // GLOB pattern matching: expr GLOB pattern (SQLite)
            vibesql_ast::Expression::Glob { expr, pattern, negated, .. } => {
                self.eval_glob(expr, pattern, *negated, row)
            }

            // IN operator with value list: expr IN (val1, val2, ...)
            vibesql_ast::Expression::InList { expr, values, negated } => {
                self.eval_in_list(expr, values, *negated, row)
            }

            // EXISTS predicate: EXISTS (SELECT ...)
            vibesql_ast::Expression::Exists { subquery, negated } => {
                self.eval_exists(subquery, *negated, row)
            }

            // Quantified comparison: expr op ALL/ANY/SOME (SELECT ...)
            vibesql_ast::Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
                self.eval_quantified(expr, op, quantifier, subquery, row)
            }

            // IS NULL / IS NOT NULL
            vibesql_ast::Expression::IsNull { expr, negated } => {
                self.eval_is_null(expr, *negated, row)
            }

            // IS DISTINCT FROM / IS NOT DISTINCT FROM (SQL:1999)
            vibesql_ast::Expression::IsDistinctFrom { left, right, negated } => {
                self.eval_is_distinct_from(left, right, *negated, row)
            }

            // IS TRUE / IS FALSE / IS UNKNOWN (SQL:1999)
            vibesql_ast::Expression::IsTruthValue { expr, truth_value, negated } => {
                let val = self.eval(expr, row)?;
                // SQL:1999 three-valued logic for IS TRUE/FALSE/UNKNOWN:
                // - IS TRUE: TRUE if expr is TRUE, FALSE if expr is FALSE or UNKNOWN
                // - IS FALSE: TRUE if expr is FALSE, FALSE if expr is TRUE or UNKNOWN
                // - IS UNKNOWN: TRUE if expr is UNKNOWN (NULL), FALSE if expr is TRUE or FALSE
                // - IS NOT X: negates the result
                //
                // SQLite compatibility: integers are treated as booleans
                // - 0 is FALSE
                // - Non-zero integers are TRUE
                // - NULL is UNKNOWN
                let result = match truth_value {
                    vibesql_ast::TruthValue::True => match &val {
                        vibesql_types::SqlValue::Boolean(true) => true,
                        vibesql_types::SqlValue::Integer(n) => *n != 0,
                        vibesql_types::SqlValue::Bigint(n) => *n != 0,
                        vibesql_types::SqlValue::Smallint(n) => *n != 0,
                        _ => false,
                    },
                    vibesql_ast::TruthValue::False => matches!(
                        val,
                        vibesql_types::SqlValue::Boolean(false)
                            | vibesql_types::SqlValue::Integer(0)
                            | vibesql_types::SqlValue::Bigint(0)
                            | vibesql_types::SqlValue::Smallint(0)
                    ),
                    vibesql_ast::TruthValue::Unknown => {
                        matches!(val, vibesql_types::SqlValue::Null)
                    }
                };
                let final_result = if *negated { !result } else { result };
                Ok(vibesql_types::SqlValue::Boolean(final_result))
            }

            // Function expressions - handle scalar functions (not aggregates)
            vibesql_ast::Expression::Function { name, args, character_unit } => {
                self.eval_function(name.display(), args, character_unit, row)
            }

            // Current date/time functions
            vibesql_ast::Expression::CurrentDate => {
                let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                super::super::functions::eval_scalar_function("CURRENT_DATE", &[], &None, &sql_mode)
            }
            vibesql_ast::Expression::CurrentTime { precision: _ } => {
                // For now, ignore precision and call existing function
                // Phase 2 will implement precision-aware formatting
                let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                super::super::functions::eval_scalar_function("CURRENT_TIME", &[], &None, &sql_mode)
            }
            vibesql_ast::Expression::CurrentTimestamp { precision: _ } => {
                // For now, ignore precision and call existing function
                // Phase 2 will implement precision-aware formatting
                let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                super::super::functions::eval_scalar_function(
                    "CURRENT_TIMESTAMP",
                    &[],
                    &None,
                    &sql_mode,
                )
            }

            // Unary operations (delegate to shared function)
            vibesql_ast::Expression::UnaryOp { op, expr } => self.eval_unary(op, expr, row),

            // Window functions - look up pre-computed values
            vibesql_ast::Expression::WindowFunction { function, over } => {
                if let Some(mapping) = self.window_mapping {
                    let key = WindowFunctionKey::from_expression(function, over);
                    if let Some(&col_idx) = mapping.get(&key) {
                        // Extract the pre-computed value from the appended column
                        let value =
                            row.values.get(col_idx).cloned().ok_or({
                                ExecutorError::ColumnIndexOutOfBounds { index: col_idx }
                            })?;
                        Ok(value)
                    } else {
                        Err(ExecutorError::UnsupportedExpression(format!(
                            "Window function not found in mapping: {:?}",
                            expr
                        )))
                    }
                } else {
                    // Extract function name for SQLite-compatible error message
                    // Window functions in WHERE, GROUP BY, or HAVING clauses are misuse
                    let function_name = match function {
                        vibesql_ast::WindowFunctionSpec::Aggregate { name, .. }
                        | vibesql_ast::WindowFunctionSpec::Ranking { name, .. }
                        | vibesql_ast::WindowFunctionSpec::Value { name, .. } => name.to_string(),
                    };
                    Err(ExecutorError::MisuseOfWindowFunction { function_name })
                }
            }

            // Aggregate functions - should be evaluated in aggregation context
            vibesql_ast::Expression::AggregateFunction { name, .. } => {
                // SQLite-compatible error message for aggregate misuse in execution context
                // This error occurs when an aggregate is evaluated outside of aggregation,
                // such as in ORDER BY clauses of non-aggregate queries.
                // Uses "misuse of aggregate: X()" format (with colon) to match SQLite's expr.c
                Err(ExecutorError::MisuseOfAggregateContext { function_name: name.to_string() })
            }

            // Full-text search
            vibesql_ast::Expression::MatchAgainst { columns, search_modifier, mode } => {
                self.eval_match_against(columns, search_modifier, mode, row)
            }

            // Session variable (@@sql_mode, @@version, etc.)
            vibesql_ast::Expression::SessionVariable { name } => {
                // Get session variable from database metadata
                if let Some(db) = self.database {
                    // Get the session variable value from the database metadata
                    if let Some(value) = db.get_session_variable(name) {
                        Ok(value.clone())
                    } else {
                        // Variable not found - return NULL (MySQL behavior)
                        Ok(vibesql_types::SqlValue::Null)
                    }
                } else {
                    // No database context available
                    Err(ExecutorError::UnsupportedExpression(format!(
                        "Session variable @@{} cannot be evaluated without database context",
                        name
                    )))
                }
            }

            // N-ary conjunction (flattened AND chain)
            // SQL three-valued logic: FALSE dominates, then NULL, then TRUE
            vibesql_ast::Expression::Conjunction(terms) => {
                use vibesql_types::SqlValue;
                let mut has_null = false;
                for term in terms {
                    let val = self.eval(term, row)?;
                    match val {
                        SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                        SqlValue::Boolean(true) => {}
                        SqlValue::Null => has_null = true,
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(format!(
                                "Conjunction term is not boolean: {:?}",
                                val
                            )))
                        }
                    }
                }
                if has_null {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(true))
                }
            }

            // N-ary disjunction (flattened OR chain)
            // SQL three-valued logic: TRUE dominates, then NULL, then FALSE
            vibesql_ast::Expression::Disjunction(terms) => {
                use vibesql_types::SqlValue;
                let mut has_null = false;
                for term in terms {
                    let val = self.eval(term, row)?;
                    match val {
                        SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
                        SqlValue::Boolean(false) => {}
                        SqlValue::Null => has_null = true,
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(format!(
                                "Disjunction term is not boolean: {:?}",
                                val
                            )))
                        }
                    }
                }
                if has_null {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(false))
                }
            }

            // COLLATE expression - evaluate inner expression (collation affects string comparison)
            // TODO: Full collation support - for now just evaluate the inner expression
            vibesql_ast::Expression::Collate { expr, .. } => self.eval(expr, row),

            // Unsupported expressions
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }

    /// Evaluate a MATCH...AGAINST full-text search expression
    fn eval_match_against(
        &self,
        columns: &[String],
        search_modifier: &vibesql_ast::Expression,
        mode: &vibesql_ast::FulltextMode,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Evaluate the search string
        let search_value = self.eval(search_modifier, row)?;
        let search_string: arcstr::ArcStr = match search_value {
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => s,
            vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Boolean(false)),
            other => arcstr::ArcStr::from(other.to_string().as_str()),
        };

        // Collect text values from the specified columns
        let mut text_values: Vec<arcstr::ArcStr> = Vec::new();
        for column_name in columns {
            // Try to resolve column in inner schema
            let col_value = if let Some(col_index) = self.get_column_index_cached(None, column_name)
            {
                row.get(col_index).cloned()
            } else if let (Some(outer_row), Some(outer_schema)) =
                (self.outer_row, self.outer_schema)
            {
                // Try outer schema if available
                if let Some(col_index) = outer_schema.get_column_index(None, column_name) {
                    outer_row.get(col_index).cloned()
                } else {
                    None
                }
            } else {
                None
            };

            match col_value {
                Some(vibesql_types::SqlValue::Varchar(s))
                | Some(vibesql_types::SqlValue::Character(s)) => text_values.push(s),
                Some(vibesql_types::SqlValue::Null) => {
                    // NULL values are treated as empty strings in MATCH
                    text_values.push(arcstr::ArcStr::from(""));
                }
                Some(other) => text_values.push(arcstr::ArcStr::from(other.to_string().as_str())),
                None => {
                    // Column not found - return false for this match
                    return Ok(vibesql_types::SqlValue::Boolean(false));
                }
            }
        }

        // Perform full-text search
        let result = super::super::expressions::fulltext::eval_match_against(
            &search_string,
            &text_values,
            mode,
        )?;
        Ok(vibesql_types::SqlValue::Boolean(result))
    }

    /// Evaluate row value constructor comparison
    ///
    /// SQL:1999 Section 7.1: Row value constructor comparison
    /// Row values are compared element by element using lexicographic ordering:
    /// - (a, b) < (c, d) is true if a < c OR (a = c AND b < d)
    /// - (a, b) = (c, d) is true if a = c AND b = d
    /// - (a, b) <> (c, d) is true if a <> c OR b <> d
    fn eval_row_value_comparison(
        &self,
        left_exprs: &[vibesql_ast::Expression],
        op: &vibesql_ast::BinaryOperator,
        right_exprs: &[vibesql_ast::Expression],
        row: &vibesql_storage::Row,
    ) -> Result<SqlValue, ExecutorError> {
        // Row values must have the same number of elements
        if left_exprs.len() != right_exprs.len() {
            return Err(ExecutorError::UnsupportedExpression(format!(
                "Row value constructor size mismatch: left has {} elements, right has {}",
                left_exprs.len(),
                right_exprs.len()
            )));
        }

        // Empty row values are not allowed
        if left_exprs.is_empty() {
            return Err(ExecutorError::UnsupportedExpression(
                "Empty row value constructors are not allowed".to_string(),
            ));
        }

        // Evaluate all elements
        let mut left_values = Vec::with_capacity(left_exprs.len());
        let mut right_values = Vec::with_capacity(right_exprs.len());

        for (left_expr, right_expr) in left_exprs.iter().zip(right_exprs.iter()) {
            left_values.push(self.eval(left_expr, row)?);
            right_values.push(self.eval(right_expr, row)?);
        }

        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();

        // Perform comparison based on operator
        match op {
            vibesql_ast::BinaryOperator::Equal => {
                // (a, b) = (c, d) → a = c AND b = d
                let mut has_null = false;
                for (left_val, right_val) in left_values.iter().zip(right_values.iter()) {
                    let cmp_result = ExpressionEvaluator::eval_binary_op_static(
                        left_val, op, right_val, sql_mode.clone(),
                    )?;
                    match cmp_result {
                        SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                        SqlValue::Null => has_null = true,
                        SqlValue::Boolean(true) => {}
                        _ => {
                            return Err(ExecutorError::TypeError(format!(
                                "Comparison returned non-boolean: {:?}",
                                cmp_result
                            )))
                        }
                    }
                }
                if has_null {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(true))
                }
            }

            vibesql_ast::BinaryOperator::NotEqual => {
                // (a, b) <> (c, d) → a <> c OR b <> d
                let mut has_null = false;
                for (left_val, right_val) in left_values.iter().zip(right_values.iter()) {
                    let eq_result = ExpressionEvaluator::eval_binary_op_static(
                        left_val,
                        &vibesql_ast::BinaryOperator::Equal,
                        right_val,
                        sql_mode.clone(),
                    )?;
                    match eq_result {
                        SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(true)),
                        SqlValue::Null => has_null = true,
                        SqlValue::Boolean(true) => {}
                        _ => {
                            return Err(ExecutorError::TypeError(format!(
                                "Comparison returned non-boolean: {:?}",
                                eq_result
                            )))
                        }
                    }
                }
                if has_null {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(false))
                }
            }

            vibesql_ast::BinaryOperator::LessThan
            | vibesql_ast::BinaryOperator::LessThanOrEqual
            | vibesql_ast::BinaryOperator::GreaterThan
            | vibesql_ast::BinaryOperator::GreaterThanOrEqual => {
                self.eval_row_value_ordering(&left_values, op, &right_values, sql_mode)
            }

            _ => Err(ExecutorError::UnsupportedExpression(format!(
                "Unsupported operator for row value comparison: {:?}",
                op
            ))),
        }
    }

    /// Helper for lexicographic ordering comparison of row values
    fn eval_row_value_ordering(
        &self,
        left_values: &[SqlValue],
        op: &vibesql_ast::BinaryOperator,
        right_values: &[SqlValue],
        sql_mode: vibesql_types::SqlMode,
    ) -> Result<SqlValue, ExecutorError> {
        let is_less = matches!(
            op,
            vibesql_ast::BinaryOperator::LessThan | vibesql_ast::BinaryOperator::LessThanOrEqual
        );
        let is_or_equal = matches!(
            op,
            vibesql_ast::BinaryOperator::LessThanOrEqual
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
        );

        let strict_op = if is_less {
            vibesql_ast::BinaryOperator::LessThan
        } else {
            vibesql_ast::BinaryOperator::GreaterThan
        };

        let eq_op = vibesql_ast::BinaryOperator::Equal;
        let mut has_null = false;

        for i in 0..left_values.len() {
            let left_val = &left_values[i];
            let right_val = &right_values[i];

            let strict_result = ExpressionEvaluator::eval_binary_op_static(
                left_val, &strict_op, right_val, sql_mode.clone(),
            )?;
            match strict_result {
                SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
                SqlValue::Null => has_null = true,
                SqlValue::Boolean(false) => {
                    let eq_result = ExpressionEvaluator::eval_binary_op_static(
                        left_val, &eq_op, right_val, sql_mode.clone(),
                    )?;
                    match eq_result {
                        SqlValue::Boolean(true) => {}
                        SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                        SqlValue::Null => has_null = true,
                        _ => {
                            return Err(ExecutorError::TypeError(format!(
                                "Comparison returned non-boolean: {:?}",
                                eq_result
                            )))
                        }
                    }
                }
                _ => {
                    return Err(ExecutorError::TypeError(format!(
                        "Comparison returned non-boolean: {:?}",
                        strict_result
                    )))
                }
            }
        }

        if has_null {
            Ok(SqlValue::Null)
        } else if is_or_equal {
            Ok(SqlValue::Boolean(true))
        } else {
            Ok(SqlValue::Boolean(false))
        }
    }
}
