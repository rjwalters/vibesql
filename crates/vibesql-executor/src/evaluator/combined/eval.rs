//! Main evaluation entry point for combined expressions

use vibesql_types::{SqlValue, TypeAffinity};

use super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::{errors::ExecutorError, select::WindowFunctionKey};

/// Format a float value in SQLite-compatible text format.
///
/// SQLite ensures floats always have a decimal point when converted to text.
/// For example, 10.0 becomes "10.0", not "10". This is important for type
/// affinity comparisons where TEXT '10' should not equal REAL 10.0.
///
/// The formatting follows SQLite's rules:
/// - Whole numbers get ".0" suffix (10.0 → "10.0")
/// - Normal decimals display normally (10.5 → "10.5")
/// - Very small/large numbers use scientific notation
fn format_float_for_text_comparison(n: f64) -> String {
    if n.is_nan() {
        return "NaN".to_string();
    }
    if n.is_infinite() {
        return if n > 0.0 { "Inf".to_string() } else { "-Inf".to_string() };
    }

    // Use Rust's default formatting
    let s = n.to_string();

    // If the string doesn't contain a decimal point or 'e'/'E' (scientific notation),
    // it's a whole number that needs ".0" appended to match SQLite behavior
    if !s.contains('.') && !s.contains('e') && !s.contains('E') {
        format!("{}.0", s)
    } else {
        s
    }
}

/// Resolve the rowid of `table_name` within a single outer scope described by
/// `schema` (a [`CombinedSchema`] level) and its aligned `row` (issue #6087).
///
/// The scope is only considered if it actually contains the table. Resolution
/// order mirrors the inner-scope rowid logic: an INTEGER PRIMARY KEY column is a
/// rowid alias, so its value (read from `row` at the schema's `start_idx +
/// rowid_alias_column`) wins; otherwise the row's tracked rowid for the table is
/// used. WITHOUT ROWID tables and VIEWs have neither and yield `None`.
fn rowid_from_scope(
    schema: &crate::schema::CombinedSchema,
    row: &vibesql_storage::Row,
    table_name: &str,
) -> Option<SqlValue> {
    let table_id = vibesql_catalog::TableIdentifier::from(table_name);
    let (start_idx, table_schema) = schema.table_schemas.get(&table_id)?;

    // WITHOUT ROWID tables and VIEWs have no rowid pseudo-column.
    if table_schema.without_rowid || table_schema.is_view {
        return None;
    }

    // INTEGER PRIMARY KEY is an alias for rowid; its column value IS the rowid.
    if let Some(ipk_col_idx) = table_schema.rowid_alias_column {
        if let Some(value) = row.get(start_idx + ipk_col_idx) {
            return Some(value.clone());
        }
    }

    // Otherwise use the tracked rowid for this table.
    row.get_row_id_for_table(Some(table_name)).map(|id| SqlValue::Bigint(id as i64))
}

impl CombinedExpressionEvaluator<'_> {
    /// Get the SQLite type affinity of an expression if it's a column reference.
    ///
    /// Returns Some(affinity) if the expression is a column reference and we can
    /// determine its declared type from the schema. Returns None for literals,
    /// function calls, and other non-column expressions.
    pub(in crate::evaluator) fn get_expression_affinity(
        &self,
        expr: &vibesql_ast::Expression,
    ) -> Option<TypeAffinity> {
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
            // A scalar subquery carries the affinity of its (single) result
            // column, so `col = (SELECT y FROM blob_table)` compares by the
            // real column affinity of `y` (NONE for a BLOB column) rather than
            // treating the produced value as an affinity-less literal. Without
            // this, `text_col = (SELECT blob_int_col)` wrongly coerced the
            // integer to text and matched (rowvalue9 3.5/3.6/4.tn.4/4.tn.6,
            // issue #6045).
            vibesql_ast::Expression::ScalarSubquery(subquery) => self.database.and_then(|db| {
                crate::evaluator::combined::subqueries::schema_utils::get_subquery_first_column_affinity(
                    subquery, db,
                )
            }),
            // Literals, functions, and other expressions don't have column affinity
            _ => None,
        }
    }

    /// Look up a column's declared collation in the combined schema.
    ///
    /// Returns `None` when the column has no declared collation (default
    /// BINARY) or cannot be resolved.
    pub(super) fn column_collation_of(
        &self,
        col_id: &vibesql_ast::ColumnIdentifier,
    ) -> Option<String> {
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

    /// Resolve an outer-table rowid reference from the correlation chain (issue #6087).
    ///
    /// A correlated subquery may reference the outer query's rowid, e.g.
    /// `SELECT y FROM d2 WHERE d2.rowid = d1.rowid` where `d1` is the *outer*
    /// table. `d1` is not present in this evaluator's own `self.schema`, so the
    /// normal rowid resolution (which only consults `self.schema` + the inner
    /// `row`) cannot find it and would return NULL — binding the correlated
    /// reference to nothing and yielding the first outer row's result for every
    /// outer row. This walks the outer scopes — the immediate parent
    /// (`outer_row` + `outer_schema`) and then the `outer_context` chain — and
    /// returns the rowid of `table` from the first scope that both contains the
    /// table and carries a resolvable rowid for it.
    ///
    /// `table` must be a qualified table name; an unqualified `rowid` inside a
    /// correlated subquery resolves to the subquery's own tables and never
    /// reaches here.
    fn resolve_outer_rowid(&self, table: Option<&str>) -> Option<SqlValue> {
        let table_name = table?;

        // Try the immediate parent scope: outer_row + outer_schema.
        if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
            if let Some(value) = rowid_from_scope(outer_schema, outer_row, table_name) {
                return Some(value);
            }
        }

        // Walk the outer_context chain (grandparent scopes and beyond) so
        // deeply nested correlated subqueries can still reach an ancestor's
        // outer-table rowid.
        let mut current_context = self.outer_context;
        while let Some(ctx) = current_context {
            if let Some(ctx_row) = ctx.outer_row {
                // The context's own inner schema/row form one enclosing scope.
                if let Some(value) = rowid_from_scope(ctx.schema, ctx_row, table_name) {
                    return Some(value);
                }
                // The context's outer_schema (its parent) forms the next.
                if let Some(ctx_outer_schema) = ctx.outer_schema {
                    if let Some(value) = rowid_from_scope(ctx_outer_schema, ctx_row, table_name) {
                        return Some(value);
                    }
                }
            }
            current_context = ctx.outer_context;
        }

        None
    }

    /// Get the effective collation for an expression.
    ///
    /// Implements SQLite's collation rules (datatype3.html §7.1) via the
    /// shared resolver in `evaluator::collation`: explicit COLLATE
    /// propagates out of compound expressions; a column's implicit collation
    /// is carried only through unary `+` and CAST — it does NOT leak through
    /// `||` or other operators/functions (issue #5792).
    pub(crate) fn get_expression_collation(
        &self,
        expr: &vibesql_ast::Expression,
    ) -> Option<String> {
        crate::evaluator::collation::resolve_expression_collation(expr, &|col_id| {
            self.column_collation_of(col_id)
        })
    }

    /// Resolve the collating sequence for a binary comparison `left <op>
    /// right` per SQLite datatype3 §7.1 (explicit COLLATE on either side
    /// first, then left column's collation — including its default BINARY —
    /// then right column's). `None` means BINARY.
    pub(super) fn comparison_collation(
        &self,
        left: &vibesql_ast::Expression,
        right: &vibesql_ast::Expression,
    ) -> Option<String> {
        crate::evaluator::collation::comparison_collation(left, right, &|col_id| {
            self.column_collation_of(col_id)
        })
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

    /// Check if a SqlValue is numeric (INTEGER, REAL, or NUMERIC storage class).
    ///
    /// Used by `apply_affinity_for_comparison` to detect numeric values produced by
    /// non-literal expressions (e.g., aggregate function results like `count(*)`,
    /// arithmetic like `1+1`, scalar subqueries) so that TEXT-affinity coercion
    /// applies to them, mirroring SQLite's affinity rules.
    fn is_numeric_value(val: &SqlValue) -> bool {
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

        // Case 1: Left is TEXT column, right is a non-column numeric value.
        // SQLite converts numeric values to text for comparison with TEXT columns.
        // This applies to numeric literals AND non-column numeric expressions
        // (aggregates like count(*), arithmetic like 1+1, function calls, scalar subqueries).
        // It does NOT apply to column refs (those have a column affinity, even if NONE);
        // TEXT-column vs NONE-column produces strict type-class inequality per whereB.test.
        // Floats must preserve their decimal representation (10.0 → "10.0", not "10")
        // so that TEXT '10' != REAL 10.0 (different string representations).
        if left_affinity == Some(TypeAffinity::Text)
            && right_affinity.is_none()
            && Self::is_numeric_value(&right_val)
        {
            let right_as_text = match &right_val {
                SqlValue::Integer(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Smallint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Bigint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Unsigned(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Float(n) => SqlValue::Varchar(arcstr::ArcStr::from(
                    format_float_for_text_comparison(*n as f64),
                )),
                SqlValue::Real(n) => {
                    SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(*n)))
                }
                SqlValue::Double(n) => {
                    SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(*n)))
                }
                SqlValue::Numeric(n) => {
                    SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(*n)))
                }
                _ => right_val,
            };
            return (left_val, right_as_text);
        }

        // Case 2: Right is TEXT column, left is a non-column numeric value.
        // Same as Case 1 but symmetric - floats must preserve decimal representation.
        if right_affinity == Some(TypeAffinity::Text)
            && left_affinity.is_none()
            && Self::is_numeric_value(&left_val)
        {
            let left_as_text = match &left_val {
                SqlValue::Integer(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Smallint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Bigint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Unsigned(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Float(n) => SqlValue::Varchar(arcstr::ArcStr::from(
                    format_float_for_text_comparison(*n as f64),
                )),
                SqlValue::Real(n) => {
                    SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(*n)))
                }
                SqlValue::Double(n) => {
                    SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(*n)))
                }
                SqlValue::Numeric(n) => {
                    SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(*n)))
                }
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
        let is_right_non_numeric_affinity =
            matches!(right_affinity, Some(TypeAffinity::None) | Some(TypeAffinity::Text) | None);
        if is_left_numeric_affinity
            && is_right_non_numeric_affinity
            && matches!(right_val, SqlValue::Varchar(_) | SqlValue::Character(_))
        {
            let right_coerced = Self::try_coerce_string_to_numeric(&right_val);
            return (left_val, right_coerced);
        }

        // Case 6: Right is NUMERIC/INTEGER/REAL column, left is NONE/TEXT column with TEXT value
        // Symmetric case of Case 5
        let is_left_non_numeric_affinity =
            matches!(left_affinity, Some(TypeAffinity::None) | Some(TypeAffinity::Text) | None);
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

    /// Apply SQLite type affinity rules for IN expression comparisons.
    ///
    /// IN expressions have different affinity rules than regular comparisons:
    /// - For INTEGER columns, string literals are NOT coerced to integers
    /// - For REAL columns, string literals ARE coerced to REAL
    /// - For TEXT columns, numeric literals are converted to text
    pub fn apply_affinity_for_in_comparison(
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
                SqlValue::Float(n) => SqlValue::Varchar(arcstr::ArcStr::from(
                    format_float_for_text_comparison(*n as f64),
                )),
                SqlValue::Real(n) => {
                    SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(*n)))
                }
                SqlValue::Double(n) => {
                    SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(*n)))
                }
                SqlValue::Numeric(n) => {
                    SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(*n)))
                }
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
                SqlValue::Float(n) => SqlValue::Varchar(arcstr::ArcStr::from(
                    format_float_for_text_comparison(*n as f64),
                )),
                SqlValue::Real(n) => {
                    SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(*n)))
                }
                SqlValue::Double(n) => {
                    SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(*n)))
                }
                SqlValue::Numeric(n) => {
                    SqlValue::Varchar(arcstr::ArcStr::from(format_float_for_text_comparison(*n)))
                }
                _ => left_val,
            };
            return (left_as_text, right_val);
        }

        // Case 3: Left is REAL column, right is string literal
        // REAL affinity DOES coerce strings to numbers in IN expressions
        if left_affinity == Some(TypeAffinity::Real) && self.is_string_literal(right_expr) {
            let right_coerced = Self::try_coerce_string_to_numeric(&right_val);
            return (left_val, right_coerced);
        }

        // Case 4: Right is REAL column, left is string literal
        if right_affinity == Some(TypeAffinity::Real) && self.is_string_literal(left_expr) {
            let left_coerced = Self::try_coerce_string_to_numeric(&left_val);
            return (left_coerced, right_val);
        }

        // For IN expressions, INTEGER affinity does NOT coerce strings
        // (unlike regular comparison). No affinity conversion needed.
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
                    // Walk the FULL outer_schema chain, not just its immediate level.
                    // For a doubly-nested correlated subquery the innermost
                    // evaluator's `outer_schema` is an empty merged schema whose own
                    // `outer_schema` field links to the real outer table, so a
                    // single-level check spuriously reported the table missing and
                    // raised NoSuchColumn even though `get_column_index` (which walks
                    // the chain) would have resolved it. Issue #6047 (follow-up to the
                    // unqualified-column fix in #4618).
                    let outer_has_table = self
                        .outer_schema
                        .is_some_and(|outer| outer.contains_table_in_chain(&table_id));
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
                // WITHOUT ROWID tables do NOT have the rowid pseudo-column (Issue #4953)
                let column_lower = column.to_lowercase();
                if column_lower == "rowid" || column_lower == "_rowid_" || column_lower == "oid" {
                    // First check if schema has a real column with this name
                    if self.get_column_index_cached(table, column).is_none() {
                        // Check if the table is WITHOUT ROWID - if so, error
                        let table_id = table.map(vibesql_catalog::TableIdentifier::from);

                        // Correlated outer-table rowid (issue #6087). When the
                        // rowid is qualified by a table that is NOT one of this
                        // scope's own tables (e.g. `d1.rowid` inside
                        // `SELECT y FROM d2 WHERE d2.rowid = d1.rowid` where `d1`
                        // is the *outer* table), it must resolve against the outer
                        // row, not the inner one. Resolving it here — before the
                        // inner-row fallbacks below — is essential: the inner
                        // row's single `row_id` field answers
                        // `get_row_id_for_table(Some("d1"))` for ANY table name, so
                        // the fallback at the end of this block would otherwise
                        // bind `d1.rowid` to the *inner* d2 row's rowid, making the
                        // correlated predicate `d2.rowid = d1.rowid` trivially true
                        // for every inner row and returning the first outer row's
                        // result to all outer rows (rowvalue9 4.tn.4 / 4.tn.6).
                        if let Some(ref table_id) = table_id {
                            if !self.schema.table_schemas.contains_key(table_id) {
                                if let Some(value) = self.resolve_outer_rowid(table) {
                                    return Ok(value);
                                }
                                // Table is not in this scope and has no resolvable
                                // outer rowid: fall through to NULL (matching the
                                // derived-table behavior at the end of this block).
                                return Ok(vibesql_types::SqlValue::Null);
                            }
                        }
                        if let Some(ref table_id) = table_id {
                            if let Some((_, table_schema)) = self.schema.table_schemas.get(table_id)
                            {
                                // WITHOUT ROWID tables (#4953) and VIEWs (#5492)
                                // both lack the rowid pseudo-column.
                                if table_schema.without_rowid || table_schema.is_view {
                                    return Err(ExecutorError::ColumnNotFound {
                                        column_name: column.to_string(),
                                        table_name: table_id.display().to_string(),
                                        searched_tables: vec![table_id.display().to_string()],
                                        available_columns: table_schema
                                            .columns
                                            .iter()
                                            .map(|c| c.name.clone())
                                            .collect(),
                                    });
                                }
                            }
                        } else {
                            // Unqualified rowid - check if any table in scope is
                            // WITHOUT ROWID or a VIEW (neither has a rowid).
                            for (tid, (_, table_schema)) in &self.schema.table_schemas {
                                if table_schema.without_rowid || table_schema.is_view {
                                    return Err(ExecutorError::ColumnNotFound {
                                        column_name: column.to_string(),
                                        table_name: tid.display().to_string(),
                                        searched_tables: self.schema.table_names(),
                                        available_columns: table_schema
                                            .columns
                                            .iter()
                                            .map(|c| c.name.clone())
                                            .collect(),
                                    });
                                }
                            }
                        }

                        // Issue #4536: Check for INTEGER PRIMARY KEY alias column
                        // If the table has an INTEGER PRIMARY KEY, it acts as an alias for rowid.
                        // The column's value IS the rowid, so return that column's value.
                        // Look up the table's schema to find rowid_alias_column
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
                    // Issue #4783, #4903: USING column semantics differ from SQLite in OUTER JOINs
                    // For unqualified references to USING columns in RIGHT/FULL OUTER JOINs,
                    // apply N-way COALESCE semantics: return first non-NULL from chain.
                    // Qualified references (t1.col) should NOT use coalesce - they return the raw value.
                    //
                    // Issue #4905: For alias table references (e.g., j1.id where j1 aliases a USING join),
                    // we SHOULD apply COALESCE semantics because the alias table represents the
                    // coalesced join result, not individual underlying tables.
                    let should_coalesce = if table.is_none() {
                        true // Unqualified references always use COALESCE for USING columns
                    } else if let Some(table_name) = table {
                        // Check if this is an alias table
                        use vibesql_catalog::TableIdentifier;
                        let table_id = TableIdentifier::unquoted(table_name);
                        self.schema.alias_tables.contains(&table_id)
                    } else {
                        false
                    };

                    if should_coalesce {
                        if let Some(indices) = self.schema.get_using_coalesce_indices(column) {
                            // Apply N-way COALESCE: return first non-NULL value from chain
                            for &idx in indices {
                                if let Some(val) = row.get(idx) {
                                    if *val != vibesql_types::SqlValue::Null {
                                        return Ok(val.clone());
                                    }
                                }
                            }
                            // All values are NULL, return NULL
                            return Ok(vibesql_types::SqlValue::Null);
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
                // FIX for issue #4994: Use loop-based traversal to support unlimited nesting depth
                // instead of the previous manual 2-level check
                let mut current_context = self.outer_context;
                while let Some(ctx) = current_context {
                    // Check this context's schema (which represents the parent evaluator's inner schema)
                    if let Some(outer_row) = ctx.outer_row {
                        if let Some(col_index) = ctx.schema.get_column_index(table, column) {
                            return outer_row
                                .get(col_index)
                                .cloned()
                                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                        }
                        // Also check this context's outer_schema (grandparent's schema)
                        if let Some(outer_schema) = ctx.outer_schema {
                            if let Some(col_index) = outer_schema.get_column_index(table, column) {
                                return outer_row.get(col_index).cloned().ok_or(
                                    ExecutorError::ColumnIndexOutOfBounds { index: col_index },
                                );
                            }
                        }
                    }
                    // Move to the next level in the context chain
                    current_context = ctx.outer_context;
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
                        // (a, b) op (c, d) uses lexicographic ordering. The
                        // right-hand side may also be a scalar subquery that
                        // returns a row of the same arity, e.g.
                        // (a, b, c) > (SELECT a, b, c FROM t WHERE ...).
                        if is_comparison {
                            match (left.as_ref(), right.as_ref()) {
                                (
                                    vibesql_ast::Expression::RowValueConstructor(left_values),
                                    vibesql_ast::Expression::RowValueConstructor(right_values),
                                ) => {
                                    return self.eval_row_value_comparison(
                                        left_values,
                                        op,
                                        right_values,
                                        row,
                                    );
                                }
                                (
                                    vibesql_ast::Expression::RowValueConstructor(tuple_exprs),
                                    vibesql_ast::Expression::ScalarSubquery(subquery),
                                ) => {
                                    return self.eval_row_value_subquery_comparison(
                                        tuple_exprs,
                                        op,
                                        subquery,
                                        true,
                                        row,
                                    );
                                }
                                (
                                    vibesql_ast::Expression::ScalarSubquery(subquery),
                                    vibesql_ast::Expression::RowValueConstructor(tuple_exprs),
                                ) => {
                                    return self.eval_row_value_subquery_comparison(
                                        tuple_exprs,
                                        op,
                                        subquery,
                                        false,
                                        row,
                                    );
                                }
                                (
                                    vibesql_ast::Expression::ScalarSubquery(left_sub),
                                    vibesql_ast::Expression::ScalarSubquery(right_sub),
                                ) => {
                                    // Two multi-column subqueries compared as row
                                    // values, e.g. (SELECT a,b)==(SELECT x,y). When
                                    // both are single-column this returns None and
                                    // we fall through to scalar comparison.
                                    if let Some(result) = self.eval_two_subquery_row_comparison(
                                        left_sub, op, right_sub, row,
                                    )? {
                                        return Ok(result);
                                    }
                                }
                                // A row value compared against anything that is
                                // not a row value or a scalar subquery (e.g.
                                // `(a, b) = 1`) is a misuse per SQLite.
                                (vibesql_ast::Expression::RowValueConstructor(elems), _)
                                | (_, vibesql_ast::Expression::RowValueConstructor(elems))
                                    if elems.len() > 1 =>
                                {
                                    return Err(ExecutorError::RowValueMisused);
                                }
                                _ => {}
                            }
                        }

                        // Get effective collation per SQLite datatype3 §7.1:
                        // explicit COLLATE (left, then right) > left column's
                        // collation (its default BINARY blocks the right's) >
                        // right column's collation > BINARY.
                        let collation = if is_comparison {
                            self.comparison_collation(left, right)
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
                            } else if collation_lower == "rtrim" {
                                // For RTRIM collation, trim trailing whitespace from both string values
                                let left_transformed = match &left_val {
                                    SqlValue::Varchar(s) => {
                                        SqlValue::Varchar(arcstr::ArcStr::from(s.trim_end()))
                                    }
                                    SqlValue::Character(s) => {
                                        SqlValue::Character(arcstr::ArcStr::from(s.trim_end()))
                                    }
                                    other => other.clone(),
                                };
                                let right_transformed = match &right_val {
                                    SqlValue::Varchar(s) => {
                                        SqlValue::Varchar(arcstr::ArcStr::from(s.trim_end()))
                                    }
                                    SqlValue::Character(s) => {
                                        SqlValue::Character(arcstr::ArcStr::from(s.trim_end()))
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
            vibesql_ast::Expression::Like { expr, pattern, negated, escape } => {
                self.eval_like(expr, pattern, escape, *negated, row)
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
                // SQLite compatibility: any value is coerced to a boolean via the
                // shared truthiness rule (numeric prefix != 0), covering TEXT/BLOB
                // operands too (`'3' IS TRUE` -> 1, `'abc' IS TRUE` -> 0). NULL is
                // UNKNOWN. See crate::evaluator::operators::is_truthy.
                use crate::evaluator::operators::is_truthy;
                let is_null = matches!(val, vibesql_types::SqlValue::Null);
                let result = match truth_value {
                    vibesql_ast::TruthValue::True => !is_null && is_truthy(&val),
                    vibesql_ast::TruthValue::False => !is_null && !is_truthy(&val),
                    vibesql_ast::TruthValue::Unknown => is_null,
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
                super::super::functions::eval_scalar_function(
                    "CURRENT_DATE",
                    &[],
                    &None,
                    &sql_mode,
                    crate::evaluator::SchemaExprContext::None,
                )
            }
            vibesql_ast::Expression::CurrentTime { precision: _ } => {
                // For now, ignore precision and call existing function
                // Phase 2 will implement precision-aware formatting
                let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                super::super::functions::eval_scalar_function(
                    "CURRENT_TIME",
                    &[],
                    &None,
                    &sql_mode,
                    crate::evaluator::SchemaExprContext::None,
                )
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
                    crate::evaluator::SchemaExprContext::None,
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

            // Pseudo-variable (OLD.column, NEW.column in triggers)
            // Resolved against the trigger context if one was threaded into this evaluator
            // (e.g. for the synthetic SELECT used by UPDATE...FROM inside a trigger body, #5082).
            vibesql_ast::Expression::PseudoVariable { pseudo_table, column } => {
                if let Some(ctx) = self.trigger_context {
                    ctx.resolve_pseudo_var(*pseudo_table, column)
                } else {
                    // Outside of a trigger body, `NEW.col`/`OLD.col` references an
                    // unknown column. SQLite reports this as a plain column-resolution
                    // error (e.g. `no such column: new.x`), so emit that exact wording
                    // for compatibility rather than an "unsupported expression" message.
                    let pseudo_table_lower = match pseudo_table {
                        vibesql_ast::PseudoTable::Old => "old",
                        vibesql_ast::PseudoTable::New => "new",
                    };
                    Err(ExecutorError::SqliteCompatError(format!(
                        "no such column: {}.{}",
                        pseudo_table_lower, column
                    )))
                }
            }

            // RAISE() trigger-program error/abort expression (SQLite).
            vibesql_ast::Expression::Raise { action, error_message } => {
                self.eval_raise(*action, error_message.as_deref(), row)
            }

            // Row value constructor in a scalar context (bare in a SELECT list,
            // function argument, arithmetic operand, ...) — "row value misused".
            vibesql_ast::Expression::RowValueConstructor(_) => Err(ExecutorError::RowValueMisused),

            // Unsupported expressions
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }

    /// Evaluate a `RAISE(...)` trigger-program expression.
    ///
    /// `RAISE(IGNORE)` yields [`ExecutorError::RaiseIgnore`] (a control-flow
    /// signal the trigger loop intercepts to skip the current row).
    /// `RAISE(ABORT|FAIL|ROLLBACK, msg)` evaluates the message expression and
    /// yields [`ExecutorError::Raise`] carrying the action and rendered message.
    fn eval_raise(
        &self,
        action: vibesql_ast::RaiseAction,
        error_message: Option<&vibesql_ast::Expression>,
        row: &vibesql_storage::Row,
    ) -> Result<SqlValue, ExecutorError> {
        match action {
            vibesql_ast::RaiseAction::Ignore => Err(ExecutorError::RaiseIgnore),
            _ => {
                let message = match error_message {
                    Some(expr) => {
                        crate::evaluator::raise::render_raise_message(self.eval(expr, row)?)
                    }
                    None => String::new(),
                };
                Err(ExecutorError::Raise { action, message })
            }
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
    pub(in crate::evaluator) fn eval_row_value_comparison(
        &self,
        left_exprs: &[vibesql_ast::Expression],
        op: &vibesql_ast::BinaryOperator,
        right_exprs: &[vibesql_ast::Expression],
        row: &vibesql_storage::Row,
    ) -> Result<SqlValue, ExecutorError> {
        // Row values must have the same number of elements (SQLite reports a
        // mismatched-arity comparison as "row value misused").
        if left_exprs.len() != right_exprs.len() {
            return Err(ExecutorError::RowValueMisused);
        }

        // Empty row values are not allowed
        if left_exprs.is_empty() {
            return Err(ExecutorError::UnsupportedExpression(
                "Empty row value constructors are not allowed".to_string(),
            ));
        }

        // Evaluate each element pair and apply per-element collation and
        // per-column SQLite type affinity, mirroring the scalar comparison path
        // so that mixed TEXT/NUMERIC tuples compare per SQLite rules instead of
        // by raw storage class.
        let mut left_values = Vec::with_capacity(left_exprs.len());
        let mut right_values = Vec::with_capacity(right_exprs.len());

        for (left_expr, right_expr) in left_exprs.iter().zip(right_exprs.iter()) {
            let left_val = self.eval(left_expr, row)?;
            let right_val = self.eval(right_expr, row)?;
            let collation = self
                .get_expression_collation(left_expr)
                .or_else(|| self.get_expression_collation(right_expr));
            let (left_val, right_val) = crate::evaluator::row_value::apply_collation_to_pair(
                left_val,
                right_val,
                collation.as_deref(),
            );
            let (left_val, right_val) =
                self.apply_affinity_for_comparison(left_expr, left_val, right_expr, right_val);
            left_values.push(left_val);
            right_values.push(right_val);
        }

        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();

        crate::evaluator::row_value::compare_row_values(
            &left_values,
            op,
            &right_values,
            |l, o, r| ExpressionEvaluator::eval_binary_op_static(l, o, r, sql_mode.clone()),
        )
    }

    /// Compare a row-value tuple against a scalar subquery that yields a single
    /// row of the same arity, e.g. `(a, b, c) > (SELECT a, b, c FROM t WHERE ...)`.
    ///
    /// `tuple_on_left` is true when the tuple is the left-hand operand; when the
    /// subquery is on the left the operator is flipped so the lexicographic
    /// comparison is always evaluated tuple-on-left.
    pub(in crate::evaluator) fn eval_row_value_subquery_comparison(
        &self,
        tuple_exprs: &[vibesql_ast::Expression],
        op: &vibesql_ast::BinaryOperator,
        subquery: &vibesql_ast::SelectStmt,
        tuple_on_left: bool,
        row: &vibesql_storage::Row,
    ) -> Result<SqlValue, ExecutorError> {
        if tuple_exprs.is_empty() {
            return Err(ExecutorError::UnsupportedExpression(
                "Empty row value constructors are not allowed".to_string(),
            ));
        }

        let (tuple_values, subquery_values) =
            self.eval_row_value_tuple_and_subquery(tuple_exprs, subquery, row)?;

        // Always evaluate with the tuple on the left. `subquery OP tuple` is
        // equivalent to `tuple flip(OP) subquery`, so only the operator flips.
        let effective_op = if tuple_on_left {
            *op
        } else {
            crate::evaluator::row_value::flip_comparison_operator(op)
        };

        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();

        crate::evaluator::row_value::compare_row_values(
            &tuple_values,
            &effective_op,
            &subquery_values,
            |l, o, r| ExpressionEvaluator::eval_binary_op_static(l, o, r, sql_mode.clone()),
        )
    }

    /// `(a, b) IS [NOT] (SELECT ...)` — NULL-safe row-value comparison against a
    /// scalar subquery that yields a single row of the same arity.
    pub(in crate::evaluator) fn eval_row_value_is_distinct_subquery(
        &self,
        tuple_exprs: &[vibesql_ast::Expression],
        subquery: &vibesql_ast::SelectStmt,
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<SqlValue, ExecutorError> {
        if tuple_exprs.is_empty() {
            return Err(ExecutorError::UnsupportedExpression(
                "Empty row value constructors are not allowed".to_string(),
            ));
        }

        let (tuple_values, subquery_values) =
            self.eval_row_value_tuple_and_subquery(tuple_exprs, subquery, row)?;

        Ok(crate::evaluator::row_value::row_values_is_distinct(
            &tuple_values,
            &subquery_values,
            negated,
        ))
    }

    /// Shared helper for row-value-vs-subquery operators: evaluates the tuple
    /// elements and the subquery row, applying per-element affinity (treating the
    /// subquery columns as non-column values, like literals).
    fn eval_row_value_tuple_and_subquery(
        &self,
        tuple_exprs: &[vibesql_ast::Expression],
        subquery: &vibesql_ast::SelectStmt,
        row: &vibesql_storage::Row,
    ) -> Result<(Vec<SqlValue>, Vec<SqlValue>), ExecutorError> {
        let arity = tuple_exprs.len();
        let subquery_values = self.eval_scalar_subquery_as_row(subquery, row, arity)?;

        // Per-position affinity of the subquery's result columns, so a BLOB
        // (NONE-affinity) subquery column suppresses TEXT coercion of the tuple
        // side — matching `(c, a) = (SELECT x, y FROM blob_table)` in SQLite
        // (rowvalue9 3.6 / 4.tn.4, issue #6045). Falls back to treating the
        // value as an affinity-less literal when the column affinity can't be
        // resolved.
        let sub_affinities: Vec<Option<vibesql_types::TypeAffinity>> = (0..arity)
            .map(|i| {
                self.database.and_then(|db| {
                    crate::evaluator::combined::subqueries::schema_utils::get_subquery_column_affinity_at(
                        subquery, i, db,
                    )
                })
            })
            .collect();

        let mut tuple_values = Vec::with_capacity(arity);
        let mut other_values = Vec::with_capacity(arity);
        for (idx, (tuple_expr, sub_val)) in tuple_exprs.iter().zip(subquery_values).enumerate() {
            let tuple_val = self.eval(tuple_expr, row)?;
            // Per-element collation from the tuple side (explicit COLLATE or
            // column-declared collation), e.g. `(a COLLATE nocase, b) = (SELECT ...)`.
            let collation = self.get_expression_collation(tuple_expr);
            let (tuple_val, sub_val) = crate::evaluator::row_value::apply_collation_to_pair(
                tuple_val,
                sub_val,
                collation.as_deref(),
            );
            let tuple_affinity = self.get_expression_affinity(tuple_expr);
            let (tuple_val, sub_val) = ExpressionEvaluator::apply_affinity_for_comparison_values(
                tuple_val,
                tuple_affinity,
                sub_val,
                sub_affinities[idx],
            );
            tuple_values.push(tuple_val);
            other_values.push(sub_val);
        }
        Ok((tuple_values, other_values))
    }

    /// Compare two scalar subqueries as row values, e.g.
    /// `(SELECT a, b) == (SELECT x, y)`. Returns `Ok(None)` when both are
    /// single-column (caller falls back to scalar comparison); `Ok(Some(result))`
    /// for the multi-column row-value comparison. Mismatched arities error.
    pub(in crate::evaluator) fn eval_two_subquery_row_comparison(
        &self,
        left_sub: &vibesql_ast::SelectStmt,
        op: &vibesql_ast::BinaryOperator,
        right_sub: &vibesql_ast::SelectStmt,
        row: &vibesql_storage::Row,
    ) -> Result<Option<SqlValue>, ExecutorError> {
        if crate::evaluator::row_value::subquery_is_obviously_single_column(left_sub)
            && crate::evaluator::row_value::subquery_is_obviously_single_column(right_sub)
        {
            return Ok(None);
        }

        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "Subquery execution requires database reference".to_string(),
        ))?;
        let left_arity = super::subqueries::schema_utils::compute_select_list_column_count(
            left_sub,
            database,
            self.cte_context,
        )?;
        let right_arity = super::subqueries::schema_utils::compute_select_list_column_count(
            right_sub,
            database,
            self.cte_context,
        )?;

        if left_arity == 1 && right_arity == 1 {
            return Ok(None);
        }
        if left_arity != right_arity {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: left_arity,
                actual: right_arity,
            });
        }

        let left_values = self.eval_scalar_subquery_as_row(left_sub, row, left_arity)?;
        let right_values = self.eval_scalar_subquery_as_row(right_sub, row, right_arity)?;

        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
        Ok(Some(crate::evaluator::row_value::compare_row_values(
            &left_values,
            op,
            &right_values,
            |l, o, r| ExpressionEvaluator::eval_binary_op_static(l, o, r, sql_mode.clone()),
        )?))
    }

    /// `(a, b) IS [NOT] (c, d)` — NULL-safe element-wise row-value comparison.
    /// SQLite applies the same per-column affinity to IS / IS NOT as to = / <>.
    pub(in crate::evaluator) fn eval_row_value_is_distinct(
        &self,
        left_exprs: &[vibesql_ast::Expression],
        right_exprs: &[vibesql_ast::Expression],
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<SqlValue, ExecutorError> {
        if left_exprs.len() != right_exprs.len() {
            return Err(ExecutorError::RowValueMisused);
        }
        if left_exprs.is_empty() {
            return Err(ExecutorError::UnsupportedExpression(
                "Empty row value constructors are not allowed".to_string(),
            ));
        }

        // Element-wise NULL-safe distinctness. Elements may themselves be row
        // values (e.g. `(2,(2,0)) IS (2,(2,0))`), which recurse structurally.
        let mut any_distinct = false;
        for (left_expr, right_expr) in left_exprs.iter().zip(right_exprs.iter()) {
            if self.row_value_element_is_distinct(left_expr, right_expr, row)? {
                any_distinct = true;
                break;
            }
        }

        // negated == true corresponds to IS (NULL-safe equality).
        Ok(SqlValue::Boolean(if negated { !any_distinct } else { any_distinct }))
    }

    /// NULL-safe distinctness of a single row-value element pair.
    ///
    /// Nested row values on both sides recurse structurally; a row value paired
    /// with a scalar (or mismatched nested arity) is a misuse per SQLite.
    fn row_value_element_is_distinct(
        &self,
        left_expr: &vibesql_ast::Expression,
        right_expr: &vibesql_ast::Expression,
        row: &vibesql_storage::Row,
    ) -> Result<bool, ExecutorError> {
        match (left_expr, right_expr) {
            (
                vibesql_ast::Expression::RowValueConstructor(left_elems),
                vibesql_ast::Expression::RowValueConstructor(right_elems),
            ) => {
                if left_elems.len() != right_elems.len() {
                    return Err(ExecutorError::RowValueMisused);
                }
                for (l, r) in left_elems.iter().zip(right_elems.iter()) {
                    if self.row_value_element_is_distinct(l, r, row)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            (vibesql_ast::Expression::RowValueConstructor(_), _)
            | (_, vibesql_ast::Expression::RowValueConstructor(_)) => {
                Err(ExecutorError::RowValueMisused)
            }
            _ => {
                let left_val = self.eval(left_expr, row)?;
                let right_val = self.eval(right_expr, row)?;
                let collation = self
                    .get_expression_collation(left_expr)
                    .or_else(|| self.get_expression_collation(right_expr));
                let (left_val, right_val) = crate::evaluator::row_value::apply_collation_to_pair(
                    left_val,
                    right_val,
                    collation.as_deref(),
                );
                let (left_val, right_val) =
                    self.apply_affinity_for_comparison(left_expr, left_val, right_expr, right_val);
                Ok(super::super::core::values_are_distinct(&left_val, &right_val))
            }
        }
    }
}
