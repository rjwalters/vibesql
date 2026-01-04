//! Column name derivation for SELECT results

use super::builder::SelectExecutor;
use crate::{errors::ExecutorError, schema::CombinedSchema, select::join::FromResult};

/// Column naming mode based on PRAGMA settings
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum ColumnNamingMode {
    /// full_column_names=ON: Always use "table.column" format
    Full,
    /// short_column_names=ON (default): Use just column name ("f1")
    Short,
    /// Both PRAGMAs OFF: Use full expression text as column name
    Expression,
}

impl SelectExecutor<'_> {
    /// Determine the column naming mode from database PRAGMA settings
    fn get_column_naming_mode(&self) -> ColumnNamingMode {
        let full = self.database.full_column_names();
        let short = self.database.short_column_names();

        // SQLite behavior:
        // - full_column_names takes precedence when ON
        // - Otherwise, if short_column_names is ON, use short names
        // - If both are OFF, use expression text
        if full {
            ColumnNamingMode::Full
        } else if short {
            ColumnNamingMode::Short
        } else {
            ColumnNamingMode::Expression
        }
    }

    /// Derive column names from SELECT list
    ///
    /// Column naming follows SQLite's PRAGMA settings:
    /// - `full_column_names=ON`: Use "table.column" format
    /// - `short_column_names=ON` (default): Use just the column name
    /// - Both OFF: Use the expression text (e.g., "test1 . f1" preserving spaces)
    ///
    /// Note: Explicit aliases always take precedence over PRAGMA-based naming.
    pub(super) fn derive_column_names(
        &self,
        select_list: &[vibesql_ast::SelectItem],
        from_result: Option<&FromResult>,
    ) -> Result<Vec<String>, ExecutorError> {
        let mode = self.get_column_naming_mode();
        self.derive_column_names_internal(select_list, from_result, mode)
    }

    /// Derive simple column names (without table prefix) for internal use
    ///
    /// This is used when column names are needed for schema purposes (e.g., view creation)
    /// where the table prefix would cause issues with column lookups.
    pub(super) fn derive_simple_column_names(
        &self,
        select_list: &[vibesql_ast::SelectItem],
        from_result: Option<&FromResult>,
    ) -> Result<Vec<String>, ExecutorError> {
        self.derive_column_names_internal(select_list, from_result, ColumnNamingMode::Short)
    }

    /// Internal implementation with configurable naming mode
    fn derive_column_names_internal(
        &self,
        select_list: &[vibesql_ast::SelectItem],
        from_result: Option<&FromResult>,
        mode: ColumnNamingMode,
    ) -> Result<Vec<String>, ExecutorError> {
        let mut column_names = Vec::new();
        let schema = from_result.map(|fr| &fr.schema);

        for item in select_list {
            match item {
                vibesql_ast::SelectItem::Wildcard { alias } => {
                    // SELECT * [AS (col1, col2, ...)] - expand to all column names from schema
                    // Skip hidden columns (from NATURAL JOIN deduplication)
                    // For RIGHT/FULL OUTER JOINs, hidden columns with replacements are included
                    // Respects full_column_names PRAGMA when there are multiple tables with
                    // duplicate columns
                    //
                    // SQL Standard Column Ordering for USING/NATURAL JOINs:
                    // According to SQL:2011, SELECT * from JOIN USING should output:
                    // 1. The common/USING columns (merged) - appear FIRST
                    // 2. Remaining columns from the left table
                    // 3. Remaining columns from the right table
                    if let Some(from_res) = from_result {
                        // Get all column names in order from the combined schema
                        // Store (index, column_name, table_name_for_display, is_joined)
                        let mut table_columns: Vec<(usize, String, String, bool)> = Vec::new();

                        // Build a set of replacement target indices (columns that are replacement
                        // sources) These should be skipped since they're
                        // output via the hidden column's position
                        let replacement_targets: std::collections::HashSet<usize> =
                            from_res.schema.column_replacement_map.values().copied().collect();

                        // Sort table_schemas by start_index for deterministic iteration order
                        // HashMap iteration is non-deterministic; sorting ensures consistent results
                        let mut sorted_tables: Vec<_> =
                            from_res.schema.table_schemas.iter().collect();
                        sorted_tables.sort_by_key(|(_, (start_index, _))| *start_index);

                        for (table_id, (start_index, table_schema)) in sorted_tables {
                            // Get the effective table name using TableIdentifier's display form
                            let effective_table_name = table_id.display().to_string();

                            for (col_idx, col_schema) in table_schema.columns.iter().enumerate() {
                                let abs_idx = start_index + col_idx;

                                // Skip columns that are replacement targets (they're output via
                                // replacement)
                                if replacement_targets.contains(&abs_idx) {
                                    continue;
                                }

                                // Skip right-side USING columns (they're output via the left-side
                                // column with COALESCE applied)
                                if from_res.schema.is_using_coalesce_right_side(abs_idx) {
                                    continue;
                                }

                                // For hidden columns, check if they have a replacement or coalesce pair
                                // If so, include the name (value comes from replacement/coalesce)
                                // If not, skip them
                                if from_res.schema.is_column_hidden(abs_idx) {
                                    let has_replacement =
                                        from_res.schema.get_column_replacement(abs_idx).is_some();
                                    let has_coalesce = from_res
                                        .schema
                                        .get_using_coalesce_right_for_left(abs_idx)
                                        .is_some();
                                    if !has_replacement && !has_coalesce {
                                        continue;
                                    }
                                }

                                let col_name = col_schema.name.clone();
                                // Check if this is a USING/NATURAL JOIN column
                                let is_joined = from_res
                                    .schema
                                    .joined_columns
                                    .contains(&col_name.to_lowercase());
                                table_columns.push((
                                    abs_idx,
                                    col_name,
                                    effective_table_name.clone(),
                                    is_joined,
                                ));
                            }
                        }

                        // Sort columns per SQL standard for USING/NATURAL JOINs:
                        // 1. Joined columns first (in order of their original position)
                        // 2. Then remaining columns (in order of their original position)
                        // This ensures USING columns appear first in SELECT * output
                        table_columns.sort_by_key(|(idx, _, _, is_joined)| (!*is_joined, *idx));

                        // Apply derived column list if present
                        if let Some(derived_cols) = alias {
                            if derived_cols.len() != table_columns.len() {
                                return Err(ExecutorError::ColumnCountMismatch {
                                    expected: table_columns.len(),
                                    provided: derived_cols.len(),
                                });
                            }
                            column_names.extend(derived_cols.clone());
                        } else {
                            // SELECT * uses table prefix when full_column_names=ON
                            // BUT only when there's ambiguity (multiple tables or explicit alias)
                            let has_multiple_tables = from_res.schema.table_schemas.len() > 1;

                            for (_, col_name, effective_table_name, _) in table_columns {
                                let display_name = match mode {
                                    ColumnNamingMode::Full if has_multiple_tables => {
                                        format!("{}.{}", effective_table_name, col_name)
                                    }
                                    _ => col_name,
                                };
                                column_names.push(display_name);
                            }
                        }
                    } else {
                        return Err(ExecutorError::UnsupportedFeature(
                            "SELECT * requires FROM clause".to_string(),
                        ));
                    }
                }
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, alias } => {
                    // SELECT table.* [AS (col1, col2, ...)] or SELECT alias.* [AS (col1, col2,
                    // ...)] Respects full_column_names PRAGMA
                    if let Some(from_res) = from_result {
                        // Find the table/alias in the schema
                        // TableKey lookup is case-insensitive
                        let result = from_res.schema.get_table(qualifier).cloned();

                        if let Some((_start_index, table_schema)) = result {
                            // Apply derived column list if present
                            if let Some(derived_cols) = alias {
                                if derived_cols.len() != table_schema.columns.len() {
                                    return Err(ExecutorError::ColumnCountMismatch {
                                        expected: table_schema.columns.len(),
                                        provided: derived_cols.len(),
                                    });
                                }
                                column_names.extend(derived_cols.clone());
                            } else {
                                // SELECT table.* always uses just the column name, never
                                // table-qualified This matches
                                // SQLite behavior where full_column_names PRAGMA
                                // does not affect wildcard expansion
                                for col_schema in &table_schema.columns {
                                    column_names.push(col_schema.name.clone());
                                }
                            }
                        } else {
                            return Err(ExecutorError::TableNotFound(qualifier.clone()));
                        }
                    } else {
                        return Err(ExecutorError::UnsupportedFeature(
                            "SELECT table.* without FROM not supported".to_string(),
                        ));
                    }
                }
                vibesql_ast::SelectItem::Expression { expr, alias, source_text, .. } => {
                    // If there's an alias, use it (aliases always take precedence)
                    if let Some(alias_name) = alias {
                        column_names.push(alias_name.clone());
                    } else {
                        column_names.push(derive_expression_name_impl(
                            expr,
                            schema,
                            source_text,
                            mode,
                        ));
                    }
                }
            }
        }

        Ok(column_names)
    }
}

/// Helper function to derive a column name from an expression
///
/// # Arguments
/// * `expr` - The expression to derive a name from
/// * `schema` - Optional schema to use for resolving original column names.
/// * `source_text` - Optional source text from the parser (preserves original formatting)
/// * `mode` - Column naming mode based on PRAGMA settings
fn derive_expression_name_impl(
    expr: &vibesql_ast::Expression,
    schema: Option<&CombinedSchema>,
    source_text: &Option<String>,
    mode: ColumnNamingMode,
) -> String {
    match expr {
        vibesql_ast::Expression::ColumnRef(col_id) => {
            let table = col_id.table_canonical();
            let column = col_id.column_canonical();
            match mode {
                ColumnNamingMode::Full => {
                    // Use schema to get the full column name (table.column format)
                    // with original table name from schema, not the query alias
                    if let Some(s) = schema {
                        s.get_full_column_name(table, column)
                    } else if let Some(t) = table {
                        format!("{}.{}", t, column)
                    } else {
                        column.to_string()
                    }
                }
                ColumnNamingMode::Short => {
                    // Use schema to get just the original column name (preserves case)
                    if let Some(s) = schema {
                        s.get_original_column_name(table, column)
                    } else {
                        column.to_string()
                    }
                }
                ColumnNamingMode::Expression => {
                    // Use the original source text to preserve exact formatting (e.g., "test1 .
                    // f1")
                    if let Some(src) = source_text {
                        src.clone()
                    } else if let Some(s) = schema {
                        s.get_full_column_name(table, column)
                    } else if let Some(t) = table {
                        format!("{}.{}", t, column)
                    } else {
                        column.to_string()
                    }
                }
            }
        }
        vibesql_ast::Expression::Function { name, args, character_unit: _ } => {
            // For functions, use source text in expression mode, otherwise generate name(args)
            // format
            if mode == ColumnNamingMode::Expression {
                if let Some(src) = source_text {
                    return src.clone();
                }
            }
            let args_str = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter()
                    .map(|e| derive_expression_name_impl(e, schema, &None, mode))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            format!("{}({})", name, args_str)
        }
        vibesql_ast::Expression::AggregateFunction { name, distinct, args, .. } => {
            // For aggregate functions, use source text in expression mode, otherwise generate
            // name(args) format
            if mode == ColumnNamingMode::Expression {
                if let Some(src) = source_text {
                    return src.clone();
                }
            }
            let distinct_str = if *distinct { "DISTINCT " } else { "" };
            let args_str = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter()
                    .map(|e| derive_expression_name_impl(e, schema, &None, mode))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            format!("{}({}{})", name, distinct_str, args_str)
        }
        vibesql_ast::Expression::BinaryOp { left, op, right } => {
            // For binary operations, prefer source text to preserve formatting
            if let Some(src) = source_text {
                return src.clone();
            }
            format!(
                "({} {} {})",
                derive_expression_name_impl(left, schema, &None, mode),
                match op {
                    vibesql_ast::BinaryOperator::Plus => "+",
                    vibesql_ast::BinaryOperator::Minus => "-",
                    vibesql_ast::BinaryOperator::Multiply => "*",
                    vibesql_ast::BinaryOperator::Divide => "/",
                    vibesql_ast::BinaryOperator::Equal => "=",
                    vibesql_ast::BinaryOperator::NotEqual => "!=",
                    vibesql_ast::BinaryOperator::LessThan => "<",
                    vibesql_ast::BinaryOperator::LessThanOrEqual => "<=",
                    vibesql_ast::BinaryOperator::GreaterThan => ">",
                    vibesql_ast::BinaryOperator::GreaterThanOrEqual => ">=",
                    vibesql_ast::BinaryOperator::And => "AND",
                    vibesql_ast::BinaryOperator::Or => "OR",
                    vibesql_ast::BinaryOperator::Concat => "||",
                    _ => "?",
                },
                derive_expression_name_impl(right, schema, &None, mode)
            )
        }
        vibesql_ast::Expression::Wildcard => {
            // For wildcard (*), return "*" - used in COUNT(*) and similar
            "*".to_string()
        }
        vibesql_ast::Expression::Literal(val) => {
            // For literals, use a clean string representation
            match val {
                vibesql_types::SqlValue::Integer(n) => n.to_string(),
                vibesql_types::SqlValue::Smallint(n) => n.to_string(),
                vibesql_types::SqlValue::Bigint(n) => n.to_string(),
                vibesql_types::SqlValue::Unsigned(n) => n.to_string(),
                vibesql_types::SqlValue::Double(f) => f.to_string(),
                vibesql_types::SqlValue::Float(f) => f.to_string(),
                vibesql_types::SqlValue::Real(f) => f.to_string(),
                vibesql_types::SqlValue::Numeric(f) => f.to_string(),
                vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                    format!("'{}'", s)
                }
                vibesql_types::SqlValue::Boolean(b) => b.to_string(),
                vibesql_types::SqlValue::Date(d) => format!("'{}'", d),
                vibesql_types::SqlValue::Time(t) => format!("'{}'", t),
                vibesql_types::SqlValue::Timestamp(ts) => format!("'{}'", ts),
                vibesql_types::SqlValue::Interval(i) => format!("INTERVAL '{}'", i),
                vibesql_types::SqlValue::Vector(v) => {
                    let formatted: Vec<String> = v.iter().map(|f| f.to_string()).collect();
                    format!("[{}]", formatted.join(", "))
                }
                vibesql_types::SqlValue::Blob(b) => {
                    let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                    format!("x'{}'", hex)
                }
                vibesql_types::SqlValue::Null => "NULL".to_string(),
            }
        }
        _ => {
            // For other expression types, use source text if available
            if let Some(src) = source_text {
                src.clone()
            } else {
                "?column?".to_string()
            }
        }
    }
}
