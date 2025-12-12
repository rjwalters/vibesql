//! Column name derivation for SELECT results

use super::builder::SelectExecutor;
use crate::{errors::ExecutorError, schema::CombinedSchema, select::join::FromResult};

impl SelectExecutor<'_> {
    /// Derive column names from SELECT list
    ///
    /// Column names follow SQLite's `full_column_names=ON` behavior:
    /// - For wildcards (`SELECT *`), columns are prefixed with the table alias/name
    ///   from the FROM clause (e.g., `a.f1` when using `FROM test1 a`)
    /// - For explicit column references, columns are prefixed with the original
    ///   table name from the schema (e.g., `test1.f1` even when using alias `a`)
    /// - Explicit aliases always take precedence
    pub(super) fn derive_column_names(
        &self,
        select_list: &[vibesql_ast::SelectItem],
        from_result: Option<&FromResult>,
    ) -> Result<Vec<String>, ExecutorError> {
        self.derive_column_names_internal(select_list, from_result, true)
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
        self.derive_column_names_internal(select_list, from_result, false)
    }

    /// Internal implementation that can optionally include table prefixes
    fn derive_column_names_internal(
        &self,
        select_list: &[vibesql_ast::SelectItem],
        from_result: Option<&FromResult>,
        include_table_prefix: bool,
    ) -> Result<Vec<String>, ExecutorError> {
        let mut column_names = Vec::new();
        let schema = from_result.map(|fr| &fr.schema);

        for item in select_list {
            match item {
                vibesql_ast::SelectItem::Wildcard { alias } => {
                    // SELECT * [AS (col1, col2, ...)] - expand to all column names from schema
                    if let Some(from_res) = from_result {
                        // Get all column names in order from the combined schema
                        let mut table_columns: Vec<(usize, String)> = Vec::new();

                        for (table_key, (start_index, table_schema)) in
                            &from_res.schema.table_schemas
                        {
                            for (col_idx, col_schema) in table_schema.columns.iter().enumerate() {
                                let col_name = if include_table_prefix {
                                    // Use the table key (alias or table name) as the prefix
                                    format!("{}.{}", table_key.as_str(), col_schema.name.clone())
                                } else {
                                    col_schema.name.clone()
                                };
                                table_columns.push((start_index + col_idx, col_name));
                            }
                        }

                        // Sort by index to maintain column order
                        table_columns.sort_by_key(|(idx, _)| *idx);

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
                            for (_, name) in table_columns {
                                column_names.push(name);
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
                    // ...)]
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
                                // Add all column names from this table in order
                                for col_schema in &table_schema.columns {
                                    let col_name = if include_table_prefix {
                                        // Use the qualifier (alias or table name) as the prefix
                                        format!("{}.{}", qualifier, col_schema.name.clone())
                                    } else {
                                        col_schema.name.clone()
                                    };
                                    column_names.push(col_name);
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
                    // If there's an alias, use it
                    if let Some(alias_name) = alias {
                        column_names.push(alias_name.clone());
                    } else if matches!(expr, vibesql_ast::Expression::ColumnRef { .. }) {
                        // For simple column references, use full table.column format
                        // when include_table_prefix is true
                        column_names
                            .push(derive_expression_name_impl(expr, schema, include_table_prefix));
                    } else if let Some(src) = source_text {
                        // For complex expressions, use original source text
                        // (e.g., "f1+F2" preserves the exact expression text)
                        column_names.push(src.clone());
                    } else {
                        // Derive name from the expression, using schema to preserve original case
                        column_names
                            .push(derive_expression_name_impl(expr, schema, include_table_prefix));
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
///   When provided, ColumnRef expressions will use the schema's full column name
///   (table.column format with original table name) instead of the parsed identifier.
/// * `include_table_prefix` - Whether to include the table prefix for column references
fn derive_expression_name_impl(
    expr: &vibesql_ast::Expression,
    schema: Option<&CombinedSchema>,
    include_table_prefix: bool,
) -> String {
    match expr {
        vibesql_ast::Expression::ColumnRef { table, column } => {
            if include_table_prefix {
                // Use schema to get the full column name (table.column format)
                // with original table name from schema, not the query alias
                if let Some(s) = schema {
                    s.get_full_column_name(table.as_deref(), column)
                } else {
                    column.clone()
                }
            } else {
                // Use schema to get just the original column name (preserves case)
                if let Some(s) = schema {
                    s.get_original_column_name(table.as_deref(), column)
                } else {
                    column.clone()
                }
            }
        }
        vibesql_ast::Expression::Function { name, args, character_unit: _ } => {
            // For functions, use name(args) format
            let args_str = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter()
                    .map(|e| derive_expression_name_impl(e, schema, include_table_prefix))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            format!("{}({})", name, args_str)
        }
        vibesql_ast::Expression::AggregateFunction { name, distinct, args } => {
            // For aggregate functions, use name(DISTINCT args) format
            let distinct_str = if *distinct { "DISTINCT " } else { "" };
            let args_str = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter()
                    .map(|e| derive_expression_name_impl(e, schema, include_table_prefix))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            format!("{}({}{})", name, distinct_str, args_str)
        }
        vibesql_ast::Expression::BinaryOp { left, op, right } => {
            // For binary operations, create descriptive name
            format!(
                "({} {} {})",
                derive_expression_name_impl(left, schema, include_table_prefix),
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
                derive_expression_name_impl(right, schema, include_table_prefix)
            )
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
                vibesql_types::SqlValue::Null => "NULL".to_string(),
            }
        }
        _ => "?column?".to_string(), // Default for other expression types
    }
}
