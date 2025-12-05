//! Derived table (subquery) scanning logic
//!
//! Handles execution of subqueries in FROM clauses (derived tables)
//! by executing the subquery and wrapping it with an alias.

use crate::{errors::ExecutorError, schema::CombinedSchema, select::SelectResult};

/// Derive a column name from an expression (simplified version from columns.rs)
fn derive_column_name_from_expr(expr: &vibesql_ast::Expression) -> String {
    match expr {
        vibesql_ast::Expression::ColumnRef { table: _, column } => column.clone(),
        vibesql_ast::Expression::Function { name, args, character_unit: _ } => {
            let args_str = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter().map(derive_column_name_from_expr).collect::<Vec<_>>().join(", ")
            };
            format!("{}({})", name, args_str)
        }
        vibesql_ast::Expression::AggregateFunction { name, distinct, args } => {
            let distinct_str = if *distinct { "DISTINCT " } else { "" };
            let args_str = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter().map(derive_column_name_from_expr).collect::<Vec<_>>().join(", ")
            };
            format!("{}({}{})", name, distinct_str, args_str)
        }
        vibesql_ast::Expression::BinaryOp { left, op, right } => {
            format!(
                "({} {} {})",
                derive_column_name_from_expr(left),
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
                derive_column_name_from_expr(right)
            )
        }
        vibesql_ast::Expression::Literal(val) => match val {
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
        },
        _ => "?column?".to_string(),
    }
}

/// Execute a derived table (subquery with alias)
///
/// SQL:1999 Feature E051-09: Supports optional column renaming via `column_aliases`.
/// When provided, the derived table's columns are renamed to the specified aliases,
/// allowing syntax like: `FROM (SELECT a, b FROM t) AS mytemp (x, y)`
pub(crate) fn execute_derived_table<F>(
    query: &vibesql_ast::SelectStmt,
    alias: &str,
    column_aliases: Option<&Vec<String>>,
    execute_subquery: F,
) -> Result<super::FromResult, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<SelectResult, ExecutorError>,
{
    // Execute subquery to get rows and column names
    let subquery_result = execute_subquery(query)?;
    let rows = subquery_result.rows;
    let subquery_columns = subquery_result.columns;

    // Derive schema from SELECT list
    let mut column_names = Vec::new();
    let mut column_types = Vec::new();

    let mut col_index = 0;
    for item in &query.select_list {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. }
            | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                // For SELECT * or SELECT table.*, use the column names from the subquery result
                // This preserves the actual column names instead of generating generic ones
                if let Some(first_row) = rows.first() {
                    for (j, value) in first_row.values.iter().enumerate() {
                        // Use actual column name from subquery if available
                        let col_name = subquery_columns
                            .get(col_index + j)
                            .cloned()
                            .unwrap_or_else(|| format!("column{}", col_index + j + 1));
                        column_names.push(col_name);
                        column_types.push(value.get_type());
                    }
                    col_index += first_row.values.len();
                } else {
                    // No rows - use column names from subquery metadata
                    for col_name in &subquery_columns {
                        column_names.push(col_name.clone());
                        column_types.push(vibesql_types::DataType::Null);
                    }
                }
            }
            vibesql_ast::SelectItem::Expression { expr, alias: col_alias } => {
                // Use alias if provided, otherwise derive from expression
                let col_name = if let Some(a) = col_alias {
                    a.clone()
                } else {
                    derive_column_name_from_expr(expr)
                };
                column_names.push(col_name);

                // Infer type from first row if available
                let col_type = if let Some(first_row) = rows.first() {
                    if col_index < first_row.values.len() {
                        first_row.values[col_index].get_type()
                    } else {
                        vibesql_types::DataType::Null
                    }
                } else {
                    vibesql_types::DataType::Null
                };
                column_types.push(col_type);
                col_index += 1;
            }
        }
    }

    // SQL:1999 E051-09: Apply column aliases if provided
    // This allows renaming derived table columns without modifying the inner query:
    // FROM (SELECT a, b FROM t) AS mytemp (x, y)  -- columns a,b become x,y
    if let Some(aliases) = column_aliases {
        if aliases.len() != column_names.len() {
            return Err(ExecutorError::ColumnCountMismatch {
                expected: column_names.len(),
                provided: aliases.len(),
            });
        }
        column_names = aliases.clone();
    }

    // Create schema with table alias
    let schema = CombinedSchema::from_derived_table(alias.to_string(), column_names, column_types);

    Ok(super::FromResult::from_rows(schema, rows))
}
