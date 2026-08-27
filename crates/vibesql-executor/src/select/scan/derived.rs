//! Derived table (subquery) scanning logic
//!
//! Handles execution of subqueries in FROM clauses (derived tables)
//! by executing the subquery and wrapping it with an alias.

use crate::{errors::ExecutorError, schema::CombinedSchema, select::SelectResult};

/// Determine the `DataType` a compound SELECT (UNION/INTERSECT/EXCEPT) should
/// stamp on its derived-table column at `idx`, for affinity purposes.
///
/// SQLite gives a compound-select column the *shared* affinity of its
/// branches when every branch agrees, and no affinity (BLOB/None) when they
/// disagree — never simply "whichever type the first row happens to have".
/// For example `SELECT a FROM int_tbl UNION ALL SELECT d FROM
/// other_int_tbl` (both branches INTEGER) keeps INTEGER affinity end to end
/// (window_pushdown.rs's `union_all_text_literal_vs_numeric_branch_affinity`,
/// issue #5749), while `SELECT id FROM int_tbl UNION SELECT id FROM
/// text_tbl` (branches disagree) gets no affinity at all (issue #6172,
/// affinity3.test 210/250). Since the AST doesn't expose each branch's
/// statically-resolved affinity at this call site, approximate the same
/// distinction from the already-executed result: scan every row's actual
/// runtime value at this column position (skipping NULLs, which carry no
/// affinity information and are not part of SQLite's static per-branch
/// affinity determination either) and require them to agree on affinity.
pub(super) fn compound_column_data_type(
    rows: &[vibesql_storage::Row],
    idx: usize,
) -> vibesql_types::DataType {
    use vibesql_types::{SqlValue, TypeAffinity};

    let mut representative: Option<vibesql_types::DataType> = None;
    let mut representative_affinity: Option<TypeAffinity> = None;
    for row in rows {
        let Some(value) = row.values.get(idx) else { continue };
        if matches!(value, SqlValue::Null) {
            continue;
        }
        let data_type = value.get_type();
        let affinity = data_type.sqlite_affinity();
        match representative_affinity {
            None => {
                representative_affinity = Some(affinity);
                representative = Some(data_type);
            }
            Some(existing) if existing == affinity => {}
            Some(_) => return vibesql_types::DataType::BinaryLargeObject,
        }
    }
    representative.unwrap_or(vibesql_types::DataType::BinaryLargeObject)
}

/// Derive a column name from an expression (simplified version from columns.rs)
fn derive_column_name_from_expr(expr: &vibesql_ast::Expression) -> String {
    match expr {
        vibesql_ast::Expression::ColumnRef(col_id) => col_id.column_canonical().to_string(),
        vibesql_ast::Expression::Function { name, args, character_unit: _ } => {
            let args_str = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter().map(derive_column_name_from_expr).collect::<Vec<_>>().join(", ")
            };
            format!("{}({})", name, args_str)
        }
        vibesql_ast::Expression::AggregateFunction { name, distinct, args, .. } => {
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
            vibesql_types::SqlValue::Blob(b) => {
                let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                format!("x'{}'", hex)
            }
            vibesql_types::SqlValue::Null => "NULL".to_string(),
        },
        vibesql_ast::Expression::Wildcard => "*".to_string(),
        // COLLATE is a transparent wrapper for naming purposes: SQLite's
        // sqlite3ColumnsFromExprList() skips TK_COLLATE when deriving result-column
        // names, so `SELECT x COLLATE rtrim` in a derived table yields a column
        // named `x`. Recurse to handle nested COLLATE wrappers too.
        vibesql_ast::Expression::Collate { expr, .. } => derive_column_name_from_expr(expr),
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

    // A compound SELECT (UNION/INTERSECT/EXCEPT)'s result columns need
    // `compound_column_data_type`'s shared-or-none affinity treatment rather
    // than blindly trusting row 1's runtime type (which only reflects
    // whichever branch happened to produce the first row — see that
    // function's doc comment for the two issue #6172/#5749 cases this
    // distinguishes).
    let is_compound = query.set_operation.is_some();

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
                        column_types.push(if is_compound {
                            compound_column_data_type(&rows, col_index + j)
                        } else {
                            value.get_type()
                        });
                    }
                    col_index += first_row.values.len();
                } else {
                    // No rows - use column names from subquery metadata
                    for col_name in &subquery_columns {
                        column_names.push(col_name.to_string());
                        column_types.push(vibesql_types::DataType::Null);
                    }
                }
            }
            vibesql_ast::SelectItem::Expression { expr, alias: col_alias, .. } => {
                // Use alias if provided, otherwise derive from expression
                let col_name = if let Some(a) = col_alias {
                    a.clone()
                } else {
                    derive_column_name_from_expr(expr)
                };
                column_names.push(col_name);

                // Infer type from first row if available (except for a
                // compound select — see `compound_column_data_type`).
                let col_type = if is_compound {
                    compound_column_data_type(&rows, col_index)
                } else if let Some(first_row) = rows.first() {
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

#[cfg(test)]
mod tests {
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

    use super::compound_column_data_type;

    fn rows(values: Vec<SqlValue>) -> Vec<Row> {
        values.into_iter().map(|v| Row::new(vec![v])).collect()
    }

    /// Every branch agrees on affinity → keep it (window_pushdown.rs's
    /// `union_all_text_literal_vs_numeric_branch_affinity`, issue #5749).
    #[test]
    fn all_rows_same_affinity_keeps_that_affinity() {
        let dt =
            compound_column_data_type(&rows(vec![SqlValue::Integer(1), SqlValue::Integer(2)]), 0);
        assert_eq!(dt, DataType::Integer);

        let dt = compound_column_data_type(
            &rows(vec![SqlValue::Varchar("a".into()), SqlValue::Varchar("b".into())]),
            0,
        );
        assert_eq!(dt.sqlite_affinity(), vibesql_types::TypeAffinity::Text);
    }

    /// Branches disagree on affinity → no affinity at all (issue #6172,
    /// affinity3.test 210/250).
    #[test]
    fn mixed_affinity_rows_yield_no_affinity() {
        let dt = compound_column_data_type(
            &rows(vec![SqlValue::Integer(9), SqlValue::Varchar("7".into())]),
            0,
        );
        assert_eq!(dt, DataType::BinaryLargeObject);
        assert_eq!(dt.sqlite_affinity(), vibesql_types::TypeAffinity::None);
    }

    /// NULLs carry no affinity information and must not decide the outcome —
    /// they are skipped, not treated as a disagreeing branch.
    #[test]
    fn nulls_are_skipped() {
        let dt = compound_column_data_type(
            &rows(vec![SqlValue::Null, SqlValue::Integer(1), SqlValue::Null]),
            0,
        );
        assert_eq!(dt, DataType::Integer);
    }

    /// An all-NULL (or empty) column has nothing to infer from → no affinity.
    #[test]
    fn all_null_or_empty_yields_no_affinity() {
        assert_eq!(
            compound_column_data_type(&rows(vec![SqlValue::Null, SqlValue::Null]), 0),
            DataType::BinaryLargeObject
        );
        assert_eq!(compound_column_data_type(&[], 0), DataType::BinaryLargeObject);
    }

    /// A row too short for the requested column position is skipped rather
    /// than panicking.
    #[test]
    fn short_rows_are_skipped() {
        let rows = vec![Row::new(vec![SqlValue::Integer(1)]), Row::new(vec![])];
        assert_eq!(compound_column_data_type(&rows, 0), DataType::Integer);
    }
}
