//! Column reference extraction and validation
//!
//! Validates column references in SELECT list and WHERE clause expressions
//! against the available schema BEFORE row processing begins. This ensures
//! that column errors are caught even when tables are empty.

use vibesql_ast::Expression;

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Represents a column reference extracted from an expression
#[derive(Debug)]
pub struct ColumnReference {
    /// Optional table qualifier
    pub table: Option<String>,
    /// Column name
    pub column: String,
}

/// Extract all column references from an expression recursively
pub fn extract_column_refs(expr: &Expression, refs: &mut Vec<ColumnReference>) {
    match expr {
        Expression::ColumnRef(col_id) => {
            // Skip "*" - it's a wildcard, not an actual column reference
            // This handles cases like COUNT(*) parsed as ColumnRef { column: "*" }
            let column = col_id.column_canonical();
            if column != "*" {
                refs.push(ColumnReference {
                    table: col_id.table_canonical().map(|t| t.to_string()),
                    column: column.to_string(),
                });
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            extract_column_refs(left, refs);
            extract_column_refs(right, refs);
        }
        Expression::UnaryOp { expr, .. } => {
            extract_column_refs(expr, refs);
        }
        Expression::Function { args, .. } => {
            for arg in args {
                extract_column_refs(arg, refs);
            }
        }
        Expression::AggregateFunction { args, .. } => {
            for arg in args {
                extract_column_refs(arg, refs);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                extract_column_refs(op, refs);
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    extract_column_refs(cond, refs);
                }
                extract_column_refs(&case_when.result, refs);
            }
            if let Some(else_expr) = else_result {
                extract_column_refs(else_expr, refs);
            }
        }
        Expression::IsNull { expr, .. } => {
            extract_column_refs(expr, refs);
        }
        Expression::IsDistinctFrom { left, right, .. } => {
            extract_column_refs(left, refs);
            extract_column_refs(right, refs);
        }
        Expression::IsTruthValue { expr, .. } => {
            extract_column_refs(expr, refs);
        }
        Expression::Between { expr, low, high, .. } => {
            extract_column_refs(expr, refs);
            extract_column_refs(low, refs);
            extract_column_refs(high, refs);
        }
        Expression::InList { expr, values, .. } => {
            extract_column_refs(expr, refs);
            for val in values {
                extract_column_refs(val, refs);
            }
        }
        Expression::In { expr, .. } => {
            // Only validate the left-hand expression
            // Subquery columns are validated separately when executing the subquery
            extract_column_refs(expr, refs);
        }
        Expression::Exists { .. } => {
            // EXISTS subqueries are validated separately
        }
        Expression::Cast { expr, .. } => {
            extract_column_refs(expr, refs);
        }
        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            extract_column_refs(expr, refs);
            extract_column_refs(pattern, refs);
        }
        Expression::Position { substring, string, .. } => {
            extract_column_refs(substring, refs);
            extract_column_refs(string, refs);
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(char_expr) = removal_char {
                extract_column_refs(char_expr, refs);
            }
            extract_column_refs(string, refs);
        }
        Expression::Extract { expr, .. } => {
            extract_column_refs(expr, refs);
        }
        Expression::ScalarSubquery(_) => {
            // Scalar subquery columns are validated when executing the subquery
        }
        Expression::QuantifiedComparison { expr, .. } => {
            extract_column_refs(expr, refs);
        }
        Expression::Interval { value, .. } => {
            extract_column_refs(value, refs);
        }
        Expression::WindowFunction { function, over } => {
            // Extract from window function arguments
            match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
                    for arg in args {
                        extract_column_refs(arg, refs);
                    }
                }
            }
            // Extract from PARTITION BY
            if let Some(partition) = &over.partition_by {
                for expr in partition {
                    extract_column_refs(expr, refs);
                }
            }
            // Extract from ORDER BY
            if let Some(order) = &over.order_by {
                for item in order {
                    extract_column_refs(&item.expr, refs);
                }
            }
        }
        Expression::MatchAgainst { search_modifier, .. } => {
            extract_column_refs(search_modifier, refs);
        }
        Expression::PseudoVariable { .. } => {
            // OLD/NEW pseudo-variables in triggers - skip validation
        }
        Expression::Conjunction(children)
        | Expression::Disjunction(children)
        | Expression::RowValueConstructor(children) => {
            for child in children {
                extract_column_refs(child, refs);
            }
        }

        Expression::Collate { expr, .. } => {
            extract_column_refs(expr, refs);
        }

        Expression::Raise { error_message, .. } => {
            if let Some(msg) = error_message {
                extract_column_refs(msg, refs);
            }
        }

        // Terminals with no column references
        Expression::Literal(_)
        | Expression::CollatedLiteral { .. }
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::DuplicateKeyValue { .. }
        | Expression::NextValue { .. }
        | Expression::SessionVariable { .. }
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_) => {}
    }
}

/// Check if a column name is a ROWID pseudo-column alias (SQLite compatibility)
/// Returns true for 'rowid', '_rowid_', and 'oid' (case-insensitive)
pub fn is_rowid_pseudo_column(column: &str) -> bool {
    let lower = column.to_lowercase();
    lower == "rowid" || lower == "_rowid_" || lower == "oid"
}

/// Validate a single column reference against the schema (and optionally outer schema)
pub fn validate_column_ref(
    col_ref: &ColumnReference,
    schema: &CombinedSchema,
    outer_schema: Option<&CombinedSchema>,
) -> Result<(), ExecutorError> {
    // Check for ambiguous unqualified column references (SQLite compatibility)
    // This must be checked BEFORE resolving the column, as SQLite requires
    // an error when a column name exists in multiple joined tables.
    // Only applies to unqualified references - qualified ones (e.g., t1.col) are never ambiguous.
    if col_ref.table.is_none() && schema.is_column_ambiguous(&col_ref.column) {
        return Err(ExecutorError::AmbiguousColumnName { column_name: col_ref.column.clone() });
    }

    // Check if column exists in inner schema
    if schema.get_column_index(col_ref.table.as_deref(), &col_ref.column).is_some() {
        return Ok(());
    }

    // SQLite compatibility: Allow ROWID pseudo-column references
    // Only if there's no actual column with that name (real columns take precedence)
    // WITHOUT ROWID tables do NOT have the rowid pseudo-column (Issue #4953)
    if is_rowid_pseudo_column(&col_ref.column) {
        // Verify the qualifier matches a table in the schema (if qualified)
        if let Some(ref qualifier) = col_ref.table {
            let qualifier_lower = qualifier.to_lowercase();

            // Check if the qualified table is a WITHOUT ROWID table or a VIEW.
            // Neither exposes the rowid pseudo-column: WITHOUT ROWID tables have
            // no rowid (#4953), and views have no implicit rowid at all (#5492).
            for (table_id, (_, table_schema)) in &schema.table_schemas {
                if table_id.canonical() == qualifier_lower
                    && (table_schema.without_rowid || table_schema.is_view)
                {
                    return Err(ExecutorError::ColumnNotFound {
                        column_name: col_ref.column.clone(),
                        table_name: qualifier.clone(),
                        searched_tables: vec![qualifier.clone()],
                        available_columns: table_schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    });
                }
            }

            let table_exists =
                schema.table_schemas.keys().any(|k| k.canonical() == qualifier_lower);
            let table_in_outer = outer_schema.is_some_and(|outer| {
                outer.table_schemas.keys().any(|k| k.canonical() == qualifier_lower)
            });
            if table_exists || table_in_outer {
                return Ok(());
            }
        } else {
            // Unqualified ROWID is valid if there's at least one table in scope
            // AND none of the tables are WITHOUT ROWID or a VIEW (neither
            // exposes a rowid pseudo-column — #4953 / #5492).
            if !schema.table_schemas.is_empty() {
                // Check if any table in scope is WITHOUT ROWID or a VIEW.
                let has_without_rowid_table =
                    schema.table_schemas.values().any(|(_, ts)| ts.without_rowid || ts.is_view);
                if has_without_rowid_table {
                    // Get the first WITHOUT ROWID table or VIEW for error message
                    if let Some((table_id, (_, table_schema))) = schema
                        .table_schemas
                        .iter()
                        .find(|(_, (_, ts))| ts.without_rowid || ts.is_view)
                    {
                        return Err(ExecutorError::ColumnNotFound {
                            column_name: col_ref.column.clone(),
                            table_name: table_id.display().to_string(),
                            searched_tables: schema.table_names(),
                            available_columns: table_schema
                                .columns
                                .iter()
                                .map(|c| c.name.clone())
                                .collect(),
                        });
                    }
                }
                return Ok(());
            }
        }
    }

    // For correlated subqueries, also check outer schema (#2694)
    if let Some(outer) = outer_schema {
        if outer.get_column_index(col_ref.table.as_deref(), &col_ref.column).is_some() {
            return Ok(());
        }
    }

    // Column not found - build error with context
    let mut searched_tables: Vec<String> = if let Some(ref table) = col_ref.table {
        // If qualified, only report that table
        vec![table.clone()]
    } else {
        // If unqualified, list all tables that were searched
        schema.table_names()
    };

    // Collect available columns for suggestions (from both schemas)
    let mut available_columns: Vec<String> = schema
        .table_schemas
        .values()
        .flat_map(|(_, table_schema)| table_schema.columns.iter().map(|c| c.name.clone()))
        .collect();

    // Include outer schema tables and columns in error message
    if let Some(outer) = outer_schema {
        if col_ref.table.is_none() {
            searched_tables.extend(outer.table_names());
        }
        available_columns.extend(
            outer
                .table_schemas
                .values()
                .flat_map(|(_, table_schema)| table_schema.columns.iter().map(|c| c.name.clone())),
        );
    }

    Err(ExecutorError::ColumnNotFound {
        column_name: col_ref.column.clone(),
        table_name: col_ref.table.clone().unwrap_or_else(|| {
            // Use the first table name if no qualifier was provided
            searched_tables.first().cloned().unwrap_or_else(|| "unknown".to_string())
        }),
        searched_tables,
        available_columns,
    })
}

#[cfg(test)]
mod tests {
    use vibesql_ast::ColumnIdentifier;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use super::*;

    fn make_columns() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            ColumnSchema {
                name: "NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
        ]
    }

    fn make_test_schema() -> CombinedSchema {
        let table_schema = TableSchema::new("PRODUCTS".to_string(), make_columns());
        CombinedSchema::from_table("PRODUCTS".to_string(), table_schema)
    }

    fn make_view_schema() -> CombinedSchema {
        let mut view_schema = TableSchema::new("V1".to_string(), make_columns());
        view_schema.set_is_view(true);
        CombinedSchema::from_table("V1".to_string(), view_schema)
    }

    #[test]
    fn test_rowid_resolves_on_table() {
        // Base tables expose the rowid pseudo-column (and its aliases).
        let schema = make_test_schema();
        for name in ["rowid", "oid", "_rowid_", "ROWID"] {
            let col_ref = ColumnReference { table: None, column: name.to_string() };
            assert!(
                validate_column_ref(&col_ref, &schema, None).is_ok(),
                "expected `{name}` to resolve on a base table"
            );
            let qualified =
                ColumnReference { table: Some("PRODUCTS".to_string()), column: name.to_string() };
            assert!(
                validate_column_ref(&qualified, &schema, None).is_ok(),
                "expected `PRODUCTS.{name}` to resolve on a base table"
            );
        }
    }

    #[test]
    fn test_rowid_errors_on_view() {
        // Views have no implicit rowid: rowid/oid/_rowid_ must error (#5492).
        let schema = make_view_schema();
        for name in ["rowid", "oid", "_rowid_", "ROWID"] {
            let col_ref = ColumnReference { table: None, column: name.to_string() };
            assert!(
                matches!(
                    validate_column_ref(&col_ref, &schema, None),
                    Err(ExecutorError::ColumnNotFound { .. })
                ),
                "expected unqualified `{name}` to error on a view"
            );
            let qualified =
                ColumnReference { table: Some("V1".to_string()), column: name.to_string() };
            assert!(
                matches!(
                    validate_column_ref(&qualified, &schema, None),
                    Err(ExecutorError::ColumnNotFound { .. })
                ),
                "expected qualified `V1.{name}` to error on a view"
            );
        }
    }

    #[test]
    fn test_view_column_named_rowid_resolves() {
        // A view that explicitly exposes a real column named "rowid" resolves to
        // that column rather than erroring (matches sqlite3 3.51.0).
        let mut cols = make_columns();
        cols[0].name = "ROWID".to_string();
        let mut view_schema = TableSchema::new("V2".to_string(), cols);
        view_schema.set_is_view(true);
        let schema = CombinedSchema::from_table("V2".to_string(), view_schema);
        let col_ref = ColumnReference { table: None, column: "rowid".to_string() };
        assert!(validate_column_ref(&col_ref, &schema, None).is_ok());
    }

    #[test]
    fn test_extract_column_refs_simple() {
        let expr = Expression::ColumnRef(ColumnIdentifier::simple("id", false));
        let mut refs = Vec::new();
        extract_column_refs(&expr, &mut refs);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].column, "id");
        assert!(refs[0].table.is_none());
    }

    #[test]
    fn test_extract_column_refs_qualified() {
        let expr =
            Expression::ColumnRef(ColumnIdentifier::qualified("products", false, "id", false));
        let mut refs = Vec::new();
        extract_column_refs(&expr, &mut refs);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].column, "id");
        assert_eq!(refs[0].table, Some("products".to_string()));
    }

    #[test]
    fn test_is_rowid_pseudo_column() {
        assert!(is_rowid_pseudo_column("rowid"));
        assert!(is_rowid_pseudo_column("ROWID"));
        assert!(is_rowid_pseudo_column("_rowid_"));
        assert!(is_rowid_pseudo_column("oid"));
        assert!(!is_rowid_pseudo_column("id"));
        assert!(!is_rowid_pseudo_column("row_id"));
    }

    #[test]
    fn test_validate_column_ref_valid() {
        let schema = make_test_schema();
        let col_ref = ColumnReference { table: None, column: "id".to_string() };
        let result = validate_column_ref(&col_ref, &schema, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_column_ref_invalid() {
        let schema = make_test_schema();
        let col_ref = ColumnReference { table: None, column: "invalid_column".to_string() };
        let result = validate_column_ref(&col_ref, &schema, None);
        assert!(result.is_err());
        match result {
            Err(ExecutorError::ColumnNotFound { column_name, available_columns, .. }) => {
                assert_eq!(column_name, "invalid_column");
                assert!(available_columns.contains(&"ID".to_string()));
                assert!(available_columns.contains(&"NAME".to_string()));
            }
            _ => panic!("Expected ColumnNotFound error"),
        }
    }

    #[test]
    fn test_validate_qualified_column_ref() {
        let schema = make_test_schema();
        let col_ref =
            ColumnReference { table: Some("products".to_string()), column: "id".to_string() };
        let result = validate_column_ref(&col_ref, &schema, None);
        assert!(result.is_ok());
    }
}
