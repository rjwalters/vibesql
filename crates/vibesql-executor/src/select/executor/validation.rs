//! Column validation for SELECT statements
//!
//! Validates column references in SELECT list and WHERE clause expressions
//! against the available schema BEFORE row processing begins. This ensures
//! that column errors are caught even when tables are empty.

#![allow(clippy::collapsible_if)]

use vibesql_ast::{Expression, FromClause, SelectItem, SelectStmt};

use crate::{errors::ExecutorError, limits::MAX_TABLES_IN_JOIN, schema::CombinedSchema};

/// Count the number of tables in a FROM clause
///
/// This recursively counts all table references in the FROM clause,
/// including tables in JOIN operations and subqueries.
///
/// SQLite enforces a limit of 64 tables in a single join.
fn count_tables_in_from_clause(from: &FromClause) -> usize {
    match from {
        FromClause::Table { .. } => 1,
        FromClause::Join { left, right, .. } => {
            count_tables_in_from_clause(left) + count_tables_in_from_clause(right)
        }
        FromClause::Subquery { .. } => 1, // Subquery counts as 1 table
        FromClause::Values { .. } => 1,   // VALUES clause counts as 1 table
    }
}

/// Validate that the number of tables in a join doesn't exceed SQLite's limit
///
/// SQLite enforces a limit of 64 tables in a single join operation.
/// This validation catches queries like `SELECT * FROM t, t, t, ... (65+ times)`
/// before execution begins.
///
/// Returns an error if the table count exceeds MAX_TABLES_IN_JOIN (64).
pub fn validate_join_table_limit(stmt: &SelectStmt) -> Result<(), ExecutorError> {
    if let Some(from_clause) = &stmt.from {
        let table_count = count_tables_in_from_clause(from_clause);
        if table_count > MAX_TABLES_IN_JOIN {
            return Err(ExecutorError::JoinTableLimitExceeded {
                table_count,
                max_tables: MAX_TABLES_IN_JOIN,
            });
        }
    }
    Ok(())
}

/// Represents a column reference extracted from an expression
#[derive(Debug)]
struct ColumnReference {
    /// Optional table qualifier
    table: Option<String>,
    /// Column name
    column: String,
}

/// Extract all column references from an expression recursively
fn extract_column_refs(expr: &Expression, refs: &mut Vec<ColumnReference>) {
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

        // Terminals with no column references
        Expression::Literal(_)
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
fn is_rowid_pseudo_column(column: &str) -> bool {
    let lower = column.to_lowercase();
    lower == "rowid" || lower == "_rowid_" || lower == "oid"
}

/// Check if a function name is an aggregate function
fn is_aggregate_function(name: &str) -> bool {
    let upper = name.to_uppercase();
    matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "TOTAL" | "GROUP_CONCAT")
}

/// Check if an aggregate function has wrong number of arguments
/// Returns Some((function_name, arg_count)) if there's an error, None otherwise
fn check_aggregate_arg_count(expr: &Expression) -> Option<String> {
    match expr {
        Expression::AggregateFunction { name, args, distinct, .. } => {
            let upper = name.to_uppercase();
            let arg_count = args.len();

            // Check for wildcard in non-COUNT aggregates
            let has_wildcard = args.iter().any(|arg| {
                let is_wildcard = matches!(arg, Expression::Wildcard);
                let is_star_ref = matches!(
                    arg,
                    Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() && col_id.column_canonical() == "*"
                );
                is_wildcard || is_star_ref
            });

            match upper.as_str() {
                "COUNT" => {
                    // Multi-arg COUNT without DISTINCT is an error
                    // SQLite: "wrong number of arguments to function count()"
                    if arg_count > 1 && !*distinct {
                        Some(name.to_string())
                    } else {
                        None
                    }
                }
                "MIN" | "MAX" => {
                    if has_wildcard || arg_count == 0 {
                        Some(name.to_string()) // Preserve original case for SQLite-compatible error
                    } else {
                        None
                    }
                }
                "SUM" | "AVG" | "TOTAL" => {
                    if has_wildcard || arg_count == 0 || arg_count > 1 {
                        Some(name.to_string()) // Preserve original case for SQLite-compatible error
                    } else {
                        None
                    }
                }
                "GROUP_CONCAT" => {
                    if arg_count == 0 || arg_count > 2 {
                        Some(name.to_string()) // Preserve original case for SQLite-compatible error
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
        Expression::Function { name, args, .. } => {
            // Check if this is an aggregate function with wrong args
            if is_aggregate_function(name.as_str()) {
                let upper = name.to_uppercase();
                let arg_count = args.len();

                // Check for wildcard
                let has_wildcard = args.iter().any(|arg| {
                    matches!(arg, Expression::Wildcard)
                        || matches!(
                            arg,
                            Expression::ColumnRef(col_id) if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() && col_id.column_canonical() == "*"
                        )
                });

                match upper.as_str() {
                    "COUNT" => {
                        // count(a, b) without DISTINCT is wrong
                        // Regular count without DISTINCT can only have 0-1 args
                        if arg_count > 1 {
                            Some(name.to_string()) // Preserve original case
                        } else {
                            None
                        }
                    }
                    "MIN" | "MAX" => {
                        // Multi-arg min/max are scalar, so only check single arg case
                        if arg_count <= 1 && (has_wildcard || arg_count == 0) {
                            Some(name.to_string()) // Preserve original case
                        } else {
                            None
                        }
                    }
                    "SUM" | "AVG" | "TOTAL" => {
                        if has_wildcard || arg_count == 0 || arg_count > 1 {
                            Some(name.to_string()) // Preserve original case
                        } else {
                            None
                        }
                    }
                    "GROUP_CONCAT" => {
                        if arg_count == 0 || arg_count > 2 {
                            Some(name.to_string()) // Preserve original case
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            } else {
                // Check function arguments recursively
                for arg in args {
                    if let Some(found) = check_aggregate_arg_count(arg) {
                        return Some(found);
                    }
                }
                None
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            check_aggregate_arg_count(left).or_else(|| check_aggregate_arg_count(right))
        }
        Expression::UnaryOp { expr, .. } => check_aggregate_arg_count(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(found) = check_aggregate_arg_count(op) {
                    return Some(found);
                }
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    if let Some(found) = check_aggregate_arg_count(cond) {
                        return Some(found);
                    }
                }
                if let Some(found) = check_aggregate_arg_count(&case_when.result) {
                    return Some(found);
                }
            }
            if let Some(else_expr) = else_result {
                check_aggregate_arg_count(else_expr)
            } else {
                None
            }
        }
        Expression::IsNull { expr, .. } => check_aggregate_arg_count(expr),
        Expression::Cast { expr, .. } => check_aggregate_arg_count(expr),
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(found) = check_aggregate_arg_count(child) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

/// Find the first aggregate function in an expression
/// Returns the function name (original case preserved) if found, None otherwise
fn find_aggregate_in_expression(expr: &Expression) -> Option<String> {
    match expr {
        Expression::AggregateFunction { name, .. } => Some(name.to_string()), // Preserve original case
        Expression::Function { name, args, .. } => {
            // Check if this function is a built-in aggregate
            // Note: MIN/MAX with multiple args are scalar functions in SQLite
            if is_aggregate_function(name.as_str()) {
                let upper = name.to_uppercase();
                if matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1 {
                    // Multi-arg min/max are scalar, not aggregate
                    None
                } else {
                    Some(name.to_string()) // Preserve original case
                }
            } else {
                // Check function arguments recursively
                for arg in args {
                    if let Some(found) = find_aggregate_in_expression(arg) {
                        return Some(found);
                    }
                }
                None
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            find_aggregate_in_expression(left).or_else(|| find_aggregate_in_expression(right))
        }
        Expression::UnaryOp { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(found) = find_aggregate_in_expression(op) {
                    return Some(found);
                }
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    if let Some(found) = find_aggregate_in_expression(cond) {
                        return Some(found);
                    }
                }
                if let Some(found) = find_aggregate_in_expression(&case_when.result) {
                    return Some(found);
                }
            }
            if let Some(else_expr) = else_result {
                find_aggregate_in_expression(else_expr)
            } else {
                None
            }
        }
        Expression::IsNull { expr, .. } => find_aggregate_in_expression(expr),
        Expression::IsDistinctFrom { left, right, .. } => {
            find_aggregate_in_expression(left).or_else(|| find_aggregate_in_expression(right))
        }
        Expression::IsTruthValue { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Between { expr, low, high, .. } => find_aggregate_in_expression(expr)
            .or_else(|| find_aggregate_in_expression(low))
            .or_else(|| find_aggregate_in_expression(high)),
        Expression::InList { expr, values, .. } => {
            if let Some(found) = find_aggregate_in_expression(expr) {
                return Some(found);
            }
            for val in values {
                if let Some(found) = find_aggregate_in_expression(val) {
                    return Some(found);
                }
            }
            None
        }
        Expression::In { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Exists { .. } => None, // EXISTS subqueries have their own scope
        Expression::Cast { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Like { expr, pattern, .. } => {
            find_aggregate_in_expression(expr).or_else(|| find_aggregate_in_expression(pattern))
        }
        Expression::Position { substring, string, .. } => {
            find_aggregate_in_expression(substring).or_else(|| find_aggregate_in_expression(string))
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(char_expr) = removal_char {
                if let Some(found) = find_aggregate_in_expression(char_expr) {
                    return Some(found);
                }
            }
            find_aggregate_in_expression(string)
        }
        Expression::Extract { expr, .. } => find_aggregate_in_expression(expr),
        Expression::ScalarSubquery(_) => None, // Subqueries have their own scope
        Expression::QuantifiedComparison { expr, .. } => find_aggregate_in_expression(expr),
        Expression::Interval { value, .. } => find_aggregate_in_expression(value),
        Expression::WindowFunction { .. } => None, // Window functions are not regular aggregates
        Expression::MatchAgainst { search_modifier, .. } => {
            find_aggregate_in_expression(search_modifier)
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(found) = find_aggregate_in_expression(child) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

/// Find nested aggregate function in an expression
///
/// A nested aggregate is when one aggregate's arguments contain another aggregate,
/// e.g., `SUM(MIN(x))`. This is invalid in SQL.
///
/// Returns Some(inner_aggregate_name) if found, None otherwise.
fn find_nested_aggregate(expr: &Expression) -> Option<String> {
    match expr {
        Expression::AggregateFunction { args, .. } => {
            // Check if any argument contains an aggregate function
            for arg in args {
                if let Some(inner_name) = find_aggregate_in_expression(arg) {
                    return Some(inner_name);
                }
            }
            None
        }
        Expression::Function { name, args, .. } => {
            // Check if this function is a built-in aggregate with nested aggregate args
            if is_aggregate_function(name.as_str()) {
                let upper = name.to_uppercase();
                // Multi-arg MIN/MAX are scalar functions, not aggregates
                let is_scalar_minmax = matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1;
                if !is_scalar_minmax {
                    // This is an aggregate - check for nested aggregates in args
                    for arg in args {
                        if let Some(inner_name) = find_aggregate_in_expression(arg) {
                            return Some(inner_name);
                        }
                    }
                }
            }
            // Check arguments recursively (for non-aggregate functions)
            for arg in args {
                if let Some(found) = find_nested_aggregate(arg) {
                    return Some(found);
                }
            }
            None
        }
        Expression::BinaryOp { left, right, .. } => {
            find_nested_aggregate(left).or_else(|| find_nested_aggregate(right))
        }
        Expression::UnaryOp { expr, .. } => find_nested_aggregate(expr),
        Expression::Cast { expr, .. } => find_nested_aggregate(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(found) = find_nested_aggregate(op) {
                    return Some(found);
                }
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    if let Some(found) = find_nested_aggregate(cond) {
                        return Some(found);
                    }
                }
                if let Some(found) = find_nested_aggregate(&case_when.result) {
                    return Some(found);
                }
            }
            if let Some(else_expr) = else_result {
                find_nested_aggregate(else_expr)
            } else {
                None
            }
        }
        Expression::IsNull { expr, .. } => find_nested_aggregate(expr),
        Expression::Between { expr, low, high, .. } => find_nested_aggregate(expr)
            .or_else(|| find_nested_aggregate(low))
            .or_else(|| find_nested_aggregate(high)),
        Expression::InList { expr, values, .. } => {
            if let Some(found) = find_nested_aggregate(expr) {
                return Some(found);
            }
            for val in values {
                if let Some(found) = find_nested_aggregate(val) {
                    return Some(found);
                }
            }
            None
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(found) = find_nested_aggregate(child) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}

/// Validate a single column reference against the schema (and optionally outer schema)
fn validate_column_ref(
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
    if is_rowid_pseudo_column(&col_ref.column) {
        // Verify the qualifier matches a table in the schema (if qualified)
        if let Some(ref qualifier) = col_ref.table {
            let qualifier_lower = qualifier.to_lowercase();
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
            if !schema.table_schemas.is_empty() {
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

/// Validate all column references with optional procedural context and outer schema
///
/// When a procedural context is provided, variable names from the context are
/// allowed as column references (they will be resolved at runtime).
///
/// When an outer_schema is provided (for correlated subqueries), column references
/// are also validated against the outer schema. This fixes issue #2694 where
/// correlated subqueries failed to resolve outer table references during validation.
pub fn validate_select_columns_with_context(
    select_list: &[SelectItem],
    where_clause: Option<&Expression>,
    schema: &CombinedSchema,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    outer_schema: Option<&CombinedSchema>,
) -> Result<(), ExecutorError> {
    // Collect procedure variable names if in procedural context
    let proc_vars: std::collections::HashSet<String> = procedural_context
        .map(|ctx| {
            ctx.get_available_names()
                .into_iter()
                .flat_map(|name| vec![name.clone(), name.to_lowercase()])
                .collect()
        })
        .unwrap_or_default();

    // Collect SELECT aliases (SQLite extension: allow aliases in WHERE clause)
    let select_aliases: std::collections::HashSet<String> = select_list
        .iter()
        .filter_map(|item| {
            if let SelectItem::Expression { alias: Some(alias), .. } = item {
                Some(vec![alias.clone(), alias.to_lowercase()])
            } else {
                None
            }
        })
        .flatten()
        .collect();

    // Combine procedure variables and SELECT aliases for WHERE clause validation
    let allowed_in_where: std::collections::HashSet<String> =
        proc_vars.union(&select_aliases).cloned().collect();

    let mut column_refs = Vec::new();
    let mut where_column_refs = Vec::new();

    // Extract column references from SELECT list
    for item in select_list {
        match item {
            SelectItem::Expression { expr, .. } => {
                extract_column_refs(expr, &mut column_refs);
            }
            SelectItem::Wildcard { .. } => {
                // Wildcard doesn't reference specific columns
            }
            SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Validate that the qualifier matches a known table (check both schemas)
                let qualifier_lower = qualifier.to_lowercase();
                let table_in_inner =
                    schema.table_schemas.keys().any(|k| k.canonical() == qualifier_lower);
                let table_in_outer = outer_schema.is_some_and(|outer| {
                    outer.table_schemas.keys().any(|k| k.canonical() == qualifier_lower)
                });

                if !table_in_inner && !table_in_outer {
                    let mut available_tables: Vec<String> = schema.table_names();
                    if let Some(outer) = outer_schema {
                        available_tables.extend(outer.table_names());
                    }
                    return Err(ExecutorError::InvalidTableQualifier {
                        qualifier: qualifier.clone(),
                        column: "*".to_string(),
                        available_tables,
                    });
                }
            }
        }
    }

    // Extract column references from WHERE clause (tracked separately for alias resolution)
    if let Some(where_expr) = where_clause {
        extract_column_refs(where_expr, &mut where_column_refs);

        // Check for wrong argument count FIRST (takes priority over misuse)
        // SQLite reports arg count errors before context errors
        if let Some(agg_name) = check_aggregate_arg_count(where_expr) {
            return Err(ExecutorError::WrongNumberOfArguments { function_name: agg_name });
        }

        // Check for aggregate functions in WHERE clause (misuse of aggregate)
        if let Some(agg_name) = find_aggregate_in_expression(where_expr) {
            return Err(ExecutorError::MisuseOfAggregate { function_name: agg_name });
        }
    }

    // Validate SELECT list column references (only procedure variables allowed)
    for col_ref in &column_refs {
        // Skip validation for procedure variables (unqualified only)
        if col_ref.table.is_none() {
            if proc_vars.contains(&col_ref.column)
                || proc_vars.contains(&col_ref.column.to_lowercase())
            {
                continue;
            }
        }
        validate_column_ref(col_ref, schema, outer_schema)?;
    }

    // Validate WHERE clause column references (aliases + procedure variables allowed)
    log::debug!("Validating WHERE: allowed_in_where={:?}", allowed_in_where);
    for col_ref in &where_column_refs {
        // Skip validation for allowed names (procedure variables and SELECT aliases)
        if col_ref.table.is_none() {
            if allowed_in_where.contains(&col_ref.column)
                || allowed_in_where.contains(&col_ref.column.to_lowercase())
            {
                log::debug!("WHERE validation: allowing alias '{}'", col_ref.column);
                continue;
            }
        }
        log::debug!("WHERE validation: checking column '{}' against schema", col_ref.column);
        validate_column_ref(col_ref, schema, outer_schema)?;
    }

    Ok(())
}

/// Validate aggregate function argument counts in SELECT list
///
/// This validates that aggregate functions have the correct number of arguments:
/// - MIN, MAX, SUM, AVG, TOTAL require exactly 1 argument (no wildcard)
/// - COUNT allows 0-1 arguments (supports *)
/// - GROUP_CONCAT requires 1-2 arguments
///
/// Returns an error with SQLite-compatible message if validation fails.
pub fn validate_aggregate_arguments(select_list: &[SelectItem]) -> Result<(), ExecutorError> {
    for item in select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if let Some(agg_name) = check_aggregate_arg_count(expr) {
                return Err(ExecutorError::WrongNumberOfArguments { function_name: agg_name });
            }
        }
    }
    Ok(())
}

/// Validate that there are no nested aggregate functions in the SELECT list
///
/// Nested aggregates like `SUM(MIN(x))` are invalid in SQL.
/// Returns an error with SQLite-compatible message if nested aggregates are found.
///
/// Note: This uses the "misuse of aggregate function X()" format (with "function")
/// as SQLite detects this during name resolution, not during execution.
pub fn validate_no_nested_aggregates(select_list: &[SelectItem]) -> Result<(), ExecutorError> {
    for item in select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if let Some(inner_agg_name) = find_nested_aggregate(expr) {
                return Err(ExecutorError::MisuseOfAggregate { function_name: inner_agg_name });
            }
        }
    }
    Ok(())
}

/// Validate column references in SELECT list and WHERE clause against schema
///
/// Simple validation without procedural context - used for standard SELECT queries.
#[cfg(test)]
pub fn validate_select_columns(
    select_list: &[SelectItem],
    where_clause: Option<&Expression>,
    schema: &CombinedSchema,
) -> Result<(), ExecutorError> {
    validate_select_columns_with_context(select_list, where_clause, schema, None, None)
}

/// Build a set of aggregate alias names from the SELECT list
///
/// An "aggregate alias" is an alias that refers to an expression containing
/// an aggregate function, e.g., `min(f1) AS m` makes `m` an aggregate alias.
fn build_aggregate_aliases(select_list: &[SelectItem]) -> std::collections::HashSet<String> {
    let mut aliases = std::collections::HashSet::new();

    for item in select_list {
        if let SelectItem::Expression { expr, alias: Some(alias_name), .. } = item {
            // Check if this expression contains an aggregate
            if expression_contains_aggregate(expr) {
                // Store alias in lowercase for case-insensitive matching
                aliases.insert(alias_name.to_lowercase());
            }
        }
    }

    aliases
}

/// Check if an expression contains an aggregate function
fn expression_contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { .. } => true,
        Expression::Function { name, args, .. } => {
            // Check if this function is a built-in aggregate
            if is_aggregate_function(name.as_str()) {
                let upper = name.to_uppercase();
                // Multi-arg MIN/MAX are scalar functions
                if matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1 {
                    // Still check arguments for nested aggregates
                    args.iter().any(expression_contains_aggregate)
                } else {
                    true
                }
            } else {
                // Check function arguments
                args.iter().any(expression_contains_aggregate)
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            expression_contains_aggregate(left) || expression_contains_aggregate(right)
        }
        Expression::UnaryOp { expr, .. } => expression_contains_aggregate(expr),
        Expression::Cast { expr, .. } => expression_contains_aggregate(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_contains_aggregate(e))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(expression_contains_aggregate)
                        || expression_contains_aggregate(&w.result)
                })
                || else_result.as_ref().is_some_and(|e| expression_contains_aggregate(e))
        }
        Expression::IsNull { expr, .. } => expression_contains_aggregate(expr),
        Expression::Between { expr, low, high, .. } => {
            expression_contains_aggregate(expr)
                || expression_contains_aggregate(low)
                || expression_contains_aggregate(high)
        }
        Expression::InList { expr, values, .. } => {
            expression_contains_aggregate(expr) || values.iter().any(expression_contains_aggregate)
        }
        Expression::In { expr, .. } => expression_contains_aggregate(expr),
        Expression::Like { expr, pattern, .. } => {
            expression_contains_aggregate(expr) || expression_contains_aggregate(pattern)
        }
        Expression::Position { substring, string, .. } => {
            expression_contains_aggregate(substring) || expression_contains_aggregate(string)
        }
        Expression::Trim { removal_char, string, .. } => {
            removal_char.as_ref().is_some_and(|e| expression_contains_aggregate(e))
                || expression_contains_aggregate(string)
        }
        Expression::Extract { expr, .. } => expression_contains_aggregate(expr),
        Expression::Interval { value, .. } => expression_contains_aggregate(value),
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            children.iter().any(expression_contains_aggregate)
        }
        // Subqueries have their own scope
        Expression::ScalarSubquery(_) | Expression::Exists { .. } => false,
        // Window functions are not aggregates in this context
        Expression::WindowFunction { .. } => false,
        // Other expressions don't contain aggregates
        _ => false,
    }
}

/// Check for misuse of aliased aggregates in HAVING clause
///
/// SQLite error: When an aggregate alias (e.g., `m` from `min(f1) AS m`) is used
/// inside another aggregate function in the HAVING clause (e.g., `HAVING max(m) < 10`),
/// it's an error. This function detects such misuse.
///
/// The `schema_columns` set contains column names from the actual table schema.
/// If a column reference matches an actual table column, it's NOT a reference to an alias,
/// even if an alias with the same name exists.
///
/// Returns Some(alias_name) if misuse is found, None otherwise.
fn find_aliased_aggregate_misuse_in_expression(
    expr: &Expression,
    aggregate_aliases: &std::collections::HashSet<String>,
    schema_columns: &std::collections::HashSet<String>,
    inside_aggregate: bool,
) -> Option<String> {
    match expr {
        // Check if this is an aggregate function - if so, mark that we're inside one
        Expression::AggregateFunction { args, .. } => {
            for arg in args {
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    arg,
                    aggregate_aliases,
                    schema_columns,
                    true,
                ) {
                    return Some(alias);
                }
            }
            None
        }
        Expression::Function { name, args, .. } => {
            // Check if this function is a built-in aggregate
            let is_agg = is_aggregate_function(name.as_str());
            let upper = name.to_uppercase();
            // Multi-arg MIN/MAX are scalar functions
            let effectively_aggregate =
                is_agg && !(matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1);

            let new_inside_aggregate = inside_aggregate || effectively_aggregate;

            for arg in args {
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    arg,
                    aggregate_aliases,
                    schema_columns,
                    new_inside_aggregate,
                ) {
                    return Some(alias);
                }
            }
            None
        }
        // Column reference - check if it's an aggregate alias used inside an aggregate
        Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            let column = col_id.column_canonical();
            // If this column exists in the actual table schema, it's a real column reference,
            // not a reference to a SELECT alias (even if an alias with the same name exists).
            // Table columns take precedence over aliases in HAVING clause.
            if schema_columns.contains(&column.to_lowercase()) {
                return None; // Real column, not an alias reference
            }

            if inside_aggregate && aggregate_aliases.contains(&column.to_lowercase()) {
                // Found misuse: aggregate alias used inside another aggregate
                Some(column.to_string())
            } else {
                None
            }
        }
        Expression::ColumnRef(_) => None, // Qualified refs can't be aliases
        // Recursively check composite expressions
        Expression::BinaryOp { left, right, .. } => find_aliased_aggregate_misuse_in_expression(
            left,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        )
        .or_else(|| {
            find_aliased_aggregate_misuse_in_expression(
                right,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
        }),
        Expression::UnaryOp { expr, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        Expression::Cast { expr, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    op,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                ) {
                    return Some(alias);
                }
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                        cond,
                        aggregate_aliases,
                        schema_columns,
                        inside_aggregate,
                    ) {
                        return Some(alias);
                    }
                }
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    &when_clause.result,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                ) {
                    return Some(alias);
                }
            }
            if let Some(else_expr) = else_result {
                return find_aliased_aggregate_misuse_in_expression(
                    else_expr,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                );
            }
            None
        }
        Expression::IsNull { expr, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        Expression::Between { expr, low, high, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        )
        .or_else(|| {
            find_aliased_aggregate_misuse_in_expression(
                low,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
        })
        .or_else(|| {
            find_aliased_aggregate_misuse_in_expression(
                high,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
        }),
        Expression::InList { expr, values, .. } => {
            if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                expr,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            ) {
                return Some(alias);
            }
            for val in values {
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    val,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                ) {
                    return Some(alias);
                }
            }
            None
        }
        Expression::In { expr, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        Expression::Like { expr, pattern, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        )
        .or_else(|| {
            find_aliased_aggregate_misuse_in_expression(
                pattern,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
        }),
        Expression::Position { substring, string, .. } => {
            find_aliased_aggregate_misuse_in_expression(
                substring,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
            .or_else(|| {
                find_aliased_aggregate_misuse_in_expression(
                    string,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                )
            })
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(rc) = removal_char {
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    rc,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                ) {
                    return Some(alias);
                }
            }
            find_aliased_aggregate_misuse_in_expression(
                string,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
        }
        Expression::Extract { expr, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        Expression::Interval { value, .. } => find_aliased_aggregate_misuse_in_expression(
            value,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(alias) = find_aliased_aggregate_misuse_in_expression(
                    child,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                ) {
                    return Some(alias);
                }
            }
            None
        }
        // Subqueries have their own scope
        Expression::ScalarSubquery(_) | Expression::Exists { .. } => None,
        Expression::QuantifiedComparison { expr, .. } => {
            find_aliased_aggregate_misuse_in_expression(
                expr,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
        }
        Expression::IsDistinctFrom { left, right, .. } => {
            find_aliased_aggregate_misuse_in_expression(
                left,
                aggregate_aliases,
                schema_columns,
                inside_aggregate,
            )
            .or_else(|| {
                find_aliased_aggregate_misuse_in_expression(
                    right,
                    aggregate_aliases,
                    schema_columns,
                    inside_aggregate,
                )
            })
        }
        Expression::IsTruthValue { expr, .. } => find_aliased_aggregate_misuse_in_expression(
            expr,
            aggregate_aliases,
            schema_columns,
            inside_aggregate,
        ),
        // Other expressions don't contain column refs that could be aggregate aliases
        _ => None,
    }
}

/// Validate HAVING clause for misuse of aliased aggregates
///
/// This should be called after building the aggregate aliases from the SELECT list.
/// Returns an error if an aggregate alias is used inside another aggregate in HAVING.
///
/// The `schema` parameter provides the actual table columns. If a column reference
/// matches an actual table column, it's not considered an alias reference, even if
/// an alias with the same name exists in the SELECT list.
pub fn validate_having_aliased_aggregates(
    having_clause: Option<&Expression>,
    select_list: &[SelectItem],
    schema: &CombinedSchema,
) -> Result<(), ExecutorError> {
    let Some(having_expr) = having_clause else {
        return Ok(());
    };

    // Build the set of aggregate aliases
    let aggregate_aliases = build_aggregate_aliases(select_list);

    if aggregate_aliases.is_empty() {
        return Ok(()); // No aggregate aliases to check
    }

    // Build the set of actual table column names (lowercase for case-insensitive matching)
    let schema_columns: std::collections::HashSet<String> = schema
        .table_schemas
        .values()
        .flat_map(|(_, table_schema)| table_schema.columns.iter().map(|c| c.name.to_lowercase()))
        .collect();

    // Check for misuse in HAVING clause
    if let Some(alias_name) = find_aliased_aggregate_misuse_in_expression(
        having_expr,
        &aggregate_aliases,
        &schema_columns,
        false,
    ) {
        return Err(ExecutorError::MisuseOfAliasedAggregate { alias_name });
    }

    Ok(())
}

/// Check if an expression contains a column reference that belongs to the outer schema
/// (not the subquery's own tables)
///
/// Returns Some(column_name) if an outer column is found, None otherwise.
fn find_outer_column_in_expression(
    expr: &Expression,
    subquery_tables: &[String],
    inner_schema: Option<&CombinedSchema>,
    outer_schema: &CombinedSchema,
) -> Option<String> {
    match expr {
        Expression::ColumnRef(col_id) => {
            let table = col_id.table_canonical();
            let column = col_id.column_canonical();

            // Skip wildcard
            if column == "*" {
                return None;
            }

            if let Some(table_name) = table {
                // Qualified column reference - check if table is in subquery's tables
                let table_lower = table_name.to_lowercase();
                if !subquery_tables.iter().any(|t| t.to_lowercase() == table_lower) {
                    // Not in subquery's tables - this is an outer column
                    if outer_schema.get_column_index(Some(table_name), column).is_some() {
                        return Some(column.to_string());
                    }
                }
            } else {
                // Unqualified column reference - check if it's in outer schema but NOT in inner
                // First check if it's in the inner schema (subquery's tables)
                let in_inner = inner_schema
                    .map(|s| s.get_column_index(None, column).is_some())
                    .unwrap_or(false);

                if !in_inner {
                    // Not in inner schema - check if it's in outer schema
                    if outer_schema.get_column_index(None, column).is_some() {
                        return Some(column.to_string());
                    }
                }
            }
            None
        }
        Expression::BinaryOp { left, right, .. } => {
            find_outer_column_in_expression(left, subquery_tables, inner_schema, outer_schema)
                .or_else(|| find_outer_column_in_expression(right, subquery_tables, inner_schema, outer_schema))
        }
        Expression::UnaryOp { expr, .. } => {
            find_outer_column_in_expression(expr, subquery_tables, inner_schema, outer_schema)
        }
        Expression::Function { args, .. } => {
            for arg in args {
                if let Some(col) = find_outer_column_in_expression(arg, subquery_tables, inner_schema, outer_schema)
                {
                    return Some(col);
                }
            }
            None
        }
        Expression::Cast { expr, .. } => {
            find_outer_column_in_expression(expr, subquery_tables, inner_schema, outer_schema)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(col) =
                    find_outer_column_in_expression(op, subquery_tables, inner_schema, outer_schema)
                {
                    return Some(col);
                }
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    if let Some(col) =
                        find_outer_column_in_expression(cond, subquery_tables, inner_schema, outer_schema)
                    {
                        return Some(col);
                    }
                }
                if let Some(col) =
                    find_outer_column_in_expression(&when_clause.result, subquery_tables, inner_schema, outer_schema)
                {
                    return Some(col);
                }
            }
            if let Some(else_expr) = else_result {
                find_outer_column_in_expression(else_expr, subquery_tables, inner_schema, outer_schema)
            } else {
                None
            }
        }
        Expression::IsNull { expr, .. } => {
            find_outer_column_in_expression(expr, subquery_tables, inner_schema, outer_schema)
        }
        Expression::Between { expr, low, high, .. } => {
            find_outer_column_in_expression(expr, subquery_tables, inner_schema, outer_schema)
                .or_else(|| find_outer_column_in_expression(low, subquery_tables, inner_schema, outer_schema))
                .or_else(|| find_outer_column_in_expression(high, subquery_tables, inner_schema, outer_schema))
        }
        Expression::InList { expr, values, .. } => {
            if let Some(col) =
                find_outer_column_in_expression(expr, subquery_tables, inner_schema, outer_schema)
            {
                return Some(col);
            }
            for val in values {
                if let Some(col) =
                    find_outer_column_in_expression(val, subquery_tables, inner_schema, outer_schema)
                {
                    return Some(col);
                }
            }
            None
        }
        Expression::Like { expr, pattern, .. } => {
            find_outer_column_in_expression(expr, subquery_tables, inner_schema, outer_schema)
                .or_else(|| find_outer_column_in_expression(pattern, subquery_tables, inner_schema, outer_schema))
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(col) =
                    find_outer_column_in_expression(child, subquery_tables, inner_schema, outer_schema)
                {
                    return Some(col);
                }
            }
            None
        }
        // Terminals and subqueries (have their own scope)
        _ => None,
    }
}

/// Check if an aggregate function's arguments reference columns from the outer schema
///
/// Returns Some(aggregate_name) if misuse is found, None otherwise.
fn find_aggregate_with_outer_column(
    expr: &Expression,
    subquery_tables: &[String],
    inner_schema: Option<&CombinedSchema>,
    outer_schema: &CombinedSchema,
) -> Option<String> {
    match expr {
        Expression::AggregateFunction { name, args, .. } => {
            // Check if any argument references an outer column
            for arg in args {
                if find_outer_column_in_expression(arg, subquery_tables, inner_schema, outer_schema).is_some() {
                    return Some(name.to_string());
                }
            }
            None
        }
        Expression::Function { name, args, .. } => {
            // Check if this is an aggregate function
            if is_aggregate_function(name.as_str()) {
                let upper = name.to_uppercase();
                // Multi-arg MIN/MAX are scalar functions
                if matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1 {
                    // Scalar function - check args recursively
                    for arg in args {
                        if let Some(found) =
                            find_aggregate_with_outer_column(arg, subquery_tables, inner_schema, outer_schema)
                        {
                            return Some(found);
                        }
                    }
                    None
                } else {
                    // Aggregate function - check if args reference outer columns
                    for arg in args {
                        if find_outer_column_in_expression(arg, subquery_tables, inner_schema, outer_schema)
                            .is_some()
                        {
                            return Some(name.to_string());
                        }
                    }
                    None
                }
            } else {
                // Non-aggregate function - check args recursively
                for arg in args {
                    if let Some(found) =
                        find_aggregate_with_outer_column(arg, subquery_tables, inner_schema, outer_schema)
                    {
                        return Some(found);
                    }
                }
                None
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            find_aggregate_with_outer_column(left, subquery_tables, inner_schema, outer_schema)
                .or_else(|| find_aggregate_with_outer_column(right, subquery_tables, inner_schema, outer_schema))
        }
        Expression::UnaryOp { expr, .. } => {
            find_aggregate_with_outer_column(expr, subquery_tables, inner_schema, outer_schema)
        }
        Expression::Cast { expr, .. } => {
            find_aggregate_with_outer_column(expr, subquery_tables, inner_schema, outer_schema)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if let Some(found) =
                    find_aggregate_with_outer_column(op, subquery_tables, inner_schema, outer_schema)
                {
                    return Some(found);
                }
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    if let Some(found) =
                        find_aggregate_with_outer_column(cond, subquery_tables, inner_schema, outer_schema)
                    {
                        return Some(found);
                    }
                }
                if let Some(found) =
                    find_aggregate_with_outer_column(&when_clause.result, subquery_tables, inner_schema, outer_schema)
                {
                    return Some(found);
                }
            }
            if let Some(else_expr) = else_result {
                find_aggregate_with_outer_column(else_expr, subquery_tables, inner_schema, outer_schema)
            } else {
                None
            }
        }
        Expression::IsNull { expr, .. } => {
            find_aggregate_with_outer_column(expr, subquery_tables, inner_schema, outer_schema)
        }
        Expression::Between { expr, low, high, .. } => {
            find_aggregate_with_outer_column(expr, subquery_tables, inner_schema, outer_schema)
                .or_else(|| find_aggregate_with_outer_column(low, subquery_tables, inner_schema, outer_schema))
                .or_else(|| find_aggregate_with_outer_column(high, subquery_tables, inner_schema, outer_schema))
        }
        Expression::InList { expr, values, .. } => {
            if let Some(found) =
                find_aggregate_with_outer_column(expr, subquery_tables, inner_schema, outer_schema)
            {
                return Some(found);
            }
            for val in values {
                if let Some(found) =
                    find_aggregate_with_outer_column(val, subquery_tables, inner_schema, outer_schema)
                {
                    return Some(found);
                }
            }
            None
        }
        Expression::Like { expr, pattern, .. } => {
            find_aggregate_with_outer_column(expr, subquery_tables, inner_schema, outer_schema)
                .or_else(|| find_aggregate_with_outer_column(pattern, subquery_tables, inner_schema, outer_schema))
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                if let Some(found) =
                    find_aggregate_with_outer_column(child, subquery_tables, inner_schema, outer_schema)
                {
                    return Some(found);
                }
            }
            None
        }
        // Subqueries have their own scope
        Expression::ScalarSubquery(_) | Expression::Exists { .. } | Expression::In { .. } => None,
        _ => None,
    }
}

/// Extract table names from a FROM clause
fn extract_table_names(from: Option<&vibesql_ast::FromClause>) -> Vec<String> {
    let mut tables = Vec::new();
    if let Some(from_clause) = from {
        extract_table_names_recursive(from_clause, &mut tables);
    }
    tables
}

/// Recursively extract table names from a FROM clause
fn extract_table_names_recursive(from: &vibesql_ast::FromClause, tables: &mut Vec<String>) {
    match from {
        vibesql_ast::FromClause::Table { name, alias, .. } => {
            tables.push(alias.clone().unwrap_or_else(|| name.clone()));
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            extract_table_names_recursive(left, tables);
            extract_table_names_recursive(right, tables);
        }
        vibesql_ast::FromClause::Subquery { alias, .. } => {
            tables.push(alias.clone());
        }
        vibesql_ast::FromClause::Values { alias, .. } => {
            tables.push(alias.clone());
        }
    }
}

/// Build a CombinedSchema from table names in a FROM clause
///
/// Looks up each table in the database and builds a schema containing all columns.
fn build_schema_from_tables(
    tables: &[String],
    database: &vibesql_storage::Database,
) -> Option<CombinedSchema> {
    if tables.is_empty() {
        return None;
    }

    let mut total_columns = 0;
    let mut table_schemas =
        std::collections::HashMap::<vibesql_catalog::TableIdentifier, (usize, vibesql_catalog::TableSchema)>::new();

    for table_name in tables {
        // Skip subquery aliases - they don't have schemas in the database
        // and we can't validate against them without executing the subquery
        if let Some(table) = database.get_table(table_name) {
            let table_schema = table.schema.clone();
            let table_id = vibesql_catalog::TableIdentifier::unquoted(table_name);
            table_schemas.insert(table_id, (total_columns, table_schema.clone()));
            total_columns += table_schema.columns.len();
        }
    }

    if table_schemas.is_empty() {
        None
    } else {
        Some(CombinedSchema {
            table_schemas,
            total_columns,
            hidden_columns: std::collections::HashSet::new(),
            outer_schema: None,
            duplicate_aliases: std::collections::HashSet::new(),
            joined_columns: std::collections::HashSet::new(),
        })
    }
}

/// Validate that aggregates in a subquery's SELECT list don't reference outer columns
///
/// When a subquery contains an aggregate function whose arguments reference columns
/// from an outer query (not from the subquery's own tables), it's a misuse of aggregate.
/// SQLite returns "misuse of aggregate: X()" for this case.
///
/// Example:
/// ```sql
/// SELECT max((SELECT count(x) FROM t35b)) FROM t35a;
/// -- Error: x is from outer t35a, not from t35b
/// ```
///
/// Returns an error if an aggregate references an outer column.
///
/// Note: This validation is skipped if the subquery's FROM clause contains CTEs or
/// subquery aliases, since we can't reliably determine the inner schema without
/// executing the CTE/subquery first.
pub fn validate_no_aggregate_with_outer_column(
    stmt: &vibesql_ast::SelectStmt,
    outer_schema: &CombinedSchema,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    // Get the tables in the subquery's FROM clause
    let subquery_tables = extract_table_names(stmt.from.as_ref());

    // Build the inner schema from the FROM clause tables
    let inner_schema = build_schema_from_tables(&subquery_tables, database);

    // If we have tables in FROM but couldn't build an inner schema,
    // those tables are likely CTEs or subquery aliases. Skip validation
    // since we can't reliably determine if a column is from inner or outer scope.
    if !subquery_tables.is_empty() && inner_schema.is_none() {
        return Ok(());
    }

    // Check each item in the select list
    for item in &stmt.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if let Some(agg_name) =
                find_aggregate_with_outer_column(expr, &subquery_tables, inner_schema.as_ref(), outer_schema)
            {
                return Err(ExecutorError::MisuseOfAggregateContext {
                    function_name: agg_name,
                });
            }
        }
    }

    // Also check HAVING clause
    if let Some(having) = &stmt.having {
        if let Some(agg_name) =
            find_aggregate_with_outer_column(having, &subquery_tables, inner_schema.as_ref(), outer_schema)
        {
            return Err(ExecutorError::MisuseOfAggregateContext {
                function_name: agg_name,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use super::*;

    fn make_test_schema() -> CombinedSchema {
        let columns = vec![
            ColumnSchema {
                name: "ID".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                default_value: None,
                generated_expr: None,
                collation: None,
            },
            ColumnSchema {
                name: "NAME".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
            },
        ];
        let table_schema = TableSchema::new("PRODUCTS".to_string(), columns);
        CombinedSchema::from_table("PRODUCTS".to_string(), table_schema)
    }

    #[test]
    fn test_valid_column_ref() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("id", false)),
            alias: None,
            source_text: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_column_ref() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "invalid_column",
                false,
            )),
            alias: None,
            source_text: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
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
    fn test_qualified_column_ref() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "products", false, "id", false,
            )),
            alias: None,
            source_text: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_qualified_column_ref() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "products",
                false,
                "invalid_column",
                false,
            )),
            alias: None,
            source_text: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
        assert!(result.is_err());
        match result {
            Err(ExecutorError::ColumnNotFound { column_name, .. }) => {
                assert_eq!(column_name, "invalid_column");
            }
            _ => panic!("Expected ColumnNotFound error"),
        }
    }

    #[test]
    fn test_column_in_expression() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Expression {
            expr: Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Plus,
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "invalid_col",
                    false,
                ))),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(1))),
            },
            alias: None,
            source_text: None,
        }];

        let result = validate_select_columns(&select_list, None, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_where_clause_validation() {
        let schema = make_test_schema();
        let select_list = vec![SelectItem::Wildcard { alias: None }];
        let where_clause = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "nonexistent",
                false,
            ))),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(1))),
        };

        let result = validate_select_columns(&select_list, Some(&where_clause), &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_min_star_invalid() {
        // MIN(*) should be invalid - returns error with function name (preserving original case)
        let expr = Expression::AggregateFunction {
            name: vibesql_ast::FunctionIdentifier::new("MIN"),
            distinct: false,
            args: vec![Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("*", false))],
            order_by: None,
            filter: None,
        };
        let result = check_aggregate_arg_count(&expr);
        assert!(result.is_some(), "MIN(*) should be invalid");
        assert_eq!(result.unwrap(), "MIN"); // Preserves original case
    }

    #[test]
    fn test_max_star_invalid() {
        // MAX(*) should be invalid
        let expr = Expression::AggregateFunction {
            name: vibesql_ast::FunctionIdentifier::new("MAX"),
            distinct: false,
            args: vec![Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("*", false))],
            order_by: None,
            filter: None,
        };
        let result = check_aggregate_arg_count(&expr);
        assert!(result.is_some(), "MAX(*) should be invalid");
        assert_eq!(result.unwrap(), "MAX"); // Preserves original case
    }

    #[test]
    fn test_min_no_args_invalid() {
        // MIN() with no arguments should be invalid
        let expr = Expression::AggregateFunction {
            name: vibesql_ast::FunctionIdentifier::new("MIN"),
            distinct: false,
            args: vec![],
            order_by: None,
            filter: None,
        };
        let result = check_aggregate_arg_count(&expr);
        assert!(result.is_some(), "MIN() should be invalid");
        assert_eq!(result.unwrap(), "MIN"); // Preserves original case
    }

    #[test]
    fn test_validate_aggregate_arguments() {
        // Test the public function
        let select_list = vec![SelectItem::Expression {
            expr: Expression::AggregateFunction {
                name: vibesql_ast::FunctionIdentifier::new("MIN"),
                distinct: false,
                args: vec![Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "*", false,
                ))],
                order_by: None,
                filter: None,
            },
            alias: None,
            source_text: None,
        }];
        let result = super::validate_aggregate_arguments(&select_list);
        assert!(result.is_err());
    }

    // Tests for misuse of aliased aggregates (#4432)

    /// Create a schema with f1 and f2 columns (for aliased aggregate tests)
    fn make_f1_f2_schema() -> CombinedSchema {
        let columns = vec![
            ColumnSchema {
                name: "f1".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
            },
            ColumnSchema {
                name: "f2".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
            },
        ];
        let table_schema = TableSchema::new("test1".to_string(), columns);
        CombinedSchema::from_table("test1".to_string(), table_schema)
    }

    #[test]
    fn test_having_with_aliased_aggregate_inside_aggregate() {
        // SELECT min(f1) AS m FROM test1 GROUP BY f1 HAVING max(m+5)<10
        // The alias 'm' refers to an aggregate and is used inside max() - should error
        // Note: 'm' is NOT a column in the table, so it's treated as an alias reference
        let select_list = vec![SelectItem::Expression {
            expr: Expression::AggregateFunction {
                name: vibesql_ast::FunctionIdentifier::new("min"),
                distinct: false,
                args: vec![Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "f1", false,
                ))],
                order_by: None,
                filter: None,
            },
            alias: Some("m".to_string()),
            source_text: None,
        }];

        // HAVING max(m+5)<10
        let having_expr = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::LessThan,
            left: Box::new(Expression::AggregateFunction {
                name: vibesql_ast::FunctionIdentifier::new("max"),
                distinct: false,
                args: vec![Expression::BinaryOp {
                    op: vibesql_ast::BinaryOperator::Plus,
                    left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                        "m", false,
                    ))),
                    right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(5))),
                }],
                order_by: None,
                filter: None,
            }),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(10))),
        };

        // Use schema with f1, f2 - 'm' is not a column, so it's an alias reference
        let schema = make_f1_f2_schema();
        let result = validate_having_aliased_aggregates(Some(&having_expr), &select_list, &schema);
        assert!(result.is_err());
        match result {
            Err(ExecutorError::MisuseOfAliasedAggregate { alias_name }) => {
                assert_eq!(alias_name, "m");
            }
            _ => panic!("Expected MisuseOfAliasedAggregate error"),
        }
    }

    #[test]
    fn test_having_with_aggregate_alias_not_inside_aggregate() {
        // SELECT min(f1) AS m FROM test1 GROUP BY f1 HAVING m>0
        // The alias 'm' refers to an aggregate but is NOT used inside another aggregate
        // This is a gray area - SQLite actually treats this as an error too,
        // because the alias cannot be resolved in HAVING context at all.
        // For now, we only detect the case where it's inside an aggregate.
        let select_list = vec![SelectItem::Expression {
            expr: Expression::AggregateFunction {
                name: vibesql_ast::FunctionIdentifier::new("min"),
                distinct: false,
                args: vec![Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "f1", false,
                ))],
                order_by: None,
                filter: None,
            },
            alias: Some("m".to_string()),
            source_text: None,
        }];

        // HAVING m>0 - alias used directly, not inside an aggregate
        let having_expr = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "m", false,
            ))),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(0))),
        };

        // This should pass our current validation (alias not inside aggregate)
        // SQLite would error on this too, but we'll catch it later during evaluation
        let schema = make_f1_f2_schema();
        let result = validate_having_aliased_aggregates(Some(&having_expr), &select_list, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_having_without_aggregate_alias() {
        // SELECT count(*) FROM test1 GROUP BY f1 HAVING f1>0
        // No aliased aggregate, should pass
        let select_list = vec![SelectItem::Expression {
            expr: Expression::AggregateFunction {
                name: vibesql_ast::FunctionIdentifier::new("count"),
                distinct: false,
                args: vec![Expression::Wildcard],
                order_by: None,
                filter: None,
            },
            alias: None, // No alias
            source_text: None,
        }];

        let having_expr = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "f1", false,
            ))),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(0))),
        };

        let schema = make_f1_f2_schema();
        let result = validate_having_aliased_aggregates(Some(&having_expr), &select_list, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_having_with_non_aggregate_alias() {
        // SELECT f1 AS x, count(*) FROM test1 GROUP BY f1 HAVING max(x)<10
        // 'x' is an alias for f1, not an aggregate - should pass
        let select_list = vec![
            SelectItem::Expression {
                expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("f1", false)),
                alias: Some("x".to_string()),
                source_text: None,
            },
            SelectItem::Expression {
                expr: Expression::AggregateFunction {
                    name: vibesql_ast::FunctionIdentifier::new("count"),
                    distinct: false,
                    args: vec![Expression::Wildcard],
                    order_by: None,
                    filter: None,
                },
                alias: None,
                source_text: None,
            },
        ];

        // HAVING max(x)<10 - 'x' is not an aggregate alias
        let having_expr = Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::LessThan,
            left: Box::new(Expression::AggregateFunction {
                name: vibesql_ast::FunctionIdentifier::new("max"),
                distinct: false,
                args: vec![Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "x", false,
                ))],
                order_by: None,
                filter: None,
            }),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(10))),
        };

        let schema = make_f1_f2_schema();
        let result = validate_having_aliased_aggregates(Some(&having_expr), &select_list, &schema);
        assert!(result.is_ok());
    }

    #[test]
    fn test_having_alias_shadows_column_uses_column() {
        // SELECT - col2 * - AVG(-col2) AS col0 FROM tab0 GROUP BY col2 HAVING AVG(col0) IS NULL
        // The alias 'col0' happens to match the aggregate expression, but 'col0' is also
        // a real column in the table. In this case, the HAVING clause refers to the
        // actual column col0, NOT the alias. This should NOT be an error.
        let select_list = vec![SelectItem::Expression {
            expr: Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Multiply,
                left: Box::new(Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Minus,
                    expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                        "col2", false,
                    ))),
                }),
                right: Box::new(Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Minus,
                    expr: Box::new(Expression::AggregateFunction {
                        name: vibesql_ast::FunctionIdentifier::new("AVG"),
                        distinct: false,
                        args: vec![Expression::UnaryOp {
                            op: vibesql_ast::UnaryOperator::Minus,
                            expr: Box::new(Expression::ColumnRef(
                                vibesql_ast::ColumnIdentifier::simple("col2", false),
                            )),
                        }],
                        order_by: None,
                        filter: None,
                    }),
                }),
            },
            alias: Some("col0".to_string()), // Alias matches a column name!
            source_text: None,
        }];

        // HAVING AVG(col0) IS NULL - col0 is a real column, not the alias
        let having_expr = Expression::IsNull {
            expr: Box::new(Expression::AggregateFunction {
                name: vibesql_ast::FunctionIdentifier::new("AVG"),
                distinct: false,
                args: vec![Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "col0", false,
                ))],
                order_by: None,
                filter: None,
            }),
            negated: false,
        };

        // Schema with col0, col1, col2 - col0 exists as an actual column
        let columns = vec![
            ColumnSchema {
                name: "col0".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
            },
            ColumnSchema {
                name: "col1".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
            },
            ColumnSchema {
                name: "col2".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
            },
        ];
        let table_schema = TableSchema::new("tab0".to_string(), columns);
        let schema = CombinedSchema::from_table("tab0".to_string(), table_schema);

        // This should pass - col0 refers to the real column, not the alias
        let result = validate_having_aliased_aggregates(Some(&having_expr), &select_list, &schema);
        assert!(result.is_ok(), "Expected Ok but got {:?}", result);
    }

    #[test]
    fn test_build_aggregate_aliases() {
        // Test the helper function
        let select_list = vec![
            SelectItem::Expression {
                expr: Expression::AggregateFunction {
                    name: vibesql_ast::FunctionIdentifier::new("min"),
                    distinct: false,
                    args: vec![Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                        "f1", false,
                    ))],
                    order_by: None,
                    filter: None,
                },
                alias: Some("m".to_string()),
                source_text: None,
            },
            SelectItem::Expression {
                expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("f2", false)),
                alias: Some("col2".to_string()),
                source_text: None,
            },
            SelectItem::Expression {
                // coalesce(min(f1)+5, 11) AS m2
                expr: Expression::Function {
                    name: vibesql_ast::FunctionIdentifier::new("coalesce"),
                    args: vec![
                        Expression::BinaryOp {
                            op: vibesql_ast::BinaryOperator::Plus,
                            left: Box::new(Expression::AggregateFunction {
                                name: vibesql_ast::FunctionIdentifier::new("min"),
                                distinct: false,
                                args: vec![Expression::ColumnRef(
                                    vibesql_ast::ColumnIdentifier::simple("f1", false),
                                )],
                                order_by: None,
                                filter: None,
                            }),
                            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(
                                5,
                            ))),
                        },
                        Expression::Literal(vibesql_types::SqlValue::Integer(11)),
                    ],
                    character_unit: None,
                },
                alias: Some("m2".to_string()),
                source_text: None,
            },
        ];

        let aliases = build_aggregate_aliases(&select_list);
        assert!(aliases.contains("m")); // min(f1) AS m is an aggregate alias
        assert!(!aliases.contains("col2")); // f2 AS col2 is NOT an aggregate alias
        assert!(aliases.contains("m2")); // coalesce(min(f1)+5, 11) contains an aggregate
    }

    // Tests for join table limit validation (#4711)

    #[test]
    fn test_join_table_limit_under_limit() {
        // Create a SELECT with 64 tables (exactly at limit) - should pass
        // SELECT * FROM t1, t2, t3, ... (64 times)
        let mut from: FromClause = FromClause::Table {
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        };
        for i in 2..=64 {
            from = FromClause::Join {
                left: Box::new(from),
                right: Box::new(FromClause::Table {
                    name: format!("t{}", i),
                    alias: None,
                    column_aliases: None,
                    quoted: false,
                }),
                join_type: vibesql_ast::JoinType::Cross,
                condition: None,
                using_columns: None,
                natural: false,
            };
        }
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(from),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };
        // Should pass - exactly 64 tables
        assert!(validate_join_table_limit(&stmt).is_ok());
    }

    #[test]
    fn test_join_table_limit_exceeds_limit() {
        // Create a SELECT with 65 tables (exceeds limit) - should fail
        let mut from: FromClause = FromClause::Table {
            name: "t1".to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
        };
        for i in 2..=65 {
            from = FromClause::Join {
                left: Box::new(from),
                right: Box::new(FromClause::Table {
                    name: format!("t{}", i),
                    alias: None,
                    column_aliases: None,
                    quoted: false,
                }),
                join_type: vibesql_ast::JoinType::Cross,
                condition: None,
                using_columns: None,
                natural: false,
            };
        }
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(from),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };
        // Should fail - 65 tables exceeds limit
        let result = validate_join_table_limit(&stmt);
        assert!(result.is_err());
        match result {
            Err(ExecutorError::JoinTableLimitExceeded { table_count, max_tables }) => {
                assert_eq!(table_count, 65);
                assert_eq!(max_tables, 64);
            }
            _ => panic!("Expected JoinTableLimitExceeded error"),
        }
    }

    #[test]
    fn test_join_table_limit_no_from_clause() {
        // SELECT 1 (no FROM clause) - should pass
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                alias: None,
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };
        assert!(validate_join_table_limit(&stmt).is_ok());
    }
}
