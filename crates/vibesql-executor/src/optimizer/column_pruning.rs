//! Column Pruning for Post-Join Processing
//!
//! This module implements column pruning optimization that projects only the columns
//! needed for aggregation after JOIN operations complete. This significantly reduces
//! memory pressure and improves performance for multi-way JOIN queries with GROUP BY.
//!
//! ## Problem (Issue #4355)
//!
//! TPC-H Q7 (6-way join) was 7x slower than SQLite. Profiling showed 70% of time
//! spent in post-join processing (GROUP BY, aggregation). The root cause: JOIN
//! results carry ALL columns from ALL tables (54 columns), but only 14 are needed.
//!
//! ## Solution
//!
//! After JOIN completes, project only the columns needed for:
//! - GROUP BY expressions
//! - SELECT list expressions (including aggregate function arguments)
//! - HAVING clause (if present)
//!
//! This reduces rows from 54 columns to 14 columns, cutting memory and CPU overhead
//! by ~74% for GROUP BY evaluation.

use std::collections::HashSet;

use vibesql_ast::{Expression, GroupByClause, GroupingElement, MixedGroupingItem, SelectItem};

/// Represents a column reference (table, column) pair
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// Table name (or alias) - None for unqualified references
    pub table: Option<String>,
    /// Column name
    pub column: String,
}

impl ColumnRef {
    pub fn new(table: Option<String>, column: String) -> Self {
        Self { table: table.map(|t| t.to_lowercase()), column: column.to_lowercase() }
    }

    #[allow(dead_code)]
    pub fn qualified(table: &str, column: &str) -> Self {
        Self { table: Some(table.to_lowercase()), column: column.to_lowercase() }
    }

    #[allow(dead_code)]
    pub fn unqualified(column: &str) -> Self {
        Self { table: None, column: column.to_lowercase() }
    }
}

/// Collect all column references needed for aggregation query processing
///
/// Returns the set of columns referenced by:
/// - SELECT list expressions (including aggregate arguments)
/// - GROUP BY expressions
/// - HAVING clause (if any)
///
/// This determines the minimum set of columns needed after JOIN.
pub fn collect_required_columns(
    select_list: &[SelectItem],
    group_by: Option<&GroupByClause>,
    having: Option<&Expression>,
) -> HashSet<ColumnRef> {
    let mut columns = HashSet::new();

    // Collect from SELECT list
    for item in select_list {
        match item {
            SelectItem::Expression { expr, .. } => {
                collect_columns_from_expr(expr, &mut columns);
            }
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                // Wildcards need all columns - return empty set to signal "no pruning"
                return HashSet::new();
            }
        }
    }

    // Collect from GROUP BY
    if let Some(gb) = group_by {
        collect_columns_from_group_by(gb, &mut columns);
    }

    // Collect from HAVING
    if let Some(having_expr) = having {
        collect_columns_from_expr(having_expr, &mut columns);
    }

    columns
}

/// Collect columns from GROUP BY clause (handles all variants)
fn collect_columns_from_group_by(gb: &GroupByClause, columns: &mut HashSet<ColumnRef>) {
    match gb {
        GroupByClause::Simple(exprs) => {
            for expr in exprs {
                collect_columns_from_expr(expr, columns);
            }
        }
        GroupByClause::Rollup(elements) | GroupByClause::Cube(elements) => {
            collect_columns_from_grouping_elements(elements, columns);
        }
        GroupByClause::GroupingSets(sets) => {
            for set in sets {
                for expr in &set.columns {
                    collect_columns_from_expr(expr, columns);
                }
            }
        }
        GroupByClause::Mixed(items) => {
            for item in items {
                match item {
                    MixedGroupingItem::Simple(expr) => {
                        collect_columns_from_expr(expr, columns);
                    }
                    MixedGroupingItem::Rollup(elements) | MixedGroupingItem::Cube(elements) => {
                        collect_columns_from_grouping_elements(elements, columns);
                    }
                    MixedGroupingItem::GroupingSets(sets) => {
                        for set in sets {
                            for expr in &set.columns {
                                collect_columns_from_expr(expr, columns);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Helper to collect columns from GroupingElement list
fn collect_columns_from_grouping_elements(
    elements: &[GroupingElement],
    columns: &mut HashSet<ColumnRef>,
) {
    for element in elements {
        match element {
            GroupingElement::Single(expr) => {
                collect_columns_from_expr(expr, columns);
            }
            GroupingElement::Composite(exprs) => {
                for expr in exprs {
                    collect_columns_from_expr(expr, columns);
                }
            }
        }
    }
}

/// Recursively collect column references from an expression
pub fn collect_columns_from_expr(expr: &Expression, columns: &mut HashSet<ColumnRef>) {
    match expr {
        Expression::ColumnRef(col_id) => {
            let column = col_id.column_canonical();
            // Skip the special "*" wildcard (used in COUNT(*))
            if column != "*" {
                columns.insert(ColumnRef::new(
                    col_id.table_canonical().map(|t| t.to_string()),
                    column.to_string(),
                ));
            }
        }

        Expression::BinaryOp { left, right, .. } => {
            collect_columns_from_expr(left, columns);
            collect_columns_from_expr(right, columns);
        }

        Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
            for e in exprs {
                collect_columns_from_expr(e, columns);
            }
        }

        Expression::UnaryOp { expr, .. } => {
            collect_columns_from_expr(expr, columns);
        }

        Expression::Function { args, .. } => {
            for arg in args {
                collect_columns_from_expr(arg, columns);
            }
        }

        Expression::AggregateFunction { args, order_by, filter, .. } => {
            for arg in args {
                collect_columns_from_expr(arg, columns);
            }
            // Also collect columns from ORDER BY expressions within the aggregate
            if let Some(order_items) = order_by {
                for item in order_items {
                    collect_columns_from_expr(&item.expr, columns);
                }
            }
            // Also collect columns from FILTER clause within the aggregate
            // e.g., COUNT(a) FILTER (WHERE b='odd') requires column 'b'
            if let Some(filter_expr) = filter {
                collect_columns_from_expr(filter_expr, columns);
            }
        }

        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_columns_from_expr(op, columns);
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    collect_columns_from_expr(cond, columns);
                }
                collect_columns_from_expr(&clause.result, columns);
            }
            if let Some(else_res) = else_result {
                collect_columns_from_expr(else_res, columns);
            }
        }

        Expression::ScalarSubquery(subquery) => {
            // Extract columns from subquery's SELECT list (may reference outer columns)
            // Issue #4618: Column references in SELECT list like `(SELECT b)` where `b`
            // comes from the outer query must be collected to avoid incorrect pruning.
            for item in &subquery.select_list {
                if let SelectItem::Expression { expr, .. } = item {
                    collect_columns_from_expr(expr, columns);
                }
            }
            // Also extract columns from subquery's WHERE clause (may reference outer columns)
            if let Some(where_clause) = &subquery.where_clause {
                collect_columns_from_expr(where_clause, columns);
            }
        }

        Expression::In { expr, subquery, .. } => {
            collect_columns_from_expr(expr, columns);
            // Extract columns from IN subquery's SELECT list (may reference outer columns)
            for item in &subquery.select_list {
                if let SelectItem::Expression { expr, .. } = item {
                    collect_columns_from_expr(expr, columns);
                }
            }
            // Also extract columns from IN subquery's WHERE clause (may reference outer columns)
            if let Some(where_clause) = &subquery.where_clause {
                collect_columns_from_expr(where_clause, columns);
            }
        }

        Expression::InList { expr, values, .. } => {
            collect_columns_from_expr(expr, columns);
            for v in values {
                collect_columns_from_expr(v, columns);
            }
        }

        Expression::IsNull { expr, .. } => {
            collect_columns_from_expr(expr, columns);
        }

        Expression::IsDistinctFrom { left, right, .. } => {
            collect_columns_from_expr(left, columns);
            collect_columns_from_expr(right, columns);
        }

        Expression::IsTruthValue { expr, .. } => {
            collect_columns_from_expr(expr, columns);
        }

        Expression::Cast { expr, .. } => {
            collect_columns_from_expr(expr, columns);
        }

        Expression::Position { substring, string, .. } => {
            collect_columns_from_expr(substring, columns);
            collect_columns_from_expr(string, columns);
        }

        Expression::Trim { removal_char, string, .. } => {
            if let Some(rc) = removal_char {
                collect_columns_from_expr(rc, columns);
            }
            collect_columns_from_expr(string, columns);
        }

        Expression::Between { expr, low, high, .. } => {
            collect_columns_from_expr(expr, columns);
            collect_columns_from_expr(low, columns);
            collect_columns_from_expr(high, columns);
        }

        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            collect_columns_from_expr(expr, columns);
            collect_columns_from_expr(pattern, columns);
        }

        Expression::Extract { expr, .. } => {
            collect_columns_from_expr(expr, columns);
        }

        Expression::Exists { subquery, .. } => {
            // Extract columns from EXISTS subquery's SELECT list (may reference outer columns)
            for item in &subquery.select_list {
                if let SelectItem::Expression { expr, .. } = item {
                    collect_columns_from_expr(expr, columns);
                }
            }
            // Also extract columns from EXISTS subquery's WHERE clause (may reference outer
            // columns)
            if let Some(where_clause) = &subquery.where_clause {
                collect_columns_from_expr(where_clause, columns);
            }
        }

        Expression::QuantifiedComparison { expr, subquery, .. } => {
            collect_columns_from_expr(expr, columns);
            // Extract columns from subquery's SELECT list (may reference outer columns)
            for item in &subquery.select_list {
                if let SelectItem::Expression { expr, .. } = item {
                    collect_columns_from_expr(expr, columns);
                }
            }
            // Also extract columns from subquery's WHERE clause (may reference outer columns)
            if let Some(where_clause) = &subquery.where_clause {
                collect_columns_from_expr(where_clause, columns);
            }
        }

        Expression::Interval { value, .. } => {
            collect_columns_from_expr(value, columns);
        }

        // Window functions - collect from both function spec and window spec
        Expression::WindowFunction { function, over } => {
            // Collect from the window function spec
            match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
                    for arg in args {
                        collect_columns_from_expr(arg, columns);
                    }
                }
            }
            // Collect from window spec
            if let Some(partition_exprs) = &over.partition_by {
                for expr in partition_exprs {
                    collect_columns_from_expr(expr, columns);
                }
            }
            if let Some(order_items) = &over.order_by {
                for order_item in order_items {
                    collect_columns_from_expr(&order_item.expr, columns);
                }
            }
        }

        Expression::MatchAgainst { search_modifier, .. } => {
            collect_columns_from_expr(search_modifier, columns);
        }

        Expression::PseudoVariable { .. } => {
            // Pseudo variables like OLD.col or NEW.col in triggers
            // These are handled specially and don't map to regular columns
        }

        Expression::RowValueConstructor(exprs) => {
            for e in exprs {
                collect_columns_from_expr(e, columns);
            }
        }

        Expression::Collate { expr, .. } => {
            collect_columns_from_expr(expr, columns);
        }

        Expression::Raise { error_message, .. } => {
            if let Some(msg) = error_message {
                collect_columns_from_expr(msg, columns);
            }
        }

        // Expressions that don't reference columns
        Expression::Literal(_)
        | Expression::CollatedLiteral { .. }
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::DuplicateKeyValue { .. }
        | Expression::NextValue { .. }
        | Expression::SessionVariable { .. }
        | Expression::Wildcard => {
            // No column references in these expressions
        }
    }
}

/// Compute column indices to keep based on required columns and schema
///
/// Returns a vector of column indices that should be kept in projected rows.
/// Returns None if all columns should be kept (wildcards, unresolvable references).
pub fn compute_projection_indices(
    required_columns: &HashSet<ColumnRef>,
    schema: &crate::schema::CombinedSchema,
) -> Option<Vec<usize>> {
    // Empty set means wildcard was used - keep all columns
    if required_columns.is_empty() {
        return None;
    }

    let mut indices = HashSet::new();

    for col_ref in required_columns {
        match &col_ref.table {
            Some(table_name) => {
                // Qualified reference: table.column
                if let Some(idx) =
                    schema.get_column_index(Some(table_name.as_str()), &col_ref.column)
                {
                    indices.insert(idx);
                } else {
                    // Column not found - keep all columns to be safe
                    return None;
                }
            }
            None => {
                // Unqualified reference: try to resolve against all tables
                let mut found = false;
                for (tbl_id, (_start, tbl_schema)) in &schema.table_schemas {
                    if tbl_schema.columns.iter().any(|c| c.name.to_lowercase() == col_ref.column) {
                        if let Some(idx) =
                            schema.get_column_index(Some(tbl_id.canonical()), &col_ref.column)
                        {
                            indices.insert(idx);
                            found = true;
                            break;
                        }
                    }
                }
                if !found {
                    // Column not found - keep all columns to be safe
                    return None;
                }
            }
        }
    }

    if indices.is_empty() {
        return None;
    }

    // Sort indices for consistent ordering
    let mut sorted: Vec<_> = indices.into_iter().collect();
    sorted.sort_unstable();
    Some(sorted)
}

/// Project rows to keep only the specified columns
///
/// This is the core optimization: instead of carrying wide rows (54 columns)
/// through GROUP BY processing, we project down to narrow rows (14 columns).
pub fn project_rows(
    rows: Vec<vibesql_storage::Row>,
    indices: &[usize],
) -> Vec<vibesql_storage::Row> {
    rows.into_iter()
        .map(|row| {
            let projected_values: Vec<_> =
                indices.iter().map(|&idx| row.values[idx].clone()).collect();
            vibesql_storage::Row::new(projected_values)
        })
        .collect()
}

/// Remap a CombinedSchema to reflect projected columns
///
/// After projecting rows from wide (54 columns) to narrow (14 columns),
/// we need a new schema that:
/// 1. Only includes the columns that were kept
/// 2. Has correct column indices (0-13 instead of scattered across 0-53)
/// 3. Preserves table aliases for qualified column references
///
/// Returns the remapped schema where column lookups resolve to the correct
/// projected indices.
pub fn remap_schema(
    original_schema: &crate::schema::CombinedSchema,
    projection_indices: &[usize],
) -> crate::schema::CombinedSchema {
    use std::collections::HashMap;

    // Build a map from original index -> new projected index
    let old_to_new: HashMap<usize, usize> = projection_indices
        .iter()
        .enumerate()
        .map(|(new_idx, &old_idx)| (old_idx, new_idx))
        .collect();

    // For each table in the original schema, build a new TableSchema with only the kept columns
    let mut new_table_schemas: HashMap<
        vibesql_catalog::TableIdentifier,
        (usize, vibesql_catalog::TableSchema),
    > = HashMap::new();

    // Track new column offset for each table
    for (table_id, (original_start, original_table_schema)) in &original_schema.table_schemas {
        // Find which columns from this table are in the projection
        let mut kept_columns: Vec<(usize, vibesql_catalog::ColumnSchema)> = Vec::new();

        for (col_idx, column) in original_table_schema.columns.iter().enumerate() {
            let absolute_idx = original_start + col_idx;
            if let Some(&new_idx) = old_to_new.get(&absolute_idx) {
                kept_columns.push((new_idx, column.clone()));
            }
        }

        // Skip tables with no kept columns
        if kept_columns.is_empty() {
            continue;
        }

        // Sort by new index to preserve column order
        kept_columns.sort_by_key(|(new_idx, _)| *new_idx);

        // Find the minimum new index - this is the new start for this table
        let new_start = kept_columns.iter().map(|(idx, _)| *idx).min().unwrap_or(0);

        // Create new columns vector
        let new_columns: Vec<vibesql_catalog::ColumnSchema> =
            kept_columns.into_iter().map(|(_, col)| col).collect();

        // Create new TableSchema with filtered columns
        let new_table_schema =
            vibesql_catalog::TableSchema::new(original_table_schema.name.clone(), new_columns);

        new_table_schemas.insert(table_id.clone(), (new_start, new_table_schema));
    }

    crate::schema::CombinedSchema {
        table_schemas: new_table_schemas,
        total_columns: projection_indices.len(),
        hidden_columns: std::collections::HashSet::new(),
        always_hidden_columns: std::collections::HashSet::new(),
        outer_schema: None,
        duplicate_aliases: std::collections::HashSet::new(),
        joined_columns: std::collections::HashSet::new(),
        using_coalesce_indices: std::collections::HashMap::new(),
        column_replacement_map: std::collections::HashMap::new(),
        alias_tables: std::collections::HashSet::new(),
        shadowed_tables: std::collections::HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::BinaryOperator;
    use vibesql_catalog::ColumnSchema;
    use vibesql_types::{DataType, SqlValue};

    use super::*;
    use crate::schema::CombinedSchema;

    #[test]
    fn test_collect_simple_column_ref() {
        let expr =
            Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified("t", false, "c", false));

        let mut columns = HashSet::new();
        collect_columns_from_expr(&expr, &mut columns);

        assert_eq!(columns.len(), 1);
        assert!(columns.contains(&ColumnRef::qualified("t", "c")));
    }

    #[test]
    fn test_collect_unqualified_column_ref() {
        let expr = Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("col", false));

        let mut columns = HashSet::new();
        collect_columns_from_expr(&expr, &mut columns);

        assert_eq!(columns.len(), 1);
        assert!(columns.contains(&ColumnRef::unqualified("col")));
    }

    #[test]
    fn test_collect_from_binary_op() {
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "t1", false, "a", false,
            ))),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "t2", false, "b", false,
            ))),
        };

        let mut columns = HashSet::new();
        collect_columns_from_expr(&expr, &mut columns);

        assert_eq!(columns.len(), 2);
        assert!(columns.contains(&ColumnRef::qualified("t1", "a")));
        assert!(columns.contains(&ColumnRef::qualified("t2", "b")));
    }

    #[test]
    fn test_collect_from_aggregate() {
        let expr = Expression::AggregateFunction {
            name: vibesql_ast::FunctionIdentifier::new("SUM"),
            args: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "price", false,
                ))),
                op: BinaryOperator::Multiply,
                right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "qty", false,
                ))),
            }],
            distinct: false,
            order_by: None,
            filter: None,
        };

        let mut columns = HashSet::new();
        collect_columns_from_expr(&expr, &mut columns);

        assert_eq!(columns.len(), 2);
        assert!(columns.contains(&ColumnRef::unqualified("price")));
        assert!(columns.contains(&ColumnRef::unqualified("qty")));
    }

    #[test]
    fn test_collect_from_extract() {
        let expr = Expression::Extract {
            field: vibesql_ast::IntervalUnit::Year,
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "lineitem",
                false,
                "l_shipdate",
                false,
            ))),
        };

        let mut columns = HashSet::new();
        collect_columns_from_expr(&expr, &mut columns);

        assert_eq!(columns.len(), 1);
        assert!(columns.contains(&ColumnRef::qualified("lineitem", "l_shipdate")));
    }

    #[test]
    fn test_project_rows() {
        let rows = vec![
            vibesql_storage::Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Integer(2),
                SqlValue::Integer(3),
                SqlValue::Integer(4),
            ]),
            vibesql_storage::Row::new(vec![
                SqlValue::Integer(5),
                SqlValue::Integer(6),
                SqlValue::Integer(7),
                SqlValue::Integer(8),
            ]),
        ];

        let indices = vec![0, 2]; // Keep columns 0 and 2

        let projected = project_rows(rows, &indices);

        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0].values.as_slice(), &[SqlValue::Integer(1), SqlValue::Integer(3)]);
        assert_eq!(projected[1].values.as_slice(), &[SqlValue::Integer(5), SqlValue::Integer(7)]);
    }

    /// Helper to create a simple table schema with given columns
    fn make_table_schema(name: &str, columns: &[(&str, DataType)]) -> vibesql_catalog::TableSchema {
        let cols: Vec<ColumnSchema> = columns
            .iter()
            .map(|(col_name, dt)| ColumnSchema::new(col_name.to_string(), dt.clone(), true))
            .collect();
        vibesql_catalog::TableSchema::new(name.to_string(), cols)
    }

    #[test]
    fn test_remap_schema_single_table() {
        // Create a table with 4 columns
        let table = make_table_schema(
            "orders",
            &[
                ("id", DataType::Integer),
                ("customer", DataType::Varchar { max_length: None }),
                ("amount", DataType::DoublePrecision),
                ("date", DataType::Date),
            ],
        );
        let schema = CombinedSchema::from_table("orders".to_string(), table);

        // Project columns 0 and 2 (id, amount)
        let indices = vec![0, 2];
        let remapped = remap_schema(&schema, &indices);

        // Should have 2 columns total
        assert_eq!(remapped.total_columns, 2);

        // Column lookups should work with new indices
        assert_eq!(remapped.get_column_index(Some("orders"), "id"), Some(0));
        assert_eq!(remapped.get_column_index(Some("orders"), "amount"), Some(1));

        // Original columns 1 and 3 should not be found
        assert!(remapped.get_column_index(Some("orders"), "customer").is_none());
        assert!(remapped.get_column_index(Some("orders"), "date").is_none());
    }

    #[test]
    fn test_remap_schema_multiple_tables() {
        // Simulate a JOIN of orders (3 cols) and items (2 cols) = 5 total columns
        // Original layout: orders.id(0), orders.customer(1), orders.amount(2), items.item_id(3),
        // items.price(4)
        let orders = make_table_schema(
            "orders",
            &[
                ("id", DataType::Integer),
                ("customer", DataType::Varchar { max_length: None }),
                ("amount", DataType::DoublePrecision),
            ],
        );
        let items = make_table_schema(
            "items",
            &[("item_id", DataType::Integer), ("price", DataType::DoublePrecision)],
        );

        let schema = CombinedSchema::combine(
            CombinedSchema::from_table("orders".to_string(), orders),
            "items".to_string(),
            items,
        );

        assert_eq!(schema.total_columns, 5);

        // Project: orders.id(0), orders.amount(2), items.price(4)
        // New layout: id(0), amount(1), price(2)
        let indices = vec![0, 2, 4];
        let remapped = remap_schema(&schema, &indices);

        assert_eq!(remapped.total_columns, 3);

        // Check orders columns
        assert_eq!(remapped.get_column_index(Some("orders"), "id"), Some(0));
        assert_eq!(remapped.get_column_index(Some("orders"), "amount"), Some(1));
        assert!(remapped.get_column_index(Some("orders"), "customer").is_none());

        // Check items columns
        assert_eq!(remapped.get_column_index(Some("items"), "price"), Some(2));
        assert!(remapped.get_column_index(Some("items"), "item_id").is_none());
    }

    #[test]
    fn test_remap_schema_table_fully_pruned() {
        // Simulate a JOIN where one table gets completely pruned
        let t1 = make_table_schema("t1", &[("a", DataType::Integer), ("b", DataType::Integer)]);
        let t2 = make_table_schema("t2", &[("c", DataType::Integer), ("d", DataType::Integer)]);

        let schema = CombinedSchema::combine(
            CombinedSchema::from_table("t1".to_string(), t1),
            "t2".to_string(),
            t2,
        );

        // Only keep columns from t2
        let indices = vec![2, 3];
        let remapped = remap_schema(&schema, &indices);

        assert_eq!(remapped.total_columns, 2);

        // t1 should not exist in the remapped schema
        assert!(remapped.get_column_index(Some("t1"), "a").is_none());
        assert!(remapped.get_column_index(Some("t1"), "b").is_none());

        // t2 columns should be at new indices
        assert_eq!(remapped.get_column_index(Some("t2"), "c"), Some(0));
        assert_eq!(remapped.get_column_index(Some("t2"), "d"), Some(1));
    }
}
