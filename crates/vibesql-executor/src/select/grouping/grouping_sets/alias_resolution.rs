//! GROUP BY alias resolution
//!
//! Handles resolution of SELECT list aliases and column positions
//! in GROUP BY clauses, enabling standard SQL alias syntax.

use super::ResolvedGroupingSet;
use vibesql_ast::Expression;

/// Resolve GROUP BY expression that might be a SELECT list alias or column position
///
/// Similar to ORDER BY alias resolution, handles three cases:
/// 1. Numeric literal (e.g., GROUP BY 1, 2, 3) - returns the expression from that position in SELECT list
/// 2. Simple column reference that matches a SELECT list alias - returns the SELECT list expression
/// 3. Otherwise - returns the original GROUP BY expression
///
/// This enables standard SQL behavior where GROUP BY can reference SELECT aliases:
/// ```sql
/// SELECT n_name as nation, COUNT(*) FROM ... GROUP BY nation
/// ```
pub fn resolve_group_by_alias(
    group_expr: &Expression,
    select_list: &[vibesql_ast::SelectItem],
) -> Expression {
    // Check for numeric column position (GROUP BY 1, 2, 3, etc.)
    if let Expression::Literal(vibesql_types::SqlValue::Integer(pos)) = group_expr {
        if *pos > 0 && (*pos as usize) <= select_list.len() {
            // Valid column position, return the expression at that position
            let idx = (*pos as usize) - 1;
            if let vibesql_ast::SelectItem::Expression { expr, .. } = &select_list[idx] {
                return expr.clone();
            }
        }
    }

    // Check if GROUP BY expression is a simple column reference (no table qualifier)
    if let Expression::ColumnRef { table: None, column } = group_expr {
        // Search for matching alias in SELECT list (case-insensitive)
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { expr, alias: Some(alias_name) } = item {
                if alias_name.eq_ignore_ascii_case(column) {
                    // Found matching alias, use the SELECT list expression
                    return expr.clone();
                }
            }
        }
    }

    // Not an alias or column position, use the original expression
    group_expr.clone()
}

/// Resolve all aliases in a ResolvedGroupingSet against SELECT list
///
/// Processes each GROUP BY expression, resolving any aliases to their
/// underlying expressions from the SELECT list.
pub fn resolve_grouping_set_aliases(
    set: &ResolvedGroupingSet,
    select_list: &[vibesql_ast::SelectItem],
) -> ResolvedGroupingSet {
    ResolvedGroupingSet {
        group_by_exprs: set
            .group_by_exprs
            .iter()
            .map(|expr| resolve_group_by_alias(expr, select_list))
            .collect(),
        rolled_up: set.rolled_up.clone(),
    }
}

/// Resolve aliases in base expressions for GroupingContext
///
/// Used to ensure GROUPING() function can properly match aliased columns
pub fn resolve_base_expressions_aliases(
    base_exprs: &[Expression],
    select_list: &[vibesql_ast::SelectItem],
) -> Vec<Expression> {
    base_exprs.iter().map(|expr| resolve_group_by_alias(expr, select_list)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::SelectItem;

    fn col(name: &str) -> Expression {
        Expression::ColumnRef { table: None, column: name.to_string() }
    }

    fn select_item(expr: Expression, alias: Option<&str>) -> SelectItem {
        SelectItem::Expression { expr, alias: alias.map(|s| s.to_string()) }
    }

    #[test]
    fn test_resolve_group_by_alias_simple() {
        // SELECT n_name AS nation, COUNT(*) FROM ... GROUP BY nation
        let select_list = vec![
            select_item(col("n_name"), Some("nation")),
            select_item(
                Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![Expression::Wildcard],
                },
                None,
            ),
        ];

        // GROUP BY nation should resolve to n_name
        let group_expr = col("nation");
        let resolved = resolve_group_by_alias(&group_expr, &select_list);

        assert!(
            matches!(resolved, Expression::ColumnRef { table: None, column } if column == "n_name")
        );
    }

    #[test]
    fn test_resolve_group_by_alias_case_insensitive() {
        // Alias matching should be case-insensitive
        let select_list = vec![select_item(col("n_name"), Some("NATION"))];

        // GROUP BY nation (lowercase) should match NATION (uppercase)
        let group_expr = col("nation");
        let resolved = resolve_group_by_alias(&group_expr, &select_list);

        assert!(
            matches!(resolved, Expression::ColumnRef { table: None, column } if column == "n_name")
        );
    }

    #[test]
    fn test_resolve_group_by_alias_numeric_position() {
        // GROUP BY 1 should return the first SELECT item
        let select_list =
            vec![select_item(col("n_name"), Some("nation")), select_item(col("amount"), None)];

        let group_expr = Expression::Literal(vibesql_types::SqlValue::Integer(1));
        let resolved = resolve_group_by_alias(&group_expr, &select_list);

        assert!(
            matches!(resolved, Expression::ColumnRef { table: None, column } if column == "n_name")
        );

        // GROUP BY 2 should return the second SELECT item
        let group_expr2 = Expression::Literal(vibesql_types::SqlValue::Integer(2));
        let resolved2 = resolve_group_by_alias(&group_expr2, &select_list);

        assert!(
            matches!(resolved2, Expression::ColumnRef { table: None, column } if column == "amount")
        );
    }

    #[test]
    fn test_resolve_group_by_alias_no_match() {
        // GROUP BY expression that doesn't match any alias should remain unchanged
        let select_list = vec![select_item(col("n_name"), Some("nation"))];

        // GROUP BY something_else should not resolve to anything
        let group_expr = col("something_else");
        let resolved = resolve_group_by_alias(&group_expr, &select_list);

        assert!(
            matches!(resolved, Expression::ColumnRef { table: None, column } if column == "something_else")
        );
    }

    #[test]
    fn test_resolve_group_by_alias_qualified_column() {
        // Qualified column references (e.g., t.col) should NOT resolve aliases
        let select_list = vec![select_item(col("n_name"), Some("nation"))];

        // GROUP BY t.nation should NOT resolve to n_name (it's table-qualified)
        let group_expr =
            Expression::ColumnRef { table: Some("t".to_string()), column: "nation".to_string() };
        let resolved = resolve_group_by_alias(&group_expr, &select_list);

        // Should remain unchanged
        assert!(
            matches!(resolved, Expression::ColumnRef { table: Some(t), column } if t == "t" && column == "nation")
        );
    }

    #[test]
    fn test_resolve_group_by_alias_expression_alias() {
        // SELECT SUBSTR(o_orderdate, 1, 4) AS o_year ... GROUP BY o_year
        let substr_expr = Expression::Function {
            name: "SUBSTR".to_string(),
            args: vec![
                col("o_orderdate"),
                Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                Expression::Literal(vibesql_types::SqlValue::Integer(4)),
            ],
            character_unit: None,
        };
        let select_list = vec![select_item(substr_expr.clone(), Some("o_year"))];

        // GROUP BY o_year should resolve to SUBSTR(o_orderdate, 1, 4)
        let group_expr = col("o_year");
        let resolved = resolve_group_by_alias(&group_expr, &select_list);

        // Should be the SUBSTR expression
        assert!(matches!(resolved, Expression::Function { name, .. } if name == "SUBSTR"));
    }

    #[test]
    fn test_resolve_grouping_set_aliases() {
        // Test resolving aliases in a full grouping set
        let select_list = vec![
            select_item(col("n1_n_name"), Some("supp_nation")),
            select_item(col("n2_n_name"), Some("cust_nation")),
        ];

        let original_set = ResolvedGroupingSet {
            group_by_exprs: vec![col("supp_nation"), col("cust_nation")],
            rolled_up: vec![false, false],
        };

        let resolved_set = resolve_grouping_set_aliases(&original_set, &select_list);

        // Both should resolve to the actual column names
        assert_eq!(resolved_set.group_by_exprs.len(), 2);
        assert!(
            matches!(&resolved_set.group_by_exprs[0], Expression::ColumnRef { table: None, column } if column == "n1_n_name")
        );
        assert!(
            matches!(&resolved_set.group_by_exprs[1], Expression::ColumnRef { table: None, column } if column == "n2_n_name")
        );
        // rolled_up should be preserved
        assert_eq!(resolved_set.rolled_up, vec![false, false]);
    }

    #[test]
    fn test_resolve_base_expressions_aliases() {
        let select_list =
            vec![select_item(col("a_col"), Some("a")), select_item(col("b_col"), Some("b"))];

        let base_exprs = vec![col("a"), col("b")];
        let resolved = resolve_base_expressions_aliases(&base_exprs, &select_list);

        assert_eq!(resolved.len(), 2);
        assert!(
            matches!(&resolved[0], Expression::ColumnRef { table: None, column } if column == "a_col")
        );
        assert!(
            matches!(&resolved[1], Expression::ColumnRef { table: None, column } if column == "b_col")
        );
    }
}
