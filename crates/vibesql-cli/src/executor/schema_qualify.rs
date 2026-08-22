//! Re-bind an attached schema's view body to its containing schema (#6407).
//!
//! ## Why this exists
//!
//! An attachment's own `.vbsql` file is a *standalone* database: every object
//! in it is persisted **schema-relative** (the writer,
//! `Database::save_attached_schema_sql_dump`, strips the saving session's
//! `aux.` qualifier so the file can be re-attached under any alias). That is
//! correct for the object *names*, but it is not sufficient for a view's
//! *body*: an unqualified table reference inside the body is late-bound, and
//! `Catalog::get_table` resolves an unqualified name in SQLite search order —
//! temp, then main, then attached. So after a reload, a body that read
//! `SELECT x FROM aux.t` (persisted as `SELECT x FROM t`) silently binds to
//! `main.t` whenever a same-named table exists in `main`, returning **another
//! database's rows** instead of erroring.
//!
//! SQLite's rule is that an unqualified name inside a view body resolves
//! within the schema that *contains* the view, not through the
//! connection-wide search path. [`qualify_unqualified_tables`] applies that
//! rule at re-attach time: every unqualified base-table reference in the
//! reloaded view body is rewritten to `<attach-alias>.<table>`, using the
//! alias *this* session attached the file under. Already-qualified references
//! (`main.mt`, `other.u`) are left untouched — the writer never stripped
//! those, so bare means "this attachment" and qualified means "that schema".
//!
//! ## What is deliberately not rewritten
//!
//! * **CTE names.** A name bound by an enclosing `WITH` clause is not a base table, so it is
//!   tracked in a scope stack and skipped. Scoping follows SQL's own rule: CTE *i* sees CTEs `0..i`
//!   (plus itself when `RECURSIVE`), and the outer query sees all of them.
//! * **Derived-table / join / VALUES aliases.** These never appear as a [`FromClause::Table`] name.
//! * **Table-valued functions** ([`FromClause::TableFunction`]) — a function name, not a table.
//! * **Column-reference qualifiers** ([`Expression::ColumnRef`]). A body written `SELECT t.x FROM
//!   t` qualifies its columns with the table's *unqualified* name, and the executor does not match
//!   a bare column qualifier against a schema-qualified table. Rather than rewrite every column
//!   reference (which would need alias tracking to avoid mangling `a.x` in `FROM t AS a`), an
//!   unaliased table keeps its bare name as an explicit **correlation name**: `FROM t` becomes
//!   `FROM aux.t AS t`, which is exactly what the unaliased form already meant, so `t.x` keeps
//!   resolving. A reference that already carries an alias is left alone.
//!
//! ## Known limitation
//!
//! Qualification is detected with a plain `.` scan over the stored name
//! string, matching how every other caller in this crate composes and splits
//! `schema.table` keys. A *quoted* identifier that itself contains a dot
//! (`CREATE VIEW v AS SELECT * FROM "my.table"`) is therefore treated as
//! already-qualified and left alone. That is the same assumption the rest of
//! the qualified-name plumbing makes, so this does not introduce a new
//! inconsistency.
//!
//! ## Triggers are not covered — see #6477
//!
//! A trigger's body is stored as [`TriggerAction::RawSql`] and re-parsed when
//! the trigger fires, and the parser rejects a qualified table name inside a
//! trigger body outright ("qualified table names are not allowed on INSERT,
//! UPDATE, and DELETE statements within triggers", matching SQLite). So the
//! same rewrite cannot be applied to a trigger body: there is no AST to
//! rewrite, and the text form that would express the binding is unparseable.
//! Fixing it properly means teaching the trigger executor to resolve a body's
//! unqualified names in the trigger's *own* schema, which is a change to
//! live-session resolution rather than to this reload path (the same
//! late-binding is already observable without any save/reload). That is
//! tracked as **#6477**;
//! `test_attach_reattach_trigger_body_binds_to_main_on_name_collision` pins
//! the actual (imperfect) behavior so it cannot change unnoticed.

use vibesql_ast::{
    CaseWhen, Expression, FrameBound, FromClause, GroupByClause, GroupingElement,
    MixedGroupingItem, OrderByItem, SelectItem, SelectStmt, WindowFunctionSpec, WindowSpec,
};

/// Rewrite every unqualified base-table reference in `stmt` to
/// `<schema_name>.<table>`, leaving already-qualified references and
/// CTE-bound names alone.
///
/// See the module docs for the full contract.
pub(super) fn qualify_unqualified_tables(stmt: &mut SelectStmt, schema_name: &str) {
    let mut cte_scope: Vec<String> = Vec::new();
    qualify_select(stmt, schema_name, &mut cte_scope);
}

fn qualify_select(stmt: &mut SelectStmt, schema: &str, cte_scope: &mut Vec<String>) {
    // A CTE list introduces names that shadow base tables for the rest of the
    // statement. Push them incrementally so CTE `i` sees only `0..i` (plus
    // itself when RECURSIVE), exactly as SQL scoping requires.
    let scope_depth = cte_scope.len();
    if let Some(ctes) = &mut stmt.with_clause {
        for cte in ctes.iter_mut() {
            let name = cte.name.to_ascii_lowercase();
            if cte.recursive {
                cte_scope.push(name.clone());
                qualify_select(&mut cte.query, schema, cte_scope);
            } else {
                qualify_select(&mut cte.query, schema, cte_scope);
                cte_scope.push(name.clone());
            }
        }
    }

    for item in &mut stmt.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            qualify_expr(expr, schema, cte_scope);
        }
    }

    if let Some(from) = &mut stmt.from {
        qualify_from(from, schema, cte_scope);
    }

    if let Some(where_clause) = &mut stmt.where_clause {
        qualify_expr(where_clause, schema, cte_scope);
    }

    if let Some(group_by) = &mut stmt.group_by {
        qualify_group_by(group_by, schema, cte_scope);
    }

    if let Some(having) = &mut stmt.having {
        qualify_expr(having, schema, cte_scope);
    }

    if let Some(defs) = &mut stmt.window_definitions {
        for def in defs.iter_mut() {
            qualify_window_spec(&mut def.spec, schema, cte_scope);
        }
    }

    if let Some(order_by) = &mut stmt.order_by {
        qualify_order_by(order_by, schema, cte_scope);
    }

    if let Some(limit) = &mut stmt.limit {
        qualify_expr(limit, schema, cte_scope);
    }

    if let Some(offset) = &mut stmt.offset {
        qualify_expr(offset, schema, cte_scope);
    }

    // The WITH clause scopes over the whole compound select, so the right arm
    // is walked with the CTE names still in scope.
    if let Some(set_op) = &mut stmt.set_operation {
        qualify_select(&mut set_op.right, schema, cte_scope);
    }

    if let Some(rows) = &mut stmt.values {
        for row in rows.iter_mut() {
            for expr in row.iter_mut() {
                qualify_expr(expr, schema, cte_scope);
            }
        }
    }

    cte_scope.truncate(scope_depth);
}

fn qualify_from(from: &mut FromClause, schema: &str, cte_scope: &mut Vec<String>) {
    match from {
        FromClause::Table { name, alias, .. } => {
            if !name.contains('.') && !cte_scope.iter().any(|c| c.eq_ignore_ascii_case(name)) {
                // Preserve the bare name as an explicit correlation name when
                // the reference had no alias of its own. A body written
                // `SELECT t.x FROM t` qualifies its columns with the table's
                // *unqualified* name, and the executor does not match a bare
                // column qualifier against a schema-qualified table — so
                // rewriting the FROM entry alone would turn a working view
                // into "Column 'x' not found (searched tables: aux.t)".
                // `FROM aux.t AS t` keeps `t.x` resolving and is exactly what
                // the unaliased form already means.
                if alias.is_none() {
                    *alias = Some(name.clone());
                }
                *name = format!("{}.{}", schema, name);
            }
        }
        FromClause::Subquery { query, .. } => qualify_select(query, schema, cte_scope),
        FromClause::Join { left, right, condition, .. } => {
            qualify_from(left, schema, cte_scope);
            qualify_from(right, schema, cte_scope);
            if let Some(cond) = condition {
                qualify_expr(cond, schema, cte_scope);
            }
        }
        FromClause::Values { rows, .. } => {
            for row in rows.iter_mut() {
                for expr in row.iter_mut() {
                    qualify_expr(expr, schema, cte_scope);
                }
            }
        }
        // A table-valued function name is a function, never a table.
        FromClause::TableFunction { args, .. } => {
            for expr in args.iter_mut() {
                qualify_expr(expr, schema, cte_scope);
            }
        }
    }
}

fn qualify_group_by(group_by: &mut GroupByClause, schema: &str, cte_scope: &mut Vec<String>) {
    match group_by {
        GroupByClause::Simple(exprs) => {
            for expr in exprs.iter_mut() {
                qualify_expr(expr, schema, cte_scope);
            }
        }
        GroupByClause::Rollup(elements) | GroupByClause::Cube(elements) => {
            for element in elements.iter_mut() {
                match element {
                    GroupingElement::Single(expr) => qualify_expr(expr, schema, cte_scope),
                    GroupingElement::Composite(exprs) => {
                        for expr in exprs.iter_mut() {
                            qualify_expr(expr, schema, cte_scope);
                        }
                    }
                }
            }
        }
        GroupByClause::GroupingSets(sets) => {
            for set in sets.iter_mut() {
                for expr in set.columns.iter_mut() {
                    qualify_expr(expr, schema, cte_scope);
                }
            }
        }
        GroupByClause::Mixed(items) => {
            for item in items.iter_mut() {
                match item {
                    MixedGroupingItem::Simple(expr) => qualify_expr(expr, schema, cte_scope),
                    MixedGroupingItem::Rollup(elements) | MixedGroupingItem::Cube(elements) => {
                        for element in elements.iter_mut() {
                            match element {
                                GroupingElement::Single(expr) => {
                                    qualify_expr(expr, schema, cte_scope)
                                }
                                GroupingElement::Composite(exprs) => {
                                    for expr in exprs.iter_mut() {
                                        qualify_expr(expr, schema, cte_scope);
                                    }
                                }
                            }
                        }
                    }
                    MixedGroupingItem::GroupingSets(sets) => {
                        for set in sets.iter_mut() {
                            for expr in set.columns.iter_mut() {
                                qualify_expr(expr, schema, cte_scope);
                            }
                        }
                    }
                }
            }
        }
    }
}

fn qualify_order_by(items: &mut [OrderByItem], schema: &str, cte_scope: &mut Vec<String>) {
    for item in items.iter_mut() {
        qualify_expr(&mut item.expr, schema, cte_scope);
    }
}

fn qualify_case_whens(clauses: &mut [CaseWhen], schema: &str, cte_scope: &mut Vec<String>) {
    for clause in clauses.iter_mut() {
        for cond in clause.conditions.iter_mut() {
            qualify_expr(cond, schema, cte_scope);
        }
        qualify_expr(&mut clause.result, schema, cte_scope);
    }
}

fn qualify_window_spec(spec: &mut WindowSpec, schema: &str, cte_scope: &mut Vec<String>) {
    if let Some(partition_by) = &mut spec.partition_by {
        for expr in partition_by.iter_mut() {
            qualify_expr(expr, schema, cte_scope);
        }
    }
    if let Some(order_by) = &mut spec.order_by {
        qualify_order_by(order_by, schema, cte_scope);
    }
    if let Some(frame) = &mut spec.frame {
        if let FrameBound::Preceding(expr) | FrameBound::Following(expr) = &mut frame.start {
            qualify_expr(expr, schema, cte_scope);
        }
        if let Some(FrameBound::Preceding(expr) | FrameBound::Following(expr)) = &mut frame.end {
            qualify_expr(expr, schema, cte_scope);
        }
    }
}

fn qualify_window_function(
    spec: &mut WindowFunctionSpec,
    schema: &str,
    cte_scope: &mut Vec<String>,
) {
    match spec {
        WindowFunctionSpec::Aggregate { args, filter, .. } => {
            for arg in args.iter_mut() {
                qualify_expr(arg, schema, cte_scope);
            }
            if let Some(filter_expr) = filter {
                qualify_expr(filter_expr, schema, cte_scope);
            }
        }
        WindowFunctionSpec::Ranking { args, .. } | WindowFunctionSpec::Value { args, .. } => {
            for arg in args.iter_mut() {
                qualify_expr(arg, schema, cte_scope);
            }
        }
    }
}

/// Mutable mirror of `vibesql_ast::visitor::walk_expression`.
///
/// The match is exhaustive on purpose (no `_ =>` arm): a new
/// subquery-bearing `Expression` variant must fail to compile here rather
/// than silently skip re-qualification and reintroduce the wrong-database
/// binding this module exists to prevent.
fn qualify_expr(expr: &mut Expression, schema: &str, cte_scope: &mut Vec<String>) {
    match expr {
        // Leaves: no table reference and no nested SELECT.
        Expression::Literal(_)
        | Expression::CollatedLiteral { .. }
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::ColumnRef(_)
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::DuplicateKeyValue { .. }
        | Expression::NextValue { .. }
        | Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. } => {}

        Expression::BinaryOp { left, right, .. }
        | Expression::IsDistinctFrom { left, right, .. } => {
            qualify_expr(left, schema, cte_scope);
            qualify_expr(right, schema, cte_scope);
        }

        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children.iter_mut() {
                qualify_expr(child, schema, cte_scope);
            }
        }

        Expression::UnaryOp { expr: inner, .. }
        | Expression::IsNull { expr: inner, .. }
        | Expression::IsTruthValue { expr: inner, .. }
        | Expression::Cast { expr: inner, .. }
        | Expression::Extract { expr: inner, .. }
        | Expression::Collate { expr: inner, .. } => qualify_expr(inner, schema, cte_scope),

        Expression::Function { args, .. } => {
            for arg in args.iter_mut() {
                qualify_expr(arg, schema, cte_scope);
            }
        }

        Expression::AggregateFunction { args, order_by, filter, .. } => {
            for arg in args.iter_mut() {
                qualify_expr(arg, schema, cte_scope);
            }
            if let Some(order_items) = order_by {
                qualify_order_by(order_items, schema, cte_scope);
            }
            if let Some(filter_expr) = filter {
                qualify_expr(filter_expr, schema, cte_scope);
            }
        }

        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                qualify_expr(op, schema, cte_scope);
            }
            qualify_case_whens(when_clauses, schema, cte_scope);
            if let Some(else_expr) = else_result {
                qualify_expr(else_expr, schema, cte_scope);
            }
        }

        Expression::ScalarSubquery(select) => qualify_select(select, schema, cte_scope),

        Expression::In { expr: inner, subquery, .. }
        | Expression::QuantifiedComparison { expr: inner, subquery, .. } => {
            qualify_expr(inner, schema, cte_scope);
            qualify_select(subquery, schema, cte_scope);
        }

        Expression::Exists { subquery, .. } => qualify_select(subquery, schema, cte_scope),

        Expression::InList { expr: inner, values, .. } => {
            qualify_expr(inner, schema, cte_scope);
            for value in values.iter_mut() {
                qualify_expr(value, schema, cte_scope);
            }
        }

        Expression::Between { expr: inner, low, high, .. } => {
            qualify_expr(inner, schema, cte_scope);
            qualify_expr(low, schema, cte_scope);
            qualify_expr(high, schema, cte_scope);
        }

        Expression::Position { substring, string, .. } => {
            qualify_expr(substring, schema, cte_scope);
            qualify_expr(string, schema, cte_scope);
        }

        Expression::Trim { removal_char, string, .. } => {
            if let Some(removal) = removal_char {
                qualify_expr(removal, schema, cte_scope);
            }
            qualify_expr(string, schema, cte_scope);
        }

        Expression::Like { expr: inner, pattern, escape, .. }
        | Expression::Glob { expr: inner, pattern, escape, .. } => {
            qualify_expr(inner, schema, cte_scope);
            qualify_expr(pattern, schema, cte_scope);
            if let Some(escape_expr) = escape {
                qualify_expr(escape_expr, schema, cte_scope);
            }
        }

        Expression::Interval { value, .. } => qualify_expr(value, schema, cte_scope),

        Expression::WindowFunction { function, over } => {
            qualify_window_function(function, schema, cte_scope);
            qualify_window_spec(over, schema, cte_scope);
        }

        Expression::MatchAgainst { search_modifier, .. } => {
            qualify_expr(search_modifier, schema, cte_scope)
        }

        Expression::RowValueConstructor(values) => {
            for value in values.iter_mut() {
                qualify_expr(value, schema, cte_scope);
            }
        }

        Expression::Raise { error_message, .. } => {
            if let Some(msg) = error_message {
                qualify_expr(msg, schema, cte_scope);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use vibesql_parser::parse_with_arena_fallback;

    use super::*;

    /// Parse a `SELECT`, re-qualify it against `aux`, and return the rewritten
    /// AST's FROM-clause table names in traversal order.
    fn qualified_table_names(sql: &str) -> Vec<String> {
        let vibesql_ast::Statement::Select(mut select) =
            parse_with_arena_fallback(sql).expect("parse")
        else {
            panic!("expected SELECT");
        };
        qualify_unqualified_tables(&mut select, "aux");

        let mut names = Vec::new();
        collect_from_names(&select, &mut names);
        names
    }

    fn collect_from_names(stmt: &SelectStmt, out: &mut Vec<String>) {
        if let Some(ctes) = &stmt.with_clause {
            for cte in ctes {
                collect_from_names(&cte.query, out);
            }
        }
        if let Some(from) = &stmt.from {
            collect_from(from, out);
        }
        if let Some(where_clause) = &stmt.where_clause {
            collect_expr(where_clause, out);
        }
        for item in &stmt.select_list {
            if let SelectItem::Expression { expr, .. } = item {
                collect_expr(expr, out);
            }
        }
        if let Some(set_op) = &stmt.set_operation {
            collect_from_names(&set_op.right, out);
        }
    }

    fn collect_from(from: &FromClause, out: &mut Vec<String>) {
        match from {
            // Rendered as `name AS alias` so the correlation-name behavior is
            // visible to the assertions below, not just the rewritten name.
            FromClause::Table { name, alias, .. } => out.push(match alias {
                Some(a) => format!("{} AS {}", name, a),
                None => name.clone(),
            }),
            FromClause::Subquery { query, .. } => collect_from_names(query, out),
            FromClause::Join { left, right, condition, .. } => {
                collect_from(left, out);
                collect_from(right, out);
                if let Some(cond) = condition {
                    collect_expr(cond, out);
                }
            }
            FromClause::TableFunction { name, .. } => out.push(format!("tvf:{}", name)),
            FromClause::Values { .. } => {}
        }
    }

    fn collect_expr(expr: &Expression, out: &mut Vec<String>) {
        match expr {
            Expression::ScalarSubquery(select) => collect_from_names(select, out),
            Expression::In { expr: inner, subquery, .. }
            | Expression::QuantifiedComparison { expr: inner, subquery, .. } => {
                collect_expr(inner, out);
                collect_from_names(subquery, out);
            }
            Expression::Exists { subquery, .. } => collect_from_names(subquery, out),
            Expression::BinaryOp { left, right, .. } => {
                collect_expr(left, out);
                collect_expr(right, out);
            }
            Expression::Conjunction(children) | Expression::Disjunction(children) => {
                for child in children {
                    collect_expr(child, out);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn qualifies_a_bare_from_table() {
        assert_eq!(qualified_table_names("SELECT x FROM t"), vec!["aux.t AS t"]);
    }

    #[test]
    fn keeps_the_bare_name_as_a_correlation_name_only_when_unaliased() {
        // `FROM t` -> `FROM aux.t AS t`, so a body that qualifies its columns
        // with the table's unqualified name (`t.x`) keeps resolving. An
        // existing alias is authoritative and must not be overwritten.
        assert_eq!(qualified_table_names("SELECT t.x FROM t"), vec!["aux.t AS t"]);
        assert_eq!(qualified_table_names("SELECT a.x FROM t AS a"), vec!["aux.t AS a"]);
        // Nothing is invented for a reference that was not rewritten.
        assert_eq!(qualified_table_names("SELECT x FROM main.t"), vec!["main.t"]);
    }

    #[test]
    fn leaves_an_already_qualified_reference_alone() {
        // `main.t` / `other.t` survived `strip_schema_qualifier` on the writer
        // side precisely because they name a *different* schema — bare means
        // "this attachment", qualified means "that schema".
        assert_eq!(qualified_table_names("SELECT x FROM main.t"), vec!["main.t"]);
        assert_eq!(qualified_table_names("SELECT x FROM other.t"), vec!["other.t"]);
    }

    #[test]
    fn qualifies_both_sides_of_a_join() {
        assert_eq!(
            qualified_table_names("SELECT a.x FROM t AS a JOIN u AS b ON a.x = b.x"),
            vec!["aux.t AS a", "aux.u AS b"]
        );
    }

    #[test]
    fn qualifies_inside_a_from_subquery() {
        assert_eq!(
            qualified_table_names("SELECT x FROM (SELECT x FROM t) AS s"),
            vec!["aux.t AS t"]
        );
    }

    #[test]
    fn qualifies_inside_a_scalar_subquery_and_exists() {
        assert_eq!(
            qualified_table_names("SELECT (SELECT MAX(x) FROM u) AS m FROM t"),
            vec!["aux.t AS t", "aux.u AS u"]
        );
        assert_eq!(
            qualified_table_names("SELECT x FROM t WHERE EXISTS (SELECT 1 FROM u)"),
            vec!["aux.t AS t", "aux.u AS u"]
        );
        assert_eq!(
            qualified_table_names("SELECT x FROM t WHERE x IN (SELECT y FROM u)"),
            vec!["aux.t AS t", "aux.u AS u"]
        );
    }

    #[test]
    fn qualifies_both_arms_of_a_set_operation() {
        assert_eq!(
            qualified_table_names("SELECT x FROM t UNION ALL SELECT y FROM u"),
            vec!["aux.t AS t", "aux.u AS u"]
        );
    }

    #[test]
    fn does_not_qualify_a_cte_name_but_does_qualify_its_body() {
        // `c` is bound by the WITH clause, so it is not a base table; the
        // table `t` *inside* the CTE body still is.
        assert_eq!(
            qualified_table_names("WITH c AS (SELECT x FROM t) SELECT x FROM c"),
            vec!["aux.t AS t", "c"]
        );
    }

    #[test]
    fn does_not_qualify_a_recursive_cte_self_reference() {
        let names = qualified_table_names(
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) \
             SELECT n FROM c",
        );
        assert!(
            names.iter().all(|n| n == "c"),
            "a recursive CTE's self-reference must not be schema-qualified, got {:?}",
            names
        );
    }

    #[test]
    fn does_not_qualify_a_table_valued_function() {
        assert_eq!(
            qualified_table_names("SELECT value FROM json_each('[1,2,3]')"),
            vec!["tvf:json_each"]
        );
    }
}
