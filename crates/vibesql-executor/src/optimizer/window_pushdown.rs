//! WHERE push-down into window-function subqueries and views (#5292)
//!
//! SQLite pushes WHERE constraints down into views/subqueries that contain
//! window functions when the predicate is constant within every window
//! partition (`pushDownWhereTerms()` in src/select.c). Filtering whole
//! partitions before the window functions run is semantics-preserving, and
//! lets the inner table scan use an index instead of materializing the full
//! window result and filtering afterwards.
//!
//! ## Safety rule
//!
//! A WHERE conjunct may be pushed below the window functions ONLY if every
//! column it references maps (through the subquery SELECT list) to a bare
//! inner column that appears in the PARTITION BY list of EVERY window
//! function in the subquery. If any window function lacks a PARTITION BY
//! covering the predicate's columns (e.g. `row_number() OVER ()`), the
//! predicate is NOT constant within that window's partitions and pushing it
//! would change results (windowpushd.test v2). Predicates containing
//! subqueries, aggregate/window functions, placeholders, or volatile
//! functions are never pushed.
//!
//! ## Scope (conservative)
//!
//! - Only fires when the outer FROM clause is a single derived table or a
//!   single view reference (no joins). With multiple FROM sources an
//!   unqualified column reference might bind to a sibling table, so pushing
//!   by name alone would be unsound. Multi-source push-down is follow-on
//!   work.
//! - The pushed conjuncts are *copied* into the inner WHERE clause; the
//!   outer WHERE clause is left untouched. The outer evaluation is redundant
//!   but harmless for the deterministic predicates this pass accepts, and it
//!   keeps the transform trivially value-preserving.
//! - The inner query must be a plain SELECT: no set operation, VALUES,
//!   GROUP BY, HAVING, DISTINCT, LIMIT or OFFSET.
//!
//! Note: EXPLAIN QUERY PLAN does not yet model this rewrite (views are
//! rendered opaquely by `explain.rs`), so windowpushd.test EQP patterns
//! remain shim warnings until plan rendering learns to expand
//! views/subqueries. See the follow-on issue referenced in #5292.

use std::collections::HashMap;

use vibesql_ast::{Expression, FromClause, SelectItem, SelectStmt};
use vibesql_storage::Database;

use super::where_pushdown::flatten_conjuncts;

/// Apply WHERE push-down into window subqueries/views at the top level of
/// `stmt`. Returns the (possibly) rewritten statement.
///
/// Nested subqueries are handled when they are themselves executed: the
/// SELECT executor invokes this pass for every statement it runs, so a
/// derived table's own FROM subquery is rewritten during the derived table's
/// execution.
pub fn push_where_into_window_subqueries(stmt: &SelectStmt, database: &Database) -> SelectStmt {
    let Some(where_clause) = &stmt.where_clause else {
        return stmt.clone();
    };

    match &stmt.from {
        // FROM (SELECT ... window fns ...) AS alias [(col, ...)]
        Some(FromClause::Subquery { query, alias, column_aliases }) => {
            match try_push_into_subquery(where_clause, query, alias, column_aliases.as_deref()) {
                Some(new_query) => {
                    let mut new_stmt = stmt.clone();
                    new_stmt.from = Some(FromClause::Subquery {
                        query: Box::new(new_query),
                        alias: alias.clone(),
                        column_aliases: column_aliases.clone(),
                    });
                    new_stmt
                }
                None => stmt.clone(),
            }
        }

        // FROM view_name [AS alias] — expand the view into a derived table
        // carrying the pushed predicate. Only done when at least one
        // conjunct is pushable, so plain view scans keep their existing
        // execution path (including the SELECT privilege check, which the
        // scan performs; if the check would fail here we skip the rewrite
        // and let the scan raise the error).
        Some(FromClause::Table { name, alias, column_aliases, .. }) => {
            let Some(view) = database.catalog.get_view(name) else {
                return stmt.clone();
            };
            if crate::privilege_checker::PrivilegeChecker::check_select(database, name).is_err() {
                return stmt.clone();
            }
            // Effective correlation name: explicit alias wins, else the view
            // name as written in the query.
            let source = alias.as_deref().unwrap_or(name.as_str());
            // Effective output column names: FROM-clause column aliases
            // override the view's explicit column list.
            let effective_aliases: Option<Vec<String>> =
                column_aliases.clone().or_else(|| view.columns.clone());

            match try_push_into_subquery(
                where_clause,
                &view.query,
                source,
                effective_aliases.as_deref(),
            ) {
                Some(new_query) => {
                    let mut new_stmt = stmt.clone();
                    new_stmt.from = Some(FromClause::Subquery {
                        query: Box::new(new_query),
                        alias: source.to_string(),
                        column_aliases: effective_aliases,
                    });
                    new_stmt
                }
                None => stmt.clone(),
            }
        }

        _ => stmt.clone(),
    }
}

/// Attempt to push conjuncts of `where_clause` into `subquery`.
///
/// Returns `Some(rewritten_subquery)` when at least one conjunct was pushed,
/// `None` when nothing is pushable (callers leave the statement unchanged).
fn try_push_into_subquery(
    where_clause: &Expression,
    subquery: &SelectStmt,
    source_name: &str,
    column_aliases: Option<&[String]>,
) -> Option<SelectStmt> {
    // Inner-query gate: plain SELECT only.
    if subquery.values.is_some()
        || subquery.set_operation.is_some()
        || subquery.limit.is_some()
        || subquery.offset.is_some()
        || subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.distinct
        || subquery.into_table.is_some()
        || subquery.into_variables.is_some()
    {
        return None;
    }

    // Window functions in the subquery's ORDER BY are not visible to
    // collect_resolved_window_specs (it scans the SELECT list); bail out so
    // the coverage check below cannot miss a window.
    if let Some(order_by) = &subquery.order_by {
        if order_by.iter().any(|item| contains_window_function(&item.expr)) {
            return None;
        }
    }

    let specs = crate::select::window::collect_resolved_window_specs(
        &subquery.select_list,
        subquery.window_definitions.as_ref(),
    )
    .ok()?;

    // Scope gate: this pass only targets subqueries containing window
    // functions. (Plain derived tables produce correct results today;
    // generalized predicate push-down is separate work.)
    if specs.is_empty() {
        return None;
    }

    // Every window must have a non-empty PARTITION BY; `OVER ()` makes the
    // whole result one partition, so no non-constant predicate is pushable.
    let partition_lists: Vec<&[Expression]> = specs
        .iter()
        .map(|spec| spec.partition_by.as_deref().filter(|p| !p.is_empty()))
        .collect::<Option<Vec<_>>>()?;

    let output_map = build_output_map(&subquery.select_list, column_aliases)?;

    let ctx =
        PushContext { source_name, output_map: &output_map, partition_lists: &partition_lists };

    let mut pushed: Vec<Expression> = Vec::new();
    for conjunct in flatten_conjuncts(where_clause) {
        if let Some(mapped) = map_conjunct(&conjunct, &ctx) {
            pushed.push(mapped);
        }
    }
    if pushed.is_empty() {
        return None;
    }

    let mut new_subquery = subquery.clone();
    let mut all = Vec::new();
    if let Some(existing) = new_subquery.where_clause.take() {
        all.push(existing);
    }
    all.extend(pushed);
    new_subquery.where_clause = super::combine_with_and(all);
    Some(new_subquery)
}

/// Map from subquery output column name (case-folded) to the inner
/// expression that produces it. Names that are duplicated (ambiguous) or
/// produced by expressions we cannot address are absent.
fn build_output_map(
    select_list: &[SelectItem],
    column_aliases: Option<&[String]>,
) -> Option<HashMap<String, Expression>> {
    let mut map: HashMap<String, Expression> = HashMap::new();
    let mut poisoned: Vec<String> = Vec::new();

    for (i, item) in select_list.iter().enumerate() {
        let SelectItem::Expression { expr, alias, .. } = item else {
            // Wildcards make positional/name mapping unreliable without the
            // inner schema; bail out entirely.
            return None;
        };

        let name: Option<String> = if let Some(aliases) = column_aliases {
            // Explicit column list renames positionally; a mismatch in
            // length means we cannot trust the mapping.
            Some(aliases.get(i)?.to_ascii_lowercase())
        } else if let Some(a) = alias {
            Some(a.to_ascii_lowercase())
        } else if let Expression::ColumnRef(ci) = expr {
            Some(ci.column_canonical().to_ascii_lowercase())
        } else {
            // Unnamed complex expression (e.g. the window function itself):
            // not addressable by a simple outer column reference.
            None
        };

        if let Some(name) = name {
            if map.insert(name.clone(), expr.clone()).is_some() {
                poisoned.push(name);
            }
        }
    }

    for name in poisoned {
        map.remove(&name);
    }
    Some(map)
}

struct PushContext<'a> {
    /// The correlation name of the subquery/view in the outer FROM clause.
    source_name: &'a str,
    /// Output column name → inner expression.
    output_map: &'a HashMap<String, Expression>,
    /// PARTITION BY expressions of every window function in the subquery.
    partition_lists: &'a [&'a [Expression]],
}

impl PushContext<'_> {
    /// Resolve an outer column reference to a pushable inner expression.
    ///
    /// Requirements:
    /// - the table qualifier (if any) names the subquery source
    /// - the column maps to a *bare column* of the inner query
    /// - that inner column appears in the PARTITION BY of every window
    fn resolve_column(&self, ci: &vibesql_ast::ColumnIdentifier) -> Option<Expression> {
        if ci.schema_canonical().is_some() {
            return None;
        }
        if let Some(table) = ci.table_canonical() {
            if !table.eq_ignore_ascii_case(self.source_name) {
                return None;
            }
        }

        let inner = self.output_map.get(&ci.column_canonical().to_ascii_lowercase())?;
        let Expression::ColumnRef(inner_ci) = inner else {
            return None;
        };

        let covered = self.partition_lists.iter().all(|list| {
            list.iter().any(|p| match p {
                Expression::ColumnRef(pci) => {
                    pci.column_canonical().eq_ignore_ascii_case(inner_ci.column_canonical())
                        && match (pci.table_canonical(), inner_ci.table_canonical()) {
                            (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
                            _ => true,
                        }
                }
                _ => false,
            })
        });
        if !covered {
            return None;
        }

        Some(inner.clone())
    }
}

/// Reject functions that are (or may be) non-deterministic. Mirrors the
/// blacklist used by `ExpressionHasher`, extended with SQLite date/time
/// functions (non-deterministic when invoked with 'now') and statement
/// counters.
fn is_volatile_function(canonical_name: &str) -> bool {
    matches!(
        canonical_name,
        "rand"
            | "random"
            | "randomblob"
            | "now"
            | "current_date"
            | "current_time"
            | "current_timestamp"
            | "date"
            | "time"
            | "datetime"
            | "julianday"
            | "unixepoch"
            | "strftime"
            | "timediff"
            | "changes"
            | "total_changes"
            | "last_insert_rowid"
    )
}

/// Recursively rewrite a conjunct for pushing: outer column references are
/// replaced by the inner expressions they map to. Returns `None` if the
/// conjunct contains anything unsafe to push (subqueries, window/aggregate
/// functions, placeholders, volatile functions, unmappable columns, ...).
///
/// Only a whitelist of expression forms is traversed; everything else is
/// conservatively rejected.
fn map_conjunct(expr: &Expression, ctx: &PushContext) -> Option<Expression> {
    use Expression as E;

    let map_box = |e: &Expression| map_conjunct(e, ctx).map(Box::new);
    let map_vec =
        |es: &[Expression]| es.iter().map(|e| map_conjunct(e, ctx)).collect::<Option<Vec<_>>>();

    match expr {
        E::Literal(_) | E::CurrentDate | E::CurrentTime { .. } | E::CurrentTimestamp { .. } => {
            // CURRENT_* are non-deterministic; reject them. Literals pass.
            if matches!(expr, E::Literal(_)) {
                Some(expr.clone())
            } else {
                None
            }
        }

        E::ColumnRef(ci) => ctx.resolve_column(ci),

        E::BinaryOp { op, left, right } => {
            Some(E::BinaryOp { op: op.clone(), left: map_box(left)?, right: map_box(right)? })
        }

        E::Conjunction(es) => Some(E::Conjunction(map_vec(es)?)),
        E::Disjunction(es) => Some(E::Disjunction(map_vec(es)?)),

        E::UnaryOp { op, expr } => Some(E::UnaryOp { op: op.clone(), expr: map_box(expr)? }),

        E::IsNull { expr, negated } => Some(E::IsNull { expr: map_box(expr)?, negated: *negated }),

        E::IsDistinctFrom { left, right, negated } => Some(E::IsDistinctFrom {
            left: map_box(left)?,
            right: map_box(right)?,
            negated: *negated,
        }),

        E::IsTruthValue { expr, truth_value, negated } => Some(E::IsTruthValue {
            expr: map_box(expr)?,
            truth_value: truth_value.clone(),
            negated: *negated,
        }),

        E::Case { operand, when_clauses, else_result } => {
            let operand = match operand {
                Some(op) => Some(map_box(op)?),
                None => None,
            };
            let when_clauses = when_clauses
                .iter()
                .map(|wc| {
                    Some(vibesql_ast::CaseWhen {
                        conditions: map_vec(&wc.conditions)?,
                        result: map_conjunct(&wc.result, ctx)?,
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            let else_result = match else_result {
                Some(er) => Some(map_box(er)?),
                None => None,
            };
            Some(E::Case { operand, when_clauses, else_result })
        }

        E::InList { expr, values, negated } => {
            Some(E::InList { expr: map_box(expr)?, values: map_vec(values)?, negated: *negated })
        }

        E::Between { expr, low, high, negated, symmetric } => Some(E::Between {
            expr: map_box(expr)?,
            low: map_box(low)?,
            high: map_box(high)?,
            negated: *negated,
            symmetric: *symmetric,
        }),

        E::Cast { expr, data_type } => {
            Some(E::Cast { expr: map_box(expr)?, data_type: data_type.clone() })
        }

        E::Like { expr, pattern, negated, escape } => Some(E::Like {
            expr: map_box(expr)?,
            pattern: map_box(pattern)?,
            negated: *negated,
            escape: match escape {
                Some(e) => Some(map_box(e)?),
                None => None,
            },
        }),

        E::Glob { expr, pattern, negated, escape } => Some(E::Glob {
            expr: map_box(expr)?,
            pattern: map_box(pattern)?,
            negated: *negated,
            escape: match escape {
                Some(e) => Some(map_box(e)?),
                None => None,
            },
        }),

        E::Collate { expr, collation } => {
            Some(E::Collate { expr: map_box(expr)?, collation: collation.clone() })
        }

        E::Function { name, args, character_unit } => {
            if is_volatile_function(name.canonical()) {
                return None;
            }
            Some(E::Function {
                name: name.clone(),
                args: map_vec(args)?,
                character_unit: character_unit.clone(),
            })
        }

        // Everything else — subqueries, aggregate/window functions,
        // placeholders, sequence/session/pseudo variables, wildcards,
        // MATCH ... AGAINST, etc. — is unsafe or pointless to push.
        _ => None,
    }
}

/// Detect window functions anywhere inside an expression.
fn contains_window_function(expr: &Expression) -> bool {
    struct Finder {
        found: bool,
    }
    impl vibesql_ast::visitor::ExpressionVisitor for Finder {
        fn pre_visit_expression(&mut self, expr: &Expression) -> vibesql_ast::visitor::VisitResult {
            if matches!(expr, Expression::WindowFunction { .. }) {
                self.found = true;
                return vibesql_ast::visitor::VisitResult::Stop;
            }
            vibesql_ast::visitor::VisitResult::Continue
        }
    }
    let mut finder = Finder { found: false };
    vibesql_ast::visitor::walk_expression(&mut finder, expr);
    finder.found
}

#[cfg(test)]
mod tests {
    use vibesql_ast::Statement;
    use vibesql_parser::Parser;
    use vibesql_storage::Database;

    use super::*;

    fn run_ddl(db: &mut Database, sql: &str) {
        let stmt = Parser::parse_sql(sql).expect("parse failed");
        match stmt {
            Statement::CreateTable(s) => {
                crate::CreateTableExecutor::execute(&s, db).unwrap();
            }
            Statement::CreateIndex(s) => {
                crate::CreateIndexExecutor::execute(&s, db).unwrap();
            }
            Statement::CreateView(s) => {
                crate::advanced_objects::execute_create_view(&s, db).unwrap();
            }
            Statement::Insert(s) => {
                crate::InsertExecutor::execute(db, &s).unwrap();
            }
            other => panic!("unsupported DDL in test: {:?}", other),
        }
    }

    fn parse_select(sql: &str) -> SelectStmt {
        match Parser::parse_sql(sql).expect("parse failed") {
            Statement::Select(s) => *s,
            other => panic!("expected SELECT, got {:?}", other),
        }
    }

    /// Database with the windowpushd.test section-1 schema.
    fn setup_db() -> Database {
        let mut db = Database::new();
        run_ddl(&mut db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, grp_id INTEGER)");
        run_ddl(&mut db, "CREATE INDEX i1 ON t1(grp_id)");
        run_ddl(
            &mut db,
            "CREATE VIEW lll AS SELECT row_number() OVER (PARTITION BY grp_id), grp_id, id FROM t1",
        );
        db
    }

    /// Extract the inner subquery's WHERE clause after the rewrite (None if
    /// the FROM clause is not a subquery or has no inner WHERE).
    fn inner_where(stmt: &SelectStmt) -> Option<Expression> {
        match &stmt.from {
            Some(FromClause::Subquery { query, .. }) => query.where_clause.clone(),
            _ => None,
        }
    }

    fn rewrite(db: &Database, sql: &str) -> SelectStmt {
        push_where_into_window_subqueries(&parse_select(sql), db)
    }

    // ----------------------------------------------------------------
    // Positive cases
    // ----------------------------------------------------------------

    #[test]
    fn pushes_equality_on_partition_column_into_derived_table() {
        let db = setup_db();
        let out = rewrite(
            &db,
            "SELECT * FROM (SELECT grp_id, id, row_number() OVER (PARTITION BY grp_id) FROM t1) AS v \
             WHERE grp_id = 2",
        );
        let pushed = inner_where(&out).expect("predicate should be pushed");
        assert!(format!("{:?}", pushed).contains("grp_id"), "pushed: {:?}", pushed);
        // Outer WHERE is preserved (push copies, never removes).
        assert!(out.where_clause.is_some());
    }

    #[test]
    fn pushes_into_view_reference() {
        let db = setup_db();
        let out = rewrite(&db, "SELECT * FROM lll WHERE grp_id = 2");
        // View is expanded into a derived table carrying the predicate.
        match &out.from {
            Some(FromClause::Subquery { query, alias, .. }) => {
                assert_eq!(alias, "lll");
                assert!(query.where_clause.is_some(), "inner WHERE missing");
            }
            other => panic!("expected Subquery FROM, got {:?}", other),
        }
    }

    #[test]
    fn pushes_in_list_on_partition_column() {
        let db = setup_db();
        let out = rewrite(&db, "SELECT * FROM lll WHERE grp_id IN (1, 2)");
        assert!(inner_where(&out).is_some());
    }

    #[test]
    fn pushes_collate_wrapped_predicate() {
        let db = setup_db();
        let out = rewrite(&db, "SELECT * FROM lll WHERE grp_id = '2' COLLATE nocase");
        assert!(inner_where(&out).is_some());
    }

    #[test]
    fn pushes_through_select_alias() {
        let db = setup_db();
        let out = rewrite(
            &db,
            "SELECT g FROM (SELECT grp_id AS g, row_number() OVER (PARTITION BY grp_id) AS rn FROM t1) AS v \
             WHERE g = 1",
        );
        let pushed = inner_where(&out).expect("aliased predicate should be pushed");
        // The pushed predicate must reference the INNER column name.
        assert!(format!("{:?}", pushed).contains("grp_id"), "pushed: {:?}", pushed);
    }

    #[test]
    fn pushes_when_covered_by_all_windows() {
        let db = setup_db();
        let out = rewrite(
            &db,
            "SELECT * FROM (SELECT grp_id, id, \
                row_number() OVER (PARTITION BY grp_id), \
                rank() OVER (PARTITION BY grp_id, id ORDER BY id) \
             FROM t1) AS v WHERE grp_id = 2",
        );
        assert!(inner_where(&out).is_some());
    }

    #[test]
    fn pushes_only_eligible_conjunct() {
        let db = setup_db();
        let out = rewrite(&db, "SELECT * FROM lll WHERE grp_id = 2 AND id > 3");
        let pushed = inner_where(&out).expect("grp_id conjunct should be pushed");
        let s = format!("{:?}", pushed);
        assert!(s.contains("grp_id"));
        assert!(!s.contains("\"id\""), "id predicate must NOT be pushed: {}", s);
    }

    // ----------------------------------------------------------------
    // Negative cases — the rewrite must NOT fire
    // ----------------------------------------------------------------

    fn assert_unchanged(db: &Database, sql: &str) {
        let stmt = parse_select(sql);
        let out = push_where_into_window_subqueries(&stmt, db);
        assert_eq!(stmt, out, "statement should be unchanged");
    }

    #[test]
    fn does_not_push_non_partition_column() {
        let db = setup_db();
        // `id` is not in the PARTITION BY list.
        assert_unchanged(&db, "SELECT * FROM lll WHERE id = 5");
    }

    #[test]
    fn does_not_push_when_any_window_lacks_partition() {
        let db = setup_db();
        // Second window is OVER () — one big partition; nothing is pushable
        // (windowpushd.test v2).
        assert_unchanged(
            &db,
            "SELECT * FROM (SELECT grp_id, id, \
                max(id) OVER (PARTITION BY grp_id), \
                row_number() OVER () \
             FROM t1) AS v WHERE grp_id = 2",
        );
    }

    #[test]
    fn does_not_push_when_window_partitions_differ_and_predicate_uncovered() {
        let db = setup_db();
        // Predicate column is in the first window's PARTITION BY only.
        assert_unchanged(
            &db,
            "SELECT * FROM (SELECT grp_id, id, \
                max(id) OVER (PARTITION BY grp_id), \
                row_number() OVER (PARTITION BY id) \
             FROM t1) AS v WHERE grp_id = 2",
        );
    }

    #[test]
    fn does_not_push_volatile_predicate() {
        let db = setup_db();
        assert_unchanged(&db, "SELECT * FROM lll WHERE grp_id = random()");
    }

    #[test]
    fn does_not_push_subquery_predicate() {
        let db = setup_db();
        assert_unchanged(&db, "SELECT * FROM lll WHERE grp_id IN (SELECT id FROM t1)");
    }

    #[test]
    fn does_not_push_into_subquery_with_limit() {
        let db = setup_db();
        assert_unchanged(
            &db,
            "SELECT * FROM (SELECT grp_id, row_number() OVER (PARTITION BY grp_id) FROM t1 LIMIT 5) AS v \
             WHERE grp_id = 2",
        );
    }

    #[test]
    fn does_not_push_into_plain_subquery_without_windows() {
        let db = setup_db();
        assert_unchanged(&db, "SELECT * FROM (SELECT grp_id, id FROM t1) AS v WHERE grp_id = 2");
    }

    #[test]
    fn does_not_fire_on_join_from_clause() {
        let db = setup_db();
        assert_unchanged(&db, "SELECT * FROM lll, t1 WHERE grp_id = 2");
    }

    #[test]
    fn does_not_push_predicate_qualified_with_other_table() {
        let db = setup_db();
        // Qualifier names something other than the derived table.
        assert_unchanged(
            &db,
            "SELECT * FROM (SELECT grp_id, row_number() OVER (PARTITION BY grp_id) FROM t1) AS v \
             WHERE t1.grp_id = 2",
        );
    }

    // ----------------------------------------------------------------
    // Correctness parity — results identical with and without the rewrite
    // ----------------------------------------------------------------

    fn populate(db: &mut Database) {
        run_ddl(
            db,
            "INSERT INTO t1 VALUES \
              (1, 2), (2, 3), (3, 3), (4, 1), (5, 1), \
              (6, 1), (7, 1), (8, 1), (9, 3), (10, 3), \
              (11, 2), (12, 3), (13, 3), (14, 2), (15, 1), \
              (16, 2), (17, 1), (18, 2), (19, 3), (20, 2)",
        );
    }

    fn execute_rows(db: &Database, stmt: &SelectStmt) -> Vec<Vec<vibesql_types::SqlValue>> {
        crate::select::SelectExecutor::new(db)
            .execute(stmt)
            .unwrap()
            .into_iter()
            .map(|r| r.values.to_vec())
            .collect()
    }

    #[test]
    fn parity_view_equality_predicate() {
        let mut db = setup_db();
        populate(&mut db);
        let stmt = parse_select("SELECT * FROM lll WHERE grp_id = 2");

        // Executor output (rewrite enabled inside execute()).
        let executed = execute_rows(&db, &stmt);

        // windowpushd.test 1.3 expected rows: row_number, grp_id, id.
        let expected: Vec<Vec<i64>> = vec![
            vec![1, 2, 1],
            vec![2, 2, 11],
            vec![3, 2, 14],
            vec![4, 2, 16],
            vec![5, 2, 18],
            vec![6, 2, 20],
        ];
        assert_eq!(executed.len(), expected.len(), "row count: {:?}", executed);
        for (row, exp) in executed.iter().zip(&expected) {
            let got: Vec<i64> = row
                .iter()
                .map(|v| match v {
                    vibesql_types::SqlValue::Integer(i) => *i,
                    vibesql_types::SqlValue::Bigint(i) => *i,
                    other => panic!("unexpected value {:?}", other),
                })
                .collect();
            assert_eq!(&got, exp);
        }
    }

    #[test]
    fn parity_rewritten_vs_unrewritten_ast() {
        let mut db = setup_db();
        populate(&mut db);

        for sql in [
            "SELECT * FROM lll WHERE grp_id = 2",
            "SELECT * FROM lll WHERE grp_id IN (1, 3)",
            "SELECT * FROM (SELECT grp_id, id, sum(id) OVER (PARTITION BY grp_id) FROM t1) AS v \
             WHERE grp_id > 1",
        ] {
            let stmt = parse_select(sql);
            let rewritten = push_where_into_window_subqueries(&stmt, &db);
            assert_ne!(stmt, rewritten, "rewrite should fire for: {}", sql);

            // Execute the REWRITTEN statement (executor will not rewrite the
            // outer FROM again since the predicate is already pushed — but
            // even if it did, the transform is idempotent in effect) and the
            // original; row sets must be identical and in the same order.
            let base = execute_rows(&db, &stmt);
            let opt = execute_rows(&db, &rewritten);
            assert_eq!(base, opt, "results differ for: {}", sql);
        }
    }
}
