//! Whole-schema re-validation for `ALTER TABLE ... DROP COLUMN`.
//!
//! SQLite re-parses the entire schema twice when a column is dropped
//! (`alter.c`, `renameTestSchema`):
//!
//! 1. **Pre-check** (`zWhen = ""`): any view or trigger that is *already*
//!    broken — independent of the drop — aborts the ALTER with
//!    `error in <type> <name>: <inner error>`.
//! 2. **Post-check** (`zWhen = "after drop column"`): the schema as it would
//!    look *after* the drop is re-resolved; a view/trigger/table-level CHECK
//!    left dangling by the drop aborts with
//!    `error in <type> <name> after drop column: <inner error>`.
//!
//! Both checks run before any mutation, so a failed DROP COLUMN leaves the
//! schema and rows untouched (atomicity for free). See issue #5795 /
//! `alterdropcol.test` sections 3 and 5.
//!
//! The static resolvers here are deliberately **conservative**: they only
//! report a missing column when the query scope is fully understood (plain
//! tables/views in FROM, no CTEs / set operations / derived tables). Anything
//! more complex is skipped rather than risk a false positive that would block
//! a DROP COLUMN SQLite allows. Constructs we do not descend into are treated
//! as valid (false negatives only).

use vibesql_ast::{
    visitor::{walk_expression, walk_statement, ExpressionVisitor, StatementVisitor, VisitResult},
    ColumnConstraintKind, ColumnIdentifier, Expression, FromClause, PseudoTable, SelectItem,
    Statement, TableConstraintKind, TriggerAction,
};
use vibesql_catalog::{TableSchema, TriggerDefinition, ViewDefinition};
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Pre-drop check: abort if any existing view or trigger is already broken
/// (references a column that does not resolve against the *current* schema).
///
/// Matches SQLite's first schema re-parse: the inner error carries no
/// "after drop column" suffix because the object was broken before the drop.
pub(super) fn precheck_schema_objects(database: &Database) -> Result<(), ExecutorError> {
    check_schema_objects(database, None)
}

/// Post-drop check: abort if dropping `column` from `table_name` would leave a
/// dependent table-level CHECK, view, or trigger referencing a gone column.
///
/// Resolution runs against a *simulated* post-drop schema (the altered table's
/// column set minus `column`), so nothing needs to be rolled back on failure.
pub(super) fn postcheck_schema_objects(
    database: &Database,
    table_name: &str,
    column: &str,
) -> Result<(), ExecutorError> {
    // Table-level CHECK constraints (and column-level CHECKs owned by *other*
    // columns) on the altered table must still resolve after the drop. The
    // in-memory `check_constraints` list has no column/table origin, so the
    // origin is recovered from the verbatim CREATE TABLE text (`sql_source`),
    // exactly the text SQLite itself re-parses. A CHECK attached to the
    // dropped column is removed together with the column and is exempt.
    if let Some(table) = database.get_table(table_name) {
        if let Some(missing) = check_constraint_missing_column(&table.schema, column) {
            return Err(ExecutorError::Other(format!(
                "error in table {} after drop column: no such column: {}",
                table_name, missing
            )));
        }
    }

    check_schema_objects(database, Some((table_name, column)))
}

/// Shared pre-/post-check walk over every view and trigger in the schema.
///
/// `dropped` is `None` for the pre-check (validate against the current
/// schema) and `Some((table, column))` for the post-check (validate against
/// the schema as it would look after dropping `column` from `table`).
fn check_schema_objects(
    database: &Database,
    dropped: Option<(&str, &str)>,
) -> Result<(), ExecutorError> {
    let suffix = if dropped.is_some() { " after drop column" } else { "" };

    // Resolve the altered table to its canonical catalog name once so FROM
    // references in any spelling ("T1", "main.t1", ...) simulate the drop.
    let sim = DropSimulation::new(database, dropped);

    for name in database.catalog.list_views() {
        if let Some(view) = database.catalog.get_view(&name) {
            if let Some(missing) = find_missing_column_in_view(view, &sim) {
                return Err(ExecutorError::Other(format!(
                    "error in view {}{}: no such column: {}",
                    view.name, suffix, missing
                )));
            }
        }
    }

    for name in database.catalog.list_triggers() {
        if let Some(trigger) = database.catalog.get_trigger(&name) {
            if let Some(missing) = find_missing_pseudo_in_trigger(trigger, &sim) {
                return Err(ExecutorError::Other(format!(
                    "error in trigger {}{}: no such column: {}",
                    trigger.name, suffix, missing
                )));
            }
        }
    }

    Ok(())
}

// ============================================================================
// Simulated post-drop schema resolution
// ============================================================================

/// Column resolution against the schema, optionally simulating the drop of
/// one column from one table (the post-check view of the world).
struct DropSimulation<'a> {
    db: &'a Database,
    /// `(canonical altered-table name, dropped column)`; `None` = pre-check.
    dropped: Option<(String, &'a str)>,
}

impl<'a> DropSimulation<'a> {
    fn new(db: &'a Database, dropped: Option<(&str, &'a str)>) -> Self {
        let dropped = dropped.map(|(table, column)| {
            let canonical = db
                .catalog
                .get_table(table)
                .map(|s| s.name.clone())
                .unwrap_or_else(|| table.to_string());
            (canonical, column)
        });
        DropSimulation { db, dropped }
    }

    /// The (possibly simulated) column names of a FROM-clause relation, or
    /// `None` when they cannot be determined statically (unknown relation, or
    /// a view without a resolved column list) — the caller then skips
    /// validation of the referencing object.
    fn columns_of_relation(&self, name: &str) -> Option<Vec<String>> {
        if let Some(schema) = self.db.catalog.get_table(name) {
            let mut cols: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            if let Some((altered, dropped_col)) = &self.dropped {
                if schema.name.eq_ignore_ascii_case(altered) {
                    cols.retain(|c| !c.eq_ignore_ascii_case(dropped_col));
                }
            }
            return Some(cols);
        }
        if let Some(view) = self.db.catalog.get_view(name) {
            // A lax-created (unresolved) view has no column list; validating
            // objects built on top of it is skipped — the broken view itself
            // is reported directly when its own turn comes.
            return view.columns.clone();
        }
        None
    }
}

// ============================================================================
// View validation
// ============================================================================

/// One resolvable relation in a view's FROM clause.
struct FromSource {
    /// Lookup key: the alias if given, else the (unqualified) table name.
    /// Stored lowercase for comparison against `ColumnIdentifier` canonicals.
    key: String,
    columns: Vec<String>,
}

/// First column reference in `view`'s defining query that does not resolve
/// against the (possibly simulated) schema, in SQL text order — or `None`
/// when the view is valid or too complex to validate statically.
fn find_missing_column_in_view(view: &ViewDefinition, sim: &DropSimulation) -> Option<String> {
    let query = &view.query;

    // Constructs that introduce additional name scopes are skipped wholesale:
    // resolving them faithfully would duplicate the planner, and a false
    // positive here would block a DROP COLUMN that SQLite allows.
    if query.with_clause.is_some() || query.values.is_some() || query.set_operation.is_some() {
        return None;
    }
    let from = query.from.as_ref()?;

    let mut sources = Vec::new();
    if !collect_from_sources(from, sim, &mut sources) {
        return None;
    }

    // Select-list aliases are legal targets for references in WHERE / GROUP
    // BY / HAVING / ORDER BY (SQLite is lenient here); accepting them
    // everywhere can only under-report, never false-positive.
    let aliases: Vec<String> = query
        .select_list
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expression { alias: Some(a), .. } => Some(a.clone()),
            _ => None,
        })
        .collect();

    let scope = ViewScope { sources: &sources, aliases: &aliases };

    // Walk in SQL text order so the *first* dangling reference is reported,
    // matching SQLite's message (e.g. `d` for `SELECT d, e FROM p1`).
    for item in &query.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if let Some(missing) = first_missing_in_expr(expr, &scope) {
                return Some(missing);
            }
        }
    }
    if let Some(missing) = first_missing_in_join_conditions(from, &scope) {
        return Some(missing);
    }
    if let Some(where_clause) = &query.where_clause {
        if let Some(missing) = first_missing_in_expr(where_clause, &scope) {
            return Some(missing);
        }
    }
    if let Some(group_by) = &query.group_by {
        for expr in group_by.all_expressions() {
            if let Some(missing) = first_missing_in_expr(expr, &scope) {
                return Some(missing);
            }
        }
    }
    if let Some(having) = &query.having {
        if let Some(missing) = first_missing_in_expr(having, &scope) {
            return Some(missing);
        }
    }
    if let Some(order_by) = &query.order_by {
        for item in order_by {
            if let Some(missing) = first_missing_in_expr(&item.expr, &scope) {
                return Some(missing);
            }
        }
    }

    None
}

/// Flatten a FROM tree into per-relation column sets. Returns `false` (skip
/// the whole view) on derived tables, VALUES sources, or relations whose
/// columns cannot be determined.
fn collect_from_sources(
    from: &FromClause,
    sim: &DropSimulation,
    out: &mut Vec<FromSource>,
) -> bool {
    match from {
        FromClause::Table { name, alias, column_aliases, .. } => {
            let Some(mut cols) = sim.columns_of_relation(name) else {
                return false;
            };
            if let Some(aliases) = column_aliases {
                cols = aliases.clone();
            }
            let key = alias
                .clone()
                .unwrap_or_else(|| name.rsplit('.').next().unwrap_or(name).to_string());
            out.push(FromSource { key: key.to_ascii_lowercase(), columns: cols });
            true
        }
        FromClause::Join { left, right, .. } => {
            collect_from_sources(left, sim, out) && collect_from_sources(right, sim, out)
        }
        FromClause::Subquery { .. } | FromClause::Values { .. } => false,
    }
}

/// First dangling reference in any JOIN `ON` condition of the FROM tree.
fn first_missing_in_join_conditions(from: &FromClause, scope: &ViewScope) -> Option<String> {
    match from {
        FromClause::Join { left, right, condition, .. } => {
            if let Some(missing) = first_missing_in_join_conditions(left, scope) {
                return Some(missing);
            }
            if let Some(missing) = first_missing_in_join_conditions(right, scope) {
                return Some(missing);
            }
            condition.as_ref().and_then(|cond| first_missing_in_expr(cond, scope))
        }
        _ => None,
    }
}

struct ViewScope<'a> {
    sources: &'a [FromSource],
    aliases: &'a [String],
}

/// SQLite's implicit rowid pseudo-columns, always accepted.
fn is_rowid_pseudo(name: &str) -> bool {
    matches!(name, "rowid" | "oid" | "_rowid_")
}

/// First unresolvable `ColumnRef` in `expr` (pre-order), or `None`.
///
/// Only expression shapes enumerated here are descended into; anything else
/// (subqueries, window functions, ...) is treated as valid — conservative in
/// the direction that never blocks a legal DROP COLUMN.
fn first_missing_in_expr(expr: &Expression, scope: &ViewScope) -> Option<String> {
    match expr {
        Expression::ColumnRef(col) => check_column_ref(col, scope),
        Expression::BinaryOp { left, right, .. } => {
            first_missing_in_expr(left, scope).or_else(|| first_missing_in_expr(right, scope))
        }
        Expression::IsDistinctFrom { left, right, .. } => {
            first_missing_in_expr(left, scope).or_else(|| first_missing_in_expr(right, scope))
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            children.iter().find_map(|c| first_missing_in_expr(c, scope))
        }
        Expression::UnaryOp { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::IsTruthValue { expr, .. }
        | Expression::Collate { expr, .. }
        | Expression::Cast { expr, .. } => first_missing_in_expr(expr, scope),
        Expression::Function { args, .. } => {
            args.iter().find_map(|a| first_missing_in_expr(a, scope))
        }
        Expression::AggregateFunction { args, filter, .. } => args
            .iter()
            .find_map(|a| first_missing_in_expr(a, scope))
            .or_else(|| filter.as_ref().and_then(|f| first_missing_in_expr(f, scope))),
        Expression::Case { operand, when_clauses, else_result } => operand
            .as_ref()
            .and_then(|o| first_missing_in_expr(o, scope))
            .or_else(|| {
                when_clauses.iter().find_map(|w| {
                    w.conditions
                        .iter()
                        .find_map(|c| first_missing_in_expr(c, scope))
                        .or_else(|| first_missing_in_expr(&w.result, scope))
                })
            })
            .or_else(|| else_result.as_ref().and_then(|e| first_missing_in_expr(e, scope))),
        Expression::InList { expr, values, .. } => first_missing_in_expr(expr, scope)
            .or_else(|| values.iter().find_map(|e| first_missing_in_expr(e, scope))),
        Expression::Between { expr, low, high, .. } => first_missing_in_expr(expr, scope)
            .or_else(|| first_missing_in_expr(low, scope))
            .or_else(|| first_missing_in_expr(high, scope)),
        Expression::Like { expr, pattern, .. } => {
            first_missing_in_expr(expr, scope).or_else(|| first_missing_in_expr(pattern, scope))
        }
        Expression::Glob { expr, pattern, .. } => {
            first_missing_in_expr(expr, scope).or_else(|| first_missing_in_expr(pattern, scope))
        }
        Expression::RowValueConstructor(values) => {
            values.iter().find_map(|v| first_missing_in_expr(v, scope))
        }
        // Subqueries, window functions, and anything not enumerated above:
        // do not descend (assume valid).
        _ => None,
    }
}

/// Resolve one column reference against the view scope. Returns the display
/// name of the column when it is definitely unresolvable, `None` otherwise
/// (resolved, or too ambiguous to judge).
fn check_column_ref(col: &ColumnIdentifier, scope: &ViewScope) -> Option<String> {
    // Schema-qualified references are rare in view bodies; skip rather than
    // model schema resolution here.
    if col.schema_canonical().is_some() {
        return None;
    }
    let name = col.column_canonical();
    if is_rowid_pseudo(name) {
        return None;
    }

    if let Some(table) = col.table_canonical() {
        match scope.sources.iter().find(|s| s.key == table) {
            // Unknown qualifier: conservatively assume something we did not
            // model (rather than report the column as missing).
            None => None,
            Some(source) => {
                if source.columns.iter().any(|c| c.eq_ignore_ascii_case(name)) {
                    None
                } else {
                    Some(col.column_display().to_string())
                }
            }
        }
    } else if scope.sources.iter().any(|s| s.columns.iter().any(|c| c.eq_ignore_ascii_case(name)))
        || scope.aliases.iter().any(|a| a.eq_ignore_ascii_case(name))
    {
        None
    } else {
        Some(col.column_display().to_string())
    }
}

// ============================================================================
// Trigger validation
// ============================================================================

/// First `NEW.<col>` / `OLD.<col>` reference in `trigger` (WHEN condition
/// first, then the body statements in order) that does not name a column of
/// the trigger's target table — formatted as `new.<col>` / `old.<col>` for the
/// error message. `None` when the trigger is valid or cannot be judged.
fn find_missing_pseudo_in_trigger(
    trigger: &TriggerDefinition,
    sim: &DropSimulation,
) -> Option<String> {
    // NEW/OLD resolve against the trigger's target table (or view for
    // INSTEAD OF triggers). If the target's columns cannot be determined,
    // skip the trigger.
    let columns = sim.columns_of_relation(&trigger.table_name)?;

    let mut refs: Vec<(PseudoTable, String)> = Vec::new();
    if let Some(when) = &trigger.when_condition {
        collect_pseudo_refs_in_expr(when, &mut refs);
    }
    let TriggerAction::RawSql(sql) = &trigger.triggered_action;
    match crate::trigger_execution::TriggerFirer::parse_trigger_sql(sql) {
        Ok(statements) => {
            for stmt in &statements {
                collect_pseudo_refs_in_statement(stmt, &mut refs);
            }
        }
        // Unparseable body: cannot judge, skip.
        Err(_) => return None,
    }

    for (pseudo, column) in refs {
        if is_rowid_pseudo(&column.to_ascii_lowercase()) {
            continue;
        }
        if !columns.iter().any(|c| c.eq_ignore_ascii_case(&column)) {
            let prefix = match pseudo {
                PseudoTable::New => "new",
                PseudoTable::Old => "old",
            };
            return Some(format!("{}.{}", prefix, column));
        }
    }
    None
}

/// Visitor collecting every `NEW.x` / `OLD.x` reference in encounter order.
struct PseudoRefCollector<'a> {
    refs: &'a mut Vec<(PseudoTable, String)>,
}

impl ExpressionVisitor for PseudoRefCollector<'_> {
    fn visit_pseudo_variable(&mut self, pseudo_table: &PseudoTable, column: &str) -> VisitResult {
        self.refs.push((*pseudo_table, column.to_string()));
        VisitResult::Continue
    }
}

impl StatementVisitor for PseudoRefCollector<'_> {}

fn collect_pseudo_refs_in_expr(expr: &Expression, refs: &mut Vec<(PseudoTable, String)>) {
    let mut collector = PseudoRefCollector { refs };
    walk_expression(&mut collector, expr);
}

fn collect_pseudo_refs_in_statement(stmt: &Statement, refs: &mut Vec<(PseudoTable, String)>) {
    let mut collector = PseudoRefCollector { refs };
    walk_statement(&mut collector, stmt);
}

// ============================================================================
// Table-level CHECK validation (origin recovered from the CREATE TABLE text)
// ============================================================================

/// Display name of the first reference to `dropped` inside a CHECK constraint
/// that would *survive* the drop: a table-level CHECK, or a column-level CHECK
/// attached to a column other than the dropped one. A CHECK attached to the
/// dropped column itself is removed with the column (SQLite semantics,
/// alterdropcol.test 3.2) and never reported.
///
/// Column-vs-table origin is not tracked in `TableSchema::check_constraints`,
/// so it is recovered by re-parsing the verbatim `CREATE TABLE` text — the
/// same text SQLite's own re-parse reads. Without `sql_source` (schema built
/// programmatically) the origin is unknown and the legacy behavior (silently
/// dropping CHECKs that reference the column) is kept.
fn check_constraint_missing_column(schema: &TableSchema, dropped: &str) -> Option<String> {
    let source = schema.sql_source.as_deref()?;
    let stmt = vibesql_parser::Parser::parse_sql(source).ok()?;
    let Statement::CreateTable(create) = stmt else {
        return None;
    };

    // Table-level CHECK constraints always survive the drop.
    for constraint in &create.table_constraints {
        if let TableConstraintKind::Check { expr, .. } = &constraint.kind {
            if let Some(display) = find_column_ref_display(expr, dropped) {
                return Some(display);
            }
        }
    }

    // Column-level CHECKs on *other* columns also survive; only the dropped
    // column's own CHECKs vanish with it.
    for column in &create.columns {
        if column.name.eq_ignore_ascii_case(dropped) {
            continue;
        }
        for constraint in &column.constraints {
            if let ColumnConstraintKind::Check { expr, .. } = &constraint.kind {
                if let Some(display) = find_column_ref_display(expr, dropped) {
                    return Some(display);
                }
            }
        }
    }

    None
}

/// Display name of the first `ColumnRef` in `expr` naming `target`
/// (case-insensitive), or `None`. Mirrors
/// `super::validation::expression_references_column` but reports the name as
/// written so the error text matches SQLite byte-for-byte.
fn find_column_ref_display(expr: &Expression, target: &str) -> Option<String> {
    match expr {
        Expression::ColumnRef(col) => {
            if col.column_canonical().eq_ignore_ascii_case(target) {
                Some(col.column_display().to_string())
            } else {
                None
            }
        }
        Expression::BinaryOp { left, right, .. }
        | Expression::IsDistinctFrom { left, right, .. } => {
            find_column_ref_display(left, target).or_else(|| find_column_ref_display(right, target))
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            children.iter().find_map(|c| find_column_ref_display(c, target))
        }
        Expression::UnaryOp { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::IsTruthValue { expr, .. }
        | Expression::Collate { expr, .. }
        | Expression::Cast { expr, .. } => find_column_ref_display(expr, target),
        Expression::Function { args, .. } => {
            args.iter().find_map(|a| find_column_ref_display(a, target))
        }
        Expression::AggregateFunction { args, filter, .. } => args
            .iter()
            .find_map(|a| find_column_ref_display(a, target))
            .or_else(|| filter.as_ref().and_then(|f| find_column_ref_display(f, target))),
        Expression::Case { operand, when_clauses, else_result } => operand
            .as_ref()
            .and_then(|o| find_column_ref_display(o, target))
            .or_else(|| {
                when_clauses.iter().find_map(|w| {
                    w.conditions
                        .iter()
                        .find_map(|c| find_column_ref_display(c, target))
                        .or_else(|| find_column_ref_display(&w.result, target))
                })
            })
            .or_else(|| else_result.as_ref().and_then(|e| find_column_ref_display(e, target))),
        Expression::InList { expr, values, .. } => find_column_ref_display(expr, target)
            .or_else(|| values.iter().find_map(|e| find_column_ref_display(e, target))),
        Expression::Between { expr, low, high, .. } => find_column_ref_display(expr, target)
            .or_else(|| find_column_ref_display(low, target))
            .or_else(|| find_column_ref_display(high, target)),
        Expression::Like { expr, pattern, .. } => find_column_ref_display(expr, target)
            .or_else(|| find_column_ref_display(pattern, target)),
        Expression::Glob { expr, pattern, .. } => find_column_ref_display(expr, target)
            .or_else(|| find_column_ref_display(pattern, target)),
        Expression::RowValueConstructor(values) => {
            values.iter().find_map(|v| find_column_ref_display(v, target))
        }
        _ => None,
    }
}
