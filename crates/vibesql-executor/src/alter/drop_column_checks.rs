//! Whole-schema re-validation for `ALTER TABLE ... DROP COLUMN`.
//!
//! SQLite re-parses the entire schema twice when a column is dropped
//! (`alter.c`, `renameTestSchema`):
//!
//! 1. **Pre-check** (`zWhen = ""`): any view or trigger that is *already* broken — independent of
//!    the drop — aborts the ALTER with `error in <type> <name>: <inner error>`.
//! 2. **Post-check** (`zWhen = "after drop column"`): the schema as it would look *after* the drop
//!    is re-resolved; a view/trigger/table-level CHECK left dangling by the drop aborts with `error
//!    in <type> <name> after drop column: <inner error>`.
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
    ColumnConstraintKind, ColumnIdentifier, CommonTableExpr, Expression, FromClause, InsertSource,
    PseudoTable, SelectItem, SelectStmt, Statement, TableConstraintKind, TriggerAction,
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
    // Constraints and generated-column expressions on the altered table itself
    // must still resolve after the drop. The in-memory schema does not track a
    // constraint's column/table origin, so the origin is recovered from the
    // verbatim CREATE TABLE text (`sql_source`), exactly the text SQLite itself
    // re-parses. A constraint or generated expression that belongs to the
    // dropped column is removed together with the column and is exempt.
    if let Some(table) = database.get_table(table_name) {
        if let Some(inner) = table_self_reference_error(&table.schema, column) {
            return Err(ExecutorError::Other(format!(
                "error in table {} after drop column: {}",
                table_name, inner
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
    // SQLite: "Do not complain about syntax errors in the schema if in PRAGMA
    // writable_schema=ON mode" (altercol.test group 23, verified against
    // sqlite3 3.51.0). `writable_schema` lets a connection hand-edit
    // `sqlite_master.sql` directly, and its documented trade-off is that the
    // *next* schema-mutating statement — while the pragma is still ON —
    // tolerates whatever pre-existing brokenness that editing introduced
    // (a view/trigger referencing a column that was never valid, etc.)
    // instead of aborting the ALTER. This is gated on the CURRENT
    // `writable_schema` value at the time THIS statement runs, not on
    // whether the schema was ever corrupted under it: a later ALTER that
    // runs after the pragma has been turned back OFF still complains
    // normally (altercol.test 13.1.4-13.1.7 turn writable_schema OFF again
    // before the ALTER that is expected to raise `error in index ...`; see
    // issue #6174). Applies uniformly to both the pre-check (already-broken
    // objects) and post-check (objects broken specifically by this
    // ALTER) — SQLite's schema re-parse is skipped wholesale, not
    // selectively, while writable_schema is ON.
    if database.writable_schema() {
        return Ok(());
    }

    let suffix = if dropped.is_some() { " after drop column" } else { "" };

    // Resolve the altered table to its canonical catalog name once so FROM
    // references in any spelling ("T1", "main.t1", ...) simulate the drop.
    let sim = DropSimulation::new(database, dropped);

    // Iterate view definitions directly rather than via `list_views()` +
    // `get_view()`: views are keyed per schema (#6490), so a name-only
    // `get_view` resolves temp-first-then-main-then-attached and could skip a
    // `main` view that shares a name with a `temp`/attached-schema view
    // (mirrors the trigger loop immediately below — issue #6296).
    for view in database.catalog.iter_views() {
        if let Some(missing) = find_missing_column_in_view(view, &sim) {
            return Err(ExecutorError::Other(format!(
                "error in view {}{}: no such column: {}",
                view.name, suffix, missing
            )));
        }
    }

    // Iterate trigger definitions directly rather than via `list_triggers()` +
    // `get_trigger()`: triggers are keyed per schema, so a name-only `get_trigger`
    // resolves temp-first and would skip a `main` trigger that shares a name with
    // a `temp` trigger (issue #6296).
    for trigger in database.catalog.iter_triggers() {
        if let Some(inner) = find_trigger_resolution_error(trigger, &sim) {
            return Err(ExecutorError::Other(format!(
                "error in trigger {}{}: {}",
                trigger.name, suffix, inner
            )));
        }
    }

    Ok(())
}

// ============================================================================
// Simulated post-drop schema resolution
// ============================================================================

/// Column resolution against the schema, optionally simulating the drop of
/// one column from one table (the post-check view of the world), OR a
/// pending column rename that has not yet reached the *catalog* copy of the
/// altered table's schema.
///
/// The rename case exists because `columns::execute_rename_column` mutates
/// only the *storage* `Table` schema up front; the catalog copy (which
/// `columns_of_relation` reads, since `sqlite_master`/DML resolution reads
/// the catalog) is not re-synced until `sync_catalog_schema_from_storage`
/// runs *after* the whole ALTER — including trigger/view rewriting — returns
/// (`alter/mod.rs`). Without the `renamed` simulation, a post-rename
/// re-validation (see [`find_ambiguous_column_in_query`]) would resolve the
/// altered table's columns as they were *before* the rename, silently
/// missing exactly the newly-introduced ambiguity it exists to catch
/// (altercol.test 16.1.1).
struct DropSimulation<'a> {
    db: &'a Database,
    /// `(canonical altered-table name, dropped column)`; `None` = pre-check.
    dropped: Option<(String, &'a str)>,
    /// `(canonical altered-table name, old column, new column)`; `None` when
    /// not simulating a rename.
    renamed: Option<(String, &'a str, &'a str)>,
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
        DropSimulation { db, dropped, renamed: None }
    }

    /// Simulate `table`'s `old_col` as already renamed to `new_col`, ahead of
    /// the catalog re-sync that only happens after the whole ALTER returns.
    fn new_for_rename(db: &'a Database, table: &str, old_col: &'a str, new_col: &'a str) -> Self {
        let canonical = db
            .catalog
            .get_table(table)
            .map(|s| s.name.clone())
            .unwrap_or_else(|| table.to_string());
        DropSimulation { db, dropped: None, renamed: Some((canonical, old_col, new_col)) }
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
            if let Some((altered, old_col, new_col)) = &self.renamed {
                if schema.name.eq_ignore_ascii_case(altered) {
                    for c in cols.iter_mut() {
                        if c.eq_ignore_ascii_case(old_col) {
                            *c = (*new_col).to_string();
                        }
                    }
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

/// Re-parse a verbatim `CREATE VIEW ... AS <select>` definition and return the
/// defining `SELECT`. `None` when the text does not parse to a `CREATE VIEW`.
fn reparse_view_query(sql_definition: &str) -> Option<SelectStmt> {
    match vibesql_parser::Parser::parse_sql(sql_definition).ok()? {
        Statement::CreateView(create) => Some(*create.query),
        _ => None,
    }
}

/// First column reference in `view`'s defining query that does not resolve
/// against the (possibly simulated) schema, in SQL text order — or `None`
/// when the view is valid or too complex to validate statically.
fn find_missing_column_in_view(view: &ViewDefinition, sim: &DropSimulation) -> Option<String> {
    // Resolve against the view's verbatim `CREATE VIEW` text — the same text
    // SQLite itself re-parses — rather than the stored `query` AST. The AST can
    // drift from the definition across a persistence round-trip (e.g. a quoted
    // multi-word column such as `"big c"` currently reloads as `big`), and
    // judging a view by a drifted AST would report a column missing that the
    // definition in fact names. Fall back to the stored AST when the verbatim
    // text is absent or does not re-parse to a `CREATE VIEW`.
    let reparsed = view.sql_definition.as_deref().and_then(reparse_view_query);
    let query = reparsed.as_ref().unwrap_or(&view.query);

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

/// First column reference in `query` that resolves *ambiguously* — matching
/// more than one FROM-clause relation — against the **current** (live)
/// schema. Used as a post-rename re-validation for views: SQLite re-resolves
/// the entire view query after `ALTER TABLE ... RENAME COLUMN` rewrites it,
/// and a reference that was perfectly unambiguous before the rename can
/// become ambiguous once the renamed column duplicates a name already in
/// scope elsewhere in the FROM clause (verified against sqlite3 3.51.0,
/// altercol.test 16.1.1: `SELECT a, d FROM t1, t2` with `t2.d` renamed to
/// `a` — the *original* `t1.a` reference becomes ambiguous too, not just the
/// rewritten one).
///
/// Unlike [`find_missing_column_in_view`], this takes the query AST directly
/// (the caller already has the freshly-rewritten, freshly-parsed view text).
/// The rename has already been applied to the *storage* copy of `table`'s
/// schema by the time this runs (see `columns::execute_rename_column`'s
/// ordering: schema rename, then trigger rewrite, then view rewrite) — but
/// NOT yet to the *catalog* copy, which is only re-synced after the whole
/// ALTER (including this validation) returns. `table`/`old_column`/
/// `new_column` drive a [`DropSimulation::new_for_rename`] so `table`'s
/// columns resolve as they will look once synced, rather than the catalog's
/// still-stale pre-rename list (without this, `table` would never appear to
/// own `new_column`, and the ambiguity this function exists to catch could
/// never be detected).
pub(super) fn find_ambiguous_column_in_query(
    query: &SelectStmt,
    database: &Database,
    table: &str,
    old_column: &str,
    new_column: &str,
) -> Option<String> {
    // Same conservative scope-shape restriction as `find_missing_column_in_view`.
    if query.with_clause.is_some() || query.values.is_some() || query.set_operation.is_some() {
        return None;
    }
    let from = query.from.as_ref()?;

    let sim = DropSimulation::new_for_rename(database, table, old_column, new_column);
    let mut sources = Vec::new();
    if !collect_from_sources(from, &sim, &mut sources) {
        return None;
    }

    let aliases: Vec<String> = query
        .select_list
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expression { alias: Some(a), .. } => Some(a.clone()),
            _ => None,
        })
        .collect();

    let scope = ViewScope { sources: &sources, aliases: &aliases };

    for item in &query.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if let Some(ambiguous) = first_ambiguous_in_expr(expr, &scope) {
                return Some(ambiguous);
            }
        }
    }
    if let Some(ambiguous) = first_ambiguous_in_join_conditions(from, &scope) {
        return Some(ambiguous);
    }
    if let Some(where_clause) = &query.where_clause {
        if let Some(ambiguous) = first_ambiguous_in_expr(where_clause, &scope) {
            return Some(ambiguous);
        }
    }
    if let Some(group_by) = &query.group_by {
        for expr in group_by.all_expressions() {
            if let Some(ambiguous) = first_ambiguous_in_expr(expr, &scope) {
                return Some(ambiguous);
            }
        }
    }
    if let Some(having) = &query.having {
        if let Some(ambiguous) = first_ambiguous_in_expr(having, &scope) {
            return Some(ambiguous);
        }
    }
    if let Some(order_by) = &query.order_by {
        for item in order_by {
            if let Some(ambiguous) = first_ambiguous_in_expr(&item.expr, &scope) {
                return Some(ambiguous);
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
        FromClause::Subquery { .. }
        | FromClause::Values { .. }
        | FromClause::TableFunction { .. } => false,
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

/// First *ambiguously*-resolving reference in any JOIN `ON` condition of the
/// FROM tree. Mirrors [`first_missing_in_join_conditions`] but for the
/// post-rename ambiguity re-check (see [`find_ambiguous_column_in_query`]).
fn first_ambiguous_in_join_conditions(from: &FromClause, scope: &ViewScope) -> Option<String> {
    match from {
        FromClause::Join { left, right, condition, .. } => {
            if let Some(ambiguous) = first_ambiguous_in_join_conditions(left, scope) {
                return Some(ambiguous);
            }
            if let Some(ambiguous) = first_ambiguous_in_join_conditions(right, scope) {
                return Some(ambiguous);
            }
            condition.as_ref().and_then(|cond| first_ambiguous_in_expr(cond, scope))
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

/// First *ambiguously*-resolving `ColumnRef` in `expr` (pre-order), or `None`.
/// Mirrors [`first_missing_in_expr`]'s traversal shape exactly, but reports an
/// unqualified reference that matches more than one FROM-clause relation
/// instead of one that matches none — the post-rename re-check (see
/// [`find_ambiguous_column_in_query`]).
fn first_ambiguous_in_expr(expr: &Expression, scope: &ViewScope) -> Option<String> {
    match expr {
        Expression::ColumnRef(col) => check_column_ref_ambiguous(col, scope),
        Expression::BinaryOp { left, right, .. } => {
            first_ambiguous_in_expr(left, scope).or_else(|| first_ambiguous_in_expr(right, scope))
        }
        Expression::IsDistinctFrom { left, right, .. } => {
            first_ambiguous_in_expr(left, scope).or_else(|| first_ambiguous_in_expr(right, scope))
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            children.iter().find_map(|c| first_ambiguous_in_expr(c, scope))
        }
        Expression::UnaryOp { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::IsTruthValue { expr, .. }
        | Expression::Collate { expr, .. }
        | Expression::Cast { expr, .. } => first_ambiguous_in_expr(expr, scope),
        Expression::Function { args, .. } => {
            args.iter().find_map(|a| first_ambiguous_in_expr(a, scope))
        }
        Expression::AggregateFunction { args, filter, .. } => args
            .iter()
            .find_map(|a| first_ambiguous_in_expr(a, scope))
            .or_else(|| filter.as_ref().and_then(|f| first_ambiguous_in_expr(f, scope))),
        Expression::Case { operand, when_clauses, else_result } => operand
            .as_ref()
            .and_then(|o| first_ambiguous_in_expr(o, scope))
            .or_else(|| {
                when_clauses.iter().find_map(|w| {
                    w.conditions
                        .iter()
                        .find_map(|c| first_ambiguous_in_expr(c, scope))
                        .or_else(|| first_ambiguous_in_expr(&w.result, scope))
                })
            })
            .or_else(|| else_result.as_ref().and_then(|e| first_ambiguous_in_expr(e, scope))),
        Expression::InList { expr, values, .. } => first_ambiguous_in_expr(expr, scope)
            .or_else(|| values.iter().find_map(|e| first_ambiguous_in_expr(e, scope))),
        Expression::Between { expr, low, high, .. } => first_ambiguous_in_expr(expr, scope)
            .or_else(|| first_ambiguous_in_expr(low, scope))
            .or_else(|| first_ambiguous_in_expr(high, scope)),
        Expression::Like { expr, pattern, .. } => {
            first_ambiguous_in_expr(expr, scope).or_else(|| first_ambiguous_in_expr(pattern, scope))
        }
        Expression::Glob { expr, pattern, .. } => {
            first_ambiguous_in_expr(expr, scope).or_else(|| first_ambiguous_in_expr(pattern, scope))
        }
        Expression::RowValueConstructor(values) => {
            values.iter().find_map(|v| first_ambiguous_in_expr(v, scope))
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

/// Resolve one column reference against the view scope, reporting it as
/// *ambiguous* when it is unqualified and matches columns owned by more than
/// one FROM-clause relation. Schema-qualified references are never ambiguous
/// (a qualifier picks a single relation by construction), matching
/// `check_column_ref`'s treatment of the same case.
fn check_column_ref_ambiguous(col: &ColumnIdentifier, scope: &ViewScope) -> Option<String> {
    if col.schema_canonical().is_some() || col.table_canonical().is_some() {
        return None;
    }
    let name = col.column_canonical();
    if is_rowid_pseudo(name) {
        return None;
    }
    let match_count = scope
        .sources
        .iter()
        .filter(|s| s.columns.iter().any(|c| c.eq_ignore_ascii_case(name)))
        .count();
    if match_count > 1 {
        Some(col.column_display().to_string())
    } else {
        None
    }
}

// ============================================================================
// Trigger validation
// ============================================================================

/// First resolution error in `trigger`, returned as the inner message SQLite
/// appends after `error in trigger <name>[ <suffix>]: ` — either
/// `no such table: main.<t>` (a body statement references a table that does
/// not exist) or `no such column: <new|old>.<c>` (a `NEW.`/`OLD.` reference
/// names a column absent from the target table). `None` when the trigger is
/// valid or cannot be judged.
///
/// SQLite re-parses and re-resolves every dependent trigger on ALTER TABLE
/// RENAME/DROP; a trigger that was *already* broken (e.g. its body inserts
/// into a table that was never created) aborts the ALTER. Missing-table
/// resolution is checked first because SQLite reports it before descending
/// into NEW/OLD column resolution.
fn find_trigger_resolution_error(
    trigger: &TriggerDefinition,
    sim: &DropSimulation,
) -> Option<String> {
    // Parse the body once; an unparseable body cannot be judged, so skip it.
    let TriggerAction::RawSql(sql) = &trigger.triggered_action;
    let statements = crate::trigger_execution::TriggerFirer::parse_trigger_sql(sql).ok()?;

    // 1) A body statement that reads from / writes to a non-existent table aborts the ALTER,
    //    matching SQLite's schema re-parse (`error in trigger <name>: no such table: main.<t>`). An
    //    unqualified missing name is only prefixed with the implicit `main.` schema when the
    //    *trigger itself* lives in the main schema; a trigger living in the `temp` schema (either
    //    an explicit `CREATE TEMP TRIGGER`, or one implicitly bound to a TEMP table) reports the
    //    bare name instead (altercol/alter.test 17.1 vs 17.3: `error in trigger u7t: no such table:
    //    main.u8` for a main-schema trigger, but `error in trigger uu7t: no such table: u8` for the
    //    TEMP-schema namesake — verified against sqlite3 3.51.0).
    if let Some(missing) = find_missing_table_in_statements(&statements, sim.db, trigger.is_temp())
    {
        return Some(format!("no such table: {}", missing));
    }

    // 2) A NEW./OLD. reference to a column absent from the target table.
    if let Some(pseudo) = find_missing_pseudo_in_trigger(trigger, &statements, sim) {
        return Some(format!("no such column: {}", pseudo));
    }

    None
}

/// First `NEW.<col>` / `OLD.<col>` reference in `trigger` (WHEN condition
/// first, then the pre-parsed body `statements` in order) that does not name a
/// column of the trigger's target table — formatted as `new.<col>` /
/// `old.<col>`. `None` when the trigger is valid or cannot be judged.
fn find_missing_pseudo_in_trigger(
    trigger: &TriggerDefinition,
    statements: &[Statement],
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
    for stmt in statements {
        collect_pseudo_refs_in_statement(stmt, &mut refs);
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

// ============================================================================
// Missing-table resolution in trigger bodies
// ============================================================================

/// First base-table reference in `statements` (INSERT/UPDATE/DELETE targets and
/// FROM-clause tables, in SQL text order) that resolves to neither a table nor a
/// view — named the way SQLite's `no such table:` message spells it. An
/// unqualified name is reported as `main.<name>` when `owner_is_temp` is
/// `false` (the referencing trigger lives in the main schema), or bare
/// `<name>` when `owner_is_temp` is `true` (the referencing trigger lives in
/// the `temp` schema — see the call site in [`find_trigger_resolution_error`]).
/// CTE names in scope are excluded (they are not base tables), and the check
/// is skipped (returns `None`) for anything it cannot judge — conservative in
/// the direction that never blocks an ALTER SQLite allows.
fn find_missing_table_in_statements(
    statements: &[Statement],
    db: &Database,
    owner_is_temp: bool,
) -> Option<String> {
    let mut refs: Vec<String> = Vec::new();
    let mut cte_names: Vec<String> = Vec::new();
    for stmt in statements {
        collect_table_refs_in_statement(stmt, &mut refs, &mut cte_names);
    }

    for name in refs {
        let bare = name.rsplit('.').next().unwrap_or(&name);
        // A WITH-clause name is not a base table.
        if cte_names.iter().any(|c| c.eq_ignore_ascii_case(bare)) {
            continue;
        }
        // Resolve as written (handles `schema.table`, temp shadowing, and the
        // implicit main schema) and, failing that, by its bare name.
        if db.catalog.get_table(&name).is_some()
            || db.catalog.get_view(&name).is_some()
            || db.catalog.get_table(bare).is_some()
            || db.catalog.get_view(bare).is_some()
        {
            continue;
        }
        return Some(if name.contains('.') || owner_is_temp {
            name
        } else {
            format!("main.{}", name)
        });
    }
    None
}

/// Accumulate base-table references and CTE names from one trigger-body
/// statement. Only INSERT/UPDATE/DELETE targets and FROM-clause tables are
/// collected; subqueries nested inside expressions are intentionally not
/// descended into (a false negative is safe, a false positive is not).
fn collect_table_refs_in_statement(
    stmt: &Statement,
    refs: &mut Vec<String>,
    cte_names: &mut Vec<String>,
) {
    match stmt {
        Statement::Insert(insert) => {
            collect_cte_names(&insert.with_clause, refs, cte_names);
            let target = match &insert.schema_name {
                Some(schema) => format!("{}.{}", schema, insert.table_name),
                None => insert.table_name.clone(),
            };
            refs.push(target);
            if let InsertSource::Select(select) = &insert.source {
                collect_table_refs_in_select(select, refs, cte_names);
            }
        }
        Statement::Update(update) => {
            refs.push(update.table_name.clone());
            if let Some(from) = &update.from_clause {
                for source in from {
                    collect_table_refs_in_from(source, refs, cte_names);
                }
            }
        }
        Statement::Delete(delete) => {
            refs.push(delete.table_name.clone());
        }
        Statement::Select(select) => collect_table_refs_in_select(select, refs, cte_names),
        _ => {}
    }
}

/// Accumulate references from a SELECT: its CTE names, its FROM tables, and
/// (recursively) any set-operation right-hand side.
fn collect_table_refs_in_select(
    select: &SelectStmt,
    refs: &mut Vec<String>,
    cte_names: &mut Vec<String>,
) {
    collect_cte_names(&select.with_clause, refs, cte_names);
    if let Some(from) = &select.from {
        collect_table_refs_in_from(from, refs, cte_names);
    }
    if let Some(set_op) = &select.set_operation {
        collect_table_refs_in_select(&set_op.right, refs, cte_names);
    }
}

/// Record the names defined by a WITH clause and descend into each CTE body.
fn collect_cte_names(
    with_clause: &Option<Vec<CommonTableExpr>>,
    refs: &mut Vec<String>,
    cte_names: &mut Vec<String>,
) {
    if let Some(ctes) = with_clause {
        for cte in ctes {
            cte_names.push(cte.name.clone());
            collect_table_refs_in_select(&cte.query, refs, cte_names);
        }
    }
}

/// Accumulate base-table names from a FROM tree (recursing joins; skipping
/// subqueries, VALUES, and table-valued functions).
fn collect_table_refs_in_from(
    from: &FromClause,
    refs: &mut Vec<String>,
    cte_names: &mut Vec<String>,
) {
    match from {
        FromClause::Table { name, .. } => refs.push(name.clone()),
        FromClause::Join { left, right, .. } => {
            collect_table_refs_in_from(left, refs, cte_names);
            collect_table_refs_in_from(right, refs, cte_names);
        }
        FromClause::Subquery { query, .. } => collect_table_refs_in_select(query, refs, cte_names),
        FromClause::Values { .. } | FromClause::TableFunction { .. } => {}
    }
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
// Table self-reference validation (origin recovered from the CREATE TABLE text)
// ============================================================================

/// Full inner error text for the first surviving part of the altered table's
/// own definition that references `dropped` — a table-level CHECK / FOREIGN KEY,
/// a column-level CHECK on another column, or another column's generated
/// expression. Definitions attached to the dropped column itself are removed
/// with the column (SQLite semantics, alterdropcol.test 3.2) and never reported.
///
/// The returned string is SQLite's inner wording (`no such column: <c>`, or
/// `unknown column "<c>" in foreign key definition`); the caller prefixes it
/// with `error in table <name> after drop column: `.
///
/// Column-vs-table origin is not tracked on the in-memory schema, so it is
/// recovered by re-parsing the verbatim `CREATE TABLE` text — the same text
/// SQLite's own re-parse reads. Without `sql_source` (schema built
/// programmatically) the origin is unknown and the legacy behavior (silently
/// dropping definitions that reference the column) is kept.
fn table_self_reference_error(schema: &TableSchema, dropped: &str) -> Option<String> {
    let source = schema.sql_source.as_deref()?;
    let stmt = vibesql_parser::Parser::parse_sql(source).ok()?;
    let Statement::CreateTable(create) = stmt else {
        return None;
    };

    // Table-level constraints always survive the drop, so any reference to the
    // dropped column from one of them breaks the re-parsed schema.
    for constraint in &create.table_constraints {
        match &constraint.kind {
            TableConstraintKind::Check { expr, .. } => {
                if let Some(display) = find_column_ref_display(expr, dropped) {
                    return Some(format!("no such column: {}", display));
                }
            }
            // A FOREIGN KEY whose local column list names the dropped column is
            // reported with SQLite's dedicated FK wording rather than the generic
            // "no such column" text (alterdropcol2 2.6.1). Matched on the bare
            // local-column list — the referenced-parent columns belong to the
            // other table and are untouched here.
            TableConstraintKind::ForeignKey { columns, .. } => {
                if let Some(display) = columns.iter().find(|c| c.eq_ignore_ascii_case(dropped)) {
                    return Some(format!(
                        "unknown column \"{}\" in foreign key definition",
                        display
                    ));
                }
            }
            // A table-level `UNIQUE(...)` constraint (as opposed to an inline
            // column-level `col UNIQUE`, which is rejected earlier by the
            // dedicated `cannot drop UNIQUE column` precheck and never
            // reaches here) also survives the drop as schema text, so a
            // reference to the dropped column dangles the same way a CHECK
            // constraint would. This covers both multi-column
            // (`UNIQUE(b, c)`, alterdropcol2 2.2.2) and single-column
            // (`UNIQUE(b)`) table-level constraints — verified against
            // sqlite3 3.51.0, both report the generic "no such column"
            // re-parse error rather than "cannot drop UNIQUE column".
            TableConstraintKind::Unique { columns, .. } => {
                if let Some(display) = columns
                    .iter()
                    .find_map(|c| c.column_name().filter(|n| n.eq_ignore_ascii_case(dropped)))
                {
                    return Some(format!("no such column: {}", display));
                }
            }
            _ => {}
        }
    }

    // Column-level constraints/expressions on *other* columns also survive; only
    // the dropped column's own definitions vanish with it.
    for column in &create.columns {
        if column.name.eq_ignore_ascii_case(dropped) {
            continue;
        }
        for constraint in &column.constraints {
            if let ColumnConstraintKind::Check { expr, .. } = &constraint.kind {
                if let Some(display) = find_column_ref_display(expr, dropped) {
                    return Some(format!("no such column: {}", display));
                }
            }
        }
        // A generated column (`x AS (<expr>)` / `... STORED`) that references the
        // dropped column leaves a dangling reference after the drop
        // (alterdropcol2 2.7.1/2.7.2).
        if let Some(gen_expr) = &column.generated_expr {
            if let Some(display) = find_column_ref_display(gen_expr, dropped) {
                return Some(format!("no such column: {}", display));
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
