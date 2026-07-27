//! Core UPDATE statement execution logic
//!
//! This module contains the main execution orchestration for UPDATE statements,
//! implementing SQL's two-phase update semantics: first collect all updates
//! evaluating against original rows, then apply all updates atomically.

use std::collections::HashSet;

use vibesql_ast::{Expression, UpdateStmt};
use vibesql_catalog::TableIdentifier;
use vibesql_storage::{statistics::CostEstimator, Database, Row};
use vibesql_types::SqlValue;

use crate::{
    dml_cost::DmlOptimizer, errors::ExecutorError, evaluator::ExpressionEvaluator,
    expression_index_maintenance, partial_index_maintenance, privilege_checker::PrivilegeChecker,
    sqlite_schema::is_sqlite_schema_table,
};

use super::{
    constraints::ConstraintValidator,
    fast_path,
    foreign_keys::ForeignKeyValidator,
    from_clause::{apply_update_from_matches, execute_update_from_join},
    index_sync::{
        self, detect_surviving_replace_conflict, find_conflicting_rows_for_update,
        resolve_cross_update_conflicts_for_replace, validate_cross_update_uniqueness,
        validate_post_statement_uniqueness, validate_rowid_relocation, validate_unique_relocation,
    },
    row_selector::RowSelector,
    triggers,
    value_updater::ValueUpdater,
    PendingUpdate,
};

/// Internal implementation supporting both schema caching, procedural context, and trigger
/// context
///
/// Returns the number of updated rows plus, when the statement carries a
/// RETURNING clause, the projected NEW rows (SQLite 3.35.0+ semantics).
pub(super) fn execute_internal(
    stmt: &UpdateStmt,
    database: &mut Database,
    schema: Option<&vibesql_catalog::TableSchema>,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
) -> Result<(usize, Option<crate::select::SelectResult>), ExecutorError> {
    // Check if target is sqlite_master/sqlite_schema. Read-only by default;
    // under PRAGMA writable_schema=ON, a supported subset of UPDATEs rewrites
    // the stored CREATE TABLE source text (issue #5796; alterdropcol 8.x).
    if is_sqlite_schema_table(&stmt.table_name) {
        if database.writable_schema() {
            let matched = crate::sqlite_schema::execute_sqlite_schema_update(stmt, database)?;
            return Ok((matched, None));
        }
        return Err(ExecutorError::SqliteSystemTableReadOnly {
            table_name: stmt.table_name.clone(),
            operation: "modified".to_string(),
        });
    }

    // Check UPDATE privilege on the table
    PrivilegeChecker::check_update(database, &stmt.table_name)?;

    // Check if target is a VIEW with INSTEAD OF triggers
    if let Some(view_def) = database.catalog.get_view(&stmt.table_name).cloned() {
        return triggers::execute_update_on_view(
            database,
            stmt,
            &view_def,
            procedural_context,
            trigger_context,
        );
    }

    // Step 1: Get table schema - clone it to avoid borrow issues
    // We need owned schema because we take mutable references to database later
    // Use TableIdentifier for SQL:1999 case-sensitive lookups when quoted
    // Handle schema-qualified table names (e.g., "temp.t1")
    let table_id = if let Some((schema_part, table_part)) = stmt.table_name.split_once('.') {
        // Schema-qualified name: schema.table
        TableIdentifier::qualified(schema_part, false, table_part, stmt.quoted)
    } else {
        TableIdentifier::new(&stmt.table_name, stmt.quoted)
    };
    let schema_owned: vibesql_catalog::TableSchema = if let Some(s) = schema {
        s.clone()
    } else {
        database
            .catalog
            .get_table_by_identifier(&table_id)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
            .clone()
    };
    let schema = &schema_owned;

    // Use canonical table name from schema for all storage operations
    // This ensures case-sensitive tables (quoted identifiers) are accessed correctly
    let table_name = &schema_owned.name;

    // Prepare-time scalar-subquery arity validation (#6046). SQLite rejects a
    // multi-column subquery used where a single value is required at prepare
    // time, even when the target table is empty so the per-row path never runs
    // (rowvalue.test 15.2 SET value, 15.3 WHERE). A tuple assignment
    // `(a, b) = (SELECT x, y)` is a legal row-value assignment (arity is checked
    // by the SET-tuple path), so only single-column assignment values are
    // validated in the scalar-value context here.
    for assignment in &stmt.assignments {
        if assignment.columns.is_empty() {
            crate::select::validate_value_subquery_arity(&assignment.value, database)?;
        }
    }
    if let Some(vibesql_ast::WhereClause::Condition(where_expr)) = &stmt.where_clause {
        crate::select::validate_predicate_subquery_arity(where_expr, database)?;
    }

    // Check if table has UPDATE triggers (check once, use multiple times).
    //
    // ROW-level triggers fire even when this UPDATE runs inside another
    // trigger's body (i.e. `trigger_context.is_some()`): SQLite fires a nested
    // UPDATE's row triggers subject only to `recursive_triggers` and the depth
    // cap, which `TriggerFirer` now enforces directly (#5535). This mirrors the
    // INSERT path, which already fires nested-INSERT row triggers. Previously
    // this was gated on `trigger_context.is_none()`, so a nested UPDATE in a
    // trigger body silently skipped the target table's UPDATE triggers — e.g.
    // a BEFORE UPDATE `RAISE(IGNORE)` never ran (trigger3-6).
    //
    // STATEMENT-level triggers are a VibeSQL extension with no sqlite3 analog
    // and are NOT fired inside a trigger body; that gating uses
    // `fire_statement_triggers` below.
    let has_triggers = database
        .catalog
        .get_triggers_for_table(table_name, Some(vibesql_ast::TriggerEvent::Update(None)))
        .next()
        .is_some();

    // STATEMENT-level triggers only fire at the top level (not within another
    // trigger's body), matching the INSERT path's `trigger_context.is_none()`
    // gate on statement triggers.
    let fire_statement_triggers = has_triggers && trigger_context.is_none();

    // Resolve trigger WHEN clauses at statement-prepare time (SQLite semantics).
    // A trigger `WHEN nosuchcol` must error `no such column: nosuchcol` even when
    // the UPDATE matches zero rows (update-14.2/14.4); the per-row firing path
    // never runs in that case, so validate the WHEN clauses up front. Only at the
    // top level (not inside another trigger body), matching SQLite's prepare-time
    // resolution of the outermost statement.
    if has_triggers && trigger_context.is_none() {
        crate::TriggerFirer::validate_when_clauses_for_event(
            database,
            table_name,
            vibesql_ast::TriggerEvent::Update(None),
            None,
        )?;
    }

    // Try fast path for simple single-row PK updates without triggers
    // Conditions: no triggers, no procedural context, simple WHERE pk = value, no assertions
    // Skip fast path if assertions exist because we need rollback capability on violation
    // Skip fast path if expression indexes exist because they need maintenance
    let has_assertions = database.catalog.get_all_assertions().next().is_some();
    let has_expression_indexes = database.has_expression_indexes(table_name);
    if !has_triggers
        && procedural_context.is_none()
        && trigger_context.is_none()
        && !has_assertions
        && !has_expression_indexes
        // RETURNING needs the two-phase path so NEW rows can be captured
        && stmt.returning.is_none()
        // ORDER BY / LIMIT / OFFSET (SQLite extension) restrict which rows are
        // updated; the single-row PK fast path has no row-limiting logic, so
        // route through the standard scan path (mirrors DELETE skipping its
        // TRUNCATE fast path when ORDER BY/LIMIT is present).
        && stmt.order_by.is_none()
        && stmt.limit.is_none()
        && stmt.offset.is_none()
        // OR REPLACE / OR IGNORE need the two-phase path to resolve conflicts:
        // REPLACE deletes the conflicting row (firing its DELETE triggers) and
        // IGNORE skips the row. The fast path has no conflict-resolution logic,
        // so it would wrongly raise a PK/UNIQUE violation (issue #5490).
        && stmt.conflict_clause.is_none()
    {
        if let Some(result) = fast_path::try_fast_path_update(stmt, database, schema)? {
            // Invalidate columnar cache after fast path update
            if result > 0 {
                database.invalidate_columnar_cache(table_name);
            }
            return Ok((result, None));
        }
    }

    // Fire BEFORE STATEMENT triggers only if triggers exist.
    // Statement-level triggers are a VibeSQL extension with no sqlite3 analog
    // (SQLite triggers are always FOR EACH ROW), so a RAISE(IGNORE) at
    // statement granularity has no defined "skip the row" semantics. We keep
    // the pre-#5418 behavior (proceed) and explicitly drop the must-use
    // outcome rather than guessing.
    if fire_statement_triggers {
        let _stmt_outcome = crate::TriggerFirer::execute_before_statement_triggers(
            database,
            table_name,
            vibesql_ast::TriggerEvent::Update(None),
        )?;
    }

    // Get PK indices without cloning entire schema
    let pk_indices = schema.get_primary_key_indices();

    // Step 2: Get table from storage (for reading rows)
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Execute CTEs if present (WITH clause support)
    let cte_results = if let Some(ref cte_list) = stmt.with_clause {
        Some(crate::select::cte::execute_ctes(cte_list, database, |cte_query, prior_ctes| {
            let cte_executor = crate::SelectExecutor::new_with_cte(database, prior_ctes);
            cte_executor.execute(cte_query)
        })?)
    } else {
        None
    };

    // Step 3: Create expression evaluator with database reference for subquery support
    //         and optional procedural/trigger context for variable resolution
    let mut evaluator = if let Some(ctx) = trigger_context {
        // Trigger context takes precedence (trigger statements can't have procedural context).
        // When the UPDATE also carries a WITH clause (e.g. a trigger body
        // `WITH uset(a,b) AS (...) UPDATE t SET x=(SELECT b FROM uset WHERE a=x)`),
        // the trigger-context constructor alone would drop `cte_results`, so the
        // scalar subquery could not resolve the CTE. Chain the CTE context on so
        // both trigger pseudo-vars and CTE references resolve (Gap 1 of #5941).
        let e = ExpressionEvaluator::with_trigger_context(schema, database, ctx);
        if let Some(ref cte_ctx) = cte_results {
            e.with_cte_context(cte_ctx)
        } else {
            e
        }
    } else if let Some(ctx) = procedural_context {
        // Likewise preserve any WITH-clause CTE context alongside the procedural
        // context so scalar subqueries in SET expressions can reference the CTE.
        let e = ExpressionEvaluator::with_procedural_context(schema, database, ctx);
        if let Some(ref cte_ctx) = cte_results {
            e.with_cte_context(cte_ctx)
        } else {
            e
        }
    } else if let Some(ref cte_ctx) = cte_results {
        // Use CTE context for WITH clause support
        ExpressionEvaluator::with_database_and_cte(schema, database, cte_ctx)
    } else {
        ExpressionEvaluator::with_database(schema, database)
    };

    // Set table alias if present (SQLite extension: UPDATE t1 AS xyz SET ...)
    if let Some(ref alias) = stmt.alias {
        evaluator.set_table_alias(alias.clone());
    }

    // Step 3.5: Validate SET expressions BEFORE row selection
    // SQLite validates expressions at preparation time, not execution time.
    // This ensures errors like "no such column: x" are raised even when
    // no rows match the WHERE clause.
    // Note: Skip validation for UPDATE FROM since the SET expressions can reference
    // columns from the FROM tables, which will be validated during synthetic SELECT.
    if stmt.from_clause.is_none() {
        validate_set_expressions(schema, &stmt.assignments, database)?;
    }

    // When an alias is active, SQLite hides the original (un-aliased) table name as a
    // column qualifier: `UPDATE t1 AS a SET y=1 WHERE t1.x=1` must raise
    // `no such column: t1.x` even on an empty table (validated at prepare time).
    // The row-by-row evaluator already enforces this, but it never runs when no rows
    // match, so reject the original-name qualifier up front here.
    if let Some(ref alias) = stmt.alias {
        if let Some(vibesql_ast::WhereClause::Condition(where_expr)) = &stmt.where_clause {
            validate_alias_scoped_qualifiers(where_expr, &stmt.table_name, alias)?;
        }
    }

    // Step 3.6: Handle UPDATE FROM (multi-table UPDATE) if FROM clause is present
    // This uses a completely different code path that builds a synthetic SELECT
    // to join tables and compute SET values in the joined context.
    if let Some(ref from_clauses) = stmt.from_clause {
        return execute_update_from(
            stmt,
            from_clauses,
            database,
            schema,
            table_name,
            has_triggers,
            &pk_indices,
            trigger_context,
            cte_results.as_ref(),
        );
    }

    // Step 4: Select rows to update using RowSelector.
    //
    // Phase 1d follow-up (#5205): thread the active MVCC snapshot into
    // row selection so the WHERE-clause scan + PK fast path honor
    // visibility. Off-state collapses to the pre-MVCC live-row filter.
    let snapshot = crate::mvcc::read_snapshot(database);
    let row_selector = RowSelector::new(schema);
    let mut candidate_rows =
        row_selector.select_rows(table, &stmt.where_clause, &mut evaluator, &snapshot)?;

    // Apply ORDER BY sorting and LIMIT/OFFSET (SQLite extension for UPDATE).
    // Mirrors the DELETE ... ORDER BY ... LIMIT path: when LIMIT (and optionally
    // ORDER BY/OFFSET) is present, restrict which of the matched rows are
    // actually updated. Without ORDER BY, rows are limited in scan order, which
    // matches SQLite's UPDATE ... LIMIT behavior.
    if stmt.order_by.is_some() || stmt.limit.is_some() || stmt.offset.is_some() {
        apply_order_by_and_limit(
            &mut candidate_rows,
            stmt.order_by.as_deref(),
            &stmt.limit,
            &stmt.offset,
            &evaluator,
        )?;
    }

    // Estimate DML cost for query analysis and optimization decisions
    if std::env::var("DML_COST_DEBUG").is_ok() && !candidate_rows.is_empty() {
        if let Some(index_info) = database.get_table_index_info(table_name) {
            // Get table statistics for cost estimation (use cached if available, or fallback to
            // estimate)
            let table_stats = table.get_statistics().cloned().unwrap_or_else(|| {
                vibesql_storage::TableStatistics::estimate_from_row_count(table.row_count())
            });

            // Estimate the ratio of indexes affected based on columns being updated
            // This is a heuristic: assume columns are distributed evenly across indexes
            let total_columns = schema.columns.len();
            let changed_columns = stmt.assignments.len();
            let indexes_affected_ratio = if total_columns > 0 {
                (changed_columns as f64 / total_columns as f64).min(1.0)
            } else {
                1.0 // Conservative estimate if no columns
            };

            let cost_estimator = CostEstimator::default();
            let estimated_cost = cost_estimator.estimate_update(
                candidate_rows.len(),
                &table_stats,
                &index_info,
                indexes_affected_ratio,
            );
            eprintln!(
                "DML_COST_DEBUG: UPDATE {} rows in {} - estimated_cost: {:.2} (hash_indexes: {}, btree_indexes: {}, columnar: {}, affected_ratio: {:.2})",
                candidate_rows.len(),
                stmt.table_name,
                estimated_cost,
                index_info.hash_index_count,
                index_info.btree_index_count,
                index_info.is_native_columnar,
                indexes_affected_ratio
            );
        }
    }

    // Step 5: Create value updater
    let value_updater = ValueUpdater::new(schema, &evaluator, table_name);

    // Check conflict resolution clause
    let use_ignore = matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Ignore));
    let use_replace = matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace));
    let use_fail = matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Fail));

    // `UPDATE OR FAIL` (SQLite conflict-resolution algorithm): a constraint
    // violation stops the statement at the offending row, but — unlike the
    // default ABORT — the rows already collected/applied before that row are
    // KEPT rather than rolled back (e_update-1.8.3/1.8.9). This mirrors the
    // already-correct `INSERT OR FAIL` behavior (#6275).
    //
    // The general UPDATE path defers PK/UNIQUE checking to a post-statement
    // pass so permutation-style statements like `UPDATE p SET a = a - 1`
    // succeed even with transient intermediate duplicates (#5137). That
    // deferred, all-or-nothing model is incompatible with OR FAIL's
    // keep-the-prefix semantics, so OR FAIL uses a separate, truncating
    // uniqueness check (`truncate_updates_for_or_fail`) instead.
    //
    // Scoped to the simple, unambiguous case — no triggers (so there is no
    // interleaved BEFORE/AFTER row-trigger mutation to reconcile with a
    // truncated update list) and no foreign keys on this table (so there are
    // no deferred-FK-violation entries that would need to be un-queued for a
    // truncated-away row). Rowid relocation conflicts still use the existing
    // all-or-nothing check below (a narrower, documented gap not exercised by
    // e_update.test). OR FAIL statements outside this scope keep the existing
    // (safe) all-or-nothing behavior, matching pre-#6193 semantics.
    let use_fail_partial = use_fail && !has_triggers && schema.foreign_keys.is_empty();
    let mut fail_error: Option<ExecutorError> = None;

    // Step 6: Build list of updates (two-phase execution for SQL semantics)
    // Each `PendingUpdate` carries: row_index, old_row, new_row, changed_columns, updates_pk.
    let mut updates: Vec<PendingUpdate> = Vec::new();

    // Track rows to delete for REPLACE conflict resolution (before applying updates)
    let mut rows_to_delete_for_replace: Vec<usize> = Vec::new();

    // Phase C2 of #5085: collect deferred FK violations during the loop
    // and queue them after the loop ends, since the loop body holds an
    // immutable borrow of `database` (via `table`) and queueing requires
    // `&mut database`.
    let mut pending_deferred_violations: Vec<vibesql_storage::DeferredFkViolation> = Vec::new();

    for (row_index, row) in candidate_rows {
        // Clear CSE cache before evaluating assignment expressions for this row
        // to prevent cached column values from previous rows
        evaluator.clear_cse_cache();

        // Apply assignments to build updated row
        let (mut new_row, mut changed_columns) =
            value_updater.apply_assignments(&row, &stmt.assignments)?;

        // Recompute generated columns if any source columns changed
        // Generated columns are defined with AS(expression) syntax and must be updated
        // whenever their dependent columns are modified
        apply_generated_columns_for_update(schema, &mut new_row, &mut changed_columns)?;

        // Check if primary key is being updated
        let updates_pk = if let Some(ref pk_idx) = pk_indices {
            stmt.assignments.iter().any(|a| {
                // Tuple assignment `SET (a, b, ...) = ...`: any listed column may
                // be part of the primary key.
                if a.is_tuple() {
                    return a.columns.iter().any(|name| {
                        schema.get_column_index(name).is_some_and(|idx| pk_idx.contains(&idx))
                    });
                }

                // Check if this is a rowid assignment
                let col_name_lower = a.column.to_lowercase();
                let is_rowid = col_name_lower == "rowid"
                    || col_name_lower == "_rowid_"
                    || col_name_lower == "oid";

                if is_rowid {
                    // For INTEGER PRIMARY KEY tables, rowid IS the PK
                    // For other tables, rowid is virtual and not in PK
                    schema.rowid_alias_column.is_some()
                } else if let Some(col_index) = schema.get_column_index(&a.column) {
                    pk_idx.contains(&col_index)
                } else {
                    false
                }
            })
        } else {
            false
        };

        // For REPLACE: find and mark conflicting rows for deletion before validation
        if use_replace {
            let conflicting_indices = find_conflicting_rows_for_update(
                table, schema, database, table_name, &new_row, row_index,
            );
            rows_to_delete_for_replace.extend(conflicting_indices);
        }

        // Validate all constraints (NOT NULL, PRIMARY KEY, UNIQUE, CHECK)
        // For IGNORE: catch constraint violations and skip the row
        // For REPLACE: we've already marked conflicts for deletion, so skip PK/UNIQUE validation
        let constraint_validator = ConstraintValidator::new(schema);

        if use_ignore {
            // Non-deterministic date/time uses in index expressions /
            // partial-index predicates abort the statement even under
            // OR IGNORE — SQLite raises a runtime SQL function error, not a
            // constraint conflict, so conflict resolution does not apply
            // (issue #5324). Runs pre-application so no mutation occurs and
            // the lenient index-maintenance paths never see the row.
            crate::insert::constraints::enforce_index_expression_determinism(
                database,
                schema,
                table_name,
                &new_row.values,
            )?;

            // For IGNORE: try validation and skip row on any constraint violation
            let validation_result =
                constraint_validator.validate_row(table, table_name, row_index, &new_row, &row);
            if let Err(e) = validation_result {
                // Non-deterministic date/time use in a CHECK constraint is a
                // statement-level error, not an ignorable conflict (issue #5324).
                if e.is_non_deterministic_use() {
                    return Err(e);
                }
                continue; // Skip this row
            }

            // Validate user-defined UNIQUE indexes
            let unique_index_result =
                constraint_validator.validate_unique_indexes(database, table_name, &new_row, &row);
            if let Err(e) = unique_index_result {
                if e.is_non_deterministic_use() {
                    return Err(e);
                }
                continue; // Skip this row
            }

            // Validate foreign key constraints.
            //
            // Unlike NOT NULL / UNIQUE / PK / CHECK conflicts above, a FOREIGN
            // KEY violation is NOT subject to the OR IGNORE conflict-resolution
            // algorithm: SQLite always raises "FOREIGN KEY constraint failed"
            // for an immediate violation and defers a DEFERRABLE one to COMMIT,
            // regardless of OR IGNORE (fkey2-20.3). So propagate the immediate
            // error instead of skipping the row; only genuinely deferred
            // violations are queued.
            if !schema.foreign_keys.is_empty() {
                let deferred = ForeignKeyValidator::collect_constraints_with_old(
                    database,
                    table_name,
                    &new_row.values,
                    Some(&row.values),
                )?;
                pending_deferred_violations.extend(deferred);
            }
        } else if use_replace {
            // For REPLACE: validate NOT NULL and CHECK constraints, but skip PK/UNIQUE
            // since conflicting rows will be deleted
            validate_non_uniqueness_constraints(schema, table_name, &new_row)?;

            // Non-deterministic date/time uses in index expressions /
            // partial-index predicates abort the statement even under
            // OR REPLACE — runtime SQL function error, not a resolvable
            // conflict (issue #5324). Runs before any conflicting rows are
            // deleted, so the statement aborts with no mutation.
            crate::insert::constraints::enforce_index_expression_determinism(
                database,
                schema,
                table_name,
                &new_row.values,
            )?;

            // Validate foreign key constraints
            if !schema.foreign_keys.is_empty() {
                let deferred = ForeignKeyValidator::collect_constraints_with_old(
                    database,
                    table_name,
                    &new_row.values,
                    Some(&row.values),
                )?;
                pending_deferred_violations.extend(deferred);
            }
        } else if use_fail_partial {
            // OR FAIL (no triggers, no FKs on this table — see `use_fail_partial`
            // above): stop collecting at the first NOT NULL / CHECK violation but
            // KEEP the rows already collected, instead of propagating the error
            // and discarding everything via `?`. PK/UNIQUE conflicts are handled
            // afterward by `truncate_updates_for_or_fail`, which can also cut the
            // list shorter than this loop does.
            if let Err(e) = constraint_validator.validate_row_skip_uniqueness(table_name, &new_row)
            {
                fail_error = Some(e);
                break;
            }

            // Non-deterministic date/time uses in index expressions / partial-
            // index predicates are NOT a resolvable conflict — SQLite aborts the
            // whole statement even under OR FAIL (mirrors OR IGNORE/OR REPLACE
            // above, issue #5324).
            crate::insert::constraints::enforce_index_expression_determinism(
                database,
                schema,
                table_name,
                &new_row.values,
            )?;

            // `use_fail_partial` guarantees `schema.foreign_keys.is_empty()`, so
            // there is nothing to validate/queue here.
        } else {
            // Default: validate NOT NULL and CHECK per-row.
            // PRIMARY KEY / UNIQUE checks are deferred to a post-statement pass
            // (see validate_post_statement_uniqueness below). This matches SQLite's
            // deferred UNIQUE semantics — e.g. `UPDATE p SET a = a - 1` must succeed
            // even when intermediate states transiently duplicate keys (issue #5137).
            constraint_validator.validate_row_skip_uniqueness(table_name, &new_row)?;

            // Reject non-deterministic date/time uses in index expressions /
            // partial-index predicates for the updated row (evaluation-time,
            // SQLite). Runs pre-application so the UPDATE aborts before any
            // mutation and the lenient index-maintenance paths never see it.
            crate::insert::constraints::enforce_index_expression_determinism(
                database,
                schema,
                table_name,
                &new_row.values,
            )?;

            // Enforce FOREIGN KEY constraints (child table)
            if !schema.foreign_keys.is_empty() {
                let deferred = ForeignKeyValidator::collect_constraints_with_old(
                    database,
                    table_name,
                    &new_row.values,
                    Some(&row.values),
                )?;
                pending_deferred_violations.extend(deferred);
            }
        }

        updates.push(PendingUpdate {
            row_index,
            old_row: row.clone(),
            new_row,
            changed_columns,
            updates_pk,
        });
    }

    if use_fail_partial {
        // OR FAIL: replace the deferred, all-or-nothing PK/UNIQUE checks with an
        // immediate, truncating check — a conflict on a later row keeps the
        // earlier rows' changes (and the ones already collected up to it)
        // instead of rolling back the whole statement (e_update-1.8.3/1.8.9).
        if !updates.is_empty() {
            let table_for_check = database
                .get_table(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

            // `fail_error` may already hold a NOT NULL / CHECK violation stashed
            // by the collect loop when it `break`ed at the offending row (rows
            // before it are kept in `updates`). `truncate_updates_for_or_fail`
            // must still run — a PK/UNIQUE conflict among that already-collected
            // prefix happens at an EARLIER row than the NOT NULL/CHECK stop and
            // so takes precedence, cutting `updates` even shorter. But when it
            // finds no such conflict it returns `None`, and that `None` must NOT
            // clobber the pre-existing NOT NULL/CHECK error — otherwise the whole
            // statement silently reports success (the #6193 doctor bug). So only
            // let a Some(err) result replace `fail_error`; keep the original
            // otherwise.
            if let Some(err) = index_sync::truncate_updates_for_or_fail(
                &mut updates,
                schema,
                table_for_check,
                database,
                table_name,
            ) {
                fail_error = Some(err);
            }

            // Rowid relocation conflicts keep the existing all-or-nothing check
            // (a narrower, documented gap — not exercised by e_update.test): if
            // no PK/UNIQUE truncation already happened, a rowid-relocation
            // conflict still aborts the whole statement rather than partially
            // applying.
            if fail_error.is_none() {
                validate_rowid_relocation(&updates, schema, table_for_check)?;
            }
        }
    } else if !use_replace && !use_ignore {
        // Cross-update uniqueness validation: check if multiple updates would produce
        // the same PK or UNIQUE constraint values. This must be done after collecting
        // all updates but before applying them to ensure SQL's two-phase semantics.
        if updates.len() > 1 {
            validate_cross_update_uniqueness(&updates, schema)?;
        }

        // Deferred uniqueness check (issue #5137): validate PK / UNIQUE / user-defined unique
        // indexes against the post-statement table state. Rows that are themselves being
        // updated to a different key are excluded from "existing" entries, allowing
        // statements like `UPDATE p SET a = a - 1` to succeed even when intermediate
        // states transiently duplicate keys.
        if !updates.is_empty() {
            // Re-borrow the table — `database` may have been mutated above for REPLACE,
            // and we need an immutable read of the current PK/UNIQUE indexes.
            let table_for_check = database
                .get_table(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;
            validate_post_statement_uniqueness(
                &updates,
                schema,
                table_for_check,
                database,
                table_name,
            )?;

            // Explicit `UPDATE ... SET rowid = <expr>` on a virtual-rowid table
            // (no INTEGER PRIMARY KEY): relocating onto a rowid another live row
            // already occupies is `UNIQUE constraint failed: <table>.rowid` in
            // sqlite3 (triggerC-7.x). IPK tables write the real PK column and are
            // covered by the PK check above.
            validate_rowid_relocation(&updates, schema, table_for_check)?;

            // Regular (non-rowid) UNIQUE / PRIMARY KEY columns also get sqlite3's
            // IMMEDIATE row-by-row intermediate-collision check (issue #5588): a
            // single-statement swap / ascending-shift on a UNIQUE column errors even
            // though its FINAL state is duplicate-free. The deferred validator above
            // still permits #5137 descending-shift / negation cases that sqlite3
            // accepts; this additive check only rejects what sqlite3 rejects.
            validate_unique_relocation(&updates, schema, table_for_check, database, table_name)?;
        }
    }

    // For REPLACE: handle cross-update conflicts by keeping only the last update
    // for each PK/UNIQUE value. Earlier updates with conflicting values are removed
    // from updates and their rows are deleted instead.
    if use_replace && updates.len() > 1 {
        let removed_indices = resolve_cross_update_conflicts_for_replace(&mut updates, schema);
        rows_to_delete_for_replace.extend(removed_indices);
    }

    // For REPLACE: delete conflicting rows before applying updates
    if use_replace && !rows_to_delete_for_replace.is_empty() {
        // De-duplicate and sort
        rows_to_delete_for_replace.sort_unstable();
        rows_to_delete_for_replace.dedup();

        // Filter out any rows that we're going to update (shouldn't delete our own rows)
        let update_indices: HashSet<usize> = updates.iter().map(|u| u.row_index).collect();
        rows_to_delete_for_replace.retain(|idx| !update_indices.contains(idx));

        if !rows_to_delete_for_replace.is_empty() {
            // Get rows for index cleanup
            let rows_for_index: Vec<(usize, Row)> = rows_to_delete_for_replace
                .iter()
                .filter_map(|&idx| table.scan().get(idx).map(|r| (idx, r.clone())))
                .collect();

            // Issue #5490: UPDATE OR REPLACE removes the conflicting row(s), and
            // SQLite fires that row's DELETE triggers (BEFORE before removal,
            // AFTER once it is gone) — matching the INSERT OR REPLACE path
            // (`insert/replace.rs`). Without this, a WITHOUT ROWID `UPDATE OR
            // REPLACE` that resolves a PRIMARY KEY conflict silently dropped the
            // replaced row's DELETE trigger (triggerF.test 1.2/1.3/1.4).
            //
            // These conflict-deletes are gated on `recursive_triggers`: SQLite
            // fires REPLACE conflict-resolution DELETE triggers "if and only if
            // recursive triggers are enabled" (lang_conflict.html). The row is
            // still removed either way; only the trigger firing is suppressed
            // when recursive_triggers is OFF (triggerC-5.3, #5840).
            let fire_delete_triggers = database
                .catalog
                .get_triggers_for_table(table_name, Some(vibesql_ast::TriggerEvent::Delete))
                .next()
                .is_some()
                && database.recursive_triggers();

            // SQLite processes REPLACE conflict-resolution deletes INTERLEAVED
            // when DELETE triggers fire: for each conflicting row R it runs
            // BEFORE DELETE on R, physically removes R, then runs AFTER DELETE on
            // R, *before* moving to the next conflict row. A trigger body that
            // reads the table mid-statement (e.g. `SELECT count(*) FROM t`)
            // therefore observes the table shrink between conflict deletions
            // (triggerC-5.1.7 / 5.2.7). Firing all BEFOREs, then batch-deleting,
            // then all AFTERs made every trigger body observe the same stale
            // pre-deletion state. This mirrors the INSERT OR REPLACE interleaved
            // path in `insert/replace.rs` (#5840).
            //
            // A RAISE(IGNORE) in a BEFORE DELETE trigger abandons that row's
            // deletion (#5418 parity): the conflicting row survives and may still
            // collide with the pending UPDATE's NEW row.
            if fire_delete_triggers {
                // Defer compaction across the whole loop so each remaining row's
                // physical index stays valid until every conflict row is
                // processed. If an ancestor statement already owns the iteration
                // guard for this table, defer our compaction to it.
                let defer_compaction = crate::compaction_guard::is_iterating(table_name);
                let _iter_guard = crate::compaction_guard::IterationGuard::new(table_name);

                // Phase 1c (Issue #5150 / #5136): capture the active txn id
                // before any mutable borrow so we can stamp xmax on the
                // REPLACE-conflict tombstones when MVCC is on.
                #[cfg(feature = "mvcc_enabled")]
                let mvcc_delete_txn_id = database.transaction_id();

                // Rows whose deletion was abandoned by a BEFORE DELETE
                // RAISE(IGNORE) — they stay live and may still collide with the
                // pending UPDATE's NEW row (#5490).
                let mut abandoned: Vec<(usize, Row)> = Vec::new();
                let mut any_deleted = false;

                for (idx, row) in rows_for_index {
                    // BEFORE(R): a RAISE(IGNORE) abandons this row's delete.
                    let outcome = crate::TriggerFirer::execute_before_triggers(
                        database,
                        table_name,
                        vibesql_ast::TriggerEvent::Delete,
                        Some(&row),
                        None,
                    )?;
                    if outcome == crate::TriggerOutcome::SkipRow {
                        abandoned.push((idx, row));
                        continue;
                    }

                    // A trigger body may have already deleted R (e.g. a nested
                    // DELETE on this table). Skip R rather than double-deleting.
                    if database.get_table(table_name).map(|t| t.is_row_deleted(idx)).unwrap_or(true)
                    {
                        continue;
                    }

                    // Per-row index maintenance (indices stay valid: compaction
                    // is deferred to the end of the loop).
                    let rows_refs: Vec<(usize, &Row)> = vec![(idx, &row)];
                    database.batch_update_indexes_for_delete(table_name, &rows_refs);
                    expression_index_maintenance::maintain_expression_indexes_for_delete(
                        database, table_name, &row, idx,
                    );
                    partial_index_maintenance::maintain_partial_indexes_for_delete(
                        database, table_name, &row, idx,
                    );

                    // delete(R): flip the deletion bitmap for just this row so the
                    // AFTER trigger (and the next conflict row's BEFORE trigger)
                    // observes R as gone. Compaction is deferred.
                    {
                        let table_mut = database
                            .get_table_mut(table_name)
                            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

                        #[cfg(feature = "mvcc_enabled")]
                        if let Some(id) = mvcc_delete_txn_id {
                            table_mut.stamp_row_xmax_inplace(idx, id);
                        }

                        if table_mut.mark_deleted_inplace(idx) {
                            any_deleted = true;
                        }
                    }

                    // Invalidate the database-level columnar cache between the
                    // bitmap delete and the AFTER trigger so a trigger body's
                    // `SELECT count(*)` observes the decremented count.
                    database.invalidate_columnar_cache(table_name);

                    // AFTER(R): the row is already gone; a RAISE(IGNORE) here
                    // cannot un-delete it, so SkipRow is a no-op.
                    let _after_outcome = crate::TriggerFirer::execute_after_triggers(
                        database,
                        table_name,
                        vibesql_ast::TriggerEvent::Delete,
                        Some(&row),
                        None,
                    )?;
                }

                // Issue #5490 (doctor): a BEFORE DELETE RAISE(IGNORE) that
                // abandoned a conflict-row deletion leaves the conflicting row
                // live. If a pending update's NEW row would land on the same
                // PK/UNIQUE key, applying it would create a duplicate key.
                // sqlite3 3.51 (recursive_triggers=ON) raises
                // `UNIQUE constraint failed: <table>.<col>`.
                detect_surviving_replace_conflict(
                    &updates, schema, &abandoned, database, table_name,
                )?;

                // Compact once, after every conflict row is processed (unless an
                // ancestor interleaved loop owns compaction). If it compacts,
                // every row index moved and the indexes must be rebuilt.
                if any_deleted && !defer_compaction {
                    if let Some(table_mut) = database.get_table_mut(table_name) {
                        if table_mut.compact_if_needed() {
                            database.rebuild_indexes(table_name);
                            expression_index_maintenance::rebuild_expression_indexes_after_compaction(
                                database, table_name,
                            );
                            partial_index_maintenance::rebuild_partial_indexes_after_compaction(
                                database, table_name,
                            );
                        }
                    }
                }
            } else {
                // ---- No DELETE triggers fire: fast batch-delete path ----

                // Update indexes before deletion
                let rows_refs: Vec<(usize, &Row)> =
                    rows_for_index.iter().map(|(idx, row)| (*idx, row)).collect();
                database.batch_update_indexes_for_delete(table_name, &rows_refs);

                // Maintain expression indexes for each deleted row
                for (row_index, row) in &rows_for_index {
                    expression_index_maintenance::maintain_expression_indexes_for_delete(
                        database, table_name, row, *row_index,
                    );
                    partial_index_maintenance::maintain_partial_indexes_for_delete(
                        database, table_name, row, *row_index,
                    );
                }

                // Phase 1c (Issue #5150 / #5136): capture the active txn id
                // before the mutable borrow so we can stamp xmax on the
                // REPLACE-conflict tombstones when MVCC is on.
                #[cfg(feature = "mvcc_enabled")]
                let mvcc_delete_txn_id = database.transaction_id();

                // Delete conflicting rows
                let table_mut = database
                    .get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

                #[cfg(feature = "mvcc_enabled")]
                if let Some(id) = mvcc_delete_txn_id {
                    for &idx in &rows_to_delete_for_replace {
                        table_mut.stamp_row_xmax_inplace(idx, id);
                    }
                }

                let delete_result = table_mut.delete_by_indices_batch(&rows_to_delete_for_replace);

                // No compaction: the deleted keys' index entries were already
                // removed by `batch_update_indexes_for_delete`, and the
                // bitmap-delete model keeps every surviving row's physical
                // position stable, so no row-id renumbering is needed (issue
                // #5524 / #5537).
                if delete_result.compacted {
                    database.rebuild_indexes(table_name);
                    // Partial indexes need WHERE-predicate evaluation per row;
                    // the storage `rebuild_indexes` path skips them.
                    partial_index_maintenance::rebuild_partial_indexes_after_compaction(
                        database, table_name,
                    );
                }

                // Invalidate the database-level columnar cache since rows were
                // removed (the table-level cache is handled by delete_by_indices).
                if delete_result.deleted_count > 0 {
                    database.invalidate_columnar_cache(table_name);
                }
            }

            // Issue #5490: deleting the conflicting row(s) above may have
            // COMPACTED the table (the storage layer compacts once the
            // tombstone ratio crosses its threshold — and a prior tombstone
            // from an earlier statement makes that easy to reach even when this
            // statement only removes one row). Compaction shifts every live
            // row's physical index, leaving the `PendingUpdate::row_index`
            // values captured before the delete stale — the apply phase would
            // then write to the wrong slot or a freed slot ("Column index out
            // of bounds"). Re-resolve each update's physical index from the
            // current table state by matching its OLD row (unique by PK).
            // Previously this drift was masked only by the CLI/persistence
            // reload between statements; pure in-memory sessions hit it.
            remap_update_indices_after_compaction(database, table_name, &mut updates)?;
        }
    }

    // Phase C2 of #5085: push deferred FK violations collected during
    // per-row validation onto the active transaction's queue. The
    // immutable borrows of `database` (via `table`) used during
    // validation have been released by this point.
    for v in pending_deferred_violations {
        database.queue_deferred_fk_violation(v);
    }

    // Step 7: Handle CASCADE updates for primary key changes (before triggers)
    // This must happen after validation but before applying parent updates
    for u in &updates {
        if u.updates_pk {
            ForeignKeyValidator::check_no_child_references(
                database, table_name, &u.old_row, &u.new_row,
            )?;
        }
    }

    // Cost-based optimization: Log update cost with indexes_affected_ratio
    if !updates.is_empty() {
        // Compute aggregate changed columns across all updates
        let mut all_changed_columns = HashSet::new();
        for u in &updates {
            all_changed_columns.extend(u.changed_columns.iter().copied());
        }

        let optimizer = DmlOptimizer::new(database, table_name);
        let indexes_affected_ratio =
            optimizer.compute_indexes_affected_ratio(&all_changed_columns, schema);
        let _update_cost = optimizer.estimate_update_cost(updates.len(), indexes_affected_ratio);

        // Log optimization insight: selective updates (low affected ratio) are much cheaper
        if std::env::var("DML_COST_DEBUG").is_ok() && indexes_affected_ratio < 1.0 {
            eprintln!(
                "DML_COST_DEBUG: UPDATE on {} - {} rows, {:.0}% indexes affected (selective update optimization)",
                stmt.table_name,
                updates.len(),
                indexes_affected_ratio * 100.0
            );
        }
    }

    // Phase 1c (Issue #5150 / #5136): stamp xmin on every new row with
    // the active txn id when the `mvcc_enabled` feature is on. We must
    // fetch the txn id here, *before* taking the mutable borrow on
    // `table_mut`, because `database.transaction_id()` borrows the
    // database immutably. When the feature is off this is a no-op so
    // the off-state matches main bit-for-bit.
    let txn_id = database.transaction_id();
    for u in updates.iter_mut() {
        vibesql_storage::stamp_xmin_for_write(&mut u.new_row, txn_id);
        // The new version is by definition live; xmax must be None
        // regardless of feature state.
        u.new_row.xmax = None;
    }

    // Step 8: Apply the updates and fire BEFORE/AFTER ROW triggers.
    //
    // Issue #5486: SQLite fires row triggers INTERLEAVED per row — for each
    // affected row R it runs the BEFORE trigger(s) on R, applies R's change,
    // then runs the AFTER trigger(s) on R, *before* moving to R+1. A trigger
    // body that reads the table mid-statement (e.g. `SELECT sum(a) FROM tbl`)
    // must therefore see exactly the rows processed so far. The previous
    // implementation fired all BEFOREs, then applied all rows, then all
    // AFTERs, which made such triggers observe the wrong running state.
    //
    // A RAISE(IGNORE) in a BEFORE UPDATE trigger abandons that row: it is
    // neither updated nor counted. AFTER triggers run after the row is already
    // updated; a RAISE(IGNORE) there cannot revert it (sqlite3 3.51 keeps the
    // modified row), so SkipRow is a no-op for AFTER.
    let mut index_updates = Vec::new();

    // When a RETURNING expression contains a subquery, that subquery must be
    // recomputed per row as each row's NEW state is applied (it observes the
    // incremental post-UPDATE table state), matching SQLite's correlated-
    // subquery treatment (returning1.test section 20). Detect this up front so
    // the common subquery-free RETURNING keeps the cheap batch path with zero
    // behavior change. `per_row_returning`, when `Some`, holds the fully
    // projected RETURNING result and suppresses the statement-end projection.
    let returning_needs_per_row =
        stmt.returning.as_deref().is_some_and(crate::dml_returning::returning_has_subquery);
    let mut per_row_returning: Option<crate::select::SelectResult> = None;

    if has_triggers {
        // Register this table as under interleaved iteration so a nested DELETE
        // on it (fired by a row trigger below) defers compaction — compacting
        // would shift our not-yet-processed physical row indices (#5486).
        let _iter_guard = crate::compaction_guard::IterationGuard::new(table_name);
        for u in &mut updates {
            // BEFORE(R): a RAISE(IGNORE) drops this row entirely.
            let before_outcome = crate::TriggerFirer::execute_before_triggers(
                database,
                table_name,
                vibesql_ast::TriggerEvent::Update(None),
                Some(&u.old_row),
                Some(&u.new_row),
            )?;
            if before_outcome == crate::TriggerOutcome::SkipRow {
                continue;
            }

            // A trigger fired for an earlier row (or this row's own BEFORE
            // trigger) may have deleted this row mid-statement — e.g.
            // trigger1-1.11, where an AFTER UPDATE trigger runs
            // `DELETE FROM t WHERE a = old.a + 2`. SQLite does not update a row
            // that no longer exists, so skip it rather than erroring on the
            // now-vacant storage slot.
            if database.get_table(table_name).map(|t| t.is_row_deleted(u.row_index)).unwrap_or(true)
            {
                continue;
            }

            // Item 3 (#5840): a BEFORE UPDATE trigger may have written to THIS
            // same row (e.g. `UPDATE t SET c = ... WHERE id = old.id`). Those
            // writes already landed in storage. `u.new_row` was snapshotted
            // pre-trigger, so re-applying it verbatim would clobber the
            // trigger's writes. SQLite instead re-reads the current row and
            // applies only the parent statement's SET columns on top, so a
            // trigger's write to a column the parent does NOT set survives —
            // and is visible to AFTER triggers, RETURNING, and index
            // maintenance (all of which consume `u.new_row` below). Merge the
            // current stored values for every column outside
            // `u.changed_columns` back into `u.new_row`.
            if let Some(current) =
                database.get_table(table_name).and_then(|t| t.get_row(u.row_index))
            {
                if current.values != u.old_row.values {
                    for col_idx in 0..u.new_row.values.len() {
                        if !u.changed_columns.contains(&col_idx) {
                            if let Some(v) = current.values.get(col_idx) {
                                u.new_row.values[col_idx] = v.clone();
                            }
                        }
                    }
                }
            }

            // apply(R): mutate just this row so the AFTER trigger (and the
            // BEFORE trigger of the next row) observes R's change.
            {
                let table_mut = database
                    .get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;
                table_mut
                    .update_row_selective(u.row_index, u.new_row.clone(), &u.changed_columns)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            }

            // Invalidate the database-level columnar cache NOW, between the
            // per-row apply and the AFTER trigger (#5543, the UPDATE analogue
            // of the #5523/#5542 DELETE fix). `update_row_selective` above only
            // invalidates the *table-level* columnar cache; the columnar
            // aggregate path that serves a trigger body's
            // `SELECT sum(col)/max(col)/col FROM t` reads from the *database*-
            // level LRU snapshot (`Database::get_columnar`). Left un-invalidated
            // until the loop end (~the post-loop `invalidate_columnar_cache`
            // below), an AFTER UPDATE trigger — and the next row's BEFORE
            // trigger — would observe the *pre-update* column values for this
            // and prior rows. Dropping the stale snapshot here makes every
            // mid-statement read see the applied updates, matching sqlite3
            // 3.51. This is gated on `has_triggers`, so the common no-trigger
            // bulk UPDATE (the `else` branch below) pays no per-row cost.
            // Native columnar tables short-circuit this call (they maintain
            // columnar data incrementally).
            database.invalidate_columnar_cache(table_name);

            // AFTER(R): the row is already updated; drop the must-use outcome
            // since SkipRow cannot un-apply it (#5418).
            let _after_outcome = crate::TriggerFirer::execute_after_triggers(
                database,
                table_name,
                vibesql_ast::TriggerEvent::Update(None),
                Some(&u.old_row),
                Some(&u.new_row),
            )?;

            index_updates.push((
                u.row_index,
                u.old_row.clone(),
                u.new_row.clone(),
                u.changed_columns.clone(),
            ));
        }
        // Keep only the rows that actually applied (BEFORE-IGNORE'd rows are
        // dropped) so RETURNING / change-count reflect SQLite semantics.
        let applied: HashSet<usize> = index_updates.iter().map(|(idx, ..)| *idx).collect();
        updates.retain(|u| applied.contains(&u.row_index));
    } else if returning_needs_per_row {
        // No triggers, but RETURNING contains a subquery: apply each row's NEW
        // state, then project that row's RETURNING against the now-updated table
        // so subqueries recompute after each step. Applying and projecting are
        // interleaved (the batch fast path below cannot express this because it
        // projects only once, after every row is already updated).
        let items = stmt.returning.as_deref().expect("returning present");
        let visible_columns = crate::dml_returning::visible_columns(schema);
        let columns =
            crate::dml_returning::derive_returning_columns(items, schema, None, &visible_columns)?;
        let mut result_rows: Vec<Row> = Vec::with_capacity(updates.len());

        for u in &updates {
            {
                let table_mut = database
                    .get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;
                table_mut
                    .update_row_selective(u.row_index, u.new_row.clone(), &u.changed_columns)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            }

            // Drop the stale database-level columnar snapshot so the RETURNING
            // subquery sees this row's applied NEW values.
            database.invalidate_columnar_cache(table_name);

            let projected = crate::dml_returning::project_returning_row(
                items,
                &columns,
                schema,
                database,
                &u.new_row,
                &visible_columns,
                cte_results.as_ref(),
            )?;
            result_rows.push(projected);

            index_updates.push((
                u.row_index,
                u.old_row.clone(),
                u.new_row.clone(),
                u.changed_columns.clone(),
            ));
        }

        per_row_returning = Some(crate::select::SelectResult { columns, rows: result_rows });
    } else {
        // No triggers: apply all updates in one batch borrow.
        let table_mut = database
            .get_table_mut(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;
        for u in &updates {
            table_mut
                .update_row_selective(u.row_index, u.new_row.clone(), &u.changed_columns)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

            index_updates.push((
                u.row_index,
                u.old_row.clone(),
                u.new_row.clone(),
                u.changed_columns.clone(),
            ));
        }
    }

    let update_count = index_updates.len();

    // Now update user-defined indexes after releasing table borrow
    // Pass changed_columns to skip indexes that don't involve any modified columns
    // Clone for rollback support if assertions exist
    let index_updates_for_rollback: Vec<_> = index_updates
        .iter()
        .map(|(idx, old, _new, changed)| (*idx, old.clone(), changed.clone()))
        .collect();
    for (index, old_row, new_row, changed_columns) in index_updates {
        database.update_indexes_for_update(
            table_name,
            &old_row,
            &new_row,
            index,
            Some(&changed_columns),
        );

        // Maintain expression indexes for this update
        expression_index_maintenance::maintain_expression_indexes_for_update(
            database, table_name, &old_row, &new_row, index,
        );
        partial_index_maintenance::maintain_partial_indexes_for_update(
            database, table_name, &old_row, &new_row, index,
        );
    }

    // Invalidate the database-level columnar cache since table data changed.
    // Note: Table-level cache is invalidated by update_row_fast()/update_row_selective().
    // Both invalidations are necessary because they manage separate caches:
    // - Table-level cache: used by Table::scan_columnar() for SIMD filtering
    // - Database-level cache: used by Database::get_columnar() for cached access
    if update_count > 0 {
        database.invalidate_columnar_cache(table_name);
    }

    // Fire AFTER STATEMENT triggers only if triggers exist
    if fire_statement_triggers {
        // Statement-level RAISE(IGNORE) has no sqlite3 analog (see BEFORE
        // STATEMENT note above); drop the must-use outcome (#5418).
        let _stmt_outcome = crate::TriggerFirer::execute_after_statement_triggers(
            database,
            table_name,
            vibesql_ast::TriggerEvent::Update(None),
        )?;
    }

    // Check all assertions after UPDATE completes (SQL:1999 Feature F671/F672)
    // This ensures database-wide integrity constraints are maintained
    if let Err(assertion_error) =
        crate::advanced_objects::AssertionChecker::check_all_assertions(database)
    {
        // Assertion violated - rollback the update by restoring old values
        if let Some(table_mut) = database.get_table_mut(table_name) {
            for (index, old_row, changed_columns) in &index_updates_for_rollback {
                // Restore the old row values for changed columns
                let _ = table_mut.update_row_selective(*index, old_row.clone(), changed_columns);
            }
        }
        // Also invalidate cache after rollback
        database.invalidate_columnar_cache(table_name);
        return Err(assertion_error);
    }

    // Project RETURNING items against the NEW rows (SQLite 3.35.0+), with
    // the statement's WITH-clause CTEs (if any) visible to subqueries in
    // RETURNING expressions (issue #5359).
    //
    // The RETURNING clause does NOT honor the table alias, even though the
    // rest of the statement (WHERE/SET) does. In SQLite, `UPDATE t1 AS a ...
    // RETURNING a.b` raises `no such column: a.b` while `RETURNING t1.b`
    // succeeds — the opposite of WHERE/SET resolution (see returning1.test
    // 7.7/7.8, issue #5840 item 6). Pass `None` so RETURNING resolves
    // qualified references against the real table name, not the alias.
    let returning = if let Some(per_row) = per_row_returning {
        // Already projected per row (subquery-bearing RETURNING).
        Some(per_row)
    } else if let Some(items) = &stmt.returning {
        let new_rows: Vec<&Row> = updates.iter().map(|u| &u.new_row).collect();
        Some(crate::dml_returning::project_returning(
            items,
            schema,
            database,
            None,
            &new_rows,
            cte_results.as_ref(),
        )?)
    } else {
        None
    };

    // OR FAIL: the rows collected before the offending row (if any) have
    // already been applied by the write path above, matching SQLite's "keep
    // prior changes" semantics; surface the stashed violation now instead of
    // reporting success (mirrors `INSERT OR FAIL`, #6275).
    if let Some(e) = fail_error {
        return Err(e);
    }

    Ok((update_count, returning))
}

/// Re-resolve each pending update's physical `row_index` against the current
/// table state after a REPLACE-conflict delete (issue #5490).
///
/// Deleting the conflicting row(s) can compact the table, which shifts every
/// live row's physical index and invalidates the `row_index` captured when the
/// updates were collected. We locate each update's row by its OLD row value
/// (unique by primary key for the REPLACE path) among the live rows. Updates
/// whose old row can no longer be found — e.g. it was itself the conflicting
/// row removed by a sibling update — are dropped.
fn remap_update_indices_after_compaction(
    database: &Database,
    table_name: &str,
    updates: &mut Vec<PendingUpdate>,
) -> Result<(), ExecutorError> {
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    // Build a value -> physical index map over the live rows. Keyed on the full
    // row values so it works for both WITHOUT ROWID (PK-keyed) and ROWID tables.
    let mut live_index: std::collections::HashMap<&[SqlValue], usize> =
        std::collections::HashMap::new();
    for (idx, row) in table.scan_live() {
        live_index.entry(row.values.as_ref()).or_insert(idx);
    }

    updates.retain_mut(|u| {
        if let Some(&idx) = live_index.get(u.old_row.values.as_ref()) {
            u.row_index = idx;
            true
        } else {
            // The old row is gone (already removed as a conflicting row). Drop
            // this update; there is nothing left to modify.
            false
        }
    });

    Ok(())
}

/// Validate only NOT NULL and CHECK constraints (for REPLACE conflict resolution)
/// This skips PK and UNIQUE validation since conflicting rows will be deleted
fn validate_non_uniqueness_constraints(
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    new_row: &Row,
) -> Result<(), ExecutorError> {
    // Check NOT NULL constraints
    for (col_idx, col) in schema.columns.iter().enumerate() {
        let value =
            new_row.get(col_idx).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_idx })?;

        if !col.nullable && *value == SqlValue::Null {
            // SQLite-compatible format: "NOT NULL constraint failed: <table>.<column>"
            return Err(ExecutorError::SqliteCompatError(format!(
                "NOT NULL constraint failed: {}.{}",
                table_name, col.name
            )));
        }
    }

    // Check CHECK constraints
    if !schema.check_constraints.is_empty() {
        let evaluator = ExpressionEvaluator::new(schema);

        for (constraint_name, check_expr) in &schema.check_constraints {
            let result = evaluator.eval(check_expr, new_row)?;

            // CHECK passes if the result is NULL or casts to a non-zero
            // NUMERIC; it fails when the result casts to zero (integer 0 /
            // real 0.0), which includes non-numeric text like 'abc' → 0.
            if crate::evaluator::operators::check_constraint_violated(&result) {
                // SQLite-compatible error format: "CHECK constraint failed: <name_or_expr>"
                return Err(ExecutorError::SqliteCompatError(format!(
                    "CHECK constraint failed: {}",
                    constraint_name
                )));
            }
        }
    }

    Ok(())
}

/// Validate SET expressions at preparation time (before row selection).
///
/// SQLite validates expressions when preparing statements, not during execution.
/// This ensures errors like "no such column: x" or "no such function: y" are
/// raised even when no rows would match the WHERE clause.
///
/// This validation walks each assignment's value expression and checks that:
/// - All column references exist in the table schema
/// - All function calls refer to valid functions
/// Reject column qualifiers that name the original (un-aliased) table while an
/// alias is active. SQLite scoping: `UPDATE t1 AS a SET ... WHERE t1.x=1` raises
/// `no such column: t1.x` because the alias `a` is the only valid qualifier for
/// the target table. The alias itself (and unqualified columns) still resolve.
///
/// This runs at prepare time so the error is raised even when no rows match the
/// WHERE clause (e.g. an empty table), matching SQLite. The row-by-row evaluator
/// enforces the same rule in `eval.rs` for the rows-present case.
fn validate_alias_scoped_qualifiers(
    expr: &Expression,
    table_name: &str,
    alias: &str,
) -> Result<(), ExecutorError> {
    match expr {
        Expression::ColumnRef(col_id) => {
            if let Some(qualifier) = col_id.table_canonical() {
                if qualifier.eq_ignore_ascii_case(table_name)
                    && !qualifier.eq_ignore_ascii_case(alias)
                {
                    return Err(ExecutorError::NoSuchColumn {
                        column_ref: format!(
                            "{}.{}",
                            col_id.table_display().unwrap_or(qualifier),
                            col_id.column_display()
                        ),
                    });
                }
            }
            Ok(())
        }
        Expression::BinaryOp { left, right, .. } => {
            validate_alias_scoped_qualifiers(left, table_name, alias)?;
            validate_alias_scoped_qualifiers(right, table_name, alias)
        }
        Expression::UnaryOp { expr, .. } => {
            validate_alias_scoped_qualifiers(expr, table_name, alias)
        }
        Expression::IsNull { expr, .. } => {
            validate_alias_scoped_qualifiers(expr, table_name, alias)
        }
        Expression::Between { expr, low, high, .. } => {
            validate_alias_scoped_qualifiers(expr, table_name, alias)?;
            validate_alias_scoped_qualifiers(low, table_name, alias)?;
            validate_alias_scoped_qualifiers(high, table_name, alias)
        }
        Expression::InList { expr, values, .. } => {
            validate_alias_scoped_qualifiers(expr, table_name, alias)?;
            for item in values {
                validate_alias_scoped_qualifiers(item, table_name, alias)?;
            }
            Ok(())
        }
        Expression::Function { args, .. } => {
            for arg in args {
                validate_alias_scoped_qualifiers(arg, table_name, alias)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_set_expressions(
    schema: &vibesql_catalog::TableSchema,
    assignments: &[vibesql_ast::Assignment],
    database: &Database,
) -> Result<(), ExecutorError> {
    for assignment in assignments {
        // Tuple assignment `SET (a, b, ...) = ...`: validate every target
        // column in the list, then the shared RHS expression.
        if assignment.is_tuple() {
            for name in &assignment.columns {
                if schema.get_column_index(name).is_none() {
                    return Err(ExecutorError::NoSuchColumn { column_ref: name.clone() });
                }
            }
            validate_expression(&assignment.value, schema, database)?;
            continue;
        }

        // Validate target column exists (LHS of assignment)
        // Special case: rowid is a virtual column that always exists (SQLite compatibility)
        let col_name_lower = assignment.column.to_lowercase();
        let is_rowid =
            col_name_lower == "rowid" || col_name_lower == "_rowid_" || col_name_lower == "oid";
        if !is_rowid && schema.get_column_index(&assignment.column).is_none() {
            return Err(ExecutorError::NoSuchColumn { column_ref: assignment.column.clone() });
        }

        // Skip DEFAULT keyword - it's always valid in SET context
        if matches!(assignment.value, Expression::Default) {
            continue;
        }

        // Recursively validate the expression (RHS of assignment)
        validate_expression(&assignment.value, schema, database)?;
    }
    Ok(())
}

/// Recursively validate an expression, checking column and function references.
fn validate_expression(
    expr: &Expression,
    schema: &vibesql_catalog::TableSchema,
    database: &Database,
) -> Result<(), ExecutorError> {
    match expr {
        // Column reference - verify it exists in schema
        Expression::ColumnRef(col_id) => {
            let column_name = col_id.column_canonical();

            // Check for ROWID pseudo-columns (always valid)
            let column_lower = column_name.to_lowercase();
            if column_lower == "rowid" || column_lower == "_rowid_" || column_lower == "oid" {
                // ROWID is always valid if there's no real column with that name
                if schema.get_column_index(column_name).is_none() {
                    return Ok(());
                }
            }

            // Check if column exists in schema
            if schema.get_column_index(column_name).is_none() {
                return Err(ExecutorError::NoSuchColumn { column_ref: column_name.to_string() });
            }
            Ok(())
        }

        // Function call - verify the function exists
        Expression::Function { name, args, character_unit } => {
            // Validate the function name
            let func_name = name.display();
            validate_function_exists(&func_name, database)?;

            // Recursively validate all arguments
            for arg in args {
                validate_expression(arg, schema, database)?;
            }

            // character_unit doesn't need validation
            let _ = character_unit;
            Ok(())
        }

        // Binary operations - validate both sides
        Expression::BinaryOp { left, right, .. } => {
            validate_expression(left, schema, database)?;
            validate_expression(right, schema, database)?;
            Ok(())
        }

        // Unary operations - validate the operand
        Expression::UnaryOp { expr, .. } => validate_expression(expr, schema, database),

        // CASE expression - validate all parts
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_expression(op, schema, database)?;
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    validate_expression(cond, schema, database)?;
                }
                validate_expression(&case_when.result, schema, database)?;
            }
            if let Some(else_expr) = else_result {
                validate_expression(else_expr, schema, database)?;
            }
            Ok(())
        }

        // CAST expression
        Expression::Cast { expr, .. } => validate_expression(expr, schema, database),

        // BETWEEN expression
        Expression::Between { expr, low, high, .. } => {
            validate_expression(expr, schema, database)?;
            validate_expression(low, schema, database)?;
            validate_expression(high, schema, database)?;
            Ok(())
        }

        // IN list
        Expression::InList { expr, values, .. } => {
            validate_expression(expr, schema, database)?;
            for val in values {
                validate_expression(val, schema, database)?;
            }
            Ok(())
        }

        // LIKE/GLOB
        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            validate_expression(expr, schema, database)?;
            validate_expression(pattern, schema, database)?;
            Ok(())
        }

        // IS NULL
        Expression::IsNull { expr, .. } => validate_expression(expr, schema, database),

        // Collate
        Expression::Collate { expr, .. } => validate_expression(expr, schema, database),

        // RAISE: validate the error-message expression if present
        Expression::Raise { error_message, .. } => {
            if let Some(msg) = error_message {
                validate_expression(msg, schema, database)?;
            }
            Ok(())
        }

        // POSITION
        Expression::Position { substring, string, .. } => {
            validate_expression(substring, schema, database)?;
            validate_expression(string, schema, database)?;
            Ok(())
        }

        // TRIM
        Expression::Trim { removal_char, string, .. } => {
            if let Some(rc) = removal_char {
                validate_expression(rc, schema, database)?;
            }
            validate_expression(string, schema, database)?;
            Ok(())
        }

        // EXTRACT
        Expression::Extract { expr, .. } => validate_expression(expr, schema, database),

        // INTERVAL
        Expression::Interval { value, .. } => validate_expression(value, schema, database),

        // Conjunction/Disjunction
        Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
            for e in exprs {
                validate_expression(e, schema, database)?;
            }
            Ok(())
        }

        // Expressions that don't need validation (literals, special keywords, etc.)
        Expression::Literal(_)
        | Expression::CollatedLiteral { .. }
        | Expression::Default
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Wildcard => Ok(()),

        // Subqueries - these have their own validation during execution
        Expression::In { .. }
        | Expression::ScalarSubquery(_)
        | Expression::Exists { .. }
        | Expression::QuantifiedComparison { .. } => Ok(()),

        // Other expressions that we don't deeply validate at this stage
        Expression::DuplicateKeyValue { .. }
        | Expression::IsTruthValue { .. }
        | Expression::IsDistinctFrom { .. }
        | Expression::WindowFunction { .. }
        | Expression::AggregateFunction { .. }
        | Expression::NextValue { .. }
        | Expression::MatchAgainst { .. }
        | Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. }
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::RowValueConstructor(_) => Ok(()),
    }
}

/// Check if a function name refers to a valid function.
fn validate_function_exists(name: &str, database: &Database) -> Result<(), ExecutorError> {
    let name_upper = name.to_uppercase();

    // Check built-in scalar functions
    static BUILTIN_FUNCTIONS: &[&str] = &[
        // Aggregate functions
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "TOTAL",
        "GROUP_CONCAT",
        // String functions
        "LENGTH",
        "SUBSTR",
        "SUBSTRING",
        "UPPER",
        "LOWER",
        "TRIM",
        "LTRIM",
        "RTRIM",
        "REPLACE",
        "INSTR",
        "PRINTF",
        "QUOTE",
        "HEX",
        "UNHEX",
        "ZEROBLOB",
        "CHAR",
        "UNICODE",
        "GLOB",
        "LIKE",
        "CONCAT",
        "CONCAT_WS",
        "REVERSE",
        "LEFT",
        "RIGHT",
        "LPAD",
        "RPAD",
        "REPEAT",
        "SPACE",
        "SOUNDEX",
        // Math functions
        "ABS",
        "ROUND",
        "RANDOM",
        "SIGN",
        "CEIL",
        "CEILING",
        "FLOOR",
        "TRUNC",
        "MOD",
        "POWER",
        "SQRT",
        "LOG",
        "LOG10",
        "LOG2",
        "LN",
        "EXP",
        "SIN",
        "COS",
        "TAN",
        "ASIN",
        "ACOS",
        "ATAN",
        "ATAN2",
        "DEGREES",
        "RADIANS",
        "PI",
        // Type functions
        "TYPEOF",
        "CAST",
        "COALESCE",
        "IFNULL",
        "NULLIF",
        "IIF",
        "CASE",
        // Date/time functions
        "DATE",
        "TIME",
        "DATETIME",
        "JULIANDAY",
        "STRFTIME",
        "NOW",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "UNIXEPOCH",
        "TIMEDIFF",
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "DAYOFWEEK",
        "DAYOFYEAR",
        "WEEKDAY",
        "WEEK",
        "QUARTER",
        "EXTRACT",
        "DATE_ADD",
        "DATE_SUB",
        "DATEDIFF",
        "TIMESTAMPDIFF",
        "ADDDATE",
        "SUBDATE",
        "MAKEDATE",
        "MAKETIME",
        "STR_TO_DATE",
        "DATE_FORMAT",
        "TIME_FORMAT",
        "LAST_DAY",
        "MONTHNAME",
        "DAYNAME",
        // SQLite-specific
        "LAST_INSERT_ROWID",
        "CHANGES",
        "TOTAL_CHANGES",
        "SQLITE_VERSION",
        "SQLITE_SEARCH_COUNT",
        "SQLITE_SEARCH_COUNT_RESET",
        "INSTR",
        "LIKELY",
        "UNLIKELY",
        "LOAD_EXTENSION",
        "MAX",
        "MIN", // Can be used as scalar functions too
        // Encryption (if enabled)
        "MD5",
        "SHA1",
        "SHA256",
        "SHA384",
        "SHA512",
        // JSON functions
        "JSON",
        "JSON_ARRAY",
        "JSON_OBJECT",
        "JSON_EXTRACT",
        "JSON_QUOTE",
        "JSON_TYPE",
        "JSON_VALID",
        "JSON_ARRAY_LENGTH",
        "JSON_PATCH",
        "JSON_REMOVE",
        "JSON_REPLACE",
        "JSON_SET",
        "JSON_INSERT",
        "JSON_GROUP_ARRAY",
        "JSON_GROUP_OBJECT",
        "JSON_EACH",
        "JSON_TREE",
        // JSONB functions (binary JSON representation)
        "JSONB",
        "JSONB_ARRAY",
        "JSONB_OBJECT",
        "JSONB_EXTRACT",
        "JSONB_PATCH",
        "JSONB_REMOVE",
        "JSONB_REPLACE",
        "JSONB_SET",
        "JSONB_INSERT",
        // Window functions (also valid as regular aggregates in some contexts)
        "ROW_NUMBER",
        "RANK",
        "DENSE_RANK",
        "NTILE",
        "LAG",
        "LEAD",
        "FIRST_VALUE",
        "LAST_VALUE",
        "NTH_VALUE",
        "CUME_DIST",
        "PERCENT_RANK",
        // Misc
        "ZEROBLOB",
        "RANDOMBLOB",
        "BLOB",
        "OCTET_LENGTH",
        "BIT_LENGTH",
        "POSITION",
        "OVERLAY",
        "SIMILAR",
        "ISNULL",
        "NVL",
        "DECODE",
        "IF",
        "FORMAT",
        "VERSION",
    ];

    if BUILTIN_FUNCTIONS.contains(&name_upper.as_str()) {
        return Ok(());
    }

    // Check user-defined functions in the database
    if database.catalog.get_function(&name_upper).is_some() {
        return Ok(());
    }

    // Function not found
    Err(ExecutorError::NoSuchFunction { function_name: name.to_string() })
}

/// Recompute generated columns after UPDATE assignments are applied.
///
/// Generated columns (defined with AS(expression) syntax) must be recomputed whenever
/// their dependent columns are modified. This function:
/// 1. Evaluates each generated column's expression against the updated row
/// 2. Updates the row with the new computed values
/// 3. Tracks generated columns in changed_columns for index maintenance
fn apply_generated_columns_for_update(
    schema: &vibesql_catalog::TableSchema,
    row: &mut Row,
    changed_columns: &mut HashSet<usize>,
) -> Result<(), ExecutorError> {
    // Check if there are any generated columns
    let has_generated = schema.columns.iter().any(|col| col.generated_expr.is_some());
    if !has_generated {
        return Ok(());
    }

    // Create evaluator with the current row values.
    // GeneratedColumn context: non-deterministic date/time uses are rejected
    // at evaluation time (SQLite semantics, date2-140 / date3-620).
    let evaluator = ExpressionEvaluator::new(schema)
        .with_schema_context(crate::evaluator::SchemaExprContext::GeneratedColumn);

    for (col_idx, col) in schema.columns.iter().enumerate() {
        if let Some(generated_expr) = &col.generated_expr {
            // Evaluate the generated expression against the current row
            let generated_value = evaluator.eval(generated_expr, row)?;
            // STRICT tables (issue #6173) enforce the rigid strict-datatype
            // rules on the recomputed generated value, matching the INSERT path.
            let coerced_value = if let Some(st) = schema.strict_type_of(col_idx) {
                crate::strict::enforce_strict_type(generated_value, st, &schema.name, &col.name)?
            } else {
                crate::insert::validation::coerce_value(generated_value, &col.data_type)?
            };

            // Check if the value actually changed
            let old_value = row.get(col_idx);
            let value_changed = old_value != Some(&coerced_value);

            // Update the row with the new generated value
            row.set(col_idx, coerced_value)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

            // Track the generated column as changed for index maintenance
            if value_changed {
                changed_columns.insert(col_idx);
            }
        }
    }

    Ok(())
}

/// Execute UPDATE FROM (multi-table UPDATE) - SQLite 3.33.0+ syntax
///
/// This handles UPDATE statements with FROM clause that join other tables:
/// ```sql
/// UPDATE t1 SET col = t2.val FROM t2 WHERE t1.id = t2.id;
/// ```
///
/// The implementation:
/// 1. Builds a synthetic SELECT joining target table with FROM tables
/// 2. Computes SET expression values in the joined context
/// 3. Applies the pre-computed values to target rows
fn execute_update_from(
    stmt: &UpdateStmt,
    from_clauses: &[vibesql_ast::FromClause],
    database: &mut Database,
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    has_triggers: bool,
    pk_indices: &Option<Vec<usize>>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext<'_>>,
    cte_results: Option<&std::collections::HashMap<String, crate::select::cte::CteResult>>,
) -> Result<(usize, Option<crate::select::SelectResult>), ExecutorError> {
    // STATEMENT-level triggers (a VibeSQL extension) only fire at the top level,
    // never within another trigger's body — matching `execute_internal`. ROW
    // triggers (`has_triggers`) still fire when nested (#5535).
    let fire_statement_triggers = has_triggers && trigger_context.is_none();

    // Execute the join and get matched rows with computed SET values
    // Issue #5082: pass trigger_context so the synthetic SELECT can resolve
    // OLD/NEW pseudo-variables when this UPDATE runs inside a trigger body.
    let join_result =
        execute_update_from_join(stmt, from_clauses, database, schema, trigger_context)?;

    // Fire BEFORE STATEMENT triggers if needed
    if fire_statement_triggers {
        // Statement-level RAISE(IGNORE) has no sqlite3 analog; drop the
        // must-use outcome (#5418).
        let _stmt_outcome = crate::TriggerFirer::execute_before_statement_triggers(
            database,
            table_name,
            vibesql_ast::TriggerEvent::Update(None),
        )?;
    }

    // Check conflict resolution clause — matches the default UPDATE path
    // (executor.rs:239-240). The parser populates `stmt.conflict_clause` for
    // `UPDATE OR IGNORE ... FROM` and `UPDATE OR REPLACE ... FROM`; this dispatch
    // path must honor it (issue #5144).
    let use_ignore = matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Ignore));
    let use_replace = matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace));

    // Convert join results to update operations
    let mut updates =
        apply_update_from_matches(&join_result.matched_rows, &stmt.assignments, schema)?;

    if updates.is_empty() {
        // Fire AFTER STATEMENT triggers even when no rows matched
        if fire_statement_triggers {
            // Statement-level RAISE(IGNORE) has no sqlite3 analog; drop the
            // must-use outcome (#5418).
            let _stmt_outcome = crate::TriggerFirer::execute_after_statement_triggers(
                database,
                table_name,
                vibesql_ast::TriggerEvent::Update(None),
            )?;
        }
        return Ok((0, empty_returning(stmt, schema, database, cte_results)?));
    }

    // Validate constraints for each update.
    //
    // Issue #5140: PRIMARY KEY / UNIQUE checks are deferred to a post-statement pass
    // (matching the default UPDATE path's #5137 fix in PR #5138). The per-row
    // `validate_row_skip_uniqueness` enforces NOT NULL / CHECK only; deferred-aware
    // PK / UNIQUE / user-defined-unique-index validation runs after the loop via
    // `validate_post_statement_uniqueness`. Without this, statements like
    //     UPDATE p SET a = a + delta.shift FROM delta WHERE p.a = delta.id
    // fail with a spurious "UNIQUE constraint failed" when intermediate states
    // transiently duplicate keys.
    //
    // Issue #5144: when `stmt.conflict_clause` is IGNORE or REPLACE, the per-row
    // skip / evict semantics replace the deferred-UNIQUE post-statement pass. The
    // logic mirrors the default UPDATE path at executor.rs:239-468.
    let constraint_validator = ConstraintValidator::new(schema);

    // Track rows to delete for REPLACE conflict resolution (before applying updates)
    let mut rows_to_delete_for_replace: Vec<usize> = Vec::new();

    // Phase C2 of #5085: collect deferred FK violations during the loop
    // and queue them after the loop ends, since `database` is immutably
    // borrowed once we re-fetch `table` for the post-statement PK/UNIQUE check.
    let mut pending_deferred_violations: Vec<vibesql_storage::DeferredFkViolation> = Vec::new();

    if use_ignore {
        // IGNORE: per-row validate; on any violation, skip the row entirely.
        // FK deferred violations are collected per-row and only kept for surviving rows.
        let table = database
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

        let mut kept_updates: Vec<PendingUpdate> = Vec::with_capacity(updates.len());

        for u in updates.drain(..) {
            // Non-deterministic date/time uses in index expressions /
            // partial-index predicates abort the statement even under
            // OR IGNORE — SQLite raises a runtime SQL function error, not a
            // constraint conflict, so conflict resolution does not apply
            // (issue #5324).
            crate::insert::constraints::enforce_index_expression_determinism(
                database,
                schema,
                table_name,
                &u.new_row.values,
            )?;

            // Per-row: try full validation including PK/UNIQUE; skip on violation.
            let validation_result = constraint_validator.validate_row(
                table,
                table_name,
                u.row_index,
                &u.new_row,
                &u.old_row,
            );
            if let Err(e) = validation_result {
                // Non-deterministic date/time use in a CHECK constraint is a
                // statement-level error, not an ignorable conflict (issue #5324).
                if e.is_non_deterministic_use() {
                    return Err(e);
                }
                continue;
            }

            // Validate user-defined UNIQUE indexes
            let unique_index_result = constraint_validator
                .validate_unique_indexes(database, table_name, &u.new_row, &u.old_row);
            if let Err(e) = unique_index_result {
                if e.is_non_deterministic_use() {
                    return Err(e);
                }
                continue;
            }

            // Validate foreign key constraints (only retain deferred violations
            // for kept rows — FK collection is per-row, so an Err means we skip).
            if !schema.foreign_keys.is_empty() {
                match ForeignKeyValidator::collect_constraints_with_old(
                    database,
                    table_name,
                    &u.new_row.values,
                    Some(&u.old_row.values),
                ) {
                    Ok(deferred) => pending_deferred_violations.extend(deferred),
                    Err(_) => continue,
                }
            }

            kept_updates.push(u);
        }

        updates = kept_updates;

        // Push deferred FK violations now that the immutable `table` borrow is released.
        for v in pending_deferred_violations {
            database.queue_deferred_fk_violation(v);
        }
    } else if use_replace {
        // REPLACE: find conflicting rows for each update (to delete), validate
        // only NOT NULL/CHECK (since conflicts will be removed by deletion).
        {
            let table = database
                .get_table(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

            for u in &updates {
                let conflicting_indices = find_conflicting_rows_for_update(
                    table,
                    schema,
                    database,
                    table_name,
                    &u.new_row,
                    u.row_index,
                );
                rows_to_delete_for_replace.extend(conflicting_indices);

                // NOT NULL + CHECK only (no PK/UNIQUE — those collisions get
                // resolved by deletion).
                validate_non_uniqueness_constraints(schema, table_name, &u.new_row)?;

                // Non-deterministic date/time uses in index expressions /
                // partial-index predicates abort the statement even under
                // OR REPLACE — runtime SQL function error, not a resolvable
                // conflict (issue #5324). Runs before any conflicting rows
                // are deleted, so the statement aborts with no mutation.
                crate::insert::constraints::enforce_index_expression_determinism(
                    database,
                    schema,
                    table_name,
                    &u.new_row.values,
                )?;

                // Foreign key constraints still apply.
                if !schema.foreign_keys.is_empty() {
                    let deferred = ForeignKeyValidator::collect_constraints_with_old(
                        database,
                        table_name,
                        &u.new_row.values,
                        Some(&u.old_row.values),
                    )?;
                    pending_deferred_violations.extend(deferred);
                }
            }
        }

        // Push deferred FK violations now that the immutable `table` borrow is released.
        for v in pending_deferred_violations {
            database.queue_deferred_fk_violation(v);
        }

        // For REPLACE: handle cross-update conflicts by keeping only the last update
        // for each PK/UNIQUE value. Earlier updates with conflicting values are
        // removed from updates and their rows are deleted instead.
        if updates.len() > 1 {
            let removed_indices = resolve_cross_update_conflicts_for_replace(&mut updates, schema);
            rows_to_delete_for_replace.extend(removed_indices);
        }

        // Pre-stage REPLACE deletions BEFORE the update apply phase. Mirrors the
        // default path at executor.rs:411-468.
        if !rows_to_delete_for_replace.is_empty() {
            rows_to_delete_for_replace.sort_unstable();
            rows_to_delete_for_replace.dedup();

            // Filter out any rows that we're going to update (don't delete our own rows).
            let update_indices: HashSet<usize> = updates.iter().map(|u| u.row_index).collect();
            rows_to_delete_for_replace.retain(|idx| !update_indices.contains(idx));

            if !rows_to_delete_for_replace.is_empty() {
                // Get rows for index cleanup (immutable borrow scope).
                let mut rows_for_index: Vec<(usize, Row)> = {
                    let table = database
                        .get_table(table_name)
                        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
                    rows_to_delete_for_replace
                        .iter()
                        .filter_map(|&idx| table.scan().get(idx).map(|r| (idx, r.clone())))
                        .collect()
                };

                // Issue #5490: UPDATE OR REPLACE ... FROM removes the conflicting
                // row(s) too, so fire the replaced row's DELETE triggers (BEFORE
                // before removal, AFTER once gone) — matching the non-FROM path
                // above and INSERT OR REPLACE. Gated on `recursive_triggers`:
                // REPLACE conflict-resolution DELETE triggers fire only when
                // recursive triggers are enabled (lang_conflict.html;
                // triggerC-5.3, #5840). The row is removed regardless.
                let fire_delete_triggers = database
                    .catalog
                    .get_triggers_for_table(table_name, Some(vibesql_ast::TriggerEvent::Delete))
                    .next()
                    .is_some()
                    && database.recursive_triggers();

                if fire_delete_triggers {
                    let mut kept = Vec::with_capacity(rows_for_index.len());
                    // Conflict rows whose deletion was abandoned by a BEFORE
                    // DELETE RAISE(IGNORE) — they stay live.
                    let mut abandoned: Vec<(usize, Row)> = Vec::new();
                    for (idx, row) in rows_for_index {
                        let outcome = crate::TriggerFirer::execute_before_triggers(
                            database,
                            table_name,
                            vibesql_ast::TriggerEvent::Delete,
                            Some(&row),
                            None,
                        )?;
                        if outcome != crate::TriggerOutcome::SkipRow {
                            kept.push((idx, row));
                        } else {
                            abandoned.push((idx, row));
                        }
                    }
                    rows_for_index = kept;
                    let surviving: HashSet<usize> =
                        rows_for_index.iter().map(|(idx, _)| *idx).collect();
                    rows_to_delete_for_replace.retain(|idx| surviving.contains(idx));

                    // Issue #5490 (doctor): same duplicate-PK guard as the
                    // non-FROM path. A BEFORE DELETE RAISE(IGNORE) that left a
                    // conflict row live makes the pending update's NEW row a
                    // duplicate-key violation; match sqlite3 3.51's
                    // `UNIQUE constraint failed` + table-unchanged behavior by
                    // aborting before any storage mutation.
                    detect_surviving_replace_conflict(
                        &updates, schema, &abandoned, database, table_name,
                    )?;
                }

                // Update indexes before deletion
                let rows_refs: Vec<(usize, &Row)> =
                    rows_for_index.iter().map(|(idx, row)| (*idx, row)).collect();
                database.batch_update_indexes_for_delete(table_name, &rows_refs);

                // Maintain expression indexes for each deleted row
                for (row_index, row) in &rows_for_index {
                    expression_index_maintenance::maintain_expression_indexes_for_delete(
                        database, table_name, row, *row_index,
                    );
                    partial_index_maintenance::maintain_partial_indexes_for_delete(
                        database, table_name, row, *row_index,
                    );
                }

                // Phase 1c (Issue #5150 / #5136): capture the active txn
                // id before the mutable borrow so we can stamp xmax on
                // the REPLACE-conflict tombstones when MVCC is on.
                #[cfg(feature = "mvcc_enabled")]
                let mvcc_delete_txn_id = database.transaction_id();

                // Delete conflicting rows
                let table_mut = database
                    .get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

                #[cfg(feature = "mvcc_enabled")]
                if let Some(id) = mvcc_delete_txn_id {
                    for &idx in &rows_to_delete_for_replace {
                        table_mut.stamp_row_xmax_inplace(idx, id);
                    }
                }

                let delete_result = table_mut.delete_by_indices_batch(&rows_to_delete_for_replace);

                // Handle index maintenance based on compaction.
                //
                // No compaction: the deleted keys' index entries were already
                // removed by `batch_update_indexes_for_delete`, and the
                // bitmap-delete model keeps every surviving row's physical
                // position stable, so no row-id renumbering is needed (issue
                // #5524 / #5537).
                if delete_result.compacted {
                    database.rebuild_indexes(table_name);
                    // Partial indexes need WHERE-predicate evaluation per row;
                    // the storage `rebuild_indexes` path skips them. Without
                    // this call, partial-index row indices would point at the
                    // wrong table rows after compaction (silent corruption).
                    partial_index_maintenance::rebuild_partial_indexes_after_compaction(
                        database, table_name,
                    );
                    // Stale `updates` row indices after compaction are repaired
                    // by `remap_update_indices_after_compaction` below.
                }

                if delete_result.deleted_count > 0 {
                    database.invalidate_columnar_cache(table_name);
                }

                // Fire AFTER DELETE triggers now that the conflicting rows are
                // gone (issue #5490).
                if fire_delete_triggers {
                    for (_, row) in &rows_for_index {
                        let _after_outcome = crate::TriggerFirer::execute_after_triggers(
                            database,
                            table_name,
                            vibesql_ast::TriggerEvent::Delete,
                            Some(row),
                            None,
                        )?;
                    }
                }

                // Re-resolve stale update indices after a possibly-compacting
                // REPLACE-conflict delete (issue #5490 — see the non-FROM path).
                remap_update_indices_after_compaction(database, table_name, &mut updates)?;
            }
        }
    } else {
        // Default: per-row NOT NULL/CHECK; PK/UNIQUE deferred to post-statement pass.
        for u in &updates {
            constraint_validator.validate_row_skip_uniqueness(table_name, &u.new_row)?;

            // Reject non-deterministic date/time uses in index expressions /
            // partial-index predicates (evaluation-time, SQLite semantics).
            crate::insert::constraints::enforce_index_expression_determinism(
                database,
                schema,
                table_name,
                &u.new_row.values,
            )?;

            // Validate foreign key constraints
            if !schema.foreign_keys.is_empty() {
                let deferred = ForeignKeyValidator::collect_constraints_with_old(
                    database,
                    table_name,
                    &u.new_row.values,
                    Some(&u.old_row.values),
                )?;
                pending_deferred_violations.extend(deferred);
            }
        }

        // Push deferred FK violations onto the queue now that the table has not
        // yet been re-borrowed for the post-statement uniqueness check.
        for v in pending_deferred_violations {
            database.queue_deferred_fk_violation(v);
        }
    }

    // After IGNORE may have filtered to zero rows.
    if updates.is_empty() {
        // Fire AFTER STATEMENT triggers even when all rows skipped.
        if fire_statement_triggers {
            // Statement-level RAISE(IGNORE) has no sqlite3 analog; drop the
            // must-use outcome (#5418).
            let _stmt_outcome = crate::TriggerFirer::execute_after_statement_triggers(
                database,
                table_name,
                vibesql_ast::TriggerEvent::Update(None),
            )?;
        }
        return Ok((0, empty_returning(stmt, schema, database, cte_results)?));
    }

    // Cross-update uniqueness validation: catches multiple updates landing on the
    // same final PK / UNIQUE value (e.g. `UPDATE p SET a = 5 FROM ...` matching
    // multiple rows). Skip for IGNORE/REPLACE since those modes have their own
    // per-row resolution (matches the default-path gate at executor.rs:377).
    if !use_replace && !use_ignore && updates.len() > 1 {
        validate_cross_update_uniqueness(&updates, schema)?;
    }

    // Deferred uniqueness check (issue #5140 — port of #5138 to UPDATE FROM):
    // validate PK / UNIQUE / user-defined unique indexes against the post-statement
    // table state. Rows that are themselves being updated to a different key are
    // excluded from "existing" entries, allowing cross-row PK shifts via FROM
    // (`UPDATE p SET a = a + delta.shift FROM delta WHERE p.a = delta.id`) to
    // succeed even when intermediate states transiently duplicate keys.
    //
    // Skipped for IGNORE/REPLACE since those modes use per-row validation/resolution
    // (matches the default-path gate at executor.rs:388).
    if !use_replace && !use_ignore && !updates.is_empty() {
        // Re-borrow the table — the FK queue push above released the prior immutable borrow.
        let table_for_check = database
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
        validate_post_statement_uniqueness(
            &updates,
            schema,
            table_for_check,
            database,
            table_name,
        )?;

        // Virtual-rowid relocation collision check (see default-path call).
        validate_rowid_relocation(&updates, schema, table_for_check)?;

        // Regular (non-rowid) UNIQUE / PRIMARY KEY immediate intermediate-collision
        // check (issue #5588) — see default-path call for rationale.
        validate_unique_relocation(&updates, schema, table_for_check, database, table_name)?;
    }

    // Handle CASCADE updates for primary key changes
    for u in &updates {
        if u.updates_pk {
            ForeignKeyValidator::check_no_child_references(
                database, table_name, &u.old_row, &u.new_row,
            )?;
        }
    }

    // Phase 1c (Issue #5150 / #5136): stamp xmin on every new row with
    // the active txn id when the `mvcc_enabled` feature is on. Fetch the
    // txn id before taking the mutable borrow on `table_mut`. Off-state
    // is a no-op.
    let txn_id = database.transaction_id();
    for u in updates.iter_mut() {
        vibesql_storage::stamp_xmin_for_write(&mut u.new_row, txn_id);
        u.new_row.xmax = None;
    }

    // Apply the updates and fire BEFORE/AFTER ROW triggers interleaved per
    // row (issue #5486): BEFORE(R) -> apply(R) -> AFTER(R) before moving to
    // R+1, so a trigger body reading the table mid-statement sees exactly the
    // rows processed so far. See the matching note in `execute_internal`. A
    // RAISE(IGNORE) in a BEFORE trigger drops that row; SkipRow in an AFTER
    // trigger is a no-op (the row is already applied).
    let mut index_updates = Vec::new();
    if has_triggers {
        // See the matching note in `execute_internal` — defer nested-DELETE
        // compaction of this table while we iterate its physical indices.
        let _iter_guard = crate::compaction_guard::IterationGuard::new(table_name);
        for u in &updates {
            let before_outcome = crate::TriggerFirer::execute_before_triggers(
                database,
                table_name,
                vibesql_ast::TriggerEvent::Update(None),
                Some(&u.old_row),
                Some(&u.new_row),
            )?;
            if before_outcome == crate::TriggerOutcome::SkipRow {
                continue;
            }

            // Skip rows an interleaved trigger deleted mid-statement (see the
            // matching note in `execute_internal`).
            if database.get_table(table_name).map(|t| t.is_row_deleted(u.row_index)).unwrap_or(true)
            {
                continue;
            }

            {
                let table_mut = database
                    .get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
                table_mut
                    .update_row_selective(u.row_index, u.new_row.clone(), &u.changed_columns)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            }

            // Drop the stale database-level columnar snapshot between apply(R)
            // and AFTER(R) (#5543) — same rationale as `execute_internal`: a
            // trigger body's `SELECT sum(col)/max(col)/col FROM t` is served
            // from `Database::get_columnar`'s LRU, which `update_row_selective`
            // does not invalidate, so without this an AFTER/next-BEFORE UPDATE
            // trigger would read pre-update values. Gated on `has_triggers`, so
            // the no-trigger `else` branch is untouched; native columnar tables
            // short-circuit the call.
            database.invalidate_columnar_cache(table_name);

            let _after_outcome = crate::TriggerFirer::execute_after_triggers(
                database,
                table_name,
                vibesql_ast::TriggerEvent::Update(None),
                Some(&u.old_row),
                Some(&u.new_row),
            )?;

            index_updates.push((
                u.row_index,
                u.old_row.clone(),
                u.new_row.clone(),
                u.changed_columns.clone(),
            ));
        }
        let applied: HashSet<usize> = index_updates.iter().map(|(idx, ..)| *idx).collect();
        updates.retain(|u| applied.contains(&u.row_index));
    } else {
        let table_mut = database
            .get_table_mut(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
        for u in &updates {
            table_mut
                .update_row_selective(u.row_index, u.new_row.clone(), &u.changed_columns)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

            index_updates.push((
                u.row_index,
                u.old_row.clone(),
                u.new_row.clone(),
                u.changed_columns.clone(),
            ));
        }
    }

    let update_count = index_updates.len();

    // Update indexes
    for (index, old_row, new_row, changed_columns) in index_updates {
        database.update_indexes_for_update(
            table_name,
            &old_row,
            &new_row,
            index,
            Some(&changed_columns),
        );

        expression_index_maintenance::maintain_expression_indexes_for_update(
            database, table_name, &old_row, &new_row, index,
        );
        partial_index_maintenance::maintain_partial_indexes_for_update(
            database, table_name, &old_row, &new_row, index,
        );
    }

    // Invalidate columnar cache
    if update_count > 0 {
        database.invalidate_columnar_cache(table_name);
    }

    // Fire AFTER STATEMENT triggers
    if fire_statement_triggers {
        // Statement-level RAISE(IGNORE) has no sqlite3 analog; drop the
        // must-use outcome (#5418).
        let _stmt_outcome = crate::TriggerFirer::execute_after_statement_triggers(
            database,
            table_name,
            vibesql_ast::TriggerEvent::Update(None),
        )?;
    }

    // Check assertions
    if let Err(assertion_error) =
        crate::advanced_objects::AssertionChecker::check_all_assertions(database)
    {
        return Err(assertion_error);
    }

    // Mark pk_indices as used (it's available for future enhancements)
    let _ = pk_indices;

    // Project RETURNING items against the NEW rows (SQLite 3.35.0+), with
    // the statement's WITH-clause CTEs (if any) visible to subqueries in
    // RETURNING expressions (issue #5363).
    let returning = if let Some(items) = &stmt.returning {
        let new_rows: Vec<&Row> = updates.iter().map(|u| &u.new_row).collect();
        // RETURNING does not honor the table alias (see issue #5840 item 6 /
        // returning1.test 7.7-7.8); resolve against the real table name.
        Some(crate::dml_returning::project_returning(
            items,
            schema,
            database,
            None,
            &new_rows,
            cte_results,
        )?)
    } else {
        None
    };

    Ok((update_count, returning))
}

/// Build an empty RETURNING result (column names only) for statements whose
/// RETURNING clause matched zero rows. Returns `None` when the statement has
/// no RETURNING clause.
fn empty_returning(
    stmt: &UpdateStmt,
    schema: &vibesql_catalog::TableSchema,
    database: &Database,
    cte_results: Option<&std::collections::HashMap<String, crate::select::cte::CteResult>>,
) -> Result<Option<crate::select::SelectResult>, ExecutorError> {
    stmt.returning
        .as_ref()
        .map(|items| {
            // RETURNING does not honor the table alias (issue #5840 item 6);
            // resolve against the real table name.
            crate::dml_returning::project_returning(items, schema, database, None, &[], cte_results)
        })
        .transpose()
}

/// Sort the candidate rows by the UPDATE `ORDER BY` clause (if any) and then
/// apply `OFFSET`/`LIMIT` (SQLite extension for UPDATE).
///
/// This mirrors the DELETE executor's `apply_order_by_and_limit`. When
/// `order_by` is `None` the rows are left in their scan order before LIMIT/OFFSET
/// are applied, matching `UPDATE ... LIMIT n` semantics.
fn apply_order_by_and_limit(
    rows_and_indices: &mut Vec<(usize, Row)>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    limit: &Option<Expression>,
    offset: &Option<Expression>,
    evaluator: &ExpressionEvaluator,
) -> Result<(), ExecutorError> {
    use vibesql_ast::OrderDirection;

    // Sort rows by ORDER BY columns (if present)
    if let Some(order_by) = order_by {
        rows_and_indices.sort_by(|a, b| {
            for item in order_by {
                // Evaluate the ORDER BY expression for both rows
                let val_a = evaluator.eval(&item.expr, &a.1).unwrap_or(SqlValue::Null);
                let val_b = evaluator.eval(&item.expr, &b.1).unwrap_or(SqlValue::Null);

                // Compare values with proper NULL handling.
                // NULLS FIRST: nulls come first (default for DESC)
                // NULLS LAST: nulls come last (default for ASC)
                let nulls_first = match item.nulls_order {
                    Some(vibesql_ast::NullsOrder::First) => true,
                    Some(vibesql_ast::NullsOrder::Last) => false,
                    None => matches!(item.direction, OrderDirection::Desc),
                };

                let cmp = match (&val_a, &val_b) {
                    (SqlValue::Null, SqlValue::Null) => std::cmp::Ordering::Equal,
                    (SqlValue::Null, _) => {
                        if nulls_first {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Greater
                        }
                    }
                    (_, SqlValue::Null) => {
                        if nulls_first {
                            std::cmp::Ordering::Greater
                        } else {
                            std::cmp::Ordering::Less
                        }
                    }
                    _ => val_a.partial_cmp(&val_b).unwrap_or(std::cmp::Ordering::Equal),
                };

                // Apply direction
                let cmp = match item.direction {
                    OrderDirection::Desc => cmp.reverse(),
                    OrderDirection::Asc => cmp,
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    // Evaluate OFFSET expression if present
    let offset_val = if let Some(ref offset_expr) = offset {
        let empty_row = Row::new(vec![]);
        let value = evaluator.eval(offset_expr, &empty_row)?;
        // SQLite numeric-affinity coercion shared with SELECT and DELETE
        // (#6193): INTEGER as-is, a losslessly-integral REAL or numeric TEXT
        // ('4' -> 4, '1.0' -> 1) accepted, NULL/BLOB/non-numeric TEXT/
        // non-integral REAL raise `datatype mismatch`. A negative OFFSET is
        // then treated as 0 (SQLite semantics, #5747).
        match crate::select::coerce_limit_offset_to_i64(value)? {
            n if n < 0 => 0,
            n => n as usize,
        }
    } else {
        0
    };

    // Evaluate LIMIT expression if present
    let limit_val = if let Some(ref limit_expr) = limit {
        let empty_row = Row::new(vec![]);
        let value = evaluator.eval(limit_expr, &empty_row)?;
        // Same numeric-affinity coercion as OFFSET above (#6193). A negative
        // LIMIT means "no limit" (SQLite semantics, #5747); a non-coercible
        // value (NULL, BLOB, 'abc', 1.2) raises `datatype mismatch`.
        match crate::select::coerce_limit_offset_to_i64(value)? {
            n if n < 0 => None,
            n => Some(n as usize),
        }
    } else {
        None
    };

    // Apply OFFSET: skip first N rows
    if offset_val > 0 {
        if offset_val >= rows_and_indices.len() {
            rows_and_indices.clear();
        } else {
            rows_and_indices.drain(..offset_val);
        }
    }

    // Apply LIMIT: keep only first N rows
    if let Some(limit) = limit_val {
        rows_and_indices.truncate(limit);
    }

    // ORDER BY only determines *which* rows fall within the LIMIT; the rows are
    // then always modified in rowid (physical-index) order, so BEFORE/AFTER
    // UPDATE triggers fire in rowid order regardless of the ORDER BY direction
    // (SQLite lang_update.html R-10927-26133; e_update-3.5).
    rows_and_indices.sort_by_key(|(index, _)| *index);

    Ok(())
}

#[cfg(test)]
mod alias_scope_tests {
    use super::validate_alias_scoped_qualifiers;
    use crate::errors::ExecutorError;
    use vibesql_ast::{BinaryOperator, ColumnIdentifier, Expression};
    use vibesql_types::SqlValue;

    fn qualified_eq(table: &str, column: &str) -> Expression {
        Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(ColumnIdentifier::table_column(table, column))),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }
    }

    fn unqualified_eq(column: &str) -> Expression {
        Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple(column, false))),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }
    }

    #[test]
    fn original_table_name_qualifier_is_rejected_when_alias_active() {
        // UPDATE t1 AS a SET ... WHERE t1.x=1 -> `no such column: t1.x` (wherelimit-0.5.2).
        let expr = qualified_eq("t1", "x");
        let err = validate_alias_scoped_qualifiers(&expr, "t1", "a").unwrap_err();
        match err {
            ExecutorError::NoSuchColumn { column_ref } => assert_eq!(column_ref, "t1.x"),
            other => panic!("expected NoSuchColumn, got {other:?}"),
        }
    }

    #[test]
    fn alias_qualifier_is_accepted() {
        // UPDATE t1 AS a SET ... WHERE a.x=1 still resolves.
        let expr = qualified_eq("a", "x");
        assert!(validate_alias_scoped_qualifiers(&expr, "t1", "a").is_ok());
    }

    #[test]
    fn unqualified_column_is_accepted() {
        // UPDATE t1 AS a SET ... WHERE x=1 still resolves (wherelimit-0.5.1).
        let expr = unqualified_eq("x");
        assert!(validate_alias_scoped_qualifiers(&expr, "t1", "a").is_ok());
    }
}
