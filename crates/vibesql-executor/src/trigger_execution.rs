//! Trigger execution logic for firing triggers on DML operations

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use vibesql_ast::{PseudoTable, TriggerEvent, TriggerGranularity, TriggerTiming};
use vibesql_catalog::{TableSchema, TriggerDefinition};
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Maximum trigger recursion depth before VibeSQL aborts with
/// "too many levels of trigger recursion".
///
/// VibeSQL previously used a far stricter cap of 16, which wrongly rejected
/// legitimate recursive-trigger programs at depth 17..=N that SQLite accepts
/// (e.g. triggerC-2.2/2.3 drive ~500 levels). SQLite's compile-time default is
/// `SQLITE_MAX_TRIGGER_DEPTH = 1000` (sqlite3 3.51.0). See #5479.
///
/// On native targets this now matches SQLite exactly: **1000** (#5534). It used
/// to be 700, capped for stack safety because trigger recursion is implemented
/// as native Rust call-stack recursion (~8.7 KiB/level in release) and an 8 MiB
/// thread overflows at depth ~960 — so a static 1000 would CRASH before the cap
/// check fired. We removed that stack dependency by growing the native stack
/// on demand at the trigger-recursion entry point (`stacker::maybe_grow` in
/// [`grow_stack_for_trigger`]): each nested level ensures a large red-zone of
/// free stack before re-entering DML, allocating a fresh heap-backed stack
/// segment when the current thread's stack runs low. Reaching the 1000 cap is
/// therefore a clean "too many levels of trigger recursion" error, never an
/// overflow, regardless of the thread's fixed stack size.
///
/// WASM has no `stacker` backend, so trigger recursion there stays on the fixed
/// native stack and must keep the lower stack-safe cap; see
/// [`MAX_TRIGGER_RECURSION_DEPTH`]'s wasm definition below.
///
/// The runtime `sqlite3_limit(SQLITE_LIMIT_TRIGGER_DEPTH)` clamp
/// ([`effective_trigger_depth_limit`]) clamps a connection's limit into
/// `[1, MAX_TRIGGER_RECURSION_DEPTH]`, so raising this cap automatically raises
/// the runtime ceiling too while preserving the downward-clamp safety (#5536).
#[cfg(not(target_arch = "wasm32"))]
const MAX_TRIGGER_RECURSION_DEPTH: usize = 1000;

/// WASM trigger recursion cap.
///
/// `stacker` has no wasm backend, so on wasm trigger recursion stays on the
/// fixed native stack and we keep the previously-proven stack-safe value of 700
/// (the analysis in the native variant above applies: native call-stack
/// recursion at ~8.7 KiB/level, 700 keeps headroom below an 8 MiB overflow
/// cliff). Reaching SQLite's full 1000 on wasm would require an explicit
/// heap-allocated work stack instead of native recursion (Option B in #5534).
#[cfg(target_arch = "wasm32")]
const MAX_TRIGGER_RECURSION_DEPTH: usize = 700;

/// Free-stack red zone required before entering one more trigger-recursion
/// level, and the size of each fresh stack segment `stacker` allocates when the
/// red zone is not met. A single trigger level (parse + plan + DML + fire) costs
/// ~8.7 KiB in release and ~50 KiB in debug; 1 MiB of red zone is comfortably
/// more than one level needs, and a 8 MiB new-segment keeps allocations rare
/// (one segment covers ~900 release / ~160 debug further levels) so the 1000-cap
/// recursion grows at most a couple of heap segments. Native targets only.
#[cfg(not(target_arch = "wasm32"))]
const TRIGGER_STACK_RED_ZONE: usize = 1024 * 1024;
#[cfg(not(target_arch = "wasm32"))]
const TRIGGER_STACK_GROW_SIZE: usize = 8 * 1024 * 1024;

/// Ensure there is enough native stack to descend one more trigger-recursion
/// level, growing the stack on demand, then run `f`.
///
/// This is the load-bearing stack-safety primitive for #5534: it lets nested
/// trigger firing reach SQLite's full `SQLITE_MAX_TRIGGER_DEPTH = 1000` without
/// depending on the (fixed) worker-thread stack being large enough for 1000
/// native frames. `stacker::maybe_grow` is a no-op when ample stack remains, so
/// the common shallow case has negligible overhead.
///
/// On wasm (`stacker` unavailable) this is a transparent pass-through; the lower
/// wasm cap keeps native recursion within the fixed wasm stack.
#[cfg(not(target_arch = "wasm32"))]
#[inline]
fn grow_stack_for_trigger<R>(f: impl FnOnce() -> R) -> R {
    stacker::maybe_grow(TRIGGER_STACK_RED_ZONE, TRIGGER_STACK_GROW_SIZE, f)
}

#[cfg(target_arch = "wasm32")]
#[inline]
fn grow_stack_for_trigger<R>(f: impl FnOnce() -> R) -> R {
    f()
}

/// Resolve the effective trigger recursion-depth limit for a connection.
///
/// SQLite lets a connection lower its trigger-recursion limit at runtime via
/// `sqlite3_limit(db, SQLITE_LIMIT_TRIGGER_DEPTH, N)` (exercised by
/// triggerC-3.5.x / 3.6.x). VibeSQL honors that here (#5536):
///   - no per-connection limit set: the compile-time stack-safe cap
///     `MAX_TRIGGER_RECURSION_DEPTH`,
///   - a per-connection limit `N`: `N` clamped into the stack-safe range
///     `[1, MAX_TRIGGER_RECURSION_DEPTH]`.
///
/// Clamping matches SQLite, which never lets a runtime limit exceed the
/// compile-time `SQLITE_MAX_TRIGGER_DEPTH`, and never lets it drop below 1
/// (a value <= 0 is treated as "query only" by `sqlite3_limit` and leaves the
/// existing limit unchanged; here a non-positive stored value falls back to the
/// cap). Lowering the limit can only make recursion abort *sooner*, so it is
/// always stack-safe.
fn effective_trigger_depth_limit(db: &Database) -> usize {
    match db.trigger_depth_limit() {
        Some(n) if n >= 1 => (n as usize).min(MAX_TRIGGER_RECURSION_DEPTH),
        _ => MAX_TRIGGER_RECURSION_DEPTH,
    }
}

thread_local! {
    /// Current trigger recursion depth for this thread
    static TRIGGER_RECURSION_DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// RAII guard for managing trigger recursion depth
/// Increments depth on creation, decrements on drop
struct RecursionGuard;

impl RecursionGuard {
    /// Create a new recursion guard, incrementing the depth
    ///
    /// The effective limit is the connection's runtime
    /// `SQLITE_LIMIT_TRIGGER_DEPTH` (when lowered via `sqlite3_limit`), clamped
    /// to the stack-safe compile-time cap. See [`effective_trigger_depth_limit`]
    /// and #5536.
    ///
    /// # Returns
    /// Ok(RecursionGuard) if depth is within limits, Err if limit exceeded
    fn new(db: &Database) -> Result<Self, ExecutorError> {
        let limit = effective_trigger_depth_limit(db);
        TRIGGER_RECURSION_DEPTH.with(|depth| {
            let current = depth.get();
            if current >= limit {
                // SQLite emits exactly "too many levels of trigger recursion"
                // (sqlite3 3.51.0; see triggerC.test 2.x / 6.x and `sqlite3VdbeError`).
                // Use `Other` so the message surfaces verbatim (no "Unsupported
                // expression:" wrapper) and the TCL conformance shim passes it
                // through unchanged. The depth cap now matches SQLite's full
                // SQLITE_MAX_TRIGGER_DEPTH (1000) on native targets via on-demand
                // stack growth; see the stack-safety note on
                // MAX_TRIGGER_RECURSION_DEPTH.
                Err(ExecutorError::Other(
                    "too many levels of trigger recursion".to_string(),
                ))
            } else {
                depth.set(current + 1);
                Ok(RecursionGuard)
            }
        })
    }
}

impl Drop for RecursionGuard {
    fn drop(&mut self) {
        TRIGGER_RECURSION_DEPTH.with(|depth| {
            depth.set(depth.get().saturating_sub(1));
        });
    }
}

thread_local! {
    /// Names of triggers currently executing on this thread, keyed by the
    /// lowercased trigger name with a reference count. Used to honor
    /// `PRAGMA recursive_triggers = off`: when that pragma is off, a trigger is
    /// not re-fired by DML performed while the same trigger is already running.
    ///
    /// A count (rather than a set) is required because with
    /// `recursive_triggers = on` the *same* trigger can legitimately appear on
    /// the stack multiple times (recursive firing), and each level must be
    /// balanced by its own pop. Trigger names are SQLite-unique per schema, so
    /// the lowercased name is a stable identity key.
    static ACTIVE_TRIGGERS: RefCell<HashMap<String, usize>> =
        RefCell::new(HashMap::new());
}

/// RAII guard that records a trigger as "currently executing" for the duration
/// of its action, so the `recursive_triggers = off` suppression check can tell
/// whether re-entering a trigger would be recursive.
struct ActiveTriggerGuard {
    key: String,
}

impl ActiveTriggerGuard {
    fn new(trigger_name: &str) -> Self {
        let key = trigger_name.to_lowercase();
        ACTIVE_TRIGGERS.with(|active| {
            *active.borrow_mut().entry(key.clone()).or_insert(0) += 1;
        });
        ActiveTriggerGuard { key }
    }
}

impl Drop for ActiveTriggerGuard {
    fn drop(&mut self) {
        ACTIVE_TRIGGERS.with(|active| {
            let mut map = active.borrow_mut();
            if let Some(count) = map.get_mut(&self.key) {
                *count -= 1;
                if *count == 0 {
                    map.remove(&self.key);
                }
            }
        });
    }
}

/// Returns true if a trigger with this name is already executing on the current
/// thread (i.e. firing it now would be a recursive re-entry).
fn is_trigger_active(trigger_name: &str) -> bool {
    let key = trigger_name.to_lowercase();
    ACTIVE_TRIGGERS.with(|active| active.borrow().get(&key).is_some_and(|&c| c > 0))
}

/// Execution context for triggers with OLD/NEW row access
/// Provides pseudo-variable resolution for trigger bodies
pub struct TriggerContext<'a> {
    /// OLD row - available for UPDATE and DELETE triggers
    pub old_row: Option<&'a Row>,
    /// NEW row - available for INSERT and UPDATE triggers
    pub new_row: Option<&'a Row>,
    /// Table schema for column lookups
    pub table_schema: &'a TableSchema,
    /// True when the trigger target is a VIEW (INSTEAD OF trigger). Views have
    /// no rowid, so `NEW.rowid` / `OLD.rowid` must error there — matching both
    /// sqlite3 and the view-rowid rejection from #5492. The pseudo-schema built
    /// from a view definition does not carry `without_rowid`, so we track the
    /// view-ness explicitly rather than inferring it from the schema.
    pub is_view: bool,
}

impl<'a> TriggerContext<'a> {
    /// Resolve a pseudo-variable reference to a SqlValue
    ///
    /// # Arguments
    /// * `pseudo_table` - Which pseudo-table (OLD or NEW)
    /// * `column` - Column name to retrieve
    ///
    /// # Returns
    /// Ok(SqlValue) with the column value, or Err if invalid
    ///
    /// # Errors
    /// - If OLD/NEW is not available for this trigger type
    /// - If column doesn't exist in table schema
    pub fn resolve_pseudo_var(
        &self,
        pseudo_table: PseudoTable,
        column: &str,
    ) -> Result<SqlValue, ExecutorError> {
        // Get the appropriate row
        let row = match pseudo_table {
            PseudoTable::Old => self.old_row.ok_or_else(|| {
                ExecutorError::UnsupportedExpression(
                    "OLD pseudo-variable not available in this trigger context".to_string(),
                )
            })?,
            PseudoTable::New => self.new_row.ok_or_else(|| {
                ExecutorError::UnsupportedExpression(
                    "NEW pseudo-variable not available in this trigger context".to_string(),
                )
            })?,
        };

        // Find column index in schema. A *real* column always wins over the
        // rowid pseudo-column — including a real column literally named
        // `rowid` / `oid` / `_rowid_` (triggerD-1.x). Only when no real column
        // matches do we fall back to the rowid pseudo-column below.
        // `get_column_index` matches the case-insensitive resolution the column
        // evaluator uses (the parser uppercases unquoted identifiers).
        if let Some(col_idx) = self.table_schema.get_column_index(column) {
            return Ok(row.values[col_idx].clone());
        }

        // SQLite compatibility: resolve the ROWID pseudo-column on the firing
        // OLD/NEW row. `rowid`, `_rowid_`, and `oid` are aliases that yield the
        // row's unique identifier (#5485). This mirrors the rowid resolution in
        // the column evaluator (`evaluator/expressions/eval.rs`): real columns
        // already took precedence above.
        let column_lower = column.to_lowercase();
        if column_lower == "rowid" || column_lower == "_rowid_" || column_lower == "oid" {
            // A VIEW has no rowid. An INSTEAD OF trigger fires on a view, so
            // `NEW.rowid` / `OLD.rowid` there must error — consistent with the
            // view-rowid rejection in #5492 (no such column: rowid).
            //
            // WITHOUT ROWID tables likewise do not expose the rowid
            // pseudo-column (Issue #4953): error to match sqlite3.
            if !self.is_view && !self.table_schema.without_rowid {
                // INTEGER PRIMARY KEY acts as a rowid alias: its column value IS
                // the rowid, so return that column's value (#4536).
                if let Some(ipk_col_idx) = self.table_schema.rowid_alias_column {
                    return row
                        .values
                        .get(ipk_col_idx)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: ipk_col_idx });
                }

                // Otherwise return the firing row's rowid. For a row that already
                // has a rowid (AFTER INSERT, BEFORE/AFTER UPDATE/DELETE, or an
                // explicit `INSERT INTO t(rowid,...)`), that's `row.row_id`. For a
                // BEFORE INSERT on an auto-allocated rowid the row is not yet
                // written and SQLite reports `new.rowid` as -1 (triggerC-4.1.2):
                // mirror that sentinel here.
                return Ok(SqlValue::Bigint(row.row_id.map(|id| id as i64).unwrap_or(-1)));
            }
        }

        // Not a real column and not a resolvable rowid pseudo-column.
        Err(ExecutorError::ColumnNotFound {
            column_name: column.to_string(),
            table_name: self.table_schema.name.clone(),
            searched_tables: vec![self.table_schema.name.clone()],
            available_columns: self
                .table_schema
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect(),
        })
    }
}

/// Outcome of firing a trigger (or set of triggers) for a row.
///
/// `RAISE(IGNORE)` inside a trigger body asks SQLite to abandon the current
/// row's DML operation without raising an error and continue with the rest of
/// the statement. The trigger firing functions translate the internal
/// [`ExecutorError::RaiseIgnore`] signal into [`TriggerOutcome::SkipRow`] so the
/// DML caller can drop that row; every other (real) error still propagates via
/// `Err`. `RAISE(ABORT|FAIL|ROLLBACK, ..)` are *not* represented here — they are
/// genuine aborts and propagate as [`ExecutorError::Raise`].
///
/// Marked `#[must_use]` (#5418): the trigger-firing helpers return this so the
/// DML caller can drop the current row on [`TriggerOutcome::SkipRow`]. A call
/// site that writes `fire(..)?;` and discards the value would silently swallow
/// a `RAISE(IGNORE)` and apply the row anyway. The attribute turns every such
/// drop into a compile-time warning so new call sites must decide explicitly
/// what to do with `SkipRow`.
#[must_use = "a TriggerOutcome::SkipRow must be honored (skip the row); discarding it silently applies a RAISE(IGNORE)'d row"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerOutcome {
    /// Triggers completed normally; the DML operation should proceed.
    Proceed,
    /// A `RAISE(IGNORE)` fired; the current row should be skipped, and the
    /// surrounding statement should continue with the next row.
    SkipRow,
}

/// Helper struct for trigger firing (execution during DML operations)
pub struct TriggerFirer;

impl TriggerFirer {
    /// Find triggers for a table and event
    ///
    /// # Arguments
    /// * `db` - Database reference
    /// * `table_name` - Name of the table to find triggers for
    /// * `timing` - Trigger timing (BEFORE, AFTER, INSTEAD OF)
    /// * `event` - Trigger event (INSERT, UPDATE, DELETE)
    ///
    /// # Returns
    /// Vector of trigger definitions matching the criteria, sorted by creation order
    pub fn find_triggers(
        db: &Database,
        table_name: &str,
        timing: TriggerTiming,
        event: TriggerEvent,
    ) -> Vec<TriggerDefinition> {
        Self::find_triggers_in_schema(db, table_name, timing, event, None)
    }

    /// Find triggers for a table and event, restricted to the schema the DML
    /// target table resolved to.
    ///
    /// `dml_schema` is the internal schema name (`main` or `temp_<id>`) the target
    /// table resolved to for the current statement (see
    /// [`Catalog::resolve_table_schema_name`]). When `None`, no schema filtering
    /// is applied (legacy schema-unaware behavior). Schema-aware firing keeps a
    /// `main` trigger from firing on a same-named `temp` table and vice versa
    /// (triggerD-3.1/3.2).
    pub fn find_triggers_in_schema(
        db: &Database,
        table_name: &str,
        timing: TriggerTiming,
        event: TriggerEvent,
        dml_schema: Option<&str>,
    ) -> Vec<TriggerDefinition> {
        db.catalog
            .get_triggers_for_table_in_schema(table_name, Some(event.clone()), dml_schema)
            .filter(|trigger| trigger.timing == timing && trigger.enabled) // Skip disabled triggers
            .cloned()
            .collect()
    }

    /// Check if an UPDATE OF trigger should fire based on which columns changed
    ///
    /// # Arguments
    /// * `trigger` - Trigger definition
    /// * `old_row` - OLD row values
    /// * `new_row` - NEW row values
    /// * `table_schema` - Table schema for column lookup
    ///
    /// # Returns
    /// true if the trigger should fire, false otherwise
    fn should_fire_update_of(
        trigger: &TriggerDefinition,
        old_row: &Row,
        new_row: &Row,
        table_schema: &TableSchema,
    ) -> bool {
        match &trigger.event {
            TriggerEvent::Update(Some(columns)) => {
                // Check if any of the specified columns changed
                for col_name in columns {
                    if let Some(col_idx) =
                        table_schema.columns.iter().position(|c| &c.name == col_name)
                    {
                        if col_idx < old_row.values.len()
                            && col_idx < new_row.values.len()
                            && old_row.values[col_idx] != new_row.values[col_idx]
                        {
                            return true; // At least one monitored column changed
                        }
                    }
                }
                false // None of the monitored columns changed
            }
            _ => true, // Not an UPDATE OF trigger, always fire
        }
    }

    /// Execute a single trigger
    ///
    /// # Arguments
    /// * `db` - Mutable database reference
    /// * `trigger` - Trigger definition to execute
    /// * `old_row` - OLD row for UPDATE/DELETE (None for INSERT)
    /// * `new_row` - NEW row for INSERT/UPDATE (None for DELETE)
    ///
    /// # Returns
    /// Ok(()) if trigger executed successfully, Err if execution failed
    ///
    /// # Notes
    /// - For ROW-level triggers, this is called once per affected row
    /// - For STATEMENT-level triggers, this is called once per statement
    /// - WHEN conditions are evaluated here
    pub fn execute_trigger(
        db: &mut Database,
        trigger: &TriggerDefinition,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
    ) -> Result<TriggerOutcome, ExecutorError> {
        // Grow the native stack on demand before descending one more trigger
        // level (#5534). Firing a trigger re-enters the full DML path (parse +
        // plan + execute + fire), which is deep native recursion; without this
        // the 1000-deep recursion SQLite allows would overflow a fixed
        // worker-thread stack. `maybe_grow` is ~free when stack is plentiful, so
        // the non-recursive common case is unaffected. The depth *limit* is
        // still enforced by `RecursionGuard` in the per-timing entry points; this
        // only guarantees we have stack to reach that limit cleanly.
        grow_stack_for_trigger(|| Self::execute_trigger_inner(db, trigger, old_row, new_row))
    }

    /// Inner body of [`Self::execute_trigger`], run on a guaranteed-sufficient
    /// native stack (see `grow_stack_for_trigger`).
    fn execute_trigger_inner(
        db: &mut Database,
        trigger: &TriggerDefinition,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
    ) -> Result<TriggerOutcome, ExecutorError> {
        // 1. Evaluate WHEN condition (if present)
        if let Some(when_expr) = &trigger.when_condition {
            let condition_result = Self::evaluate_when_condition(
                db,
                &trigger.table_name,
                when_expr,
                old_row,
                new_row,
            )?;

            // Skip trigger execution if WHEN condition is false
            if !condition_result {
                return Ok(TriggerOutcome::Proceed);
            }
        }

        // Mark this trigger as executing for the duration of its action so that
        // `PRAGMA recursive_triggers = off` can suppress re-entry into the same
        // trigger from nested DML (#5535). The guard is dropped — and the
        // trigger un-marked — when the action completes or errors out.
        let _active = ActiveTriggerGuard::new(&trigger.name);

        // 2. Execute trigger action
        Self::execute_trigger_action(db, trigger, old_row, new_row)
    }

    /// Evaluate WHEN condition for a trigger
    ///
    /// # Arguments
    /// * `db` - Database reference
    /// * `table_name` - Name of the table
    /// * `when_expr` - WHEN condition expression
    /// * `old_row` - OLD row (for UPDATE/DELETE)
    /// * `new_row` - NEW row (for INSERT/UPDATE)
    ///
    /// # Returns
    /// Ok(true) if condition evaluates to true, Ok(false) otherwise
    fn evaluate_when_condition(
        db: &Database,
        table_name: &str,
        when_expr: &vibesql_ast::Expression,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
    ) -> Result<bool, ExecutorError> {
        // Resolve the schema for OLD/NEW column resolution. For a normal table
        // trigger this is the base table's schema. For an INSTEAD OF trigger the
        // target is a VIEW (views are not tables, so `get_table` returns None),
        // in which case we build a pseudo-schema from the view definition —
        // mirroring `execute_trigger_action`. Without this fallback, an INSTEAD
        // OF trigger carrying a WHEN clause always failed with TableNotFound.
        let schema = Self::resolve_trigger_schema(db, table_name)?;
        // INSTEAD OF triggers fire on a view (not a table); the rowid
        // pseudo-column is unavailable there (#5485 / #5492).
        let is_view = db.catalog.get_table(table_name).is_none();

        // Use NEW row as the base row for evaluation (prefer NEW over OLD)
        // The trigger context will handle OLD/NEW pseudo-variable references
        let row = new_row.or(old_row).ok_or_else(|| {
            ExecutorError::UnsupportedExpression(
                "WHEN condition requires a row context".to_string(),
            )
        })?;

        // Create trigger context for OLD/NEW pseudo-variable resolution
        let trigger_context = TriggerContext { old_row, new_row, table_schema: &schema, is_view };

        // Create evaluator with trigger context
        let evaluator =
            crate::ExpressionEvaluator::with_trigger_context(&schema, db, &trigger_context);
        let result = evaluator.eval(when_expr, row)?;

        // Convert to boolean
        match result {
            vibesql_types::SqlValue::Boolean(b) => Ok(b),
            vibesql_types::SqlValue::Null => Ok(false),
            _ => Err(ExecutorError::UnsupportedExpression(
                "WHEN condition must evaluate to boolean".to_string(),
            )),
        }
    }

    /// Execute trigger action statements
    ///
    /// # Arguments
    /// * `db` - Mutable database reference
    /// * `trigger` - Trigger definition
    /// * `old_row` - OLD row (for UPDATE/DELETE)
    /// * `new_row` - NEW row (for INSERT/UPDATE)
    ///
    /// # Returns
    /// Ok(()) if action executed successfully, Err if execution failed
    fn execute_trigger_action(
        db: &mut Database,
        trigger: &TriggerDefinition,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
    ) -> Result<TriggerOutcome, ExecutorError> {
        // Extract SQL from trigger action
        let sql = match &trigger.triggered_action {
            vibesql_ast::TriggerAction::RawSql(sql) => sql.clone(),
        };

        // Parse the trigger action SQL
        let statements = Self::parse_trigger_sql(&sql)?;

        // Get table schema for trigger context.
        // For INSTEAD OF triggers on views, build schema from the view definition.
        let schema = Self::resolve_trigger_schema(db, &trigger.table_name)?;
        // INSTEAD OF triggers fire on a view (not a table); the rowid
        // pseudo-column is unavailable there (#5485 / #5492).
        let is_view = db.catalog.get_table(&trigger.table_name).is_none();

        // Create trigger context for OLD/NEW pseudo-variable resolution
        let trigger_context = TriggerContext { old_row, new_row, table_schema: &schema, is_view };

        // Execute each statement in the trigger body with trigger context.
        // A RAISE(IGNORE) inside any statement abandons the rest of this
        // trigger's action for the current row and asks the caller to skip the
        // row (SQLite semantics), without raising a user-visible error.
        for statement in statements {
            match Self::execute_statement(db, &statement, &trigger_context) {
                Ok(()) => {}
                Err(ExecutorError::RaiseIgnore) => return Ok(TriggerOutcome::SkipRow),
                Err(e) => return Err(e),
            }
        }

        Ok(TriggerOutcome::Proceed)
    }

    /// Resolve the schema used for OLD/NEW column resolution when a trigger fires.
    ///
    /// For a regular table trigger this is the base table's schema. For an
    /// INSTEAD OF trigger the target is a VIEW — views are not tables, so
    /// `catalog.get_table` returns None and we fall back to a pseudo-schema
    /// built from the view definition. The schema is returned by value (owned)
    /// because the view path constructs it on the fly; the base-table path
    /// clones the catalog entry.
    fn resolve_trigger_schema(
        db: &Database,
        table_name: &str,
    ) -> Result<TableSchema, ExecutorError> {
        if let Some(table_schema) = db.catalog.get_table(table_name) {
            Ok(table_schema.clone())
        } else if let Some(view_def) = db.catalog.get_view(table_name) {
            Self::build_view_schema(db, view_def)
        } else {
            Err(ExecutorError::TableNotFound(table_name.to_string()))
        }
    }

    /// Build a pseudo TableSchema from a view definition for trigger OLD/NEW column resolution
    fn build_view_schema(
        db: &Database,
        view_def: &vibesql_catalog::ViewDefinition,
    ) -> Result<TableSchema, ExecutorError> {
        // Execute the view's SELECT query to get column names
        let select_executor = crate::SelectExecutor::new(db);
        let result = select_executor.execute_with_columns(&view_def.query)?;

        // Use explicit column names if provided, otherwise derive from SELECT
        let column_names: Vec<String> = if let Some(ref cols) = view_def.columns {
            cols.clone()
        } else {
            result.columns.clone()
        };

        // Build columns with a generic data type
        let columns: Vec<vibesql_catalog::ColumnSchema> = column_names
            .into_iter()
            .map(|name| {
                vibesql_catalog::ColumnSchema::new(
                    name,
                    vibesql_types::DataType::Varchar { max_length: None },
                    true,
                )
            })
            .collect();

        Ok(TableSchema::new(view_def.name.clone(), columns))
    }

    /// Parse trigger SQL into statements
    ///
    /// Public so that validation layers (e.g. the Raft replication
    /// freeze pass in `vibesql-consensus::freeze`, #5381) can inspect a
    /// trigger body with **exactly** the parsing this executor will use
    /// when the trigger fires — validating against a different parse
    /// would silently let nondeterminism through.
    ///
    /// # Arguments
    /// * `sql` - Raw SQL string from trigger action
    ///
    /// # Returns
    /// Vector of parsed statements
    pub fn parse_trigger_sql(sql: &str) -> Result<Vec<vibesql_ast::Statement>, ExecutorError> {
        // Split the BEGIN ... END body into statements with the shared,
        // string-literal- and comment-aware splitter. This is the same
        // splitter `Parser::validate_trigger_body` uses at create time, so the
        // create-time and fire-time paths cannot drift, and a `;` inside a
        // string literal (e.g. `INSERT INTO log VALUES('a;b')`) is no longer
        // mistaken for a statement separator. The splitter strips the
        // BEGIN/END wrapper and drops empty / comment-only fragments.
        let mut statements = Vec::new();
        for stmt_sql in vibesql_parser::split_trigger_body_statements(sql) {
            // Parse each body statement as a trigger-program statement so
            // `RAISE()` is admitted at fire time. SQLite only permits RAISE()
            // within a trigger-program, so the general `parse_sql` entry point
            // rejects it at parse time; a trigger body legitimately contains
            // it (see `Parser::parse_sql_in_trigger_body`).
            match vibesql_parser::Parser::parse_sql_in_trigger_body(&stmt_sql) {
                Ok(stmt) => statements.push(stmt),
                Err(e) => {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Failed to parse trigger SQL: {}",
                        e.message
                    )))
                }
            }
        }

        // If no statements parsed (e.g., trigger body was only comments), that's OK
        // Just return empty vector
        Ok(statements)
    }

    /// Execute a single statement from trigger body
    ///
    /// # Arguments
    /// * `db` - Mutable database reference
    /// * `statement` - Statement to execute
    /// * `trigger_context` - Trigger context with OLD/NEW row data
    ///
    /// # Returns
    /// Ok(()) if statement executed successfully
    fn execute_statement(
        db: &mut Database,
        statement: &vibesql_ast::Statement,
        trigger_context: &TriggerContext,
    ) -> Result<(), ExecutorError> {
        use vibesql_ast::Statement;

        match statement {
            Statement::Insert(insert_stmt) => {
                // Execute INSERT with trigger context support
                crate::insert::execute_insert_with_trigger_context(
                    db,
                    insert_stmt,
                    trigger_context,
                )?;
                Ok(())
            }
            Statement::Update(update_stmt) => {
                // Execute UPDATE with trigger context support
                crate::update::execute_update_with_trigger_context(
                    db,
                    update_stmt,
                    trigger_context,
                )?;
                Ok(())
            }
            Statement::Delete(delete_stmt) => {
                // Execute DELETE with trigger context support
                crate::delete::execute_delete_with_trigger_context(
                    db,
                    delete_stmt,
                    trigger_context,
                )?;
                Ok(())
            }
            Statement::Select(select_stmt) => {
                // Execute SELECT but ignore results (useful for side effects, e.g.
                // a body statement of the form `SELECT CASE WHEN NEW.x ... THEN
                // raise(IGNORE) END`).
                //
                // The SELECT must be evaluated with the trigger's NEW/OLD
                // pseudo-row context (#5445): a from-less SELECT that references
                // NEW/OLD has no FROM clause to resolve them from, so the
                // evaluator needs the firing row's context — exactly as the WHEN
                // clause and the body's DML statements already receive it. Before
                // this, the SELECT was executed with a plain `SelectExecutor::new`
                // and a body like `SELECT CASE WHEN NEW.id = 1 THEN raise(IGNORE)
                // END` failed at fire time with "Column reference requires FROM
                // clause".
                let executor =
                    crate::SelectExecutor::new_with_trigger_context(db, trigger_context);
                executor.execute_with_columns(select_stmt)?;
                Ok(())
            }
            _ => Err(ExecutorError::UnsupportedExpression(format!(
                "Statement type not supported in triggers: {:?}",
                statement
            ))),
        }
    }

    /// Whether firing `trigger` right now must be suppressed because
    /// `PRAGMA recursive_triggers` is off and the same trigger is already
    /// executing on this thread.
    ///
    /// SQLite's `recursive_triggers = off` does not blanket-disable nested
    /// trigger firing; it only prevents a trigger from being re-entered while it
    /// is already running. A *different* trigger reached via nested DML still
    /// fires, and a trigger fired at the top level (e.g. the DELETE trigger run
    /// for a REPLACE-induced delete) still fires because it is not yet on the
    /// stack. This per-trigger rule is what `trigger3-6` and `triggerC-5.3.*`
    /// depend on (#5535). When `recursive_triggers = on` (the default) this
    /// always returns false and the depth cap (#5479) governs recursion instead.
    fn suppressed_by_recursive_triggers_off(db: &Database, trigger: &TriggerDefinition) -> bool {
        !db.recursive_triggers() && is_trigger_active(&trigger.name)
    }

    /// Execute all BEFORE ROW-level triggers for an operation
    ///
    /// # Arguments
    /// * `db` - Mutable database reference
    /// * `table_name` - Name of the table
    /// * `event` - Trigger event (INSERT, UPDATE, DELETE)
    /// * `old_row` - OLD row (for UPDATE/DELETE)
    /// * `new_row` - NEW row (for INSERT/UPDATE)
    ///
    /// # Returns
    /// Ok(()) if all triggers executed successfully
    pub fn execute_before_triggers(
        db: &mut Database,
        table_name: &str,
        event: TriggerEvent,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
    ) -> Result<TriggerOutcome, ExecutorError> {
        Self::execute_before_triggers_in_schema(db, table_name, event, old_row, new_row, None)
    }

    /// Schema-aware variant of [`Self::execute_before_triggers`].
    ///
    /// `dml_schema` is the internal schema name the DML target resolved to; only
    /// triggers bound to that schema fire (triggerD-3.1/3.2). `None` preserves the
    /// legacy schema-unaware behavior.
    pub fn execute_before_triggers_in_schema(
        db: &mut Database,
        table_name: &str,
        event: TriggerEvent,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
        dml_schema: Option<&str>,
    ) -> Result<TriggerOutcome, ExecutorError> {
        // Check recursion depth before executing any triggers
        let _guard = RecursionGuard::new(db)?;

        let triggers =
            Self::find_triggers_in_schema(db, table_name, TriggerTiming::Before, event, dml_schema);

        // Get table schema for UPDATE OF checking
        let table_schema = db
            .catalog
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?
            .clone();

        for trigger in triggers {
            // Only execute ROW-level triggers in this method
            if trigger.granularity == TriggerGranularity::Row {
                // PRAGMA recursive_triggers = off: don't re-enter a running trigger.
                if Self::suppressed_by_recursive_triggers_off(db, &trigger) {
                    continue;
                }

                // For UPDATE OF triggers, check if monitored columns changed
                if let (Some(old), Some(new)) = (old_row, new_row) {
                    if !Self::should_fire_update_of(&trigger, old, new, &table_schema) {
                        continue; // Skip this trigger
                    }
                }

                // A RAISE(IGNORE) in any trigger abandons the current row.
                if Self::execute_trigger(db, &trigger, old_row, new_row)?
                    == TriggerOutcome::SkipRow
                {
                    return Ok(TriggerOutcome::SkipRow);
                }
            }
        }

        Ok(TriggerOutcome::Proceed)
    }

    /// Execute all BEFORE STATEMENT-level triggers for an operation
    ///
    /// # Arguments
    /// * `db` - Mutable database reference
    /// * `table_name` - Name of the table
    /// * `event` - Trigger event (INSERT, UPDATE, DELETE)
    ///
    /// # Returns
    /// Ok(()) if all triggers executed successfully
    pub fn execute_before_statement_triggers(
        db: &mut Database,
        table_name: &str,
        event: TriggerEvent,
    ) -> Result<TriggerOutcome, ExecutorError> {
        Self::execute_before_statement_triggers_in_schema(db, table_name, event, None)
    }

    /// Schema-aware variant of [`Self::execute_before_statement_triggers`].
    pub fn execute_before_statement_triggers_in_schema(
        db: &mut Database,
        table_name: &str,
        event: TriggerEvent,
        dml_schema: Option<&str>,
    ) -> Result<TriggerOutcome, ExecutorError> {
        // Check recursion depth before executing any triggers
        let _guard = RecursionGuard::new(db)?;

        let triggers =
            Self::find_triggers_in_schema(db, table_name, TriggerTiming::Before, event, dml_schema);

        for trigger in triggers {
            // Only execute STATEMENT-level triggers in this method
            if trigger.granularity == TriggerGranularity::Statement {
                // PRAGMA recursive_triggers = off: don't re-enter a running trigger.
                if Self::suppressed_by_recursive_triggers_off(db, &trigger) {
                    continue;
                }
                // Statement-level triggers don't have OLD/NEW row access
                if Self::execute_trigger(db, &trigger, None, None)? == TriggerOutcome::SkipRow {
                    return Ok(TriggerOutcome::SkipRow);
                }
            }
        }

        Ok(TriggerOutcome::Proceed)
    }

    /// Execute all AFTER ROW-level triggers for an operation
    ///
    /// # Arguments
    /// * `db` - Mutable database reference
    /// * `table_name` - Name of the table
    /// * `event` - Trigger event (INSERT, UPDATE, DELETE)
    /// * `old_row` - OLD row (for UPDATE/DELETE)
    /// * `new_row` - NEW row (for INSERT/UPDATE)
    ///
    /// # Returns
    /// Ok(()) if all triggers executed successfully
    pub fn execute_after_triggers(
        db: &mut Database,
        table_name: &str,
        event: TriggerEvent,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
    ) -> Result<TriggerOutcome, ExecutorError> {
        Self::execute_after_triggers_in_schema(db, table_name, event, old_row, new_row, None)
    }

    /// Schema-aware variant of [`Self::execute_after_triggers`].
    pub fn execute_after_triggers_in_schema(
        db: &mut Database,
        table_name: &str,
        event: TriggerEvent,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
        dml_schema: Option<&str>,
    ) -> Result<TriggerOutcome, ExecutorError> {
        // Check recursion depth before executing any triggers
        let _guard = RecursionGuard::new(db)?;

        let triggers =
            Self::find_triggers_in_schema(db, table_name, TriggerTiming::After, event, dml_schema);

        // Get table schema for UPDATE OF checking
        let table_schema = db
            .catalog
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?
            .clone();

        for trigger in triggers {
            // Only execute ROW-level triggers in this method
            if trigger.granularity == TriggerGranularity::Row {
                // PRAGMA recursive_triggers = off: don't re-enter a running trigger.
                if Self::suppressed_by_recursive_triggers_off(db, &trigger) {
                    continue;
                }

                // For UPDATE OF triggers, check if monitored columns changed
                if let (Some(old), Some(new)) = (old_row, new_row) {
                    if !Self::should_fire_update_of(&trigger, old, new, &table_schema) {
                        continue; // Skip this trigger
                    }
                }

                if Self::execute_trigger(db, &trigger, old_row, new_row)?
                    == TriggerOutcome::SkipRow
                {
                    return Ok(TriggerOutcome::SkipRow);
                }
            }
        }

        Ok(TriggerOutcome::Proceed)
    }

    /// Execute all AFTER STATEMENT-level triggers for an operation
    ///
    /// # Arguments
    /// * `db` - Mutable database reference
    /// * `table_name` - Name of the table
    /// * `event` - Trigger event (INSERT, UPDATE, DELETE)
    ///
    /// # Returns
    /// Ok(()) if all triggers executed successfully
    pub fn execute_after_statement_triggers(
        db: &mut Database,
        table_name: &str,
        event: TriggerEvent,
    ) -> Result<TriggerOutcome, ExecutorError> {
        Self::execute_after_statement_triggers_in_schema(db, table_name, event, None)
    }

    /// Schema-aware variant of [`Self::execute_after_statement_triggers`].
    pub fn execute_after_statement_triggers_in_schema(
        db: &mut Database,
        table_name: &str,
        event: TriggerEvent,
        dml_schema: Option<&str>,
    ) -> Result<TriggerOutcome, ExecutorError> {
        // Check recursion depth before executing any triggers
        let _guard = RecursionGuard::new(db)?;

        let triggers =
            Self::find_triggers_in_schema(db, table_name, TriggerTiming::After, event, dml_schema);

        for trigger in triggers {
            // Only execute STATEMENT-level triggers in this method
            if trigger.granularity == TriggerGranularity::Statement {
                // PRAGMA recursive_triggers = off: don't re-enter a running trigger.
                if Self::suppressed_by_recursive_triggers_off(db, &trigger) {
                    continue;
                }
                // Statement-level triggers don't have OLD/NEW row access
                if Self::execute_trigger(db, &trigger, None, None)? == TriggerOutcome::SkipRow {
                    return Ok(TriggerOutcome::SkipRow);
                }
            }
        }

        Ok(TriggerOutcome::Proceed)
    }
}
