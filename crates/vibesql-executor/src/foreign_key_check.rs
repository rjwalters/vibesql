//! Shared foreign-key validation helpers.
//!
//! This module centralizes two pieces of FK enforcement that must stay in sync
//! across multiple call sites (INSERT, UPDATE, PRAGMA `foreign_key_check`):
//!
//! 1. [`detect_fk_mismatch`] — Verify that the parent table actually has a PK, UNIQUE constraint,
//!    or non-partial UNIQUE INDEX that *exactly* covers the columns referenced by the FK. SQLite
//!    raises a `foreign key mismatch` error before any row-existence check when no such key exists.
//! 2. [`fk_values_equal`] — SQLite-style equality for FK comparisons that honours numeric coercion
//!    as well as the parent column's `NOCASE` / `RTRIM` collation. The non-collation logic mirrors
//!    VibeSQL's [`vibesql_types::SqlValue`] strict equality; the collation logic mirrors the
//!    helpers used in `select::grouping::aggregates`.
//!
//! See issue #5084 for context.

use vibesql_catalog::{ForeignKeyConstraint, TableSchema};
use vibesql_storage::{Database, DeferredFkViolation, DeferredFkViolationKind};

/// Returns `Some` (the child / parent table names to embed in the error) when
/// the parent table does **not** have a key that exactly covers the FK's
/// referenced columns.
///
/// SQLite's rule (see `fkey.c::sqlite3FkLocateIndex`): the parent column list
/// must match either the PRIMARY KEY or a UNIQUE constraint or a non-partial
/// UNIQUE INDEX, *as a set* (order-independent, exact coverage — supersets are
/// not acceptable).
pub fn detect_fk_mismatch(
    db: &Database,
    child_table: &str,
    fk: &ForeignKeyConstraint,
) -> Option<(String, String)> {
    let parent = db.catalog.get_table(&fk.parent_table)?;

    // Resolve parent column indices by name when names are present. The
    // cached `fk.parent_column_indices` can lag behind the parent schema
    // when the FK was registered before the parent table existed (e.g. on
    // SQL-dump reload, where alphabetic sort can place children first).
    let resolved_indices = resolve_parent_indices(parent, fk);

    if parent_has_matching_key(db, parent, &resolved_indices) {
        None
    } else {
        Some((child_table.to_string(), fk.parent_table.clone()))
    }
}

/// Full FK-definition validation for a single constraint: unlike
/// [`detect_fk_mismatch`] (which only reports the "wrong key shape" case and
/// silently returns `None` when the parent table itself is missing —
/// existing callers separately raise `TableNotFound` once a row-existence
/// scan actually needs the parent), this also catches the missing-parent-
/// table case so callers get a single schema-level check.
///
/// EVIDENCE-OF R-35763-48267 / R-03108-63659: a foreign key DML error is
/// reported when the parent table does not exist ("no such table") *or* when
/// the parent key columns are not backed by a PK/UNIQUE/non-partial UNIQUE
/// INDEX ("foreign key mismatch") — both are schema-level errors, detected
/// regardless of the actual row values.
pub(crate) fn check_fk_definition_error(
    db: &Database,
    child_table: &str,
    fk: &ForeignKeyConstraint,
) -> Option<crate::errors::ExecutorError> {
    if db.catalog.get_table(&fk.parent_table).is_none() {
        // A name that resolves to a VIEW (not a table) is a "foreign key
        // mismatch", not "no such table" — SQLite's `sqlite3FkLocateIndex`
        // only reports "no such table" when the name is entirely unknown to
        // the schema (fkey2-10.1.2: `REFERENCES v(y)` where `v` is a VIEW).
        // A view can never back an FK parent key (no PK/UNIQUE of its own).
        if db.catalog.get_view(&fk.parent_table).is_some() {
            return Some(crate::errors::ExecutorError::ForeignKeyMismatch {
                child: child_table.to_string(),
                parent: fk.parent_table.clone(),
            });
        }
        return Some(
            crate::errors::ExecutorError::TableNotFound(fk.parent_table.clone())
                .with_main_schema_qualifier(),
        );
    }
    detect_fk_mismatch(db, child_table, fk)
        .map(|(child, parent)| crate::errors::ExecutorError::ForeignKeyMismatch { child, parent })
}

/// Statement-prepare-time FK schema validation.
///
/// EVIDENCE-OF R-45488-08504 / R-48391-38472: when the database schema
/// contains an FK definition error that spans more than one table
/// definition (missing parent table, or a parent key not backed by a
/// PK/UNIQUE/non-partial UNIQUE INDEX), the error is not detected at
/// `CREATE TABLE` time — instead it surfaces the first time an application
/// *prepares* a DML statement (`INSERT`/`UPDATE`/`DELETE`) against either the
/// child or the parent table "in ways that use the foreign keys". Critically
/// this happens **before any row is touched**, so it must fire even when the
/// statement ultimately affects zero rows (e_fkey-20.*).
///
/// Row-driven validation elsewhere in this module (`check_fk_definition_error`
/// called per-row from the INSERT/UPDATE paths) already catches this for any
/// statement that processes at least one row. This entry point additionally
/// covers:
///   1. UPDATE/DELETE statements that end up touching zero rows (their per-row FK loops never run
///      at all).
///   2. DML against the *parent* side of a broken FK — SQLite reports the same error when preparing
///      a statement against the referenced table, not just the referencing one.
///   3. DML against a table several cascade-action hops away from the broken FK
///      (fkey2-20150416-100, Part of #6170): preparing DML against `t1` must also detect that `t0`
///      (which references `t1` via an `ON DELETE`/`ON UPDATE` action) has its own child, `t`, whose
///      FK definition is broken — SQLite recursively re-validates a cascade child's schema while
///      compiling the cascade action, so the error surfaces transitively, not just one hop away.
pub fn validate_fk_schema_for_dml(
    db: &Database,
    table_name: &str,
) -> Result<(), crate::errors::ExecutorError> {
    if !db.foreign_keys_enabled() {
        return Ok(());
    }

    if db.catalog.get_table(table_name).is_none() {
        return Ok(());
    }

    // Skip the O(tables) walk entirely when nothing in the schema declares
    // any FK at all (the overwhelmingly common case).
    let has_any_fks = db
        .catalog
        .list_tables()
        .iter()
        .any(|t| db.catalog.get_table(t).map(|s| !s.foreign_keys.is_empty()).unwrap_or(false));

    // 1. This table's own outgoing FKs.
    if let Some(schema) = db.catalog.get_table(table_name) {
        for fk in &schema.foreign_keys {
            if let Some(err) = check_fk_definition_error(db, table_name, fk) {
                return Err(err);
            }
        }
    }

    if !has_any_fks {
        return Ok(());
    }

    // 2. Transitive closure of tables that reference `table_name`, either directly or via a chain
    //    ("X references Y references table_name"). A `visited` set guards against infinite loops on
    //    FK reference cycles and avoids re-checking the same table twice in a diamond shape.
    //
    //    This is a two-pass discover-then-check walk rather than a single-pass "check edges as
    //    each node is first discovered" walk. A single-pass walk is sensitive to traversal order:
    //    `frontier` here is a plain `Vec` popped LIFO (not a true FIFO BFS), and when two ancestor
    //    chains of *unequal depth* converge on a node, LIFO order can discover that node via the
    //    shorter chain before the longer chain's ancestor has entered `visited` — so an edge to
    //    that not-yet-visited ancestor is silently skipped and never re-checked, since each node's
    //    outgoing FKs were previously examined only once, at first-discovery time (#6583, a
    //    follow-up to the same-round-diamond case #6570/#6581 already fixed below). Splitting into
    //    "discover the full closure" (pass 1, any traversal order is fine — only set membership
    //    matters) then "check every edge whose endpoints are both in the closure" (pass 2, run only
    //    after the closure is complete) sidesteps traversal-order sensitivity entirely, for both
    //    same-round diamonds and asymmetric-depth convergence.
    let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
    visited.insert(table_name.to_ascii_lowercase());
    let mut frontier = vec![table_name.to_string()];
    while let Some(current) = frontier.pop() {
        for other_name in db.catalog.list_tables() {
            let key = other_name.to_ascii_lowercase();
            if visited.contains(&key) {
                continue;
            }
            let Some(other_schema) = db.catalog.get_table(&other_name) else {
                continue;
            };
            let references_current = other_schema
                .foreign_keys
                .iter()
                .any(|fk| fk.parent_table.eq_ignore_ascii_case(&current));
            if !references_current {
                continue;
            }
            visited.insert(key);
            frontier.push(other_name.clone());
        }
    }

    // Pass 2: now that the full closure of tables reachable from `table_name` is known, check
    // every outgoing FK edge from a table in the closure whose parent is *also* in the closure.
    // This catches a broken edge at a convergence node regardless of which chain's discovery
    // order happened to reach that node first in pass 1.
    for other_name in db.catalog.list_tables() {
        if !visited.contains(&other_name.to_ascii_lowercase()) {
            continue;
        }
        let Some(other_schema) = db.catalog.get_table(&other_name) else {
            continue;
        };
        for fk in &other_schema.foreign_keys {
            if !visited.contains(&fk.parent_table.to_ascii_lowercase()) {
                continue;
            }
            if let Some(err) = check_fk_definition_error(db, &other_name, fk) {
                return Err(err);
            }
        }
    }

    Ok(())
}

/// Resolve the FK's parent-side column indices, preferring `parent_column_names`
/// over the cached `parent_column_indices` (which may carry placeholder zeros
/// when the parent did not yet exist at FK creation time).
fn resolve_parent_indices(parent: &TableSchema, fk: &ForeignKeyConstraint) -> Vec<usize> {
    // Names available and non-empty — resolve lazily.
    let names_usable =
        !fk.parent_column_names.is_empty() && fk.parent_column_names.iter().any(|n| !n.is_empty());
    if names_usable {
        let by_name: Vec<Option<usize>> = fk
            .parent_column_names
            .iter()
            .map(|n| if n.is_empty() { None } else { parent.get_column_index(n) })
            .collect();

        // Only fall back to indices when *all* names resolved cleanly. A
        // partial resolution would mix stale placeholders and real indices.
        if by_name.iter().all(|opt| opt.is_some()) {
            return by_name.into_iter().map(|opt| opt.unwrap()).collect();
        }

        // At least one explicitly-named parent column does not exist on the
        // parent table at all (fkey2-10.1.1: `REFERENCES p(c)` where `p` has
        // no column `c`). This can never be satisfied by any PK/UNIQUE/INDEX
        // on the parent, so report it as unresolvable (empty) rather than
        // falling through to the stale/placeholder `parent_column_indices` —
        // that cached index can coincidentally alias a real key and silently
        // mask a genuine "foreign key mismatch".
        return Vec::new();
    }

    // Empty parent_column_names with a single placeholder index typically
    // means "REFERENCES <table>" with no column list (defaults to PK).
    // Substitute the parent's actual PK indices when available.
    if fk.parent_column_names.iter().all(|n| n.is_empty()) {
        if let Some(pk_indices) = parent.get_primary_key_indices() {
            if pk_indices.len() == fk.parent_column_indices.len() {
                return pk_indices;
            }
        }
    }

    fk.parent_column_indices.clone()
}

/// True when the parent table exposes a PK / UNIQUE / non-partial UNIQUE INDEX
/// that exactly covers the supplied column set.
fn parent_has_matching_key(db: &Database, parent: &TableSchema, parent_indices: &[usize]) -> bool {
    if parent_indices.is_empty() {
        return false;
    }

    // 1. PRIMARY KEY exact match (set equality on column indices).
    if let Some(pk_indices) = parent.get_primary_key_indices() {
        if column_set_eq(&pk_indices, parent_indices) {
            return true;
        }
    }

    // 2. UNIQUE constraint exact match.
    for unique_idx_set in parent.get_unique_constraint_indices() {
        if column_set_eq(&unique_idx_set, parent_indices) {
            return true;
        }
    }

    // 3. Non-partial UNIQUE INDEX exact match. Partial indexes (those with a WHERE clause) are
    //    excluded — SQLite's `sqlite3FkLocateIndex` only accepts indexes that cover every parent
    //    row. Expression indexes can never back an FK either.
    //
    //    EVIDENCE-OF R-00376-39212: the UNIQUE index must use the collation
    //    sequences specified in the CREATE TABLE statement for the parent
    //    table — an index column with an *explicit* `COLLATE` clause that
    //    differs from the underlying column's own declared collation (its
    //    `CREATE TABLE`-time collation, defaulting to BINARY) cannot back an
    //    FK (e_fkey-18.5).
    for index in db.catalog.get_table_indexes(&parent.name) {
        if !index.is_unique || index.has_expression_columns() || index.is_partial() {
            continue;
        }
        let mut index_col_indices: Vec<usize> = Vec::with_capacity(index.columns.len());
        let mut all_resolved = true;
        let mut collation_mismatch = false;
        for col in &index.columns {
            match col.column_name().and_then(|n| parent.get_column_index(n)) {
                Some(idx) => {
                    if let Some(explicit) = col.explicit_collation() {
                        let declared = parent
                            .columns
                            .get(idx)
                            .and_then(|c| c.collation.as_deref())
                            .unwrap_or("BINARY");
                        if !explicit.eq_ignore_ascii_case(declared) {
                            collation_mismatch = true;
                            break;
                        }
                    }
                    index_col_indices.push(idx)
                }
                None => {
                    all_resolved = false;
                    break;
                }
            }
        }
        if !all_resolved || collation_mismatch {
            continue;
        }
        if column_set_eq(&index_col_indices, parent_indices) {
            return true;
        }
    }

    // 4. Fallback: a single-column FK against a column that is itself declared with column-level
    //    UNIQUE is represented in `unique_constraints`, so the match succeeds at step 2 above.
    false
}

/// True when `changed_columns` overlaps any column that could plausibly back
/// an FK parent key on `table_name`: the PRIMARY KEY, any UNIQUE constraint,
/// or any non-partial, non-expression UNIQUE INDEX.
///
/// Used to decide whether an UPDATE needs to run the (relatively expensive)
/// child-reference scan
/// ([`crate::update::foreign_keys::ForeignKeyValidator::check_no_child_references`]). Historically
/// that scan only fired when the update touched the table's PRIMARY KEY, silently skipping
/// cascade/RESTRICT/NO ACTION enforcement for any FK whose parent key is a UNIQUE constraint/index
/// instead (e_fkey-18.*, fkey2-genfkey.2/3 — `t3` references `t1(b, c)` where `t1`'s actual PK is
/// `a`). This helper is intentionally permissive (any candidate key, not
/// "the exact key some FK targets") — the per-FK old/new-value comparison
/// inside `check_no_child_references` does the precise work; this is only a
/// cheap up-front filter to avoid scanning every UPDATE unconditionally.
pub fn changed_columns_touch_any_key(
    db: &Database,
    table: &TableSchema,
    changed_columns: &[usize],
) -> bool {
    if changed_columns.is_empty() {
        return false;
    }
    if let Some(pk_indices) = table.get_primary_key_indices() {
        if pk_indices.iter().any(|i| changed_columns.contains(i)) {
            return true;
        }
    }
    for unique_idx_set in table.get_unique_constraint_indices() {
        if unique_idx_set.iter().any(|i| changed_columns.contains(i)) {
            return true;
        }
    }
    for index in db.catalog.get_table_indexes(&table.name) {
        if !index.is_unique || index.has_expression_columns() {
            continue;
        }
        let touches = index.columns.iter().any(|col| {
            col.column_name()
                .and_then(|n| table.get_column_index(n))
                .is_some_and(|idx| changed_columns.contains(&idx))
        });
        if touches {
            return true;
        }
    }
    false
}

/// Set-equality on column-index slices (order-independent, no duplicates).
fn column_set_eq(a: &[usize], b: &[usize]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_sorted: Vec<usize> = a.to_vec();
    let mut b_sorted: Vec<usize> = b.to_vec();
    a_sorted.sort_unstable();
    b_sorted.sort_unstable();
    a_sorted == b_sorted
}

/// SQLite-style equality for FK comparisons that respects the parent column's
/// collation (NOCASE / RTRIM) on top of strict typed equality and numeric
/// coercion.
pub fn fk_values_equal(
    child: &vibesql_types::SqlValue,
    parent: &vibesql_types::SqlValue,
    parent_collation: Option<&str>,
) -> bool {
    if child == parent {
        return true;
    }
    if let (Some(c), Some(p)) = (sql_value_as_f64(child), sql_value_as_f64(parent)) {
        if c == p {
            return true;
        }
    }
    if let (Some(c), Some(p)) = (sql_value_as_text(child), sql_value_as_text(parent)) {
        match parent_collation.map(|s| s.to_ascii_lowercase()) {
            Some(ref name) if name == "nocase" => return c.eq_ignore_ascii_case(p),
            Some(ref name) if name == "rtrim" => {
                return c.trim_end_matches(' ') == p.trim_end_matches(' ');
            }
            _ => {}
        }
    }
    false
}

/// SQLite-style equality for deciding whether a parent-key **value has
/// changed** across an UPDATE (EVIDENCE-OF: R-27383-10246 — "An ON UPDATE
/// action is only taken if the values of the parent key are modified so that
/// the new parent key values are not equal to the old. The default collation
/// sequence and affinity are used to determine if the new values are
/// 'distinct' from the old or not.").
///
/// Deliberately narrower than [`fk_values_equal`]: it applies the parent
/// column's own collation (NOCASE / RTRIM) to text/text comparisons, but does
/// **not** treat cross-storage-class values as numerically equal. Column
/// *affinity* already normalizes a written value to a single storage class
/// at write time (e.g. `a INTEGER` coerces `'1'` to `Integer(1)` before this
/// check ever runs); a column with **no** affinity (e.g. plain `b`, no type
/// name) performs no such coercion, so `Integer(1)` and `Text("1")` are
/// genuinely different values there and must be treated as *changed* even
/// though they are numerically equal (e_fkey-52.5: `b` has no affinity,
/// `UPDATE zeus SET b = '1'` on a stored `Integer(1)` must cascade). Using
/// [`fk_values_equal`]'s cross-type numeric branch here — which exists for
/// matching a *child* FK value against a *parent* key value, a different
/// comparison with its own leniency rules — would wrongly skip that cascade.
pub fn fk_key_value_changed(
    old: &vibesql_types::SqlValue,
    new: &vibesql_types::SqlValue,
    parent_collation: Option<&str>,
) -> bool {
    if old == new {
        return false;
    }
    if let (Some(o), Some(n)) = (sql_value_as_text(old), sql_value_as_text(new)) {
        match parent_collation.map(|s| s.to_ascii_lowercase()) {
            Some(ref name) if name == "nocase" => return !o.eq_ignore_ascii_case(n),
            Some(ref name) if name == "rtrim" => {
                return o.trim_end_matches(' ') != n.trim_end_matches(' ');
            }
            _ => {}
        }
    }
    true
}

fn sql_value_as_f64(v: &vibesql_types::SqlValue) -> Option<f64> {
    use vibesql_types::SqlValue::*;
    match v {
        Integer(i) => Some(*i as f64),
        Smallint(i) => Some(*i as f64),
        Bigint(i) => Some(*i as f64),
        Unsigned(i) => Some(*i as f64),
        Float(f) => Some(*f as f64),
        Real(r) => Some(*r as f64),
        Double(d) | Numeric(d) => Some(*d),
        Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        Character(s) | Varchar(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn sql_value_as_text(v: &vibesql_types::SqlValue) -> Option<&str> {
    use vibesql_types::SqlValue::*;
    match v {
        Character(s) | Varchar(s) => Some(s.as_str()),
        _ => None,
    }
}

/// Resolve the parent column collations for an FK (ordered to align with
/// the FK's parent-side columns). Uses [`resolve_parent_indices`] so that
/// stale placeholder indices left behind after a SQL-dump reload do not
/// cause us to read collation from the wrong column. Missing parent tables
/// or out-of-range indices yield `None`.
pub fn parent_collations_for_fk(db: &Database, fk: &ForeignKeyConstraint) -> Vec<Option<String>> {
    if let Some(parent) = db.catalog.get_table(&fk.parent_table) {
        let indices = resolve_parent_indices(parent, fk);
        indices
            .iter()
            .map(|&idx| parent.columns.get(idx).and_then(|c| c.collation.clone()))
            .collect()
    } else {
        vec![None; fk.parent_column_indices.len()]
    }
}

/// Public helper — re-exported so callers (INSERT/UPDATE/PRAGMA) can use the
/// same lazy parent-index resolution as [`detect_fk_mismatch`]. This keeps
/// row-existence comparisons aligned with mismatch detection so that they
/// agree on what "the parent FK columns" mean even after a SQL-dump reload
/// reordered children before parents.
pub fn resolved_parent_indices_for_fk(db: &Database, fk: &ForeignKeyConstraint) -> Vec<usize> {
    if let Some(parent) = db.catalog.get_table(&fk.parent_table) {
        resolve_parent_indices(parent, fk)
    } else {
        fk.parent_column_indices.clone()
    }
}

/// Outcome of a single-FK row-existence check on the INSERT path.
///
/// Encapsulates the bit-for-bit-identical steps 4-6 that
/// [`crate::insert::foreign_keys::validate_foreign_key_constraints`] and
/// [`crate::insert::row_validator::RowValidator::validate_foreign_keys`]
/// previously performed inline:
///
/// 4. Parent-table existence check (collation-aware, via [`fk_values_equal`]).
/// 5. Self-FK row-self check (Phase C3 of #5085 / fkey8-3.0).
/// 6. Defer-or-error decision (`in_txn && (initially_deferred || session_defer)`).
///
/// The caller is responsible for either pushing the deferred violation onto
/// the active transaction's queue (after the immutable `&Database` borrow
/// drops — see #5125 / PR #5141) or returning the immediate error.
#[derive(Debug)]
pub(crate) enum FkRowCheck {
    /// Parent key exists in the parent table, *or* the FK is self-referential
    /// and the inserted row itself satisfies its own FK (fkey8-3.0). The
    /// caller should proceed without queueing or erroring.
    Ok,
    /// Missing parent row, but the constraint is `INITIALLY DEFERRED` *or*
    /// the session has `PRAGMA defer_foreign_keys=ON`, *and* a transaction
    /// is active. The caller must queue this violation onto the
    /// transaction's deferred-FK queue (after the immutable borrow drops).
    Deferred(DeferredFkViolation),
    /// Missing parent row and not deferrable in the current context. The
    /// caller must return [`ExecutorError::ConstraintViolation`] with the
    /// FK name (empty string when unnamed), child column list, and parent
    /// table name. The helper does not construct the error itself so the
    /// caller-side formatting stays in one place.
    Violation,
}

/// Per-FK row-existence check that encapsulates steps 4-6 of the INSERT
/// FK validation pipeline. Both
/// [`crate::insert::foreign_keys::validate_foreign_key_constraints`] and
/// [`crate::insert::row_validator::RowValidator::validate_foreign_keys`]
/// call this; the caller-side wrapper handles the PRAGMA gate (step 1),
/// mismatch check (step 2), NULL-skip (step 3), and the post-loop queue
/// push (step 7).
///
/// # Preconditions
///
/// * `fk_values` must be non-empty and contain no NULLs — the caller is responsible for the
///   NULL-skip. (NULL FK values pass FK enforcement per SQL / SQLite.)
/// * The PRAGMA `foreign_keys` gate, schema-mismatch check, and parent-table existence must already
///   be verified by the caller. This helper does **not** call [`detect_fk_mismatch`] and does
///   **not** look up the parent table — the caller passes those results in.
///
/// # Self-referential multi-row INSERT (fkey1-5.1)
///
/// `batch_full_rows` carries the previously-validated rows from the same
/// multi-row VALUES list. When the FK is self-referential (`fk.parent_table`
/// equals `table_name`), the self-FK row-self check (step 5) also searches
/// the batch so row N can resolve its FK against rows 0..N-1 — matching
/// SQLite's insert-in-declaration-order semantics for
/// `INSERT INTO t11 VALUES (1,NULL),(2,1),(3,2)` where `t11.parent`
/// references `t11.x`. Callers that do not stage a batch (e.g. the
/// bulk-transfer path in `validate_foreign_key_constraints`) pass an
/// empty slice and get the original single-row behaviour.
///
/// # Borrow-checker pattern
///
/// Takes `&Database` (not `&mut`), so `RowValidator` (immutable borrow)
/// keeps working unchanged. When the outcome is [`FkRowCheck::Deferred`],
/// the caller appends the violation to a local `Vec<DeferredFkViolation>`
/// accumulator and pushes onto the database's queue *after* the immutable
/// borrow drops. This preserves the pattern introduced in #5125 / PR #5141.
pub(crate) fn check_fk_row_existence(
    db: &Database,
    table_name: &str,
    fk: &ForeignKeyConstraint,
    fk_idx: usize,
    fk_values: &[vibesql_types::SqlValue],
    full_row_values: &[vibesql_types::SqlValue],
    batch_full_rows: &[Vec<vibesql_types::SqlValue>],
) -> Result<FkRowCheck, crate::errors::ExecutorError> {
    // Parent table is required for the existence scan. Caller has already
    // confirmed schema mismatch is OK (and a mismatch error path would
    // have returned before reaching this helper).
    let parent_table = db.get_table(&fk.parent_table).ok_or_else(|| {
        crate::errors::ExecutorError::TableNotFound(fk.parent_table.clone())
            .with_main_schema_qualifier()
    })?;

    let parent_collations = parent_collations_for_fk(db, fk);
    let parent_indices = resolved_parent_indices_for_fk(db, fk);

    // Step 4: parent-table existence scan.
    //
    // Phase 1d follow-up (#5205): under MVCC the parent-existence check
    // must respect the active snapshot — an INSERT on the child must not
    // "see" a parent row inserted by an uncommitted concurrent
    // transaction (it would otherwise let the child slip past FK
    // enforcement only to be orphaned at the other side's rollback).
    // Off-state (`mvcc_enabled` OFF): the previous code scanned every
    // physical row via `scan()` (including bitmap-deleted ones); we
    // preserve that exactly with `#[cfg]`-gated branches.
    let snapshot = crate::mvcc::read_snapshot(db);
    let key_exists = {
        #[cfg(feature = "mvcc_enabled")]
        {
            parent_table.scan_visible(&snapshot).any(|(_, parent_row)| {
                parent_indices.iter().zip(fk_values).enumerate().all(
                    |(i, (&parent_idx, fk_val))| match parent_row.get(parent_idx) {
                        Some(parent_val) => fk_values_equal(
                            fk_val,
                            parent_val,
                            parent_collations.get(i).and_then(|c| c.as_deref()),
                        ),
                        None => false,
                    },
                )
            })
        }
        #[cfg(not(feature = "mvcc_enabled"))]
        {
            let _ = &snapshot;
            parent_table.scan().iter().any(|parent_row| {
                parent_indices.iter().zip(fk_values).enumerate().all(
                    |(i, (&parent_idx, fk_val))| match parent_row.get(parent_idx) {
                        Some(parent_val) => fk_values_equal(
                            fk_val,
                            parent_val,
                            parent_collations.get(i).and_then(|c| c.as_deref()),
                        ),
                        None => false,
                    },
                )
            })
        }
    };
    if key_exists {
        return Ok(FkRowCheck::Ok);
    }

    // Step 5: self-FK row-self check (Phase C3 of #5085 / fkey8-3.0) +
    // multi-row sibling check (fkey1-5.1). When the FK points back at the
    // table being inserted into, two extra rescue paths apply:
    //   (a) The row itself can satisfy the constraint — SQLite checks the
    //       parent index *after* the row is inserted.
    //   (b) For multi-row INSERTs, an earlier row in the same VALUES list
    //       may have been the intended parent; SQLite inserts rows in
    //       declaration order and a later row's FK target can resolve to
    //       a sibling already inserted in the same statement.
    // Both checks use full-row values (not the partial FK extract) so the
    // candidate row participates as a whole row in its own FK check.
    if fk.parent_table.eq_ignore_ascii_case(table_name) {
        let row_matches = |candidate: &[vibesql_types::SqlValue]| -> bool {
            parent_indices.iter().zip(fk_values).enumerate().all(|(i, (&parent_idx, fk_val))| {
                match candidate.get(parent_idx) {
                    Some(parent_val) => fk_values_equal(
                        fk_val,
                        parent_val,
                        parent_collations.get(i).and_then(|c| c.as_deref()),
                    ),
                    None => false,
                }
            })
        };
        if row_matches(full_row_values) || batch_full_rows.iter().any(|r| row_matches(r)) {
            return Ok(FkRowCheck::Ok);
        }
    }

    // Step 6: defer-or-error decision. Outside a transaction, deferred
    // constraints still error immediately (matches SQLite — deferred
    // enforcement requires a transaction context). Mismatch errors are
    // never deferred, but the caller already filtered those out before
    // reaching this helper.
    let session_defer = db.defer_foreign_keys();
    let in_txn = db.in_transaction();
    // NOT DEFERRABLE constraints are always checked immediately regardless
    // of any (meaningless-but-parseable) INITIALLY DEFERRED clause — SQLite
    // grammar allows "NOT DEFERRABLE INITIALLY DEFERRED" but the INITIALLY
    // clause only takes effect when the constraint is actually DEFERRABLE
    // (e_fkey-34.*: only a `DEFERRABLE INITIALLY DEFERRED` constraint may be
    // violated mid-transaction; all six other DEFERRABLE/NOT DEFERRABLE x
    // INITIALLY DEFERRED/IMMEDIATE combinations must error immediately).
    // `PRAGMA defer_foreign_keys=ON` is a blanket per-transaction override
    // that defers *every* constraint regardless of its own DEFERRABLE
    // status (EVIDENCE-OF R-18981-16292, fkey6-1.8), so `session_defer` is
    // intentionally NOT gated on `fk.is_deferrable`.
    let should_defer = in_txn && (session_defer || (fk.is_deferrable && fk.initially_deferred));
    if should_defer {
        return Ok(FkRowCheck::Deferred(DeferredFkViolation {
            child_table: table_name.to_string(),
            fk_index: fk_idx,
            child_row: full_row_values.to_vec(),
            kind: DeferredFkViolationKind::ChildInsertOrUpdate,
        }));
    }

    Ok(FkRowCheck::Violation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_set_eq_same_order() {
        assert!(column_set_eq(&[1, 2, 3], &[1, 2, 3]));
    }

    #[test]
    fn column_set_eq_reordered() {
        assert!(column_set_eq(&[3, 1, 2], &[1, 2, 3]));
    }

    #[test]
    fn column_set_eq_different_lengths() {
        assert!(!column_set_eq(&[1, 2], &[1, 2, 3]));
    }

    #[test]
    fn column_set_eq_disjoint() {
        assert!(!column_set_eq(&[1, 2], &[3, 4]));
    }

    #[test]
    fn fk_values_equal_strict() {
        let v = vibesql_types::SqlValue::Integer(42);
        assert!(fk_values_equal(&v, &v, None));
    }

    #[test]
    fn fk_values_equal_numeric_coercion() {
        let child = vibesql_types::SqlValue::Integer(88);
        let parent = vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("88"));
        assert!(fk_values_equal(&child, &parent, None));
    }

    #[test]
    fn fk_values_equal_nocase() {
        let child = vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Alpha"));
        let parent = vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("ALPHA"));
        assert!(fk_values_equal(&child, &parent, Some("NOCASE")));
        assert!(!fk_values_equal(&child, &parent, None));
    }

    #[test]
    fn fk_values_equal_rtrim() {
        let child = vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("abc"));
        let parent = vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("abc   "));
        assert!(fk_values_equal(&child, &parent, Some("rtrim")));
        assert!(!fk_values_equal(&child, &parent, None));
    }

    #[test]
    fn fk_values_equal_distinct() {
        let child = vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("foo"));
        let parent = vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("bar"));
        assert!(!fk_values_equal(&child, &parent, None));
        assert!(!fk_values_equal(&child, &parent, Some("nocase")));
    }

    // -----------------------------------------------------------------
    // check_fk_row_existence tests — one per FkRowCheck variant.
    // -----------------------------------------------------------------

    use vibesql_catalog::{ColumnSchema, ReferentialAction};
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

    fn child_fk() -> ForeignKeyConstraint {
        ForeignKeyConstraint {
            name: Some("fk_c_pid".to_string()),
            column_names: vec!["pid".to_string()],
            column_indices: vec![1],
            parent_table: "p".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        }
    }

    fn setup_parent_child(parent_rows: &[i64]) -> (Database, ForeignKeyConstraint) {
        let mut db = Database::new();
        db.set_foreign_keys_enabled(true);

        // Parent: p(id INTEGER PRIMARY KEY)
        let p = TableSchema::with_primary_key(
            "p".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
            vec!["id".to_string()],
        );
        db.create_table(p).unwrap();

        // Child: c(id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p(id))
        let child_fk = child_fk();
        let child_columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("pid".to_string(), DataType::Integer, true),
        ];
        let mut c =
            TableSchema::with_primary_key("c".to_string(), child_columns, vec!["id".to_string()]);
        c.foreign_keys.push(child_fk.clone());
        db.create_table(c).unwrap();

        for id in parent_rows {
            db.insert_row("p", Row::new(vec![SqlValue::Integer(*id)])).unwrap();
        }

        (db, child_fk)
    }

    #[test]
    fn check_fk_row_existence_ok_parent_exists() {
        let (db, fk) = setup_parent_child(&[1, 2, 3]);
        let full_row = vec![SqlValue::Integer(10), SqlValue::Integer(2)];
        let fk_values = vec![SqlValue::Integer(2)];

        let outcome = check_fk_row_existence(&db, "c", &fk, 0, &fk_values, &full_row, &[]).unwrap();
        assert!(matches!(outcome, FkRowCheck::Ok), "expected Ok, got {:?}", outcome);
    }

    #[test]
    fn check_fk_row_existence_ok_self_row_self_check() {
        // Self-referential FK: t(id INTEGER PRIMARY KEY, parent INTEGER REFERENCES t(id)).
        // INSERT (5, 5) — the row satisfies its own FK (fkey8-3.0 pattern).
        let mut db = Database::new();
        db.set_foreign_keys_enabled(true);

        let fk = ForeignKeyConstraint {
            name: Some("fk_t_self".to_string()),
            column_names: vec!["parent".to_string()],
            column_indices: vec![1],
            parent_table: "t".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };
        let cols = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("parent".to_string(), DataType::Integer, true),
        ];
        let mut t = TableSchema::with_primary_key("t".to_string(), cols, vec!["id".to_string()]);
        t.foreign_keys.push(fk.clone());
        db.create_table(t).unwrap();
        // Parent table is empty — only the row-self check should rescue us.

        let full_row = vec![SqlValue::Integer(5), SqlValue::Integer(5)];
        let fk_values = vec![SqlValue::Integer(5)];

        let outcome = check_fk_row_existence(&db, "t", &fk, 0, &fk_values, &full_row, &[]).unwrap();
        assert!(
            matches!(outcome, FkRowCheck::Ok),
            "self-FK row-self check must accept, got {:?}",
            outcome
        );
    }

    #[test]
    fn check_fk_row_existence_ok_self_fk_sibling_in_batch() {
        // fkey1-5.1: multi-row INSERT into a self-referential parent.
        // t(x INTEGER PRIMARY KEY, parent INTEGER REFERENCES t(x)).
        // INSERT VALUES (1, NULL), (2, 1), (3, 2).
        // Row (2, 1) is validated with batch_full_rows = [(1, NULL)] in scope;
        // the parent table is still empty, the self-row doesn't match
        // (parent_idx=0 holds 2, fk_val=1), so the sibling rescue must catch it.
        let mut db = Database::new();
        db.set_foreign_keys_enabled(true);

        let fk = ForeignKeyConstraint {
            name: Some("fk_t_self".to_string()),
            column_names: vec!["parent".to_string()],
            column_indices: vec![1],
            parent_table: "t".to_string(),
            parent_column_names: vec!["x".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };
        let cols = vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, false),
            ColumnSchema::new("parent".to_string(), DataType::Integer, true),
        ];
        let mut t = TableSchema::with_primary_key("t".to_string(), cols, vec!["x".to_string()]);
        t.foreign_keys.push(fk.clone());
        db.create_table(t).unwrap();

        let earlier_row = vec![SqlValue::Integer(1), SqlValue::Null];
        let batch = vec![earlier_row];

        let full_row = vec![SqlValue::Integer(2), SqlValue::Integer(1)];
        let fk_values = vec![SqlValue::Integer(1)];

        let outcome =
            check_fk_row_existence(&db, "t", &fk, 0, &fk_values, &full_row, &batch).unwrap();
        assert!(
            matches!(outcome, FkRowCheck::Ok),
            "multi-row self-FK sibling rescue must accept, got {:?}",
            outcome
        );

        // Empty batch (single-row path) must still fail for the same row.
        let outcome_no_batch =
            check_fk_row_existence(&db, "t", &fk, 0, &fk_values, &full_row, &[]).unwrap();
        assert!(
            matches!(outcome_no_batch, FkRowCheck::Violation),
            "without batch_full_rows the same row must violate, got {:?}",
            outcome_no_batch
        );
    }

    #[test]
    fn check_fk_row_existence_violation_immediate() {
        // Missing parent row, no transaction, no deferral — immediate violation.
        let (db, fk) = setup_parent_child(&[1, 2, 3]);
        let full_row = vec![SqlValue::Integer(10), SqlValue::Integer(999)];
        let fk_values = vec![SqlValue::Integer(999)];

        let outcome = check_fk_row_existence(&db, "c", &fk, 0, &fk_values, &full_row, &[]).unwrap();
        assert!(matches!(outcome, FkRowCheck::Violation), "expected Violation, got {:?}", outcome);
    }

    #[test]
    fn check_fk_row_existence_violation_when_deferred_outside_transaction() {
        // INITIALLY DEFERRED but no transaction — should still error immediately
        // (matches SQLite: deferred enforcement requires a transaction context).
        // `check_fk_row_existence` reads the `fk` parameter directly (it does
        // not re-fetch from the catalog), so we only need to flip the local
        // copy of the constraint here.
        let (db, mut fk) = setup_parent_child(&[1]);
        fk.is_deferrable = true;
        fk.initially_deferred = true;

        let full_row = vec![SqlValue::Integer(10), SqlValue::Integer(999)];
        let fk_values = vec![SqlValue::Integer(999)];

        assert!(!db.in_transaction(), "test precondition: no transaction");
        let outcome = check_fk_row_existence(&db, "c", &fk, 0, &fk_values, &full_row, &[]).unwrap();
        assert!(
            matches!(outcome, FkRowCheck::Violation),
            "deferred-but-outside-txn must error immediately, got {:?}",
            outcome
        );
    }

    #[test]
    fn check_fk_row_existence_deferred_initially_deferred_in_txn() {
        // INITIALLY DEFERRED + transaction active — caller should queue.
        let (mut db, mut fk) = setup_parent_child(&[1]);
        fk.is_deferrable = true;
        fk.initially_deferred = true;

        // Open a transaction so in_transaction() == true.
        db.begin_transaction().unwrap();
        assert!(db.in_transaction());

        let full_row = vec![SqlValue::Integer(10), SqlValue::Integer(999)];
        let fk_values = vec![SqlValue::Integer(999)];

        let outcome = check_fk_row_existence(&db, "c", &fk, 0, &fk_values, &full_row, &[]).unwrap();
        match outcome {
            FkRowCheck::Deferred(v) => {
                assert_eq!(v.child_table, "c");
                assert_eq!(v.fk_index, 0);
                assert_eq!(v.child_row, full_row);
                assert_eq!(v.kind, DeferredFkViolationKind::ChildInsertOrUpdate);
            }
            other => panic!("expected Deferred, got {:?}", other),
        }
    }

    #[test]
    fn check_fk_row_existence_not_deferrable_initially_deferred_in_txn_is_immediate() {
        // NOT DEFERRABLE INITIALLY DEFERRED (SQLite grammar allows this
        // combination, but NOT DEFERRABLE always wins: the INITIALLY
        // DEFERRED clause is a no-op unless the constraint is actually
        // DEFERRABLE). Even inside an open transaction with no session
        // `defer_foreign_keys` override, this must violate immediately —
        // not queue as deferred (e_fkey-34.*, fkey2#6170).
        let (mut db, mut fk) = setup_parent_child(&[1]);
        fk.is_deferrable = false;
        fk.initially_deferred = true;

        db.begin_transaction().unwrap();
        assert!(db.in_transaction());

        let full_row = vec![SqlValue::Integer(10), SqlValue::Integer(999)];
        let fk_values = vec![SqlValue::Integer(999)];

        let outcome = check_fk_row_existence(&db, "c", &fk, 0, &fk_values, &full_row, &[]).unwrap();
        assert!(
            matches!(outcome, FkRowCheck::Violation),
            "NOT DEFERRABLE INITIALLY DEFERRED must violate immediately even in a transaction, got {:?}",
            outcome
        );
    }

    #[test]
    fn check_fk_row_existence_deferred_via_session_pragma() {
        // Constraint is NOT deferrable, but the session pragma
        // defer_foreign_keys=ON overrides per-constraint defaults
        // (and we're inside a transaction).
        let (mut db, fk) = setup_parent_child(&[1]);
        db.set_defer_foreign_keys(true);

        db.begin_transaction().unwrap();
        assert!(db.in_transaction());

        let full_row = vec![SqlValue::Integer(10), SqlValue::Integer(999)];
        let fk_values = vec![SqlValue::Integer(999)];

        let outcome = check_fk_row_existence(&db, "c", &fk, 0, &fk_values, &full_row, &[]).unwrap();
        assert!(
            matches!(outcome, FkRowCheck::Deferred(_)),
            "session pragma must defer, got {:?}",
            outcome
        );
    }

    // -----------------------------------------------------------------
    // validate_fk_schema_for_dml — diamond-topology BFS coverage (#6570)
    // -----------------------------------------------------------------

    #[test]
    fn validate_fk_schema_for_dml_diamond_detects_non_discovering_broken_edge() {
        // Diamond topology (issue #6570, follow-up to #6568):
        //
        //   t1 <- X  (X.t1_id REFERENCES t1(id), valid)
        //   t1 <- Y  (Y.t1_id REFERENCES t1(id), valid)
        //   X  <- W  (W.x_junk REFERENCES X(junk), BROKEN: `junk` is not backed
        //             by any PK/UNIQUE/non-partial UNIQUE INDEX on X)
        //   Y  <- W  (W.y_id REFERENCES Y(id), valid)
        //
        // W is discovered via its valid edge to Y. Before the #6570 fix, the
        // BFS only checked the single edge that discovered a node, so W's
        // broken edge to X was silently skipped whenever X had already been
        // marked `visited` by the time W's FKs were (partially) checked.
        // After the fix, all of W's outgoing FKs whose parent is already in
        // the closure are checked at discovery time, so the broken edge must
        // surface as a `ForeignKeyMismatch { child: "w", parent: "x" }`.
        let mut db = Database::new();
        db.set_foreign_keys_enabled(true);

        // 1. t1 — DML target, no FKs of its own.
        let t1 = TableSchema::with_primary_key(
            "t1".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
            vec!["id".to_string()],
        );
        db.create_table(t1).unwrap();

        // 2. X — valid FK to t1, plus a plain (non-unique, non-PK) `junk` column.
        let x_t1_fk = ForeignKeyConstraint {
            name: Some("fk_x_t1".to_string()),
            column_names: vec!["t1_id".to_string()],
            column_indices: vec![1],
            parent_table: "t1".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };
        let x_columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("t1_id".to_string(), DataType::Integer, true),
            ColumnSchema::new("junk".to_string(), DataType::Integer, true),
        ];
        let mut x =
            TableSchema::with_primary_key("x".to_string(), x_columns, vec!["id".to_string()]);
        x.foreign_keys.push(x_t1_fk);
        db.create_table(x).unwrap();

        // 3. Y — valid FK to t1.
        let y_t1_fk = ForeignKeyConstraint {
            name: Some("fk_y_t1".to_string()),
            column_names: vec!["t1_id".to_string()],
            column_indices: vec![1],
            parent_table: "t1".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };
        let y_columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("t1_id".to_string(), DataType::Integer, true),
        ];
        let mut y =
            TableSchema::with_primary_key("y".to_string(), y_columns, vec!["id".to_string()]);
        y.foreign_keys.push(y_t1_fk);
        db.create_table(y).unwrap();

        // 4. W — broken FK to X(junk) (no PK/UNIQUE/non-partial UNIQUE INDEX backs `junk`), plus a
        //    valid FK to Y(id).
        let w_x_fk = ForeignKeyConstraint {
            name: Some("fk_w_x".to_string()),
            column_names: vec!["x_junk".to_string()],
            column_indices: vec![1],
            parent_table: "x".to_string(),
            parent_column_names: vec!["junk".to_string()],
            parent_column_indices: vec![2],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };
        let w_y_fk = ForeignKeyConstraint {
            name: Some("fk_w_y".to_string()),
            column_names: vec!["y_id".to_string()],
            column_indices: vec![2],
            parent_table: "y".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };
        let w_columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("x_junk".to_string(), DataType::Integer, true),
            ColumnSchema::new("y_id".to_string(), DataType::Integer, true),
        ];
        let mut w =
            TableSchema::with_primary_key("w".to_string(), w_columns, vec!["id".to_string()]);
        w.foreign_keys.push(w_x_fk);
        w.foreign_keys.push(w_y_fk);
        db.create_table(w).unwrap();

        let result = validate_fk_schema_for_dml(&db, "t1");
        match result {
            Err(crate::errors::ExecutorError::ForeignKeyMismatch { child, parent }) => {
                assert_eq!(child, "w");
                assert_eq!(parent, "x");
            }
            other => panic!(
                "expected Err(ForeignKeyMismatch {{ child: \"w\", parent: \"x\" }}), got {:?}",
                other
            ),
        }
    }

    #[test]
    fn validate_fk_schema_for_dml_cycle_does_not_infinite_loop() {
        // FK reference cycle: a REFERENCES b, b REFERENCES a. The node-level
        // `visited` set must still bound frontier growth even though the
        // per-edge check (post-#6570 fix) now looks at all of a node's FKs
        // whose parent is in `visited`, not just the discovering edge.
        //
        // `Catalog::create_table` rejects a genuine multi-table FK cycle at
        // creation time (`check_circular_foreign_keys`), so a cyclic pair
        // cannot be built via two ordinary `db.create_table` calls — the
        // second call always errors out before the cycle ever reaches the
        // catalog. To exercise the BFS's own cycle guard, this test builds
        // "a" and "b" acyclically first, then uses the catalog's existing
        // `replace_table_schema` (the same mechanism ALTER TABLE column
        // operations use to push a mutated schema back into the catalog
        // without re-running the create-time cycle check) to retroactively
        // complete the cycle on "a".
        let mut db = Database::new();
        db.set_foreign_keys_enabled(true);

        let b_a_fk = ForeignKeyConstraint {
            name: Some("fk_b_a".to_string()),
            column_names: vec!["aid".to_string()],
            column_indices: vec![1],
            parent_table: "a".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };

        // 1. "a" — no FKs yet, just the PK "b" will reference.
        let a_columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("bid".to_string(), DataType::Integer, true),
        ];
        let a = TableSchema::with_primary_key(
            "a".to_string(),
            a_columns.clone(),
            vec!["id".to_string()],
        );
        db.create_table(a).unwrap();

        // 2. "b" — valid FK to "a". Not cyclic yet (a has no FKs at all).
        let b_columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("aid".to_string(), DataType::Integer, true),
        ];
        let mut b =
            TableSchema::with_primary_key("b".to_string(), b_columns, vec!["id".to_string()]);
        b.foreign_keys.push(b_a_fk);
        db.create_table(b).unwrap();

        // 3. Retroactively complete the cycle: swap in a new version of "a" with a valid FK to "b",
        //    bypassing the create-time cycle guard.
        let a_b_fk = ForeignKeyConstraint {
            name: Some("fk_a_b".to_string()),
            column_names: vec!["bid".to_string()],
            column_indices: vec![1],
            parent_table: "b".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };
        let mut a_with_cycle =
            TableSchema::with_primary_key("a".to_string(), a_columns, vec!["id".to_string()]);
        a_with_cycle.foreign_keys.push(a_b_fk);
        db.catalog.replace_table_schema("a", a_with_cycle);

        // Both FKs are well-formed (each parent has a PK covering the
        // referenced column), so this must terminate and return Ok — the
        // test's real assertion is simply that it returns at all (i.e. the
        // BFS's `visited` set bounds frontier growth on a genuine cycle).
        let result = validate_fk_schema_for_dml(&db, "a");
        assert!(result.is_ok(), "cyclic FK graph must not error: {:?}", result);
    }

    // -----------------------------------------------------------------
    // validate_fk_schema_for_dml — asymmetric-depth convergence (#6583,
    // follow-up to #6570/#6581)
    // -----------------------------------------------------------------

    /// Builds the shared four-table-per-branch topology from #6583's repro:
    ///
    /// ```text
    /// t1 <- a1 <- a2 <- a
    /// t1 <- c1 <- c2 <- c
    /// a  <- w   (w.a_junk -> a(junk))
    /// c  <- w   (w.c_id   -> c(id))
    /// ```
    ///
    /// `a` and `c` each gain a plain (non-unique, non-PK) `junk` column so
    /// either side can be made the "broken" FK target by the caller. Tables
    /// are created in the issue's exact order (`t1, a1, c1, a2, c2, a, c,
    /// w`) so the LIFO `frontier` in `validate_fk_schema_for_dml` explores
    /// the `c`-branch to completion (discovering `w` via its edge to `c`)
    /// while the `a`-branch is still unvisited at the bottom of the stack —
    /// the exact interleaving that made the single-pass BFS order-sensitive
    /// before the #6583 fix.
    fn setup_asymmetric_convergence(a_broken: bool) -> Database {
        let mut db = Database::new();
        db.set_foreign_keys_enabled(true);

        let t1 = TableSchema::with_primary_key(
            "t1".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
            vec!["id".to_string()],
        );
        db.create_table(t1).unwrap();

        fn chain_link_fk(parent_table: &str) -> ForeignKeyConstraint {
            ForeignKeyConstraint {
                name: Some(format!("fk_to_{parent_table}")),
                column_names: vec!["pid".to_string()],
                column_indices: vec![1],
                parent_table: parent_table.to_string(),
                parent_column_names: vec!["id".to_string()],
                parent_column_indices: vec![0],
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::NoAction,
                is_deferrable: false,
                initially_deferred: false,
            }
        }

        fn chain_link_columns() -> Vec<ColumnSchema> {
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("pid".to_string(), DataType::Integer, true),
            ]
        }

        // a1 -> t1, c1 -> t1 (creation order interleaved per the repro).
        let mut a1 = TableSchema::with_primary_key(
            "a1".to_string(),
            chain_link_columns(),
            vec!["id".to_string()],
        );
        a1.foreign_keys.push(chain_link_fk("t1"));
        db.create_table(a1).unwrap();

        let mut c1 = TableSchema::with_primary_key(
            "c1".to_string(),
            chain_link_columns(),
            vec!["id".to_string()],
        );
        c1.foreign_keys.push(chain_link_fk("t1"));
        db.create_table(c1).unwrap();

        // a2 -> a1, c2 -> c1.
        let mut a2 = TableSchema::with_primary_key(
            "a2".to_string(),
            chain_link_columns(),
            vec!["id".to_string()],
        );
        a2.foreign_keys.push(chain_link_fk("a1"));
        db.create_table(a2).unwrap();

        let mut c2 = TableSchema::with_primary_key(
            "c2".to_string(),
            chain_link_columns(),
            vec!["id".to_string()],
        );
        c2.foreign_keys.push(chain_link_fk("c1"));
        db.create_table(c2).unwrap();

        // a -> a2 (leaf, has an extra plain `junk` column). c -> c2 (same shape).
        let leaf_columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("pid".to_string(), DataType::Integer, true),
            ColumnSchema::new("junk".to_string(), DataType::Integer, true),
        ];
        let mut a = TableSchema::with_primary_key(
            "a".to_string(),
            leaf_columns.clone(),
            vec!["id".to_string()],
        );
        a.foreign_keys.push(chain_link_fk("a2"));
        db.create_table(a).unwrap();

        let mut c =
            TableSchema::with_primary_key("c".to_string(), leaf_columns, vec!["id".to_string()]);
        c.foreign_keys.push(chain_link_fk("c2"));
        db.create_table(c).unwrap();

        // w -> a(junk) and w -> c(id) — exactly one of the two is "broken"
        // (points at a column with no PK/UNIQUE/non-partial UNIQUE INDEX)
        // depending on `a_broken`.
        let w_to_a = ForeignKeyConstraint {
            name: Some("fk_w_a".to_string()),
            column_names: vec!["a_ref".to_string()],
            column_indices: vec![1],
            parent_table: "a".to_string(),
            parent_column_names: vec![if a_broken { "junk" } else { "id" }.to_string()],
            parent_column_indices: vec![if a_broken { 2 } else { 0 }],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };
        let w_to_c = ForeignKeyConstraint {
            name: Some("fk_w_c".to_string()),
            column_names: vec!["c_ref".to_string()],
            column_indices: vec![2],
            parent_table: "c".to_string(),
            parent_column_names: vec![if a_broken { "id" } else { "junk" }.to_string()],
            parent_column_indices: vec![if a_broken { 0 } else { 2 }],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };
        let w_columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("a_ref".to_string(), DataType::Integer, true),
            ColumnSchema::new("c_ref".to_string(), DataType::Integer, true),
        ];
        let mut w =
            TableSchema::with_primary_key("w".to_string(), w_columns, vec!["id".to_string()]);
        w.foreign_keys.push(w_to_a);
        w.foreign_keys.push(w_to_c);
        db.create_table(w).unwrap();

        db
    }

    #[test]
    fn validate_fk_schema_for_dml_asymmetric_depth_detects_broken_edge_on_unvisited_branch() {
        // Exact repro from #6583: the broken edge (`w -> a(junk)`) lives on
        // the branch (`a1`/`a2`/`a`) that the LIFO frontier has NOT yet
        // visited at the moment `w` is discovered via its valid edge to `c`.
        // Before the #6583 fix this silently returned `Ok(())`.
        let db = setup_asymmetric_convergence(/* a_broken = */ true);

        let result = validate_fk_schema_for_dml(&db, "t1");
        match result {
            Err(crate::errors::ExecutorError::ForeignKeyMismatch { child, parent }) => {
                assert_eq!(child, "w");
                assert_eq!(parent, "a");
            }
            other => panic!(
                "expected Err(ForeignKeyMismatch {{ child: \"w\", parent: \"a\" }}), got {:?}",
                other
            ),
        }
    }

    #[test]
    fn validate_fk_schema_for_dml_asymmetric_depth_detects_broken_edge_on_visited_branch() {
        // Mirror image of the exact repro: the broken edge (`w -> c(junk)`)
        // is swapped onto the branch (`c1`/`c2`/`c`) that the LIFO frontier
        // *has* already fully visited by the time `w` is discovered (the
        // branch the pre-#6583 single-pass code already happened to check
        // correctly). Asserts the fix does not depend on which side is
        // broken — both directions must be caught.
        let db = setup_asymmetric_convergence(/* a_broken = */ false);

        let result = validate_fk_schema_for_dml(&db, "t1");
        match result {
            Err(crate::errors::ExecutorError::ForeignKeyMismatch { child, parent }) => {
                assert_eq!(child, "w");
                assert_eq!(parent, "c");
            }
            other => panic!(
                "expected Err(ForeignKeyMismatch {{ child: \"w\", parent: \"c\" }}), got {:?}",
                other
            ),
        }
    }

    #[test]
    fn validate_fk_schema_for_dml_three_way_convergence_detects_broken_edge() {
        // Three independent chains from `t1` (`a1<-a`, `b1<-b`, `c1<-c`)
        // converge on `w`, which has FKs to all three leaves. Only the edge
        // to `a` is broken (`a.junk` has no PK/UNIQUE). This generalizes
        // #6570's two-way diamond and #6583's asymmetric two-way case to
        // three simultaneous converging ancestors.
        let mut db = Database::new();
        db.set_foreign_keys_enabled(true);

        let t1 = TableSchema::with_primary_key(
            "t1".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
            vec!["id".to_string()],
        );
        db.create_table(t1).unwrap();

        fn link_fk(parent_table: &str) -> ForeignKeyConstraint {
            ForeignKeyConstraint {
                name: Some(format!("fk_to_{parent_table}")),
                column_names: vec!["pid".to_string()],
                column_indices: vec![1],
                parent_table: parent_table.to_string(),
                parent_column_names: vec!["id".to_string()],
                parent_column_indices: vec![0],
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::NoAction,
                is_deferrable: false,
                initially_deferred: false,
            }
        }

        for mid in ["a1", "b1", "c1"] {
            let columns = vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("pid".to_string(), DataType::Integer, true),
            ];
            let mut table =
                TableSchema::with_primary_key(mid.to_string(), columns, vec!["id".to_string()]);
            table.foreign_keys.push(link_fk("t1"));
            db.create_table(table).unwrap();
        }

        for (leaf, mid) in [("a", "a1"), ("b", "b1"), ("c", "c1")] {
            let columns = vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("pid".to_string(), DataType::Integer, true),
                ColumnSchema::new("junk".to_string(), DataType::Integer, true),
            ];
            let mut table =
                TableSchema::with_primary_key(leaf.to_string(), columns, vec!["id".to_string()]);
            table.foreign_keys.push(link_fk(mid));
            db.create_table(table).unwrap();
        }

        let w_to_a = ForeignKeyConstraint {
            name: Some("fk_w_a".to_string()),
            column_names: vec!["a_ref".to_string()],
            column_indices: vec![1],
            parent_table: "a".to_string(),
            parent_column_names: vec!["junk".to_string()],
            parent_column_indices: vec![2],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };
        let w_to_b = ForeignKeyConstraint {
            name: Some("fk_w_b".to_string()),
            column_names: vec!["b_ref".to_string()],
            column_indices: vec![2],
            parent_table: "b".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };
        let w_to_c = ForeignKeyConstraint {
            name: Some("fk_w_c".to_string()),
            column_names: vec!["c_ref".to_string()],
            column_indices: vec![3],
            parent_table: "c".to_string(),
            parent_column_names: vec!["id".to_string()],
            parent_column_indices: vec![0],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
            is_deferrable: false,
            initially_deferred: false,
        };
        let w_columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("a_ref".to_string(), DataType::Integer, true),
            ColumnSchema::new("b_ref".to_string(), DataType::Integer, true),
            ColumnSchema::new("c_ref".to_string(), DataType::Integer, true),
        ];
        let mut w =
            TableSchema::with_primary_key("w".to_string(), w_columns, vec!["id".to_string()]);
        w.foreign_keys.push(w_to_a);
        w.foreign_keys.push(w_to_b);
        w.foreign_keys.push(w_to_c);
        db.create_table(w).unwrap();

        let result = validate_fk_schema_for_dml(&db, "t1");
        match result {
            Err(crate::errors::ExecutorError::ForeignKeyMismatch { child, parent }) => {
                assert_eq!(child, "w");
                assert_eq!(parent, "a");
            }
            other => panic!(
                "expected Err(ForeignKeyMismatch {{ child: \"w\", parent: \"a\" }}), got {:?}",
                other
            ),
        }
    }
}
