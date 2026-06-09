//! Shared foreign-key validation helpers.
//!
//! This module centralizes two pieces of FK enforcement that must stay in sync
//! across multiple call sites (INSERT, UPDATE, PRAGMA `foreign_key_check`):
//!
//! 1. [`detect_fk_mismatch`] — Verify that the parent table actually has a PK,
//!    UNIQUE constraint, or non-partial UNIQUE INDEX that *exactly* covers the
//!    columns referenced by the FK. SQLite raises a `foreign key mismatch`
//!    error before any row-existence check when no such key exists.
//! 2. [`fk_values_equal`] — SQLite-style equality for FK comparisons that
//!    honours numeric coercion as well as the parent column's `NOCASE` /
//!    `RTRIM` collation. The non-collation logic mirrors VibeSQL's
//!    [`vibesql_types::SqlValue`] strict equality; the collation logic mirrors
//!    the helpers used in `select::grouping::aggregates`.
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

/// Resolve the FK's parent-side column indices, preferring `parent_column_names`
/// over the cached `parent_column_indices` (which may carry placeholder zeros
/// when the parent did not yet exist at FK creation time).
fn resolve_parent_indices(
    parent: &TableSchema,
    fk: &ForeignKeyConstraint,
) -> Vec<usize> {
    // Names available and non-empty — resolve lazily.
    let names_usable = !fk.parent_column_names.is_empty()
        && fk.parent_column_names.iter().any(|n| !n.is_empty());
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

        // Names don't resolve — likely an empty-FK-list (REFERENCES p1)
        // pointing at the parent's PK. Fall through to the cached indices.
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
fn parent_has_matching_key(
    db: &Database,
    parent: &TableSchema,
    parent_indices: &[usize],
) -> bool {
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

    // 3. Non-partial UNIQUE INDEX exact match. Partial indexes (those with a
    //    WHERE clause) are excluded — SQLite's `sqlite3FkLocateIndex` only
    //    accepts indexes that cover every parent row. Expression indexes can
    //    never back an FK either.
    for index in db.catalog.get_table_indexes(&parent.name) {
        if !index.is_unique || index.has_expression_columns() || index.is_partial() {
            continue;
        }
        let mut index_col_indices: Vec<usize> = Vec::with_capacity(index.columns.len());
        let mut all_resolved = true;
        for col in &index.columns {
            match col.column_name().and_then(|n| parent.get_column_index(n)) {
                Some(idx) => index_col_indices.push(idx),
                None => {
                    all_resolved = false;
                    break;
                }
            }
        }
        if !all_resolved {
            continue;
        }
        if column_set_eq(&index_col_indices, parent_indices) {
            return true;
        }
    }

    // 4. Fallback: a single-column FK against a column that is itself
    //    declared with column-level UNIQUE is represented in
    //    `unique_constraints`, so the match succeeds at step 2 above.
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
pub fn parent_collations_for_fk(
    db: &Database,
    fk: &ForeignKeyConstraint,
) -> Vec<Option<String>> {
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
pub fn resolved_parent_indices_for_fk(
    db: &Database,
    fk: &ForeignKeyConstraint,
) -> Vec<usize> {
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
/// * `fk_values` must be non-empty and contain no NULLs — the caller is
///   responsible for the NULL-skip. (NULL FK values pass FK enforcement
///   per SQL / SQLite.)
/// * The PRAGMA `foreign_keys` gate, schema-mismatch check, and parent-table
///   existence must already be verified by the caller. This helper does
///   **not** call [`detect_fk_mismatch`] and does **not** look up the
///   parent table — the caller passes those results in.
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
            parent_indices.iter().zip(fk_values).enumerate().all(
                |(i, (&parent_idx, fk_val))| match candidate.get(parent_idx) {
                    Some(parent_val) => fk_values_equal(
                        fk_val,
                        parent_val,
                        parent_collations.get(i).and_then(|c| c.as_deref()),
                    ),
                    None => false,
                },
            )
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
    let should_defer = in_txn && (fk.initially_deferred || session_defer);
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
        let mut c = TableSchema::with_primary_key(
            "c".to_string(),
            child_columns,
            vec!["id".to_string()],
        );
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
}
