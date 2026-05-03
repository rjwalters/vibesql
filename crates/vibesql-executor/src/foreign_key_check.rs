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
use vibesql_storage::Database;

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

    // 3. Non-partial UNIQUE INDEX exact match. `IndexMetadata` does not yet
    //    track a `where_clause`, so every UNIQUE index in the catalog is
    //    treated as full-coverage. Expression indexes can never back an FK.
    for index in db.catalog.get_table_indexes(&parent.name) {
        if !index.is_unique || index.has_expression_columns() {
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
}
