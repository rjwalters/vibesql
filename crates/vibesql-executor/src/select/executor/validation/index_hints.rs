//! Validation of SQLite index hints (`INDEXED BY` / `NOT INDEXED`)
//!
//! SQLite validates `INDEXED BY <name>` at prepare time: the named index must
//! exist and must be an index on the table it is attached to, otherwise the
//! statement fails with `no such index: <name>`. VibeSQL's planner chooses
//! indexes independently, so after validation the hint is simply ignored
//! (matching the intent of PR #5234); this module only provides the
//! SQLite-compatible error behavior (issue #5235).
//!
//! `NOT INDEXED` never errors.

use vibesql_ast::{FromClause, IndexHint, SelectStmt};

use crate::errors::ExecutorError;

/// Validate all `INDEXED BY` hints in a SELECT statement's FROM tree.
///
/// Walks the FROM clause recursively (through JOIN branches, derived-table
/// subqueries, and VALUES), plus CTE bodies and set-operation arms, so the
/// whole statement is validated up front like SQLite's prepare step —
/// subquery execution may otherwise be short-circuited before its hints are
/// seen.
///
/// Errors with `ExecutorError::NoSuchIndex` (rendered as
/// `no such index: <name>`) when an `INDEXED BY` hint names an index that
/// does not exist or that belongs to a different table. A hint on a CTE name
/// also errors, matching SQLite (a CTE is not a real table, so no index can
/// belong to it).
pub fn validate_index_hints(
    stmt: &SelectStmt,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    validate_select(stmt, &[], database)
}

/// Validate a SELECT scope with the CTE names inherited from enclosing scopes.
///
/// The CTE-name scope is purely additive across nested scopes: an outer CTE
/// remains visible inside derived-table subqueries, set-operation arms, and
/// later CTE bodies, and an inner WITH can only add names. So a simple
/// extended `Vec` threaded down the recursion is sufficient — no
/// shadowing/removal logic is needed.
fn validate_select(
    stmt: &SelectStmt,
    outer_cte_names: &[String],
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    // CTE names visible to this statement's FROM clause: a table reference
    // that resolves to a CTE can never carry a valid INDEXED BY hint.
    let mut cte_names: Vec<String> = outer_cte_names.to_vec();
    if let Some(ctes) = &stmt.with_clause {
        for cte in ctes {
            // Push the CTE's own name before validating its body so a
            // self-reference (`WITH x AS (... FROM x INDEXED BY i)`) errors
            // rather than passing; SQLite errors on self-reference too
            // (`circular reference` / `no query solution` — exact message
            // parity is out of scope). Earlier CTEs in the same WITH clause
            // plus the outer scope are also visible to the body.
            cte_names.push(cte.name.to_lowercase());
            validate_select(&cte.query, &cte_names, database)?;
        }
    }

    if let Some(from) = &stmt.from {
        validate_from_clause(from, &cte_names, database)?;
    }

    // Set-operation arms (UNION / INTERSECT / EXCEPT)
    if let Some(set_op) = &stmt.set_operation {
        validate_select(&set_op.right, &cte_names, database)?;
    }

    Ok(())
}

/// Recursively validate index hints in a FROM clause tree.
fn validate_from_clause(
    from: &FromClause,
    cte_names: &[String],
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    match from {
        FromClause::Table { name, index_hint, .. } => {
            if let Some(IndexHint::IndexedBy(index_name)) = index_hint {
                validate_indexed_by(name, index_name, cte_names, database)?;
            }
            Ok(())
        }
        FromClause::Join { left, right, .. } => {
            validate_from_clause(left, cte_names, database)?;
            validate_from_clause(right, cte_names, database)
        }
        FromClause::Subquery { query, .. } => validate_select(query, cte_names, database),
        FromClause::Values { .. } => Ok(()),
        FromClause::TableFunction { .. } => Ok(()),
    }
}

/// Validate a single `INDEXED BY` hint against the catalog.
fn validate_indexed_by(
    table_name: &str,
    index_name: &str,
    cte_names: &[String],
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    let no_such_index = || ExecutorError::NoSuchIndex { index_name: index_name.to_string() };

    // A CTE shadows any same-named real table; an index can never belong to
    // a CTE, so INDEXED BY on a CTE reference always fails (SQLite parity).
    let table_lower = table_name.to_lowercase();
    if cte_names.contains(&table_lower) {
        return Err(no_such_index());
    }

    let metadata = database.get_index(index_name).ok_or_else(no_such_index)?;

    // The index must be on this specific table (case-insensitive). Accept
    // both the full FROM name and its unqualified tail so schema-qualified
    // references like `main.t1 INDEXED BY t1x` canonicalize correctly.
    let index_table_lower = metadata.table_name.to_lowercase();
    let unqualified_lower = table_lower.rsplit('.').next().unwrap_or(&table_lower);
    if index_table_lower != table_lower && index_table_lower != unqualified_lower {
        return Err(no_such_index());
    }

    Ok(())
}
