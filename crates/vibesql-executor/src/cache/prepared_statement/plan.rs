//! Query plan caching for prepared statements
//!
//! This module provides cached execution plans that bypass the full query planning
//! pipeline for simple query patterns. When a prepared statement is created, we
//! analyze the AST to detect patterns that can use fast execution paths.
//!
//! ## Supported Patterns
//!
//! ### Primary Key Point Lookup
//!
//! Queries like `SELECT * FROM table WHERE pk_col = ?` are detected and cached with:
//! - Table name
//! - Primary key column indices in schema
//! - Parameter indices for PK values
//! - Projection column indices (or wildcard flag)
//!
//! At execution time, this allows direct index lookup without:
//! - Re-parsing the AST
//! - Checking if it's a simple query
//! - Resolving table/column names
//! - Building schema objects
//!
//! ## Performance Impact
//!
//! For simple point SELECT via prepared statement:
//! - Without plan caching: ~60µs (AST manipulation, pattern detection, table lookup)
//! - With plan caching: ~1-5µs (direct index lookup)
//!
//! ## Cache Invalidation
//!
//! Cached plans are invalidated when:
//! - DDL modifies the referenced table (schema change)
//! - The table is dropped
//! - Indexes are added/removed

use std::sync::{Arc, OnceLock};
use vibesql_ast::{DeleteStmt, Expression, FromClause, SelectItem, SelectStmt, Statement, WhereClause};

/// Cached execution plan for a prepared statement
#[derive(Debug, Clone)]
pub enum CachedPlan {
    /// Simple primary key point lookup
    /// SELECT [cols] FROM table WHERE pk_col = ? [AND pk_col2 = ? ...]
    PkPointLookup(PkPointLookupPlan),

    /// Simple fast-path eligible query
    /// Caches the result of is_simple_point_query() check to avoid
    /// recomputing it on every execution.
    SimpleFastPath(SimpleFastPathPlan),

    /// Simple primary key DELETE
    /// DELETE FROM table WHERE pk_col = ? [AND pk_col2 = ? ...]
    PkDelete(PkDeletePlan),

    /// Query doesn't match any fast-path pattern
    /// Fall back to standard execution
    Standard,
}

impl CachedPlan {
    /// Check if this is a cacheable fast-path plan
    pub fn is_fast_path(&self) -> bool {
        !matches!(self, CachedPlan::Standard)
    }
}

/// Cached plan for simple fast-path eligible queries
///
/// This caches the result of `is_simple_point_query()` to avoid
/// recomputing it on every execution. Provides moderate speedup for
/// queries that pass fast-path validation but don't match the
/// specialized PkPointLookup pattern.
///
/// ## Performance Optimization (#3780)
///
/// The `resolved_columns` field caches column names derived from the SELECT list
/// after the first execution. This eliminates repeated:
/// - Table lookups via `database.get_table()`
/// - SELECT list iteration and column name derivation
/// - Schema column iteration for wildcards
///
/// This reduces per-query overhead from ~5-15µs to ~1-2µs for repeated executions.
#[derive(Debug, Clone)]
pub struct SimpleFastPathPlan {
    /// Table name (normalized to uppercase for case-insensitive matching)
    pub table_name: String,

    /// Lazily-cached column names derived from the SELECT list
    /// Populated on first execution to avoid repeated column name derivation
    resolved_columns: Arc<OnceLock<Arc<[String]>>>,
}

impl SimpleFastPathPlan {
    /// Create a new SimpleFastPathPlan with the given table name
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            resolved_columns: Arc::new(OnceLock::new()),
        }
    }

    /// Get or initialize the cached column names
    ///
    /// The resolver function is called only on the first invocation and derives
    /// column names from the SELECT list and table schema.
    pub fn get_or_resolve_columns<F>(&self, resolver: F) -> Option<&Arc<[String]>>
    where
        F: FnOnce() -> Option<Vec<String>>,
    {
        self.resolved_columns.get_or_init(|| {
            // If resolution fails, we return an empty array as a sentinel
            resolver().map(|v| v.into()).unwrap_or_else(|| Arc::from([]))
        });

        // Check if we got a valid resolution (non-empty)
        self.resolved_columns.get().filter(|cols| !cols.is_empty())
    }

    /// Check if columns have been resolved
    pub fn is_resolved(&self) -> bool {
        self.resolved_columns.get().is_some()
    }
}

/// Cached plan for primary key point lookup queries
#[derive(Debug, Clone)]
pub struct PkPointLookupPlan {
    /// Table name (normalized to uppercase for case-insensitive matching)
    pub table_name: String,

    /// Primary key column names in order
    pub pk_columns: Vec<String>,

    /// Mapping from parameter index (0-based) to PK column index
    /// e.g., for `WHERE pk1 = ? AND pk2 = ?`, this would be [(0, 0), (1, 1)]
    pub param_to_pk_col: Vec<(usize, usize)>,

    /// Projection type
    pub projection: ProjectionPlan,

    /// Lazily-cached resolved projection (column indices and output names)
    /// Populated on first execution to avoid repeated schema lookups
    resolved: Arc<OnceLock<ResolvedProjection>>,
}

/// Resolved projection information cached after first execution
///
/// This avoids repeated O(n) column name lookups on every query.
#[derive(Debug, Clone)]
pub struct ResolvedProjection {
    /// Column indices in the source table for projection
    pub column_indices: Vec<usize>,
    /// Output column names (may include aliases)
    pub column_names: Arc<[String]>,
}

impl PkPointLookupPlan {
    /// Get or initialize the resolved projection for this plan
    ///
    /// The resolver function is called only on the first invocation and takes
    /// the projection plan to resolve column names to indices.
    pub fn get_or_resolve<F>(&self, resolver: F) -> Option<&ResolvedProjection>
    where
        F: FnOnce(&ProjectionPlan) -> Option<ResolvedProjection>,
    {
        self.resolved.get_or_init(|| {
            // We need to handle the case where resolution fails
            // Using a sentinel value or Option would complicate the API
            // Instead, if resolution fails, we return a "failed" marker
            resolver(&self.projection).unwrap_or(ResolvedProjection {
                column_indices: vec![],
                column_names: Arc::from([]),
            })
        });

        // Check if we got a valid resolution (non-empty)
        self.resolved.get().filter(|r| !r.column_names.is_empty() || matches!(self.projection, ProjectionPlan::Wildcard))
    }

    /// Check if projection has been resolved
    pub fn is_resolved(&self) -> bool {
        self.resolved.get().is_some()
    }
}

/// How to project columns from the result
#[derive(Debug, Clone)]
pub enum ProjectionPlan {
    /// SELECT * - return all columns
    Wildcard,

    /// SELECT col1, col2, ... - return specific columns by index
    Columns(Vec<ColumnProjection>),
}

/// A single column in the projection
#[derive(Debug, Clone)]
pub struct ColumnProjection {
    /// Column name in the table
    pub column_name: String,

    /// Output alias (if specified via AS)
    pub alias: Option<String>,
}

/// Cached plan for primary key DELETE statements
/// DELETE FROM table WHERE pk_col = ? [AND pk_col2 = ? ...]
///
/// This plan bypasses:
/// - AST cloning during parameter binding
/// - Schema cloning during execution
/// - Re-checking the DELETE pattern on every execution
///
/// At execution time, we extract PK values directly from params and call delete_by_pk_fast.
#[derive(Debug, Clone)]
pub struct PkDeletePlan {
    /// Table name (normalized to uppercase for case-insensitive matching)
    pub table_name: String,

    /// Primary key column names in order (for validation)
    pub pk_columns: Vec<String>,

    /// Mapping from parameter index (0-based) to PK column index
    /// e.g., for `WHERE id = ?`, this would be [(0, 0)]
    pub param_to_pk_col: Vec<(usize, usize)>,

    /// Cached validation result: true = fast path is valid (no triggers/FKs)
    /// Uses Arc<OnceLock> so the cache survives Clone
    fast_path_valid: Arc<OnceLock<bool>>,
}

impl PkDeletePlan {
    /// Create a new PkDeletePlan
    pub fn new(table_name: String, pk_columns: Vec<String>, param_to_pk_col: Vec<(usize, usize)>) -> Self {
        Self {
            table_name,
            pk_columns,
            param_to_pk_col,
            fast_path_valid: Arc::new(OnceLock::new()),
        }
    }

    /// Build the PK values array from parameters
    pub fn build_pk_values(&self, params: &[vibesql_types::SqlValue]) -> Vec<vibesql_types::SqlValue> {
        let mut pk_values = vec![vibesql_types::SqlValue::Null; self.pk_columns.len()];
        for &(param_idx, pk_col_idx) in &self.param_to_pk_col {
            if param_idx < params.len() && pk_col_idx < pk_values.len() {
                pk_values[pk_col_idx] = params[param_idx].clone();
            }
        }
        pk_values
    }

    /// Get cached validation result, if available
    pub fn is_fast_path_valid(&self) -> Option<bool> {
        self.fast_path_valid.get().copied()
    }

    /// Set validation result (can only be called once)
    /// Returns the cached value (either newly set or existing)
    pub fn set_fast_path_valid(&self, valid: bool) -> bool {
        *self.fast_path_valid.get_or_init(|| valid)
    }
}

/// Analyze a prepared statement and create a cached plan if possible
pub fn analyze_for_plan(stmt: &Statement) -> CachedPlan {
    match stmt {
        Statement::Select(select) => analyze_select(select),
        Statement::Delete(delete) => analyze_delete(delete),
        _ => CachedPlan::Standard,
    }
}

/// Analyze a SELECT statement for caching opportunities
fn analyze_select(stmt: &SelectStmt) -> CachedPlan {
    // First, try to create a PkPointLookup plan for the most optimized path
    if let Some(plan) = try_analyze_pk_lookup(stmt) {
        return CachedPlan::PkPointLookup(plan);
    }

    // Next, check if the query is eligible for the fast path
    // This caches the result of is_simple_point_query() to avoid recomputing it every execution
    if crate::select::is_simple_point_query(stmt) {
        if let Some(table_name) = extract_single_table_name(stmt) {
            return CachedPlan::SimpleFastPath(SimpleFastPathPlan::new(
                table_name.to_uppercase(),
            ));
        }
    }

    CachedPlan::Standard
}

/// Analyze a DELETE statement for PK delete optimization
fn analyze_delete(stmt: &DeleteStmt) -> CachedPlan {
    // Must have a WHERE clause
    let where_clause = match &stmt.where_clause {
        Some(WhereClause::Condition(expr)) => expr,
        _ => return CachedPlan::Standard,
    };

    // Extract parameter-to-column mappings from WHERE clause
    let param_mappings = match extract_pk_param_mappings(where_clause) {
        Some(mappings) if !mappings.is_empty() => mappings,
        _ => return CachedPlan::Standard,
    };

    // Build the plan
    let pk_columns: Vec<String> = param_mappings.iter().map(|(_, col)| col.clone()).collect();
    let param_to_pk_col: Vec<(usize, usize)> = param_mappings
        .iter()
        .enumerate()
        .map(|(pk_idx, (param_idx, _))| (*param_idx, pk_idx))
        .collect();

    CachedPlan::PkDelete(PkDeletePlan::new(
        stmt.table_name.to_uppercase(),
        pk_columns,
        param_to_pk_col,
    ))
}

/// Try to analyze a SELECT for PK point lookup optimization
fn try_analyze_pk_lookup(stmt: &SelectStmt) -> Option<PkPointLookupPlan> {
    // Must have exactly one table in FROM (no joins, no subqueries)
    let table_name = match &stmt.from {
        Some(FromClause::Table { name, alias: None, .. }) => name.clone(),
        _ => return None,
    };

    // No complex clauses
    if stmt.with_clause.is_some()
        || stmt.set_operation.is_some()
        || stmt.group_by.is_some()
        || stmt.having.is_some()
        || stmt.distinct
        || stmt.order_by.is_some()
        || stmt.limit.is_some()
        || stmt.offset.is_some()
        || stmt.into_table.is_some()
        || stmt.into_variables.is_some()
    {
        return None;
    }

    // Check SELECT list - must be wildcard or simple column references
    let projection = analyze_select_list(&stmt.select_list)?;

    // Must have a WHERE clause with PK equality predicates
    let where_clause = stmt.where_clause.as_ref()?;

    // Extract parameter-to-column mappings from WHERE clause
    let param_mappings = extract_pk_param_mappings(where_clause)?;
    if param_mappings.is_empty() {
        return None;
    }

    // Build the plan
    // Note: We don't validate PK columns here because we don't have DB access.
    // The Session will validate at prepare time and fall back if not valid.
    let pk_columns: Vec<String> = param_mappings.iter().map(|(_, col)| col.clone()).collect();
    let param_to_pk_col: Vec<(usize, usize)> = param_mappings
        .iter()
        .enumerate()
        .map(|(pk_idx, (param_idx, _))| (*param_idx, pk_idx))
        .collect();

    Some(PkPointLookupPlan {
        table_name: table_name.to_uppercase(),
        pk_columns,
        param_to_pk_col,
        projection,
        resolved: Arc::new(OnceLock::new()),
    })
}

/// Extract the single table name from a simple FROM clause
fn extract_single_table_name(stmt: &SelectStmt) -> Option<String> {
    match &stmt.from {
        Some(FromClause::Table { name, .. }) => Some(name.clone()),
        _ => None,
    }
}

/// Analyze SELECT list and return projection plan if it's simple
fn analyze_select_list(select_list: &[SelectItem]) -> Option<ProjectionPlan> {
    if select_list.len() == 1 {
        if let SelectItem::Wildcard { .. } = &select_list[0] {
            return Some(ProjectionPlan::Wildcard);
        }
    }

    // Check each item is a simple column reference
    let mut columns = Vec::with_capacity(select_list.len());
    for item in select_list {
        match item {
            SelectItem::Wildcard { .. } => {
                // Mixed wildcard and columns - fall back
                return None;
            }
            SelectItem::QualifiedWildcard { .. } => {
                // table.* - fall back for now
                return None;
            }
            SelectItem::Expression { expr, alias } => {
                let column_name = match expr {
                    Expression::ColumnRef { column, table: None } => column.clone(),
                    Expression::ColumnRef { column, table: Some(_) } => {
                        // Qualified column ref - support it
                        column.clone()
                    }
                    _ => {
                        // Complex expression - fall back
                        return None;
                    }
                };
                columns.push(ColumnProjection { column_name, alias: alias.clone() });
            }
        }
    }

    Some(ProjectionPlan::Columns(columns))
}

/// Extract parameter-to-column mappings from WHERE clause
///
/// Returns a list of (parameter_index, column_name) pairs in order.
/// For `WHERE pk1 = ? AND pk2 = ?`, returns [(0, "pk1"), (1, "pk2")]
fn extract_pk_param_mappings(expr: &Expression) -> Option<Vec<(usize, String)>> {
    let mut mappings = Vec::new();
    collect_pk_param_mappings(expr, &mut mappings)?;

    // Sort by parameter index to ensure consistent ordering
    mappings.sort_by_key(|(idx, _)| *idx);

    Some(mappings)
}

/// Recursively collect parameter-to-column mappings
fn collect_pk_param_mappings(expr: &Expression, mappings: &mut Vec<(usize, String)>) -> Option<()> {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::And => {
                    // Recurse into both sides
                    collect_pk_param_mappings(left, mappings)?;
                    collect_pk_param_mappings(right, mappings)?;
                }
                vibesql_ast::BinaryOperator::Equal => {
                    // Check for col = ? pattern
                    if let Some(mapping) = extract_column_placeholder_pair(left, right) {
                        mappings.push(mapping);
                    } else {
                        // Non-placeholder equality - not a fast-path query
                        return None;
                    }
                }
                _ => {
                    // Other operators (OR, <, >, etc.) - not a simple PK lookup
                    return None;
                }
            }
        }
        _ => {
            // Other expression types - not supported
            return None;
        }
    }
    Some(())
}

/// Extract (param_index, column_name) from an equality expression
fn extract_column_placeholder_pair(
    left: &Expression,
    right: &Expression,
) -> Option<(usize, String)> {
    // Try col = ?
    if let Expression::ColumnRef { column, .. } = left {
        if let Expression::Placeholder(idx) = right {
            return Some((*idx, column.clone()));
        }
    }

    // Try ? = col
    if let Expression::Placeholder(idx) = left {
        if let Expression::ColumnRef { column, .. } = right {
            return Some((*idx, column.clone()));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_parser::Parser;

    fn parse_to_plan(sql: &str) -> CachedPlan {
        let stmt = Parser::parse_sql(sql).unwrap();
        analyze_for_plan(&stmt)
    }

    #[test]
    fn test_simple_pk_lookup() {
        let plan = parse_to_plan("SELECT * FROM users WHERE id = ?");
        match plan {
            CachedPlan::PkPointLookup(p) => {
                assert_eq!(p.table_name, "USERS");
                // Parser normalizes identifiers to uppercase
                assert_eq!(p.pk_columns, vec!["ID"]);
                assert_eq!(p.param_to_pk_col, vec![(0, 0)]);
                assert!(matches!(p.projection, ProjectionPlan::Wildcard));
            }
            _ => panic!("Expected PkPointLookup"),
        }
    }

    #[test]
    fn test_composite_pk_lookup() {
        let plan = parse_to_plan("SELECT * FROM orders WHERE customer_id = ? AND order_id = ?");
        match plan {
            CachedPlan::PkPointLookup(p) => {
                assert_eq!(p.table_name, "ORDERS");
                // Parser normalizes identifiers to uppercase
                assert_eq!(p.pk_columns, vec!["CUSTOMER_ID", "ORDER_ID"]);
                assert_eq!(p.param_to_pk_col.len(), 2);
            }
            _ => panic!("Expected PkPointLookup"),
        }
    }

    #[test]
    fn test_projected_columns() {
        let plan = parse_to_plan("SELECT name, email FROM users WHERE id = ?");
        match plan {
            CachedPlan::PkPointLookup(p) => {
                match p.projection {
                    ProjectionPlan::Columns(cols) => {
                        assert_eq!(cols.len(), 2);
                        // Parser normalizes identifiers to uppercase
                        assert_eq!(cols[0].column_name, "NAME");
                        assert_eq!(cols[1].column_name, "EMAIL");
                    }
                    _ => panic!("Expected Columns projection"),
                }
            }
            _ => panic!("Expected PkPointLookup"),
        }
    }

    #[test]
    fn test_not_cacheable_join() {
        let plan =
            parse_to_plan("SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = ?");
        assert!(matches!(plan, CachedPlan::Standard));
    }

    #[test]
    fn test_not_cacheable_aggregate() {
        let plan = parse_to_plan("SELECT COUNT(*) FROM users WHERE id = ?");
        assert!(matches!(plan, CachedPlan::Standard));
    }

    #[test]
    fn test_not_cacheable_order_by() {
        let plan = parse_to_plan("SELECT * FROM users WHERE id = ? ORDER BY name");
        assert!(matches!(plan, CachedPlan::Standard));
    }

    #[test]
    fn test_not_cacheable_or() {
        let plan = parse_to_plan("SELECT * FROM users WHERE id = ? OR name = ?");
        assert!(matches!(plan, CachedPlan::Standard));
    }

    #[test]
    fn test_literal_gets_simple_fast_path() {
        // Queries with literals don't get PkPointLookup (requires placeholders),
        // but they do get SimpleFastPath since they pass is_simple_point_query()
        let plan = parse_to_plan("SELECT * FROM users WHERE id = 1");
        assert!(matches!(plan, CachedPlan::SimpleFastPath(_)));
    }

    #[test]
    fn test_delete_pk_lookup() {
        let plan = parse_to_plan("DELETE FROM sbtest1 WHERE id = ?");
        match plan {
            CachedPlan::PkDelete(p) => {
                assert_eq!(p.table_name, "SBTEST1");
                assert_eq!(p.pk_columns, vec!["ID"]);
                assert_eq!(p.param_to_pk_col, vec![(0, 0)]);
            }
            other => panic!("Expected PkDelete, got {:?}", other),
        }
    }

    #[test]
    fn test_delete_without_where_not_fast_path() {
        let plan = parse_to_plan("DELETE FROM users");
        assert!(matches!(plan, CachedPlan::Standard));
    }
}
