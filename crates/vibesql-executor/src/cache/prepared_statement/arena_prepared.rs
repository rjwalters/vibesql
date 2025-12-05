//! Arena-based prepared statement for zero-allocation execution
//!
//! This module implements Option A from the arena parser optimization:
//! storing the arena-allocated AST directly in the prepared statement,
//! avoiding conversion overhead entirely for simple queries.
//!
//! # Key Benefits
//!
//! - **Zero conversion overhead**: Arena AST is used directly during execution
//! - **Near-zero allocation**: Simple prepared queries avoid heap allocation
//! - **Arena reuse**: Arena can be reset between executions for parameter binding
//!
//! # Safety
//!
//! This module uses careful unsafe code to manage self-referential data.
//! The arena is pinned to prevent moves, and the statement pointer is valid
//! as long as the arena exists.

use std::collections::HashSet;
use std::pin::Pin;

use bumpalo::Bump;
use vibesql_ast::arena::{ArenaInterner, Expression, ExtendedExpr, SelectStmt};
use vibesql_parser::arena_parser::ArenaParser;
use vibesql_types::SqlValue;

/// Arena-based prepared statement for zero-allocation execution.
///
/// This stores the parsed AST in an arena, keeping it alive for the lifetime
/// of the prepared statement. This avoids conversion overhead for simple queries.
///
/// # Safety Invariants
///
/// 1. The arena is pinned and will not move after construction
/// 2. The statement pointer is valid as long as the arena exists
/// 3. The arena is only dropped when the ArenaPreparedStatement is dropped
/// 4. The interner pointer is valid as long as the arena exists
pub struct ArenaPreparedStatement {
    /// Original SQL with placeholders
    sql: String,
    /// Arena containing the parsed statement (pinned to prevent moves)
    arena: Pin<Box<Bump>>,
    /// Pointer to the statement in the arena.
    ///
    /// SAFETY: This pointer is valid as long as `arena` is not dropped or reset.
    /// Since ArenaPreparedStatement owns the arena and only drops it in Drop,
    /// this pointer is always valid during the lifetime of the struct.
    statement_ptr: *const SelectStmt<'static>,
    /// Pointer to the interner in the arena.
    ///
    /// SAFETY: Same invariants as statement_ptr.
    interner_ptr: *const ArenaInterner<'static>,
    /// Number of parameters expected (? placeholders)
    param_count: usize,
    /// Tables referenced by this statement (for cache invalidation)
    tables: HashSet<String>,
}

// SAFETY: The arena and statement are not accessed from multiple threads.
// The statement pointer is only dereferenced through &self methods.
unsafe impl Send for ArenaPreparedStatement {}
unsafe impl Sync for ArenaPreparedStatement {}

impl ArenaPreparedStatement {
    /// Create a new arena-based prepared statement from SQL.
    ///
    /// Parses the SQL using the arena parser and stores the result in an owned arena.
    pub fn new(sql: String) -> Result<Self, ArenaParseError> {
        // Create pinned arena (won't move)
        let arena = Pin::new(Box::new(Bump::new()));

        // Parse into arena, getting both statement and interner
        let (stmt, interner): (&SelectStmt<'_>, ArenaInterner<'_>) =
            ArenaParser::parse_select_with_interner(&sql, &arena)
                .map_err(|e| ArenaParseError::ParseError(e.to_string()))?;

        // Count placeholders and extract tables (using interner for symbol resolution)
        let param_count = count_arena_placeholders(stmt);
        let tables = extract_arena_tables(stmt, &interner);

        // Store as raw pointer, erasing the lifetime.
        // SAFETY: The arena is owned by this struct and won't be dropped
        // while the statement exists. The pointer remains valid.
        let statement_ptr = stmt as *const SelectStmt<'_>;
        // Cast to 'static lifetime - this is safe because the arena owns the data
        let statement_ptr = statement_ptr.cast::<SelectStmt<'static>>();

        // Allocate interner in arena and store pointer
        // SAFETY: Same invariants as statement_ptr
        let interner_in_arena = arena.alloc(interner);
        let interner_ptr = interner_in_arena as *const ArenaInterner<'_>;
        // Cast to 'static lifetime - this is safe because the arena owns the data
        let interner_ptr = interner_ptr.cast::<ArenaInterner<'static>>();

        Ok(Self { sql, arena, statement_ptr, interner_ptr, param_count, tables })
    }

    /// Get the original SQL.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Get a reference to the arena-allocated statement.
    ///
    /// The returned reference is valid for the lifetime of this struct.
    pub fn statement(&self) -> &SelectStmt<'_> {
        // SAFETY: The pointer is valid as documented in the struct invariants.
        // We're returning a reference tied to self's lifetime, which is correct.
        unsafe { &*self.statement_ptr }
    }

    /// Get a reference to the interner for symbol resolution.
    ///
    /// The returned reference is valid for the lifetime of this struct.
    pub fn interner(&self) -> &ArenaInterner<'_> {
        // SAFETY: The pointer is valid as documented in the struct invariants.
        unsafe { &*self.interner_ptr }
    }

    /// Get the number of parameters expected.
    pub fn param_count(&self) -> usize {
        self.param_count
    }

    /// Get the tables referenced by this statement.
    pub fn tables(&self) -> &HashSet<String> {
        &self.tables
    }

    /// Get a reference to the arena for additional allocations.
    ///
    /// This can be used for allocating bound values during execution.
    pub fn arena(&self) -> &Bump {
        &self.arena
    }

    /// Validate that the correct number of parameters is provided.
    pub fn validate_params(&self, params: &[SqlValue]) -> Result<(), ArenaBindError> {
        if params.len() != self.param_count {
            return Err(ArenaBindError::ParameterCountMismatch {
                expected: self.param_count,
                actual: params.len(),
            });
        }
        Ok(())
    }
}

impl std::fmt::Debug for ArenaPreparedStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArenaPreparedStatement")
            .field("sql", &self.sql)
            .field("param_count", &self.param_count)
            .field("tables", &self.tables)
            .finish_non_exhaustive()
    }
}

/// Error during arena-based parsing.
#[derive(Debug, Clone)]
pub enum ArenaParseError {
    /// SQL parsing failed
    ParseError(String),
}

impl std::fmt::Display for ArenaParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArenaParseError::ParseError(msg) => write!(f, "Parse error: {}", msg),
        }
    }
}

impl std::error::Error for ArenaParseError {}

/// Error during arena-based parameter binding.
#[derive(Debug, Clone)]
pub enum ArenaBindError {
    /// Wrong number of parameters provided
    ParameterCountMismatch { expected: usize, actual: usize },
}

impl std::fmt::Display for ArenaBindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArenaBindError::ParameterCountMismatch { expected, actual } => {
                write!(f, "Parameter count mismatch: expected {}, got {}", expected, actual)
            }
        }
    }
}

impl std::error::Error for ArenaBindError {}

/// Count the number of placeholder parameters in an arena statement.
fn count_arena_placeholders(stmt: &SelectStmt<'_>) -> usize {
    let mut count = 0;
    visit_arena_statement(stmt, &mut |expr| {
        if matches!(expr, Expression::Placeholder(_)) {
            count += 1;
        }
    });
    count
}

/// Extract table names from an arena statement.
fn extract_arena_tables(stmt: &SelectStmt<'_>, interner: &ArenaInterner<'_>) -> HashSet<String> {
    let mut tables = HashSet::new();
    visit_arena_from_clause(stmt.from.as_ref(), &mut tables, interner);
    tables
}

/// Visit all expressions in an arena statement.
fn visit_arena_statement<F>(stmt: &SelectStmt<'_>, visitor: &mut F)
where
    F: FnMut(&Expression<'_>),
{
    // Visit CTEs
    if let Some(ctes) = &stmt.with_clause {
        for cte in ctes.iter() {
            visit_arena_statement(cte.query, visitor);
        }
    }

    // Visit select items
    for item in stmt.select_list.iter() {
        if let vibesql_ast::arena::SelectItem::Expression { expr, .. } = item {
            visit_arena_expression(expr, visitor);
        }
    }

    // Visit FROM clause
    if let Some(from) = &stmt.from {
        visit_arena_from_clause_exprs(from, visitor);
    }

    // Visit WHERE
    if let Some(where_clause) = &stmt.where_clause {
        visit_arena_expression(where_clause, visitor);
    }

    // Visit GROUP BY
    if let Some(group_by) = &stmt.group_by {
        visit_arena_group_by(group_by, visitor);
    }

    // Visit HAVING
    if let Some(having) = &stmt.having {
        visit_arena_expression(having, visitor);
    }

    // Visit ORDER BY
    if let Some(order_by) = &stmt.order_by {
        for item in order_by.iter() {
            visit_arena_expression(&item.expr, visitor);
        }
    }

    // Visit set operation
    if let Some(set_op) = &stmt.set_operation {
        visit_arena_statement(set_op.right, visitor);
    }
}

/// Visit expressions in a FROM clause.
fn visit_arena_from_clause_exprs<F>(from: &vibesql_ast::arena::FromClause<'_>, visitor: &mut F)
where
    F: FnMut(&Expression<'_>),
{
    match from {
        vibesql_ast::arena::FromClause::Table { .. } => {}
        vibesql_ast::arena::FromClause::Subquery { query, .. } => {
            visit_arena_statement(query, visitor);
        }
        vibesql_ast::arena::FromClause::Join { left, right, condition, .. } => {
            visit_arena_from_clause_exprs(left, visitor);
            visit_arena_from_clause_exprs(right, visitor);
            if let Some(cond) = condition {
                visit_arena_expression(cond, visitor);
            }
        }
    }
}

/// Extract table names from a FROM clause.
fn visit_arena_from_clause(
    from: Option<&vibesql_ast::arena::FromClause<'_>>,
    tables: &mut HashSet<String>,
    interner: &ArenaInterner<'_>,
) {
    let Some(from) = from else { return };

    match from {
        vibesql_ast::arena::FromClause::Table { name, .. } => {
            tables.insert(interner.resolve(*name).to_string());
        }
        vibesql_ast::arena::FromClause::Subquery { query, .. } => {
            visit_arena_from_clause(query.from.as_ref(), tables, interner);
        }
        vibesql_ast::arena::FromClause::Join { left, right, .. } => {
            visit_arena_from_clause(Some(left), tables, interner);
            visit_arena_from_clause(Some(right), tables, interner);
        }
    }
}

/// Visit GROUP BY clause expressions.
fn visit_arena_group_by<F>(group_by: &vibesql_ast::arena::GroupByClause<'_>, visitor: &mut F)
where
    F: FnMut(&Expression<'_>),
{
    use vibesql_ast::arena::GroupByClause;
    match group_by {
        GroupByClause::Simple(exprs) => {
            for expr in exprs.iter() {
                visit_arena_expression(expr, visitor);
            }
        }
        GroupByClause::Rollup(elements) | GroupByClause::Cube(elements) => {
            for element in elements.iter() {
                match element {
                    vibesql_ast::arena::GroupingElement::Single(expr) => {
                        visit_arena_expression(expr, visitor);
                    }
                    vibesql_ast::arena::GroupingElement::Composite(exprs) => {
                        for expr in exprs.iter() {
                            visit_arena_expression(expr, visitor);
                        }
                    }
                }
            }
        }
        GroupByClause::GroupingSets(sets) => {
            for set in sets.iter() {
                for expr in set.columns.iter() {
                    visit_arena_expression(expr, visitor);
                }
            }
        }
        GroupByClause::Mixed(items) => {
            for item in items.iter() {
                match item {
                    vibesql_ast::arena::MixedGroupingItem::Simple(expr) => {
                        visit_arena_expression(expr, visitor);
                    }
                    vibesql_ast::arena::MixedGroupingItem::Rollup(elements)
                    | vibesql_ast::arena::MixedGroupingItem::Cube(elements) => {
                        for element in elements.iter() {
                            match element {
                                vibesql_ast::arena::GroupingElement::Single(expr) => {
                                    visit_arena_expression(expr, visitor);
                                }
                                vibesql_ast::arena::GroupingElement::Composite(exprs) => {
                                    for expr in exprs.iter() {
                                        visit_arena_expression(expr, visitor);
                                    }
                                }
                            }
                        }
                    }
                    vibesql_ast::arena::MixedGroupingItem::GroupingSets(sets) => {
                        for set in sets.iter() {
                            for expr in set.columns.iter() {
                                visit_arena_expression(expr, visitor);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Visit all expressions in an arena expression tree.
fn visit_arena_expression<F>(expr: &Expression<'_>, visitor: &mut F)
where
    F: FnMut(&Expression<'_>),
{
    visitor(expr);

    match expr {
        // Hot-path inline variants
        Expression::BinaryOp { left, right, .. } => {
            visit_arena_expression(left, visitor);
            visit_arena_expression(right, visitor);
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children.iter() {
                visit_arena_expression(child, visitor);
            }
        }
        Expression::UnaryOp { expr: inner, .. } => {
            visit_arena_expression(inner, visitor);
        }
        Expression::IsNull { expr: inner, .. } => {
            visit_arena_expression(inner, visitor);
        }
        // Leaf nodes - no recursion needed
        Expression::Literal(_)
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::ColumnRef { .. }
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default => {}
        // Cold-path extended variants
        Expression::Extended(ext) => match ext {
            ExtendedExpr::Function { args, .. } | ExtendedExpr::AggregateFunction { args, .. } => {
                for arg in args.iter() {
                    visit_arena_expression(arg, visitor);
                }
            }
            ExtendedExpr::Case { operand, when_clauses, else_result } => {
                if let Some(op) = operand {
                    visit_arena_expression(op, visitor);
                }
                for w in when_clauses.iter() {
                    for c in w.conditions.iter() {
                        visit_arena_expression(c, visitor);
                    }
                    visit_arena_expression(&w.result, visitor);
                }
                if let Some(e) = else_result {
                    visit_arena_expression(e, visitor);
                }
            }
            ExtendedExpr::ScalarSubquery(select) => visit_arena_statement(select, visitor),
            ExtendedExpr::In { expr: inner, subquery, .. } => {
                visit_arena_expression(inner, visitor);
                visit_arena_statement(subquery, visitor);
            }
            ExtendedExpr::InList { expr: inner, values, .. } => {
                visit_arena_expression(inner, visitor);
                for v in values.iter() {
                    visit_arena_expression(v, visitor);
                }
            }
            ExtendedExpr::Between { expr: inner, low, high, .. } => {
                visit_arena_expression(inner, visitor);
                visit_arena_expression(low, visitor);
                visit_arena_expression(high, visitor);
            }
            ExtendedExpr::Cast { expr: inner, .. } => {
                visit_arena_expression(inner, visitor);
            }
            ExtendedExpr::Position { substring, string, .. } => {
                visit_arena_expression(substring, visitor);
                visit_arena_expression(string, visitor);
            }
            ExtendedExpr::Trim { removal_char, string, .. } => {
                if let Some(c) = removal_char {
                    visit_arena_expression(c, visitor);
                }
                visit_arena_expression(string, visitor);
            }
            ExtendedExpr::Extract { expr: inner, .. } => {
                visit_arena_expression(inner, visitor);
            }
            ExtendedExpr::Like { expr: inner, pattern, .. } => {
                visit_arena_expression(inner, visitor);
                visit_arena_expression(pattern, visitor);
            }
            ExtendedExpr::Exists { subquery, .. } => {
                visit_arena_statement(subquery, visitor);
            }
            ExtendedExpr::QuantifiedComparison { expr: inner, subquery, .. } => {
                visit_arena_expression(inner, visitor);
                visit_arena_statement(subquery, visitor);
            }
            ExtendedExpr::Interval { value, .. } => {
                visit_arena_expression(value, visitor);
            }
            ExtendedExpr::WindowFunction { function, over } => {
                match function {
                    vibesql_ast::arena::WindowFunctionSpec::Aggregate { args, .. }
                    | vibesql_ast::arena::WindowFunctionSpec::Ranking { args, .. }
                    | vibesql_ast::arena::WindowFunctionSpec::Value { args, .. } => {
                        for arg in args.iter() {
                            visit_arena_expression(arg, visitor);
                        }
                    }
                }
                if let Some(partition_by) = &over.partition_by {
                    for expr in partition_by.iter() {
                        visit_arena_expression(expr, visitor);
                    }
                }
                if let Some(order_by) = &over.order_by {
                    for item in order_by.iter() {
                        visit_arena_expression(&item.expr, visitor);
                    }
                }
            }
            ExtendedExpr::MatchAgainst { search_modifier, .. } => {
                visit_arena_expression(search_modifier, visitor);
            }
            // Extended leaf nodes - no recursion needed
            ExtendedExpr::DuplicateKeyValue { .. }
            | ExtendedExpr::NextValue { .. }
            | ExtendedExpr::PseudoVariable { .. }
            | ExtendedExpr::SessionVariable { .. } => {}
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_prepared_basic() {
        let sql = "SELECT id, name FROM users WHERE id = 1";
        let prepared = ArenaPreparedStatement::new(sql.to_string()).unwrap();

        assert_eq!(prepared.sql(), sql);
        assert_eq!(prepared.param_count(), 0);
        // Arena parser stores table names in uppercase
        assert!(
            prepared.tables().contains("USERS"),
            "Expected 'USERS' in tables {:?}",
            prepared.tables()
        );
    }

    #[test]
    fn test_arena_prepared_with_placeholder() {
        let sql = "SELECT * FROM users WHERE id = ?";
        let prepared = ArenaPreparedStatement::new(sql.to_string()).unwrap();

        assert_eq!(prepared.param_count(), 1);
        // Arena parser stores table names in uppercase
        assert!(prepared.tables().contains("USERS"));
    }

    #[test]
    fn test_arena_prepared_multiple_placeholders() {
        let sql = "SELECT * FROM users WHERE id = ? AND name = ? AND age > ?";
        let prepared = ArenaPreparedStatement::new(sql.to_string()).unwrap();

        assert_eq!(prepared.param_count(), 3);
    }

    #[test]
    fn test_arena_prepared_param_validation() {
        let sql = "SELECT * FROM users WHERE id = ?";
        let prepared = ArenaPreparedStatement::new(sql.to_string()).unwrap();

        // Correct param count should pass
        assert!(prepared.validate_params(&[SqlValue::Integer(1)]).is_ok());

        // Wrong param count should fail
        assert!(prepared.validate_params(&[]).is_err());
        assert!(prepared.validate_params(&[SqlValue::Integer(1), SqlValue::Integer(2)]).is_err());
    }

    #[test]
    fn test_arena_prepared_join_tables() {
        let sql = "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id";
        let prepared = ArenaPreparedStatement::new(sql.to_string()).unwrap();

        let tables = prepared.tables();
        // Arena parser stores table names in uppercase
        assert!(tables.contains("USERS"), "Expected 'USERS' in {:?}", tables);
        assert!(tables.contains("ORDERS"), "Expected 'ORDERS' in {:?}", tables);
    }
}
