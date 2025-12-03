//! SELECT statement types
//!
//! This module contains all types related to SELECT queries including
//! SELECT items, FROM clauses, JOINs, and ORDER BY.

use crate::Expression;

// ============================================================================
// Common Table Expressions (CTEs)
// ============================================================================

/// Common Table Expression (CTE) definition
///
/// CTEs are temporary named result sets defined with the WITH clause that exist
/// only for the duration of a single query.
///
/// Example: `WITH regional_sales AS (SELECT region, SUM(amount) FROM orders GROUP BY region)`
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr {
    /// Name of the CTE
    pub name: String,
    /// Optional column name list (e.g., `WITH cte (col1, col2) AS (...)`)
    pub columns: Option<Vec<String>>,
    /// The query defining the CTE
    pub query: Box<SelectStmt>,
}

// ============================================================================
// SELECT Statement
// ============================================================================

/// SELECT statement structure
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    /// Optional WITH clause containing CTEs
    pub with_clause: Option<Vec<CommonTableExpr>>,
    pub distinct: bool,
    pub select_list: Vec<SelectItem>,
    /// Optional INTO clause for DDL SELECT INTO statements (SQL:1999 Feature E111)
    /// Creates a new table from the query results
    pub into_table: Option<String>,
    /// Optional INTO clause for procedural SELECT INTO statements
    /// Stores query results into procedural variables (e.g., SELECT col INTO @var)
    pub into_variables: Option<Vec<String>>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expression>,
    pub group_by: Option<GroupByClause>,
    pub having: Option<Expression>,
    pub order_by: Option<Vec<OrderByItem>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    /// Set operation (UNION, INTERSECT, EXCEPT) combining this query with another
    pub set_operation: Option<SetOperation>,
}

// ============================================================================
// GROUP BY Clause (with ROLLUP, CUBE, GROUPING SETS support)
// ============================================================================

/// GROUP BY clause structure supporting OLAP extensions
///
/// SQL:1999 OLAP extensions allow multi-dimensional aggregation:
/// - Simple: `GROUP BY a, b`
/// - ROLLUP: `GROUP BY ROLLUP(a, b)` - hierarchical subtotals
/// - CUBE: `GROUP BY CUBE(a, b)` - all dimension combinations
/// - GROUPING SETS: `GROUP BY GROUPING SETS((a, b), (a), ())` - explicit groupings
/// - Mixed: `GROUP BY a, ROLLUP(b, c)` - combination of simple and OLAP
#[derive(Debug, Clone, PartialEq)]
pub enum GroupByClause {
    /// Simple GROUP BY with list of expressions
    /// Example: `GROUP BY a, b, c`
    Simple(Vec<Expression>),

    /// ROLLUP creates subtotals that roll up from the most detailed level
    /// to a grand total, following the order of columns specified.
    ///
    /// Example: `GROUP BY ROLLUP(d_year, i_category)`
    /// Equivalent to: `GROUPING SETS ((d_year, i_category), (d_year), ())`
    Rollup(Vec<GroupingElement>),

    /// CUBE creates subtotals for all combinations of dimensions.
    ///
    /// Example: `GROUP BY CUBE(a, b)`
    /// Equivalent to: `GROUPING SETS ((a, b), (a), (b), ())`
    Cube(Vec<GroupingElement>),

    /// GROUPING SETS explicitly specifies which groupings to compute.
    ///
    /// Example: `GROUP BY GROUPING SETS ((d_year, d_moy), (d_year), ())`
    GroupingSets(Vec<GroupingSet>),

    /// Mixed GROUP BY combining simple expressions with ROLLUP/CUBE/GROUPING SETS
    ///
    /// Example: `GROUP BY region, ROLLUP(year, quarter)`
    /// The simple expressions (region) appear in ALL generated grouping sets,
    /// while ROLLUP/CUBE/GROUPING SETS columns are expanded normally.
    ///
    /// `GROUP BY a, ROLLUP(b, c)` is equivalent to:
    /// `GROUPING SETS ((a, b, c), (a, b), (a))`
    ///
    /// Multiple ROLLUP/CUBE create a cross-product of their expansions.
    Mixed(Vec<MixedGroupingItem>),
}

/// A single grouping element within ROLLUP or CUBE
///
/// Can be a single expression or a composite (multiple expressions treated as one unit)
#[derive(Debug, Clone, PartialEq)]
pub enum GroupingElement {
    /// Single expression: `a` in `ROLLUP(a, b)`
    Single(Expression),

    /// Composite: `(a, b)` in `ROLLUP((a, b), c)` - treated as one grouping unit
    Composite(Vec<Expression>),
}

/// A single grouping set within GROUPING SETS
///
/// Example: `(a, b)` or `()` (empty for grand total) in `GROUPING SETS ((a, b), ())`
#[derive(Debug, Clone, PartialEq)]
pub struct GroupingSet {
    pub columns: Vec<Expression>,
}

/// An item in a mixed GROUP BY clause
///
/// Can be a simple expression or a ROLLUP/CUBE/GROUPING SETS construct
#[derive(Debug, Clone, PartialEq)]
pub enum MixedGroupingItem {
    /// Simple expression that appears in all grouping sets
    /// Example: `a` in `GROUP BY a, ROLLUP(b, c)`
    Simple(Expression),

    /// ROLLUP construct
    /// Example: `ROLLUP(b, c)` in `GROUP BY a, ROLLUP(b, c)`
    Rollup(Vec<GroupingElement>),

    /// CUBE construct
    /// Example: `CUBE(b, c)` in `GROUP BY a, CUBE(b, c)`
    Cube(Vec<GroupingElement>),

    /// GROUPING SETS construct
    /// Example: `GROUPING SETS((b), (c))` in `GROUP BY a, GROUPING SETS((b), (c))`
    GroupingSets(Vec<GroupingSet>),
}

impl GroupByClause {
    /// Get all expressions in the GROUP BY clause (flattened)
    ///
    /// This returns all expressions, regardless of ROLLUP/CUBE/GROUPING SETS structure.
    /// Useful for validation and simple GROUP BY processing that doesn't need
    /// the multi-grouping-set semantics.
    pub fn all_expressions(&self) -> Vec<&Expression> {
        match self {
            GroupByClause::Simple(exprs) => exprs.iter().collect(),
            GroupByClause::Rollup(elements) | GroupByClause::Cube(elements) => elements
                .iter()
                .flat_map(|e| match e {
                    GroupingElement::Single(expr) => vec![expr],
                    GroupingElement::Composite(exprs) => exprs.iter().collect(),
                })
                .collect(),
            GroupByClause::GroupingSets(sets) => {
                sets.iter().flat_map(|s| s.columns.iter()).collect()
            }
            GroupByClause::Mixed(items) => items
                .iter()
                .flat_map(|item| match item {
                    MixedGroupingItem::Simple(expr) => vec![expr],
                    MixedGroupingItem::Rollup(elements) | MixedGroupingItem::Cube(elements) => {
                        elements
                            .iter()
                            .flat_map(|e| match e {
                                GroupingElement::Single(expr) => vec![expr],
                                GroupingElement::Composite(exprs) => exprs.iter().collect(),
                            })
                            .collect()
                    }
                    MixedGroupingItem::GroupingSets(sets) => {
                        sets.iter().flat_map(|s| s.columns.iter()).collect()
                    }
                })
                .collect(),
        }
    }

    /// Get the number of expressions (flattened)
    pub fn len(&self) -> usize {
        self.all_expressions().len()
    }

    /// Check if the GROUP BY clause is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check if this is a simple GROUP BY (not ROLLUP/CUBE/GROUPING SETS)
    pub fn is_simple(&self) -> bool {
        matches!(self, GroupByClause::Simple(_))
    }

    /// Get the simple expressions if this is a simple GROUP BY
    pub fn as_simple(&self) -> Option<&Vec<Expression>> {
        match self {
            GroupByClause::Simple(exprs) => Some(exprs),
            _ => None,
        }
    }
}

/// Set operation combining two SELECT statements
#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation {
    pub op: SetOperator,
    pub all: bool, // true = ALL, false = DISTINCT (default)
    pub right: Box<SelectStmt>,
}

/// Item in the SELECT list
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// SELECT * [AS (col1, col2, ...)]
    /// SQL:1999 Feature E051-07: Derived column lists
    Wildcard { alias: Option<Vec<String>> },
    /// SELECT table.* [AS (col1, col2, ...)] or SELECT alias.* [AS (col1, col2, ...)]
    /// SQL:1999 Feature E051-08: Correlation names in FROM clause with derived column lists
    QualifiedWildcard { qualifier: String, alias: Option<Vec<String>> },
    /// SELECT expr [AS alias]
    Expression { expr: Expression, alias: Option<String> },
}

/// FROM clause
#[derive(Debug, Clone, PartialEq)]
pub enum FromClause {
    Table {
        name: String,
        alias: Option<String>,
        /// SQL:1999 Feature E051-09: Optional column renaming in table alias
        /// Example: `FROM t AS myalias (x, y)` renames columns to x, y
        column_aliases: Option<Vec<String>>,
    },
    Join {
        left: Box<FromClause>,
        right: Box<FromClause>,
        join_type: JoinType,
        condition: Option<Expression>,
        /// True for NATURAL JOIN (joins on common column names)
        natural: bool,
    },
    /// Subquery in FROM clause (derived table)
    /// SQL:1999 requires AS alias for derived tables
    /// Example: FROM (SELECT * FROM users WHERE active = TRUE) AS active_users
    /// SQL:1999 Feature E051-09: Optional column renaming
    /// Example: FROM (SELECT a, b FROM t) AS mytemp (x, y)
    Subquery {
        query: Box<SelectStmt>,
        alias: String,
        /// Optional column renaming for derived table columns
        column_aliases: Option<Vec<String>>,
    },
}

/// JOIN types
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
    /// Semi-join: Returns left rows that have a match in right (no duplicates)
    Semi,
    /// Anti-join: Returns left rows that have NO match in right
    Anti,
}

/// ORDER BY item
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expression,
    pub direction: OrderDirection,
}

/// Sort direction
#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

/// Set operators for combining SELECT statements
#[derive(Debug, Clone, PartialEq)]
pub enum SetOperator {
    /// UNION - combines results from two queries, removing duplicates (unless ALL specified)
    Union,
    /// INTERSECT - returns only rows that appear in both queries
    Intersect,
    /// EXCEPT - returns rows from left query that don't appear in right query (SQL standard)
    Except,
}
