//! Arena-allocated SELECT statement types.

use bumpalo::collections::Vec as BumpVec;

use super::expression::{Expression, OrderByItem};
use super::interner::Symbol;

/// Common Table Expression (CTE) definition
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr<'arena> {
    pub name: Symbol,
    pub columns: Option<BumpVec<'arena, Symbol>>,
    pub query: &'arena SelectStmt<'arena>,
}

/// Arena-allocated SELECT statement structure
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt<'arena> {
    pub with_clause: Option<BumpVec<'arena, CommonTableExpr<'arena>>>,
    pub distinct: bool,
    pub select_list: BumpVec<'arena, SelectItem<'arena>>,
    pub into_table: Option<Symbol>,
    pub into_variables: Option<BumpVec<'arena, Symbol>>,
    pub from: Option<FromClause<'arena>>,
    pub where_clause: Option<Expression<'arena>>,
    pub group_by: Option<GroupByClause<'arena>>,
    pub having: Option<Expression<'arena>>,
    pub order_by: Option<BumpVec<'arena, OrderByItem<'arena>>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub set_operation: Option<SetOperation<'arena>>,
}

/// GROUP BY clause structure
#[derive(Debug, Clone, PartialEq)]
pub enum GroupByClause<'arena> {
    Simple(BumpVec<'arena, Expression<'arena>>),
    Rollup(BumpVec<'arena, GroupingElement<'arena>>),
    Cube(BumpVec<'arena, GroupingElement<'arena>>),
    GroupingSets(BumpVec<'arena, GroupingSet<'arena>>),
    Mixed(BumpVec<'arena, MixedGroupingItem<'arena>>),
}

/// A single grouping element within ROLLUP or CUBE
#[derive(Debug, Clone, PartialEq)]
pub enum GroupingElement<'arena> {
    Single(Expression<'arena>),
    Composite(BumpVec<'arena, Expression<'arena>>),
}

/// A single grouping set within GROUPING SETS
#[derive(Debug, Clone, PartialEq)]
pub struct GroupingSet<'arena> {
    pub columns: BumpVec<'arena, Expression<'arena>>,
}

/// An item in a mixed GROUP BY clause
#[derive(Debug, Clone, PartialEq)]
pub enum MixedGroupingItem<'arena> {
    Simple(Expression<'arena>),
    Rollup(BumpVec<'arena, GroupingElement<'arena>>),
    Cube(BumpVec<'arena, GroupingElement<'arena>>),
    GroupingSets(BumpVec<'arena, GroupingSet<'arena>>),
}

/// Set operation combining two SELECT statements
#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation<'arena> {
    pub op: SetOperator,
    pub all: bool,
    pub right: &'arena SelectStmt<'arena>,
}

/// Item in the SELECT list
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem<'arena> {
    Wildcard { alias: Option<BumpVec<'arena, Symbol>> },
    QualifiedWildcard { qualifier: Symbol, alias: Option<BumpVec<'arena, Symbol>> },
    Expression { expr: Expression<'arena>, alias: Option<Symbol> },
}

/// FROM clause
#[derive(Debug, Clone, PartialEq)]
pub enum FromClause<'arena> {
    Table {
        name: Symbol,
        alias: Option<Symbol>,
        /// SQL:1999 Feature E051-09: Optional column renaming in table alias
        column_aliases: Option<BumpVec<'arena, Symbol>>,
    },
    Join {
        left: &'arena FromClause<'arena>,
        right: &'arena FromClause<'arena>,
        join_type: JoinType,
        condition: Option<Expression<'arena>>,
        natural: bool,
    },
    Subquery {
        query: &'arena SelectStmt<'arena>,
        alias: Symbol,
        /// SQL:1999 Feature E051-09: Optional column renaming for derived tables
        column_aliases: Option<BumpVec<'arena, Symbol>>,
    },
}

/// JOIN types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
    Semi,
    Anti,
}

/// Set operators for combining SELECT statements
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SetOperator {
    Union,
    Intersect,
    Except,
}
