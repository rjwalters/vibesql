//! Arena-allocated Expression types.

use bumpalo::collections::Vec as BumpVec;
use vibesql_types::SqlValue;

use crate::{BinaryOperator, UnaryOperator};
use super::SelectStmt;

/// Arena-allocated SQL Expression.
///
/// This is the arena-based version of [`crate::Expression`] where all recursive
/// references use arena allocation instead of `Box`.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression<'arena> {
    /// Literal value (42, 'hello', TRUE, NULL)
    Literal(SqlValue),

    /// Parameter placeholder (?) for prepared statements
    Placeholder(usize),

    /// Numbered parameter placeholder ($1, $2, etc.)
    NumberedPlaceholder(usize),

    /// Named parameter placeholder (:name)
    NamedPlaceholder(&'arena str),

    /// Column reference (id, users.id)
    ColumnRef {
        table: Option<&'arena str>,
        column: &'arena str,
    },

    /// Binary operation (a + b, x = y, etc.)
    /// Note: AND/OR chains should use Conjunction/Disjunction for efficiency
    BinaryOp {
        op: BinaryOperator,
        left: &'arena Expression<'arena>,
        right: &'arena Expression<'arena>,
    },

    /// Flattened conjunction (AND chain): a AND b AND c AND ...
    /// Stored as a flat vector for O(1) depth traversal and better cache locality.
    /// Always contains 2+ children (single predicates remain as-is).
    Conjunction(BumpVec<'arena, Expression<'arena>>),

    /// Flattened disjunction (OR chain): a OR b OR c OR ...
    /// Stored as a flat vector for O(1) depth traversal and better cache locality.
    /// Always contains 2+ children (single predicates remain as-is).
    Disjunction(BumpVec<'arena, Expression<'arena>>),

    /// Unary operation (NOT x, -5)
    UnaryOp {
        op: UnaryOperator,
        expr: &'arena Expression<'arena>,
    },

    /// Function call (UPPER(x), SUBSTRING(x, 1, 3))
    Function {
        name: &'arena str,
        args: BumpVec<'arena, Expression<'arena>>,
        character_unit: Option<CharacterUnit>,
    },

    /// Aggregate function call (COUNT, SUM, AVG, MIN, MAX)
    AggregateFunction {
        name: &'arena str,
        distinct: bool,
        args: BumpVec<'arena, Expression<'arena>>,
    },

    /// IS NULL / IS NOT NULL
    IsNull {
        expr: &'arena Expression<'arena>,
        negated: bool,
    },

    /// Wildcard (*)
    Wildcard,

    /// CASE expression
    Case {
        operand: Option<&'arena Expression<'arena>>,
        when_clauses: BumpVec<'arena, CaseWhen<'arena>>,
        else_result: Option<&'arena Expression<'arena>>,
    },

    /// Scalar subquery
    ScalarSubquery(&'arena SelectStmt<'arena>),

    /// IN operator with subquery
    In {
        expr: &'arena Expression<'arena>,
        subquery: &'arena SelectStmt<'arena>,
        negated: bool,
    },

    /// IN operator with value list
    InList {
        expr: &'arena Expression<'arena>,
        values: BumpVec<'arena, Expression<'arena>>,
        negated: bool,
    },

    /// BETWEEN predicate
    Between {
        expr: &'arena Expression<'arena>,
        low: &'arena Expression<'arena>,
        high: &'arena Expression<'arena>,
        negated: bool,
        symmetric: bool,
    },

    /// CAST expression
    Cast {
        expr: &'arena Expression<'arena>,
        data_type: vibesql_types::DataType,
    },

    /// POSITION expression
    Position {
        substring: &'arena Expression<'arena>,
        string: &'arena Expression<'arena>,
        character_unit: Option<CharacterUnit>,
    },

    /// TRIM expression
    Trim {
        position: Option<TrimPosition>,
        removal_char: Option<&'arena Expression<'arena>>,
        string: &'arena Expression<'arena>,
    },

    /// EXTRACT expression
    Extract {
        field: IntervalUnit,
        expr: &'arena Expression<'arena>,
    },

    /// LIKE pattern matching
    Like {
        expr: &'arena Expression<'arena>,
        pattern: &'arena Expression<'arena>,
        negated: bool,
    },

    /// EXISTS predicate
    Exists {
        subquery: &'arena SelectStmt<'arena>,
        negated: bool,
    },

    /// Quantified comparison (ALL, ANY, SOME)
    QuantifiedComparison {
        expr: &'arena Expression<'arena>,
        op: BinaryOperator,
        quantifier: Quantifier,
        subquery: &'arena SelectStmt<'arena>,
    },

    /// Current date/time functions
    CurrentDate,
    CurrentTime { precision: Option<u32> },
    CurrentTimestamp { precision: Option<u32> },

    /// INTERVAL expression
    Interval {
        value: &'arena Expression<'arena>,
        unit: IntervalUnit,
        leading_precision: Option<u32>,
        fractional_precision: Option<u32>,
    },

    /// DEFAULT keyword
    Default,

    /// VALUES() function for ON DUPLICATE KEY UPDATE
    DuplicateKeyValue { column: &'arena str },

    /// Window function with OVER clause
    WindowFunction {
        function: WindowFunctionSpec<'arena>,
        over: WindowSpec<'arena>,
    },

    /// NEXT VALUE FOR sequence expression
    NextValue { sequence_name: &'arena str },

    /// MATCH...AGAINST full-text search
    MatchAgainst {
        columns: BumpVec<'arena, &'arena str>,
        search_modifier: &'arena Expression<'arena>,
        mode: FulltextMode,
    },

    /// Pseudo-variable reference (OLD/NEW in triggers)
    PseudoVariable {
        pseudo_table: PseudoTable,
        column: &'arena str,
    },

    /// Session/system variable reference
    SessionVariable { name: &'arena str },
}

/// Full-text search mode specification
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FulltextMode {
    NaturalLanguage,
    Boolean,
    QueryExpansion,
}

/// Pseudo-table reference for trigger context
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PseudoTable {
    Old,
    New,
}

/// CASE WHEN clause structure
#[derive(Debug, Clone, PartialEq)]
pub struct CaseWhen<'arena> {
    pub conditions: BumpVec<'arena, Expression<'arena>>,
    pub result: Expression<'arena>,
}

/// Quantifier for quantified comparisons
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Quantifier {
    All,
    Any,
    Some,
}

/// Window function specification
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunctionSpec<'arena> {
    Aggregate {
        name: &'arena str,
        args: BumpVec<'arena, Expression<'arena>>,
    },
    Ranking {
        name: &'arena str,
        args: BumpVec<'arena, Expression<'arena>>,
    },
    Value {
        name: &'arena str,
        args: BumpVec<'arena, Expression<'arena>>,
    },
}

/// Window specification (OVER clause)
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec<'arena> {
    pub partition_by: Option<BumpVec<'arena, Expression<'arena>>>,
    pub order_by: Option<BumpVec<'arena, OrderByItem<'arena>>>,
    pub frame: Option<WindowFrame<'arena>>,
}

/// Window frame specification
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame<'arena> {
    pub unit: FrameUnit,
    pub start: FrameBound<'arena>,
    pub end: Option<FrameBound<'arena>>,
}

/// Frame unit type
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FrameUnit {
    Rows,
    Range,
}

/// Frame boundary specification
#[derive(Debug, Clone, PartialEq)]
pub enum FrameBound<'arena> {
    UnboundedPreceding,
    Preceding(&'arena Expression<'arena>),
    CurrentRow,
    Following(&'arena Expression<'arena>),
    UnboundedFollowing,
}

/// TRIM position specification
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TrimPosition {
    Both,
    Leading,
    Trailing,
}

/// Character measurement unit
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CharacterUnit {
    Characters,
    Octets,
}

/// Interval unit
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntervalUnit {
    Microsecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    SecondMicrosecond,
    MinuteMicrosecond,
    MinuteSecond,
    HourMicrosecond,
    HourSecond,
    HourMinute,
    DayMicrosecond,
    DaySecond,
    DayMinute,
    DayHour,
    YearMonth,
}

/// ORDER BY item
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem<'arena> {
    pub expr: Expression<'arena>,
    pub direction: OrderDirection,
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}
