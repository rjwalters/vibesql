//! Arena-allocated Expression types.
//!
//! This module uses a two-tier enum design to minimize Expression size:
//! - `Expression` contains common, hot-path variants inline (~48 bytes)
//! - `ExtendedExpr` contains rare, cold-path variants via arena reference
//!
//! This reduces Expression from ~160 bytes to ~48 bytes, allowing ~1.3 nodes
//! per cache line instead of ~0.4, significantly improving traversal performance.

use bumpalo::collections::Vec as BumpVec;
use vibesql_types::SqlValue;

use super::interner::Symbol;
use super::SelectStmt;
use crate::{BinaryOperator, UnaryOperator};

/// Reference to an arena-allocated Expression.
pub type ExprRef<'arena> = &'arena Expression<'arena>;

/// Arena-allocated SQL Expression (hot-path variants).
///
/// This enum contains the most common expression variants inline for optimal
/// cache performance. Rare variants are accessed via `Extended(&ExtendedExpr)`.
///
/// # Size Optimization
///
/// The two-tier design keeps this enum at ~48 bytes (fits in a cache line with
/// room for another node), compared to ~160 bytes if all variants were inline.
///
/// **Hot path (inline)**: Literal, ColumnRef, BinaryOp, UnaryOp, Placeholder, IsNull, Wildcard
/// **Cold path (Extended)**: WindowFunction, Case, Function, AggregateFunction, subqueries, etc.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression<'arena> {
    // === Inline variants (common, hot path) ===
    /// Literal value (42, 'hello', TRUE, NULL)
    /// Most common leaf node in expressions.
    Literal(SqlValue),

    /// Parameter placeholder (?) for prepared statements
    Placeholder(usize),

    /// Numbered parameter placeholder ($1, $2, etc.)
    NumberedPlaceholder(usize),

    /// Named parameter placeholder (:name)
    NamedPlaceholder(Symbol),

    /// Column reference (id, users.id)
    /// Second most common expression type.
    ColumnRef {
        table: Option<Symbol>,
        column: Symbol,
    },

    /// Binary operation (a + b, x = y, etc.)
    /// Note: AND/OR chains should use Conjunction/Disjunction for efficiency
    BinaryOp {
        op: BinaryOperator,
        left: ExprRef<'arena>,
        right: ExprRef<'arena>,
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
        expr: ExprRef<'arena>,
    },

    /// IS NULL / IS NOT NULL
    /// Common predicate in WHERE clauses.
    IsNull {
        expr: ExprRef<'arena>,
        negated: bool,
    },

    /// Wildcard (*)
    Wildcard,

    /// Current date/time functions (no arguments)
    CurrentDate,
    CurrentTime {
        precision: Option<u32>,
    },
    CurrentTimestamp {
        precision: Option<u32>,
    },

    /// DEFAULT keyword
    Default,

    // === Extended variants (rare, cold path) ===
    /// Extended expression variants (arena-allocated separately).
    /// Access via pattern matching or helper methods.
    Extended(&'arena ExtendedExpr<'arena>),
}

/// Extended expression variants (cold path).
///
/// These variants are less common and/or larger in size. They are allocated
/// separately in the arena and referenced via a single pointer from `Expression::Extended`.
///
/// This separation keeps the main `Expression` enum small for better cache utilization
/// during tree traversal, while still supporting the full SQL expression grammar.
#[derive(Debug, Clone, PartialEq)]
pub enum ExtendedExpr<'arena> {
    /// Function call (UPPER(x), SUBSTRING(x, 1, 3))
    Function {
        name: Symbol,
        args: BumpVec<'arena, Expression<'arena>>,
        character_unit: Option<CharacterUnit>,
    },

    /// Aggregate function call (COUNT, SUM, AVG, MIN, MAX)
    AggregateFunction { name: Symbol, distinct: bool, args: BumpVec<'arena, Expression<'arena>> },

    /// CASE expression
    Case {
        operand: Option<ExprRef<'arena>>,
        when_clauses: BumpVec<'arena, CaseWhen<'arena>>,
        else_result: Option<ExprRef<'arena>>,
    },

    /// Scalar subquery
    ScalarSubquery(&'arena SelectStmt<'arena>),

    /// IN operator with subquery
    In { expr: ExprRef<'arena>, subquery: &'arena SelectStmt<'arena>, negated: bool },

    /// IN operator with value list
    InList { expr: ExprRef<'arena>, values: BumpVec<'arena, Expression<'arena>>, negated: bool },

    /// BETWEEN predicate
    Between {
        expr: ExprRef<'arena>,
        low: ExprRef<'arena>,
        high: ExprRef<'arena>,
        negated: bool,
        symmetric: bool,
    },

    /// CAST expression
    Cast { expr: ExprRef<'arena>, data_type: vibesql_types::DataType },

    /// POSITION expression
    Position {
        substring: ExprRef<'arena>,
        string: ExprRef<'arena>,
        character_unit: Option<CharacterUnit>,
    },

    /// TRIM expression
    Trim {
        position: Option<TrimPosition>,
        removal_char: Option<ExprRef<'arena>>,
        string: ExprRef<'arena>,
    },

    /// EXTRACT expression
    Extract { field: IntervalUnit, expr: ExprRef<'arena> },

    /// LIKE pattern matching
    Like { expr: ExprRef<'arena>, pattern: ExprRef<'arena>, negated: bool },

    /// EXISTS predicate
    Exists { subquery: &'arena SelectStmt<'arena>, negated: bool },

    /// Quantified comparison (ALL, ANY, SOME)
    QuantifiedComparison {
        expr: ExprRef<'arena>,
        op: BinaryOperator,
        quantifier: Quantifier,
        subquery: &'arena SelectStmt<'arena>,
    },

    /// INTERVAL expression
    Interval {
        value: ExprRef<'arena>,
        unit: IntervalUnit,
        leading_precision: Option<u32>,
        fractional_precision: Option<u32>,
    },

    /// VALUES() function for ON DUPLICATE KEY UPDATE
    DuplicateKeyValue { column: Symbol },

    /// Window function with OVER clause
    WindowFunction { function: WindowFunctionSpec<'arena>, over: WindowSpec<'arena> },

    /// NEXT VALUE FOR sequence expression
    NextValue { sequence_name: Symbol },

    /// MATCH...AGAINST full-text search
    MatchAgainst {
        columns: BumpVec<'arena, Symbol>,
        search_modifier: ExprRef<'arena>,
        mode: FulltextMode,
    },

    /// Pseudo-variable reference (OLD/NEW in triggers)
    PseudoVariable { pseudo_table: PseudoTable, column: Symbol },

    /// Session/system variable reference
    SessionVariable { name: Symbol },
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
    Aggregate { name: Symbol, args: BumpVec<'arena, Expression<'arena>> },
    Ranking { name: Symbol, args: BumpVec<'arena, Expression<'arena>> },
    Value { name: Symbol, args: BumpVec<'arena, Expression<'arena>> },
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
