//! SQL Expression types for use in SELECT, WHERE, and other clauses

use vibesql_types::SqlValue;

use crate::{BinaryOperator, OrderByItem, SelectStmt, UnaryOperator};

/// SQL Expression (can appear in SELECT, WHERE, etc.)
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// Literal value (42, 'hello', TRUE, NULL)
    Literal(SqlValue),

    /// Parameter placeholder (?) for prepared statements
    /// The index is 0-based and assigned in order of appearance in the SQL
    /// Example: WHERE id = ? AND name = ? -> Placeholder(0), Placeholder(1)
    Placeholder(usize),

    /// Numbered parameter placeholder ($1, $2, etc.) for prepared statements
    /// PostgreSQL-style: 1-indexed as written in SQL
    /// Example: WHERE id = $1 AND name = $2
    /// The number is the explicit position (1-indexed) of the parameter
    NumberedPlaceholder(usize),

    /// Named parameter placeholder (:name) for prepared statements
    /// Used by many ORMs and applications for readability
    /// Example: WHERE id = :user_id AND name = :name
    NamedPlaceholder(String),

    /// Column reference (id, users.id)
    ColumnRef {
        table: Option<String>,
        column: String,
    },

    /// Binary operation (a + b, x = y, etc.)
    /// Note: AND/OR chains should use Conjunction/Disjunction for efficiency
    BinaryOp {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },

    /// Flattened conjunction (AND chain): a AND b AND c AND ...
    /// Stored as a flat vector for O(1) depth traversal and better cache locality.
    /// Always contains 2+ children (single predicates remain as-is).
    Conjunction(Vec<Expression>),

    /// Flattened disjunction (OR chain): a OR b OR c OR ...
    /// Stored as a flat vector for O(1) depth traversal and better cache locality.
    /// Always contains 2+ children (single predicates remain as-is).
    Disjunction(Vec<Expression>),

    /// Unary operation (NOT x, -5)
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },

    /// Function call (UPPER(x), SUBSTRING(x, 1, 3))
    Function {
        name: String,
        args: Vec<Expression>,
        character_unit: Option<CharacterUnit>,
    },

    /// Aggregate function call (COUNT, SUM, AVG, MIN, MAX)
    /// SQL:1999 Section 6.16: Set functions
    /// Example: COUNT(DISTINCT customer_id), SUM(ALL amount)
    AggregateFunction {
        name: String,
        distinct: bool, // true = DISTINCT, false = ALL (implicit)
        args: Vec<Expression>,
    },

    /// IS NULL / IS NOT NULL
    IsNull {
        expr: Box<Expression>,
        negated: bool, // false = IS NULL, true = IS NOT NULL
    },

    /// Wildcard (*)
    Wildcard,

    /// CASE expression (both simple and searched forms)
    /// Simple CASE:  CASE x WHEN 1 THEN 'a' ELSE 'b' END
    /// Searched CASE: CASE WHEN x>0 THEN 'pos' ELSE 'neg' END
    Case {
        /// Operand for simple CASE (None for searched CASE)
        operand: Option<Box<Expression>>,

        /// List of WHEN clauses with conditions and results
        /// - Simple CASE: conditions are comparison values (OR'd together)
        /// - Searched CASE: conditions are boolean expressions (OR'd together)
        when_clauses: Vec<CaseWhen>,

        /// Optional ELSE result (defaults to NULL if None)
        else_result: Option<Box<Expression>>,
    },

    /// Scalar subquery (returns single value)
    /// Example: WHERE salary > (SELECT AVG(salary) FROM employees)
    ScalarSubquery(Box<SelectStmt>),

    /// IN operator with subquery
    /// Example: WHERE id IN (SELECT user_id FROM orders)
    /// Example: WHERE status NOT IN (SELECT blocked_status FROM config)
    In {
        expr: Box<Expression>,
        subquery: Box<SelectStmt>,
        negated: bool, // false = IN, true = NOT IN
    },

    /// IN operator with value list
    /// Example: WHERE id IN (1, 2, 3)
    /// Example: WHERE name NOT IN ('admin', 'root')
    InList {
        expr: Box<Expression>,
        values: Vec<Expression>,
        negated: bool, // false = IN, true = NOT IN
    },

    /// BETWEEN predicate
    /// Example: WHERE age BETWEEN 18 AND 65
    /// Example: WHERE price NOT BETWEEN 10.0 AND 20.0
    /// Example: WHERE value BETWEEN SYMMETRIC 10 AND 1
    /// Equivalent to: expr >= low AND expr <= high (or negated)
    /// SYMMETRIC: swaps bounds if low > high before evaluation
    Between {
        expr: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
        negated: bool,   // false = BETWEEN, true = NOT BETWEEN
        symmetric: bool, // false = ASYMMETRIC (default), true = SYMMETRIC
    },

    /// CAST expression
    /// Example: CAST(value AS INTEGER)
    /// Example: CAST('123' AS NUMERIC(10, 2))
    Cast {
        expr: Box<Expression>,
        data_type: vibesql_types::DataType,
    },

    /// POSITION expression
    /// Example: POSITION('lo' IN 'hello')
    /// Example: POSITION('lo' IN 'hello' USING CHARACTERS)
    /// SQL:1999 Section 6.29: String value functions
    /// Returns 1-indexed position of substring in string, or 0 if not found
    Position {
        substring: Box<Expression>,
        string: Box<Expression>,
        character_unit: Option<CharacterUnit>,
    },

    /// TRIM expression
    /// Example: TRIM(BOTH 'x' FROM 'xxxhelloxxx')
    /// Example: TRIM(LEADING '0' FROM '00042')
    /// Example: TRIM(TRAILING '.' FROM 'test...')
    /// Example: TRIM('x' FROM 'xxxhelloxxx') -- defaults to BOTH
    /// Example: TRIM('  hello  ') -- defaults to BOTH ' '
    /// SQL:1999 Section 6.29: String value functions
    Trim {
        position: Option<TrimPosition>,
        removal_char: Option<Box<Expression>>,
        string: Box<Expression>,
    },

    /// EXTRACT expression
    /// Example: EXTRACT(YEAR FROM date_column)
    /// Example: EXTRACT(MONTH FROM '2024-01-15')
    /// Example: EXTRACT(DAY FROM order_date)
    /// SQL:1999 Section 6.18: Datetime value function
    /// Extracts a date/time field from a datetime or interval expression
    Extract {
        field: IntervalUnit,
        expr: Box<Expression>,
    },

    /// LIKE pattern matching
    /// Example: name LIKE 'John%'
    /// Example: email NOT LIKE '%spam%'
    /// Pattern wildcards: % (any chars), _ (single char)
    Like {
        expr: Box<Expression>,
        pattern: Box<Expression>,
        negated: bool, // false = LIKE, true = NOT LIKE
    },

    /// EXISTS predicate
    /// Example: WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = c.id)
    /// Example: WHERE NOT EXISTS (SELECT 1 FROM orders WHERE customer_id = c.id)
    /// Returns TRUE if subquery returns at least one row, FALSE otherwise
    /// Never returns NULL (unlike most predicates)
    Exists {
        subquery: Box<SelectStmt>,
        negated: bool, // false = EXISTS, true = NOT EXISTS
    },

    /// Quantified comparison (ALL, ANY, SOME)
    /// Example: salary > ALL (SELECT salary FROM dept WHERE dept_id = 10)
    /// Example: price < ANY (SELECT price FROM competitors)
    /// Example: quantity = SOME (SELECT quantity FROM inventory)
    /// SOME is a synonym for ANY
    QuantifiedComparison {
        expr: Box<Expression>,
        op: BinaryOperator,
        quantifier: Quantifier,
        subquery: Box<SelectStmt>,
    },

    /// Current date/time functions
    /// CURRENT_DATE - Returns current date (no precision)
    /// CURRENT_TIME[(precision)] - Returns current time with optional precision
    /// CURRENT_TIMESTAMP[(precision)] - Returns current timestamp with optional precision
    CurrentDate,
    CurrentTime {
        precision: Option<u32>,
    },
    CurrentTimestamp {
        precision: Option<u32>,
    },

    /// INTERVAL expression
    /// Example: INTERVAL '5' DAY
    /// Example: INTERVAL '1-6' YEAR TO MONTH
    /// Example: INTERVAL '5 12:30:45' DAY TO SECOND
    /// SQL:1999 Section 6.12: Interval literal
    Interval {
        value: Box<Expression>,
        unit: IntervalUnit,
        /// For compound intervals: YEAR TO MONTH, DAY TO SECOND, etc.
        leading_precision: Option<u32>,
        fractional_precision: Option<u32>,
    },

    /// DEFAULT keyword - represents default value for column
    /// Used in INSERT and UPDATE statements
    Default,

    /// VALUES() function - references insert value in ON DUPLICATE KEY UPDATE
    /// Used in MySQL ON DUPLICATE KEY UPDATE clause
    /// Example: INSERT INTO t VALUES (1, 2) ON DUPLICATE KEY UPDATE col = VALUES(col)
    DuplicateKeyValue {
        column: String,
    },

    /// Window function with OVER clause
    /// Example: ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)
    /// Example: SUM(amount) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
    /// Applies a function over a window of rows related to the current row
    WindowFunction {
        function: WindowFunctionSpec,
        over: WindowSpec,
    },

    /// NEXT VALUE FOR sequence expression
    /// Example: NEXT VALUE FOR user_id_seq
    /// Returns the next value from the specified sequence generator
    /// SQL:1999 Section 6.13: Next value expression
    NextValue {
        sequence_name: String,
    },

    /// MATCH...AGAINST full-text search expression
    /// Example: MATCH(title, body) AGAINST ('search term')
    /// Example: MATCH(title) AGAINST ('+mysql -oracle' IN BOOLEAN MODE)
    /// Example: MATCH(title) AGAINST ('term' WITH QUERY EXPANSION)
    MatchAgainst {
        /// Columns to search
        columns: Vec<String>,
        /// Search string or phrase
        search_modifier: Box<Expression>,
        /// Search mode specification
        mode: FulltextMode,
    },

    /// Pseudo-variable reference (OLD.column or NEW.column in triggers)
    /// Used in trigger bodies to reference row values before/after DML operations
    /// Example: OLD.username (before update/delete), NEW.username (after insert/update)
    /// SQL:1999 Section 13.1: Triggered action
    PseudoVariable {
        pseudo_table: PseudoTable,
        column: String,
    },

    /// Session/system variable reference (@@sql_mode, @@version, etc.)
    /// MySQL session and system variables using @@ prefix
    /// Example: @@sql_mode, @@session.sql_mode, @@global.max_connections
    /// Can be used in expressions: SELECT REPLACE(@@sql_mode, 'STRICT', '')
    SessionVariable {
        name: String,
    },
}

/// Full-text search mode specification
#[derive(Debug, Clone, PartialEq)]
pub enum FulltextMode {
    /// Natural language mode (default)
    NaturalLanguage,
    /// Boolean mode with operators (+, -, *, etc.)
    Boolean,
    /// Natural language with query expansion
    QueryExpansion,
}

/// Pseudo-table reference for trigger context
/// Used in OLD.column and NEW.column expressions
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PseudoTable {
    /// OLD - references row before UPDATE/DELETE
    Old,
    /// NEW - references row after INSERT/UPDATE
    New,
}

/// CASE WHEN clause structure
/// Supports multiple conditions (OR'd together) per WHEN clause
/// Example: WHEN 1, 2, 3 THEN 'low' means: WHEN x=1 OR x=2 OR x=3 THEN 'low'
#[derive(Debug, Clone, PartialEq)]
pub struct CaseWhen {
    /// Multiple conditions (OR'd together)
    /// For simple CASE: these are comparison values
    /// For searched CASE: these are boolean expressions
    pub conditions: Vec<Expression>,

    /// Result expression when any condition matches
    pub result: Expression,
}

/// Quantifier for quantified comparisons
#[derive(Debug, Clone, PartialEq)]
pub enum Quantifier {
    /// ALL - comparison must be TRUE for all rows
    All,
    /// ANY - comparison must be TRUE for at least one row
    Any,
    /// SOME - synonym for ANY
    Some,
}

/// Window function specification
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunctionSpec {
    /// Aggregate function used as window function
    /// Example: SUM(salary), AVG(price), COUNT(*)
    Aggregate { name: String, args: Vec<Expression> },

    /// Ranking function
    /// Example: ROW_NUMBER(), RANK(), DENSE_RANK(), NTILE(4)
    Ranking { name: String, args: Vec<Expression> },

    /// Value function
    /// Example: LAG(salary, 1), LEAD(price, 2), FIRST_VALUE(name), LAST_VALUE(amount)
    Value { name: String, args: Vec<Expression> },
}

/// Window specification (OVER clause)
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    /// PARTITION BY clause - divides rows into partitions
    /// Example: PARTITION BY department_id
    pub partition_by: Option<Vec<Expression>>,

    /// ORDER BY clause - defines order within each partition
    /// Example: ORDER BY salary DESC, hire_date
    pub order_by: Option<Vec<OrderByItem>>,

    /// Frame clause - defines which rows are included in the window frame
    /// Example: ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    pub frame: Option<WindowFrame>,
}

/// Window frame specification
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    /// Frame unit (ROWS or RANGE)
    pub unit: FrameUnit,

    /// Frame start boundary
    pub start: FrameBound,

    /// Frame end boundary (defaults to CURRENT ROW if None)
    pub end: Option<FrameBound>,
}

/// Frame unit type
#[derive(Debug, Clone, PartialEq)]
pub enum FrameUnit {
    /// ROWS - physical rows
    Rows,
    /// RANGE - logical range based on ORDER BY values
    Range,
}

/// Frame boundary specification
#[derive(Debug, Clone, PartialEq)]
pub enum FrameBound {
    /// UNBOUNDED PRECEDING - start of partition
    UnboundedPreceding,

    /// N PRECEDING - N rows before current row
    Preceding(Box<Expression>),

    /// CURRENT ROW - the current row
    CurrentRow,

    /// N FOLLOWING - N rows after current row
    Following(Box<Expression>),

    /// UNBOUNDED FOLLOWING - end of partition
    UnboundedFollowing,
}

/// TRIM position specification
/// Determines which side(s) to trim characters from
#[derive(Debug, Clone, PartialEq)]
pub enum TrimPosition {
    /// BOTH - trim from both leading and trailing (default)
    Both,
    /// LEADING - trim from start of string only
    Leading,
    /// TRAILING - trim from end of string only
    Trailing,
}

/// Character measurement unit for string functions
/// SQL:1999 Section 6.29: String value functions
/// Used in USING clause for CHARACTER_LENGTH, SUBSTRING, POSITION
#[derive(Debug, Clone, PartialEq)]
pub enum CharacterUnit {
    /// USING CHARACTERS - character-based measurement (default)
    Characters,
    /// USING OCTETS - byte-based measurement
    Octets,
}

/// Interval unit for INTERVAL expressions
/// SQL:1999 Section 6.12: Interval literal
/// Used in INTERVAL '5' DAY, DATE_ADD(), etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IntervalUnit {
    /// Simple units
    Microsecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,

    /// Compound units (for MySQL compatibility)
    /// SECOND_MICROSECOND
    SecondMicrosecond,
    /// MINUTE_MICROSECOND
    MinuteMicrosecond,
    /// MINUTE_SECOND
    MinuteSecond,
    /// HOUR_MICROSECOND
    HourMicrosecond,
    /// HOUR_SECOND
    HourSecond,
    /// HOUR_MINUTE
    HourMinute,
    /// DAY_MICROSECOND
    DayMicrosecond,
    /// DAY_SECOND
    DaySecond,
    /// DAY_MINUTE
    DayMinute,
    /// DAY_HOUR
    DayHour,
    /// YEAR_MONTH
    YearMonth,
}
