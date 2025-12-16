//! SQL pretty-printer for AST nodes
//!
//! This module provides the [`ToSql`] trait for converting AST nodes back to valid SQL strings.
//!
//! # Example
//!
//! ```
//! use vibesql_ast::pretty_print::ToSql;
//! use vibesql_ast::{BinaryOperator, Expression};
//! use vibesql_types::SqlValue;
//!
//! // Convert operators to SQL
//! assert_eq!(BinaryOperator::Equal.to_sql(), "=");
//! assert_eq!(BinaryOperator::GreaterThan.to_sql(), ">");
//!
//! // Convert expressions to SQL
//! let expr = Expression::Literal(SqlValue::Integer(42));
//! assert_eq!(expr.to_sql(), "42");
//! ```

use vibesql_types::{DataType, SqlValue};

use crate::{
    expression::{
        CaseWhen, CharacterUnit, Expression, FrameBound, FrameUnit, FulltextMode, IntervalUnit,
        PseudoTable, Quantifier, TrimPosition, WindowFrame, WindowFunctionSpec, WindowSpec,
    },
    operators::{BinaryOperator, UnaryOperator},
    select::{
        CommonTableExpr, FromClause, GroupByClause, GroupingElement, GroupingSet, JoinType,
        MixedGroupingItem, OrderByItem, OrderDirection, SelectItem, SelectStmt, SetOperation,
        SetOperator,
    },
};

/// Trait for converting AST nodes to SQL strings
pub trait ToSql {
    /// Convert this AST node to a SQL string
    fn to_sql(&self) -> String;
}

// ============================================================================
// Operators
// ============================================================================

impl ToSql for BinaryOperator {
    fn to_sql(&self) -> String {
        match self {
            BinaryOperator::Plus => "+".to_string(),
            BinaryOperator::Minus => "-".to_string(),
            BinaryOperator::Multiply => "*".to_string(),
            BinaryOperator::Divide => "/".to_string(),
            BinaryOperator::IntegerDivide => "DIV".to_string(),
            BinaryOperator::Modulo => "%".to_string(),
            BinaryOperator::Equal => "=".to_string(),
            BinaryOperator::NotEqual => "<>".to_string(),
            BinaryOperator::LessThan => "<".to_string(),
            BinaryOperator::LessThanOrEqual => "<=".to_string(),
            BinaryOperator::GreaterThan => ">".to_string(),
            BinaryOperator::GreaterThanOrEqual => ">=".to_string(),
            BinaryOperator::And => "AND".to_string(),
            BinaryOperator::Or => "OR".to_string(),
            BinaryOperator::Concat => "||".to_string(),
            BinaryOperator::CosineDistance => "<->".to_string(),
            BinaryOperator::NegativeInnerProduct => "<#>".to_string(),
            BinaryOperator::L2Distance => "<=>".to_string(),
        }
    }
}

impl ToSql for UnaryOperator {
    fn to_sql(&self) -> String {
        match self {
            UnaryOperator::Not => "NOT".to_string(),
            UnaryOperator::Minus => "-".to_string(),
            UnaryOperator::Plus => "+".to_string(),
            UnaryOperator::IsNull => "IS NULL".to_string(),
            UnaryOperator::IsNotNull => "IS NOT NULL".to_string(),
        }
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Format a SQL string literal with proper escaping
fn format_string_literal(s: &str) -> String {
    // Escape single quotes by doubling them
    let escaped = s.replace('\'', "''");
    format!("'{}'", escaped)
}

/// Format an identifier, quoting if necessary
fn format_identifier(name: &str) -> String {
    // Check if the identifier needs quoting (contains special chars or is a reserved word)
    let needs_quoting = name.contains(' ')
        || name.contains('-')
        || name.contains('.')
        || name.chars().next().is_some_and(|c| c.is_ascii_digit());

    if needs_quoting {
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        name.to_string()
    }
}

// ============================================================================
// SqlValue
// ============================================================================

impl ToSql for SqlValue {
    fn to_sql(&self) -> String {
        match self {
            SqlValue::Integer(i) => i.to_string(),
            SqlValue::Smallint(i) => i.to_string(),
            SqlValue::Bigint(i) => i.to_string(),
            SqlValue::Unsigned(u) => u.to_string(),
            SqlValue::Numeric(n) => {
                if n.is_nan() || n.is_infinite() {
                    // Can't represent these in SQL, use NULL as fallback
                    "NULL".to_string()
                } else if n.fract() == 0.0 {
                    format!("{:.1}", n) // Show as 1.0 not 1
                } else {
                    n.to_string()
                }
            }
            SqlValue::Float(f) => {
                let f64_val = *f as f64;
                if f64_val.is_nan() || f64_val.is_infinite() {
                    "NULL".to_string()
                } else if f64_val.fract() == 0.0 {
                    format!("{:.1}", f64_val)
                } else {
                    f64_val.to_string()
                }
            }
            SqlValue::Real(r) => {
                let f64_val = *r as f64;
                if f64_val.is_nan() || f64_val.is_infinite() {
                    "NULL".to_string()
                } else if f64_val.fract() == 0.0 {
                    format!("{:.1}", f64_val)
                } else {
                    f64_val.to_string()
                }
            }
            SqlValue::Double(d) => {
                if d.is_nan() || d.is_infinite() {
                    "NULL".to_string()
                } else if d.fract() == 0.0 {
                    format!("{:.1}", d)
                } else {
                    d.to_string()
                }
            }
            SqlValue::Character(s) | SqlValue::Varchar(s) => format_string_literal(s),
            SqlValue::Boolean(true) => "TRUE".to_string(),
            SqlValue::Boolean(false) => "FALSE".to_string(),
            SqlValue::Date(s) => format!("DATE '{}'", s),
            SqlValue::Time(s) => format!("TIME '{}'", s),
            SqlValue::Timestamp(s) => format!("TIMESTAMP '{}'", s),
            SqlValue::Interval(s) => format!("INTERVAL '{}'", s),
            SqlValue::Vector(v) => {
                let formatted: Vec<String> = v.iter().map(|x| x.to_string()).collect();
                format!("[{}]", formatted.join(", "))
            }
            SqlValue::Null => "NULL".to_string(),
        }
    }
}

// ============================================================================
// DataType
// ============================================================================

impl ToSql for DataType {
    fn to_sql(&self) -> String {
        match self {
            DataType::Integer => "INTEGER".to_string(),
            DataType::Smallint => "SMALLINT".to_string(),
            DataType::Bigint => "BIGINT".to_string(),
            DataType::Unsigned => "BIGINT UNSIGNED".to_string(),
            DataType::Numeric { precision, scale } => {
                if *scale == 0 {
                    format!("NUMERIC({})", precision)
                } else {
                    format!("NUMERIC({}, {})", precision, scale)
                }
            }
            DataType::Decimal { precision, scale } => {
                if *scale == 0 {
                    format!("DECIMAL({})", precision)
                } else {
                    format!("DECIMAL({}, {})", precision, scale)
                }
            }
            DataType::Float { precision } => {
                if *precision == 53 {
                    "FLOAT".to_string()
                } else {
                    format!("FLOAT({})", precision)
                }
            }
            DataType::Real => "REAL".to_string(),
            DataType::DoublePrecision => "DOUBLE PRECISION".to_string(),
            DataType::Character { length } => format!("CHARACTER({})", length),
            DataType::Varchar { max_length } => match max_length {
                Some(len) => format!("VARCHAR({})", len),
                None => "VARCHAR".to_string(),
            },
            DataType::CharacterLargeObject => "CLOB".to_string(),
            DataType::Name => "NAME".to_string(),
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Time { with_timezone } => {
                if *with_timezone {
                    "TIME WITH TIME ZONE".to_string()
                } else {
                    "TIME".to_string()
                }
            }
            DataType::Timestamp { with_timezone } => {
                if *with_timezone {
                    "TIMESTAMP WITH TIME ZONE".to_string()
                } else {
                    "TIMESTAMP".to_string()
                }
            }
            DataType::Interval { start_field, end_field } => {
                let start = interval_field_to_sql(start_field);
                match end_field {
                    Some(end) => format!("INTERVAL {} TO {}", start, interval_field_to_sql(end)),
                    None => format!("INTERVAL {}", start),
                }
            }
            DataType::BinaryLargeObject => "BLOB".to_string(),
            DataType::Bit { length } => match length {
                Some(len) => format!("BIT({})", len),
                None => "BIT".to_string(),
            },
            DataType::Vector { dimensions } => format!("VECTOR({})", dimensions),
            DataType::UserDefined { type_name } => type_name.clone(),
            DataType::Null => "NULL".to_string(),
        }
    }
}

fn interval_field_to_sql(field: &vibesql_types::IntervalField) -> &'static str {
    use vibesql_types::IntervalField;
    match field {
        IntervalField::Year => "YEAR",
        IntervalField::Month => "MONTH",
        IntervalField::Day => "DAY",
        IntervalField::Hour => "HOUR",
        IntervalField::Minute => "MINUTE",
        IntervalField::Second => "SECOND",
    }
}

// ============================================================================
// Expression
// ============================================================================

impl ToSql for Expression {
    fn to_sql(&self) -> String {
        match self {
            Expression::Literal(value) => value.to_sql(),

            Expression::Placeholder(_) => "?".to_string(),

            Expression::NumberedPlaceholder(n) => format!("${}", n),

            Expression::NamedPlaceholder(name) => format!(":{}", name),

            Expression::ColumnRef(col_id) => {
                // Use display form which preserves user's original input
                col_id.display().to_string()
            }

            Expression::BinaryOp { op, left, right } => {
                let left_sql = left.to_sql();
                let right_sql = right.to_sql();
                let op_sql = op.to_sql();

                // Add parentheses for nested binary ops to ensure correct precedence
                let left_str =
                    if needs_parens(left, op, true) { format!("({})", left_sql) } else { left_sql };
                let right_str = if needs_parens(right, op, false) {
                    format!("({})", right_sql)
                } else {
                    right_sql
                };

                format!("{} {} {}", left_str, op_sql, right_str)
            }

            Expression::Conjunction(exprs) => {
                let parts: Vec<String> = exprs
                    .iter()
                    .map(|e| {
                        // Parenthesize OR expressions within AND
                        if matches!(e, Expression::Disjunction(_)) {
                            format!("({})", e.to_sql())
                        } else {
                            e.to_sql()
                        }
                    })
                    .collect();
                parts.join(" AND ")
            }

            Expression::Disjunction(exprs) => {
                let parts: Vec<String> = exprs.iter().map(|e| e.to_sql()).collect();
                parts.join(" OR ")
            }

            Expression::UnaryOp { op, expr } => {
                let expr_sql = expr.to_sql();
                match op {
                    UnaryOperator::Not => format!("NOT {}", maybe_paren(&expr_sql, expr)),
                    UnaryOperator::Minus => format!("-{}", maybe_paren(&expr_sql, expr)),
                    UnaryOperator::Plus => format!("+{}", maybe_paren(&expr_sql, expr)),
                    UnaryOperator::IsNull => format!("{} IS NULL", expr_sql),
                    UnaryOperator::IsNotNull => format!("{} IS NOT NULL", expr_sql),
                }
            }

            Expression::Function { name, args, character_unit } => {
                let args_sql: Vec<String> = args.iter().map(|a| a.to_sql()).collect();
                let mut result = format!("{}({})", name.to_uppercase(), args_sql.join(", "));
                if let Some(unit) = character_unit {
                    result.push_str(&format!(" USING {}", unit.to_sql()));
                }
                result
            }

            Expression::AggregateFunction { name, distinct, args, order_by } => {
                let args_sql: Vec<String> = args.iter().map(|a| a.to_sql()).collect();
                let order_by_sql = order_by.as_ref().map(|items| {
                    let items_sql: Vec<String> = items
                        .iter()
                        .map(|item| {
                            let dir = match item.direction {
                                crate::OrderDirection::Asc => "",
                                crate::OrderDirection::Desc => " DESC",
                            };
                            format!("{}{}", item.expr.to_sql(), dir)
                        })
                        .collect();
                    format!(" ORDER BY {}", items_sql.join(", "))
                });
                if *distinct {
                    format!(
                        "{}(DISTINCT {}{})",
                        name.to_uppercase(),
                        args_sql.join(", "),
                        order_by_sql.unwrap_or_default()
                    )
                } else {
                    format!(
                        "{}({}{})",
                        name.to_uppercase(),
                        args_sql.join(", "),
                        order_by_sql.unwrap_or_default()
                    )
                }
            }

            Expression::IsNull { expr, negated } => {
                let expr_sql = expr.to_sql();
                if *negated {
                    format!("{} IS NOT NULL", expr_sql)
                } else {
                    format!("{} IS NULL", expr_sql)
                }
            }

            Expression::IsDistinctFrom { left, right, negated } => {
                let left_sql = left.to_sql();
                let right_sql = right.to_sql();
                if *negated {
                    format!("{} IS NOT DISTINCT FROM {}", left_sql, right_sql)
                } else {
                    format!("{} IS DISTINCT FROM {}", left_sql, right_sql)
                }
            }

            Expression::IsTruthValue { expr, truth_value, negated } => {
                let expr_sql = expr.to_sql();
                let tv_str = match truth_value {
                    crate::TruthValue::True => "TRUE",
                    crate::TruthValue::False => "FALSE",
                    crate::TruthValue::Unknown => "UNKNOWN",
                };
                if *negated {
                    format!("{} IS NOT {}", expr_sql, tv_str)
                } else {
                    format!("{} IS {}", expr_sql, tv_str)
                }
            }

            Expression::Wildcard => "*".to_string(),

            Expression::Case { operand, when_clauses, else_result } => {
                let mut result = "CASE".to_string();
                if let Some(op) = operand {
                    result.push_str(&format!(" {}", op.to_sql()));
                }
                for when in when_clauses {
                    result.push_str(&format!(" {}", when.to_sql()));
                }
                if let Some(else_expr) = else_result {
                    result.push_str(&format!(" ELSE {}", else_expr.to_sql()));
                }
                result.push_str(" END");
                result
            }

            Expression::ScalarSubquery(query) => format!("({})", query.to_sql()),

            Expression::In { expr, subquery, negated } => {
                let not_str = if *negated { "NOT " } else { "" };
                format!("{} {}IN ({})", expr.to_sql(), not_str, subquery.to_sql())
            }

            Expression::InList { expr, values, negated } => {
                let values_sql: Vec<String> = values.iter().map(|v| v.to_sql()).collect();
                let not_str = if *negated { "NOT " } else { "" };
                format!("{} {}IN ({})", expr.to_sql(), not_str, values_sql.join(", "))
            }

            Expression::Between { expr, low, high, negated, symmetric } => {
                let not_str = if *negated { "NOT " } else { "" };
                let sym_str = if *symmetric { "SYMMETRIC " } else { "" };
                format!(
                    "{} {}BETWEEN {}{} AND {}",
                    expr.to_sql(),
                    not_str,
                    sym_str,
                    low.to_sql(),
                    high.to_sql()
                )
            }

            Expression::Cast { expr, data_type } => {
                format!("CAST({} AS {})", expr.to_sql(), data_type.to_sql())
            }

            Expression::Position { substring, string, character_unit } => {
                let mut result = format!("POSITION({} IN {})", substring.to_sql(), string.to_sql());
                if let Some(unit) = character_unit {
                    result.push_str(&format!(" USING {}", unit.to_sql()));
                }
                result
            }

            Expression::Trim { position, removal_char, string } => {
                let mut result = "TRIM(".to_string();
                if let Some(pos) = position {
                    result.push_str(&format!("{} ", pos.to_sql()));
                }
                if let Some(char_expr) = removal_char {
                    result.push_str(&format!("{} FROM ", char_expr.to_sql()));
                }
                result.push_str(&string.to_sql());
                result.push(')');
                result
            }

            Expression::Extract { field, expr } => {
                format!("EXTRACT({} FROM {})", field.to_sql(), expr.to_sql())
            }

            Expression::Like { expr, pattern, negated } => {
                let not_str = if *negated { "NOT " } else { "" };
                format!("{} {}LIKE {}", expr.to_sql(), not_str, pattern.to_sql())
            }

            Expression::Exists { subquery, negated } => {
                let not_str = if *negated { "NOT " } else { "" };
                format!("{}EXISTS ({})", not_str, subquery.to_sql())
            }

            Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
                format!(
                    "{} {} {} ({})",
                    expr.to_sql(),
                    op.to_sql(),
                    quantifier.to_sql(),
                    subquery.to_sql()
                )
            }

            Expression::CurrentDate => "CURRENT_DATE".to_string(),

            Expression::CurrentTime { precision } => match precision {
                Some(p) => format!("CURRENT_TIME({})", p),
                None => "CURRENT_TIME".to_string(),
            },

            Expression::CurrentTimestamp { precision } => match precision {
                Some(p) => format!("CURRENT_TIMESTAMP({})", p),
                None => "CURRENT_TIMESTAMP".to_string(),
            },

            Expression::Interval { value, unit, leading_precision, fractional_precision } => {
                let mut result = format!("INTERVAL {} {}", value.to_sql(), unit.to_sql());
                if let Some(lp) = leading_precision {
                    result.push_str(&format!("({})", lp));
                }
                if let Some(fp) = fractional_precision {
                    result.push_str(&format!("({})", fp));
                }
                result
            }

            Expression::Default => "DEFAULT".to_string(),

            Expression::DuplicateKeyValue { column } => {
                format!("VALUES({})", format_identifier(column))
            }

            Expression::WindowFunction { function, over } => {
                format!("{} OVER ({})", function.to_sql(), over.to_sql())
            }

            Expression::NextValue { sequence_name } => {
                format!("NEXT VALUE FOR {}", format_identifier(sequence_name))
            }

            Expression::MatchAgainst { columns, search_modifier, mode } => {
                let cols_sql: Vec<String> = columns.iter().map(|c| format_identifier(c)).collect();
                let mode_sql = match mode {
                    FulltextMode::NaturalLanguage => "",
                    FulltextMode::Boolean => " IN BOOLEAN MODE",
                    FulltextMode::QueryExpansion => " WITH QUERY EXPANSION",
                };
                format!(
                    "MATCH({}) AGAINST ({}{})",
                    cols_sql.join(", "),
                    search_modifier.to_sql(),
                    mode_sql
                )
            }

            Expression::PseudoVariable { pseudo_table, column } => {
                format!("{}.{}", pseudo_table.to_sql(), format_identifier(column))
            }

            Expression::SessionVariable { name } => format!("@@{}", name),

            Expression::RowValueConstructor(values) => {
                let values_sql: Vec<String> = values.iter().map(|v| v.to_sql()).collect();
                format!("({})", values_sql.join(", "))
            }

            Expression::Collate { expr, collation } => {
                format!("{} COLLATE {}", expr.to_sql(), collation)
            }
        }
    }
}

/// Check if parentheses are needed for a child expression
fn needs_parens(expr: &Expression, parent_op: &BinaryOperator, is_left: bool) -> bool {
    match expr {
        Expression::BinaryOp { op, .. } => {
            let child_prec = operator_precedence(op);
            let parent_prec = operator_precedence(parent_op);

            // Need parens if child has lower precedence
            // Or if same precedence and on the right (for left-associative ops)
            child_prec < parent_prec || (child_prec == parent_prec && !is_left)
        }
        Expression::Conjunction(_) | Expression::Disjunction(_) => true,
        _ => false,
    }
}

/// Get operator precedence (higher = binds tighter)
fn operator_precedence(op: &BinaryOperator) -> u8 {
    match op {
        BinaryOperator::Or => 1,
        BinaryOperator::And => 2,
        BinaryOperator::Equal
        | BinaryOperator::NotEqual
        | BinaryOperator::LessThan
        | BinaryOperator::LessThanOrEqual
        | BinaryOperator::GreaterThan
        | BinaryOperator::GreaterThanOrEqual
        | BinaryOperator::CosineDistance
        | BinaryOperator::NegativeInnerProduct
        | BinaryOperator::L2Distance => 3,
        BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Concat => 4,
        BinaryOperator::Multiply
        | BinaryOperator::Divide
        | BinaryOperator::IntegerDivide
        | BinaryOperator::Modulo => 5,
    }
}

/// Maybe wrap expression in parentheses if it's complex
fn maybe_paren(sql: &str, expr: &Expression) -> String {
    match expr {
        Expression::BinaryOp { .. }
        | Expression::Conjunction(_)
        | Expression::Disjunction(_)
        | Expression::UnaryOp { .. } => format!("({})", sql),
        _ => sql.to_string(),
    }
}

// ============================================================================
// Expression helper types
// ============================================================================

impl ToSql for CaseWhen {
    fn to_sql(&self) -> String {
        let conds: Vec<String> = self.conditions.iter().map(|c| c.to_sql()).collect();
        format!("WHEN {} THEN {}", conds.join(", "), self.result.to_sql())
    }
}

impl ToSql for Quantifier {
    fn to_sql(&self) -> String {
        match self {
            Quantifier::All => "ALL".to_string(),
            Quantifier::Any => "ANY".to_string(),
            Quantifier::Some => "SOME".to_string(),
        }
    }
}

impl ToSql for CharacterUnit {
    fn to_sql(&self) -> String {
        match self {
            CharacterUnit::Characters => "CHARACTERS".to_string(),
            CharacterUnit::Octets => "OCTETS".to_string(),
        }
    }
}

impl ToSql for TrimPosition {
    fn to_sql(&self) -> String {
        match self {
            TrimPosition::Both => "BOTH".to_string(),
            TrimPosition::Leading => "LEADING".to_string(),
            TrimPosition::Trailing => "TRAILING".to_string(),
        }
    }
}

impl ToSql for IntervalUnit {
    fn to_sql(&self) -> String {
        match self {
            IntervalUnit::Microsecond => "MICROSECOND".to_string(),
            IntervalUnit::Second => "SECOND".to_string(),
            IntervalUnit::Minute => "MINUTE".to_string(),
            IntervalUnit::Hour => "HOUR".to_string(),
            IntervalUnit::Day => "DAY".to_string(),
            IntervalUnit::Week => "WEEK".to_string(),
            IntervalUnit::Month => "MONTH".to_string(),
            IntervalUnit::Quarter => "QUARTER".to_string(),
            IntervalUnit::Year => "YEAR".to_string(),
            IntervalUnit::SecondMicrosecond => "SECOND_MICROSECOND".to_string(),
            IntervalUnit::MinuteMicrosecond => "MINUTE_MICROSECOND".to_string(),
            IntervalUnit::MinuteSecond => "MINUTE_SECOND".to_string(),
            IntervalUnit::HourMicrosecond => "HOUR_MICROSECOND".to_string(),
            IntervalUnit::HourSecond => "HOUR_SECOND".to_string(),
            IntervalUnit::HourMinute => "HOUR_MINUTE".to_string(),
            IntervalUnit::DayMicrosecond => "DAY_MICROSECOND".to_string(),
            IntervalUnit::DaySecond => "DAY_SECOND".to_string(),
            IntervalUnit::DayMinute => "DAY_MINUTE".to_string(),
            IntervalUnit::DayHour => "DAY_HOUR".to_string(),
            IntervalUnit::YearMonth => "YEAR_MONTH".to_string(),
        }
    }
}

impl ToSql for PseudoTable {
    fn to_sql(&self) -> String {
        match self {
            PseudoTable::Old => "OLD".to_string(),
            PseudoTable::New => "NEW".to_string(),
        }
    }
}

impl ToSql for WindowFunctionSpec {
    fn to_sql(&self) -> String {
        match self {
            WindowFunctionSpec::Aggregate { name, args } => {
                let args_sql: Vec<String> = args.iter().map(|a| a.to_sql()).collect();
                format!("{}({})", name.to_uppercase(), args_sql.join(", "))
            }
            WindowFunctionSpec::Ranking { name, args } => {
                let args_sql: Vec<String> = args.iter().map(|a| a.to_sql()).collect();
                if args_sql.is_empty() {
                    format!("{}()", name.to_uppercase())
                } else {
                    format!("{}({})", name.to_uppercase(), args_sql.join(", "))
                }
            }
            WindowFunctionSpec::Value { name, args } => {
                let args_sql: Vec<String> = args.iter().map(|a| a.to_sql()).collect();
                format!("{}({})", name.to_uppercase(), args_sql.join(", "))
            }
        }
    }
}

impl ToSql for WindowSpec {
    fn to_sql(&self) -> String {
        let mut parts = Vec::new();

        if let Some(partition_by) = &self.partition_by {
            let exprs: Vec<String> = partition_by.iter().map(|e| e.to_sql()).collect();
            parts.push(format!("PARTITION BY {}", exprs.join(", ")));
        }

        if let Some(order_by) = &self.order_by {
            let items: Vec<String> = order_by.iter().map(|o| o.to_sql()).collect();
            parts.push(format!("ORDER BY {}", items.join(", ")));
        }

        if let Some(frame) = &self.frame {
            parts.push(frame.to_sql());
        }

        parts.join(" ")
    }
}

impl ToSql for WindowFrame {
    fn to_sql(&self) -> String {
        let unit = match self.unit {
            FrameUnit::Rows => "ROWS",
            FrameUnit::Range => "RANGE",
        };

        match &self.end {
            Some(end) => {
                format!("{} BETWEEN {} AND {}", unit, self.start.to_sql(), end.to_sql())
            }
            None => format!("{} {}", unit, self.start.to_sql()),
        }
    }
}

impl ToSql for FrameBound {
    fn to_sql(&self) -> String {
        match self {
            FrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
            FrameBound::Preceding(n) => format!("{} PRECEDING", n.to_sql()),
            FrameBound::CurrentRow => "CURRENT ROW".to_string(),
            FrameBound::Following(n) => format!("{} FOLLOWING", n.to_sql()),
            FrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
        }
    }
}

// ============================================================================
// SELECT Statement
// ============================================================================

impl ToSql for SelectStmt {
    fn to_sql(&self) -> String {
        let mut result = String::new();

        // WITH clause
        if let Some(ctes) = &self.with_clause {
            let cte_strs: Vec<String> = ctes.iter().map(|c| c.to_sql()).collect();
            result.push_str(&format!("WITH {} ", cte_strs.join(", ")));
        }

        // Check if this is a VALUES statement
        if let Some(rows) = &self.values {
            // VALUES clause
            result.push_str("VALUES");
            let row_strs: Vec<String> = rows
                .iter()
                .map(|row| {
                    let exprs: Vec<String> = row.iter().map(|e| e.to_sql()).collect();
                    format!("({})", exprs.join(", "))
                })
                .collect();
            result.push_str(&row_strs.join(", "));
        } else {
            // SELECT
            result.push_str("SELECT ");

            // DISTINCT
            if self.distinct {
                result.push_str("DISTINCT ");
            }

            // Select list
            let items: Vec<String> = self.select_list.iter().map(|i| i.to_sql()).collect();
            result.push_str(&items.join(", "));

            // INTO clause (DDL)
            if let Some(table) = &self.into_table {
                result.push_str(&format!(" INTO {}", format_identifier(table)));
            }

            // INTO variables (procedural)
            if let Some(vars) = &self.into_variables {
                result.push_str(&format!(" INTO {}", vars.join(", ")));
            }

            // FROM
            if let Some(from) = &self.from {
                result.push_str(&format!(" FROM {}", from.to_sql()));
            }

            // WHERE
            if let Some(where_clause) = &self.where_clause {
                result.push_str(&format!(" WHERE {}", where_clause.to_sql()));
            }

            // GROUP BY
            if let Some(group_by) = &self.group_by {
                result.push_str(&format!(" GROUP BY {}", group_by.to_sql()));
            }

            // HAVING
            if let Some(having) = &self.having {
                result.push_str(&format!(" HAVING {}", having.to_sql()));
            }
        }

        // ORDER BY (applies to both SELECT and VALUES)
        if let Some(order_by) = &self.order_by {
            let items: Vec<String> = order_by.iter().map(|o| o.to_sql()).collect();
            result.push_str(&format!(" ORDER BY {}", items.join(", ")));
        }

        // LIMIT
        if let Some(limit) = &self.limit {
            result.push_str(&format!(" LIMIT {}", limit.to_sql()));
        }

        // OFFSET
        if let Some(offset) = &self.offset {
            result.push_str(&format!(" OFFSET {}", offset.to_sql()));
        }

        // Set operation
        if let Some(set_op) = &self.set_operation {
            result.push_str(&format!(" {}", set_op.to_sql()));
        }

        result
    }
}

impl ToSql for CommonTableExpr {
    fn to_sql(&self) -> String {
        let mut result = format_identifier(&self.name);
        if let Some(cols) = &self.columns {
            result.push_str(&format!(" ({})", cols.join(", ")));
        }
        result.push_str(&format!(" AS ({})", self.query.to_sql()));
        result
    }
}

impl ToSql for SelectItem {
    fn to_sql(&self) -> String {
        match self {
            SelectItem::Wildcard { alias } => match alias {
                Some(cols) => format!("* AS ({})", cols.join(", ")),
                None => "*".to_string(),
            },
            SelectItem::QualifiedWildcard { qualifier, alias } => {
                let base = format!("{}.*", format_identifier(qualifier));
                match alias {
                    Some(cols) => format!("{} AS ({})", base, cols.join(", ")),
                    None => base,
                }
            }
            SelectItem::Expression { expr, alias, .. } => {
                let expr_sql = expr.to_sql();
                match alias {
                    Some(a) => format!("{} AS {}", expr_sql, format_identifier(a)),
                    None => expr_sql,
                }
            }
        }
    }
}

impl ToSql for FromClause {
    fn to_sql(&self) -> String {
        match self {
            FromClause::Table { name, alias, column_aliases, quoted } => {
                // Format name with quotes if it was originally quoted
                let mut result = if *quoted {
                    format!("\"{}\"", name.replace('"', "\"\""))
                } else {
                    format_identifier(name)
                };
                if let Some(a) = alias {
                    result.push_str(&format!(" AS {}", format_identifier(a)));
                    if let Some(cols) = column_aliases {
                        result.push_str(&format!(" ({})", cols.join(", ")));
                    }
                }
                result
            }
            FromClause::Join { left, right, join_type, condition, using_columns, natural } => {
                let mut result = left.to_sql();

                if *natural {
                    result.push_str(" NATURAL");
                }

                result.push_str(&format!(" {} {}", join_type.to_sql(), right.to_sql()));

                if let Some(cond) = condition {
                    result.push_str(&format!(" ON {}", cond.to_sql()));
                }

                if let Some(cols) = using_columns {
                    result.push_str(&format!(" USING ({})", cols.join(", ")));
                }

                result
            }
            FromClause::Subquery { query, alias, column_aliases } => {
                let mut result = format!("({}) AS {}", query.to_sql(), format_identifier(alias));
                if let Some(cols) = column_aliases {
                    result.push_str(&format!(" ({})", cols.join(", ")));
                }
                result
            }
            FromClause::Values { rows, alias, column_aliases } => {
                let rows_sql: Vec<String> = rows
                    .iter()
                    .map(|row| {
                        let cols: Vec<String> = row.iter().map(|e| e.to_sql()).collect();
                        format!("({})", cols.join(", "))
                    })
                    .collect();
                let mut result =
                    format!("(VALUES {}) AS {}", rows_sql.join(", "), format_identifier(alias));
                if let Some(cols) = column_aliases {
                    result.push_str(&format!(" ({})", cols.join(", ")));
                }
                result
            }
        }
    }
}

impl ToSql for JoinType {
    fn to_sql(&self) -> String {
        match self {
            JoinType::Inner => "INNER JOIN".to_string(),
            JoinType::LeftOuter => "LEFT OUTER JOIN".to_string(),
            JoinType::RightOuter => "RIGHT OUTER JOIN".to_string(),
            JoinType::FullOuter => "FULL OUTER JOIN".to_string(),
            JoinType::Cross => "CROSS JOIN".to_string(),
            JoinType::Semi => "SEMI JOIN".to_string(),
            JoinType::Anti => "ANTI JOIN".to_string(),
        }
    }
}

impl ToSql for OrderByItem {
    fn to_sql(&self) -> String {
        format!("{} {}", self.expr.to_sql(), self.direction.to_sql())
    }
}

impl ToSql for OrderDirection {
    fn to_sql(&self) -> String {
        match self {
            OrderDirection::Asc => "ASC".to_string(),
            OrderDirection::Desc => "DESC".to_string(),
        }
    }
}

impl ToSql for SetOperation {
    fn to_sql(&self) -> String {
        let op = self.op.to_sql();
        let all_str = if self.all { " ALL" } else { "" };
        format!("{}{} {}", op, all_str, self.right.to_sql())
    }
}

impl ToSql for SetOperator {
    fn to_sql(&self) -> String {
        match self {
            SetOperator::Union => "UNION".to_string(),
            SetOperator::Intersect => "INTERSECT".to_string(),
            SetOperator::Except => "EXCEPT".to_string(),
        }
    }
}

// ============================================================================
// GROUP BY
// ============================================================================

impl ToSql for GroupByClause {
    fn to_sql(&self) -> String {
        match self {
            GroupByClause::Simple(exprs) => {
                let strs: Vec<String> = exprs.iter().map(|e| e.to_sql()).collect();
                strs.join(", ")
            }
            GroupByClause::Rollup(elements) => {
                let strs: Vec<String> = elements.iter().map(|e| e.to_sql()).collect();
                format!("ROLLUP({})", strs.join(", "))
            }
            GroupByClause::Cube(elements) => {
                let strs: Vec<String> = elements.iter().map(|e| e.to_sql()).collect();
                format!("CUBE({})", strs.join(", "))
            }
            GroupByClause::GroupingSets(sets) => {
                let strs: Vec<String> = sets.iter().map(|s| s.to_sql()).collect();
                format!("GROUPING SETS({})", strs.join(", "))
            }
            GroupByClause::Mixed(items) => {
                let strs: Vec<String> = items.iter().map(|i| i.to_sql()).collect();
                strs.join(", ")
            }
        }
    }
}

impl ToSql for GroupingElement {
    fn to_sql(&self) -> String {
        match self {
            GroupingElement::Single(expr) => expr.to_sql(),
            GroupingElement::Composite(exprs) => {
                let strs: Vec<String> = exprs.iter().map(|e| e.to_sql()).collect();
                format!("({})", strs.join(", "))
            }
        }
    }
}

impl ToSql for GroupingSet {
    fn to_sql(&self) -> String {
        if self.columns.is_empty() {
            "()".to_string()
        } else {
            let strs: Vec<String> = self.columns.iter().map(|e| e.to_sql()).collect();
            format!("({})", strs.join(", "))
        }
    }
}

impl ToSql for MixedGroupingItem {
    fn to_sql(&self) -> String {
        match self {
            MixedGroupingItem::Simple(expr) => expr.to_sql(),
            MixedGroupingItem::Rollup(elements) => {
                let strs: Vec<String> = elements.iter().map(|e| e.to_sql()).collect();
                format!("ROLLUP({})", strs.join(", "))
            }
            MixedGroupingItem::Cube(elements) => {
                let strs: Vec<String> = elements.iter().map(|e| e.to_sql()).collect();
                format!("CUBE({})", strs.join(", "))
            }
            MixedGroupingItem::GroupingSets(sets) => {
                let strs: Vec<String> = sets.iter().map(|s| s.to_sql()).collect();
                format!("GROUPING SETS({})", strs.join(", "))
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnIdentifier;

    #[test]
    fn test_binary_operators() {
        assert_eq!(BinaryOperator::Plus.to_sql(), "+");
        assert_eq!(BinaryOperator::Equal.to_sql(), "=");
        assert_eq!(BinaryOperator::And.to_sql(), "AND");
        assert_eq!(BinaryOperator::Or.to_sql(), "OR");
        assert_eq!(BinaryOperator::Concat.to_sql(), "||");
    }

    #[test]
    fn test_unary_operators() {
        assert_eq!(UnaryOperator::Not.to_sql(), "NOT");
        assert_eq!(UnaryOperator::Minus.to_sql(), "-");
        assert_eq!(UnaryOperator::IsNull.to_sql(), "IS NULL");
    }

    #[test]
    fn test_sql_values() {
        assert_eq!(SqlValue::Integer(42).to_sql(), "42");
        assert_eq!(SqlValue::Varchar("hello".into()).to_sql(), "'hello'");
        assert_eq!(SqlValue::Varchar("it's".into()).to_sql(), "'it''s'");
        assert_eq!(SqlValue::Boolean(true).to_sql(), "TRUE");
        assert_eq!(SqlValue::Null.to_sql(), "NULL");
    }

    #[test]
    fn test_data_types() {
        assert_eq!(DataType::Integer.to_sql(), "INTEGER");
        assert_eq!(DataType::Varchar { max_length: Some(255) }.to_sql(), "VARCHAR(255)");
        assert_eq!(DataType::Numeric { precision: 10, scale: 2 }.to_sql(), "NUMERIC(10, 2)");
        assert_eq!(DataType::Boolean.to_sql(), "BOOLEAN");
    }

    #[test]
    fn test_column_ref() {
        let expr = Expression::ColumnRef(ColumnIdentifier::simple("id", false));
        assert_eq!(expr.to_sql(), "id");

        let expr =
            Expression::ColumnRef(ColumnIdentifier::qualified("users", false, "name", false));
        assert_eq!(expr.to_sql(), "users.name");
    }

    #[test]
    fn test_binary_op() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("id", false))),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        };
        assert_eq!(expr.to_sql(), "id = 1");
    }

    #[test]
    fn test_simple_select() {
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::ColumnRef(ColumnIdentifier::simple("id", false)),
                alias: None,
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };
        assert_eq!(stmt.to_sql(), "SELECT id FROM users");
    }

    #[test]
    fn test_select_with_where() {
        let stmt = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![
                SelectItem::Expression {
                    expr: Expression::ColumnRef(ColumnIdentifier::simple("id", false)),
                    alias: None,
                    source_text: None,
                },
                SelectItem::Expression {
                    expr: Expression::ColumnRef(ColumnIdentifier::simple("name", false)),
                    alias: None,
                    source_text: None,
                },
            ],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("active", false))),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };
        assert_eq!(stmt.to_sql(), "SELECT id, name FROM users WHERE active = 1");
    }

    #[test]
    fn test_select_distinct_with_order() {
        let stmt = SelectStmt {
            with_clause: None,
            distinct: true,
            select_list: vec![SelectItem::Expression {
                expr: Expression::ColumnRef(ColumnIdentifier::simple("name", false)),
                alias: None,
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "users".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: Some(vec![OrderByItem {
                expr: Expression::ColumnRef(ColumnIdentifier::simple("name", false)),
                direction: OrderDirection::Asc,
                nulls_order: None,
            }]),
            limit: Some(Expression::Literal(SqlValue::Integer(10))),
            offset: None,
            set_operation: None,
            values: None,
        };
        assert_eq!(stmt.to_sql(), "SELECT DISTINCT name FROM users ORDER BY name ASC LIMIT 10");
    }

    #[test]
    fn test_join() {
        let from = FromClause::Join {
            left: Box::new(FromClause::Table {
                name: "orders".to_string(),
                alias: Some("o".to_string()),
                column_aliases: None,
                quoted: false,
            }),
            right: Box::new(FromClause::Table {
                name: "customers".to_string(),
                alias: Some("c".to_string()),
                column_aliases: None,
                quoted: false,
            }),
            join_type: JoinType::Inner,
            condition: Some(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef(ColumnIdentifier::qualified("o", false, "customer_id", false))),
                right: Box::new(Expression::ColumnRef(ColumnIdentifier::qualified("c", false, "id", false))),
            }),
            using_columns: None,
            natural: false,
        };

        assert_eq!(from.to_sql(), "orders AS o INNER JOIN customers AS c ON o.customer_id = c.id");
    }

    #[test]
    fn test_aggregate_function() {
        let expr = Expression::AggregateFunction {
            name: "count".to_string(),
            distinct: true,
            args: vec![Expression::ColumnRef(ColumnIdentifier::simple("id", false))],
            order_by: None,
        };
        assert_eq!(expr.to_sql(), "COUNT(DISTINCT id)");
    }

    #[test]
    fn test_case_expression() {
        let expr = Expression::Case {
            operand: None,
            when_clauses: vec![CaseWhen {
                conditions: vec![Expression::BinaryOp {
                    op: BinaryOperator::GreaterThan,
                    left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("x", false))),
                    right: Box::new(Expression::Literal(SqlValue::Integer(0))),
                }],
                result: Expression::Literal(SqlValue::Varchar("positive".into())),
            }],
            else_result: Some(Box::new(Expression::Literal(SqlValue::Varchar(
                "non-positive".into(),
            )))),
        };
        assert_eq!(expr.to_sql(), "CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END");
    }

    #[test]
    fn test_in_list() {
        let expr = Expression::InList {
            expr: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("id", false))),
            values: vec![
                Expression::Literal(SqlValue::Integer(1)),
                Expression::Literal(SqlValue::Integer(2)),
                Expression::Literal(SqlValue::Integer(3)),
            ],
            negated: false,
        };
        assert_eq!(expr.to_sql(), "id IN (1, 2, 3)");
    }

    #[test]
    fn test_between() {
        let expr = Expression::Between {
            expr: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("age", false))),
            low: Box::new(Expression::Literal(SqlValue::Integer(18))),
            high: Box::new(Expression::Literal(SqlValue::Integer(65))),
            negated: false,
            symmetric: false,
        };
        assert_eq!(expr.to_sql(), "age BETWEEN 18 AND 65");
    }

    #[test]
    fn test_group_by_rollup() {
        let group_by = GroupByClause::Rollup(vec![
            GroupingElement::Single(Expression::ColumnRef(ColumnIdentifier::simple("year", false))),
            GroupingElement::Single(Expression::ColumnRef(ColumnIdentifier::simple("month", false))),
        ]);
        assert_eq!(group_by.to_sql(), "ROLLUP(year, month)");
    }

    #[test]
    fn test_window_function() {
        let expr = Expression::WindowFunction {
            function: WindowFunctionSpec::Ranking { name: "row_number".to_string(), args: vec![] },
            over: WindowSpec {
                partition_by: Some(vec![Expression::ColumnRef(ColumnIdentifier::simple("dept", false))]),
                order_by: Some(vec![OrderByItem {
                    expr: Expression::ColumnRef(ColumnIdentifier::simple("salary", false)),
                    direction: OrderDirection::Desc,
                    nulls_order: None,
                }]),
                frame: None,
            },
        };
        assert_eq!(expr.to_sql(), "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)");
    }
}
