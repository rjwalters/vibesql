//! Conversion from arena-allocated AST types to standard (heap-allocated) types.
//!
//! This module provides `From` implementations to convert arena-based AST nodes
//! to their standard equivalents. This enables using the faster arena parser
//! while still producing AST compatible with the existing executor/planner.
//!
//! # Performance
//!
//! While conversion incurs allocation overhead, the overall performance is still
//! better than the standard parser because:
//! - Arena parsing is 30-40% faster than standard parsing
//! - Conversion allocates fewer, larger chunks (better cache locality)
//! - Many string values are small and benefit from SSO (Small String Optimization)

use crate::{
    CaseWhen, CharacterUnit, CommonTableExpr, Expression, FrameBound, FrameUnit, FromClause,
    FulltextMode, GroupByClause, GroupingElement, GroupingSet, IntervalUnit, JoinType,
    MixedGroupingItem, OrderByItem, OrderDirection, PseudoTable, Quantifier, SelectItem,
    SelectStmt, SetOperation, SetOperator, TrimPosition, WindowFrame, WindowFunctionSpec,
    WindowSpec,
};

use super::{
    CaseWhen as ArenaCaseWhen,
    CharacterUnit as ArenaCharacterUnit, CommonTableExpr as ArenaCommonTableExpr,
    Expression as ArenaExpression, FrameBound as ArenaFrameBound, FrameUnit as ArenaFrameUnit,
    FromClause as ArenaFromClause, FulltextMode as ArenaFulltextMode,
    GroupByClause as ArenaGroupByClause, GroupingElement as ArenaGroupingElement,
    GroupingSet as ArenaGroupingSet, IntervalUnit as ArenaIntervalUnit, JoinType as ArenaJoinType,
    MixedGroupingItem as ArenaMixedGroupingItem, OrderByItem as ArenaOrderByItem,
    OrderDirection as ArenaOrderDirection, PseudoTable as ArenaPseudoTable,
    Quantifier as ArenaQuantifier, SelectItem as ArenaSelectItem, SelectStmt as ArenaSelectStmt,
    SetOperation as ArenaSetOperation, SetOperator as ArenaSetOperator,
    TrimPosition as ArenaTrimPosition, WindowFrame as ArenaWindowFrame,
    WindowFunctionSpec as ArenaWindowFunctionSpec, WindowSpec as ArenaWindowSpec,
};

// ============================================================================
// Expression Conversion
// ============================================================================

impl<'arena> From<&ArenaExpression<'arena>> for Expression {
    fn from(expr: &ArenaExpression<'arena>) -> Self {
        match expr {
            ArenaExpression::Literal(v) => Expression::Literal(v.clone()),
            ArenaExpression::Placeholder(i) => Expression::Placeholder(*i),
            ArenaExpression::NumberedPlaceholder(i) => Expression::NumberedPlaceholder(*i),
            ArenaExpression::NamedPlaceholder(s) => Expression::NamedPlaceholder((*s).to_string()),
            ArenaExpression::ColumnRef { table, column } => Expression::ColumnRef {
                table: table.map(|s| s.to_string()),
                column: (*column).to_string(),
            },
            ArenaExpression::BinaryOp { op, left, right } => Expression::BinaryOp {
                op: *op,
                left: Box::new(Expression::from(*left)),
                right: Box::new(Expression::from(*right)),
            },
            ArenaExpression::UnaryOp { op, expr } => Expression::UnaryOp {
                op: *op,
                expr: Box::new(Expression::from(*expr)),
            },
            ArenaExpression::Function {
                name,
                args,
                character_unit,
            } => Expression::Function {
                name: (*name).to_string(),
                args: args.iter().map(Expression::from).collect(),
                character_unit: character_unit.map(CharacterUnit::from),
            },
            ArenaExpression::AggregateFunction {
                name,
                distinct,
                args,
            } => Expression::AggregateFunction {
                name: (*name).to_string(),
                distinct: *distinct,
                args: args.iter().map(Expression::from).collect(),
            },
            ArenaExpression::IsNull { expr, negated } => Expression::IsNull {
                expr: Box::new(Expression::from(*expr)),
                negated: *negated,
            },
            ArenaExpression::Wildcard => Expression::Wildcard,
            ArenaExpression::Case {
                operand,
                when_clauses,
                else_result,
            } => Expression::Case {
                operand: operand.map(|e| Box::new(Expression::from(e))),
                when_clauses: when_clauses.iter().map(CaseWhen::from).collect(),
                else_result: else_result.map(|e| Box::new(Expression::from(e))),
            },
            ArenaExpression::ScalarSubquery(q) => {
                Expression::ScalarSubquery(Box::new(SelectStmt::from(*q)))
            }
            ArenaExpression::In {
                expr,
                subquery,
                negated,
            } => Expression::In {
                expr: Box::new(Expression::from(*expr)),
                subquery: Box::new(SelectStmt::from(*subquery)),
                negated: *negated,
            },
            ArenaExpression::InList {
                expr,
                values,
                negated,
            } => Expression::InList {
                expr: Box::new(Expression::from(*expr)),
                values: values.iter().map(Expression::from).collect(),
                negated: *negated,
            },
            ArenaExpression::Between {
                expr,
                low,
                high,
                negated,
                symmetric,
            } => Expression::Between {
                expr: Box::new(Expression::from(*expr)),
                low: Box::new(Expression::from(*low)),
                high: Box::new(Expression::from(*high)),
                negated: *negated,
                symmetric: *symmetric,
            },
            ArenaExpression::Cast { expr, data_type } => Expression::Cast {
                expr: Box::new(Expression::from(*expr)),
                data_type: data_type.clone(),
            },
            ArenaExpression::Position {
                substring,
                string,
                character_unit,
            } => Expression::Position {
                substring: Box::new(Expression::from(*substring)),
                string: Box::new(Expression::from(*string)),
                character_unit: character_unit.map(CharacterUnit::from),
            },
            ArenaExpression::Trim {
                position,
                removal_char,
                string,
            } => Expression::Trim {
                position: position.map(TrimPosition::from),
                removal_char: removal_char.map(|e| Box::new(Expression::from(e))),
                string: Box::new(Expression::from(*string)),
            },
            ArenaExpression::Extract { field, expr } => Expression::Extract {
                field: IntervalUnit::from(*field),
                expr: Box::new(Expression::from(*expr)),
            },
            ArenaExpression::Like {
                expr,
                pattern,
                negated,
            } => Expression::Like {
                expr: Box::new(Expression::from(*expr)),
                pattern: Box::new(Expression::from(*pattern)),
                negated: *negated,
            },
            ArenaExpression::Exists { subquery, negated } => Expression::Exists {
                subquery: Box::new(SelectStmt::from(*subquery)),
                negated: *negated,
            },
            ArenaExpression::QuantifiedComparison {
                expr,
                op,
                quantifier,
                subquery,
            } => Expression::QuantifiedComparison {
                expr: Box::new(Expression::from(*expr)),
                op: *op,
                quantifier: Quantifier::from(*quantifier),
                subquery: Box::new(SelectStmt::from(*subquery)),
            },
            ArenaExpression::CurrentDate => Expression::CurrentDate,
            ArenaExpression::CurrentTime { precision } => {
                Expression::CurrentTime { precision: *precision }
            }
            ArenaExpression::CurrentTimestamp { precision } => {
                Expression::CurrentTimestamp { precision: *precision }
            }
            ArenaExpression::Interval {
                value,
                unit,
                leading_precision,
                fractional_precision,
            } => Expression::Interval {
                value: Box::new(Expression::from(*value)),
                unit: IntervalUnit::from(*unit),
                leading_precision: *leading_precision,
                fractional_precision: *fractional_precision,
            },
            ArenaExpression::Default => Expression::Default,
            ArenaExpression::DuplicateKeyValue { column } => Expression::DuplicateKeyValue {
                column: (*column).to_string(),
            },
            ArenaExpression::WindowFunction { function, over } => Expression::WindowFunction {
                function: WindowFunctionSpec::from(function),
                over: WindowSpec::from(over),
            },
            ArenaExpression::NextValue { sequence_name } => Expression::NextValue {
                sequence_name: (*sequence_name).to_string(),
            },
            ArenaExpression::MatchAgainst {
                columns,
                search_modifier,
                mode,
            } => Expression::MatchAgainst {
                columns: columns.iter().map(|s| (*s).to_string()).collect(),
                search_modifier: Box::new(Expression::from(*search_modifier)),
                mode: FulltextMode::from(*mode),
            },
            ArenaExpression::PseudoVariable {
                pseudo_table,
                column,
            } => Expression::PseudoVariable {
                pseudo_table: PseudoTable::from(*pseudo_table),
                column: (*column).to_string(),
            },
            ArenaExpression::SessionVariable { name } => Expression::SessionVariable {
                name: (*name).to_string(),
            },
        }
    }
}

// By-value conversion for owned Expression
impl<'arena> From<ArenaExpression<'arena>> for Expression {
    fn from(expr: ArenaExpression<'arena>) -> Self {
        Expression::from(&expr)
    }
}

// ============================================================================
// Helper Type Conversions
// ============================================================================

impl<'arena> From<&ArenaCaseWhen<'arena>> for CaseWhen {
    fn from(cw: &ArenaCaseWhen<'arena>) -> Self {
        CaseWhen {
            conditions: cw.conditions.iter().map(Expression::from).collect(),
            result: Expression::from(&cw.result),
        }
    }
}

impl From<ArenaCharacterUnit> for CharacterUnit {
    fn from(cu: ArenaCharacterUnit) -> Self {
        match cu {
            ArenaCharacterUnit::Characters => CharacterUnit::Characters,
            ArenaCharacterUnit::Octets => CharacterUnit::Octets,
        }
    }
}

impl From<ArenaTrimPosition> for TrimPosition {
    fn from(tp: ArenaTrimPosition) -> Self {
        match tp {
            ArenaTrimPosition::Both => TrimPosition::Both,
            ArenaTrimPosition::Leading => TrimPosition::Leading,
            ArenaTrimPosition::Trailing => TrimPosition::Trailing,
        }
    }
}

impl From<ArenaIntervalUnit> for IntervalUnit {
    fn from(iu: ArenaIntervalUnit) -> Self {
        match iu {
            ArenaIntervalUnit::Microsecond => IntervalUnit::Microsecond,
            ArenaIntervalUnit::Second => IntervalUnit::Second,
            ArenaIntervalUnit::Minute => IntervalUnit::Minute,
            ArenaIntervalUnit::Hour => IntervalUnit::Hour,
            ArenaIntervalUnit::Day => IntervalUnit::Day,
            ArenaIntervalUnit::Week => IntervalUnit::Week,
            ArenaIntervalUnit::Month => IntervalUnit::Month,
            ArenaIntervalUnit::Quarter => IntervalUnit::Quarter,
            ArenaIntervalUnit::Year => IntervalUnit::Year,
            ArenaIntervalUnit::SecondMicrosecond => IntervalUnit::SecondMicrosecond,
            ArenaIntervalUnit::MinuteMicrosecond => IntervalUnit::MinuteMicrosecond,
            ArenaIntervalUnit::MinuteSecond => IntervalUnit::MinuteSecond,
            ArenaIntervalUnit::HourMicrosecond => IntervalUnit::HourMicrosecond,
            ArenaIntervalUnit::HourSecond => IntervalUnit::HourSecond,
            ArenaIntervalUnit::HourMinute => IntervalUnit::HourMinute,
            ArenaIntervalUnit::DayMicrosecond => IntervalUnit::DayMicrosecond,
            ArenaIntervalUnit::DaySecond => IntervalUnit::DaySecond,
            ArenaIntervalUnit::DayMinute => IntervalUnit::DayMinute,
            ArenaIntervalUnit::DayHour => IntervalUnit::DayHour,
            ArenaIntervalUnit::YearMonth => IntervalUnit::YearMonth,
        }
    }
}

impl From<ArenaQuantifier> for Quantifier {
    fn from(q: ArenaQuantifier) -> Self {
        match q {
            ArenaQuantifier::All => Quantifier::All,
            ArenaQuantifier::Any => Quantifier::Any,
            ArenaQuantifier::Some => Quantifier::Some,
        }
    }
}

impl From<ArenaFulltextMode> for FulltextMode {
    fn from(m: ArenaFulltextMode) -> Self {
        match m {
            ArenaFulltextMode::NaturalLanguage => FulltextMode::NaturalLanguage,
            ArenaFulltextMode::Boolean => FulltextMode::Boolean,
            ArenaFulltextMode::QueryExpansion => FulltextMode::QueryExpansion,
        }
    }
}

impl From<ArenaPseudoTable> for PseudoTable {
    fn from(pt: ArenaPseudoTable) -> Self {
        match pt {
            ArenaPseudoTable::Old => PseudoTable::Old,
            ArenaPseudoTable::New => PseudoTable::New,
        }
    }
}

// ============================================================================
// Window Function Conversions
// ============================================================================

impl<'arena> From<&ArenaWindowFunctionSpec<'arena>> for WindowFunctionSpec {
    fn from(wf: &ArenaWindowFunctionSpec<'arena>) -> Self {
        match wf {
            ArenaWindowFunctionSpec::Aggregate { name, args } => WindowFunctionSpec::Aggregate {
                name: (*name).to_string(),
                args: args.iter().map(Expression::from).collect(),
            },
            ArenaWindowFunctionSpec::Ranking { name, args } => WindowFunctionSpec::Ranking {
                name: (*name).to_string(),
                args: args.iter().map(Expression::from).collect(),
            },
            ArenaWindowFunctionSpec::Value { name, args } => WindowFunctionSpec::Value {
                name: (*name).to_string(),
                args: args.iter().map(Expression::from).collect(),
            },
        }
    }
}

impl<'arena> From<&ArenaWindowSpec<'arena>> for WindowSpec {
    fn from(ws: &ArenaWindowSpec<'arena>) -> Self {
        WindowSpec {
            partition_by: ws
                .partition_by
                .as_ref()
                .map(|v| v.iter().map(Expression::from).collect()),
            order_by: ws
                .order_by
                .as_ref()
                .map(|v| v.iter().map(OrderByItem::from).collect()),
            frame: ws.frame.as_ref().map(WindowFrame::from),
        }
    }
}

impl<'arena> From<&ArenaWindowFrame<'arena>> for WindowFrame {
    fn from(wf: &ArenaWindowFrame<'arena>) -> Self {
        WindowFrame {
            unit: FrameUnit::from(wf.unit),
            start: FrameBound::from(&wf.start),
            end: wf.end.as_ref().map(FrameBound::from),
        }
    }
}

impl From<ArenaFrameUnit> for FrameUnit {
    fn from(fu: ArenaFrameUnit) -> Self {
        match fu {
            ArenaFrameUnit::Rows => FrameUnit::Rows,
            ArenaFrameUnit::Range => FrameUnit::Range,
        }
    }
}

impl<'arena> From<&ArenaFrameBound<'arena>> for FrameBound {
    fn from(fb: &ArenaFrameBound<'arena>) -> Self {
        match fb {
            ArenaFrameBound::UnboundedPreceding => FrameBound::UnboundedPreceding,
            ArenaFrameBound::Preceding(e) => {
                FrameBound::Preceding(Box::new(Expression::from(*e)))
            }
            ArenaFrameBound::CurrentRow => FrameBound::CurrentRow,
            ArenaFrameBound::Following(e) => {
                FrameBound::Following(Box::new(Expression::from(*e)))
            }
            ArenaFrameBound::UnboundedFollowing => FrameBound::UnboundedFollowing,
        }
    }
}

// ============================================================================
// OrderBy Conversion
// ============================================================================

impl<'arena> From<&ArenaOrderByItem<'arena>> for OrderByItem {
    fn from(obi: &ArenaOrderByItem<'arena>) -> Self {
        OrderByItem {
            expr: Expression::from(&obi.expr),
            direction: OrderDirection::from(obi.direction),
        }
    }
}

impl From<ArenaOrderDirection> for OrderDirection {
    fn from(od: ArenaOrderDirection) -> Self {
        match od {
            ArenaOrderDirection::Asc => OrderDirection::Asc,
            ArenaOrderDirection::Desc => OrderDirection::Desc,
        }
    }
}

// ============================================================================
// SELECT Statement Conversion
// ============================================================================

impl<'arena> From<&ArenaSelectStmt<'arena>> for SelectStmt {
    fn from(stmt: &ArenaSelectStmt<'arena>) -> Self {
        SelectStmt {
            with_clause: stmt
                .with_clause
                .as_ref()
                .map(|v| v.iter().map(CommonTableExpr::from).collect()),
            distinct: stmt.distinct,
            select_list: stmt.select_list.iter().map(SelectItem::from).collect(),
            into_table: stmt.into_table.map(|s| s.to_string()),
            into_variables: stmt
                .into_variables
                .as_ref()
                .map(|v| v.iter().map(|s| (*s).to_string()).collect()),
            from: stmt.from.as_ref().map(FromClause::from),
            where_clause: stmt.where_clause.as_ref().map(Expression::from),
            group_by: stmt.group_by.as_ref().map(GroupByClause::from),
            having: stmt.having.as_ref().map(Expression::from),
            order_by: stmt
                .order_by
                .as_ref()
                .map(|v| v.iter().map(OrderByItem::from).collect()),
            limit: stmt.limit,
            offset: stmt.offset,
            set_operation: stmt.set_operation.as_ref().map(SetOperation::from),
        }
    }
}

impl<'arena> From<&ArenaCommonTableExpr<'arena>> for CommonTableExpr {
    fn from(cte: &ArenaCommonTableExpr<'arena>) -> Self {
        CommonTableExpr {
            name: cte.name.to_string(),
            columns: cte
                .columns
                .as_ref()
                .map(|v| v.iter().map(|s| (*s).to_string()).collect()),
            query: Box::new(SelectStmt::from(cte.query)),
        }
    }
}

impl<'arena> From<&ArenaSelectItem<'arena>> for SelectItem {
    fn from(item: &ArenaSelectItem<'arena>) -> Self {
        match item {
            ArenaSelectItem::Wildcard { alias } => SelectItem::Wildcard {
                alias: alias
                    .as_ref()
                    .map(|v| v.iter().map(|s| (*s).to_string()).collect()),
            },
            ArenaSelectItem::QualifiedWildcard { qualifier, alias } => {
                SelectItem::QualifiedWildcard {
                    qualifier: (*qualifier).to_string(),
                    alias: alias
                        .as_ref()
                        .map(|v| v.iter().map(|s| (*s).to_string()).collect()),
                }
            }
            ArenaSelectItem::Expression { expr, alias } => SelectItem::Expression {
                expr: Expression::from(expr),
                alias: alias.map(|s| s.to_string()),
            },
        }
    }
}

impl<'arena> From<&ArenaFromClause<'arena>> for FromClause {
    fn from(fc: &ArenaFromClause<'arena>) -> Self {
        match fc {
            ArenaFromClause::Table { name, alias } => FromClause::Table {
                name: (*name).to_string(),
                alias: alias.map(|s| s.to_string()),
            },
            ArenaFromClause::Join {
                left,
                right,
                join_type,
                condition,
                natural,
            } => FromClause::Join {
                left: Box::new(FromClause::from(*left)),
                right: Box::new(FromClause::from(*right)),
                join_type: JoinType::from(*join_type),
                condition: condition.as_ref().map(Expression::from),
                natural: *natural,
            },
            ArenaFromClause::Subquery { query, alias } => FromClause::Subquery {
                query: Box::new(SelectStmt::from(*query)),
                alias: (*alias).to_string(),
            },
        }
    }
}

impl From<ArenaJoinType> for JoinType {
    fn from(jt: ArenaJoinType) -> Self {
        match jt {
            ArenaJoinType::Inner => JoinType::Inner,
            ArenaJoinType::LeftOuter => JoinType::LeftOuter,
            ArenaJoinType::RightOuter => JoinType::RightOuter,
            ArenaJoinType::FullOuter => JoinType::FullOuter,
            ArenaJoinType::Cross => JoinType::Cross,
            ArenaJoinType::Semi => JoinType::Semi,
            ArenaJoinType::Anti => JoinType::Anti,
        }
    }
}

impl<'arena> From<&ArenaGroupByClause<'arena>> for GroupByClause {
    fn from(gb: &ArenaGroupByClause<'arena>) -> Self {
        match gb {
            ArenaGroupByClause::Simple(exprs) => {
                GroupByClause::Simple(exprs.iter().map(Expression::from).collect())
            }
            ArenaGroupByClause::Rollup(elements) => {
                GroupByClause::Rollup(elements.iter().map(GroupingElement::from).collect())
            }
            ArenaGroupByClause::Cube(elements) => {
                GroupByClause::Cube(elements.iter().map(GroupingElement::from).collect())
            }
            ArenaGroupByClause::GroupingSets(sets) => {
                GroupByClause::GroupingSets(sets.iter().map(GroupingSet::from).collect())
            }
            ArenaGroupByClause::Mixed(items) => {
                GroupByClause::Mixed(items.iter().map(MixedGroupingItem::from).collect())
            }
        }
    }
}

impl<'arena> From<&ArenaGroupingElement<'arena>> for GroupingElement {
    fn from(ge: &ArenaGroupingElement<'arena>) -> Self {
        match ge {
            ArenaGroupingElement::Single(expr) => GroupingElement::Single(Expression::from(expr)),
            ArenaGroupingElement::Composite(exprs) => {
                GroupingElement::Composite(exprs.iter().map(Expression::from).collect())
            }
        }
    }
}

impl<'arena> From<&ArenaGroupingSet<'arena>> for GroupingSet {
    fn from(gs: &ArenaGroupingSet<'arena>) -> Self {
        GroupingSet {
            columns: gs.columns.iter().map(Expression::from).collect(),
        }
    }
}

impl<'arena> From<&ArenaMixedGroupingItem<'arena>> for MixedGroupingItem {
    fn from(mgi: &ArenaMixedGroupingItem<'arena>) -> Self {
        match mgi {
            ArenaMixedGroupingItem::Simple(expr) => {
                MixedGroupingItem::Simple(Expression::from(expr))
            }
            ArenaMixedGroupingItem::Rollup(elements) => {
                MixedGroupingItem::Rollup(elements.iter().map(GroupingElement::from).collect())
            }
            ArenaMixedGroupingItem::Cube(elements) => {
                MixedGroupingItem::Cube(elements.iter().map(GroupingElement::from).collect())
            }
            ArenaMixedGroupingItem::GroupingSets(sets) => {
                MixedGroupingItem::GroupingSets(sets.iter().map(GroupingSet::from).collect())
            }
        }
    }
}

impl<'arena> From<&ArenaSetOperation<'arena>> for SetOperation {
    fn from(so: &ArenaSetOperation<'arena>) -> Self {
        SetOperation {
            op: SetOperator::from(so.op),
            all: so.all,
            right: Box::new(SelectStmt::from(so.right)),
        }
    }
}

impl From<ArenaSetOperator> for SetOperator {
    fn from(so: ArenaSetOperator) -> Self {
        match so {
            ArenaSetOperator::Union => SetOperator::Union,
            ArenaSetOperator::Intersect => SetOperator::Intersect,
            ArenaSetOperator::Except => SetOperator::Except,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bumpalo::Bump;
    use vibesql_types::SqlValue;

    #[test]
    fn test_literal_conversion() {
        let _arena = Bump::new();
        let arena_expr = ArenaExpression::Literal(SqlValue::Integer(42));
        let std_expr = Expression::from(&arena_expr);
        assert_eq!(std_expr, Expression::Literal(SqlValue::Integer(42)));
    }

    #[test]
    fn test_column_ref_conversion() {
        let arena = Bump::new();
        let table = arena.alloc_str("users");
        let column = arena.alloc_str("id");
        let arena_expr = ArenaExpression::ColumnRef {
            table: Some(table),
            column,
        };
        let std_expr = Expression::from(&arena_expr);
        assert_eq!(
            std_expr,
            Expression::ColumnRef {
                table: Some("users".to_string()),
                column: "id".to_string(),
            }
        );
    }

    #[test]
    fn test_binary_op_conversion() {
        let arena = Bump::new();
        let left = arena.alloc(ArenaExpression::Literal(SqlValue::Integer(1)));
        let right = arena.alloc(ArenaExpression::Literal(SqlValue::Integer(2)));
        let arena_expr = ArenaExpression::BinaryOp {
            op: crate::BinaryOperator::Plus,
            left,
            right,
        };
        let std_expr = Expression::from(&arena_expr);
        match std_expr {
            Expression::BinaryOp { op, left, right } => {
                assert_eq!(op, crate::BinaryOperator::Plus);
                assert_eq!(*left, Expression::Literal(SqlValue::Integer(1)));
                assert_eq!(*right, Expression::Literal(SqlValue::Integer(2)));
            }
            _ => panic!("Expected BinaryOp"),
        }
    }
}
