//! Conversion from arena-allocated AST types to standard AST types.
//!
//! This module provides conversion functions that convert arena-allocated
//! AST nodes to their standard (heap-allocated) equivalents. This allows
//! the arena parser to be used for performance-critical parsing while
//! still producing standard AST types for downstream processing.
//!
//! # Usage
//!
//! ```text
//! use bumpalo::Bump;
//! use vibesql_ast::arena::{ArenaInterner, Converter};
//!
//! let arena = Bump::new();
//! let mut interner = ArenaInterner::new(&arena);
//! // Parse and build arena AST...
//! let converter = Converter::new(&interner);
//! let owned_stmt = converter.convert_select(&arena_stmt);
//! ```

use crate::{
    Assignment, CaseWhen, CharacterUnit, CommonTableExpr, ConflictClause, DeleteStmt, Expression,
    FrameBound, FrameUnit, FromClause, FulltextMode, GroupByClause, GroupingElement, GroupingSet,
    InsertSource, InsertStmt, IntervalUnit, JoinType, MixedGroupingItem, OrderByItem,
    OrderDirection, PseudoTable, Quantifier, SelectItem, SelectStmt, SetOperation, SetOperator,
    TrimPosition, UpdateStmt, WhereClause, WindowFrame, WindowFunctionSpec, WindowSpec,
};

use super::interner::{ArenaInterner, Symbol};
use super::{dml as arena_dml, expression as arena_expr, select as arena_select};

/// Converter for arena-allocated AST to owned AST.
///
/// This struct holds a reference to the interner used during parsing,
/// enabling symbol resolution during conversion.
pub struct Converter<'a, 'arena> {
    interner: &'a ArenaInterner<'arena>,
}

impl<'a, 'arena> Converter<'a, 'arena> {
    /// Create a new converter with the given interner.
    pub fn new(interner: &'a ArenaInterner<'arena>) -> Self {
        Converter { interner }
    }

    /// Resolve a symbol to its string value.
    #[inline]
    fn resolve(&self, sym: Symbol) -> String {
        self.interner.resolve(sym).to_string()
    }

    /// Resolve an optional symbol.
    #[inline]
    fn resolve_opt(&self, sym: Option<Symbol>) -> Option<String> {
        sym.map(|s| self.resolve(s))
    }

    // ========================================================================
    // Expression Conversion
    // ========================================================================

    /// Convert an arena Expression to an owned Expression.
    pub fn convert_expression(&self, expr: &arena_expr::Expression<'arena>) -> Expression {
        match expr {
            // === Inline variants (hot path) ===
            arena_expr::Expression::Literal(v) => Expression::Literal(v.clone()),
            arena_expr::Expression::Placeholder(i) => Expression::Placeholder(*i),
            arena_expr::Expression::NumberedPlaceholder(i) => Expression::NumberedPlaceholder(*i),
            arena_expr::Expression::NamedPlaceholder(name) => {
                Expression::NamedPlaceholder(self.resolve(*name))
            }
            arena_expr::Expression::ColumnRef { table, column } => Expression::ColumnRef {
                table: self.resolve_opt(*table),
                column: self.resolve(*column),
            },
            arena_expr::Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
                op: *op,
                left: Box::new(self.convert_expression(left)),
                right: Box::new(self.convert_expression(right)),
            },
            arena_expr::Expression::Conjunction(children) => Expression::Conjunction(
                children.iter().map(|e| self.convert_expression(e)).collect(),
            ),
            arena_expr::Expression::Disjunction(children) => Expression::Disjunction(
                children.iter().map(|e| self.convert_expression(e)).collect(),
            ),
            arena_expr::Expression::UnaryOp { op, expr } => {
                Expression::UnaryOp { op: *op, expr: Box::new(self.convert_expression(expr)) }
            }
            arena_expr::Expression::IsNull { expr, negated } => Expression::IsNull {
                expr: Box::new(self.convert_expression(expr)),
                negated: *negated,
            },
            arena_expr::Expression::Wildcard => Expression::Wildcard,
            arena_expr::Expression::CurrentDate => Expression::CurrentDate,
            arena_expr::Expression::CurrentTime { precision } => {
                Expression::CurrentTime { precision: *precision }
            }
            arena_expr::Expression::CurrentTimestamp { precision } => {
                Expression::CurrentTimestamp { precision: *precision }
            }
            arena_expr::Expression::Default => Expression::Default,

            // === Extended variants (cold path) ===
            arena_expr::Expression::Extended(ext) => self.convert_extended_expr(ext),
        }
    }

    /// Convert an ExtendedExpr to an owned Expression.
    fn convert_extended_expr(&self, ext: &arena_expr::ExtendedExpr<'arena>) -> Expression {
        match ext {
            arena_expr::ExtendedExpr::Function { name, args, character_unit } => {
                Expression::Function {
                    name: self.resolve(*name),
                    args: args.iter().map(|e| self.convert_expression(e)).collect(),
                    character_unit: character_unit.map(|u| u.into()),
                }
            }
            arena_expr::ExtendedExpr::AggregateFunction { name, distinct, args } => {
                Expression::AggregateFunction {
                    name: self.resolve(*name),
                    distinct: *distinct,
                    args: args.iter().map(|e| self.convert_expression(e)).collect(),
                }
            }
            arena_expr::ExtendedExpr::Case { operand, when_clauses, else_result } => {
                Expression::Case {
                    operand: operand.map(|e| Box::new(self.convert_expression(e))),
                    when_clauses: when_clauses
                        .iter()
                        .map(|cw| self.convert_case_when(cw))
                        .collect(),
                    else_result: else_result.map(|e| Box::new(self.convert_expression(e))),
                }
            }
            arena_expr::ExtendedExpr::ScalarSubquery(subquery) => {
                Expression::ScalarSubquery(Box::new(self.convert_select(subquery)))
            }
            arena_expr::ExtendedExpr::In { expr, subquery, negated } => Expression::In {
                expr: Box::new(self.convert_expression(expr)),
                subquery: Box::new(self.convert_select(subquery)),
                negated: *negated,
            },
            arena_expr::ExtendedExpr::InList { expr, values, negated } => Expression::InList {
                expr: Box::new(self.convert_expression(expr)),
                values: values.iter().map(|e| self.convert_expression(e)).collect(),
                negated: *negated,
            },
            arena_expr::ExtendedExpr::Between { expr, low, high, negated, symmetric } => {
                Expression::Between {
                    expr: Box::new(self.convert_expression(expr)),
                    low: Box::new(self.convert_expression(low)),
                    high: Box::new(self.convert_expression(high)),
                    negated: *negated,
                    symmetric: *symmetric,
                }
            }
            arena_expr::ExtendedExpr::Cast { expr, data_type } => Expression::Cast {
                expr: Box::new(self.convert_expression(expr)),
                data_type: data_type.clone(),
            },
            arena_expr::ExtendedExpr::Position { substring, string, character_unit } => {
                Expression::Position {
                    substring: Box::new(self.convert_expression(substring)),
                    string: Box::new(self.convert_expression(string)),
                    character_unit: character_unit.map(|u| u.into()),
                }
            }
            arena_expr::ExtendedExpr::Trim { position, removal_char, string } => Expression::Trim {
                position: position.map(|p| p.into()),
                removal_char: removal_char.map(|e| Box::new(self.convert_expression(e))),
                string: Box::new(self.convert_expression(string)),
            },
            arena_expr::ExtendedExpr::Extract { field, expr } => Expression::Extract {
                field: (*field).into(),
                expr: Box::new(self.convert_expression(expr)),
            },
            arena_expr::ExtendedExpr::Like { expr, pattern, negated } => Expression::Like {
                expr: Box::new(self.convert_expression(expr)),
                pattern: Box::new(self.convert_expression(pattern)),
                negated: *negated,
            },
            arena_expr::ExtendedExpr::Exists { subquery, negated } => Expression::Exists {
                subquery: Box::new(self.convert_select(subquery)),
                negated: *negated,
            },
            arena_expr::ExtendedExpr::QuantifiedComparison { expr, op, quantifier, subquery } => {
                Expression::QuantifiedComparison {
                    expr: Box::new(self.convert_expression(expr)),
                    op: *op,
                    quantifier: (*quantifier).into(),
                    subquery: Box::new(self.convert_select(subquery)),
                }
            }
            arena_expr::ExtendedExpr::Interval {
                value,
                unit,
                leading_precision,
                fractional_precision,
            } => Expression::Interval {
                value: Box::new(self.convert_expression(value)),
                unit: (*unit).into(),
                leading_precision: *leading_precision,
                fractional_precision: *fractional_precision,
            },
            arena_expr::ExtendedExpr::DuplicateKeyValue { column } => {
                Expression::DuplicateKeyValue { column: self.resolve(*column) }
            }
            arena_expr::ExtendedExpr::WindowFunction { function, over } => {
                Expression::WindowFunction {
                    function: self.convert_window_function_spec(function),
                    over: self.convert_window_spec(over),
                }
            }
            arena_expr::ExtendedExpr::NextValue { sequence_name } => {
                Expression::NextValue { sequence_name: self.resolve(*sequence_name) }
            }
            arena_expr::ExtendedExpr::MatchAgainst { columns, search_modifier, mode } => {
                Expression::MatchAgainst {
                    columns: columns.iter().map(|s| self.resolve(*s)).collect(),
                    search_modifier: Box::new(self.convert_expression(search_modifier)),
                    mode: (*mode).into(),
                }
            }
            arena_expr::ExtendedExpr::PseudoVariable { pseudo_table, column } => {
                Expression::PseudoVariable {
                    pseudo_table: (*pseudo_table).into(),
                    column: self.resolve(*column),
                }
            }
            arena_expr::ExtendedExpr::SessionVariable { name } => {
                Expression::SessionVariable { name: self.resolve(*name) }
            }
        }
    }

    fn convert_case_when(&self, cw: &arena_expr::CaseWhen<'arena>) -> CaseWhen {
        CaseWhen {
            conditions: cw.conditions.iter().map(|e| self.convert_expression(e)).collect(),
            result: self.convert_expression(&cw.result),
        }
    }

    fn convert_window_function_spec(
        &self,
        spec: &arena_expr::WindowFunctionSpec<'arena>,
    ) -> WindowFunctionSpec {
        match spec {
            arena_expr::WindowFunctionSpec::Aggregate { name, args } => {
                WindowFunctionSpec::Aggregate {
                    name: self.resolve(*name),
                    args: args.iter().map(|e| self.convert_expression(e)).collect(),
                }
            }
            arena_expr::WindowFunctionSpec::Ranking { name, args } => WindowFunctionSpec::Ranking {
                name: self.resolve(*name),
                args: args.iter().map(|e| self.convert_expression(e)).collect(),
            },
            arena_expr::WindowFunctionSpec::Value { name, args } => WindowFunctionSpec::Value {
                name: self.resolve(*name),
                args: args.iter().map(|e| self.convert_expression(e)).collect(),
            },
        }
    }

    fn convert_window_spec(&self, spec: &arena_expr::WindowSpec<'arena>) -> WindowSpec {
        WindowSpec {
            partition_by: spec
                .partition_by
                .as_ref()
                .map(|v| v.iter().map(|e| self.convert_expression(e)).collect()),
            order_by: spec
                .order_by
                .as_ref()
                .map(|v| v.iter().map(|item| self.convert_order_by_item(item)).collect()),
            frame: spec.frame.as_ref().map(|f| self.convert_window_frame(f)),
        }
    }

    fn convert_window_frame(&self, f: &arena_expr::WindowFrame<'arena>) -> WindowFrame {
        WindowFrame {
            unit: f.unit.into(),
            start: self.convert_frame_bound(&f.start),
            end: f.end.as_ref().map(|b| self.convert_frame_bound(b)),
        }
    }

    fn convert_frame_bound(&self, b: &arena_expr::FrameBound<'arena>) -> FrameBound {
        match b {
            arena_expr::FrameBound::UnboundedPreceding => FrameBound::UnboundedPreceding,
            arena_expr::FrameBound::Preceding(e) => {
                FrameBound::Preceding(Box::new(self.convert_expression(e)))
            }
            arena_expr::FrameBound::CurrentRow => FrameBound::CurrentRow,
            arena_expr::FrameBound::Following(e) => {
                FrameBound::Following(Box::new(self.convert_expression(e)))
            }
            arena_expr::FrameBound::UnboundedFollowing => FrameBound::UnboundedFollowing,
        }
    }

    fn convert_order_by_item(&self, item: &arena_expr::OrderByItem<'arena>) -> OrderByItem {
        OrderByItem { expr: self.convert_expression(&item.expr), direction: item.direction.into() }
    }

    // ========================================================================
    // SELECT Statement Conversion
    // ========================================================================

    /// Convert an arena SelectStmt to an owned SelectStmt.
    pub fn convert_select(&self, stmt: &arena_select::SelectStmt<'arena>) -> SelectStmt {
        SelectStmt {
            with_clause: stmt
                .with_clause
                .as_ref()
                .map(|ctes| ctes.iter().map(|cte| self.convert_cte(cte)).collect()),
            distinct: stmt.distinct,
            select_list: stmt
                .select_list
                .iter()
                .map(|item| self.convert_select_item(item))
                .collect(),
            into_table: self.resolve_opt(stmt.into_table),
            into_variables: stmt
                .into_variables
                .as_ref()
                .map(|v| v.iter().map(|s| self.resolve(*s)).collect()),
            from: stmt.from.as_ref().map(|f| self.convert_from_clause(f)),
            where_clause: stmt.where_clause.as_ref().map(|e| self.convert_expression(e)),
            group_by: stmt.group_by.as_ref().map(|g| self.convert_group_by(g)),
            having: stmt.having.as_ref().map(|e| self.convert_expression(e)),
            order_by: stmt
                .order_by
                .as_ref()
                .map(|v| v.iter().map(|item| self.convert_order_by_item(item)).collect()),
            limit: stmt.limit,
            offset: stmt.offset,
            set_operation: stmt.set_operation.as_ref().map(|so| self.convert_set_operation(so)),
        }
    }

    fn convert_cte(&self, cte: &arena_select::CommonTableExpr<'arena>) -> CommonTableExpr {
        CommonTableExpr {
            name: self.resolve(cte.name),
            columns: cte.columns.as_ref().map(|v| v.iter().map(|s| self.resolve(*s)).collect()),
            query: Box::new(self.convert_select(cte.query)),
        }
    }

    fn convert_select_item(&self, item: &arena_select::SelectItem<'arena>) -> SelectItem {
        match item {
            arena_select::SelectItem::Wildcard { alias } => SelectItem::Wildcard {
                alias: alias.as_ref().map(|v| v.iter().map(|s| self.resolve(*s)).collect()),
            },
            arena_select::SelectItem::QualifiedWildcard { qualifier, alias } => {
                SelectItem::QualifiedWildcard {
                    qualifier: self.resolve(*qualifier),
                    alias: alias.as_ref().map(|v| v.iter().map(|s| self.resolve(*s)).collect()),
                }
            }
            arena_select::SelectItem::Expression { expr, alias } => SelectItem::Expression {
                expr: self.convert_expression(expr),
                alias: self.resolve_opt(*alias),
            },
        }
    }

    fn convert_from_clause(&self, from: &arena_select::FromClause<'arena>) -> FromClause {
        match from {
            arena_select::FromClause::Table { name, alias, column_aliases } => FromClause::Table {
                name: self.resolve(*name),
                alias: self.resolve_opt(*alias),
                column_aliases: column_aliases
                    .as_ref()
                    .map(|cols| cols.iter().map(|s| self.resolve(*s)).collect()),
            },
            arena_select::FromClause::Join { left, right, join_type, condition, natural } => {
                FromClause::Join {
                    left: Box::new(self.convert_from_clause(left)),
                    right: Box::new(self.convert_from_clause(right)),
                    join_type: (*join_type).into(),
                    condition: condition.as_ref().map(|e| self.convert_expression(e)),
                    natural: *natural,
                }
            }
            arena_select::FromClause::Subquery { query, alias, column_aliases } => {
                FromClause::Subquery {
                    query: Box::new(self.convert_select(query)),
                    alias: self.resolve(*alias),
                    column_aliases: column_aliases
                        .as_ref()
                        .map(|cols| cols.iter().map(|s| self.resolve(*s)).collect()),
                }
            }
        }
    }

    fn convert_group_by(&self, gb: &arena_select::GroupByClause<'arena>) -> GroupByClause {
        match gb {
            arena_select::GroupByClause::Simple(exprs) => {
                GroupByClause::Simple(exprs.iter().map(|e| self.convert_expression(e)).collect())
            }
            arena_select::GroupByClause::Rollup(elements) => GroupByClause::Rollup(
                elements.iter().map(|e| self.convert_grouping_element(e)).collect(),
            ),
            arena_select::GroupByClause::Cube(elements) => GroupByClause::Cube(
                elements.iter().map(|e| self.convert_grouping_element(e)).collect(),
            ),
            arena_select::GroupByClause::GroupingSets(sets) => GroupByClause::GroupingSets(
                sets.iter().map(|s| self.convert_grouping_set(s)).collect(),
            ),
            arena_select::GroupByClause::Mixed(items) => GroupByClause::Mixed(
                items.iter().map(|i| self.convert_mixed_grouping_item(i)).collect(),
            ),
        }
    }

    fn convert_grouping_element(
        &self,
        ge: &arena_select::GroupingElement<'arena>,
    ) -> GroupingElement {
        match ge {
            arena_select::GroupingElement::Single(expr) => {
                GroupingElement::Single(self.convert_expression(expr))
            }
            arena_select::GroupingElement::Composite(exprs) => GroupingElement::Composite(
                exprs.iter().map(|e| self.convert_expression(e)).collect(),
            ),
        }
    }

    fn convert_grouping_set(&self, gs: &arena_select::GroupingSet<'arena>) -> GroupingSet {
        GroupingSet { columns: gs.columns.iter().map(|e| self.convert_expression(e)).collect() }
    }

    fn convert_mixed_grouping_item(
        &self,
        mgi: &arena_select::MixedGroupingItem<'arena>,
    ) -> MixedGroupingItem {
        match mgi {
            arena_select::MixedGroupingItem::Simple(expr) => {
                MixedGroupingItem::Simple(self.convert_expression(expr))
            }
            arena_select::MixedGroupingItem::Rollup(elements) => MixedGroupingItem::Rollup(
                elements.iter().map(|e| self.convert_grouping_element(e)).collect(),
            ),
            arena_select::MixedGroupingItem::Cube(elements) => MixedGroupingItem::Cube(
                elements.iter().map(|e| self.convert_grouping_element(e)).collect(),
            ),
            arena_select::MixedGroupingItem::GroupingSets(sets) => MixedGroupingItem::GroupingSets(
                sets.iter().map(|s| self.convert_grouping_set(s)).collect(),
            ),
        }
    }

    fn convert_set_operation(&self, so: &arena_select::SetOperation<'arena>) -> SetOperation {
        SetOperation {
            op: so.op.into(),
            all: so.all,
            right: Box::new(self.convert_select(so.right)),
        }
    }

    // ========================================================================
    // DML Statement Conversion
    // ========================================================================

    /// Convert an arena InsertStmt to an owned InsertStmt.
    pub fn convert_insert(&self, stmt: &arena_dml::InsertStmt<'arena>) -> InsertStmt {
        InsertStmt {
            table_name: self.resolve(stmt.table_name),
            columns: stmt.columns.iter().map(|s| self.resolve(*s)).collect(),
            source: self.convert_insert_source(&stmt.source),
            conflict_clause: stmt.conflict_clause.map(ConflictClause::from),
            on_duplicate_key_update: stmt.on_duplicate_key_update.as_ref().map(|assignments| {
                assignments.iter().map(|a| self.convert_assignment(a)).collect()
            }),
        }
    }

    fn convert_insert_source(&self, source: &arena_dml::InsertSource<'arena>) -> InsertSource {
        match source {
            arena_dml::InsertSource::Values(rows) => InsertSource::Values(
                rows.iter()
                    .map(|row| row.iter().map(|e| self.convert_expression(e)).collect())
                    .collect(),
            ),
            arena_dml::InsertSource::Select(query) => {
                InsertSource::Select(Box::new(self.convert_select(query)))
            }
        }
    }

    fn convert_assignment(&self, a: &arena_dml::Assignment<'arena>) -> Assignment {
        Assignment { column: self.resolve(a.column), value: self.convert_expression(&a.value) }
    }

    /// Convert an arena UpdateStmt to an owned UpdateStmt.
    pub fn convert_update(&self, stmt: &arena_dml::UpdateStmt<'arena>) -> UpdateStmt {
        UpdateStmt {
            table_name: self.resolve(stmt.table_name),
            assignments: stmt.assignments.iter().map(|a| self.convert_assignment(a)).collect(),
            where_clause: stmt.where_clause.as_ref().map(|wc| self.convert_where_clause(wc)),
        }
    }

    fn convert_where_clause(&self, wc: &arena_dml::WhereClause<'arena>) -> WhereClause {
        match wc {
            arena_dml::WhereClause::Condition(expr) => {
                WhereClause::Condition(self.convert_expression(expr))
            }
            arena_dml::WhereClause::CurrentOf(cursor) => {
                WhereClause::CurrentOf(self.resolve(*cursor))
            }
        }
    }

    /// Convert an arena DeleteStmt to an owned DeleteStmt.
    pub fn convert_delete(&self, stmt: &arena_dml::DeleteStmt<'arena>) -> DeleteStmt {
        DeleteStmt {
            only: stmt.only,
            table_name: self.resolve(stmt.table_name),
            where_clause: stmt.where_clause.as_ref().map(|wc| self.convert_where_clause(wc)),
        }
    }
}

// ============================================================================
// Simple From implementations for enums (don't need interner)
// ============================================================================

impl From<arena_expr::CharacterUnit> for CharacterUnit {
    fn from(u: arena_expr::CharacterUnit) -> Self {
        match u {
            arena_expr::CharacterUnit::Characters => CharacterUnit::Characters,
            arena_expr::CharacterUnit::Octets => CharacterUnit::Octets,
        }
    }
}

impl From<arena_expr::TrimPosition> for TrimPosition {
    fn from(p: arena_expr::TrimPosition) -> Self {
        match p {
            arena_expr::TrimPosition::Both => TrimPosition::Both,
            arena_expr::TrimPosition::Leading => TrimPosition::Leading,
            arena_expr::TrimPosition::Trailing => TrimPosition::Trailing,
        }
    }
}

impl From<arena_expr::IntervalUnit> for IntervalUnit {
    fn from(u: arena_expr::IntervalUnit) -> Self {
        match u {
            arena_expr::IntervalUnit::Microsecond => IntervalUnit::Microsecond,
            arena_expr::IntervalUnit::Second => IntervalUnit::Second,
            arena_expr::IntervalUnit::Minute => IntervalUnit::Minute,
            arena_expr::IntervalUnit::Hour => IntervalUnit::Hour,
            arena_expr::IntervalUnit::Day => IntervalUnit::Day,
            arena_expr::IntervalUnit::Week => IntervalUnit::Week,
            arena_expr::IntervalUnit::Month => IntervalUnit::Month,
            arena_expr::IntervalUnit::Quarter => IntervalUnit::Quarter,
            arena_expr::IntervalUnit::Year => IntervalUnit::Year,
            arena_expr::IntervalUnit::SecondMicrosecond => IntervalUnit::SecondMicrosecond,
            arena_expr::IntervalUnit::MinuteMicrosecond => IntervalUnit::MinuteMicrosecond,
            arena_expr::IntervalUnit::MinuteSecond => IntervalUnit::MinuteSecond,
            arena_expr::IntervalUnit::HourMicrosecond => IntervalUnit::HourMicrosecond,
            arena_expr::IntervalUnit::HourSecond => IntervalUnit::HourSecond,
            arena_expr::IntervalUnit::HourMinute => IntervalUnit::HourMinute,
            arena_expr::IntervalUnit::DayMicrosecond => IntervalUnit::DayMicrosecond,
            arena_expr::IntervalUnit::DaySecond => IntervalUnit::DaySecond,
            arena_expr::IntervalUnit::DayMinute => IntervalUnit::DayMinute,
            arena_expr::IntervalUnit::DayHour => IntervalUnit::DayHour,
            arena_expr::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
        }
    }
}

impl From<arena_expr::Quantifier> for Quantifier {
    fn from(q: arena_expr::Quantifier) -> Self {
        match q {
            arena_expr::Quantifier::All => Quantifier::All,
            arena_expr::Quantifier::Any => Quantifier::Any,
            arena_expr::Quantifier::Some => Quantifier::Some,
        }
    }
}

impl From<arena_expr::FulltextMode> for FulltextMode {
    fn from(m: arena_expr::FulltextMode) -> Self {
        match m {
            arena_expr::FulltextMode::NaturalLanguage => FulltextMode::NaturalLanguage,
            arena_expr::FulltextMode::Boolean => FulltextMode::Boolean,
            arena_expr::FulltextMode::QueryExpansion => FulltextMode::QueryExpansion,
        }
    }
}

impl From<arena_expr::PseudoTable> for PseudoTable {
    fn from(p: arena_expr::PseudoTable) -> Self {
        match p {
            arena_expr::PseudoTable::Old => PseudoTable::Old,
            arena_expr::PseudoTable::New => PseudoTable::New,
        }
    }
}

impl From<arena_expr::FrameUnit> for FrameUnit {
    fn from(u: arena_expr::FrameUnit) -> Self {
        match u {
            arena_expr::FrameUnit::Rows => FrameUnit::Rows,
            arena_expr::FrameUnit::Range => FrameUnit::Range,
        }
    }
}

impl From<arena_expr::OrderDirection> for OrderDirection {
    fn from(d: arena_expr::OrderDirection) -> Self {
        match d {
            arena_expr::OrderDirection::Asc => OrderDirection::Asc,
            arena_expr::OrderDirection::Desc => OrderDirection::Desc,
        }
    }
}

impl From<arena_select::JoinType> for JoinType {
    fn from(jt: arena_select::JoinType) -> Self {
        match jt {
            arena_select::JoinType::Inner => JoinType::Inner,
            arena_select::JoinType::LeftOuter => JoinType::LeftOuter,
            arena_select::JoinType::RightOuter => JoinType::RightOuter,
            arena_select::JoinType::FullOuter => JoinType::FullOuter,
            arena_select::JoinType::Cross => JoinType::Cross,
            arena_select::JoinType::Semi => JoinType::Semi,
            arena_select::JoinType::Anti => JoinType::Anti,
        }
    }
}

impl From<arena_select::SetOperator> for SetOperator {
    fn from(op: arena_select::SetOperator) -> Self {
        match op {
            arena_select::SetOperator::Union => SetOperator::Union,
            arena_select::SetOperator::Intersect => SetOperator::Intersect,
            arena_select::SetOperator::Except => SetOperator::Except,
        }
    }
}

impl From<arena_dml::ConflictClause> for ConflictClause {
    fn from(cc: arena_dml::ConflictClause) -> Self {
        match cc {
            arena_dml::ConflictClause::Replace => ConflictClause::Replace,
            arena_dml::ConflictClause::Ignore => ConflictClause::Ignore,
        }
    }
}
