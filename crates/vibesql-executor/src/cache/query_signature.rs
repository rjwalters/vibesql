//! Query signature generation for cache keys
//!
//! Generates deterministic cache keys from SQL queries by normalizing the AST
//! and creating a hash. Queries with identical structure (different literals)
//! will have the same signature.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use vibesql_ast::arena::{
    Expression as ArenaExpression, ExtendedExpr as ArenaExtendedExpr,
    FromClause as ArenaFromClause, GroupByClause as ArenaGroupByClause,
    GroupingElement as ArenaGroupingElement, GroupingSet as ArenaGroupingSet,
    MixedGroupingItem as ArenaMixedGroupingItem, SelectItem as ArenaSelectItem,
    SelectStmt as ArenaSelectStmt, WindowFunctionSpec as ArenaWindowFunctionSpec,
};
use vibesql_ast::{Expression, Statement};

/// Unique identifier for a query based on its structure
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct QuerySignature {
    hash: u64,
}

impl QuerySignature {
    /// Create a signature from SQL text (legacy string-based approach)
    pub fn from_sql(sql: &str) -> Self {
        let normalized = Self::normalize(sql);
        let mut hasher = DefaultHasher::new();
        normalized.hash(&mut hasher);
        let hash = hasher.finish();
        Self { hash }
    }

    /// Create a signature from parsed AST, ignoring literal values
    /// This allows queries with different literals but identical structure to share cached plans
    pub fn from_ast(stmt: &Statement) -> Self {
        let mut hasher = DefaultHasher::new();
        Self::hash_statement(stmt, &mut hasher);
        let hash = hasher.finish();
        Self { hash }
    }

    /// Create a signature from arena-allocated SelectStmt, ignoring literal values
    pub fn from_arena_select(select: &ArenaSelectStmt<'_>) -> Self {
        let mut hasher = DefaultHasher::new();
        "SELECT".hash(&mut hasher);
        Self::hash_arena_select(select, &mut hasher);
        let hash = hasher.finish();
        Self { hash }
    }

    /// Get the underlying hash
    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Normalize SQL: trim and collapse whitespace
    fn normalize(sql: &str) -> String {
        sql.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase()
    }

    /// Hash a statement, replacing literals with a placeholder marker
    fn hash_statement(stmt: &Statement, hasher: &mut DefaultHasher) {
        match stmt {
            Statement::Select(select) => {
                "SELECT".hash(hasher);
                Self::hash_select(select, hasher);
            }
            Statement::Insert(insert) => {
                "INSERT".hash(hasher);
                insert.table_name.hash(hasher);
                for col in &insert.columns {
                    col.hash(hasher);
                }
                // Hash the insert source structure without literals
                match &insert.source {
                    vibesql_ast::InsertSource::Values(rows) => {
                        "VALUES".hash(hasher);
                        rows.len().hash(hasher);
                        for row in rows {
                            row.len().hash(hasher);
                            for expr in row {
                                Self::hash_expression(expr, hasher);
                            }
                        }
                    }
                    vibesql_ast::InsertSource::Select(select) => {
                        "SELECT".hash(hasher);
                        Self::hash_select(select, hasher);
                    }
                }
            }
            Statement::Update(update) => {
                "UPDATE".hash(hasher);
                update.table_name.hash(hasher);
                for assignment in &update.assignments {
                    assignment.column.hash(hasher);
                    Self::hash_expression(&assignment.value, hasher);
                }
                if let Some(ref where_clause) = update.where_clause {
                    match where_clause {
                        vibesql_ast::WhereClause::Condition(expr) => {
                            Self::hash_expression(expr, hasher);
                        }
                        vibesql_ast::WhereClause::CurrentOf(cursor) => {
                            "CURRENT_OF".hash(hasher);
                            cursor.hash(hasher);
                        }
                    }
                }
            }
            Statement::Delete(delete) => {
                "DELETE".hash(hasher);
                delete.table_name.hash(hasher);
                if let Some(ref where_clause) = delete.where_clause {
                    match where_clause {
                        vibesql_ast::WhereClause::Condition(expr) => {
                            Self::hash_expression(expr, hasher);
                        }
                        vibesql_ast::WhereClause::CurrentOf(cursor) => {
                            "CURRENT_OF".hash(hasher);
                            cursor.hash(hasher);
                        }
                    }
                }
            }
            // For other statement types, fall back to discriminant hashing
            _ => {
                std::mem::discriminant(stmt).hash(hasher);
            }
        }
    }

    /// Hash a SELECT statement structure
    fn hash_select(select: &vibesql_ast::SelectStmt, hasher: &mut DefaultHasher) {
        // Hash DISTINCT
        select.distinct.hash(hasher);

        // Hash select items
        for item in &select.select_list {
            match item {
                vibesql_ast::SelectItem::Wildcard { .. } => "WILDCARD".hash(hasher),
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                    "QUALIFIED_WILDCARD".hash(hasher);
                    qualifier.hash(hasher);
                }
                vibesql_ast::SelectItem::Expression { expr, alias } => {
                    Self::hash_expression(expr, hasher);
                    alias.hash(hasher);
                }
            }
        }

        // Hash FROM clause
        if let Some(ref from) = select.from {
            Self::hash_from_clause(from, hasher);
        }

        // Hash WHERE clause
        if let Some(ref where_clause) = select.where_clause {
            Self::hash_expression(where_clause, hasher);
        }

        // Hash GROUP BY
        if let Some(ref group_by) = select.group_by {
            Self::hash_group_by(group_by, hasher);
        }

        // Hash HAVING
        if let Some(ref having) = select.having {
            Self::hash_expression(having, hasher);
        }

        // Hash ORDER BY
        if let Some(ref order_by) = select.order_by {
            for item in order_by {
                Self::hash_expression(&item.expr, hasher);
                std::mem::discriminant(&item.direction).hash(hasher);
            }
        }

        // Hash LIMIT/OFFSET (these are often literals, but we treat them as part of structure)
        select.limit.hash(hasher);
        select.offset.hash(hasher);
    }

    /// Hash a FROM clause structure
    fn hash_from_clause(from: &vibesql_ast::FromClause, hasher: &mut DefaultHasher) {
        match from {
            vibesql_ast::FromClause::Table { name, alias, .. } => {
                "TABLE".hash(hasher);
                name.hash(hasher);
                alias.hash(hasher);
            }
            vibesql_ast::FromClause::Join { left, join_type, right, condition, .. } => {
                "JOIN".hash(hasher);
                Self::hash_from_clause(left, hasher);
                std::mem::discriminant(join_type).hash(hasher);
                Self::hash_from_clause(right, hasher);
                if let Some(expr) = condition {
                    Self::hash_expression(expr, hasher);
                }
            }
            vibesql_ast::FromClause::Subquery { query, alias, .. } => {
                "SUBQUERY".hash(hasher);
                Self::hash_select(query, hasher);
                alias.hash(hasher);
            }
        }
    }

    fn hash_group_by(group_by: &vibesql_ast::GroupByClause, hasher: &mut DefaultHasher) {
        match group_by {
            vibesql_ast::GroupByClause::Simple(exprs) => {
                "SIMPLE".hash(hasher);
                for expr in exprs {
                    Self::hash_expression(expr, hasher);
                }
            }
            vibesql_ast::GroupByClause::Rollup(elements) => {
                "ROLLUP".hash(hasher);
                Self::hash_grouping_elements(elements, hasher);
            }
            vibesql_ast::GroupByClause::Cube(elements) => {
                "CUBE".hash(hasher);
                Self::hash_grouping_elements(elements, hasher);
            }
            vibesql_ast::GroupByClause::GroupingSets(sets) => {
                "GROUPING_SETS".hash(hasher);
                Self::hash_grouping_sets(sets, hasher);
            }
            vibesql_ast::GroupByClause::Mixed(items) => {
                "MIXED".hash(hasher);
                for item in items {
                    match item {
                        vibesql_ast::MixedGroupingItem::Simple(expr) => {
                            "SIMPLE".hash(hasher);
                            Self::hash_expression(expr, hasher);
                        }
                        vibesql_ast::MixedGroupingItem::Rollup(elements) => {
                            "ROLLUP".hash(hasher);
                            Self::hash_grouping_elements(elements, hasher);
                        }
                        vibesql_ast::MixedGroupingItem::Cube(elements) => {
                            "CUBE".hash(hasher);
                            Self::hash_grouping_elements(elements, hasher);
                        }
                        vibesql_ast::MixedGroupingItem::GroupingSets(sets) => {
                            "GROUPING_SETS".hash(hasher);
                            Self::hash_grouping_sets(sets, hasher);
                        }
                    }
                }
            }
        }
    }

    fn hash_grouping_sets(sets: &[vibesql_ast::GroupingSet], hasher: &mut DefaultHasher) {
        for set in sets {
            "SET".hash(hasher);
            for expr in &set.columns {
                Self::hash_expression(expr, hasher);
            }
        }
    }

    fn hash_grouping_elements(
        elements: &[vibesql_ast::GroupingElement],
        hasher: &mut DefaultHasher,
    ) {
        for element in elements {
            match element {
                vibesql_ast::GroupingElement::Single(expr) => {
                    "SINGLE".hash(hasher);
                    Self::hash_expression(expr, hasher);
                }
                vibesql_ast::GroupingElement::Composite(exprs) => {
                    "COMPOSITE".hash(hasher);
                    for expr in exprs {
                        Self::hash_expression(expr, hasher);
                    }
                }
            }
        }
    }

    /// Hash an expression, replacing literals with a placeholder marker
    fn hash_expression(expr: &Expression, hasher: &mut DefaultHasher) {
        match expr {
            // Key difference: All literals and placeholders hash to the same value
            // This allows parameterized queries to match with literal values
            Expression::Literal(_)
            | Expression::Placeholder(_)
            | Expression::NumberedPlaceholder(_)
            | Expression::NamedPlaceholder(_) => "LITERAL_PLACEHOLDER".hash(hasher),

            Expression::ColumnRef { table, column } => {
                "COLUMN".hash(hasher);
                table.hash(hasher);
                column.hash(hasher);
            }

            Expression::PseudoVariable { pseudo_table, column } => {
                "PSEUDO_VARIABLE".hash(hasher);
                std::mem::discriminant(pseudo_table).hash(hasher);
                column.hash(hasher);
            }

            Expression::BinaryOp { op, left, right } => {
                "BINARY_OP".hash(hasher);
                std::mem::discriminant(op).hash(hasher);
                Self::hash_expression(left, hasher);
                Self::hash_expression(right, hasher);
            }

            Expression::UnaryOp { op, expr } => {
                "UNARY_OP".hash(hasher);
                std::mem::discriminant(op).hash(hasher);
                Self::hash_expression(expr, hasher);
            }

            Expression::Function { name, args, character_unit } => {
                "FUNCTION".hash(hasher);
                name.to_lowercase().hash(hasher);
                for arg in args {
                    Self::hash_expression(arg, hasher);
                }
                if let Some(ref unit) = character_unit {
                    std::mem::discriminant(unit).hash(hasher);
                }
            }

            Expression::AggregateFunction { name, distinct, args } => {
                "AGGREGATE".hash(hasher);
                name.to_lowercase().hash(hasher);
                distinct.hash(hasher);
                for arg in args {
                    Self::hash_expression(arg, hasher);
                }
            }

            Expression::IsNull { expr, negated } => {
                "IS_NULL".hash(hasher);
                Self::hash_expression(expr, hasher);
                negated.hash(hasher);
            }

            Expression::Wildcard => "WILDCARD".hash(hasher),

            Expression::Case { operand, when_clauses, else_result } => {
                "CASE".hash(hasher);
                if let Some(ref op) = operand {
                    Self::hash_expression(op, hasher);
                }
                for when in when_clauses {
                    for cond in &when.conditions {
                        Self::hash_expression(cond, hasher);
                    }
                    Self::hash_expression(&when.result, hasher);
                }
                if let Some(ref else_expr) = else_result {
                    Self::hash_expression(else_expr, hasher);
                }
            }

            Expression::ScalarSubquery(subquery) => {
                "SCALAR_SUBQUERY".hash(hasher);
                Self::hash_select(subquery, hasher);
            }

            Expression::In { expr, subquery, negated } => {
                "IN_SUBQUERY".hash(hasher);
                Self::hash_expression(expr, hasher);
                Self::hash_select(subquery, hasher);
                negated.hash(hasher);
            }

            Expression::InList { expr, values, negated } => {
                "IN_LIST".hash(hasher);
                Self::hash_expression(expr, hasher);
                values.len().hash(hasher);
                for val in values {
                    Self::hash_expression(val, hasher);
                }
                negated.hash(hasher);
            }

            Expression::Between { expr, low, high, negated, symmetric } => {
                "BETWEEN".hash(hasher);
                Self::hash_expression(expr, hasher);
                Self::hash_expression(low, hasher);
                Self::hash_expression(high, hasher);
                negated.hash(hasher);
                symmetric.hash(hasher);
            }

            Expression::Cast { expr, data_type } => {
                "CAST".hash(hasher);
                Self::hash_expression(expr, hasher);
                std::mem::discriminant(data_type).hash(hasher);
            }

            Expression::Position { substring, string, character_unit } => {
                "POSITION".hash(hasher);
                Self::hash_expression(substring, hasher);
                Self::hash_expression(string, hasher);
                if let Some(ref unit) = character_unit {
                    std::mem::discriminant(unit).hash(hasher);
                }
            }

            Expression::Trim { position, removal_char, string } => {
                "TRIM".hash(hasher);
                if let Some(ref pos) = position {
                    std::mem::discriminant(pos).hash(hasher);
                }
                if let Some(ref ch) = removal_char {
                    Self::hash_expression(ch, hasher);
                }
                Self::hash_expression(string, hasher);
            }

            Expression::Extract { field, expr } => {
                "EXTRACT".hash(hasher);
                std::mem::discriminant(field).hash(hasher);
                Self::hash_expression(expr, hasher);
            }

            Expression::Like { expr, pattern, negated } => {
                "LIKE".hash(hasher);
                Self::hash_expression(expr, hasher);
                Self::hash_expression(pattern, hasher);
                negated.hash(hasher);
            }

            Expression::Exists { subquery, negated } => {
                "EXISTS".hash(hasher);
                Self::hash_select(subquery, hasher);
                negated.hash(hasher);
            }

            Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
                "QUANTIFIED".hash(hasher);
                Self::hash_expression(expr, hasher);
                std::mem::discriminant(op).hash(hasher);
                std::mem::discriminant(quantifier).hash(hasher);
                Self::hash_select(subquery, hasher);
            }

            Expression::CurrentDate => "CURRENT_DATE".hash(hasher),

            Expression::CurrentTime { precision } => {
                "CURRENT_TIME".hash(hasher);
                precision.hash(hasher);
            }

            Expression::CurrentTimestamp { precision } => {
                "CURRENT_TIMESTAMP".hash(hasher);
                precision.hash(hasher);
            }

            Expression::Interval { value, unit, leading_precision, fractional_precision } => {
                "INTERVAL".hash(hasher);
                Self::hash_expression(value, hasher);
                format!("{:?}", unit).hash(hasher);
                leading_precision.hash(hasher);
                fractional_precision.hash(hasher);
            }

            Expression::Default => "DEFAULT".hash(hasher),

            Expression::DuplicateKeyValue { column } => {
                "DUPLICATE_KEY_VALUE".hash(hasher);
                column.hash(hasher);
            }

            Expression::WindowFunction { function, over } => {
                "WINDOW_FUNCTION".hash(hasher);
                // Hash function type and arguments
                match function {
                    vibesql_ast::WindowFunctionSpec::Aggregate { name, args } => {
                        "AGGREGATE".hash(hasher);
                        name.to_lowercase().hash(hasher);
                        for arg in args {
                            Self::hash_expression(arg, hasher);
                        }
                    }
                    vibesql_ast::WindowFunctionSpec::Ranking { name, args } => {
                        "RANKING".hash(hasher);
                        name.to_lowercase().hash(hasher);
                        for arg in args {
                            Self::hash_expression(arg, hasher);
                        }
                    }
                    vibesql_ast::WindowFunctionSpec::Value { name, args } => {
                        "VALUE".hash(hasher);
                        name.to_lowercase().hash(hasher);
                        for arg in args {
                            Self::hash_expression(arg, hasher);
                        }
                    }
                }

                // Hash OVER clause components
                if let Some(ref partition_by) = over.partition_by {
                    for expr in partition_by {
                        Self::hash_expression(expr, hasher);
                    }
                }
                if let Some(ref order_by) = over.order_by {
                    for item in order_by {
                        Self::hash_expression(&item.expr, hasher);
                        std::mem::discriminant(&item.direction).hash(hasher);
                    }
                }
                if let Some(ref frame) = over.frame {
                    std::mem::discriminant(&frame.unit).hash(hasher);
                    std::mem::discriminant(&frame.start).hash(hasher);
                    if let Some(ref end) = frame.end {
                        std::mem::discriminant(end).hash(hasher);
                    }
                }
            }

            Expression::NextValue { sequence_name } => {
                "NEXT_VALUE".hash(hasher);
                sequence_name.hash(hasher);
            }

            Expression::MatchAgainst { columns, search_modifier, mode } => {
                "MATCH_AGAINST".hash(hasher);
                for col in columns {
                    col.hash(hasher);
                }
                Self::hash_expression(search_modifier, hasher);
                std::mem::discriminant(mode).hash(hasher);
            }

            Expression::SessionVariable { name } => {
                "SESSION_VARIABLE".hash(hasher);
                name.hash(hasher);
            }

            Expression::Conjunction(children) | Expression::Disjunction(children) => {
                for child in children {
                    Self::hash_expression(child, hasher);
                }
            }
        }
    }

    // ========================================================================
    // Arena-allocated type hashing
    // ========================================================================

    /// Hash an arena-allocated SELECT statement structure
    fn hash_arena_select(select: &ArenaSelectStmt<'_>, hasher: &mut DefaultHasher) {
        // Hash DISTINCT
        select.distinct.hash(hasher);

        // Hash select items
        for item in &select.select_list {
            match item {
                ArenaSelectItem::Wildcard { .. } => "WILDCARD".hash(hasher),
                ArenaSelectItem::QualifiedWildcard { qualifier, .. } => {
                    "QUALIFIED_WILDCARD".hash(hasher);
                    qualifier.hash(hasher);
                }
                ArenaSelectItem::Expression { expr, alias } => {
                    Self::hash_arena_expression(expr, hasher);
                    alias.hash(hasher);
                }
            }
        }

        // Hash FROM clause
        if let Some(ref from) = select.from {
            Self::hash_arena_from_clause(from, hasher);
        }

        // Hash WHERE clause
        if let Some(ref where_clause) = select.where_clause {
            Self::hash_arena_expression(where_clause, hasher);
        }

        // Hash GROUP BY
        if let Some(ref group_by) = select.group_by {
            Self::hash_arena_group_by(group_by, hasher);
        }

        // Hash HAVING
        if let Some(ref having) = select.having {
            Self::hash_arena_expression(having, hasher);
        }

        // Hash ORDER BY
        if let Some(ref order_by) = select.order_by {
            for item in order_by {
                Self::hash_arena_expression(&item.expr, hasher);
                std::mem::discriminant(&item.direction).hash(hasher);
            }
        }

        // Hash LIMIT/OFFSET
        select.limit.hash(hasher);
        select.offset.hash(hasher);
    }

    /// Hash an arena-allocated FROM clause structure
    fn hash_arena_from_clause(from: &ArenaFromClause<'_>, hasher: &mut DefaultHasher) {
        match from {
            ArenaFromClause::Table { name, alias, .. } => {
                "TABLE".hash(hasher);
                name.hash(hasher);
                alias.hash(hasher);
            }
            ArenaFromClause::Join { left, join_type, right, condition, .. } => {
                "JOIN".hash(hasher);
                Self::hash_arena_from_clause(left, hasher);
                std::mem::discriminant(join_type).hash(hasher);
                Self::hash_arena_from_clause(right, hasher);
                if let Some(expr) = condition {
                    Self::hash_arena_expression(expr, hasher);
                }
            }
            ArenaFromClause::Subquery { query, alias, .. } => {
                "SUBQUERY".hash(hasher);
                Self::hash_arena_select(query, hasher);
                alias.hash(hasher);
            }
        }
    }

    fn hash_arena_group_by(group_by: &ArenaGroupByClause<'_>, hasher: &mut DefaultHasher) {
        match group_by {
            ArenaGroupByClause::Simple(exprs) => {
                "SIMPLE".hash(hasher);
                for expr in exprs {
                    Self::hash_arena_expression(expr, hasher);
                }
            }
            ArenaGroupByClause::Rollup(elements) => {
                "ROLLUP".hash(hasher);
                Self::hash_arena_grouping_elements(elements, hasher);
            }
            ArenaGroupByClause::Cube(elements) => {
                "CUBE".hash(hasher);
                Self::hash_arena_grouping_elements(elements, hasher);
            }
            ArenaGroupByClause::GroupingSets(sets) => {
                "GROUPING_SETS".hash(hasher);
                Self::hash_arena_grouping_sets(sets, hasher);
            }
            ArenaGroupByClause::Mixed(items) => {
                "MIXED".hash(hasher);
                for item in items {
                    match item {
                        ArenaMixedGroupingItem::Simple(expr) => {
                            "SIMPLE".hash(hasher);
                            Self::hash_arena_expression(expr, hasher);
                        }
                        ArenaMixedGroupingItem::Rollup(elements) => {
                            "ROLLUP".hash(hasher);
                            Self::hash_arena_grouping_elements(elements, hasher);
                        }
                        ArenaMixedGroupingItem::Cube(elements) => {
                            "CUBE".hash(hasher);
                            Self::hash_arena_grouping_elements(elements, hasher);
                        }
                        ArenaMixedGroupingItem::GroupingSets(sets) => {
                            "GROUPING_SETS".hash(hasher);
                            Self::hash_arena_grouping_sets(sets, hasher);
                        }
                    }
                }
            }
        }
    }

    fn hash_arena_grouping_sets(
        sets: &bumpalo::collections::Vec<'_, ArenaGroupingSet<'_>>,
        hasher: &mut DefaultHasher,
    ) {
        for set in sets {
            "SET".hash(hasher);
            for expr in &set.columns {
                Self::hash_arena_expression(expr, hasher);
            }
        }
    }

    fn hash_arena_grouping_elements(
        elements: &bumpalo::collections::Vec<'_, ArenaGroupingElement<'_>>,
        hasher: &mut DefaultHasher,
    ) {
        for element in elements {
            match element {
                ArenaGroupingElement::Single(expr) => {
                    "SINGLE".hash(hasher);
                    Self::hash_arena_expression(expr, hasher);
                }
                ArenaGroupingElement::Composite(exprs) => {
                    "COMPOSITE".hash(hasher);
                    for expr in exprs {
                        Self::hash_arena_expression(expr, hasher);
                    }
                }
            }
        }
    }

    /// Hash an arena-allocated expression, replacing literals with a placeholder marker
    fn hash_arena_expression(expr: &ArenaExpression<'_>, hasher: &mut DefaultHasher) {
        match expr {
            // Hot-path inline variants
            // All literals and placeholders hash to the same value
            ArenaExpression::Literal(_)
            | ArenaExpression::Placeholder(_)
            | ArenaExpression::NumberedPlaceholder(_)
            | ArenaExpression::NamedPlaceholder(_) => "LITERAL_PLACEHOLDER".hash(hasher),

            ArenaExpression::ColumnRef { table, column } => {
                "COLUMN".hash(hasher);
                table.hash(hasher);
                column.hash(hasher);
            }

            ArenaExpression::BinaryOp { op, left, right } => {
                "BINARY_OP".hash(hasher);
                std::mem::discriminant(op).hash(hasher);
                Self::hash_arena_expression(left, hasher);
                Self::hash_arena_expression(right, hasher);
            }

            ArenaExpression::UnaryOp { op, expr } => {
                "UNARY_OP".hash(hasher);
                std::mem::discriminant(op).hash(hasher);
                Self::hash_arena_expression(expr, hasher);
            }

            ArenaExpression::IsNull { expr, negated } => {
                "IS_NULL".hash(hasher);
                Self::hash_arena_expression(expr, hasher);
                negated.hash(hasher);
            }

            ArenaExpression::Wildcard => "WILDCARD".hash(hasher),

            ArenaExpression::CurrentDate => "CURRENT_DATE".hash(hasher),

            ArenaExpression::CurrentTime { precision } => {
                "CURRENT_TIME".hash(hasher);
                precision.hash(hasher);
            }

            ArenaExpression::CurrentTimestamp { precision } => {
                "CURRENT_TIMESTAMP".hash(hasher);
                precision.hash(hasher);
            }

            ArenaExpression::Default => "DEFAULT".hash(hasher),

            ArenaExpression::Conjunction(children) | ArenaExpression::Disjunction(children) => {
                for child in children.iter() {
                    Self::hash_arena_expression(child, hasher);
                }
            }

            // Cold-path extended variants
            ArenaExpression::Extended(ext) => Self::hash_arena_extended_expr(ext, hasher),
        }
    }

    /// Hash an arena-allocated extended expression
    fn hash_arena_extended_expr(ext: &ArenaExtendedExpr<'_>, hasher: &mut DefaultHasher) {
        match ext {
            ArenaExtendedExpr::Function { name, args, character_unit } => {
                "FUNCTION".hash(hasher);
                name.hash(hasher);
                for arg in args {
                    Self::hash_arena_expression(arg, hasher);
                }
                if let Some(ref unit) = character_unit {
                    std::mem::discriminant(unit).hash(hasher);
                }
            }

            ArenaExtendedExpr::AggregateFunction { name, distinct, args } => {
                "AGGREGATE".hash(hasher);
                name.hash(hasher);
                distinct.hash(hasher);
                for arg in args {
                    Self::hash_arena_expression(arg, hasher);
                }
            }

            ArenaExtendedExpr::Case { operand, when_clauses, else_result } => {
                "CASE".hash(hasher);
                if let Some(op) = operand {
                    Self::hash_arena_expression(op, hasher);
                }
                for when in when_clauses {
                    for cond in &when.conditions {
                        Self::hash_arena_expression(cond, hasher);
                    }
                    Self::hash_arena_expression(&when.result, hasher);
                }
                if let Some(else_expr) = else_result {
                    Self::hash_arena_expression(else_expr, hasher);
                }
            }

            ArenaExtendedExpr::ScalarSubquery(subquery) => {
                "SCALAR_SUBQUERY".hash(hasher);
                Self::hash_arena_select(subquery, hasher);
            }

            ArenaExtendedExpr::In { expr, subquery, negated } => {
                "IN_SUBQUERY".hash(hasher);
                Self::hash_arena_expression(expr, hasher);
                Self::hash_arena_select(subquery, hasher);
                negated.hash(hasher);
            }

            ArenaExtendedExpr::InList { expr, values, negated } => {
                "IN_LIST".hash(hasher);
                Self::hash_arena_expression(expr, hasher);
                values.len().hash(hasher);
                for val in values {
                    Self::hash_arena_expression(val, hasher);
                }
                negated.hash(hasher);
            }

            ArenaExtendedExpr::Between { expr, low, high, negated, symmetric } => {
                "BETWEEN".hash(hasher);
                Self::hash_arena_expression(expr, hasher);
                Self::hash_arena_expression(low, hasher);
                Self::hash_arena_expression(high, hasher);
                negated.hash(hasher);
                symmetric.hash(hasher);
            }

            ArenaExtendedExpr::Cast { expr, data_type } => {
                "CAST".hash(hasher);
                Self::hash_arena_expression(expr, hasher);
                std::mem::discriminant(data_type).hash(hasher);
            }

            ArenaExtendedExpr::Position { substring, string, character_unit } => {
                "POSITION".hash(hasher);
                Self::hash_arena_expression(substring, hasher);
                Self::hash_arena_expression(string, hasher);
                if let Some(unit) = character_unit {
                    std::mem::discriminant(unit).hash(hasher);
                }
            }

            ArenaExtendedExpr::Trim { position, removal_char, string } => {
                "TRIM".hash(hasher);
                if let Some(pos) = position {
                    std::mem::discriminant(pos).hash(hasher);
                }
                if let Some(ch) = removal_char {
                    Self::hash_arena_expression(ch, hasher);
                }
                Self::hash_arena_expression(string, hasher);
            }

            ArenaExtendedExpr::Extract { field, expr } => {
                "EXTRACT".hash(hasher);
                std::mem::discriminant(field).hash(hasher);
                Self::hash_arena_expression(expr, hasher);
            }

            ArenaExtendedExpr::Like { expr, pattern, negated } => {
                "LIKE".hash(hasher);
                Self::hash_arena_expression(expr, hasher);
                Self::hash_arena_expression(pattern, hasher);
                negated.hash(hasher);
            }

            ArenaExtendedExpr::Exists { subquery, negated } => {
                "EXISTS".hash(hasher);
                Self::hash_arena_select(subquery, hasher);
                negated.hash(hasher);
            }

            ArenaExtendedExpr::QuantifiedComparison { expr, op, quantifier, subquery } => {
                "QUANTIFIED".hash(hasher);
                Self::hash_arena_expression(expr, hasher);
                std::mem::discriminant(op).hash(hasher);
                std::mem::discriminant(quantifier).hash(hasher);
                Self::hash_arena_select(subquery, hasher);
            }

            ArenaExtendedExpr::Interval {
                value,
                unit,
                leading_precision,
                fractional_precision,
            } => {
                "INTERVAL".hash(hasher);
                Self::hash_arena_expression(value, hasher);
                format!("{:?}", unit).hash(hasher);
                leading_precision.hash(hasher);
                fractional_precision.hash(hasher);
            }

            ArenaExtendedExpr::DuplicateKeyValue { column } => {
                "DUPLICATE_KEY_VALUE".hash(hasher);
                column.hash(hasher);
            }

            ArenaExtendedExpr::WindowFunction { function, over } => {
                "WINDOW_FUNCTION".hash(hasher);
                match function {
                    ArenaWindowFunctionSpec::Aggregate { name, args } => {
                        "AGGREGATE".hash(hasher);
                        name.hash(hasher);
                        for arg in args {
                            Self::hash_arena_expression(arg, hasher);
                        }
                    }
                    ArenaWindowFunctionSpec::Ranking { name, args } => {
                        "RANKING".hash(hasher);
                        name.hash(hasher);
                        for arg in args {
                            Self::hash_arena_expression(arg, hasher);
                        }
                    }
                    ArenaWindowFunctionSpec::Value { name, args } => {
                        "VALUE".hash(hasher);
                        name.hash(hasher);
                        for arg in args {
                            Self::hash_arena_expression(arg, hasher);
                        }
                    }
                }

                if let Some(ref partition_by) = over.partition_by {
                    for expr in partition_by {
                        Self::hash_arena_expression(expr, hasher);
                    }
                }
                if let Some(ref order_by) = over.order_by {
                    for item in order_by {
                        Self::hash_arena_expression(&item.expr, hasher);
                        std::mem::discriminant(&item.direction).hash(hasher);
                    }
                }
                if let Some(ref frame) = over.frame {
                    std::mem::discriminant(&frame.unit).hash(hasher);
                    std::mem::discriminant(&frame.start).hash(hasher);
                    if let Some(ref end) = frame.end {
                        std::mem::discriminant(end).hash(hasher);
                    }
                }
            }

            ArenaExtendedExpr::NextValue { sequence_name } => {
                "NEXT_VALUE".hash(hasher);
                sequence_name.hash(hasher);
            }

            ArenaExtendedExpr::MatchAgainst { columns, search_modifier, mode } => {
                "MATCH_AGAINST".hash(hasher);
                for col in columns {
                    col.hash(hasher);
                }
                Self::hash_arena_expression(search_modifier, hasher);
                std::mem::discriminant(mode).hash(hasher);
            }

            ArenaExtendedExpr::PseudoVariable { pseudo_table, column } => {
                "PSEUDO_VARIABLE".hash(hasher);
                std::mem::discriminant(pseudo_table).hash(hasher);
                column.hash(hasher);
            }

            ArenaExtendedExpr::SessionVariable { name } => {
                "SESSION_VARIABLE".hash(hasher);
                name.hash(hasher);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_same_query_same_signature() {
        let sig1 = QuerySignature::from_sql("SELECT * FROM users");
        let sig2 = QuerySignature::from_sql("SELECT * FROM users");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_whitespace_normalization() {
        let sig1 = QuerySignature::from_sql("SELECT * FROM users");
        let sig2 = QuerySignature::from_sql("SELECT  *  FROM  users");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_case_insensitive() {
        let sig1 = QuerySignature::from_sql("SELECT * FROM users");
        let sig2 = QuerySignature::from_sql("select * from users");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_different_queries_different_signature() {
        let sig1 = QuerySignature::from_sql("SELECT * FROM users");
        let sig2 = QuerySignature::from_sql("SELECT * FROM orders");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_different_literals_different_signature_string_based() {
        // Different literals create different signatures with string-based hashing
        let sig1 = QuerySignature::from_sql("SELECT col0 FROM tab WHERE col1 > 5");
        let sig2 = QuerySignature::from_sql("SELECT col0 FROM tab WHERE col1 > 10");
        // String-based hashing includes literals in the signature
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_ast_based_same_structure_different_literals() {
        use vibesql_ast::{
            BinaryOperator, Expression, FromClause, SelectItem, SelectStmt, Statement,
        };
        use vibesql_types::SqlValue;

        // SELECT col0 FROM tab WHERE col1 > 5
        let stmt1 = Statement::Select(Box::new(SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "col0".to_string() },
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "tab".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef { table: None, column: "col1".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(5))),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }));

        // SELECT col0 FROM tab WHERE col1 > 10 (different literal)
        let stmt2 = Statement::Select(Box::new(SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "col0".to_string() },
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "tab".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef { table: None, column: "col1".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(10))),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }));

        let sig1 = QuerySignature::from_ast(&stmt1);
        let sig2 = QuerySignature::from_ast(&stmt2);

        // AST-based signatures should be the same despite different literals
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_ast_based_different_structure() {
        use vibesql_ast::{
            BinaryOperator, Expression, FromClause, SelectItem, SelectStmt, Statement,
        };
        use vibesql_types::SqlValue;

        // SELECT col0 FROM tab WHERE col1 > 5
        let stmt1 = Statement::Select(Box::new(SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "col0".to_string() },
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "tab".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef { table: None, column: "col1".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(5))),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }));

        // SELECT col0 FROM tab WHERE col1 < 5 (different operator)
        let stmt2 = Statement::Select(Box::new(SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "col0".to_string() },
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "tab".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: Some(Expression::BinaryOp {
                op: BinaryOperator::LessThan, // Different operator!
                left: Box::new(Expression::ColumnRef { table: None, column: "col1".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(5))),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }));

        let sig1 = QuerySignature::from_ast(&stmt1);
        let sig2 = QuerySignature::from_ast(&stmt2);

        // Different structure should produce different signatures
        assert_ne!(sig1, sig2);
    }
}
