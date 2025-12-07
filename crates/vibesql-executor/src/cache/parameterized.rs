//! Parameterized plan support for literal extraction and binding
//!
//! Allows queries with different literal values to share the same execution plan
//! by extracting literals into parameters and binding them at execution time.

use vibesql_ast::{Expression, Statement};
use vibesql_types::SqlValue;

/// Type-safe literal value representation
#[derive(Clone, Debug, PartialEq)]
pub enum LiteralValue {
    Integer(i64),
    Smallint(i16),
    Bigint(i64),
    Unsigned(u64),
    Numeric(f64),
    Float(f32),
    Real(f32),
    Double(f64),
    Character(String),
    Varchar(String),
    Boolean(bool),
    Date(String),
    Time(String),
    Timestamp(String),
    Null,
}

impl LiteralValue {
    /// Convert from SqlValue
    pub fn from_sql_value(value: &SqlValue) -> Self {
        match value {
            SqlValue::Integer(n) => LiteralValue::Integer(*n),
            SqlValue::Smallint(n) => LiteralValue::Smallint(*n),
            SqlValue::Bigint(n) => LiteralValue::Bigint(*n),
            SqlValue::Unsigned(n) => LiteralValue::Unsigned(*n),
            SqlValue::Numeric(n) => LiteralValue::Numeric(*n),
            SqlValue::Float(n) => LiteralValue::Float(*n),
            SqlValue::Real(n) => LiteralValue::Real(*n),
            SqlValue::Double(n) => LiteralValue::Double(*n),
            SqlValue::Character(s) => LiteralValue::Character(s.to_string()),
            SqlValue::Varchar(s) => LiteralValue::Varchar(s.to_string()),
            SqlValue::Boolean(b) => LiteralValue::Boolean(*b),
            SqlValue::Date(s) => LiteralValue::Date(s.to_string()),
            SqlValue::Time(s) => LiteralValue::Time(s.to_string()),
            SqlValue::Timestamp(s) => LiteralValue::Timestamp(s.to_string()),
            SqlValue::Interval(s) => LiteralValue::Varchar(s.to_string()), /* Treat interval as */
            // string for now
            SqlValue::Vector(v) => {
                // Treat vector as string representation for now
                let formatted: Vec<String> = v.iter().map(|f| f.to_string()).collect();
                LiteralValue::Varchar(format!("[{}]", formatted.join(", ")))
            }
            SqlValue::Null => LiteralValue::Null,
        }
    }

    /// Convert to SQL string representation
    pub fn to_sql(&self) -> String {
        match self {
            LiteralValue::Integer(n) => n.to_string(),
            LiteralValue::Smallint(n) => n.to_string(),
            LiteralValue::Bigint(n) => n.to_string(),
            LiteralValue::Unsigned(n) => n.to_string(),
            LiteralValue::Numeric(n) => n.to_string(),
            LiteralValue::Float(n) => n.to_string(),
            LiteralValue::Real(n) => n.to_string(),
            LiteralValue::Double(n) => n.to_string(),
            LiteralValue::Character(s) | LiteralValue::Varchar(s) => {
                format!("'{}'", s.replace("'", "''"))
            }
            LiteralValue::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
            LiteralValue::Date(s) => format!("DATE '{}'", s),
            LiteralValue::Time(s) => format!("TIME '{}'", s),
            LiteralValue::Timestamp(s) => format!("TIMESTAMP '{}'", s),
            LiteralValue::Null => "NULL".to_string(),
        }
    }
}

/// Position of a parameter in a query
#[derive(Clone, Debug)]
pub struct ParameterPosition {
    pub position: usize,
    pub context: String,
}

/// Query plan with parameter placeholders
#[derive(Clone, Debug)]
pub struct ParameterizedPlan {
    pub normalized_query: String,
    pub param_positions: Vec<ParameterPosition>,
    pub literal_values: Vec<LiteralValue>,
}

impl ParameterizedPlan {
    /// Create a new parameterized plan
    pub fn new(
        normalized_query: String,
        param_positions: Vec<ParameterPosition>,
        literal_values: Vec<LiteralValue>,
    ) -> Self {
        Self { normalized_query, param_positions, literal_values }
    }

    /// Bind literal values to create executable query
    pub fn bind(&self, values: &[LiteralValue]) -> Result<String, String> {
        if values.len() != self.param_positions.len() {
            return Err(format!(
                "Expected {} parameters, got {}",
                self.param_positions.len(),
                values.len()
            ));
        }

        let mut result = self.normalized_query.clone();
        let mut offset = 0;

        for (i, value) in values.iter().enumerate() {
            if let Some(_pos) = self.param_positions.get(i) {
                let sql_value = value.to_sql();
                let placeholder = "?";

                if let Some(idx) = result[offset..].find(placeholder) {
                    let insert_pos = offset + idx;
                    result.replace_range(insert_pos..insert_pos + 1, &sql_value);
                    offset = insert_pos + sql_value.len();
                }
            }
        }

        Ok(result)
    }

    /// Get a comparison key for the plan
    pub fn comparison_key(&self) -> String {
        self.normalized_query.clone()
    }
}

/// Utility for extracting literals from AST
pub struct LiteralExtractor;

impl LiteralExtractor {
    /// Extract literals from a statement AST in order of appearance
    pub fn extract(stmt: &Statement) -> Vec<LiteralValue> {
        let mut literals = Vec::new();
        Self::extract_from_statement(stmt, &mut literals);
        literals
    }

    fn extract_from_statement(stmt: &Statement, literals: &mut Vec<LiteralValue>) {
        match stmt {
            Statement::Select(select) => Self::extract_from_select(select, literals),
            Statement::Insert(insert) => {
                // Extract from VALUES or SELECT source
                match &insert.source {
                    vibesql_ast::InsertSource::Values(rows) => {
                        for row in rows {
                            for expr in row {
                                Self::extract_from_expression(expr, literals);
                            }
                        }
                    }
                    vibesql_ast::InsertSource::Select(select) => {
                        Self::extract_from_select(select, literals);
                    }
                }
            }
            Statement::Update(update) => {
                // Extract from assignments
                for assignment in &update.assignments {
                    Self::extract_from_expression(&assignment.value, literals);
                }
                // Extract from WHERE clause
                if let Some(ref where_clause) = update.where_clause {
                    match where_clause {
                        vibesql_ast::WhereClause::Condition(expr) => {
                            Self::extract_from_expression(expr, literals);
                        }
                        vibesql_ast::WhereClause::CurrentOf(_) => {
                            // No literals in positioned update/delete
                        }
                    }
                }
            }
            Statement::Delete(delete) => {
                // Extract from WHERE clause
                if let Some(ref where_clause) = delete.where_clause {
                    match where_clause {
                        vibesql_ast::WhereClause::Condition(expr) => {
                            Self::extract_from_expression(expr, literals);
                        }
                        vibesql_ast::WhereClause::CurrentOf(_) => {
                            // No literals in positioned update/delete
                        }
                    }
                }
            }
            // Other statement types don't have literals we need to extract
            _ => {}
        }
    }

    fn extract_from_select(select: &vibesql_ast::SelectStmt, literals: &mut Vec<LiteralValue>) {
        // Extract from SELECT items
        for item in &select.select_list {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                Self::extract_from_expression(expr, literals);
            }
        }

        // Extract from FROM clause
        if let Some(ref from) = select.from {
            Self::extract_from_from_clause(from, literals);
        }

        // Extract from WHERE
        if let Some(ref where_clause) = select.where_clause {
            Self::extract_from_expression(where_clause, literals);
        }

        // Extract from GROUP BY
        if let Some(ref group_by) = select.group_by {
            Self::extract_from_group_by(group_by, literals);
        }

        // Extract from HAVING
        if let Some(ref having) = select.having {
            Self::extract_from_expression(having, literals);
        }

        // Extract from ORDER BY
        if let Some(ref order_by) = select.order_by {
            for item in order_by {
                Self::extract_from_expression(&item.expr, literals);
            }
        }
    }

    fn extract_from_from_clause(from: &vibesql_ast::FromClause, literals: &mut Vec<LiteralValue>) {
        match from {
            vibesql_ast::FromClause::Join { left, right, condition, .. } => {
                Self::extract_from_from_clause(left, literals);
                Self::extract_from_from_clause(right, literals);
                if let Some(expr) = condition {
                    Self::extract_from_expression(expr, literals);
                }
            }
            vibesql_ast::FromClause::Subquery { query, .. } => {
                Self::extract_from_select(query, literals);
            }
            vibesql_ast::FromClause::Table { .. } => {
                // No literals in table references
            }
        }
    }

    fn extract_from_group_by(
        group_by: &vibesql_ast::GroupByClause,
        literals: &mut Vec<LiteralValue>,
    ) {
        match group_by {
            vibesql_ast::GroupByClause::Simple(exprs) => {
                for expr in exprs {
                    Self::extract_from_expression(expr, literals);
                }
            }
            vibesql_ast::GroupByClause::Rollup(elements)
            | vibesql_ast::GroupByClause::Cube(elements) => {
                Self::extract_from_grouping_elements(elements, literals);
            }
            vibesql_ast::GroupByClause::GroupingSets(sets) => {
                Self::extract_from_grouping_sets(sets, literals);
            }
            vibesql_ast::GroupByClause::Mixed(items) => {
                for item in items {
                    match item {
                        vibesql_ast::MixedGroupingItem::Simple(expr) => {
                            Self::extract_from_expression(expr, literals);
                        }
                        vibesql_ast::MixedGroupingItem::Rollup(elements)
                        | vibesql_ast::MixedGroupingItem::Cube(elements) => {
                            Self::extract_from_grouping_elements(elements, literals);
                        }
                        vibesql_ast::MixedGroupingItem::GroupingSets(sets) => {
                            Self::extract_from_grouping_sets(sets, literals);
                        }
                    }
                }
            }
        }
    }

    fn extract_from_grouping_elements(
        elements: &[vibesql_ast::GroupingElement],
        literals: &mut Vec<LiteralValue>,
    ) {
        for element in elements {
            match element {
                vibesql_ast::GroupingElement::Single(expr) => {
                    Self::extract_from_expression(expr, literals);
                }
                vibesql_ast::GroupingElement::Composite(exprs) => {
                    for expr in exprs {
                        Self::extract_from_expression(expr, literals);
                    }
                }
            }
        }
    }

    fn extract_from_grouping_sets(
        sets: &[vibesql_ast::GroupingSet],
        literals: &mut Vec<LiteralValue>,
    ) {
        for set in sets {
            for expr in &set.columns {
                Self::extract_from_expression(expr, literals);
            }
        }
    }

    fn extract_from_expression(expr: &Expression, literals: &mut Vec<LiteralValue>) {
        match expr {
            Expression::Literal(value) => {
                literals.push(LiteralValue::from_sql_value(value));
            }

            Expression::BinaryOp { left, right, .. } => {
                Self::extract_from_expression(left, literals);
                Self::extract_from_expression(right, literals);
            }

            Expression::Conjunction(children) | Expression::Disjunction(children) => {
                for child in children {
                    Self::extract_from_expression(child, literals);
                }
            }

            Expression::UnaryOp { expr, .. } => {
                Self::extract_from_expression(expr, literals);
            }

            Expression::Function { args, .. } => {
                for arg in args {
                    Self::extract_from_expression(arg, literals);
                }
            }

            Expression::AggregateFunction { args, .. } => {
                for arg in args {
                    Self::extract_from_expression(arg, literals);
                }
            }

            Expression::IsNull { expr, .. } => {
                Self::extract_from_expression(expr, literals);
            }

            Expression::Case { operand, when_clauses, else_result } => {
                if let Some(ref op) = operand {
                    Self::extract_from_expression(op, literals);
                }
                for when in when_clauses {
                    for cond in &when.conditions {
                        Self::extract_from_expression(cond, literals);
                    }
                    Self::extract_from_expression(&when.result, literals);
                }
                if let Some(ref else_expr) = else_result {
                    Self::extract_from_expression(else_expr, literals);
                }
            }

            Expression::ScalarSubquery(subquery) => {
                Self::extract_from_select(subquery, literals);
            }

            Expression::In { expr, subquery, .. } => {
                Self::extract_from_expression(expr, literals);
                Self::extract_from_select(subquery, literals);
            }

            Expression::InList { expr, values, .. } => {
                Self::extract_from_expression(expr, literals);
                for val in values {
                    Self::extract_from_expression(val, literals);
                }
            }

            Expression::Between { expr, low, high, .. } => {
                Self::extract_from_expression(expr, literals);
                Self::extract_from_expression(low, literals);
                Self::extract_from_expression(high, literals);
            }

            Expression::Cast { expr, .. } => {
                Self::extract_from_expression(expr, literals);
            }

            Expression::Position { substring, string, .. } => {
                Self::extract_from_expression(substring, literals);
                Self::extract_from_expression(string, literals);
            }

            Expression::Trim { removal_char, string, .. } => {
                if let Some(ref ch) = removal_char {
                    Self::extract_from_expression(ch, literals);
                }
                Self::extract_from_expression(string, literals);
            }

            Expression::Extract { expr, .. } => {
                Self::extract_from_expression(expr, literals);
            }

            Expression::Like { expr, pattern, .. } => {
                Self::extract_from_expression(expr, literals);
                Self::extract_from_expression(pattern, literals);
            }

            Expression::Exists { subquery, .. } => {
                Self::extract_from_select(subquery, literals);
            }

            Expression::QuantifiedComparison { expr, subquery, .. } => {
                Self::extract_from_expression(expr, literals);
                Self::extract_from_select(subquery, literals);
            }

            Expression::WindowFunction { function, over } => {
                // Extract from function arguments
                match function {
                    vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                    | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                    | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
                        for arg in args {
                            Self::extract_from_expression(arg, literals);
                        }
                    }
                }

                // Extract from PARTITION BY
                if let Some(ref partition_by) = over.partition_by {
                    for expr in partition_by {
                        Self::extract_from_expression(expr, literals);
                    }
                }

                // Extract from ORDER BY
                if let Some(ref order_by) = over.order_by {
                    for item in order_by {
                        Self::extract_from_expression(&item.expr, literals);
                    }
                }

                // Extract from frame bounds
                if let Some(ref frame) = over.frame {
                    if let vibesql_ast::FrameBound::Preceding(expr)
                    | vibesql_ast::FrameBound::Following(expr) = &frame.start
                    {
                        Self::extract_from_expression(expr, literals);
                    }
                    if let Some(
                        vibesql_ast::FrameBound::Preceding(expr)
                        | vibesql_ast::FrameBound::Following(expr),
                    ) = &frame.end
                    {
                        Self::extract_from_expression(expr, literals);
                    }
                }
            }

            // These expressions don't contain literals
            Expression::ColumnRef { .. }
            | Expression::Placeholder(_)
            | Expression::NumberedPlaceholder(_)
            | Expression::NamedPlaceholder(_)
            | Expression::PseudoVariable { .. }
            | Expression::Wildcard
            | Expression::CurrentDate
            | Expression::CurrentTime { .. }
            | Expression::CurrentTimestamp { .. }
            | Expression::Interval { .. }
            | Expression::Default
            | Expression::DuplicateKeyValue { .. }
            | Expression::NextValue { .. }
            | Expression::MatchAgainst { .. }
            | Expression::SessionVariable { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_value_to_string() {
        assert_eq!(LiteralValue::Integer(42).to_sql(), "42");
        assert_eq!(LiteralValue::Varchar("hello".to_string()).to_sql(), "'hello'");
        assert_eq!(LiteralValue::Boolean(true).to_sql(), "true");
        assert_eq!(LiteralValue::Null.to_sql(), "NULL");
    }

    #[test]
    fn test_literal_value_string_escape() {
        assert_eq!(LiteralValue::Varchar("it's".to_string()).to_sql(), "'it''s'");
    }

    #[test]
    fn test_literal_extraction_simple() {
        use vibesql_ast::{
            BinaryOperator, Expression, FromClause, SelectItem, SelectStmt, Statement,
        };

        // SELECT col0 FROM tab WHERE col1 > 25 AND col2 = 'John'
        let stmt = Statement::Select(Box::new(SelectStmt {
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
                op: BinaryOperator::And,
                left: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::GreaterThan,
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "col1".to_string(),
                    }),
                    right: Box::new(Expression::Literal(SqlValue::Integer(25))),
                }),
                right: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::Equal,
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "col2".to_string(),
                    }),
                    right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("John")))),
                }),
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }));

        let literals = LiteralExtractor::extract(&stmt);

        assert_eq!(literals.len(), 2);
        assert_eq!(literals[0], LiteralValue::Integer(25));
        assert_eq!(literals[1], LiteralValue::Varchar("John".to_string()));
    }

    #[test]
    fn test_literal_extraction_in_list() {
        use vibesql_ast::{Expression, FromClause, SelectItem, SelectStmt, Statement};

        // SELECT * FROM tab WHERE id IN (1, 2, 3)
        let stmt = Statement::Select(Box::new(SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                name: "tab".to_string(),
                alias: None,
                column_aliases: None,
            }),
            where_clause: Some(Expression::InList {
                expr: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                values: vec![
                    Expression::Literal(SqlValue::Integer(1)),
                    Expression::Literal(SqlValue::Integer(2)),
                    Expression::Literal(SqlValue::Integer(3)),
                ],
                negated: false,
            }),
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }));

        let literals = LiteralExtractor::extract(&stmt);

        assert_eq!(literals.len(), 3);
        assert_eq!(literals[0], LiteralValue::Integer(1));
        assert_eq!(literals[1], LiteralValue::Integer(2));
        assert_eq!(literals[2], LiteralValue::Integer(3));
    }

    #[test]
    fn test_parameterized_plan_bind() {
        let plan = ParameterizedPlan::new(
            "SELECT * FROM users WHERE age > ?".to_string(),
            vec![ParameterPosition { position: 40, context: "age".to_string() }],
            vec![LiteralValue::Integer(25)],
        );

        let result = plan.bind(&[LiteralValue::Integer(30)]).unwrap();
        assert_eq!(result, "SELECT * FROM users WHERE age > 30");
    }

    #[test]
    fn test_parameterized_plan_bind_string() {
        let plan = ParameterizedPlan::new(
            "SELECT * FROM users WHERE name = ?".to_string(),
            vec![ParameterPosition { position: 40, context: "name".to_string() }],
            vec![LiteralValue::Varchar("John".to_string())],
        );

        let result = plan.bind(&[LiteralValue::Varchar("Jane".to_string())]).unwrap();
        assert_eq!(result, "SELECT * FROM users WHERE name = 'Jane'");
    }

    #[test]
    fn test_parameterized_plan_bind_error() {
        let plan = ParameterizedPlan::new(
            "SELECT * FROM users WHERE age > ?".to_string(),
            vec![ParameterPosition { position: 40, context: "age".to_string() }],
            vec![LiteralValue::Integer(25)],
        );

        let result = plan.bind(&[LiteralValue::Integer(30), LiteralValue::Integer(40)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_comparison_key() {
        let plan = ParameterizedPlan::new("SELECT * FROM users".to_string(), vec![], vec![]);

        assert_eq!(plan.comparison_key(), "SELECT * FROM users");
    }
}
