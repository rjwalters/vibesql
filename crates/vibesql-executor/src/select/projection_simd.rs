//! Batch projection for SELECT expressions
//!
//! This module provides batch evaluation of SELECT projection expressions.
//! Currently returns None to fall back to row-by-row projection.

use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator, schema::CombinedSchema,
};
use std::collections::HashMap;
use vibesql_ast::SelectItem;
use vibesql_storage::{QueryBufferPool, Row};

use super::window::WindowFunctionKey;

/// Attempt batch projection for SELECT expressions
///
/// Currently returns None to fall back to row-by-row projection.
/// Row-by-row projection is efficient for most queries.
#[allow(unused_variables)]
pub fn try_batch_project_simd(
    rows: &[Row],
    columns: &[SelectItem],
    evaluator: &CombinedExpressionEvaluator,
    schema: &CombinedSchema,
    window_mapping: &Option<HashMap<WindowFunctionKey, usize>>,
    buffer_pool: &QueryBufferPool,
) -> Result<Option<Vec<Row>>, ExecutorError> {
    // Fall back to row-by-row projection
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{evaluator::CombinedExpressionEvaluator, schema::CombinedSchema};
    use vibesql_ast::SelectItem;
    use vibesql_storage::{QueryBufferPool, Row};
    use vibesql_types::{DataType, SqlValue};

    fn create_test_evaluator() -> CombinedExpressionEvaluator<'static> {
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let columns = vec![ColumnSchema::new("a".to_string(), DataType::Bigint, false)];
        let table_schema = TableSchema::new("test".to_string(), columns);

        let schema =
            Box::leak(Box::new(CombinedSchema::from_table("test".to_string(), table_schema)));
        CombinedExpressionEvaluator::new(schema)
    }

    fn create_test_schema() -> CombinedSchema {
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let columns = vec![ColumnSchema::new("a".to_string(), DataType::Bigint, false)];
        let table_schema = TableSchema::new("test".to_string(), columns);

        CombinedSchema::from_table("test".to_string(), table_schema)
    }

    #[test]
    fn test_try_batch_project_simd_returns_none() {
        let rows: Vec<Row> = (0..100).map(|i| Row::new(vec![SqlValue::Bigint(i as i64)])).collect();

        let columns = vec![SelectItem::Expression {
            expr: vibesql_ast::Expression::Literal(SqlValue::Integer(42)),
            alias: None,
        }];

        let evaluator = create_test_evaluator();
        let schema = create_test_schema();
        let buffer_pool = QueryBufferPool::new();

        let result =
            try_batch_project_simd(&rows, &columns, &evaluator, &schema, &None, &buffer_pool);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }
}
