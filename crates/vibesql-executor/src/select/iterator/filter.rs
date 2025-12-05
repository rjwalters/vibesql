//! Filter iterator implementation

use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator, schema::CombinedSchema,
};

use super::RowIterator;

/// Iterator that filters rows based on a predicate expression
///
/// This iterator wraps another RowIterator and only yields rows that
/// satisfy the given predicate. Rows are evaluated lazily - predicates
/// are only evaluated for rows that are actually consumed.
///
/// # Predicate Evaluation
///
/// The predicate must evaluate to a boolean (or truthy value for SQLLogicTest compat):
/// - `true` or non-zero number: row passes
/// - `false`, `0`, or `NULL`: row is filtered out
///
/// # Example
///
/// ```text
/// let scan = TableScanIterator::new(schema.clone(), rows);
/// let predicate = Expression::Binary {
///     left: Box::new(Expression::ColumnRef { column: "age".to_string() }),
///     op: BinaryOp::GreaterThan,
///     right: Box::new(Expression::Literal(SqlValue::Integer(18))),
/// };
/// let filter = FilterIterator::new(scan, predicate, evaluator);
///
/// // Only rows where age > 18 are yielded
/// for row in filter {
///     println!("{:?}", row?);
/// }
/// ```
pub struct FilterIterator<'a, I: RowIterator> {
    source: I,
    predicate: vibesql_ast::Expression,
    evaluator: CombinedExpressionEvaluator<'a>,
}

impl<'a, I: RowIterator> FilterIterator<'a, I> {
    /// Create a new filter iterator
    ///
    /// # Arguments
    /// * `source` - The source iterator to filter
    /// * `predicate` - The expression to evaluate for each row
    /// * `evaluator` - The evaluator to use for predicate evaluation
    pub fn new(
        source: I,
        predicate: vibesql_ast::Expression,
        evaluator: CombinedExpressionEvaluator<'a>,
    ) -> Self {
        Self { source, predicate, evaluator }
    }

    /// Check if a value is "truthy" according to SQL/SQLLogicTest semantics
    ///
    /// Returns:
    /// - `Ok(true)` if the value is truthy (row should be included)
    /// - `Ok(false)` if the value is falsy (row should be filtered out)
    /// - `Err(...)` if the value is not a valid predicate type
    fn is_truthy(value: &vibesql_types::SqlValue) -> Result<bool, ExecutorError> {
        match value {
            vibesql_types::SqlValue::Boolean(b) => Ok(*b),
            vibesql_types::SqlValue::Null => Ok(false), // NULL is falsy
            // SQLLogicTest compatibility: treat integers as truthy/falsy
            vibesql_types::SqlValue::Integer(0)
            | vibesql_types::SqlValue::Smallint(0)
            | vibesql_types::SqlValue::Bigint(0) => Ok(false),
            vibesql_types::SqlValue::Integer(_)
            | vibesql_types::SqlValue::Smallint(_)
            | vibesql_types::SqlValue::Bigint(_) => Ok(true),
            vibesql_types::SqlValue::Float(f) | vibesql_types::SqlValue::Real(f) if *f == 0.0 => {
                Ok(false)
            }
            vibesql_types::SqlValue::Float(_) | vibesql_types::SqlValue::Real(_) => Ok(true),
            vibesql_types::SqlValue::Double(f) if *f == 0.0 => Ok(false),
            vibesql_types::SqlValue::Double(_) => Ok(true),
            other => Err(ExecutorError::InvalidWhereClause(format!(
                "Filter expression must evaluate to boolean, got: {:?}",
                other
            ))),
        }
    }
}

impl<'a, I: RowIterator> Iterator for FilterIterator<'a, I> {
    type Item = Result<vibesql_storage::Row, ExecutorError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Get the next row from source
            let row = match self.source.next()? {
                Ok(row) => row,
                Err(e) => return Some(Err(e)),
            };

            // Evaluate the predicate for this row
            match self.evaluator.eval(&self.predicate, &row) {
                Ok(value) => match Self::is_truthy(&value) {
                    Ok(true) => return Some(Ok(row)), // Row passes filter
                    Ok(false) => continue,            // Row filtered out, try next
                    Err(e) => return Some(Err(e)),    // Type error
                },
                Err(e) => return Some(Err(e)), // Evaluation error
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Filter can only reduce the count, never increase it
        // Lower bound is 0 (might filter everything)
        // Upper bound is the source's upper bound (might filter nothing)
        let (_, upper) = self.source.size_hint();
        (0, upper)
    }
}

impl<'a, I: RowIterator> RowIterator for FilterIterator<'a, I> {
    fn schema(&self) -> &CombinedSchema {
        self.source.schema()
    }

    fn row_size_hint(&self) -> (usize, Option<usize>) {
        // Same logic as size_hint
        let (_, upper) = self.source.row_size_hint();
        (0, upper)
    }
}
