//! Unified Execution Pipeline
//!
//! This module provides a trait-based abstraction for query execution, enabling
//! multiple execution strategies (row-oriented, columnar, native columnar) to
//! share common logic while preserving specialized hot paths.
//!
//! ## Architecture
//!
//! The `ExecutionPipeline` trait defines the core operations for query execution:
//! - Filter (WHERE clause)
//! - Projection (SELECT columns)
//! - Aggregation (GROUP BY, aggregate functions)
//! - Limit/Offset
//!
//! Each execution strategy implements this trait with its own optimized logic:
//! - `RowOrientedPipeline`: Traditional row-by-row execution
//! - `ColumnarPipeline`: SIMD-accelerated batch execution (with row-to-batch conversion)
//! - `NativeColumnarPipeline`: Zero-copy SIMD execution directly from columnar storage
//!
//! ## Benefits
//!
//! 1. **Reduced Duplication**: Common logic (evaluator setup, limit/offset) is shared
//! 2. **Extensibility**: New strategies (GPU, distributed) can implement the trait
//! 3. **Maintainability**: Single interface for all execution paths
//! 4. **Performance**: Trait methods can be specialized with `#[inline]` for hot paths
//!
//! ## Usage
//!
//! ```text
//! use vibesql_executor::pipeline::{ExecutionPipeline, ExecutionContext};
//!
//! let ctx = ExecutionContext::new(schema, database, ...);
//! let pipeline = RowOrientedPipeline::new(&ctx);
//!
//! let filtered = pipeline.apply_filter(input, predicate, &ctx)?;
//! let projected = pipeline.apply_projection(filtered, select_items, &ctx)?;
//! let result = pipeline.apply_limit_offset(projected, limit, offset)?;
//! ```

mod columnar;
mod context;
mod native_columnar;
mod row_oriented;
mod types;

pub use columnar::ColumnarPipeline;
pub use context::ExecutionContext;
pub use native_columnar::NativeColumnarPipeline;
pub use row_oriented::RowOrientedPipeline;
pub use types::{PipelineInput, PipelineOutput};

use crate::errors::ExecutorError;
use crate::evaluator::CombinedExpressionEvaluator;
use vibesql_ast::{Expression, SelectItem};

/// Unified execution pipeline trait for all query execution strategies.
///
/// Each method represents a stage in query execution. Implementations can
/// override methods to provide optimized logic for their execution model.
///
/// # Type Parameters
///
/// The trait is designed to work with different input/output representations:
/// - Row-oriented: Works with `Vec<Row>`
/// - Columnar: Works with `ColumnarBatch`
/// - Native columnar: Works directly with table storage
pub trait ExecutionPipeline {
    /// Create an evaluator with appropriate context for this pipeline.
    ///
    /// This factory method allows each pipeline implementation to create
    /// evaluators configured for their specific execution model (CTE context,
    /// outer row for correlation, procedural context, etc.).
    ///
    /// # Arguments
    /// * `ctx` - Execution context containing schema, database, and optional contexts
    ///
    /// # Returns
    /// A `CombinedExpressionEvaluator` configured for this pipeline's execution model
    ///
    /// # Default Implementation
    ///
    /// The default implementation delegates to `ctx.create_evaluator()`, which handles
    /// all the context variants (CTE, outer row, procedural, windows).
    #[inline]
    fn create_evaluator<'a>(
        &self,
        ctx: &'a ExecutionContext<'a>,
    ) -> CombinedExpressionEvaluator<'a> {
        ctx.create_evaluator()
    }

    /// Apply WHERE clause filtering to the input data.
    ///
    /// # Arguments
    /// * `input` - The input data (rows or batches)
    /// * `predicate` - Optional WHERE clause expression
    /// * `ctx` - Execution context with schema, database, and evaluator
    ///
    /// # Returns
    /// Filtered output data
    fn apply_filter(
        &self,
        input: PipelineInput<'_>,
        predicate: Option<&Expression>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError>;

    /// Apply SELECT projection to transform columns.
    ///
    /// # Arguments
    /// * `input` - The filtered data
    /// * `select_items` - The SELECT list items
    /// * `ctx` - Execution context
    ///
    /// # Returns
    /// Projected output data
    fn apply_projection(
        &self,
        input: PipelineInput<'_>,
        select_items: &[SelectItem],
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError>;

    /// Execute aggregation with optional GROUP BY.
    ///
    /// # Arguments
    /// * `input` - The filtered/projected data
    /// * `select_items` - SELECT list (may contain aggregate functions)
    /// * `group_by` - Optional GROUP BY expressions
    /// * `having` - Optional HAVING clause
    /// * `ctx` - Execution context
    ///
    /// # Returns
    /// Aggregated output data
    fn apply_aggregation(
        &self,
        input: PipelineInput<'_>,
        select_items: &[SelectItem],
        group_by: Option<&[Expression]>,
        having: Option<&Expression>,
        ctx: &ExecutionContext<'_>,
    ) -> Result<PipelineOutput, ExecutorError>;

    /// Apply LIMIT and OFFSET to the results.
    ///
    /// This has a default implementation that works for all strategies.
    ///
    /// # Arguments
    /// * `input` - The aggregated/projected data
    /// * `limit` - Optional maximum number of rows
    /// * `offset` - Optional number of rows to skip
    ///
    /// # Returns
    /// Final result rows
    #[inline]
    fn apply_limit_offset(
        &self,
        input: PipelineOutput,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        let mut rows = input.into_rows();

        // Apply offset
        if let Some(off) = offset {
            let off = off as usize;
            if off >= rows.len() {
                return Ok(Vec::new());
            }
            rows = rows.into_iter().skip(off).collect();
        }

        // Apply limit
        if let Some(lim) = limit {
            rows.truncate(lim as usize);
        }

        Ok(rows)
    }

    /// Check if the pipeline supports a specific query pattern.
    ///
    /// This allows strategies to indicate they cannot handle certain queries,
    /// enabling fallback to a more general strategy.
    ///
    /// # Arguments
    /// * `has_aggregation` - Whether the query has aggregate functions
    /// * `has_group_by` - Whether the query has GROUP BY
    /// * `has_joins` - Whether the query has JOINs
    ///
    /// # Returns
    /// `true` if this pipeline can handle the query, `false` for fallback
    fn supports_query_pattern(
        &self,
        has_aggregation: bool,
        has_group_by: bool,
        has_joins: bool,
    ) -> bool;

    /// Get the name of this pipeline for debugging/logging.
    fn name(&self) -> &'static str;
}
