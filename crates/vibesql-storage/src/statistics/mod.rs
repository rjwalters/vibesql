//! Table and column statistics for query optimization
//!
//! This module provides statistics collection and estimation for:
//! - Table cardinality (row counts)
//! - Column cardinality (distinct values)
//! - Value distributions (min/max, most common values)
//! - Selectivity estimation for predicates
//! - Cost estimation for access methods
//! - Histogram-based selectivity (Phase 5.1)
//! - Sampling for large tables (Phase 5.2)
//!
//! Statistics are used by the query optimizer for:
//! - JOIN order selection
//! - Index selection (cost-based)
//! - Predicate pushdown prioritization
//! - Cost estimation

mod column;
mod cost;
mod histogram;
mod sampling;
mod table;

pub use column::ColumnStatistics;
pub use cost::{
    estimate_row_size, estimate_type_size, AccessMethod, CostEstimator, TableIndexInfo,
    BASE_ROW_SIZE, MAX_WAL_SIZE_FACTOR,
};
pub use histogram::{BucketStrategy, Histogram, HistogramBucket};
pub use sampling::{SampleMetadata, SampleSize, SamplingConfig, SamplingMethod};
pub use table::TableStatistics;
