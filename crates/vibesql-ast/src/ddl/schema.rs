//! Schema, catalog, and database object DDL operations
//!
//! This module contains AST nodes for:
//! - CREATE/DROP SCHEMA
//! - CREATE/DROP VIEW
//! - CREATE/DROP INDEX
//! - CREATE/DROP ROLE
//! - SET SCHEMA/CATALOG/NAMES/TIME ZONE
//! - CREATE/DROP TRIGGER

use crate::Expression;

/// CREATE SCHEMA statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSchemaStmt {
    pub schema_name: String,
    pub if_not_exists: bool,
    pub schema_elements: Vec<SchemaElement>,
}

/// Schema element that can be included in CREATE SCHEMA
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaElement {
    CreateTable(super::table::CreateTableStmt),
    // Future: CreateView, Grant, etc.
}

/// DROP SCHEMA statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropSchemaStmt {
    pub schema_name: String,
    pub if_exists: bool,
    pub cascade: bool,
}

/// SET SCHEMA statement
#[derive(Debug, Clone, PartialEq)]
pub struct SetSchemaStmt {
    pub schema_name: String,
}

/// SET CATALOG statement
#[derive(Debug, Clone, PartialEq)]
pub struct SetCatalogStmt {
    pub catalog_name: String,
}

/// SET NAMES statement
#[derive(Debug, Clone, PartialEq)]
pub struct SetNamesStmt {
    pub charset_name: String,
    pub collation: Option<String>,
}

/// SET TIME ZONE statement
#[derive(Debug, Clone, PartialEq)]
pub struct SetTimeZoneStmt {
    pub zone: TimeZoneSpec,
}

/// Time zone specification for SET TIME ZONE
#[derive(Debug, Clone, PartialEq)]
pub enum TimeZoneSpec {
    Local,
    Interval(String), // e.g., "+05:00"
}

/// SET variable statement (MySQL/PostgreSQL extension)
/// Handles: SET [GLOBAL | SESSION] variable_name = value
#[derive(Debug, Clone, PartialEq)]
pub struct SetVariableStmt {
    pub scope: VariableScope,
    pub variable: String,
    pub value: Expression,
}

/// Variable scope for SET statements
#[derive(Debug, Clone, PartialEq)]
pub enum VariableScope {
    Session, // Default or explicit SESSION
    Global,  // GLOBAL keyword
}

/// CREATE ROLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateRoleStmt {
    pub role_name: String,
}

/// DROP ROLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropRoleStmt {
    pub role_name: String,
}

/// CREATE VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStmt {
    pub view_name: String,
    pub columns: Option<Vec<String>>,
    pub query: Box<crate::SelectStmt>,
    pub with_check_option: bool,
    pub or_replace: bool,
    /// Whether this is a temporary view (CREATE TEMP VIEW or CREATE TEMPORARY VIEW)
    pub temporary: bool,
}

/// DROP VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropViewStmt {
    pub view_name: String,
    pub if_exists: bool,
    pub cascade: bool,
    /// Whether RESTRICT was explicitly specified.
    /// When neither CASCADE nor RESTRICT is specified, we use SQLite-compatible
    /// behavior (allow dropping views even if dependents exist).
    /// When RESTRICT is explicit, we enforce dependency checks.
    pub restrict: bool,
}

/// CREATE TRIGGER statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTriggerStmt {
    pub trigger_name: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    pub table_name: String,
    pub granularity: TriggerGranularity,
    pub when_condition: Option<Box<Expression>>,
    pub triggered_action: TriggerAction,
}

/// Trigger timing: BEFORE | AFTER | INSTEAD OF
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// Trigger event: INSERT | UPDATE | DELETE
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerEvent {
    Insert,
    Update(Option<Vec<String>>), // Optional column list for UPDATE OF
    Delete,
}

/// Trigger granularity: FOR EACH ROW | FOR EACH STATEMENT
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerGranularity {
    Row,       // FOR EACH ROW
    Statement, // FOR EACH STATEMENT (default in SQL:1999)
}

/// Triggered action (procedural SQL)
#[derive(Debug, Clone, PartialEq)]
pub enum TriggerAction {
    /// For initial implementation, store raw SQL
    RawSql(String),
    // Future: Add procedural SQL statement support
    // Statements(Vec<Statement>),
}

/// DROP TRIGGER statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTriggerStmt {
    pub trigger_name: String,
    pub cascade: bool,
}

/// ALTER TRIGGER statement
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTriggerStmt {
    pub trigger_name: String,
    pub action: AlterTriggerAction,
}

/// ALTER TRIGGER action
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTriggerAction {
    Enable,
    Disable,
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt {
    pub if_not_exists: bool,
    pub index_name: String,
    pub table_name: String,
    pub index_type: IndexType,
    pub columns: Vec<IndexColumn>,
}

/// Index type specification
#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    /// Standard B-tree index (default)
    BTree { unique: bool },
    /// FULLTEXT index for full-text search
    Fulltext,
    /// SPATIAL index for spatial/geometric data (R-tree)
    Spatial,
    /// IVFFlat index for approximate nearest neighbor search on vectors
    IVFFlat {
        /// Distance metric to use for similarity calculations
        metric: VectorDistanceMetric,
        /// Number of clusters/lists for partitioning (default: 100)
        lists: u32,
    },
    /// HNSW index for high-performance approximate nearest neighbor search
    Hnsw {
        /// Distance metric to use for similarity calculations
        metric: VectorDistanceMetric,
        /// Maximum number of connections per node (default: 16)
        m: u32,
        /// Size of dynamic candidate list during construction (default: 64)
        ef_construction: u32,
    },
}

/// Distance metric for vector index operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorDistanceMetric {
    /// Euclidean distance (L2 norm)
    L2,
    /// Cosine similarity (1 - cosine distance)
    Cosine,
    /// Inner product (dot product) - negative for similarity
    InnerProduct,
}

/// Index column specification
#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn {
    pub column_name: String,
    pub direction: crate::select::OrderDirection,
    /// Optional prefix length for indexed columns (MySQL/SQLite feature)
    /// Example: UNIQUE (name(10)) creates index on first 10 characters of 'name'
    pub prefix_length: Option<u64>,
}

/// DROP INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStmt {
    pub if_exists: bool,
    pub index_name: String,
}

/// REINDEX statement
///
/// Rebuilds indexes to reclaim space or improve query performance.
/// Syntax: REINDEX [database_name | table_name | index_name]
#[derive(Debug, Clone, PartialEq)]
pub struct ReindexStmt {
    /// Optional target: database name, table name, or index name
    pub target: Option<String>,
}

/// ANALYZE statement
///
/// Computes table and column statistics to improve query plan optimization.
/// Syntax:
/// - ANALYZE;                          -- All tables
/// - ANALYZE table_name;               -- Specific table
/// - ANALYZE table_name (col1, col2);  -- Specific columns
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeStmt {
    /// Optional table name
    pub table_name: Option<String>,
    /// Optional column names (only valid when table_name is specified)
    pub columns: Option<Vec<String>>,
}
