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
    /// Whether to skip creation if view already exists (CREATE VIEW IF NOT EXISTS)
    pub if_not_exists: bool,
    /// Whether this is a temporary view (CREATE TEMP VIEW or CREATE TEMPORARY VIEW)
    pub temporary: bool,
    /// Original SQL definition for sqlite_master compatibility
    /// This is populated during parsing to preserve the exact SQL text
    pub sql_definition: Option<String>,
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
    /// `CREATE TRIGGER IF NOT EXISTS ...`: when true, creating a trigger whose
    /// name already exists is a no-op success rather than a "trigger already
    /// exists" error (SQLite semantics).
    pub if_not_exists: bool,
    /// Optional schema qualifier for the trigger, taken from either the
    /// `TEMP`/`TEMPORARY` modifier (`CREATE TEMP TRIGGER ...` → `Some("temp")`)
    /// or an explicit schema prefix on the name (`CREATE TRIGGER main.r1 ...`
    /// → `Some("main")`). `None` means the trigger lives in the default
    /// (main) schema. SQLite places a trigger in the same schema as its target
    /// table and uses this to disambiguate when a temp table shadows a main
    /// table of the same name (triggerD-3.1/3.2).
    pub schema: Option<String>,
    pub trigger_name: String,
    /// The trigger name exactly as it appeared in the source, including any
    /// quoting delimiters (`"tr1"`, `[tr1]`, `` `tr1` ``). `trigger_name` holds
    /// the normalized (de-quoted) identifier used for catalog lookups, but
    /// SQLite echoes the *original* spelling in the "trigger ... already exists"
    /// error (trigger1-1.2.2/1.2.3). `None` when the source spelling is not
    /// available (e.g. AST built programmatically), in which case callers fall
    /// back to `trigger_name`.
    pub name_source: Option<String>,
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
    /// `DROP TRIGGER IF EXISTS` — when true, dropping a non-existent trigger
    /// is a no-op instead of an error (SQLite / SQL:2008 semantics).
    pub if_exists: bool,
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
    /// Optional WHERE clause for partial indexes (SQLite syntax).
    ///
    /// Storage of partial-index WHERE expressions is not yet implemented;
    /// the parser captures the expression so semantic validation (e.g.
    /// rejecting window functions in the predicate) can run before
    /// execution falls back to an "unsupported" error. See issue #5091.
    pub where_clause: Option<Box<Expression>>,
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

/// Index column specification - can be either a simple column reference or an expression
#[derive(Debug, Clone, PartialEq)]
pub enum IndexColumn {
    /// Simple column reference with optional prefix length
    Column {
        column_name: String,
        direction: crate::select::OrderDirection,
        /// Optional prefix length for indexed columns (MySQL/SQLite feature)
        /// Example: UNIQUE (name(10)) creates index on first 10 characters of 'name'
        prefix_length: Option<u64>,
        /// Explicit per-key-part collation (`CREATE INDEX ... ON t(b COLLATE nocase)`).
        /// `None` means the key-part adopts the column's declared collation (or
        /// BINARY). Persisted for round-trip and used by upsert conflict-target
        /// matching (issue #5921).
        collation: Option<String>,
    },
    /// Expression index (functional index)
    /// Example: CREATE INDEX idx ON t(lower(name)) or CREATE INDEX idx ON t(a + b)
    Expression { expr: Box<Expression>, direction: crate::select::OrderDirection },
}

impl IndexColumn {
    /// Create a new simple column index
    pub fn new_column(column_name: String, direction: crate::select::OrderDirection) -> Self {
        IndexColumn::Column { column_name, direction, prefix_length: None, collation: None }
    }

    /// Create a new simple column index with an explicit collation.
    pub fn new_column_with_collation(
        column_name: String,
        direction: crate::select::OrderDirection,
        collation: Option<String>,
    ) -> Self {
        IndexColumn::Column { column_name, direction, prefix_length: None, collation }
    }

    /// Create a new column index with prefix length
    pub fn new_column_with_prefix(
        column_name: String,
        direction: crate::select::OrderDirection,
        prefix_length: u64,
    ) -> Self {
        IndexColumn::Column {
            column_name,
            direction,
            prefix_length: Some(prefix_length),
            collation: None,
        }
    }

    /// Create a new expression index
    pub fn new_expression(expr: Expression, direction: crate::select::OrderDirection) -> Self {
        IndexColumn::Expression { expr: Box::new(expr), direction }
    }

    /// Get the column name if this is a simple column reference
    pub fn column_name(&self) -> Option<&str> {
        match self {
            IndexColumn::Column { column_name, .. } => Some(column_name),
            IndexColumn::Expression { .. } => None,
        }
    }

    /// Get the column name, panicking if this is an expression index.
    /// Use this in code paths that don't support expression indexes yet.
    pub fn expect_column_name(&self) -> &str {
        match self {
            IndexColumn::Column { column_name, .. } => column_name,
            IndexColumn::Expression { .. } => {
                panic!("Expression indexes are not supported in this context")
            }
        }
    }

    /// Get the direction of this index column
    pub fn direction(&self) -> crate::select::OrderDirection {
        match self {
            IndexColumn::Column { direction, .. } => direction.clone(),
            IndexColumn::Expression { direction, .. } => direction.clone(),
        }
    }

    /// Get the prefix length if this is a column with prefix
    pub fn prefix_length(&self) -> Option<u64> {
        match self {
            IndexColumn::Column { prefix_length, .. } => *prefix_length,
            IndexColumn::Expression { .. } => None,
        }
    }

    /// Get the explicit per-key-part collation if this is a simple column with
    /// an explicit `COLLATE` clause (issue #5921).
    pub fn collation(&self) -> Option<&str> {
        match self {
            IndexColumn::Column { collation, .. } => collation.as_deref(),
            IndexColumn::Expression { .. } => None,
        }
    }

    /// Check if this is an expression index
    pub fn is_expression(&self) -> bool {
        matches!(self, IndexColumn::Expression { .. })
    }

    /// Get the expression if this is an expression index
    pub fn get_expression(&self) -> Option<&Expression> {
        match self {
            IndexColumn::Expression { expr, .. } => Some(expr),
            IndexColumn::Column { .. } => None,
        }
    }
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

/// VACUUM statement
///
/// SQLite compatibility statement. SQLite rebuilds the database file;
/// VibeSQL maps VACUUM to MVCC old-version garbage collection
/// (`Database::vacuum_mvcc`), which is a no-op when MVCC is disabled.
///
/// Syntax:
/// - VACUUM;                         -- Vacuum the main database
/// - VACUUM schema_name;             -- Vacuum a specific attached schema
/// - VACUUM INTO 'file';             -- Parsed, but rejected at runtime
/// - VACUUM schema_name INTO 'file';
#[derive(Debug, Clone, PartialEq)]
pub struct VacuumStmt {
    /// Optional schema name target (e.g. `main`)
    pub schema_name: Option<String>,
    /// Optional output file for VACUUM INTO (not supported at runtime)
    pub into_file: Option<String>,
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

/// PRAGMA statement
///
/// SQLite-specific statement for database configuration and introspection.
/// Currently parsed but treated as a no-op for SQLite compatibility.
///
/// Syntax variations:
/// - PRAGMA pragma_name;                 -- Query pragma value
/// - PRAGMA pragma_name = value;         -- Set pragma value
/// - PRAGMA pragma_name(value);          -- Set pragma value (function syntax)
/// - PRAGMA database.pragma_name;        -- Database-qualified pragma
/// - PRAGMA database.pragma_name = value;
#[derive(Debug, Clone, PartialEq)]
pub struct PragmaStmt {
    /// Optional database/schema name (before the dot)
    pub database: Option<String>,
    /// The pragma name
    pub name: String,
    /// Optional pragma value (from = value or (value) syntax)
    pub value: Option<PragmaValue>,
}

/// Value for a PRAGMA statement
#[derive(Debug, Clone, PartialEq)]
pub enum PragmaValue {
    /// Identifier value (ON, OFF, FULL, etc.)
    Identifier(String),
    /// String literal value
    String(String),
    /// Numeric value (integer or float)
    Number(String),
    /// Signed number (negative numbers)
    SignedNumber(String),
}
