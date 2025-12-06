//! Table DDL operations
//!
//! This module contains AST nodes for table-related DDL operations:
//! - CREATE TABLE
//! - DROP TABLE
//! - ALTER TABLE (add/drop column, add/drop constraint, etc.)

use vibesql_types::DataType;

use crate::Expression;

/// Referential action for foreign key constraints
#[derive(Debug, Clone, PartialEq)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

/// Storage format for tables
///
/// Tables can be stored in row-oriented (default) or columnar format.
/// Columnar storage is optimized for analytical queries (OLAP) with
/// SIMD-accelerated scans and aggregations.
///
/// # Usage
///
/// ```sql
/// -- Create a columnar table for analytics
/// CREATE TABLE lineitem (...) STORAGE COLUMNAR;
///
/// -- Explicitly create a row-oriented table
/// CREATE TABLE orders (...) STORAGE ROW;
/// ```
///
/// # Performance Trade-offs
///
/// | Format   | INSERT | Point Query | Scan       | Aggregation |
/// |----------|--------|-------------|------------|-------------|
/// | Row      | O(1)   | O(1) index  | O(n)       | O(n)        |
/// | Columnar | O(n)*  | O(n)        | O(n) SIMD  | O(n) SIMD   |
///
/// *Columnar INSERT triggers full rebuild - use for bulk-load scenarios only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageFormat {
    /// Traditional row-oriented storage (default)
    ///
    /// Optimized for OLTP workloads: fast inserts, point lookups.
    /// Use for tables with frequent writes or transactional access patterns.
    #[default]
    Row,
    /// Native columnar storage for analytical tables
    ///
    /// Optimized for OLAP workloads: fast scans, aggregations.
    /// Eliminates row-to-columnar conversion overhead for analytical queries.
    ///
    /// **Warning**: Each write operation (INSERT/UPDATE/DELETE) triggers a
    /// full rebuild of the columnar representation. Only use for tables that
    /// are bulk-loaded and rarely modified.
    Columnar,
}

impl std::fmt::Display for StorageFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageFormat::Row => write!(f, "row"),
            StorageFormat::Columnar => write!(f, "columnar"),
        }
    }
}

/// MySQL table options for CREATE TABLE
#[derive(Debug, Clone, PartialEq)]
pub enum TableOption {
    /// KEY_BLOCK_SIZE [=] value
    KeyBlockSize(Option<i64>),
    /// CONNECTION [=] 'string'
    Connection(Option<String>),
    /// INSERT_METHOD = {FIRST | LAST | NO}
    InsertMethod(InsertMethod),
    /// UNION [=] (col1, col2, ...)
    Union(Option<Vec<String>>),
    /// ROW_FORMAT [=] {DEFAULT | DYNAMIC | FIXED | COMPRESSED | REDUNDANT | COMPACT}
    RowFormat(Option<RowFormat>),
    /// DELAY_KEY_WRITE [=] value
    DelayKeyWrite(Option<i64>),
    /// TABLE_CHECKSUM [=] value | CHECKSUM [=] value
    TableChecksum(Option<i64>),
    /// STATS_SAMPLE_PAGES [=] value
    StatsSamplePages(Option<i64>),
    /// PASSWORD [=] 'string'
    Password(Option<String>),
    /// AVG_ROW_LENGTH [=] value
    AvgRowLength(Option<i64>),
    /// MIN_ROWS [=] value
    MinRows(Option<i64>),
    /// MAX_ROWS [=] value
    MaxRows(Option<i64>),
    /// SECONDARY_ENGINE [=] identifier | NULL
    SecondaryEngine(Option<String>),
    /// COLLATE [=] collation_name
    Collate(Option<String>),
    /// COMMENT [=] 'string'
    Comment(Option<String>),
    /// STORAGE [=] {ROW | COLUMNAR}
    /// VibeSQL extension for native columnar storage
    Storage(StorageFormat),
}

/// MySQL INSERT_METHOD values
#[derive(Debug, Clone, PartialEq)]
pub enum InsertMethod {
    First,
    Last,
    No,
}

/// MySQL ROW_FORMAT values
#[derive(Debug, Clone, PartialEq)]
pub enum RowFormat {
    Default,
    Dynamic,
    Fixed,
    Compressed,
    Redundant,
    Compact,
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    /// If true, don't error if the table already exists
    pub if_not_exists: bool,
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub table_constraints: Vec<TableConstraint>,
    pub table_options: Vec<TableOption>,
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub constraints: Vec<ColumnConstraint>,
    pub default_value: Option<Box<Expression>>,
    pub comment: Option<String>,
}

/// Column-level constraint
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnConstraint {
    pub name: Option<String>,
    pub kind: ColumnConstraintKind,
}

/// Column constraint types
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraintKind {
    NotNull,
    PrimaryKey,
    Unique,
    Check(Box<Expression>),
    References {
        table: String,
        column: String,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    /// AUTO_INCREMENT (MySQL) or AUTOINCREMENT (SQLite)
    /// Automatically generates sequential integer values for new rows
    AutoIncrement,
    /// KEY (MySQL-specific)
    /// Creates an index on the column
    Key,
}

/// Table-level constraint
#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint {
    pub name: Option<String>,
    pub kind: TableConstraintKind,
}

/// Table constraint types
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintKind {
    PrimaryKey {
        columns: Vec<crate::IndexColumn>,
    },
    ForeignKey {
        columns: Vec<String>,
        references_table: String,
        references_columns: Vec<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Unique {
        columns: Vec<crate::IndexColumn>,
    },
    Check {
        expr: Box<Expression>,
    },
    /// FULLTEXT index constraint
    /// Example: FULLTEXT INDEX ft_search (title, body)
    Fulltext {
        index_name: Option<String>,
        columns: Vec<crate::IndexColumn>,
    },
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt {
    pub table_name: String,
    pub if_exists: bool,
}

/// CASCADE option for TRUNCATE TABLE
#[derive(Debug, Clone, PartialEq)]
pub enum TruncateCascadeOption {
    /// CASCADE - recursively truncate dependent tables
    Cascade,
    /// RESTRICT - fail if foreign key references exist (default)
    Restrict,
}

/// TRUNCATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct TruncateTableStmt {
    pub table_names: Vec<String>,
    pub if_exists: bool,
    /// CASCADE/RESTRICT option (None = default to RESTRICT)
    pub cascade: Option<TruncateCascadeOption>,
}

/// ALTER TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableStmt {
    AddColumn(AddColumnStmt),
    DropColumn(DropColumnStmt),
    AlterColumn(AlterColumnStmt),
    AddConstraint(AddConstraintStmt),
    DropConstraint(DropConstraintStmt),
    RenameTable(RenameTableStmt),
    ModifyColumn(ModifyColumnStmt),
    ChangeColumn(ChangeColumnStmt),
}

/// ADD COLUMN operation
#[derive(Debug, Clone, PartialEq)]
pub struct AddColumnStmt {
    pub table_name: String,
    pub column_def: ColumnDef,
}

/// DROP COLUMN operation
#[derive(Debug, Clone, PartialEq)]
pub struct DropColumnStmt {
    pub table_name: String,
    pub column_name: String,
    pub if_exists: bool,
}

/// ALTER COLUMN operation
#[derive(Debug, Clone, PartialEq)]
pub enum AlterColumnStmt {
    SetDefault { table_name: String, column_name: String, default: Expression },
    DropDefault { table_name: String, column_name: String },
    SetNotNull { table_name: String, column_name: String },
    DropNotNull { table_name: String, column_name: String },
}

/// ADD CONSTRAINT operation
#[derive(Debug, Clone, PartialEq)]
pub struct AddConstraintStmt {
    pub table_name: String,
    pub constraint: TableConstraint,
}

/// DROP CONSTRAINT operation
#[derive(Debug, Clone, PartialEq)]
pub struct DropConstraintStmt {
    pub table_name: String,
    pub constraint_name: String,
}

/// RENAME TABLE operation
#[derive(Debug, Clone, PartialEq)]
pub struct RenameTableStmt {
    pub table_name: String,
    pub new_table_name: String,
}

/// MODIFY COLUMN operation (MySQL-style)
#[derive(Debug, Clone, PartialEq)]
pub struct ModifyColumnStmt {
    pub table_name: String,
    pub column_name: String,
    pub new_column_def: ColumnDef,
}

/// CHANGE COLUMN operation (MySQL-style - rename and modify)
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeColumnStmt {
    pub table_name: String,
    pub old_column_name: String,
    pub new_column_def: ColumnDef,
}
