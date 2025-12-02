//! Arena-allocated DDL (Data Definition Language) statement types.
//!
//! This module provides arena-based versions of DDL statements including
//! CREATE/DROP/ALTER TABLE, INDEX, VIEW, and transaction statements.

use bumpalo::collections::Vec as BumpVec;
use vibesql_types::DataType;

use super::expression::Expression;
use super::select::SelectStmt;

// ============================================================================
// Transaction Statements
// ============================================================================

/// BEGIN TRANSACTION statement
#[derive(Debug, Clone, PartialEq)]
pub struct BeginStmt;

/// COMMIT statement
#[derive(Debug, Clone, PartialEq)]
pub struct CommitStmt;

/// ROLLBACK statement
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackStmt;

/// SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SavepointStmt<'arena> {
    pub name: &'arena str,
}

/// ROLLBACK TO SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackToSavepointStmt<'arena> {
    pub name: &'arena str,
}

/// RELEASE SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseSavepointStmt<'arena> {
    pub name: &'arena str,
}

// ============================================================================
// CREATE TABLE
// ============================================================================

/// Referential action for foreign key constraints
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

/// Storage format for tables
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StorageFormat {
    #[default]
    Row,
    Columnar,
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt<'arena> {
    pub table_name: &'arena str,
    pub columns: BumpVec<'arena, ColumnDef<'arena>>,
    pub table_constraints: BumpVec<'arena, TableConstraint<'arena>>,
    pub storage_format: Option<StorageFormat>,
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef<'arena> {
    pub name: &'arena str,
    pub data_type: DataType,
    pub nullable: bool,
    pub constraints: BumpVec<'arena, ColumnConstraint<'arena>>,
    pub default_value: Option<&'arena Expression<'arena>>,
    pub comment: Option<&'arena str>,
}

/// Column-level constraint
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnConstraint<'arena> {
    pub name: Option<&'arena str>,
    pub kind: ColumnConstraintKind<'arena>,
}

/// Column constraint types
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraintKind<'arena> {
    NotNull,
    PrimaryKey,
    Unique,
    Check(&'arena Expression<'arena>),
    References {
        table: &'arena str,
        column: &'arena str,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    AutoIncrement,
    Key,
}

/// Table-level constraint
#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint<'arena> {
    pub name: Option<&'arena str>,
    pub kind: TableConstraintKind<'arena>,
}

/// Table constraint types
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintKind<'arena> {
    PrimaryKey {
        columns: BumpVec<'arena, IndexColumn<'arena>>,
    },
    ForeignKey {
        columns: BumpVec<'arena, &'arena str>,
        references_table: &'arena str,
        references_columns: BumpVec<'arena, &'arena str>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Unique {
        columns: BumpVec<'arena, IndexColumn<'arena>>,
    },
    Check {
        expr: &'arena Expression<'arena>,
    },
    Fulltext {
        index_name: Option<&'arena str>,
        columns: BumpVec<'arena, IndexColumn<'arena>>,
    },
}

// ============================================================================
// DROP TABLE
// ============================================================================

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt<'arena> {
    pub table_name: &'arena str,
    pub if_exists: bool,
}

/// TRUNCATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct TruncateTableStmt<'arena> {
    pub table_names: BumpVec<'arena, &'arena str>,
    pub if_exists: bool,
    pub cascade: Option<TruncateCascadeOption>,
}

/// CASCADE option for TRUNCATE TABLE
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TruncateCascadeOption {
    Cascade,
    Restrict,
}

// ============================================================================
// ALTER TABLE
// ============================================================================

/// ALTER TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableStmt<'arena> {
    AddColumn(AddColumnStmt<'arena>),
    DropColumn(DropColumnStmt<'arena>),
    AlterColumn(AlterColumnStmt<'arena>),
    AddConstraint(AddConstraintStmt<'arena>),
    DropConstraint(DropConstraintStmt<'arena>),
    RenameTable(RenameTableStmt<'arena>),
}

/// ADD COLUMN operation
#[derive(Debug, Clone, PartialEq)]
pub struct AddColumnStmt<'arena> {
    pub table_name: &'arena str,
    pub column_def: ColumnDef<'arena>,
}

/// DROP COLUMN operation
#[derive(Debug, Clone, PartialEq)]
pub struct DropColumnStmt<'arena> {
    pub table_name: &'arena str,
    pub column_name: &'arena str,
    pub if_exists: bool,
}

/// ALTER COLUMN operation
#[derive(Debug, Clone, PartialEq)]
pub enum AlterColumnStmt<'arena> {
    SetDefault {
        table_name: &'arena str,
        column_name: &'arena str,
        default: Expression<'arena>,
    },
    DropDefault {
        table_name: &'arena str,
        column_name: &'arena str,
    },
    SetNotNull {
        table_name: &'arena str,
        column_name: &'arena str,
    },
    DropNotNull {
        table_name: &'arena str,
        column_name: &'arena str,
    },
}

/// ADD CONSTRAINT operation
#[derive(Debug, Clone, PartialEq)]
pub struct AddConstraintStmt<'arena> {
    pub table_name: &'arena str,
    pub constraint: TableConstraint<'arena>,
}

/// DROP CONSTRAINT operation
#[derive(Debug, Clone, PartialEq)]
pub struct DropConstraintStmt<'arena> {
    pub table_name: &'arena str,
    pub constraint_name: &'arena str,
}

/// RENAME TABLE operation
#[derive(Debug, Clone, PartialEq)]
pub struct RenameTableStmt<'arena> {
    pub table_name: &'arena str,
    pub new_table_name: &'arena str,
}

// ============================================================================
// CREATE/DROP INDEX
// ============================================================================

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt<'arena> {
    pub if_not_exists: bool,
    pub index_name: &'arena str,
    pub table_name: &'arena str,
    pub index_type: IndexType,
    pub columns: BumpVec<'arena, IndexColumn<'arena>>,
}

/// Index type specification
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IndexType {
    BTree { unique: bool },
    Fulltext,
    Spatial,
}

/// Index column specification
#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn<'arena> {
    pub column_name: &'arena str,
    pub direction: super::expression::OrderDirection,
    pub prefix_length: Option<u64>,
}

/// DROP INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStmt<'arena> {
    pub if_exists: bool,
    pub index_name: &'arena str,
}

// ============================================================================
// CREATE/DROP VIEW
// ============================================================================

/// CREATE VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStmt<'arena> {
    pub view_name: &'arena str,
    pub columns: Option<BumpVec<'arena, &'arena str>>,
    pub query: &'arena SelectStmt<'arena>,
    pub with_check_option: bool,
    pub or_replace: bool,
    pub temporary: bool,
}

/// DROP VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropViewStmt<'arena> {
    pub view_name: &'arena str,
    pub if_exists: bool,
    pub cascade: bool,
    pub restrict: bool,
}

// ============================================================================
// ANALYZE
// ============================================================================

/// ANALYZE statement
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeStmt<'arena> {
    pub table_name: Option<&'arena str>,
    pub columns: Option<BumpVec<'arena, &'arena str>>,
}

// ============================================================================
// EXPLAIN
// ============================================================================

/// EXPLAIN statement (wraps any statement to show query plan)
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStmt<'arena> {
    pub statement: &'arena Statement<'arena>,
    pub analyze: bool,
    pub verbose: bool,
}

// ============================================================================
// Top-level Statement enum
// ============================================================================

/// A complete SQL statement (arena-allocated)
#[derive(Debug, Clone, PartialEq)]
pub enum Statement<'arena> {
    // Query
    Select(&'arena SelectStmt<'arena>),

    // DML
    Insert(super::dml::InsertStmt<'arena>),
    Update(super::dml::UpdateStmt<'arena>),
    Delete(super::dml::DeleteStmt<'arena>),

    // DDL - Table
    CreateTable(CreateTableStmt<'arena>),
    DropTable(DropTableStmt<'arena>),
    TruncateTable(TruncateTableStmt<'arena>),
    AlterTable(AlterTableStmt<'arena>),

    // DDL - Index
    CreateIndex(CreateIndexStmt<'arena>),
    DropIndex(DropIndexStmt<'arena>),

    // DDL - View
    CreateView(CreateViewStmt<'arena>),
    DropView(DropViewStmt<'arena>),

    // Transaction
    BeginTransaction(BeginStmt),
    Commit(CommitStmt),
    Rollback(RollbackStmt),
    Savepoint(SavepointStmt<'arena>),
    RollbackToSavepoint(RollbackToSavepointStmt<'arena>),
    ReleaseSavepoint(ReleaseSavepointStmt<'arena>),

    // Analysis
    Analyze(AnalyzeStmt<'arena>),
    Explain(ExplainStmt<'arena>),
}
