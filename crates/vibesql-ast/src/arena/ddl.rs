//! Arena-allocated DDL (Data Definition Language) statement types.
//!
//! This module provides arena-based versions of DDL statements including
//! CREATE/DROP/ALTER TABLE, INDEX, VIEW, and transaction statements.

use bumpalo::collections::Vec as BumpVec;
use vibesql_types::DataType;

use super::expression::Expression;
use super::interner::Symbol;
use super::select::SelectStmt;

// ============================================================================
// Transaction Statements
// ============================================================================

/// Durability hint for a transaction (arena version)
///
/// Controls how the transaction's changes are persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DurabilityHint {
    /// Use the database's default durability mode
    #[default]
    Default,
    /// Force durable commit (fsync on commit)
    Durable,
    /// Allow lazy commit (batched sync)
    Lazy,
    /// Force volatile (no WAL) for this transaction
    Volatile,
}

/// BEGIN TRANSACTION statement
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BeginStmt {
    /// Optional durability hint for this transaction
    pub durability: DurabilityHint,
}

/// COMMIT statement
#[derive(Debug, Clone, PartialEq)]
pub struct CommitStmt;

/// ROLLBACK statement
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackStmt;

/// SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SavepointStmt {
    pub name: Symbol,
}

/// ROLLBACK TO SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackToSavepointStmt {
    pub name: Symbol,
}

/// RELEASE SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseSavepointStmt {
    pub name: Symbol,
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
    pub table_name: Symbol,
    pub columns: BumpVec<'arena, ColumnDef<'arena>>,
    pub table_constraints: BumpVec<'arena, TableConstraint<'arena>>,
    pub storage_format: Option<StorageFormat>,
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef<'arena> {
    pub name: Symbol,
    pub data_type: DataType,
    pub nullable: bool,
    pub constraints: BumpVec<'arena, ColumnConstraint<'arena>>,
    pub default_value: Option<&'arena Expression<'arena>>,
    pub comment: Option<Symbol>,
}

/// Column-level constraint
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnConstraint<'arena> {
    pub name: Option<Symbol>,
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
        table: Symbol,
        column: Symbol,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    AutoIncrement,
    Key,
}

/// Table-level constraint
#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint<'arena> {
    pub name: Option<Symbol>,
    pub kind: TableConstraintKind<'arena>,
}

/// Table constraint types
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraintKind<'arena> {
    PrimaryKey {
        columns: BumpVec<'arena, IndexColumn>,
    },
    ForeignKey {
        columns: BumpVec<'arena, Symbol>,
        references_table: Symbol,
        references_columns: BumpVec<'arena, Symbol>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Unique {
        columns: BumpVec<'arena, IndexColumn>,
    },
    Check {
        expr: &'arena Expression<'arena>,
    },
    Fulltext {
        index_name: Option<Symbol>,
        columns: BumpVec<'arena, IndexColumn>,
    },
}

// ============================================================================
// DROP TABLE
// ============================================================================

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt {
    pub table_name: Symbol,
    pub if_exists: bool,
}

/// TRUNCATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct TruncateTableStmt<'arena> {
    pub table_names: BumpVec<'arena, Symbol>,
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
    DropColumn(DropColumnStmt),
    AlterColumn(AlterColumnStmt<'arena>),
    AddConstraint(AddConstraintStmt<'arena>),
    DropConstraint(DropConstraintStmt),
    RenameTable(RenameTableStmt),
    /// MySQL-style MODIFY COLUMN (change column definition without renaming)
    ModifyColumn(ModifyColumnStmt<'arena>),
    /// MySQL-style CHANGE COLUMN (rename and modify column)
    ChangeColumn(ChangeColumnStmt<'arena>),
}

/// ADD COLUMN operation
#[derive(Debug, Clone, PartialEq)]
pub struct AddColumnStmt<'arena> {
    pub table_name: Symbol,
    pub column_def: ColumnDef<'arena>,
}

/// DROP COLUMN operation
#[derive(Debug, Clone, PartialEq)]
pub struct DropColumnStmt {
    pub table_name: Symbol,
    pub column_name: Symbol,
    pub if_exists: bool,
}

/// ALTER COLUMN operation
#[derive(Debug, Clone, PartialEq)]
pub enum AlterColumnStmt<'arena> {
    SetDefault { table_name: Symbol, column_name: Symbol, default: Expression<'arena> },
    DropDefault { table_name: Symbol, column_name: Symbol },
    SetNotNull { table_name: Symbol, column_name: Symbol },
    DropNotNull { table_name: Symbol, column_name: Symbol },
}

/// ADD CONSTRAINT operation
#[derive(Debug, Clone, PartialEq)]
pub struct AddConstraintStmt<'arena> {
    pub table_name: Symbol,
    pub constraint: TableConstraint<'arena>,
}

/// DROP CONSTRAINT operation
#[derive(Debug, Clone, PartialEq)]
pub struct DropConstraintStmt {
    pub table_name: Symbol,
    pub constraint_name: Symbol,
}

/// RENAME TABLE operation
#[derive(Debug, Clone, PartialEq)]
pub struct RenameTableStmt {
    pub table_name: Symbol,
    pub new_table_name: Symbol,
}

/// MODIFY COLUMN operation (MySQL-style - change column definition without renaming)
#[derive(Debug, Clone, PartialEq)]
pub struct ModifyColumnStmt<'arena> {
    pub table_name: Symbol,
    pub column_name: Symbol,
    pub new_column_def: ColumnDef<'arena>,
}

/// CHANGE COLUMN operation (MySQL-style - rename and modify column)
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeColumnStmt<'arena> {
    pub table_name: Symbol,
    pub old_column_name: Symbol,
    pub new_column_def: ColumnDef<'arena>,
}

// ============================================================================
// CREATE/DROP INDEX
// ============================================================================

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt<'arena> {
    pub if_not_exists: bool,
    pub index_name: Symbol,
    pub table_name: Symbol,
    pub index_type: IndexType,
    pub columns: BumpVec<'arena, IndexColumn>,
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
pub struct IndexColumn {
    pub column_name: Symbol,
    pub direction: super::expression::OrderDirection,
    pub prefix_length: Option<u64>,
}

/// DROP INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStmt {
    pub if_exists: bool,
    pub index_name: Symbol,
}

// ============================================================================
// CREATE/DROP VIEW
// ============================================================================

/// CREATE VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStmt<'arena> {
    pub view_name: Symbol,
    pub columns: Option<BumpVec<'arena, Symbol>>,
    pub query: &'arena SelectStmt<'arena>,
    pub with_check_option: bool,
    pub or_replace: bool,
    pub temporary: bool,
}

/// DROP VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropViewStmt {
    pub view_name: Symbol,
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
    pub table_name: Option<Symbol>,
    pub columns: Option<BumpVec<'arena, Symbol>>,
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
    DropTable(DropTableStmt),
    TruncateTable(TruncateTableStmt<'arena>),
    AlterTable(AlterTableStmt<'arena>),

    // DDL - Index
    CreateIndex(CreateIndexStmt<'arena>),
    DropIndex(DropIndexStmt),

    // DDL - View
    CreateView(CreateViewStmt<'arena>),
    DropView(DropViewStmt),

    // Transaction
    BeginTransaction(BeginStmt),
    Commit(CommitStmt),
    Rollback(RollbackStmt),
    Savepoint(SavepointStmt),
    RollbackToSavepoint(RollbackToSavepointStmt),
    ReleaseSavepoint(ReleaseSavepointStmt),

    // Analysis
    Analyze(AnalyzeStmt<'arena>),
    Explain(ExplainStmt<'arena>),
}
