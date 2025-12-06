//! Transaction control DDL operations
//!
//! This module contains AST nodes for transaction control statements:
//! - BEGIN TRANSACTION
//! - COMMIT
//! - ROLLBACK
//! - SAVEPOINT
//! - SET TRANSACTION

/// Durability hint for a transaction
///
/// Controls how the transaction's changes are persisted.
/// This mirrors `TransactionDurability` in the storage crate.
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

impl DurabilityHint {
    /// Convert to SQL hint string for use with storage layer
    pub fn as_sql_hint(&self) -> &'static str {
        match self {
            DurabilityHint::Default => "DEFAULT",
            DurabilityHint::Durable => "DURABLE",
            DurabilityHint::Lazy => "LAZY",
            DurabilityHint::Volatile => "VOLATILE",
        }
    }
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
    pub name: String,
}

/// ROLLBACK TO SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackToSavepointStmt {
    pub name: String,
}

/// RELEASE SAVEPOINT statement
#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseSavepointStmt {
    pub name: String,
}

/// Transaction isolation level
#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    Serializable,
}

/// Transaction access mode
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

/// SET TRANSACTION statement (SQL:1999 Feature E152)
#[derive(Debug, Clone, PartialEq)]
pub struct SetTransactionStmt {
    pub local: bool, // true for SET LOCAL TRANSACTION, false for SET TRANSACTION
    pub isolation_level: Option<IsolationLevel>,
    pub access_mode: Option<TransactionAccessMode>,
}
