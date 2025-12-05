//! Top-level SQL statement types
//!
//! This module defines the Statement enum that represents all possible SQL statements.

use crate::{
    AlterCronStmt, AlterSequenceStmt, AlterTableStmt, AlterTriggerStmt, AnalyzeStmt, BeginStmt,
    CallStmt, CancelScheduleStmt, CloseCursorStmt, CommitStmt, CreateAssertionStmt,
    CreateCharacterSetStmt, CreateCollationStmt, CreateCronStmt, CreateDomainStmt,
    CreateFunctionStmt, CreateIndexStmt, CreateProcedureStmt, CreateRoleStmt, CreateSchemaStmt,
    CreateSequenceStmt, CreateTableStmt, CreateTranslationStmt, CreateTriggerStmt, CreateTypeStmt,
    CreateViewStmt, DeallocateStmt, DeclareCursorStmt, DeleteStmt, DescribeStmt, DropAssertionStmt,
    DropCharacterSetStmt, DropCollationStmt, DropCronStmt, DropDomainStmt, DropFunctionStmt,
    DropIndexStmt, DropProcedureStmt, DropRoleStmt, DropSchemaStmt, DropSequenceStmt,
    DropTableStmt, DropTranslationStmt, DropTriggerStmt, DropTypeStmt, DropViewStmt, ExecuteStmt,
    ExplainStmt, FetchStmt, GrantStmt, InsertStmt, OpenCursorStmt, PrepareStmt, ReindexStmt,
    ReleaseSavepointStmt, RevokeStmt, RollbackStmt, RollbackToSavepointStmt, SavepointStmt,
    ScheduleAfterStmt, ScheduleAtStmt, SelectStmt, SetCatalogStmt, SetNamesStmt, SetSchemaStmt,
    SetTimeZoneStmt, SetTransactionStmt, SetVariableStmt, ShowColumnsStmt, ShowCreateTableStmt,
    ShowDatabasesStmt, ShowIndexStmt, ShowTablesStmt, TruncateTableStmt, UpdateStmt,
};

// ============================================================================
// Top-level SQL Statements
// ============================================================================

/// A complete SQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(Box<SelectStmt>),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    TruncateTable(TruncateTableStmt),
    AlterTable(AlterTableStmt),
    CreateSchema(CreateSchemaStmt),
    DropSchema(DropSchemaStmt),
    SetSchema(SetSchemaStmt),
    SetCatalog(SetCatalogStmt),
    SetNames(SetNamesStmt),
    SetTimeZone(SetTimeZoneStmt),
    SetTransaction(SetTransactionStmt),
    SetVariable(SetVariableStmt),
    CreateRole(CreateRoleStmt),
    DropRole(DropRoleStmt),
    BeginTransaction(BeginStmt),
    Commit(CommitStmt),
    Rollback(RollbackStmt),
    Savepoint(SavepointStmt),
    RollbackToSavepoint(RollbackToSavepointStmt),
    ReleaseSavepoint(ReleaseSavepointStmt),
    Grant(GrantStmt),
    Revoke(RevokeStmt),
    // Advanced SQL object statements (SQL:1999)
    CreateDomain(CreateDomainStmt),
    DropDomain(DropDomainStmt),
    CreateSequence(CreateSequenceStmt),
    AlterSequence(AlterSequenceStmt),
    DropSequence(DropSequenceStmt),
    CreateType(CreateTypeStmt),
    DropType(DropTypeStmt),
    CreateCollation(CreateCollationStmt),
    DropCollation(DropCollationStmt),
    CreateCharacterSet(CreateCharacterSetStmt),
    DropCharacterSet(DropCharacterSetStmt),
    CreateTranslation(CreateTranslationStmt),
    DropTranslation(DropTranslationStmt),
    CreateView(CreateViewStmt),
    DropView(DropViewStmt),
    CreateTrigger(CreateTriggerStmt),
    AlterTrigger(AlterTriggerStmt),
    DropTrigger(DropTriggerStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    Reindex(ReindexStmt),
    Analyze(AnalyzeStmt),
    CreateAssertion(CreateAssertionStmt),
    DropAssertion(DropAssertionStmt),
    // Cursor operations (SQL:1999 Feature E121)
    DeclareCursor(DeclareCursorStmt),
    OpenCursor(OpenCursorStmt),
    Fetch(FetchStmt),
    CloseCursor(CloseCursorStmt),
    // Prepared statements (SQL:1999 Feature E141)
    Prepare(PrepareStmt),
    Execute(ExecuteStmt),
    Deallocate(DeallocateStmt),
    // Stored procedures and functions (SQL:1999 Feature P001)
    CreateProcedure(CreateProcedureStmt),
    DropProcedure(DropProcedureStmt),
    CreateFunction(CreateFunctionStmt),
    DropFunction(DropFunctionStmt),
    Call(CallStmt),
    // Database introspection (MySQL extensions)
    ShowTables(ShowTablesStmt),
    ShowDatabases(ShowDatabasesStmt),
    ShowColumns(ShowColumnsStmt),
    ShowIndex(ShowIndexStmt),
    ShowCreateTable(ShowCreateTableStmt),
    Describe(DescribeStmt),
    // Query plan introspection
    Explain(ExplainStmt),
    // Scheduled functions and cron jobs
    ScheduleAfter(ScheduleAfterStmt),
    ScheduleAt(ScheduleAtStmt),
    CreateCron(CreateCronStmt),
    DropCron(DropCronStmt),
    AlterCron(AlterCronStmt),
    CancelSchedule(CancelScheduleStmt),
}
