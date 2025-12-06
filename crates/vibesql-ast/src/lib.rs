//! Abstract Syntax Tree (AST) for SQL:1999
//!
//! This crate defines the structure of SQL statements and expressions
//! as parsed from SQL text. The AST is a tree representation that
//! preserves the semantic structure of SQL queries.
//!
//! # Arena-allocated Types
//!
//! For performance-critical code paths, this crate provides arena-allocated
//! versions of AST types in the [`arena`] module. These use bump allocation
//! for improved cache locality and reduced allocation overhead.
//!
//! # Visitor Pattern
//!
//! The [`visitor`] module provides traits for traversing and transforming
//! AST nodes without duplicating traversal logic:
//!
//! - [`visitor::ExpressionVisitor`]: Read-only expression traversal
//! - [`visitor::ExpressionMutVisitor`]: Expression transformation
//! - [`visitor::StatementVisitor`]: Statement traversal
//!
//! See the module documentation for usage examples.

pub mod arena;
pub mod visitor;

mod ddl;
mod dml;
mod expression;
mod grant;
mod introspection;
mod operators;
mod revoke;
mod select;
mod statement;

pub use ddl::{
    AddColumnStmt, AddConstraintStmt, AlterColumnStmt, AlterCronStmt, AlterSequenceStmt,
    AlterTableStmt, AlterTriggerAction, AlterTriggerStmt, AnalyzeStmt, BeginStmt, CallStmt,
    DurabilityHint,
    CancelScheduleStmt, ChangeColumnStmt, CloseCursorStmt, ColumnConstraint, ColumnConstraintKind,
    ColumnDef, CommitStmt, CreateAssertionStmt, CreateCharacterSetStmt, CreateCollationStmt,
    CreateCronStmt, CreateDomainStmt, CreateFunctionStmt, CreateIndexStmt, CreateProcedureStmt,
    CreateRoleStmt, CreateSchemaStmt, CreateSequenceStmt, CreateTableStmt, CreateTranslationStmt,
    CreateTriggerStmt, CreateTypeStmt, CreateViewStmt, CursorUpdatability, DeallocateStmt,
    DeallocateTarget, DeclareCursorStmt, DomainConstraint, DropAssertionStmt, DropBehavior,
    DropCharacterSetStmt, DropCollationStmt, DropColumnStmt, DropConstraintStmt, DropCronStmt,
    DropDomainStmt, DropFunctionStmt, DropIndexStmt, DropProcedureStmt, DropRoleStmt,
    DropSchemaStmt, DropSequenceStmt, DropTableStmt, DropTranslationStmt, DropTriggerStmt,
    DropTypeStmt, DropViewStmt, ExecuteStmt, FetchOrientation, FetchStmt, FunctionParameter,
    IndexColumn, IndexType, InsertMethod, IsolationLevel, ModifyColumnStmt, OpenCursorStmt,
    ParameterMode, PrepareStmt, PreparedStatementBody, ProceduralStatement, ProcedureBody,
    ProcedureParameter, ReferentialAction, ReindexStmt, ReleaseSavepointStmt, RenameTableStmt,
    RollbackStmt, RollbackToSavepointStmt, RowFormat, SavepointStmt, ScheduleAfterStmt,
    ScheduleAtStmt, SchemaElement, SetCatalogStmt, SetNamesStmt, SetSchemaStmt, SetTimeZoneStmt,
    SetTransactionStmt, SetVariableStmt, SqlSecurity, StorageFormat, TableConstraint,
    TableConstraintKind, TableOption, TimeZoneSpec, TransactionAccessMode, TriggerAction,
    TriggerEvent, TriggerGranularity, TriggerTiming, TruncateCascadeOption, TruncateTableStmt,
    TypeAttribute, TypeDefinition, VariableScope, VectorDistanceMetric,
};
pub use dml::{
    Assignment, ConflictClause, DeleteStmt, InsertSource, InsertStmt, UpdateStmt, WhereClause,
};
pub use expression::{
    CaseWhen, CharacterUnit, Expression, FrameBound, FrameUnit, FulltextMode, IntervalUnit,
    PseudoTable, Quantifier, TrimPosition, WindowFrame, WindowFunctionSpec, WindowSpec,
};
pub use grant::{GrantStmt, ObjectType, PrivilegeType};
pub use introspection::{
    DescribeStmt, ExplainFormat, ExplainStmt, ShowColumnsStmt, ShowCreateTableStmt,
    ShowDatabasesStmt, ShowIndexStmt, ShowTablesStmt,
};
pub use operators::{BinaryOperator, UnaryOperator};
pub use revoke::{CascadeOption, RevokeStmt};
pub use select::{
    CommonTableExpr, FromClause, GroupByClause, GroupingElement, GroupingSet, JoinType,
    MixedGroupingItem, OrderByItem, OrderDirection, SelectItem, SelectStmt, SetOperation,
    SetOperator,
};
pub use statement::Statement;
