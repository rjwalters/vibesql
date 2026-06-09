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
//!
//! # SQL Pretty-Printing
//!
//! The [`pretty_print`] module provides the [`pretty_print::ToSql`] trait for converting
//! AST nodes back to valid SQL strings:
//!
//! ```
//! use vibesql_ast::pretty_print::ToSql;
//! use vibesql_ast::BinaryOperator;
//!
//! let op = BinaryOperator::Plus;
//! assert_eq!(op.to_sql(), "+");
//! ```

pub mod arena;
pub mod pretty_print;
pub mod visitor;

// ============================================================================
// Table Reference (with case-sensitivity info)
// ============================================================================

/// A reference to a table name with case-sensitivity information.
///
/// This struct captures whether an identifier was quoted (delimited) in the
/// original SQL, which is necessary for correct SQL:1999 case handling:
///
/// - **Unquoted identifiers**: Case-insensitive (e.g., `MyTable`, `mytable`, `MYTABLE` are equivalent)
/// - **Quoted identifiers**: Case-sensitive (e.g., `"MyTable"` is different from `"mytable"`)
///
/// # Example
///
/// ```
/// use vibesql_ast::TableRef;
///
/// // Unquoted: case-insensitive
/// let unquoted = TableRef::new("MyTable".to_string(), false);
///
/// // Quoted: case-sensitive
/// let quoted = TableRef::new("MyTable".to_string(), true);
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    /// Optional schema name for qualified references (e.g., schema.table)
    pub schema_name: Option<String>,
    /// Whether the schema name was quoted (delimited) in the original SQL.
    pub schema_quoted: bool,
    /// The table name as written in the SQL
    pub name: String,
    /// Whether the identifier was quoted (delimited) in the original SQL.
    /// - `true`: Quoted identifier (case-sensitive), e.g., `"MyTable"`
    /// - `false`: Unquoted identifier (case-insensitive), e.g., `MyTable`
    pub quoted: bool,
}

impl TableRef {
    /// Create a new table reference.
    pub fn new(name: String, quoted: bool) -> Self {
        Self { schema_name: None, schema_quoted: false, name, quoted }
    }

    /// Create a qualified table reference with schema.
    pub fn qualified(
        schema_name: String,
        schema_quoted: bool,
        table_name: String,
        table_quoted: bool,
    ) -> Self {
        Self {
            schema_name: Some(schema_name),
            schema_quoted,
            name: table_name,
            quoted: table_quoted,
        }
    }

    /// Create an unquoted (case-insensitive) table reference.
    pub fn unquoted(name: String) -> Self {
        Self::new(name, false)
    }

    /// Create a quoted (case-sensitive) table reference.
    pub fn quoted_ref(name: String) -> Self {
        Self::new(name, true)
    }

    /// Get the full name (schema.table or just table)
    pub fn full_name(&self) -> String {
        match &self.schema_name {
            Some(schema) => format!("{}.{}", schema, self.name),
            None => self.name.clone(),
        }
    }

    /// Check if any part is quoted
    pub fn is_any_quoted(&self) -> bool {
        self.quoted || self.schema_quoted
    }
}

impl From<String> for TableRef {
    /// Create an unquoted table reference from a String (for backward compatibility).
    fn from(name: String) -> Self {
        Self::unquoted(name)
    }
}

impl From<&str> for TableRef {
    /// Create an unquoted table reference from a &str (for backward compatibility).
    fn from(name: &str) -> Self {
        Self::unquoted(name.to_string())
    }
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.quoted {
            // Quote the identifier in output
            write!(f, "\"{}\"", self.name.replace('"', "\"\""))
        } else {
            write!(f, "{}", self.name)
        }
    }
}

mod ddl;
mod dml;
mod expression;
mod grant;
mod identifier;
mod introspection;
mod operators;
mod revoke;
mod select;
mod statement;

pub use ddl::{
    AddColumnStmt, AddConstraintStmt, AlterColumnStmt, AlterCronStmt, AlterSequenceStmt,
    AlterTableStmt, AlterTriggerAction, AlterTriggerStmt, AnalyzeStmt, BeginStmt, CallStmt,
    CancelScheduleStmt, ChangeColumnStmt, CloseCursorStmt, ColumnConstraint, ColumnConstraintKind,
    ColumnDef, CommitStmt, ConstraintDeferral, CreateAssertionStmt, CreateCharacterSetStmt,
    CreateCollationStmt, CreateCronStmt, CreateDomainStmt, CreateFunctionStmt, CreateIndexStmt,
    CreateProcedureStmt, CreateRoleStmt, CreateSchemaStmt, CreateSequenceStmt, CreateTableStmt,
    CreateTranslationStmt, CreateTriggerStmt, CreateTypeStmt, CreateViewStmt, CursorUpdatability,
    DeallocateStmt, DeallocateTarget, DeclareCursorStmt, DomainConstraint, DropAssertionStmt,
    DropBehavior, DropCharacterSetStmt, DropCollationStmt, DropColumnStmt, DropConstraintStmt,
    DropCronStmt, DropDomainStmt, DropFunctionStmt, DropIndexStmt, DropProcedureStmt, DropRoleStmt,
    DropSchemaStmt, DropSequenceStmt, DropTableStmt, DropTranslationStmt, DropTriggerStmt,
    DropTypeStmt, DropViewStmt, DurabilityHint, ExecuteStmt, FetchOrientation, FetchStmt,
    FunctionParameter, IndexColumn, IndexType, InsertMethod, IsolationLevel, ModifyColumnStmt,
    OpenCursorStmt, ParameterMode, PragmaStmt, PragmaValue, PrepareStmt, PreparedStatementBody,
    ProceduralStatement, ProcedureBody, ProcedureParameter, ReferentialAction, ReindexStmt,
    ReleaseSavepointStmt, RenameTableStmt, RollbackStmt, RollbackToSavepointStmt, RowFormat,
    SavepointStmt, ScheduleAfterStmt, ScheduleAtStmt, SchemaElement, SetCatalogStmt, SetNamesStmt,
    SetSchemaStmt, SetTimeZoneStmt, SetTransactionStmt, SetVariableStmt, SqlSecurity,
    StorageFormat, TableConstraint, TableConstraintKind, TableOption, TimeZoneSpec,
    TransactionAccessMode, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
    TruncateCascadeOption, TruncateTableStmt, TypeAttribute, TypeDefinition, VacuumStmt,
    VariableScope, VectorDistanceMetric,
};
pub use dml::{
    Assignment, ConflictClause, DeleteStmt, InsertSource, InsertStmt, OnConflictAction,
    OnConflictClause, UpdateStmt, WhereClause,
};
pub use expression::{
    CaseWhen, CharacterUnit, Expression, FrameBound, FrameExclude, FrameUnit, FulltextMode,
    IntervalUnit, PseudoTable, Quantifier, TrimPosition, TruthValue, WindowFrame,
    WindowFunctionSpec, WindowSpec,
};
pub use grant::{GrantStmt, ObjectType, PrivilegeType};
pub use introspection::{
    DescribeStmt, ExplainFormat, ExplainStmt, ShowColumnsStmt, ShowCreateTableStmt,
    ShowDatabasesStmt, ShowIndexStmt, ShowTablesStmt,
};
pub use operators::{BinaryOperator, UnaryOperator};
pub use revoke::{CascadeOption, RevokeStmt};
pub use select::{
    CommonTableExpr, CteMaterialization, FromClause, GroupByClause, GroupingElement, GroupingSet,
    JoinType, MixedGroupingItem, NullsOrder, OrderByItem, OrderDirection, SelectItem, SelectStmt,
    SetOperation, SetOperator, WindowDefinition,
};
pub use statement::Statement;

// SQL Identifiers with SQL:1999 case sensitivity handling
pub use identifier::{ColumnIdentifier, FunctionIdentifier, Identifier, TableIdentifier};
