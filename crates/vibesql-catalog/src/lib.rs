//! Catalog - Schema Metadata Storage
//!
//! Provides metadata structures for tables and columns along with the catalog
//! registry that tracks table schemas.

/// Default schema name for SQLite compatibility.
/// SQLite uses "main" as the default database/schema name.
pub const DEFAULT_SCHEMA: &str = "main";

/// Temporary schema name for SQLite compatibility.
/// SQLite stores temporary tables in a separate "temp" schema.
/// Temp tables are not persisted and have session scope.
pub const TEMP_SCHEMA: &str = "temp";

/// Name prefix for the internal unique index materialized for a WITHOUT ROWID
/// table's PRIMARY KEY.
///
/// In SQLite the PRIMARY KEY of a WITHOUT ROWID table *is* the table B-tree and
/// gets no separate `sqlite_autoindex_*` entry, so a `UNIQUE` constraint on the
/// same table is the first real index and is named `sqlite_autoindex_<t>_1`.
/// VibeSQL still materializes a real unique index to enforce/plan the PK, but
/// names it `<prefix><table>` — outside the `sqlite_autoindex_` namespace — so
/// it neither consumes an autoindex ordinal nor surfaces in `sqlite_master`.
/// The index is regenerated from DDL on reload, so persistence layers skip it
/// on save (see issue #5882).
pub const WITHOUT_ROWID_PK_INDEX_PREFIX: &str = "_withoutrowidinternalpk_";

mod advanced_objects;
mod column;
mod domain;
pub mod errors;
mod foreign_key;
mod index;
mod privilege;
mod schema;
mod store;
mod table;
mod trigger;
mod type_definition;
mod view;

pub use advanced_objects::{
    Assertion, CharacterSet, Collation, Domain, Function, FunctionBody, FunctionParam,
    ParameterMode, Procedure, ProcedureBody, ProcedureParam, Sequence, SqlSecurity, Translation,
    UserDefinedType,
};
pub use column::ColumnSchema;
pub use domain::{DomainConstraintDef, DomainDefinition};
pub use errors::CatalogError;
pub use foreign_key::{ForeignKeyConstraint, ReferentialAction};
// Re-export identifiers from vibesql-ast
pub use index::{IndexMetadata, IndexType, IndexedColumn, SortOrder, VectorDistanceMetric};
pub use privilege::PrivilegeGrant;
pub use schema::Schema;
pub use store::{Catalog, ViewDropBehavior};
pub use table::{StorageFormat, StrictType, TableSchema};
pub use trigger::TriggerDefinition;
pub use type_definition::{TypeAttribute, TypeDefinition, TypeDefinitionKind};
pub use vibesql_ast::{ColumnIdentifier, Identifier, TableIdentifier};
pub use view::ViewDefinition;

#[cfg(test)]
mod tests;
