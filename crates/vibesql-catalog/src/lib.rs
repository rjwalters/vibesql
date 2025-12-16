//! Catalog - Schema Metadata Storage
//!
//! Provides metadata structures for tables and columns along with the catalog
//! registry that tracks table schemas.

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
pub use vibesql_ast::{ColumnIdentifier, Identifier, TableIdentifier};
pub use index::{IndexMetadata, IndexType, IndexedColumn, SortOrder, VectorDistanceMetric};
pub use privilege::PrivilegeGrant;
pub use schema::Schema;
pub use store::{Catalog, ViewDropBehavior};
pub use table::{StorageFormat, TableSchema};
pub use trigger::TriggerDefinition;
pub use type_definition::{TypeAttribute, TypeDefinition, TypeDefinitionKind};
pub use view::ViewDefinition;

#[cfg(test)]
mod tests;
