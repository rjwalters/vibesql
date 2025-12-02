//! Arena-allocated DDL types for ALTER TABLE operations.

use bumpalo::collections::Vec as BumpVec;
use vibesql_types::DataType;

use super::Expression;

/// Arena-allocated ALTER TABLE statement.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableStmt<'arena> {
    AddColumn(AddColumnStmt<'arena>),
    DropColumn(DropColumnStmt<'arena>),
    AlterColumn(AlterColumnStmt<'arena>),
    AddConstraint(AddConstraintStmt<'arena>),
    DropConstraint(DropConstraintStmt<'arena>),
    RenameTable(RenameTableStmt<'arena>),
    ModifyColumn(ModifyColumnStmt<'arena>),
    ChangeColumn(ChangeColumnStmt<'arena>),
}

/// Arena-allocated ADD COLUMN operation.
#[derive(Debug, Clone, PartialEq)]
pub struct AddColumnStmt<'arena> {
    pub table_name: &'arena str,
    pub column_def: ColumnDef<'arena>,
}

/// Arena-allocated DROP COLUMN operation.
#[derive(Debug, Clone, PartialEq)]
pub struct DropColumnStmt<'arena> {
    pub table_name: &'arena str,
    pub column_name: &'arena str,
    pub if_exists: bool,
}

/// Arena-allocated ALTER COLUMN operation.
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

/// Arena-allocated ADD CONSTRAINT operation.
#[derive(Debug, Clone, PartialEq)]
pub struct AddConstraintStmt<'arena> {
    pub table_name: &'arena str,
    pub constraint: TableConstraint<'arena>,
}

/// Arena-allocated DROP CONSTRAINT operation.
#[derive(Debug, Clone, PartialEq)]
pub struct DropConstraintStmt<'arena> {
    pub table_name: &'arena str,
    pub constraint_name: &'arena str,
}

/// Arena-allocated RENAME TABLE operation.
#[derive(Debug, Clone, PartialEq)]
pub struct RenameTableStmt<'arena> {
    pub table_name: &'arena str,
    pub new_table_name: &'arena str,
}

/// Arena-allocated MODIFY COLUMN operation (MySQL-style).
#[derive(Debug, Clone, PartialEq)]
pub struct ModifyColumnStmt<'arena> {
    pub table_name: &'arena str,
    pub column_name: &'arena str,
    pub new_column_def: ColumnDef<'arena>,
}

/// Arena-allocated CHANGE COLUMN operation (MySQL-style - rename and modify).
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeColumnStmt<'arena> {
    pub table_name: &'arena str,
    pub old_column_name: &'arena str,
    pub new_column_def: ColumnDef<'arena>,
}

/// Arena-allocated column definition.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef<'arena> {
    pub name: &'arena str,
    pub data_type: DataType,
    pub nullable: bool,
    pub constraints: BumpVec<'arena, ColumnConstraint<'arena>>,
    pub default_value: Option<&'arena Expression<'arena>>,
    pub comment: Option<&'arena str>,
}

/// Arena-allocated column-level constraint.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnConstraint<'arena> {
    pub name: Option<&'arena str>,
    pub kind: ColumnConstraintKind<'arena>,
}

/// Arena-allocated column constraint types.
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

/// Arena-allocated table-level constraint.
#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint<'arena> {
    pub name: Option<&'arena str>,
    pub kind: TableConstraintKind<'arena>,
}

/// Arena-allocated table constraint types.
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

/// Arena-allocated index column specification.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn<'arena> {
    pub column_name: &'arena str,
    pub length: Option<u32>,
    pub order: Option<SortOrder>,
}

/// Sort order for index columns.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Referential action for foreign key constraints.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}
