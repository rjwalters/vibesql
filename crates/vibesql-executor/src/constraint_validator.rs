//! Constraint validation logic for DDL operations
//!
//! This module provides reusable constraint validation that can be used by
//! CREATE TABLE, ALTER TABLE, and other DDL executors.

#![allow(clippy::new_without_default)]

use vibesql_ast::{
    pretty_print::ToSql,
    visitor::{walk_expression, ExpressionVisitor, VisitResult},
    ColumnConstraintKind, ColumnDef, Expression, TableConstraint, TableConstraintKind,
};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_types::DataType;

use crate::errors::ExecutorError;

/// Visitor that flags any bind parameter (`?`, `?NNN`, or `:name`) reached
/// while walking an expression. SQLite prohibits parameters inside CHECK
/// constraints because the constraint is evaluated at every INSERT/UPDATE with
/// no bind context (see check-5.1 / check-5.2).
struct ParameterFinder {
    found: bool,
}

impl ExpressionVisitor for ParameterFinder {
    fn visit_placeholder(&mut self, _index: usize) -> VisitResult {
        self.found = true;
        VisitResult::Stop
    }

    fn visit_numbered_placeholder(&mut self, _number: usize) -> VisitResult {
        self.found = true;
        VisitResult::Stop
    }

    fn visit_named_placeholder(&mut self, _name: &str) -> VisitResult {
        self.found = true;
        VisitResult::Stop
    }
}

/// True if `expr` contains any bind parameter. Used to reject
/// `CHECK( x < :abc )` / `CHECK( x < ? )` at CREATE TABLE time.
fn expression_has_parameter(expr: &Expression) -> bool {
    let mut finder = ParameterFinder { found: false };
    walk_expression(&mut finder, expr);
    finder.found
}

/// Visitor that flags the first column reference in a CHECK constraint that
/// cannot be resolved against the table being created. SQLite resolves CHECK
/// constraint column names against the new table only: an unqualified name
/// must be a column of the table (or a rowid alias), and a `table.column`
/// qualifier must name the table being created. A three-part `schema.table.col`
/// reference has its database qualifier silently ignored (check-8.1), so only
/// the table and column parts are validated.
struct CheckColumnResolver<'a> {
    /// Canonical (lower-cased) name of the table being created.
    table_canonical: &'a str,
    /// Canonical (lower-cased) column names of the table being created.
    columns: &'a std::collections::HashSet<String>,
    /// Display form of the first unresolved reference, if any.
    unresolved: Option<String>,
}

impl ExpressionVisitor for CheckColumnResolver<'_> {
    fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
        if let Expression::ColumnRef(col_id) = expr {
            // A table qualifier must name the table being created; the schema
            // (database) qualifier, if present, is ignored per SQLite.
            if let Some(table) = col_id.table_canonical() {
                if !table.eq_ignore_ascii_case(self.table_canonical) {
                    self.unresolved = Some(col_id.display().to_string());
                    return VisitResult::Stop;
                }
            }

            let col = col_id.column_canonical();
            // rowid and its aliases are always valid inside a CHECK constraint
            // (e.g. `CHECK(c > rowid*2)`, check-9.1).
            let is_rowid_alias = col.eq_ignore_ascii_case("rowid")
                || col == "_rowid_"
                || col.eq_ignore_ascii_case("oid");
            if !is_rowid_alias && !self.columns.contains(col) {
                self.unresolved = Some(col_id.display().to_string());
                return VisitResult::Stop;
            }
        }
        VisitResult::Continue
    }
}

/// Validate that every column referenced by a table's CHECK constraints exists
/// on the table being created. SQLite performs this resolution at CREATE TABLE
/// time and rejects the statement — leaving no table behind — when a CHECK
/// names an unknown column (check-3.3) or a foreign table (check-3.5). This
/// mirrors that behavior so an invalid definition never creates a table.
pub fn validate_check_constraint_columns(
    table_name: &str,
    columns: &[ColumnSchema],
    check_constraints: &[(String, Expression)],
) -> Result<(), ExecutorError> {
    if check_constraints.is_empty() {
        return Ok(());
    }
    let column_set: std::collections::HashSet<String> =
        columns.iter().map(|c| c.name.to_ascii_lowercase()).collect();
    let table_canonical = table_name.to_ascii_lowercase();
    for (_name, expr) in check_constraints {
        let mut resolver = CheckColumnResolver {
            table_canonical: &table_canonical,
            columns: &column_set,
            unresolved: None,
        };
        walk_expression(&mut resolver, expr);
        if let Some(column_ref) = resolver.unresolved {
            return Err(ExecutorError::NoSuchColumn { column_ref });
        }
    }
    Ok(())
}

/// Validate that a CHECK constraint expression uses only forms SQLite permits.
/// Subqueries and bind parameters are rejected at CREATE TABLE time with the
/// exact SQLite messages so an invalid definition never creates a half-formed
/// table (check-3.1, check-5.1, check-5.2).
fn validate_check_expression(expr: &Expression) -> Result<(), ExecutorError> {
    if crate::dml_returning::expression_has_subquery(expr) {
        return Err(ExecutorError::SqliteCompatError(
            "subqueries prohibited in CHECK constraints".to_string(),
        ));
    }
    if expression_has_parameter(expr) {
        return Err(ExecutorError::SqliteCompatError(
            "parameters prohibited in CHECK constraints".to_string(),
        ));
    }
    Ok(())
}

/// Result of processing constraints
pub struct ConstraintResult {
    /// Primary key column names (if any)
    pub primary_key: Option<Vec<String>>,
    /// Explicit per-key-part `COLLATE` for the PRIMARY KEY, parallel to
    /// `primary_key`. `None` for a key part with no explicit collation (falls
    /// back to the column's declared collation, then BINARY). Issue #5881.
    pub primary_key_collations: Option<Vec<Option<String>>>,
    /// UNIQUE constraints (each Vec<String> is a set of columns)
    pub unique_constraints: Vec<Vec<String>>,
    /// Explicit per-key-part `COLLATE` for each UNIQUE constraint, parallel to
    /// `unique_constraints`. Issue #5881.
    pub unique_constraint_collations: Vec<Vec<Option<String>>>,
    /// CHECK constraints (name, expression pairs)
    pub check_constraints: Vec<(String, Expression)>,
    /// Columns that should be marked as NOT NULL
    pub not_null_columns: Vec<String>,
}

impl ConstraintResult {
    /// Create an empty constraint result
    pub fn new() -> Self {
        Self {
            primary_key: None,
            primary_key_collations: None,
            unique_constraints: Vec::new(),
            unique_constraint_collations: Vec::new(),
            check_constraints: Vec::new(),
            not_null_columns: Vec::new(),
        }
    }
}

/// Extract the explicit key-part `COLLATE` names from a table-level
/// PRIMARY KEY / UNIQUE column list, positionally aligned with the columns.
/// Expression key parts (which cannot appear in these constraints today)
/// contribute `None`. Issue #5881.
fn key_part_collations(columns: &[vibesql_ast::IndexColumn]) -> Vec<Option<String>> {
    columns
        .iter()
        .map(|c| match c {
            vibesql_ast::IndexColumn::Column { collation, .. } => collation.clone(),
            vibesql_ast::IndexColumn::Expression { .. } => None,
        })
        .collect()
}

/// Deduplicate repeated key parts within a table-level PRIMARY KEY, matching
/// SQLite's index-key semantics: a later key part is dropped when an earlier
/// kept part references the same column *and* has the same effective collation.
///
/// `PRIMARY KEY(a, a, b)` collapses to `(a, b)` (both `a` parts share the
/// column's default collation), but `PRIMARY KEY(a COLLATE nocase, a)` keeps
/// both parts because their collations differ. Without this, a duplicated PK
/// column produced a malformed internal PK index whose uniqueness check never
/// fired — e.g. `INSERT`ing two rows equal under the effective collation
/// silently succeeded. Issue #6171 (WITHOUT ROWID support), matching SQLite's
/// `without_rowid7-1.1`.
fn dedup_pk_key_parts(
    pk_columns: &[String],
    pk_collations: &[Option<String>],
    columns: &[ColumnDef],
) -> (Vec<String>, Vec<Option<String>>) {
    // Effective collation for a key part: the explicit key-part COLLATE if
    // present, else the column's declared COLLATE, else BINARY.
    let effective = |name: &str, part_coll: &Option<String>| -> String {
        if let Some(c) = part_coll {
            return c.to_ascii_lowercase();
        }
        columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
            .and_then(|c| {
                c.constraints.iter().find_map(|cn| {
                    if let ColumnConstraintKind::Collate(coll) = &cn.kind {
                        Some(coll.to_ascii_lowercase())
                    } else {
                        None
                    }
                })
            })
            .unwrap_or_else(|| "binary".to_string())
    };

    let mut out_cols: Vec<String> = Vec::with_capacity(pk_columns.len());
    let mut out_colls: Vec<Option<String>> = Vec::with_capacity(pk_columns.len());
    let mut seen: Vec<(String, String)> = Vec::new();

    for (i, name) in pk_columns.iter().enumerate() {
        let part_coll = pk_collations.get(i).cloned().flatten();
        let key = (name.to_ascii_lowercase(), effective(name, &part_coll));
        if seen.contains(&key) {
            continue; // exact duplicate key part — drop it
        }
        seen.push(key);
        out_cols.push(name.clone());
        out_colls.push(part_coll);
    }

    (out_cols, out_colls)
}

/// Constraint validator for table creation and alteration
pub struct ConstraintValidator;

impl ConstraintValidator {
    /// Process all constraints from column definitions and table constraints
    ///
    /// # Arguments
    ///
    /// * `table_name` - The table name (used in SQLite-compatible error messages)
    /// * `columns` - The column definitions from the DDL statement
    /// * `table_constraints` - The table-level constraints
    ///
    /// # Returns
    ///
    /// A `ConstraintResult` containing all processed constraints, or an error if validation fails
    ///
    /// # Errors
    ///
    /// Returns `ExecutorError::MultiplePrimaryKeys` if multiple PRIMARY KEY constraints are defined
    pub fn process_constraints(
        table_name: &str,
        columns: &[ColumnDef],
        table_constraints: &[TableConstraint],
    ) -> Result<ConstraintResult, ExecutorError> {
        let mut result = ConstraintResult::new();

        // Track if we've seen a primary key at column level
        let mut has_column_level_pk = false;

        // Process column-level constraints
        for col_def in columns {
            for constraint in &col_def.constraints {
                match &constraint.kind {
                    ColumnConstraintKind::PrimaryKey { .. } => {
                        if has_column_level_pk {
                            return Err(ExecutorError::MultiplePrimaryKeys {
                                table_name: table_name.to_string(),
                            });
                        }
                        if result.primary_key.is_some() {
                            return Err(ExecutorError::MultiplePrimaryKeys {
                                table_name: table_name.to_string(),
                            });
                        }
                        result.primary_key = Some(vec![col_def.name.clone()]);
                        // Column-level PK carries no key-part COLLATE (any COLLATE
                        // is a separate column constraint that sets the column's
                        // declared collation); enforcement falls back to that.
                        result.primary_key_collations = Some(vec![None]);
                        // SQLite quirk: only INTEGER PRIMARY KEY has implicit NOT NULL
                        // Other types (TEXT, REAL, BLOB, etc.) can have NULL in PRIMARY KEY
                        if col_def.data_type == DataType::Integer {
                            result.not_null_columns.push(col_def.name.clone());
                        }
                        has_column_level_pk = true;
                    }
                    ColumnConstraintKind::Unique { .. } => {
                        result.unique_constraints.push(vec![col_def.name.clone()]);
                        // Column-level UNIQUE carries no key-part COLLATE; fall
                        // back to the column's declared collation at enforcement.
                        result.unique_constraint_collations.push(vec![None]);
                    }
                    ColumnConstraintKind::Check { expr, source_text } => {
                        // SQLite rejects subqueries and bind parameters inside
                        // CHECK constraints at CREATE TABLE time (check-3.1,
                        // check-5.1). Enforce the same prohibition so an invalid
                        // definition never leaves a half-formed table behind.
                        validate_check_expression(expr)?;
                        // Use explicit name if provided; otherwise use the
                        // verbatim CHECK source text (SQLite echoes the
                        // original operator spacing in the violation message),
                        // falling back to the re-rendered expression only when
                        // no source span was captured.
                        let constraint_name = constraint
                            .name
                            .clone()
                            .or_else(|| source_text.clone())
                            .unwrap_or_else(|| expr.to_sql());
                        result.check_constraints.push((constraint_name, (**expr).clone()));
                    }
                    ColumnConstraintKind::NotNull
                    | ColumnConstraintKind::NotNullWithConflict { .. } => {
                        result.not_null_columns.push(col_def.name.clone());
                    }
                    ColumnConstraintKind::References { .. } => {
                        // Foreign key constraints are handled separately
                        // during INSERT/UPDATE/DELETE operations
                    }
                    ColumnConstraintKind::AutoIncrement => {
                        // AUTO_INCREMENT is handled in create_table.rs by creating
                        // an internal sequence and setting the default value
                        // No constraint validation needed here
                    }
                    ColumnConstraintKind::Key => {
                        // KEY is a MySQL-specific index marker
                        // For MVP, we parse it but don't enforce indexing behavior
                        // No constraint validation needed here
                    }
                    ColumnConstraintKind::Collate(_) => {
                        // COLLATE specifies the collation for string comparisons
                        // For MVP, we parse it but don't enforce collation behavior
                        // No constraint validation needed here
                    }
                }
            }
        }

        // Process table-level constraints
        for table_constraint in table_constraints {
            match &table_constraint.kind {
                TableConstraintKind::PrimaryKey { columns: pk_cols, .. } => {
                    // Only allow one PRIMARY KEY constraint total (column-level OR table-level)
                    if result.primary_key.is_some() {
                        return Err(ExecutorError::MultiplePrimaryKeys {
                            table_name: table_name.to_string(),
                        });
                    }
                    // Extract column names from IndexColumn structs
                    let raw_column_names: Vec<String> =
                        pk_cols.iter().map(|c| c.expect_column_name().to_string()).collect();
                    // Carry the explicit per-key-part COLLATE so INSERT uniqueness
                    // enforcement can honor `PRIMARY KEY(a COLLATE nocase)` (#5881).
                    let raw_collations = key_part_collations(pk_cols);
                    // Collapse exact-duplicate key parts (same column + same
                    // effective collation) the way SQLite does, so a PK like
                    // `(a, a, b)` becomes `(a, b)` and its internal unique index
                    // is well-formed (#6171).
                    let (column_names, collations) =
                        dedup_pk_key_parts(&raw_column_names, &raw_collations, columns);
                    result.primary_key = Some(column_names.clone());
                    result.primary_key_collations = Some(collations);
                    // SQLite quirk: only INTEGER PRIMARY KEY has implicit NOT NULL
                    // For table-level constraints, check each column's type
                    for col_name in &column_names {
                        if let Some(col_def) = columns.iter().find(|c| &c.name == col_name) {
                            if col_def.data_type == DataType::Integer
                                && !result.not_null_columns.contains(col_name)
                            {
                                result.not_null_columns.push(col_name.to_string());
                            }
                        }
                    }
                }
                TableConstraintKind::Unique { columns, .. } => {
                    // Extract column names from IndexColumn structs
                    let column_names: Vec<String> =
                        columns.iter().map(|c| c.expect_column_name().to_string()).collect();
                    result.unique_constraints.push(column_names);
                    // Carry the explicit per-key-part COLLATE so INSERT uniqueness
                    // enforcement can honor `UNIQUE(a COLLATE nocase)` (#5881).
                    result.unique_constraint_collations.push(key_part_collations(columns));
                }
                TableConstraintKind::Check { expr, source_text } => {
                    // SQLite rejects subqueries and bind parameters inside CHECK
                    // constraints at CREATE TABLE time (check-3.1, check-5.1).
                    // Enforce the same prohibition for table-level CHECKs too.
                    validate_check_expression(expr)?;
                    // Use explicit name if provided; otherwise the verbatim
                    // CHECK source text, falling back to the re-rendered
                    // expression only when no source span was captured.
                    let constraint_name = table_constraint
                        .name
                        .clone()
                        .or_else(|| source_text.clone())
                        .unwrap_or_else(|| expr.to_sql());
                    result.check_constraints.push((constraint_name, (**expr).clone()));
                }
                TableConstraintKind::ForeignKey { .. } => {
                    // Foreign key constraints are handled separately
                    // during INSERT/UPDATE/DELETE operations
                }
                TableConstraintKind::Fulltext { .. } => {
                    // FULLTEXT index constraints are handled separately
                    // during table creation/schema updates
                    // TODO: Implement FULLTEXT index tracking
                }
            }
        }

        Ok(result)
    }

    /// Apply constraint results to a mutable column list
    ///
    /// This updates column nullability based on NOT NULL and PRIMARY KEY constraints
    ///
    /// # Arguments
    ///
    /// * `columns` - The column schemas to update
    /// * `constraint_result` - The constraint processing results
    pub fn apply_to_columns(columns: &mut [ColumnSchema], constraint_result: &ConstraintResult) {
        // Mark NOT NULL columns as non-nullable
        for col_name in &constraint_result.not_null_columns {
            if let Some(col) = columns.iter_mut().find(|c| c.name == *col_name) {
                col.nullable = false;
            }
        }
    }

    /// Apply constraint results to a table schema
    ///
    /// This sets the primary key, unique constraints, and check constraints on the schema
    ///
    /// # Arguments
    ///
    /// * `table_schema` - The table schema to update
    /// * `constraint_result` - The constraint processing results
    pub fn apply_to_schema(table_schema: &mut TableSchema, constraint_result: &ConstraintResult) {
        // Set primary key
        if let Some(pk) = &constraint_result.primary_key {
            table_schema.primary_key = Some(pk.clone());
            // Key-part collations are aligned with the PK column list (#5881).
            table_schema.primary_key_collations = constraint_result.primary_key_collations.clone();
        }

        // Set unique constraints
        table_schema.unique_constraints = constraint_result.unique_constraints.clone();
        table_schema.unique_constraint_collations =
            constraint_result.unique_constraint_collations.clone();

        // Set check constraints
        table_schema.check_constraints = constraint_result.check_constraints.clone();
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::ColumnConstraint;
    use vibesql_types::DataType;

    use super::*;

    fn make_column_def(name: &str, constraint_kinds: Vec<ColumnConstraintKind>) -> ColumnDef {
        make_column_def_with_type(name, DataType::Integer, constraint_kinds)
    }

    fn make_column_def_with_type(
        name: &str,
        data_type: DataType,
        constraint_kinds: Vec<ColumnConstraintKind>,
    ) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: true,
            constraints: constraint_kinds
                .into_iter()
                .map(|kind| ColumnConstraint { name: None, kind })
                .collect(),
            default_value: None,
            comment: None,
            generated_expr: None,
            is_exact_integer_type: false,
            type_source: None,
        }
    }

    fn col(name: &str) -> ColumnSchema {
        ColumnSchema::new(name.to_string(), DataType::Integer, true)
    }

    fn col_ref(name: &str) -> Expression {
        Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(name, false))
    }

    fn qualified_col_ref(table: &str, column: &str) -> Expression {
        Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(table, false, column, false))
    }

    fn lt(left: Expression, right: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(left),
            op: vibesql_ast::BinaryOperator::LessThan,
            right: Box::new(right),
        }
    }

    #[test]
    fn test_check_columns_unknown_column_rejected() {
        // check-3.3: CHECK(q < x) where `q` is not a column -> "no such column: q".
        let cols = vec![col("x"), col("y"), col("z")];
        let checks = vec![(String::new(), lt(col_ref("q"), col_ref("x")))];
        let err = validate_check_constraint_columns("t3", &cols, &checks).unwrap_err();
        match err {
            ExecutorError::NoSuchColumn { column_ref } => assert_eq!(column_ref, "q"),
            other => panic!("expected NoSuchColumn, got {other:?}"),
        }
    }

    #[test]
    fn test_check_columns_foreign_table_qualifier_rejected() {
        // check-3.5: CHECK(t2.x < x) references a different table -> "no such column: t2.x".
        let cols = vec![col("x"), col("y"), col("z")];
        let checks = vec![(String::new(), lt(qualified_col_ref("t2", "x"), col_ref("x")))];
        let err = validate_check_constraint_columns("t3", &cols, &checks).unwrap_err();
        match err {
            ExecutorError::NoSuchColumn { column_ref } => assert_eq!(column_ref, "t2.x"),
            other => panic!("expected NoSuchColumn, got {other:?}"),
        }
    }

    #[test]
    fn test_check_columns_self_table_qualifier_accepted() {
        // check-3.7: CHECK(t3.x < 25) qualifies with the table being created -> ok.
        let cols = vec![col("x"), col("y"), col("z")];
        let checks = vec![(
            String::new(),
            lt(
                qualified_col_ref("t3", "x"),
                Expression::Literal(vibesql_types::SqlValue::Integer(25)),
            ),
        )];
        assert!(validate_check_constraint_columns("t3", &cols, &checks).is_ok());
    }

    #[test]
    fn test_check_columns_rowid_alias_accepted() {
        // check-9.1: CHECK(c > rowid*2) — rowid is a valid pseudo-column.
        let cols = vec![col("a"), col("c")];
        let checks = vec![(
            String::new(),
            Expression::BinaryOp {
                left: Box::new(col_ref("c")),
                op: vibesql_ast::BinaryOperator::GreaterThan,
                right: Box::new(col_ref("rowid")),
            },
        )];
        assert!(validate_check_constraint_columns("t1", &cols, &checks).is_ok());
    }

    #[test]
    fn test_check_columns_database_qualifier_ignored() {
        // check-8.1: three-part `schema.table.column` — the (possibly bogus)
        // database qualifier is ignored; only table+column are validated.
        let cols = vec![col("b")];
        let ref_bogus_schema =
            Expression::ColumnRef(vibesql_ast::ColumnIdentifier::fully_qualified(
                "xyzzy", false, "t811", false, "b", false,
            ));
        let checks = vec![(String::new(), lt(ref_bogus_schema, col_ref("b")))];
        assert!(validate_check_constraint_columns("t811", &cols, &checks).is_ok());
    }

    #[test]
    fn test_column_level_primary_key() {
        let columns = vec![make_column_def(
            "id",
            vec![ColumnConstraintKind::PrimaryKey { on_conflict: None }],
        )];
        let result = ConstraintValidator::process_constraints("test_table", &columns, &[]).unwrap();

        assert_eq!(result.primary_key, Some(vec!["id".to_string()]));
        assert!(result.not_null_columns.contains(&"id".to_string()));
    }

    #[test]
    fn test_table_level_primary_key() {
        let columns = vec![make_column_def("id", vec![]), make_column_def("tenant_id", vec![])];
        let constraints = vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![
                    vibesql_ast::IndexColumn::Column {
                        column_name: "id".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                        collation: None,
                    },
                    vibesql_ast::IndexColumn::Column {
                        column_name: "tenant_id".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                        collation: None,
                    },
                ],
                on_conflict: None,
            },
        }];

        let result =
            ConstraintValidator::process_constraints("test_table", &columns, &constraints).unwrap();

        assert_eq!(result.primary_key, Some(vec!["id".to_string(), "tenant_id".to_string()]));
        assert!(result.not_null_columns.contains(&"id".to_string()));
        assert!(result.not_null_columns.contains(&"tenant_id".to_string()));
    }

    /// Build a table-level PRIMARY KEY constraint from `(column, collation)`
    /// key-part specs.
    fn pk_constraint(parts: &[(&str, Option<&str>)]) -> TableConstraint {
        TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: parts
                    .iter()
                    .map(|(name, coll)| vibesql_ast::IndexColumn::Column {
                        column_name: name.to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                        collation: coll.map(|c| c.to_string()),
                    })
                    .collect(),
                on_conflict: None,
            },
        }
    }

    /// `PRIMARY KEY(a, a, b)` collapses to `(a, b)` — the two `a` key parts
    /// share the column's default collation, so SQLite drops the duplicate.
    /// Matches SQLite `without_rowid7-1.1`. Issue #6171.
    #[test]
    fn test_primary_key_dedups_repeated_columns() {
        let columns = vec![make_column_def("a", vec![]), make_column_def("b", vec![])];
        let constraints = vec![pk_constraint(&[("a", None), ("a", None), ("b", None)])];

        let result = ConstraintValidator::process_constraints("t", &columns, &constraints).unwrap();

        assert_eq!(result.primary_key, Some(vec!["a".to_string(), "b".to_string()]));
        // Collations stay positionally aligned with the deduped column list.
        assert_eq!(result.primary_key_collations, Some(vec![None, None]));
    }

    /// `PRIMARY KEY(a COLLATE nocase, a)` keeps BOTH key parts: the first is
    /// nocase, the second falls back to the column's default (BINARY), so their
    /// effective collations differ and neither is a duplicate of the other.
    /// Matches SQLite `without_rowid7-2.x`. Issue #6171.
    #[test]
    fn test_primary_key_keeps_columns_with_distinct_collations() {
        let columns = vec![make_column_def("a", vec![]), make_column_def("b", vec![])];
        let constraints = vec![pk_constraint(&[("a", Some("nocase")), ("a", None)])];

        let result = ConstraintValidator::process_constraints("t", &columns, &constraints).unwrap();

        assert_eq!(result.primary_key, Some(vec!["a".to_string(), "a".to_string()]));
        assert_eq!(result.primary_key_collations, Some(vec![Some("nocase".to_string()), None]));
    }

    /// A repeated key part whose explicit COLLATE matches the column's declared
    /// COLLATE is still a duplicate (both resolve to the same effective
    /// collation) and is dropped. Issue #6171.
    #[test]
    fn test_primary_key_dedups_when_explicit_collation_matches_declared() {
        let columns = vec![
            make_column_def("a", vec![ColumnConstraintKind::Collate("nocase".to_string())]),
            make_column_def("b", vec![]),
        ];
        // (a, a COLLATE nocase) — column a is declared COLLATE nocase, so the
        // first (default) part also resolves to nocase → the two are duplicates.
        let constraints = vec![pk_constraint(&[("a", None), ("a", Some("nocase")), ("b", None)])];

        let result = ConstraintValidator::process_constraints("t", &columns, &constraints).unwrap();

        assert_eq!(result.primary_key, Some(vec!["a".to_string(), "b".to_string()]));
    }

    #[test]
    fn test_multiple_primary_keys_fails() {
        let columns = vec![make_column_def(
            "id",
            vec![ColumnConstraintKind::PrimaryKey { on_conflict: None }],
        )];
        let constraints = vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![vibesql_ast::IndexColumn::Column {
                    column_name: "id".to_string(),
                    direction: vibesql_ast::OrderDirection::Asc,
                    prefix_length: None,
                    collation: None,
                }],
                on_conflict: None,
            },
        }];

        let result = ConstraintValidator::process_constraints("test_table", &columns, &constraints);
        assert!(matches!(result, Err(ExecutorError::MultiplePrimaryKeys { .. })));
        // SQLite-compatible wording (misc1-7.1/7.2, fuzz-8.1)
        let err = result.err().expect("expected MultiplePrimaryKeys error");
        assert_eq!(err.to_string(), "table \"test_table\" has more than one primary key");
    }

    #[test]
    fn test_unique_constraints() {
        let columns = vec![
            make_column_def("email", vec![ColumnConstraintKind::Unique { on_conflict: None }]),
            make_column_def("username", vec![]),
        ];
        let constraints = vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::Unique {
                columns: vec![vibesql_ast::IndexColumn::Column {
                    column_name: "username".to_string(),
                    direction: vibesql_ast::OrderDirection::Asc,
                    prefix_length: None,
                    collation: None,
                }],
                on_conflict: None,
            },
        }];

        let result =
            ConstraintValidator::process_constraints("test_table", &columns, &constraints).unwrap();

        assert_eq!(result.unique_constraints.len(), 2);
        assert!(result.unique_constraints.contains(&vec!["email".to_string()]));
        assert!(result.unique_constraints.contains(&vec!["username".to_string()]));
    }

    #[test]
    fn test_check_constraints() {
        use vibesql_types::SqlValue;

        let check_expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "age", false,
            ))),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(0))),
        };

        let columns = vec![make_column_def(
            "age",
            // No source_text (programmatic AST) → name falls back to expr.to_sql().
            vec![ColumnConstraintKind::Check {
                expr: Box::new(check_expr.clone()),
                source_text: None,
            }],
        )];

        let result = ConstraintValidator::process_constraints("test_table", &columns, &[]).unwrap();

        assert_eq!(result.check_constraints.len(), 1);
        assert_eq!(result.check_constraints[0].1, check_expr);
        // Programmatic AST carries no source span, so the name falls back to
        // the re-rendered expression text.
        assert_eq!(result.check_constraints[0].0, check_expr.to_sql());
    }

    /// Assert that a parsed `CREATE TABLE` is rejected by constraint processing
    /// with `expected` as the exact error message.
    fn assert_create_table_rejected(sql: &str, expected: &str) {
        let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
        let create = match stmt {
            vibesql_ast::Statement::CreateTable(c) => c,
            other => panic!("expected CREATE TABLE, got {:?}", other),
        };
        let res = ConstraintValidator::process_constraints(
            &create.table_name,
            &create.columns,
            &create.table_constraints,
        );
        match res {
            Ok(_) => panic!("expected rejection of `{}`", sql),
            Err(e) => assert_eq!(e.to_string(), expected),
        }
    }

    #[test]
    fn test_check_constraint_rejects_parameter() {
        // A bind parameter inside a CHECK is rejected at CREATE TABLE time with
        // SQLite's exact message (check-5.1 / check-5.2).
        assert_create_table_rejected(
            "CREATE TABLE t5(x, y, CHECK( x*y < :abc ))",
            "parameters prohibited in CHECK constraints",
        );
        assert_create_table_rejected(
            "CREATE TABLE t5(x, y, CHECK( x*y < ? ))",
            "parameters prohibited in CHECK constraints",
        );
    }

    #[test]
    fn test_check_constraint_rejects_subquery() {
        // A subquery inside a CHECK is rejected at CREATE TABLE time with
        // SQLite's exact message (check-3.1).
        assert_create_table_rejected(
            "CREATE TABLE t3(x, y, z, CHECK( x < (SELECT min(x) FROM t1) ))",
            "subqueries prohibited in CHECK constraints",
        );
    }

    #[test]
    fn test_check_constraint_name_preserves_source_spacing() {
        // When the parser captured verbatim source text, the constraint name
        // (used in "CHECK constraint failed: <name>") echoes the original
        // operator spacing rather than the whitespace-stripped `to_sql()`.
        let check_expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "d", false,
            ))),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(0))),
        };

        // Spaced source form is preserved verbatim.
        let columns = vec![make_column_def(
            "d",
            vec![ColumnConstraintKind::Check {
                expr: Box::new(check_expr.clone()),
                source_text: Some("d > 0".to_string()),
            }],
        )];
        let result = ConstraintValidator::process_constraints("t", &columns, &[]).unwrap();
        assert_eq!(result.check_constraints[0].0, "d > 0");

        // Unspaced source form is likewise preserved verbatim (SQLite echoes
        // exactly what the user wrote).
        let columns = vec![make_column_def(
            "d",
            vec![ColumnConstraintKind::Check {
                expr: Box::new(check_expr.clone()),
                source_text: Some("d>0".to_string()),
            }],
        )];
        let result = ConstraintValidator::process_constraints("t", &columns, &[]).unwrap();
        assert_eq!(result.check_constraints[0].0, "d>0");

        // An explicit constraint name always wins over the source text.
        let mut col = make_column_def("d", vec![]);
        col.constraints.push(ColumnConstraint {
            name: Some("chk_d".to_string()),
            kind: ColumnConstraintKind::Check {
                expr: Box::new(check_expr.clone()),
                source_text: Some("d > 0".to_string()),
            },
        });
        let result = ConstraintValidator::process_constraints("t", &[col], &[]).unwrap();
        assert_eq!(result.check_constraints[0].0, "chk_d");
    }

    #[test]
    fn test_apply_to_columns() {
        let mut columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, true),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];

        let mut result = ConstraintResult::new();
        result.not_null_columns.push("id".to_string());

        ConstraintValidator::apply_to_columns(&mut columns, &result);

        assert!(!columns[0].nullable); // id should be NOT NULL
        assert!(columns[1].nullable); // name should still be nullable
    }

    // Tests for SQLite quirk: only INTEGER PRIMARY KEY has implicit NOT NULL

    #[test]
    fn test_text_primary_key_allows_null() {
        // TEXT PRIMARY KEY should NOT have implicit NOT NULL
        let columns = vec![make_column_def_with_type(
            "name",
            DataType::Varchar { max_length: None },
            vec![ColumnConstraintKind::PrimaryKey { on_conflict: None }],
        )];
        let result = ConstraintValidator::process_constraints("test_table", &columns, &[]).unwrap();

        assert_eq!(result.primary_key, Some(vec!["name".to_string()]));
        // NOT NULL should NOT be added for non-INTEGER PRIMARY KEY
        assert!(!result.not_null_columns.contains(&"name".to_string()));
    }

    #[test]
    fn test_typeless_primary_key_allows_null() {
        // Typeless columns (parsed as Varchar) should NOT have implicit NOT NULL
        let columns = vec![make_column_def_with_type(
            "c",
            DataType::Varchar { max_length: None },
            vec![ColumnConstraintKind::PrimaryKey { on_conflict: None }],
        )];
        let result = ConstraintValidator::process_constraints("test_table", &columns, &[]).unwrap();

        assert_eq!(result.primary_key, Some(vec!["c".to_string()]));
        assert!(!result.not_null_columns.contains(&"c".to_string()));
    }

    #[test]
    fn test_integer_primary_key_has_not_null() {
        // INTEGER PRIMARY KEY should still have implicit NOT NULL
        let columns = vec![make_column_def_with_type(
            "id",
            DataType::Integer,
            vec![ColumnConstraintKind::PrimaryKey { on_conflict: None }],
        )];
        let result = ConstraintValidator::process_constraints("test_table", &columns, &[]).unwrap();

        assert_eq!(result.primary_key, Some(vec!["id".to_string()]));
        assert!(result.not_null_columns.contains(&"id".to_string()));
    }

    #[test]
    fn test_table_level_pk_with_mixed_types() {
        // Table-level PK with mixed types: only INTEGER columns get NOT NULL
        let columns = vec![
            make_column_def_with_type("id", DataType::Integer, vec![]),
            make_column_def_with_type("code", DataType::Varchar { max_length: None }, vec![]),
        ];
        let constraints = vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![
                    vibesql_ast::IndexColumn::Column {
                        column_name: "id".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                        collation: None,
                    },
                    vibesql_ast::IndexColumn::Column {
                        column_name: "code".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                        collation: None,
                    },
                ],
                on_conflict: None,
            },
        }];

        let result =
            ConstraintValidator::process_constraints("test_table", &columns, &constraints).unwrap();

        assert_eq!(result.primary_key, Some(vec!["id".to_string(), "code".to_string()]));
        // Only the INTEGER column should have NOT NULL
        assert!(result.not_null_columns.contains(&"id".to_string()));
        assert!(!result.not_null_columns.contains(&"code".to_string()));
    }

    #[test]
    fn test_real_primary_key_allows_null() {
        // REAL PRIMARY KEY should NOT have implicit NOT NULL
        let columns = vec![make_column_def_with_type(
            "value",
            DataType::Real,
            vec![ColumnConstraintKind::PrimaryKey { on_conflict: None }],
        )];
        let result = ConstraintValidator::process_constraints("test_table", &columns, &[]).unwrap();

        assert_eq!(result.primary_key, Some(vec!["value".to_string()]));
        assert!(!result.not_null_columns.contains(&"value".to_string()));
    }

    #[test]
    fn test_bigint_primary_key_allows_null() {
        // BIGINT is not INTEGER, so should allow NULL in PRIMARY KEY
        let columns = vec![make_column_def_with_type(
            "big_id",
            DataType::Bigint,
            vec![ColumnConstraintKind::PrimaryKey { on_conflict: None }],
        )];
        let result = ConstraintValidator::process_constraints("test_table", &columns, &[]).unwrap();

        assert_eq!(result.primary_key, Some(vec!["big_id".to_string()]));
        // SQLite only treats INTEGER (not INT, BIGINT, etc.) specially
        assert!(!result.not_null_columns.contains(&"big_id".to_string()));
    }

    // -----------------------------------------------------------------------
    // Per-key-part COLLATE collection + effective resolution (issue #5881)
    // -----------------------------------------------------------------------

    fn index_col_with_collation(name: &str, collation: Option<&str>) -> vibesql_ast::IndexColumn {
        vibesql_ast::IndexColumn::Column {
            column_name: name.to_string(),
            direction: vibesql_ast::OrderDirection::Asc,
            prefix_length: None,
            collation: collation.map(|s| s.to_string()),
        }
    }

    #[test]
    fn test_table_level_pk_collects_key_part_collation() {
        let columns =
            vec![make_column_def_with_type("a", DataType::Varchar { max_length: None }, vec![])];
        let constraints = vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![index_col_with_collation("a", Some("nocase"))],
                on_conflict: None,
            },
        }];
        let result = ConstraintValidator::process_constraints("t", &columns, &constraints).unwrap();
        assert_eq!(result.primary_key_collations, Some(vec![Some("nocase".to_string())]));
    }

    #[test]
    fn test_table_level_unique_collects_key_part_collation() {
        let columns = vec![
            make_column_def_with_type("a", DataType::Varchar { max_length: None }, vec![]),
            make_column_def_with_type("b", DataType::Varchar { max_length: None }, vec![]),
        ];
        let constraints = vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::Unique {
                columns: vec![
                    index_col_with_collation("a", Some("rtrim")),
                    index_col_with_collation("b", None),
                ],
                on_conflict: None,
            },
        }];
        let result = ConstraintValidator::process_constraints("t", &columns, &constraints).unwrap();
        assert_eq!(
            result.unique_constraint_collations,
            vec![vec![Some("rtrim".to_string()), None]]
        );
    }

    #[test]
    fn test_column_level_pk_has_no_key_part_collation() {
        // A column-level PRIMARY KEY carries no key-part COLLATE; any collation
        // is a separate column constraint that lives on the column itself.
        let columns = vec![make_column_def_with_type(
            "id",
            DataType::Varchar { max_length: None },
            vec![ColumnConstraintKind::PrimaryKey { on_conflict: None }],
        )];
        let result = ConstraintValidator::process_constraints("t", &columns, &[]).unwrap();
        assert_eq!(result.primary_key_collations, Some(vec![None]));
    }

    #[test]
    fn test_apply_to_schema_effective_collation_prefers_key_part() {
        // Key-part COLLATE nocase on column `a` (which itself has no declared
        // collation) → effective PK collation is nocase.
        use vibesql_catalog::{ColumnSchema, TableSchema};
        let columns =
            vec![make_column_def_with_type("a", DataType::Varchar { max_length: None }, vec![])];
        let constraints = vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![index_col_with_collation("a", Some("NOCASE"))],
                on_conflict: None,
            },
        }];
        let result = ConstraintValidator::process_constraints("t", &columns, &constraints).unwrap();

        let mut schema = TableSchema::new(
            "t".to_string(),
            vec![ColumnSchema::new("a".to_string(), DataType::Varchar { max_length: None }, true)],
        );
        ConstraintValidator::apply_to_schema(&mut schema, &result);

        assert_eq!(
            schema.primary_key_effective_collations(),
            Some(vec![Some("NOCASE".to_string())])
        );
    }

    #[test]
    fn test_effective_collation_falls_back_to_column_collation() {
        // No key-part COLLATE, but the column is declared NOCASE → effective PK
        // collation falls back to the column's declared collation.
        use vibesql_catalog::{ColumnSchema, TableSchema};
        let columns =
            vec![make_column_def_with_type("a", DataType::Varchar { max_length: None }, vec![])];
        let constraints = vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![index_col_with_collation("a", None)],
                on_conflict: None,
            },
        }];
        let result = ConstraintValidator::process_constraints("t", &columns, &constraints).unwrap();

        let mut col =
            ColumnSchema::new("a".to_string(), DataType::Varchar { max_length: None }, true);
        col.collation = Some("nocase".to_string());
        let mut schema = TableSchema::new("t".to_string(), vec![col]);
        ConstraintValidator::apply_to_schema(&mut schema, &result);

        assert_eq!(
            schema.primary_key_effective_collations(),
            Some(vec![Some("nocase".to_string())])
        );
    }
}
