// ============================================================================
// Constraint Rehydration for the Binary Catalog Load Path (issue #5834)
// ============================================================================
//
// The binary catalog persists each table's columns, primary key, and the
// verbatim `CREATE TABLE` source text (`sql_source`, format v9+), but it does
// NOT serialize `TableSchema::check_constraints` or
// `TableSchema::foreign_keys` as dedicated fields. Before this module
// existed, a reloaded schema silently dropped both: violating INSERTs
// succeeded after a process restart, `PRAGMA foreign_key_list` returned no
// rows, and ON DELETE/UPDATE actions (CASCADE etc.) never fired — even
// though `sqlite_master.sql` still showed the constraint text.
//
// Fix: re-parse the persisted `sql_source` with the full main-parser grammar
// (the same grammar that accepted it at CREATE TABLE time — DDL never goes
// through the arena parser) and map the parsed CHECK and FOREIGN KEY clauses
// back onto the loaded `TableSchema`. This mirrors how views, expression
// indexes, and partial-index WHERE clauses already persist-as-SQL and
// re-parse on load (issues #5619 / #5833 / PR #5863), and requires no binary
// format bump.
//
// The mapping below intentionally replicates the semantics of the CREATE
// TABLE execution path (`vibesql-executor/src/create_table.rs` +
// `ConstraintValidator::process_constraints`), which cannot be called from
// this crate (the executor depends on storage, not vice versa):
//
// - CHECK constraint names: explicit `CONSTRAINT <name>` when given,
//   otherwise the expression's SQL text (SQLite-compatible error messages).
//   Column-level CHECKs are collected first (in column order), then
//   table-level CHECKs, matching `ConstraintValidator`.
// - FK ordering: table-level constraints in declaration order, then
//   column-level `REFERENCES` constraints in REVERSE column order (SQLite's
//   FK id assignment, mirrored from `create_table.rs`).
// - Implicit parent keys (`REFERENCES p` with no column list): parent column
//   names are stored as empty strings and indices as 0, exactly like the
//   CREATE path; enforcement resolves the parent's PK by name at runtime.
// - Unknown parent tables keep placeholder indices (SQLite stores FK
//   metadata even when the parent doesn't exist yet).
//
// A `sql_source` that fails to re-parse, or that is not a CREATE TABLE
// statement, is a hard load error — never a silently-unenforced constraint —
// following the recovery-failure policy established for expression indexes
// (issue #5833).

use std::collections::HashMap;

use crate::StorageError;

/// Column-name lookup for every table present in the catalog being loaded,
/// keyed by lowercased table name. Used to resolve FK parent column indices
/// regardless of the (arbitrary) order tables appear in the file.
///
/// `pub(crate)`: the WAL crash-recovery path (`wal::recovery`) routes replayed
/// `CreateTable` ops through the same rehydration (issue #5883).
pub(crate) type ParentColumnLookup = HashMap<String, Vec<String>>;

/// Convert an AST referential action (optional; absent = NO ACTION) into the
/// catalog representation. Mirrors `convert_action` in the executor's CREATE
/// TABLE path.
fn convert_action(
    action: &Option<vibesql_ast::ReferentialAction>,
) -> vibesql_catalog::ReferentialAction {
    match action.as_ref().unwrap_or(&vibesql_ast::ReferentialAction::NoAction) {
        vibesql_ast::ReferentialAction::Cascade => vibesql_catalog::ReferentialAction::Cascade,
        vibesql_ast::ReferentialAction::SetNull => vibesql_catalog::ReferentialAction::SetNull,
        vibesql_ast::ReferentialAction::SetDefault => {
            vibesql_catalog::ReferentialAction::SetDefault
        }
        vibesql_ast::ReferentialAction::Restrict => vibesql_catalog::ReferentialAction::Restrict,
        vibesql_ast::ReferentialAction::NoAction => vibesql_catalog::ReferentialAction::NoAction,
    }
}

/// Resolve a column name to its index within `columns`, matching
/// `TableSchema::get_column_index` semantics: exact match first, then
/// case-insensitive fallback.
fn find_column_index(columns: &[String], name: &str) -> Option<usize> {
    if let Some(idx) = columns.iter().position(|c| c == name) {
        return Some(idx);
    }
    let lower = name.to_lowercase();
    columns.iter().position(|c| c.to_lowercase() == lower)
}

/// Resolve the parent-side column names/indices for one FK clause.
///
/// When no parent column list was given (`REFERENCES p`), SQLite stores an
/// empty "to" name per FK column and the enforcement layer resolves the
/// parent's PRIMARY KEY at runtime; we replicate that with empty strings and
/// placeholder 0 indices. When the parent table is not in the lookup (it may
/// legitimately not exist), indices fall back to placeholder 0 — the runtime
/// resolver in the executor prefers name-based resolution anyway.
fn resolve_parent_columns(
    parent_table: &str,
    references_columns: &[String],
    fk_column_count: usize,
    parents: &ParentColumnLookup,
) -> (Vec<String>, Vec<usize>) {
    if references_columns.is_empty() {
        return (vec![String::new(); fk_column_count], vec![0; fk_column_count]);
    }

    let parent_cols = parents.get(&parent_table.to_lowercase());
    let indices: Vec<usize> = references_columns
        .iter()
        .map(|name| parent_cols.and_then(|cols| find_column_index(cols, name)).unwrap_or(0))
        .collect();
    (references_columns.to_vec(), indices)
}

/// Rebuild `check_constraints`, `foreign_keys`, and the INTEGER PRIMARY KEY
/// rowid alias (`rowid_alias_column` + `is_exact_integer_type`, issue #5835)
/// on a freshly loaded `TableSchema` by re-parsing its persisted
/// `sql_source`.
///
/// No-op when the schema has no `sql_source` (pre-v9 files, or tables whose
/// source was invalidated by ALTER TABLE — those fall back to the previous
/// behavior). Errors when the source no longer parses or disagrees with the
/// stored column set, so constraint loss is never silent.
pub(crate) fn rehydrate_constraints_from_sql_source(
    schema: &mut vibesql_catalog::TableSchema,
    parents: &ParentColumnLookup,
) -> Result<(), StorageError> {
    let Some(src) = schema.sql_source.clone() else {
        return Ok(());
    };

    let stmt = vibesql_parser::Parser::parse_sql(&src).map_err(|e| {
        StorageError::NotImplemented(format!(
            "Failed to re-parse CREATE TABLE source for table '{}' while rehydrating \
             constraints: {} (source: {})",
            schema.name, e, src
        ))
    })?;

    let create = match stmt {
        vibesql_ast::Statement::CreateTable(create) => create,
        other => {
            return Err(StorageError::NotImplemented(format!(
                "Persisted sql_source for table '{}' is not a CREATE TABLE statement \
                 (parsed as {:?})",
                schema.name,
                std::mem::discriminant(&other)
            )));
        }
    };

    // CREATE TABLE ... AS SELECT carries no constraint clauses.
    if create.as_query.is_some() {
        return Ok(());
    }

    // ---- STRICT flag + per-column strict types (issue #5837) ----
    // Neither `TableSchema::strict` nor `strict_types` is serialized as a
    // dedicated binary field; both are rederived here from the re-parsed
    // `sql_source`, exactly like CHECK/FK constraints below. This keeps STRICT
    // enforcement alive across a process restart without a binary format bump.
    // A column that no longer classifies (should be impossible for a table that
    // was accepted at CREATE time) leaves the table non-strict rather than
    // failing the load.
    if create.strict {
        let mut strict_types = Vec::with_capacity(schema.columns.len());
        let mut all_classified = true;
        for col in &schema.columns {
            let declared = create
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(&col.name))
                .and_then(|c| c.type_source.as_deref());
            match declared.and_then(vibesql_catalog::StrictType::classify) {
                Some(st) => strict_types.push(st),
                None => {
                    all_classified = false;
                    break;
                }
            }
        }
        if all_classified {
            schema.strict = true;
            schema.strict_types = strict_types;
        }
    }

    // ---- INTEGER PRIMARY KEY rowid aliasing (issue #5835) ----
    // Neither `TableSchema::rowid_alias_column` nor
    // `ColumnSchema::is_exact_integer_type` is serialized as a dedicated
    // binary field, so before this rehydration every reloaded schema forgot
    // that its INTEGER PRIMARY KEY column aliases the rowid. All rowid
    // reads/writes (`WHERE rowid=N`, `SELECT rowid`, `DELETE ... WHERE
    // rowid=N`, `RETURNING rowid`) then fell back to physical row numbering
    // — returning zero rows or, worse, targeting the WRONG row after a
    // process restart (intpkey-2.6).
    //
    // Rebuild both from the re-parsed source, mirroring the CREATE TABLE
    // execution path (`create_table.rs`): a single-column PRIMARY KEY whose
    // column was declared with the exact keyword "INTEGER" (not "INT")
    // aliases the rowid.
    for col_def in &create.columns {
        if let Some(idx) = schema.get_column_index(&col_def.name) {
            schema.columns[idx].is_exact_integer_type = col_def.is_exact_integer_type;
        }
    }
    if let Some(pk_cols) = schema.primary_key.clone() {
        if pk_cols.len() == 1 {
            if let Some(col_idx) = schema.get_column_index(&pk_cols[0]) {
                let col = &schema.columns[col_idx];
                if matches!(col.data_type, vibesql_types::DataType::Integer)
                    && col.is_exact_integer_type
                {
                    schema.set_rowid_alias_column(Some(col_idx));
                }
            }
        }
    }

    // ---- AUTOINCREMENT flag (issue #6173) ----
    // `TableSchema::is_autoincrement` is not a serialized binary field either
    // (same rationale as `rowid_alias_column`/`strict` above): rederive it
    // from the re-parsed source so a process restart doesn't silently forget
    // an AUTOINCREMENT column and start reusing rowids after a full DELETE.
    // Mirrors `create_table.rs`'s validation: only column-level AUTOINCREMENT
    // is checked here (the table-level `PRIMARY KEY(x AUTOINCREMENT)` spelling
    // is not yet accepted by the parser), and only takes effect when the
    // column ended up as the rowid alias above.
    if let Some(col_idx) = schema.rowid_alias_column {
        let col_name = &schema.columns[col_idx].name;
        let is_autoincrement = create.columns.iter().any(|col_def| {
            col_def.name.eq_ignore_ascii_case(col_name)
                && col_def.constraints.iter().any(|c| {
                    matches!(c.kind, vibesql_ast::ColumnConstraintKind::AutoIncrement)
                })
        });
        if is_autoincrement {
            schema.is_autoincrement = true;
        }
    }

    // ---- PRIMARY KEY key-part collations (issue #5881) ----
    // The explicit per-key-part COLLATE (`PRIMARY KEY(a COLLATE nocase)`) is not
    // a serialized binary field, so before this rehydration a reloaded WITHOUT
    // ROWID / composite PK forgot its collation and INSERT uniqueness silently
    // reverted to BINARY (a case-variant duplicate would wrongly be accepted).
    // Rederive it from the re-parsed source, aligned with `primary_key`. A
    // column-level PK (no table-level key-part COLLATE) leaves every entry
    // `None`, which falls back to the column's declared collation at enforcement.
    if let Some(pk_cols) = schema.primary_key.clone() {
        let table_level_key_parts = create.table_constraints.iter().find_map(|tc| {
            if let vibesql_ast::TableConstraintKind::PrimaryKey { columns, .. } = &tc.kind {
                Some(
                    columns
                        .iter()
                        .map(|c| match c {
                            vibesql_ast::IndexColumn::Column { collation, .. } => collation.clone(),
                            vibesql_ast::IndexColumn::Expression { .. } => None,
                        })
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            }
        });
        schema.primary_key_collations =
            Some(table_level_key_parts.unwrap_or_else(|| vec![None; pk_cols.len()]));
    }

    // ---- CHECK constraints (column-level first, then table-level) ----
    // Matches ConstraintValidator::process_constraints ordering and naming.
    let mut check_constraints: Vec<(String, vibesql_ast::Expression)> = Vec::new();
    for col_def in &create.columns {
        for constraint in &col_def.constraints {
            if let vibesql_ast::ColumnConstraintKind::Check { expr, source_text } = &constraint.kind
            {
                use vibesql_ast::pretty_print::ToSql;
                // Prefer the explicit name, then the verbatim CHECK source
                // text (preserves the original operator spacing that SQLite
                // echoes in "CHECK constraint failed: <expr>"), falling back
                // to the re-rendered expression only if no span was captured.
                let name = constraint
                    .name
                    .clone()
                    .or_else(|| source_text.clone())
                    .unwrap_or_else(|| expr.to_sql());
                check_constraints.push((name, (**expr).clone()));
            }
        }
    }
    for table_constraint in &create.table_constraints {
        if let vibesql_ast::TableConstraintKind::Check { expr, source_text } =
            &table_constraint.kind
        {
            use vibesql_ast::pretty_print::ToSql;
            let name = table_constraint
                .name
                .clone()
                .or_else(|| source_text.clone())
                .unwrap_or_else(|| expr.to_sql());
            check_constraints.push((name, (**expr).clone()));
        }
    }
    schema.check_constraints = check_constraints;

    // ---- FOREIGN KEY constraints ----
    let mut foreign_keys: Vec<vibesql_catalog::ForeignKeyConstraint> = Vec::new();

    // Table-level FKs, in declaration order.
    for constraint in &create.table_constraints {
        if let vibesql_ast::TableConstraintKind::ForeignKey {
            columns: fk_columns,
            references_table,
            references_columns,
            on_delete,
            on_update,
            deferral,
        } = &constraint.kind
        {
            let column_indices: Vec<usize> = fk_columns
                .iter()
                .map(|col_name| {
                    schema.get_column_index(col_name).ok_or_else(|| {
                        StorageError::NotImplemented(format!(
                            "FK column '{}' from persisted sql_source not found in loaded \
                             schema for table '{}'",
                            col_name, schema.name
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let (parent_column_names, parent_column_indices) = resolve_parent_columns(
                references_table,
                references_columns,
                fk_columns.len(),
                parents,
            );

            let (is_deferrable, initially_deferred) =
                deferral.map(|d| (d.is_deferrable, d.initially_deferred)).unwrap_or((false, false));

            foreign_keys.push(vibesql_catalog::ForeignKeyConstraint {
                name: constraint.name.clone(),
                column_names: fk_columns.clone(),
                column_indices,
                parent_table: references_table.clone(),
                parent_column_names,
                parent_column_indices,
                on_delete: convert_action(on_delete),
                on_update: convert_action(on_update),
                is_deferrable,
                initially_deferred,
            });
        }
    }

    // Column-level REFERENCES, added in REVERSE column order to match
    // SQLite's FK id assignment (mirrored from create_table.rs).
    let mut column_level_fks: Vec<vibesql_catalog::ForeignKeyConstraint> = Vec::new();
    for col_def in &create.columns {
        for constraint in &col_def.constraints {
            if let vibesql_ast::ColumnConstraintKind::References {
                table: ref_table,
                column: ref_column,
                on_delete,
                on_update,
                deferral,
            } = &constraint.kind
            {
                let col_idx = schema.get_column_index(&col_def.name).ok_or_else(|| {
                    StorageError::NotImplemented(format!(
                        "FK column '{}' from persisted sql_source not found in loaded \
                         schema for table '{}'",
                        col_def.name, schema.name
                    ))
                })?;

                let (parent_col_name, parent_col_idx) = if let Some(col) = ref_column {
                    let idx = parents
                        .get(&ref_table.to_lowercase())
                        .and_then(|cols| find_column_index(cols, col))
                        .unwrap_or(0);
                    (col.clone(), idx)
                } else {
                    // Implicit parent PK: empty name + placeholder index,
                    // resolved by the enforcement layer at runtime.
                    (String::new(), 0)
                };

                let (is_deferrable, initially_deferred) = deferral
                    .map(|d| (d.is_deferrable, d.initially_deferred))
                    .unwrap_or((false, false));

                column_level_fks.push(vibesql_catalog::ForeignKeyConstraint {
                    name: constraint.name.clone(),
                    column_names: vec![col_def.name.clone()],
                    column_indices: vec![col_idx],
                    parent_table: ref_table.clone(),
                    parent_column_names: vec![parent_col_name],
                    parent_column_indices: vec![parent_col_idx],
                    on_delete: convert_action(on_delete),
                    on_update: convert_action(on_update),
                    is_deferrable,
                    initially_deferred,
                });
            }
        }
    }
    foreign_keys.extend(column_level_fks.into_iter().rev());

    schema.foreign_keys = foreign_keys;

    Ok(())
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, ReferentialAction, TableSchema};
    use vibesql_types::DataType;

    use super::*;

    fn col(name: &str) -> ColumnSchema {
        ColumnSchema::new(name.to_string(), DataType::Integer, true)
    }

    fn schema_with_source(name: &str, columns: Vec<ColumnSchema>, src: &str) -> TableSchema {
        let mut schema = TableSchema::new(name.to_string(), columns);
        schema.set_sql_source(src.to_string());
        schema
    }

    #[test]
    fn no_sql_source_is_a_noop() {
        let mut schema = TableSchema::new("t".to_string(), vec![col("a")]);
        rehydrate_constraints_from_sql_source(&mut schema, &HashMap::new()).unwrap();
        assert!(schema.check_constraints.is_empty());
        assert!(schema.foreign_keys.is_empty());
    }

    #[test]
    fn rehydrates_column_and_table_level_checks() {
        let mut schema = schema_with_source(
            "t",
            vec![col("a"), col("b")],
            "CREATE TABLE t(a INTEGER CHECK(a > 0), b INTEGER, \
             CONSTRAINT b_pos CHECK(b > 0))",
        );
        rehydrate_constraints_from_sql_source(&mut schema, &HashMap::new()).unwrap();

        assert_eq!(schema.check_constraints.len(), 2);
        // Unnamed column-level CHECK gets the verbatim CHECK source text as
        // its name, preserving the original operator spacing (SQLite echoes
        // exactly this in "CHECK constraint failed: a > 0"). Same derivation
        // as ConstraintValidator at CREATE time.
        assert_eq!(schema.check_constraints[0].0, "a > 0");
        // Named table-level CHECK keeps its explicit name.
        assert_eq!(schema.check_constraints[1].0, "b_pos");
    }

    #[test]
    fn rehydrated_check_name_preserves_source_spacing() {
        // Both spaced and unspaced CHECK forms round-trip verbatim through
        // rehydration, matching SQLite's violation-message text byte-for-byte.
        let mut spaced =
            schema_with_source("t", vec![col("d")], "CREATE TABLE t(d INTEGER, CHECK (d > 0))");
        rehydrate_constraints_from_sql_source(&mut spaced, &HashMap::new()).unwrap();
        assert_eq!(spaced.check_constraints[0].0, "d > 0");

        let mut unspaced =
            schema_with_source("t", vec![col("d")], "CREATE TABLE t(d INTEGER, CHECK(d>0))");
        rehydrate_constraints_from_sql_source(&mut unspaced, &HashMap::new()).unwrap();
        assert_eq!(unspaced.check_constraints[0].0, "d>0");
    }

    #[test]
    fn rehydrates_column_level_fk_with_actions() {
        let mut parents = HashMap::new();
        parents.insert("p".to_string(), vec!["x".to_string(), "y".to_string()]);

        let mut schema = schema_with_source(
            "c",
            vec![col("a"), col("b")],
            "CREATE TABLE c(a INTEGER REFERENCES p(y) ON DELETE CASCADE ON UPDATE SET NULL, \
             b INTEGER)",
        );
        rehydrate_constraints_from_sql_source(&mut schema, &parents).unwrap();

        assert_eq!(schema.foreign_keys.len(), 1);
        let fk = &schema.foreign_keys[0];
        assert_eq!(fk.column_names, vec!["a".to_string()]);
        assert_eq!(fk.column_indices, vec![0]);
        assert_eq!(fk.parent_table, "p");
        assert_eq!(fk.parent_column_names, vec!["y".to_string()]);
        assert_eq!(fk.parent_column_indices, vec![1]);
        assert_eq!(fk.on_delete, ReferentialAction::Cascade);
        assert_eq!(fk.on_update, ReferentialAction::SetNull);
    }

    #[test]
    fn rehydrates_composite_table_level_fk() {
        let mut parents = HashMap::new();
        parents.insert("p".to_string(), vec!["x".to_string(), "y".to_string()]);

        let mut schema = schema_with_source(
            "c",
            vec![col("a"), col("b")],
            "CREATE TABLE c(a INTEGER, b INTEGER, \
             FOREIGN KEY(b, a) REFERENCES p(y, x) ON DELETE RESTRICT)",
        );
        rehydrate_constraints_from_sql_source(&mut schema, &parents).unwrap();

        assert_eq!(schema.foreign_keys.len(), 1);
        let fk = &schema.foreign_keys[0];
        assert_eq!(fk.column_names, vec!["b".to_string(), "a".to_string()]);
        assert_eq!(fk.column_indices, vec![1, 0]);
        assert_eq!(fk.parent_column_names, vec!["y".to_string(), "x".to_string()]);
        assert_eq!(fk.parent_column_indices, vec![1, 0]);
        assert_eq!(fk.on_delete, ReferentialAction::Restrict);
        assert_eq!(fk.on_update, ReferentialAction::NoAction);
    }

    #[test]
    fn implicit_parent_pk_stores_empty_names_and_placeholder_indices() {
        let mut parents = HashMap::new();
        parents.insert("p".to_string(), vec!["x".to_string()]);

        let mut schema =
            schema_with_source("c", vec![col("a")], "CREATE TABLE c(a INTEGER REFERENCES p)");
        rehydrate_constraints_from_sql_source(&mut schema, &parents).unwrap();

        assert_eq!(schema.foreign_keys.len(), 1);
        let fk = &schema.foreign_keys[0];
        // SQLite stores an empty "to" name for implicit PK references;
        // enforcement resolves the parent PK at runtime.
        assert_eq!(fk.parent_column_names, vec![String::new()]);
        assert_eq!(fk.parent_column_indices, vec![0]);
    }

    #[test]
    fn missing_parent_table_keeps_placeholder_indices() {
        let mut schema = schema_with_source(
            "c",
            vec![col("a")],
            "CREATE TABLE c(a INTEGER REFERENCES nowhere(z))",
        );
        rehydrate_constraints_from_sql_source(&mut schema, &HashMap::new()).unwrap();

        assert_eq!(schema.foreign_keys.len(), 1);
        let fk = &schema.foreign_keys[0];
        assert_eq!(fk.parent_table, "nowhere");
        assert_eq!(fk.parent_column_names, vec!["z".to_string()]);
        assert_eq!(fk.parent_column_indices, vec![0]);
    }

    #[test]
    fn column_level_fks_are_added_in_reverse_order() {
        // Mirrors SQLite's FK id assignment, and the executor CREATE path.
        let mut parents = HashMap::new();
        parents.insert("p".to_string(), vec!["x".to_string()]);

        let mut schema = schema_with_source(
            "c",
            vec![col("a"), col("b")],
            "CREATE TABLE c(a INTEGER REFERENCES p(x), b INTEGER REFERENCES p(x))",
        );
        rehydrate_constraints_from_sql_source(&mut schema, &parents).unwrap();

        assert_eq!(schema.foreign_keys.len(), 2);
        assert_eq!(schema.foreign_keys[0].column_names, vec!["b".to_string()]);
        assert_eq!(schema.foreign_keys[1].column_names, vec!["a".to_string()]);
    }

    #[test]
    fn deferrable_initially_deferred_survives() {
        let mut parents = HashMap::new();
        parents.insert("p".to_string(), vec!["x".to_string()]);

        let mut schema = schema_with_source(
            "c",
            vec![col("a")],
            "CREATE TABLE c(a INTEGER REFERENCES p(x) DEFERRABLE INITIALLY DEFERRED)",
        );
        rehydrate_constraints_from_sql_source(&mut schema, &parents).unwrap();

        let fk = &schema.foreign_keys[0];
        assert!(fk.is_deferrable);
        assert!(fk.initially_deferred);
    }

    #[test]
    fn self_referential_fk_resolves_against_own_columns() {
        let mut parents = HashMap::new();
        parents.insert("t".to_string(), vec!["id".to_string(), "parent_id".to_string()]);

        let mut schema = schema_with_source(
            "t",
            vec![col("id"), col("parent_id")],
            "CREATE TABLE t(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES t(id))",
        );
        rehydrate_constraints_from_sql_source(&mut schema, &parents).unwrap();

        let fk = &schema.foreign_keys[0];
        assert_eq!(fk.parent_table, "t");
        assert_eq!(fk.column_indices, vec![1]);
        assert_eq!(fk.parent_column_indices, vec![0]);
    }

    #[test]
    fn rehydrates_strict_flag_and_per_column_types() {
        // A STRICT table's `strict` flag and per-column strict types are not
        // serialized as binary fields — they are rederived from `sql_source`
        // on load (issue #5837). ANY must be distinguished from BLOB.
        let mut schema = schema_with_source(
            "t",
            vec![col("a"), col("b"), col("c"), col("d")],
            "CREATE TABLE t(a INT, b TEXT, c BLOB, d ANY) STRICT",
        );
        assert!(!schema.strict, "precondition: not yet rehydrated");
        rehydrate_constraints_from_sql_source(&mut schema, &HashMap::new()).unwrap();

        assert!(schema.strict);
        assert_eq!(
            schema.strict_types,
            vec![
                vibesql_catalog::StrictType::Int,
                vibesql_catalog::StrictType::Text,
                vibesql_catalog::StrictType::Blob,
                vibesql_catalog::StrictType::Any,
            ]
        );
    }

    #[test]
    fn non_strict_table_stays_non_strict_on_rehydrate() {
        let mut schema = schema_with_source("t", vec![col("a")], "CREATE TABLE t(a INTEGER)");
        rehydrate_constraints_from_sql_source(&mut schema, &HashMap::new()).unwrap();
        assert!(!schema.strict);
        assert!(schema.strict_types.is_empty());
    }

    #[test]
    fn rehydrates_primary_key_key_part_collation() {
        // The explicit `PRIMARY KEY(a COLLATE nocase)` key-part collation is not
        // a serialized binary field; it must be rederived from `sql_source` so
        // INSERT uniqueness keeps honoring it after a reload (issue #5881).
        let mut schema = schema_with_source(
            "t",
            vec![col("a"), col("b")],
            "CREATE TABLE t(a, b, PRIMARY KEY(a COLLATE nocase)) WITHOUT ROWID",
        );
        schema.primary_key = Some(vec!["a".to_string()]);
        rehydrate_constraints_from_sql_source(&mut schema, &HashMap::new()).unwrap();
        assert_eq!(schema.primary_key_collations, Some(vec![Some("nocase".to_string())]));
    }

    #[test]
    fn rehydrates_column_level_pk_with_no_key_part_collation() {
        // A column-level PK carries no key-part COLLATE, so every entry is None
        // (enforcement falls back to the column's declared collation).
        let mut schema =
            schema_with_source("t", vec![col("id")], "CREATE TABLE t(id INTEGER PRIMARY KEY)");
        schema.primary_key = Some(vec!["id".to_string()]);
        rehydrate_constraints_from_sql_source(&mut schema, &HashMap::new()).unwrap();
        assert_eq!(schema.primary_key_collations, Some(vec![None]));
    }

    #[test]
    fn unparseable_sql_source_is_a_hard_error() {
        let mut schema = TableSchema::new("t".to_string(), vec![col("a")]);
        schema.set_sql_source("CREATE GIBBERISH t(".to_string());
        let err = rehydrate_constraints_from_sql_source(&mut schema, &HashMap::new());
        assert!(err.is_err(), "constraint loss must never be silent");
    }
}
