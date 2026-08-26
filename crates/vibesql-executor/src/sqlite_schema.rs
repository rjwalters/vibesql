//! SQLite Schema Virtual Table
//!
//! Implements `sqlite_master` (and its alias `sqlite_schema`) for SQLite compatibility.
//! These virtual tables return metadata about database objects in the standard SQLite format.
//!
//! Schema:
//! ```sql
//! CREATE TABLE sqlite_master (
//!   type TEXT,      -- 'table', 'index', 'view', 'trigger'
//!   name TEXT,      -- name of the object
//!   tbl_name TEXT,  -- table the object is associated with
//!   rootpage INT,   -- internal (always 0 for VibeSQL)
//!   sql TEXT        -- CREATE statement that created the object
//! );
//! ```
//!
//! Reference: https://www.sqlite.org/schematab.html

use vibesql_ast::pretty_print::ToSql;
use vibesql_catalog::{ColumnSchema, ReferentialAction, SortOrder, TableSchema};
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use crate::{errors::ExecutorError, select::SelectResult};

/// Check if a table reference is sqlite_master or sqlite_schema.
///
/// The `main.`-qualified forms (`main.sqlite_master` / `main.sqlite_schema`) name
/// the same schema table — SQLite lets the schema table be referenced with an
/// explicit `main.` database qualifier, and the evidence suites do so heavily
/// (e.g. `list_all_views` in e_dropview.test queries `main.sqlite_master`). They
/// resolve identically to the unqualified names here (#6176).
pub fn is_sqlite_schema_table(table_name: &str) -> bool {
    let normalized = table_name.to_lowercase();
    matches!(
        normalized.as_str(),
        "sqlite_master" | "sqlite_schema" | "main.sqlite_master" | "main.sqlite_schema"
    )
}

/// Whether a table reference names `sqlite_master`/`sqlite_schema`, given the
/// catalog's live attachments.
///
/// Extends [`is_sqlite_schema_table`] (bare and `main.`-qualified forms) to
/// also recognize `<alias>.sqlite_master`/`<alias>.sqlite_schema` for any
/// currently-attached database alias. Real SQLite rejects ALTER TABLE, DROP
/// TABLE, INSERT, UPDATE, DELETE, ANALYZE, and CREATE INDEX against the
/// schema table uniformly, regardless of which live alias qualifies the name
/// (issue #6451) — this is the write-side guard counterpart to
/// [`resolve_sqlite_schema_query_scope`], which handles the analogous
/// alias-awareness for the SELECT/read path (#6436).
///
/// A qualifier that is neither `main` nor a live attachment (a stale alias
/// after `DETACH`, an alias that was never attached, or an unrelated schema
/// tag like `temp`) does NOT match — callers fall through to ordinary table
/// resolution, matching SQLite's "no such table"/"unknown database" behavior
/// for an unrecognized qualifier.
pub fn is_sqlite_schema_table_ref(catalog: &vibesql_catalog::Catalog, table_name: &str) -> bool {
    if is_sqlite_schema_table(table_name) {
        return true;
    }
    let normalized = table_name.to_lowercase();
    let Some((qualifier, object)) = normalized.split_once('.') else {
        return false;
    };
    matches!(object, "sqlite_master" | "sqlite_schema") && catalog.is_attached_schema(qualifier)
}

/// Resolve a (possibly schema-qualified) table reference against the
/// `sqlite_master`/`sqlite_schema` virtual table, returning the catalog
/// schema whose objects the query should be built from.
///
/// Handles three shapes:
///  - bare `sqlite_master` / `sqlite_schema` -> the `main` schema.
///  - `main.sqlite_master` / `main.sqlite_schema` -> the `main` schema (same as the bare form — see
///    [`is_sqlite_schema_table`]'s doc).
///  - `<alias>.sqlite_master` / `<alias>.sqlite_schema`, where `<alias>` is currently registered as
///    an attached database -> that attachment's own schema, so the query lists the attached
///    database's own catalog (tables/indexes/views/triggers), not `main`'s (#6436).
///
/// Returns `None` for anything else, including `<alias>.sqlite_master` where
/// `<alias>` is not (or no longer, e.g. after `DETACH`) an attached database —
/// callers fall through to ordinary table resolution, which reports "no such
/// table", matching SQLite's behavior for a stale/unknown alias.
pub fn resolve_sqlite_schema_query_scope(
    catalog: &vibesql_catalog::Catalog,
    table_name: &str,
) -> Option<String> {
    let normalized = table_name.to_lowercase();
    if matches!(normalized.as_str(), "sqlite_master" | "sqlite_schema") {
        return Some(vibesql_catalog::DEFAULT_SCHEMA.to_string());
    }
    let (qualifier, object) = normalized.split_once('.')?;
    if !matches!(object, "sqlite_master" | "sqlite_schema") {
        return None;
    }
    if qualifier == vibesql_catalog::DEFAULT_SCHEMA {
        return Some(vibesql_catalog::DEFAULT_SCHEMA.to_string());
    }
    if catalog.is_attached_schema(qualifier) {
        return Some(qualifier.to_string());
    }
    None
}

/// Check whether an object name is reserved for internal use.
///
/// SQLite reserves the `sqlite_` prefix for its own schema objects
/// (`sqlite_master`, `sqlite_sequence`, `sqlite_autoindex_*`, …) and rejects any
/// *user* `CREATE TABLE`/`CREATE INDEX` whose name begins with that prefix:
///
/// ```text
/// sqlite> CREATE TABLE sqlite_foo(x);
/// Error: object name reserved for internal use: sqlite_foo
/// ```
///
/// The match is case-insensitive (issue #5553 identifier folding); `SQLITE_foo`
/// is rejected just like `sqlite_foo`. This guard applies ONLY to user-issued
/// DDL — VibeSQL's own internal schema objects (sqlite_master, the
/// `sqlite_autoindex_*` auto-indexes, etc.) are created through dedicated
/// catalog APIs that never route through the user-facing executors, so they are
/// unaffected.
pub fn is_reserved_object_name(name: &str) -> bool {
    name.len() >= "sqlite_".len()
        && name.as_bytes()[.."sqlite_".len()].eq_ignore_ascii_case(b"sqlite_")
}

/// Check if a table reference is sqlite_temp_master or sqlite_temp_schema.
///
/// These are the temp-schema introspection views: they list objects that live
/// in the session temp schema (temp tables, and indexes on temp tables), and
/// are the temp-schema counterpart to `sqlite_master`. See issue #5513.
pub fn is_sqlite_temp_schema_table(table_name: &str) -> bool {
    let normalized = table_name.to_lowercase();
    matches!(
        normalized.as_str(),
        // The `temp.`-qualified forms name the temp schema's schema table, which
        // in SQLite is the temp-schema counterpart of sqlite_master (i.e.
        // sqlite_temp_master). e_dropview.test's `list_all_views` reads
        // `temp.sqlite_master` for the temp database (#6176).
        "sqlite_temp_master"
            | "sqlite_temp_schema"
            | "temp.sqlite_master"
            | "temp.sqlite_schema"
            | "temp.sqlite_temp_master"
            | "temp.sqlite_temp_schema"
    )
}

/// Get the schema for sqlite_master/sqlite_schema
pub fn get_sqlite_schema_table_schema() -> TableSchema {
    TableSchema::new(
        "sqlite_master".to_string(),
        vec![
            ColumnSchema::new("type".to_string(), DataType::Varchar { max_length: None }, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: None }, false),
            ColumnSchema::new(
                "tbl_name".to_string(),
                DataType::Varchar { max_length: None },
                false,
            ),
            ColumnSchema::new("rootpage".to_string(), DataType::Integer, false),
            ColumnSchema::new("sql".to_string(), DataType::Varchar { max_length: None }, true),
        ],
    )
}

/// Whether `index` is the implicit PRIMARY KEY index of a WITHOUT ROWID table
/// and must therefore be hidden from `sqlite_master` listings.
///
/// In SQLite, the PRIMARY KEY of a WITHOUT ROWID table *is* the table's B-tree
/// — no separate `sqlite_autoindex_*` entry exists for it in `sqlite_master`
/// (verified against sqlite3 3.51.0: `CREATE TABLE t(a, b, PRIMARY KEY(a))
/// WITHOUT ROWID` yields only the `table` row; a `UNIQUE` constraint on the
/// same table still gets its autoindex row, named `sqlite_autoindex_<t>_1`).
/// VibeSQL materializes a real unique index for the WITHOUT ROWID PK internally
/// (uniqueness enforcement and planning) under the
/// [`WITHOUT_ROWID_PK_INDEX_PREFIX`] name — outside the `sqlite_autoindex_`
/// namespace so it consumes no autoindex ordinal (issue #5882) — and we filter
/// it out at listing time here. Covers alterdropcol 7.2, where `SELECT sql FROM
/// sqlite_schema` must return only the CREATE TABLE row.
///
/// [`WITHOUT_ROWID_PK_INDEX_PREFIX`]: vibesql_catalog::WITHOUT_ROWID_PK_INDEX_PREFIX
fn is_without_rowid_pk_autoindex(
    catalog: &vibesql_catalog::Catalog,
    index: &vibesql_catalog::IndexMetadata,
) -> bool {
    if !index.name.starts_with(vibesql_catalog::WITHOUT_ROWID_PK_INDEX_PREFIX) {
        return false;
    }
    // Defensive: the internal PK index only ever exists on a WITHOUT ROWID table.
    catalog.get_table(&index.table_name).is_some_and(|table| table.without_rowid)
}

/// Build a single sqlite_master-format row.
fn schema_row(obj_type: &str, name: &str, tbl_name: &str, sql: String) -> Row {
    Row::new(vec![
        SqlValue::Varchar(arcstr::ArcStr::from(obj_type)),
        SqlValue::Varchar(arcstr::ArcStr::from(name)),
        SqlValue::Varchar(arcstr::ArcStr::from(tbl_name)),
        SqlValue::Integer(0), // rootpage - always 0 for VibeSQL
        SqlValue::Varchar(arcstr::ArcStr::from(sql)),
    ])
}

/// Build a `sqlite_master` row for an auto-created index (implicit PRIMARY KEY /
/// UNIQUE index, named `sqlite_autoindex_*`). SQLite reports these with a NULL
/// `sql` — the index is implied by its table's `CREATE TABLE` text and has no
/// standalone `CREATE INDEX` statement — so the row still exists but is filtered
/// out by the common `WHERE sql!=''` idiom (verified against sqlite3 3.51.0,
/// altercol.test 1.7/1.8/1.9/1.14). Emitting a reconstructed `CREATE UNIQUE
/// INDEX` here instead made the autoindex row leak into those schema queries.
fn schema_row_autoindex(name: &str, tbl_name: &str) -> Row {
    Row::new(vec![
        SqlValue::Varchar(arcstr::ArcStr::from("index")),
        SqlValue::Varchar(arcstr::ArcStr::from(name)),
        SqlValue::Varchar(arcstr::ArcStr::from(tbl_name)),
        SqlValue::Integer(0), // rootpage - always 0 for VibeSQL
        SqlValue::Null,
    ])
}

/// Whether `name` is an implicit PRIMARY KEY / UNIQUE auto-index (SQLite names
/// these `sqlite_autoindex_<table>_<n>`); such indexes carry a NULL `sql` in
/// `sqlite_master`.
fn is_sqlite_autoindex(name: &str) -> bool {
    name.len() >= "sqlite_autoindex_".len()
        && name[.."sqlite_autoindex_".len()].eq_ignore_ascii_case("sqlite_autoindex_")
}

/// Whether a view's/trigger's optional `schema` tag places it in
/// `schema_name`'s sqlite_master listing.
///
/// `None` (the common case: no explicit schema qualifier at CREATE time)
/// belongs to `main`. An explicit tag (e.g. `Some("main")` from `CREATE
/// TRIGGER main.tr ...`, or a hypothetical attached-alias tag) is matched
/// against `schema_name` case-insensitively. Callers must exclude temp-tagged
/// objects separately (`is_temp()`) before calling this — temp objects never
/// belong to any `sqlite_master` listing, they surface only via
/// `sqlite_temp_master`.
fn schema_tag_belongs_to(tag: Option<&str>, schema_name: &str) -> bool {
    match tag {
        None => schema_name.eq_ignore_ascii_case(vibesql_catalog::DEFAULT_SCHEMA),
        Some(s) => s.eq_ignore_ascii_case(schema_name),
    }
}

/// Execute a `sqlite_master` / `sqlite_schema` query.
///
/// Lists **main-schema** objects only. Temp-schema objects (temp tables and
/// indexes on temp tables) are reported by `sqlite_temp_master`, matching
/// SQLite 3.51.0 where a temp-table index appears in `sqlite_temp_master` and
/// never in `sqlite_master`. See issue #5513.
pub fn execute_sqlite_schema_query(
    catalog: &vibesql_catalog::Catalog,
) -> Result<SelectResult, ExecutorError> {
    execute_sqlite_schema_query_for_schema(catalog, vibesql_catalog::DEFAULT_SCHEMA)
}

/// Execute a `sqlite_master` / `sqlite_schema`-shaped query scoped to a single
/// catalog schema.
///
/// `schema_name` is either `main` (the ordinary `sqlite_master`/`sqlite_schema`
/// table) or an attached database's alias, for `<alias>.sqlite_master` /
/// `<alias>.sqlite_schema` (#6436) — the query lists that attachment's own
/// tables/indexes/views/triggers, never `main`'s. Temp-schema objects are
/// never included here regardless of `schema_name`; they are reported only by
/// `sqlite_temp_master` (see [`execute_sqlite_temp_schema_query`]), matching
/// SQLite 3.51.0's `<alias>.sqlite_temp_master` having no meaning for an
/// attached database (verified: `no such table`, not empty/main's rows).
pub fn execute_sqlite_schema_query_for_schema(
    catalog: &vibesql_catalog::Catalog,
    schema_name: &str,
) -> Result<SelectResult, ExecutorError> {
    let schema = get_sqlite_schema_table_schema();
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    // Collect each row with the creation ordinal of the object it describes, so
    // the output can be ordered to match SQLite, which lists sqlite_master rows
    // in object-creation order (a table's indexes appear right after the table
    // when created next — see pragma.test 23.1). See `SchemaRowCollector`.
    let mut collector = SchemaRowCollector::new(catalog);
    let is_main = schema_name.eq_ignore_ascii_case(vibesql_catalog::DEFAULT_SCHEMA);

    // Add tables. `list_tables()` returns the current (main) schema only
    // (temp tables are already excluded); `list_tables_in_schema` is used for
    // an attached alias so the query draws from that attachment's own tables,
    // not main's.
    let table_names =
        if is_main { catalog.list_tables() } else { catalog.list_tables_in_schema(schema_name) };
    for table_name in table_names {
        // Look up via the schema-qualified name for an attached alias so the
        // attachment's own copy is read even if a same-named main table
        // exists (must not cross-contaminate, #6436).
        let lookup_name =
            if is_main { table_name.clone() } else { format!("{schema_name}.{table_name}") };
        if let Some(table) = catalog.get_table(&lookup_name) {
            let sql = generate_create_table_sql(table);
            // SQLite echoes the *original* declared case in sqlite_master, even
            // though lookups are case-folded (issue #5553). `table.name` retains
            // the original spelling; `table_name` is the lowercase catalog key.
            collector.push(
                schema_name,
                &table.name,
                schema_row("table", &table.name, &table.name, sql),
            );
        }
    }

    // Add indexes owned by this schema. Temp-table indexes are tagged with
    // their temp schema (#5513) and surface only via sqlite_temp_master.
    for index in catalog.get_schema_indexes(schema_name) {
        if is_without_rowid_pk_autoindex(catalog, index) {
            continue;
        }
        let row = if is_sqlite_autoindex(&index.name) {
            schema_row_autoindex(&index.name, &index.table_name)
        } else {
            let sql = generate_create_index_sql(index);
            schema_row("index", &index.name, &index.table_name, sql)
        };
        collector.push(&index.schema, &index.name, row);
    }

    // Add views tagged to this schema. Temp views are tagged with the `temp`
    // schema (#5541) and surface only via sqlite_temp_master, so skip them
    // here; the main schema is the default (`schema == None`). Iterate view
    // definitions directly rather than via `list_views()` + `get_view()`:
    // views are keyed per schema (#6490), so a name-only `get_view` resolves
    // temp-first-then-main-then-attached and would drop (or duplicate) a
    // `main` view that shares a name with an `aux`/`temp` view (view analogue
    // of the trigger fix in issue #6296).
    for view in catalog.iter_views() {
        if view.is_temp() {
            continue;
        }
        if !schema_tag_belongs_to(view.schema.as_deref(), schema_name) {
            continue;
        }
        let sql = generate_create_view_sql(view);
        // tbl_name is same as name for views
        collector.push(schema_name, &view.name, schema_row("view", &view.name, &view.name, sql));
    }

    // Add triggers tagged to this schema. Temp triggers are tagged with the
    // `temp` schema (#5532) and surface only via sqlite_temp_master, so skip
    // them here. Iterate trigger definitions directly rather than via
    // `list_triggers()` + `get_trigger()`: triggers are keyed per schema, so a
    // name-only `get_trigger` resolves temp-first and would drop a `main`
    // trigger that shares a name with a `temp` trigger (issue #6296).
    for trigger in catalog.iter_triggers() {
        if trigger.is_temp() {
            continue;
        }
        if !schema_tag_belongs_to(trigger.schema.as_deref(), schema_name) {
            continue;
        }
        let sql = generate_create_trigger_sql(trigger);
        collector.push(
            schema_name,
            &trigger.name,
            schema_row("trigger", &trigger.name, &trigger.table_name, sql),
        );
    }

    Ok(SelectResult { columns: column_names, rows: collector.into_ordered_rows() })
}

/// Collects `sqlite_master` rows tagged with each object's creation ordinal and
/// emits them in SQLite's creation order.
///
/// SQLite returns `sqlite_master` rows in the order their objects were created
/// (the schema table's rowid order). VibeSQL stores tables and indexes in
/// separate collections, so the catalog records a global creation ordinal per
/// object ([`Catalog::creation_seq`](vibesql_catalog::Catalog::creation_seq))
/// that this collector uses to re-interleave them.
///
/// Objects without a recorded ordinal (e.g. after a reload, when objects are
/// re-registered in load order, or a renamed object) fall back to the order in
/// which they were pushed here — which is the historical "tables, then indexes,
/// then views, then triggers" emission order. The fallback keys sort *after*
/// every recorded ordinal, so a reload reproduces the previous ordering exactly
/// and never regresses.
struct SchemaRowCollector<'a> {
    catalog: &'a vibesql_catalog::Catalog,
    rows: Vec<(u64, Row)>,
    pushed: u64,
}

impl<'a> SchemaRowCollector<'a> {
    /// Fallback ordinals start here so they always sort after recorded ordinals.
    const FALLBACK_BASE: u64 = 1 << 62;

    fn new(catalog: &'a vibesql_catalog::Catalog) -> Self {
        SchemaRowCollector { catalog, rows: Vec::new(), pushed: 0 }
    }

    fn push(&mut self, schema: &str, name: &str, row: Row) {
        let seq =
            self.catalog.creation_seq(schema, name).unwrap_or(Self::FALLBACK_BASE + self.pushed);
        self.pushed += 1;
        self.rows.push((seq, row));
    }

    fn into_ordered_rows(mut self) -> Vec<Row> {
        // Stable sort keeps the push order among equal keys (only possible in the
        // fallback range, which already encodes push order).
        self.rows.sort_by_key(|(seq, _)| *seq);
        self.rows.into_iter().map(|(_, row)| row).collect()
    }
}

/// SQLite WHERE-clause truthiness for schema-row selection: booleans as-is,
/// non-zero numerics true, everything else (including NULL) false.
fn schema_where_is_truthy(value: &SqlValue) -> bool {
    match value {
        SqlValue::Boolean(b) => *b,
        SqlValue::Integer(n) => *n != 0,
        SqlValue::Smallint(n) => *n != 0,
        SqlValue::Bigint(n) => *n != 0,
        SqlValue::Unsigned(n) => *n != 0,
        SqlValue::Float(f) => *f != 0.0,
        SqlValue::Real(f) => *f != 0.0,
        SqlValue::Double(f) => *f != 0.0,
        SqlValue::Numeric(f) => *f != 0.0,
        _ => false,
    }
}

/// Execute an `UPDATE sqlite_master` / `UPDATE sqlite_schema` statement under
/// `PRAGMA writable_schema=ON`.
///
/// SQLite's writable_schema lets a connection rewrite `sqlite_master.sql`
/// directly; the modified text becomes the object's schema on the next reload.
/// VibeSQL implements the minimal subset the conformance suite exercises
/// (alterdropcol 8.x, issue #5796):
///
/// - Only assignments to the `sql` column are supported, and the assigned value must evaluate to a
///   string.
/// - Only `table` rows are rewritten: the new text replaces the table's verbatim `sql_source`,
///   which is what `SELECT sql FROM sqlite_schema` echoes and what ALTER TABLE's textual schema
///   rewrites operate on. The structured (parsed) schema is NOT re-derived from the new text — like
///   SQLite, writable_schema trades integrity checking for direct access, so callers can make the
///   stored text diverge from the live schema.
/// - Matching `index`/`view`/`trigger` rows are left untouched (their `sql` is reconstructed from
///   catalog metadata, not stored verbatim).
///
/// The row count returned is the number of schema rows matched by the WHERE
/// clause, mirroring SQLite's changes() semantics.
pub fn execute_sqlite_schema_update(
    stmt: &vibesql_ast::UpdateStmt,
    database: &mut vibesql_storage::Database,
) -> Result<usize, ExecutorError> {
    if stmt.from_clause.is_some() {
        return Err(ExecutorError::UnsupportedFeature(
            "UPDATE ... FROM is not supported on sqlite_master".to_string(),
        ));
    }

    // Only `SET sql = <expr>` is supported in the writable_schema subset.
    for assignment in &stmt.assignments {
        if !assignment.column.eq_ignore_ascii_case("sql") {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "writable_schema UPDATE supports only the sql column, not '{}'",
                assignment.column
            )));
        }
    }

    let schema = get_sqlite_schema_table_schema();
    let evaluator = crate::evaluator::ExpressionEvaluator::new(&schema);
    let current = execute_sqlite_schema_query(&database.catalog)?;

    // Collect (table_name, new_sql) updates first, then apply: applying while
    // iterating would alias the catalog borrow held by the row snapshot.
    let mut matched = 0usize;
    let mut table_updates: Vec<(String, String)> = Vec::new();
    for row in &current.rows {
        if let Some(where_clause) = &stmt.where_clause {
            let matches = match where_clause {
                vibesql_ast::WhereClause::Condition(expr) => {
                    schema_where_is_truthy(&evaluator.eval(expr, row)?)
                }
                vibesql_ast::WhereClause::CurrentOf(_) => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "WHERE CURRENT OF is not supported on sqlite_master".to_string(),
                    ));
                }
            };
            if !matches {
                continue;
            }
        }
        matched += 1;

        let (SqlValue::Varchar(obj_type), SqlValue::Varchar(name)) =
            (&row.values[0], &row.values[1])
        else {
            continue;
        };
        if obj_type.as_str() != "table" {
            // Index/view/trigger sql is reconstructed from catalog metadata;
            // rewriting it verbatim is outside the supported subset.
            continue;
        }

        for assignment in &stmt.assignments {
            let value = evaluator.eval(&assignment.value, row)?;
            let new_sql = match value {
                SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_string(),
                other => {
                    return Err(ExecutorError::UnsupportedFeature(format!(
                        "writable_schema UPDATE requires a string value for sql, got {:?}",
                        other
                    )));
                }
            };
            table_updates.push((name.to_string(), new_sql));
        }
    }

    for (table_name, new_sql) in table_updates {
        // Update the storage-side schema, then mirror it into the catalog —
        // the two hold separate copies that must stay identical (same pattern
        // as ALTER TABLE's sync_catalog_schema_from_storage, issue #5625).
        let synced = match database.get_table_mut(&table_name) {
            Some(table) => {
                table.schema_mut().set_sql_source(new_sql);
                Some(table.schema.clone())
            }
            None => None,
        };
        if let Some(schema) = synced {
            database.catalog.replace_table_schema(&table_name, schema);
        }
    }

    Ok(matched)
}

/// Execute a `sqlite_temp_master` / `sqlite_temp_schema` query.
///
/// Lists the session's **temp-schema** objects: temp tables, indexes on temp
/// tables, temp views, and temp triggers. Mirrors SQLite 3.51.0, where:
///   CREATE TEMP TABLE t(a); CREATE INDEX i ON t(a);
///   CREATE TEMP VIEW v AS SELECT 1; CREATE TEMP TRIGGER tr AFTER INSERT ON t ...;
///   SELECT name,type FROM sqlite_temp_master;  -- lists t, i, v and tr
/// while `sqlite_master` lists none of them. Temp tables/indexes were added in
/// #5513 (PR #5539); temp views/triggers in #5541.
pub fn execute_sqlite_temp_schema_query(
    catalog: &vibesql_catalog::Catalog,
) -> Result<SelectResult, ExecutorError> {
    let schema = get_sqlite_schema_table_schema();
    let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let mut rows = Vec::new();

    let temp_schema = catalog.temp_schema_name();

    // Add temp tables (objects living in the session temp schema).
    for table_name in catalog.list_tables_in_schema(temp_schema) {
        // Look up via the temp-qualified name so we read the temp copy even when
        // a same-named main table shadows it for unqualified lookups.
        let qualified = format!("{}.{}", temp_schema, table_name);
        if let Some(table) = catalog.get_table(&qualified) {
            let sql = generate_create_table_sql(table);
            // Echo the original declared case (issue #5553); `table_name` is the
            // lowercase catalog key.
            rows.push(schema_row("table", &table.name, &table.name, sql));
        }
    }

    // Add indexes owned by the temp schema.
    for index in catalog.get_schema_indexes(temp_schema) {
        if is_without_rowid_pk_autoindex(catalog, index) {
            continue;
        }
        if is_sqlite_autoindex(&index.name) {
            rows.push(schema_row_autoindex(&index.name, &index.table_name));
        } else {
            let sql = generate_create_index_sql(index);
            rows.push(schema_row("index", &index.name, &index.table_name, sql));
        }
    }

    // Add temp views. Filter on the per-view `temp` tag set at CREATE TEMP
    // VIEW time (#5541). Iterate view definitions directly rather than via
    // `list_views()` + `get_view()`: views are keyed per schema (#6490), and a
    // name-only `get_view` resolves temp-first, so a `temp` view sharing a
    // name with a `main`/attached-schema view would be dropped or duplicated
    // (view analogue of the trigger fix in issue #6296).
    for view in catalog.iter_views() {
        if !view.is_temp() {
            continue;
        }
        let sql = generate_create_view_sql(view);
        rows.push(schema_row("view", &view.name, &view.name, sql));
    }

    // Add temp triggers. Filter on the `temp` schema tag from CREATE TEMP
    // TRIGGER (#5532). Iterate trigger definitions directly rather than via
    // `list_triggers()` + `get_trigger()`: triggers are keyed per schema, and a
    // name-only `get_trigger` resolves temp-first, so a `temp` trigger sharing a
    // name with a `main` trigger would be emitted twice (issue #6296).
    for trigger in catalog.iter_triggers() {
        if !trigger.is_temp() {
            continue;
        }
        let sql = generate_create_trigger_sql(trigger);
        rows.push(schema_row("trigger", &trigger.name, &trigger.table_name, sql));
    }

    Ok(SelectResult { columns: column_names, rows })
}

/// Generate CREATE TABLE SQL statement for a table
fn generate_create_table_sql(table: &TableSchema) -> String {
    // SQLite stores the original CREATE TABLE statement byte-verbatim in
    // sqlite_master.sql (whitespace and formatting preserved). When we captured
    // the original source text at CREATE time (issue #5619), return it as-is
    // rather than reconstructing a normalized form from the parsed schema. We
    // only reconstruct when the verbatim text is unavailable (e.g. schemas built
    // programmatically, or after ALTER TABLE invalidated the stale source).
    if let Some(ref src) = table.sql_source {
        return src.clone();
    }

    let mut sql = format!("CREATE TABLE {} (\n", table.name);
    let mut definitions: Vec<String> = Vec::new();

    // Add column definitions
    for col in &table.columns {
        let mut col_def = format!("  {} {}", col.name, format_data_type(&col.data_type));

        // Handle generated columns (AS expression syntax)
        if let Some(ref generated_expr) = col.generated_expr {
            col_def.push_str(&format!(" AS ({})", format_expression(generated_expr)));
        } else {
            // Only non-generated columns can have NOT NULL and DEFAULT
            if !col.nullable {
                col_def.push_str(" NOT NULL");
            }

            if let Some(ref default) = col.default_value {
                col_def.push_str(&format!(" DEFAULT {}", format_expression(default)));
            }
        }

        definitions.push(col_def);
    }

    // Add PRIMARY KEY constraint
    if let Some(ref pk_cols) = table.primary_key {
        definitions.push(format!("  PRIMARY KEY ({})", pk_cols.join(", ")));
    }

    // Add UNIQUE constraints
    for unique_cols in &table.unique_constraints {
        definitions.push(format!("  UNIQUE ({})", unique_cols.join(", ")));
    }

    // Add FOREIGN KEY constraints
    for fk in &table.foreign_keys {
        let mut fk_def = format!(
            "  FOREIGN KEY ({}) REFERENCES {} ({})",
            fk.column_names.join(", "),
            fk.parent_table,
            fk.parent_column_names.join(", ")
        );

        // Add ON DELETE action if not NO ACTION
        if fk.on_delete != ReferentialAction::NoAction {
            fk_def.push_str(&format!(" ON DELETE {}", format_referential_action(&fk.on_delete)));
        }

        // Add ON UPDATE action if not NO ACTION
        if fk.on_update != ReferentialAction::NoAction {
            fk_def.push_str(&format!(" ON UPDATE {}", format_referential_action(&fk.on_update)));
        }

        // Emit DEFERRABLE clause when non-default. Matches SQLite's
        // sqlite_master format and the `introspection.rs` SHOW CREATE
        // TABLE wiring landed in Phase C3 of #5085.
        if fk.is_deferrable {
            if fk.initially_deferred {
                fk_def.push_str(" DEFERRABLE INITIALLY DEFERRED");
            } else {
                fk_def.push_str(" DEFERRABLE INITIALLY IMMEDIATE");
            }
        }

        definitions.push(fk_def);
    }

    // Add CHECK constraints
    for (name, expr) in &table.check_constraints {
        definitions.push(format!("  CONSTRAINT {} CHECK ({})", name, format_expression(expr)));
    }

    sql.push_str(&definitions.join(",\n"));
    sql.push_str("\n)");

    sql
}

/// Generate CREATE INDEX SQL statement for an index
fn generate_create_index_sql(index: &vibesql_catalog::IndexMetadata) -> String {
    let unique_str = if index.is_unique { "UNIQUE " } else { "" };

    let columns: Vec<String> = index
        .columns
        .iter()
        .map(|col| {
            let order_str = match col.order() {
                SortOrder::Ascending => "",
                SortOrder::Descending => " DESC",
            };
            match col {
                vibesql_catalog::IndexedColumn::Column { column_name, prefix_length, .. } => {
                    let prefix_str = prefix_length.map(|l| format!("({})", l)).unwrap_or_default();
                    format!("{}{}{}", column_name, prefix_str, order_str)
                }
                vibesql_catalog::IndexedColumn::Expression { expr, .. } => {
                    // SQLite echoes an expression-index column exactly as the
                    // user wrote it (verified against sqlite3 3.51.0): a bare
                    // compound expression like `t1(b+b+b+b, c)` round-trips
                    // WITHOUT an added outer paren layer (altercol-1.12.4),
                    // while an already-function-call expression like
                    // `t1(lower(a))` carries its own parens and needs no
                    // extra wrap either. VibeSQL does not retain the original
                    // source text for indexes (unlike triggers/views, which
                    // store verbatim `sql_definition`), so a bare column
                    // reference is the one shape that DOES need a synthetic
                    // wrap: `IndexedColumn::Expression` only ever holds a
                    // lone `ColumnRef` when the user wrote explicit
                    // disambiguating parens around a plain column (`t1((a))`,
                    // altercorrupt-style tests) — rendering it unwrapped would
                    // round-trip back through the parser as a plain
                    // `IndexedColumn::Column` instead, silently changing the
                    // index's on-disk metadata shape. Every other expression
                    // shape (binary/unary ops, function calls, CASE, etc.) is
                    // rendered as-is with no synthetic wrap.
                    let needs_wrap = matches!(expr.as_ref(), vibesql_ast::Expression::ColumnRef(_));
                    if needs_wrap {
                        format!("({}){}", expr.to_sql(), order_str)
                    } else {
                        format!("{}{}", expr.to_sql(), order_str)
                    }
                }
            }
        })
        .collect();

    // Partial-index WHERE clause. SQLite renders the (rewritten) predicate after
    // the column list, e.g. `CREATE INDEX i ON t(a) WHERE a>0`. The predicate is
    // kept in sync with RENAME COLUMN via `rename_column_in_table_indexes`, so it
    // already names the current column; render it verbatim from the AST. Without
    // this the emitted `sqlite_master.sql` silently dropped the WHERE clause
    // (altercol 1.12.4).
    let where_str = index
        .where_clause
        .as_ref()
        .map(|expr| format!(" WHERE {}", expr.to_sql()))
        .unwrap_or_default();

    format!(
        "CREATE {}INDEX {} ON {}({}){}",
        unique_str,
        index.name,
        index.table_name,
        columns.join(", "),
        where_str
    )
}

/// Generate CREATE VIEW SQL statement for a view
fn generate_create_view_sql(view: &vibesql_catalog::ViewDefinition) -> String {
    // Use stored SQL definition if available. SQLite stores the statement text
    // without a trailing `;` in `sqlite_master.sql`, so strip one if the captured
    // verbatim text carried it (mirrors `generate_create_trigger_sql`; verified
    // against sqlite3 3.51.0, altercol.test group 8).
    if let Some(ref sql) = view.sql_definition {
        let trimmed = sql.trim_end();
        let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed);
        return trimmed.trim_end().to_string();
    }

    // Otherwise generate from the view definition
    let columns_str =
        view.columns.as_ref().map(|cols| format!(" ({})", cols.join(", "))).unwrap_or_default();

    // Use the ToSql trait to generate valid SQL from the AST
    use vibesql_ast::pretty_print::ToSql;
    format!("CREATE VIEW {}{} AS {}", view.name, columns_str, view.query.to_sql())
}

/// Generate CREATE TRIGGER SQL statement for a trigger
fn generate_create_trigger_sql(trigger: &vibesql_catalog::TriggerDefinition) -> String {
    use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};

    // Prefer the original SQL text when available so `sqlite_master.sql` matches
    // SQLite's verbatim form (no injected BEFORE/FOR EACH ROW, original spacing
    // and quoting preserved). This mirrors `generate_create_view_sql` and is the
    // form kept consistent by `crate::trigger_rename` across ALTER TABLE RENAME.
    // SQLite stores the statement text without the trailing `;`, so strip one if
    // present.
    if let Some(ref sql) = trigger.sql_definition {
        let trimmed = sql.trim_end();
        let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed);
        return trimmed.trim_end().to_string();
    }

    let timing = match trigger.timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    };

    let granularity = match trigger.granularity {
        TriggerGranularity::Row => "ROW",
        TriggerGranularity::Statement => "STATEMENT",
    };

    let event = match &trigger.event {
        TriggerEvent::Insert => "INSERT".to_string(),
        TriggerEvent::Update(None) => "UPDATE".to_string(),
        TriggerEvent::Update(Some(cols)) => format!("UPDATE OF {}", cols.join(", ")),
        TriggerEvent::Delete => "DELETE".to_string(),
    };

    // Include WHEN clause if present
    let when_clause = trigger
        .when_condition
        .as_ref()
        .map(|expr| format!(" WHEN ({})", format_expression(expr)))
        .unwrap_or_default();

    // Include trigger body from TriggerAction
    let body = match &trigger.triggered_action {
        TriggerAction::RawSql(sql) => sql.clone(),
    };

    format!(
        "CREATE TRIGGER {} {} {} ON {} FOR EACH {}{}{}",
        trigger.name, timing, event, trigger.table_name, granularity, when_clause, body
    )
}

/// Format a DataType as a SQL string
fn format_data_type(dt: &DataType) -> String {
    match dt {
        DataType::Integer => "INTEGER".to_string(),
        DataType::Smallint => "SMALLINT".to_string(),
        DataType::Bigint => "BIGINT".to_string(),
        DataType::Unsigned => "UNSIGNED BIGINT".to_string(),
        DataType::Real => "REAL".to_string(),
        DataType::Float { precision } => format!("FLOAT({})", precision),
        DataType::DoublePrecision => "DOUBLE PRECISION".to_string(),
        DataType::Numeric { precision, scale } => format!("NUMERIC({}, {})", precision, scale),
        DataType::Decimal { precision, scale } => format!("DECIMAL({}, {})", precision, scale),
        DataType::Varchar { max_length } => {
            if let Some(len) = max_length {
                format!("VARCHAR({})", len)
            } else {
                "TEXT".to_string()
            }
        }
        DataType::Character { length } => format!("CHAR({})", length),
        DataType::CharacterLargeObject => "TEXT".to_string(),
        DataType::Name => "TEXT".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::Time { with_timezone } => {
            if *with_timezone {
                "TIME WITH TIME ZONE".to_string()
            } else {
                "TIME".to_string()
            }
        }
        DataType::Timestamp { with_timezone } => {
            if *with_timezone {
                "TIMESTAMP WITH TIME ZONE".to_string()
            } else {
                "TIMESTAMP".to_string()
            }
        }
        DataType::Interval { start_field, end_field } => {
            if let Some(end) = end_field {
                format!("INTERVAL {:?} TO {:?}", start_field, end)
            } else {
                format!("INTERVAL {:?}", start_field)
            }
        }
        DataType::BinaryLargeObject => "BLOB".to_string(),
        DataType::Bit { length } => {
            if let Some(len) = length {
                format!("BIT({})", len)
            } else {
                "BIT".to_string()
            }
        }
        DataType::UserDefined { type_name } => type_name.clone(),
        DataType::Vector { dimensions } => format!("VECTOR({})", dimensions),
        DataType::Null => "NULL".to_string(),
    }
}

/// Format a referential action as a SQL string
fn format_referential_action(action: &ReferentialAction) -> &'static str {
    match action {
        ReferentialAction::NoAction => "NO ACTION",
        ReferentialAction::Restrict => "RESTRICT",
        ReferentialAction::Cascade => "CASCADE",
        ReferentialAction::SetNull => "SET NULL",
        ReferentialAction::SetDefault => "SET DEFAULT",
    }
}

/// Format an expression for SQL output
fn format_expression(expr: &vibesql_ast::Expression) -> String {
    use vibesql_ast::pretty_print::ToSql;
    expr.to_sql()
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{Catalog, IndexMetadata, IndexType, IndexedColumn};

    use super::*;

    #[test]
    fn test_is_sqlite_schema_table() {
        assert!(is_sqlite_schema_table("sqlite_master"));
        assert!(is_sqlite_schema_table("sqlite_schema"));
        assert!(is_sqlite_schema_table("SQLITE_MASTER"));
        assert!(is_sqlite_schema_table("SQLITE_SCHEMA"));
        assert!(is_sqlite_schema_table("Sqlite_Master"));
        // #6176: main-qualified forms name the same schema table.
        assert!(is_sqlite_schema_table("main.sqlite_master"));
        assert!(is_sqlite_schema_table("main.sqlite_schema"));
        assert!(is_sqlite_schema_table("MAIN.SQLITE_MASTER"));
        assert!(!is_sqlite_schema_table("users"));
        assert!(!is_sqlite_schema_table("sqlite_stat1"));
        assert!(!is_sqlite_schema_table("information_schema.tables"));
        // sqlite_temp_master is a distinct view, not sqlite_master.
        assert!(!is_sqlite_schema_table("sqlite_temp_master"));
        // temp-qualified master belongs to the temp schema, not main.
        assert!(!is_sqlite_schema_table("temp.sqlite_master"));
    }

    /// #6451: `<alias>.sqlite_master`/`<alias>.sqlite_schema` for a
    /// currently-attached database is recognized as the schema table, just
    /// like the bare/`main.`-qualified forms; a stale/unattached alias, or an
    /// unrelated qualifier like `temp`, is not.
    #[test]
    fn test_is_sqlite_schema_table_ref() {
        let mut catalog = Catalog::new();
        catalog.attach_database("aux", ":memory:").unwrap();

        // Bare/main-qualified forms behave exactly like `is_sqlite_schema_table`.
        assert!(is_sqlite_schema_table_ref(&catalog, "sqlite_master"));
        assert!(is_sqlite_schema_table_ref(&catalog, "SQLITE_SCHEMA"));
        assert!(is_sqlite_schema_table_ref(&catalog, "main.sqlite_master"));

        // A currently-attached alias's schema table is recognized too.
        assert!(is_sqlite_schema_table_ref(&catalog, "aux.sqlite_master"));
        assert!(is_sqlite_schema_table_ref(&catalog, "AUX.SQLITE_SCHEMA"));

        // An alias that was never attached must not be treated as a hit.
        assert!(!is_sqlite_schema_table_ref(&catalog, "nosuchdb.sqlite_master"));
        // Unrelated qualifiers/tables are unaffected.
        assert!(!is_sqlite_schema_table_ref(&catalog, "temp.sqlite_master"));
        assert!(!is_sqlite_schema_table_ref(&catalog, "users"));
        assert!(!is_sqlite_schema_table_ref(&catalog, "aux.users"));

        // After DETACH, the alias no longer resolves (clean "not found", not a
        // stale hit).
        catalog.detach_database("aux").unwrap();
        assert!(!is_sqlite_schema_table_ref(&catalog, "aux.sqlite_master"));
    }

    /// #6436: `<alias>.sqlite_master`/`<alias>.sqlite_schema` for a
    /// currently-attached database resolves to that alias's own schema; an
    /// unqualified/`main.`-qualified reference resolves to `main`; anything
    /// else (unattached alias, `sqlite_temp_master`, unrelated table) resolves
    /// to `None` so callers fall through to ordinary "table not found".
    #[test]
    fn test_resolve_sqlite_schema_query_scope() {
        let mut catalog = Catalog::new();
        catalog.attach_database("aux", ":memory:").unwrap();

        assert_eq!(
            resolve_sqlite_schema_query_scope(&catalog, "sqlite_master"),
            Some("main".to_string())
        );
        assert_eq!(
            resolve_sqlite_schema_query_scope(&catalog, "SQLITE_SCHEMA"),
            Some("main".to_string())
        );
        assert_eq!(
            resolve_sqlite_schema_query_scope(&catalog, "main.sqlite_master"),
            Some("main".to_string())
        );
        assert_eq!(
            resolve_sqlite_schema_query_scope(&catalog, "aux.sqlite_master"),
            Some("aux".to_string())
        );
        assert_eq!(
            resolve_sqlite_schema_query_scope(&catalog, "AUX.SQLITE_SCHEMA"),
            Some("aux".to_string())
        );
        // An alias that was never attached must not be treated as a schema hit.
        assert_eq!(resolve_sqlite_schema_query_scope(&catalog, "nosuchdb.sqlite_master"), None);
        // sqlite_temp_master has no attached-database counterpart (verified
        // against sqlite3 3.51.0: `no such table: aux.sqlite_temp_master`).
        assert_eq!(resolve_sqlite_schema_query_scope(&catalog, "aux.sqlite_temp_master"), None);
        assert_eq!(resolve_sqlite_schema_query_scope(&catalog, "users"), None);

        // After DETACH, the alias no longer resolves (clean "not found", not a
        // stale read of the detached schema).
        catalog.detach_database("aux").unwrap();
        assert_eq!(resolve_sqlite_schema_query_scope(&catalog, "aux.sqlite_master"), None);
    }

    /// #6436: `SELECT ... FROM <alias>.sqlite_master` against an attached
    /// database lists that attachment's own tables — not `main`'s, and
    /// without cross-contamination when a same-named table exists in both.
    #[test]
    fn test_execute_sqlite_schema_query_for_attached_schema() {
        let mut catalog = Catalog::new();

        // main.t (should NOT appear in the `aux` listing).
        let main_t = TableSchema::new(
            "t".to_string(),
            vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
        );
        catalog.create_table(main_t).unwrap();

        catalog.attach_database("aux", ":memory:").unwrap();

        // aux.t — same bare name as main.t, must not cross-contaminate.
        let aux_t = TableSchema::new(
            "t".to_string(),
            vec![ColumnSchema::new("x".to_string(), DataType::Integer, true)],
        );
        catalog.create_table_in_schema("aux", aux_t).unwrap();
        let aux_t2 = TableSchema::new(
            "t2".to_string(),
            vec![ColumnSchema::new("y".to_string(), DataType::Integer, true)],
        );
        catalog.create_table_in_schema("aux", aux_t2).unwrap();

        let aux_result = execute_sqlite_schema_query_for_schema(&catalog, "aux").unwrap();
        let aux_names: Vec<String> = aux_result
            .rows
            .iter()
            .map(|r| match &r.values[1] {
                SqlValue::Varchar(s) => s.to_string(),
                _ => String::new(),
            })
            .collect();
        assert_eq!(aux_names, vec!["t".to_string(), "t2".to_string()]);

        // main's own listing is unaffected and still lists only main.t.
        let main_result = execute_sqlite_schema_query(&catalog).unwrap();
        let main_names: Vec<String> = main_result
            .rows
            .iter()
            .map(|r| match &r.values[1] {
                SqlValue::Varchar(s) => s.to_string(),
                _ => String::new(),
            })
            .collect();
        assert_eq!(main_names, vec!["t".to_string()]);

        // The `aux.t` row's `sql` text is aux's own (columns x), not main's
        // t (columns a) — confirms the by-alias lookup reads the right copy.
        let aux_t_row = aux_result
            .rows
            .iter()
            .find(|r| matches!(&r.values[1], SqlValue::Varchar(s) if s.as_str() == "t"))
            .unwrap();
        if let SqlValue::Varchar(sql) = &aux_t_row.values[4] {
            assert!(sql.contains("x INTEGER"), "expected aux.t's own columns, got: {sql}");
            assert!(!sql.contains("a INTEGER"), "must not read main.t's columns, got: {sql}");
        } else {
            panic!("Expected VARCHAR for sql column");
        }
    }

    /// #6436: an attached database with no tables yields an empty result, not
    /// an error.
    #[test]
    fn test_execute_sqlite_schema_query_for_attached_schema_empty() {
        let mut catalog = Catalog::new();
        catalog.attach_database("aux", ":memory:").unwrap();
        let result = execute_sqlite_schema_query_for_schema(&catalog, "aux").unwrap();
        assert!(result.rows.is_empty());
        assert_eq!(result.columns, vec!["type", "name", "tbl_name", "rootpage", "sql"]);
    }

    #[test]
    fn test_is_sqlite_temp_schema_table() {
        assert!(is_sqlite_temp_schema_table("sqlite_temp_master"));
        assert!(is_sqlite_temp_schema_table("sqlite_temp_schema"));
        assert!(is_sqlite_temp_schema_table("SQLITE_TEMP_MASTER"));
        assert!(is_sqlite_temp_schema_table("Sqlite_Temp_Schema"));
        // #6176: temp-qualified master/schema name the temp schema table.
        assert!(is_sqlite_temp_schema_table("temp.sqlite_master"));
        assert!(is_sqlite_temp_schema_table("temp.sqlite_schema"));
        assert!(is_sqlite_temp_schema_table("TEMP.SQLITE_MASTER"));
        assert!(!is_sqlite_temp_schema_table("sqlite_master"));
        assert!(!is_sqlite_temp_schema_table("main.sqlite_master"));
        assert!(!is_sqlite_temp_schema_table("users"));
    }

    #[test]
    fn test_temp_index_separated_from_main_in_schema_views() {
        // #5513: a temp-schema index appears in sqlite_temp_master and is
        // excluded from sqlite_master; a main index is the reverse.
        let mut catalog = Catalog::new();

        // main.t with a main index.
        let main_table = TableSchema::new(
            "t".to_string(),
            vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
        );
        catalog.create_table(main_table).unwrap();
        catalog
            .add_index(IndexMetadata::new(
                "main_i".to_string(),
                "t".to_string(),
                IndexType::BTree,
                vec![IndexedColumn::new_column("a".to_string(), SortOrder::Ascending)],
                false,
            ))
            .unwrap();

        // temp.tt with a temp index.
        let temp_schema = catalog.temp_schema_name().to_string();
        let temp_table = TableSchema::new(
            "tt".to_string(),
            vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
        );
        catalog.create_table_in_schema(&temp_schema, temp_table).unwrap();
        catalog
            .add_index(
                IndexMetadata::new(
                    "temp_i".to_string(),
                    "tt".to_string(),
                    IndexType::BTree,
                    vec![IndexedColumn::new_column("a".to_string(), SortOrder::Ascending)],
                    false,
                )
                .with_schema(temp_schema.clone()),
            )
            .unwrap();

        let master = execute_sqlite_schema_query(&catalog).unwrap();
        let master_names: Vec<String> = master
            .rows
            .iter()
            .map(|r| match &r.values[1] {
                SqlValue::Varchar(s) => s.to_string(),
                _ => String::new(),
            })
            .collect();
        assert!(master_names.contains(&"main_i".to_string()), "master should list main index");
        assert!(!master_names.contains(&"temp_i".to_string()), "master must exclude temp index");

        let temp_master = execute_sqlite_temp_schema_query(&catalog).unwrap();
        let temp_names: Vec<String> = temp_master
            .rows
            .iter()
            .map(|r| match &r.values[1] {
                SqlValue::Varchar(s) => s.to_string(),
                _ => String::new(),
            })
            .collect();
        assert!(temp_names.contains(&"temp_i".to_string()), "temp_master should list temp index");
        assert!(temp_names.contains(&"tt".to_string()), "temp_master should list temp table");
        assert!(!temp_names.contains(&"main_i".to_string()), "temp_master must exclude main index");
    }

    #[test]
    fn test_temp_view_and_trigger_separated_from_main_in_schema_views() {
        // #5541: temp views/triggers appear in sqlite_temp_master and are
        // excluded from sqlite_master; main views/triggers are the reverse.
        use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
        use vibesql_catalog::{TriggerDefinition, ViewDefinition};

        let mut catalog = Catalog::new();

        // A base table for the triggers to target.
        let base = TableSchema::new(
            "t".to_string(),
            vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
        );
        catalog.create_table(base).unwrap();

        // main view + temp view.
        catalog
            .create_view(ViewDefinition::new(
                "main_v".to_string(),
                None,
                create_minimal_select_stmt(),
                false,
            ))
            .unwrap();
        catalog
            .create_view(
                ViewDefinition::new(
                    "temp_v".to_string(),
                    None,
                    create_minimal_select_stmt(),
                    false,
                )
                .with_schema(Some("temp".to_string())),
            )
            .unwrap();

        // main trigger + temp trigger.
        let mk_trigger = |name: &str| {
            TriggerDefinition::new(
                name.to_string(),
                TriggerTiming::After,
                TriggerEvent::Insert,
                "t".to_string(),
                TriggerGranularity::Row,
                None,
                TriggerAction::RawSql("SELECT 1".to_string()),
            )
        };
        catalog.create_trigger(mk_trigger("main_tr")).unwrap();
        catalog
            .create_trigger(mk_trigger("temp_tr").with_schema(Some("temp".to_string())))
            .unwrap();

        let names = |result: &SelectResult| -> Vec<String> {
            result
                .rows
                .iter()
                .map(|r| match &r.values[1] {
                    SqlValue::Varchar(s) => s.to_string(),
                    _ => String::new(),
                })
                .collect()
        };

        let master = execute_sqlite_schema_query(&catalog).unwrap();
        let master_names = names(&master);
        assert!(master_names.contains(&"main_v".to_string()), "master should list main view");
        assert!(master_names.contains(&"main_tr".to_string()), "master should list main trigger");
        assert!(!master_names.contains(&"temp_v".to_string()), "master must exclude temp view");
        assert!(!master_names.contains(&"temp_tr".to_string()), "master must exclude temp trigger");

        let temp_master = execute_sqlite_temp_schema_query(&catalog).unwrap();
        let temp_names = names(&temp_master);
        assert!(temp_names.contains(&"temp_v".to_string()), "temp_master should list temp view");
        assert!(
            temp_names.contains(&"temp_tr".to_string()),
            "temp_master should list temp trigger"
        );
        assert!(!temp_names.contains(&"main_v".to_string()), "temp_master must exclude main view");
        assert!(
            !temp_names.contains(&"main_tr".to_string()),
            "temp_master must exclude main trigger"
        );
    }

    #[test]
    fn test_same_name_main_and_temp_triggers_each_appear_once_in_schema_views() {
        // #6296 regression: a `main` trigger and a `temp` trigger sharing a name
        // must each surface exactly once in their own schema view. The old
        // `list_triggers()` + `get_trigger()` enumeration returned duplicate bare
        // names and resolved every one temp-first, so the main trigger vanished
        // from sqlite_master and the temp trigger was listed twice in
        // sqlite_temp_master. Iterating `iter_triggers()` keys each definition by
        // schema so both are distinguishable.
        use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
        use vibesql_catalog::TriggerDefinition;

        let mut catalog = Catalog::new();

        // Two base tables (one main, one conceptually the temp target); the
        // trigger definitions only need a table name to reference.
        let t = TableSchema::new(
            "t".to_string(),
            vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
        );
        catalog.create_table(t).unwrap();
        let t2 = TableSchema::new(
            "t2".to_string(),
            vec![ColumnSchema::new("b".to_string(), DataType::Integer, true)],
        );
        catalog.create_table(t2).unwrap();

        // A `main` trigger and a `temp` trigger that share the bare name `tr`.
        let mk_trigger = |name: &str, table: &str| {
            TriggerDefinition::new(
                name.to_string(),
                TriggerTiming::After,
                TriggerEvent::Insert,
                table.to_string(),
                TriggerGranularity::Row,
                None,
                TriggerAction::RawSql("SELECT 1".to_string()),
            )
        };
        catalog.create_trigger(mk_trigger("tr", "t")).unwrap();
        catalog
            .create_trigger(mk_trigger("tr", "t2").with_schema(Some("temp".to_string())))
            .unwrap();

        let trigger_names = |result: &SelectResult| -> Vec<String> {
            result
                .rows
                .iter()
                .filter(|r| matches!(&r.values[0], SqlValue::Varchar(s) if s == "trigger"))
                .map(|r| match &r.values[1] {
                    SqlValue::Varchar(s) => s.to_string(),
                    _ => String::new(),
                })
                .collect()
        };

        // sqlite_master: the main `tr` appears exactly once; no temp namesake.
        let master = execute_sqlite_schema_query(&catalog).unwrap();
        let master_triggers = trigger_names(&master);
        assert_eq!(
            master_triggers,
            vec!["tr".to_string()],
            "sqlite_master must list the main trigger `tr` exactly once (not 0 rows)"
        );

        // sqlite_temp_master: the temp `tr` appears exactly once (not twice).
        let temp_master = execute_sqlite_temp_schema_query(&catalog).unwrap();
        let temp_triggers = trigger_names(&temp_master);
        assert_eq!(
            temp_triggers,
            vec!["tr".to_string()],
            "sqlite_temp_master must list the temp trigger `tr` exactly once (not twice)"
        );
    }

    #[test]
    fn test_sqlite_schema_empty_db() {
        let catalog = Catalog::new();
        let result = execute_sqlite_schema_query(&catalog).unwrap();

        assert_eq!(result.columns, vec!["type", "name", "tbl_name", "rootpage", "sql"]);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_get_sqlite_schema_table_schema() {
        let schema = get_sqlite_schema_table_schema();

        assert_eq!(schema.name, "sqlite_master");
        assert_eq!(schema.columns.len(), 5);
        assert_eq!(schema.columns[0].name, "type");
        assert_eq!(schema.columns[1].name, "name");
        assert_eq!(schema.columns[2].name, "tbl_name");
        assert_eq!(schema.columns[3].name, "rootpage");
        assert_eq!(schema.columns[4].name, "sql");
    }

    #[test]
    fn test_format_data_type() {
        assert_eq!(format_data_type(&DataType::Integer), "INTEGER");
        assert_eq!(format_data_type(&DataType::Varchar { max_length: Some(255) }), "VARCHAR(255)");
        assert_eq!(format_data_type(&DataType::Varchar { max_length: None }), "TEXT");
        assert_eq!(format_data_type(&DataType::Boolean), "BOOLEAN");
        assert_eq!(
            format_data_type(&DataType::Numeric { precision: 10, scale: 2 }),
            "NUMERIC(10, 2)"
        );
    }

    #[test]
    fn test_sqlite_schema_with_table() {
        let mut catalog = Catalog::new();

        // Create a simple table
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];
        let table =
            TableSchema::with_primary_key("users".to_string(), columns, vec!["id".to_string()]);
        catalog.create_table(table).unwrap();

        let result = execute_sqlite_schema_query(&catalog).unwrap();

        // Should have one row for the table
        assert_eq!(result.rows.len(), 1);

        // Check the type column
        let type_val = &result.rows[0].values[0];
        assert_eq!(type_val, &SqlValue::Varchar(arcstr::ArcStr::from("table")));

        // Check the name column
        let name_val = &result.rows[0].values[1];
        assert_eq!(name_val, &SqlValue::Varchar(arcstr::ArcStr::from("users")));

        // Check the tbl_name column (same as name for tables)
        let tbl_name_val = &result.rows[0].values[2];
        assert_eq!(tbl_name_val, &SqlValue::Varchar(arcstr::ArcStr::from("users")));

        // Check the rootpage column (always 0)
        let rootpage_val = &result.rows[0].values[3];
        assert_eq!(rootpage_val, &SqlValue::Integer(0));

        // Check the sql column contains CREATE TABLE
        let sql_val = &result.rows[0].values[4];
        if let SqlValue::Varchar(sql) = sql_val {
            assert!(sql.contains("CREATE TABLE users"));
            assert!(sql.contains("id INTEGER"));
            assert!(sql.contains("name VARCHAR(100)"));
            assert!(sql.contains("PRIMARY KEY"));
        } else {
            panic!("Expected VARCHAR for sql column");
        }
    }

    #[test]
    fn test_sqlite_schema_with_index() {
        let mut catalog = Catalog::new();

        // Create a table first
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(255) },
                false,
            ),
        ];
        let table =
            TableSchema::with_primary_key("users".to_string(), columns, vec!["id".to_string()]);
        catalog.create_table(table).unwrap();

        // Create an index
        let index = IndexMetadata::new(
            "idx_email".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("email".to_string(), SortOrder::Ascending)],
            true, // unique
        );
        catalog.add_index(index).unwrap();

        let result = execute_sqlite_schema_query(&catalog).unwrap();

        // Should have two rows: one for table, one for index
        assert_eq!(result.rows.len(), 2);

        // Find the index row
        let index_row = result
            .rows
            .iter()
            .find(|r| matches!(&r.values[0], SqlValue::Varchar(s) if s.as_str() == "index"))
            .expect("Should have an index row");

        // Check index name
        assert_eq!(index_row.values[1], SqlValue::Varchar(arcstr::ArcStr::from("idx_email")));

        // Check tbl_name for index
        assert_eq!(index_row.values[2], SqlValue::Varchar(arcstr::ArcStr::from("users")));

        // Check sql contains CREATE INDEX
        if let SqlValue::Varchar(sql) = &index_row.values[4] {
            assert!(sql.contains("CREATE UNIQUE INDEX idx_email ON users"));
        } else {
            panic!("Expected VARCHAR for sql column");
        }
    }

    #[test]
    fn test_generate_create_index_sql() {
        let index = IndexMetadata::new(
            "idx_name".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![
                IndexedColumn::new_column("last_name".to_string(), SortOrder::Ascending),
                IndexedColumn::Column {
                    column_name: "first_name".to_string(),
                    order: SortOrder::Descending,
                    prefix_length: None,
                    collation: None,
                    is_quoted: false,
                },
            ],
            false, // not unique
        );

        let sql = generate_create_index_sql(&index);
        assert_eq!(sql, "CREATE INDEX idx_name ON users(last_name, first_name DESC)");
    }

    #[test]
    fn test_generate_create_index_sql_with_prefix() {
        let index = IndexMetadata::new(
            "idx_email".to_string(),
            "users".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column_with_prefix(
                "email".to_string(),
                SortOrder::Ascending,
                50,
            )],
            true, // unique
        );

        let sql = generate_create_index_sql(&index);
        assert_eq!(sql, "CREATE UNIQUE INDEX idx_email ON users(email(50))");
    }

    /// Regression test for altercol-1.12.4: a compound-expression index
    /// column (e.g. `t1(b+b+b+b, c)`) must round-trip through
    /// `sqlite_master.sql` WITHOUT gaining a synthetic outer paren layer —
    /// verified against sqlite3 3.51.0, which echoes exactly what the user
    /// wrote (`CREATE INDEX t1i ON t1(b+b+b+b, c) WHERE b>0`, no `((...))`).
    #[test]
    fn test_generate_create_index_sql_compound_expression_no_extra_parens() {
        use vibesql_ast::{BinaryOperator, ColumnIdentifier, Expression};

        let col_ref = |name: &str| Expression::ColumnRef(ColumnIdentifier::simple(name, false));
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Plus,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Plus,
                left: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::Plus,
                    left: Box::new(col_ref("b")),
                    right: Box::new(col_ref("b")),
                }),
                right: Box::new(col_ref("b")),
            }),
            right: Box::new(col_ref("b")),
        };

        let index = IndexMetadata::new(
            "t1i".to_string(),
            "t1".to_string(),
            IndexType::BTree,
            vec![
                IndexedColumn::Expression { expr: Box::new(expr), order: SortOrder::Ascending },
                IndexedColumn::new_column("c".to_string(), SortOrder::Ascending),
            ],
            false,
        );

        let sql = generate_create_index_sql(&index);
        assert_eq!(sql, "CREATE INDEX t1i ON t1(b+b+b+b, c)");
    }

    /// A lone parenthesized column reference (`t1((a))`) DOES need the
    /// synthetic wrap: without it, re-parsing the generated SQL would produce
    /// a plain `IndexedColumn::Column` instead of an `Expression`, silently
    /// changing the index's on-disk metadata shape.
    #[test]
    fn test_generate_create_index_sql_bare_column_ref_keeps_wrap() {
        use vibesql_ast::{ColumnIdentifier, Expression};

        let expr = Expression::ColumnRef(ColumnIdentifier::simple("a", false));
        let index = IndexMetadata::new(
            "i3".to_string(),
            "t1".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::Expression { expr: Box::new(expr), order: SortOrder::Ascending }],
            false,
        );

        let sql = generate_create_index_sql(&index);
        assert_eq!(sql, "CREATE INDEX i3 ON t1((a))");
    }

    /// Regression test for #5579: `sqlite_master` must report the index name and
    /// SQL with their original case (the case the user typed in `CREATE INDEX`),
    /// matching sqlite3 3.51.0. The catalog stores the original-case name, so
    /// both the `name` column and the generated `sql` column preserve it.
    #[test]
    fn test_sqlite_schema_index_preserves_name_case() {
        let mut catalog = Catalog::new();
        catalog.set_case_sensitive_identifiers(false);

        let columns = vec![ColumnSchema::new("a".to_string(), DataType::Integer, false)];
        let table = TableSchema::new("t".to_string(), columns);
        catalog.create_table(table).unwrap();

        let index = IndexMetadata::new(
            "MyIdx".to_string(),
            "t".to_string(),
            IndexType::BTree,
            vec![IndexedColumn::new_column("a".to_string(), SortOrder::Ascending)],
            false,
        );
        catalog.add_index(index).unwrap();

        let result = execute_sqlite_schema_query(&catalog).unwrap();
        let index_row = result
            .rows
            .iter()
            .find(|r| matches!(&r.values[0], SqlValue::Varchar(s) if s.as_str() == "index"))
            .expect("Should have an index row");

        // `name` column preserves original case (not "myidx").
        assert_eq!(index_row.values[1], SqlValue::Varchar(arcstr::ArcStr::from("MyIdx")));

        // `sql` column preserves original case.
        if let SqlValue::Varchar(sql) = &index_row.values[4] {
            assert!(
                sql.contains("CREATE INDEX MyIdx ON t"),
                "sqlite_master.sql must preserve original index-name case, got: {sql}"
            );
            assert!(!sql.contains("myidx"), "index name must not be lowercased, got: {sql}");
        } else {
            panic!("Expected VARCHAR for sql column");
        }
    }

    #[test]
    fn test_generate_create_trigger_sql_basic() {
        use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
        use vibesql_catalog::TriggerDefinition;

        let trigger = TriggerDefinition {
            name: "audit_insert".to_string(),
            schema: None,
            table_name: "users".to_string(),
            timing: TriggerTiming::After,
            event: TriggerEvent::Insert,
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql(
                " BEGIN INSERT INTO audit VALUES (NEW.id); END".to_string(),
            ),
            enabled: true,
            sql_definition: None,
        };

        let sql = generate_create_trigger_sql(&trigger);
        assert_eq!(
            sql,
            "CREATE TRIGGER audit_insert AFTER INSERT ON users FOR EACH ROW BEGIN INSERT INTO audit VALUES (NEW.id); END"
        );
    }

    #[test]
    fn test_generate_create_trigger_sql_with_update_of() {
        use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
        use vibesql_catalog::TriggerDefinition;

        let trigger = TriggerDefinition {
            name: "track_status_change".to_string(),
            schema: None,
            table_name: "orders".to_string(),
            timing: TriggerTiming::Before,
            event: TriggerEvent::Update(Some(vec!["status".to_string(), "updated_at".to_string()])),
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql(" BEGIN SELECT 1; END".to_string()),
            enabled: true,
            sql_definition: None,
        };

        let sql = generate_create_trigger_sql(&trigger);
        assert!(sql.contains("UPDATE OF status, updated_at"));
        assert!(sql.contains("CREATE TRIGGER track_status_change BEFORE UPDATE OF"));
    }

    #[test]
    fn test_generate_create_trigger_sql_instead_of() {
        use vibesql_ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};
        use vibesql_catalog::TriggerDefinition;

        let trigger = TriggerDefinition {
            name: "instead_delete".to_string(),
            schema: None,
            table_name: "my_view".to_string(),
            timing: TriggerTiming::InsteadOf,
            event: TriggerEvent::Delete,
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql(
                " BEGIN DELETE FROM base_table WHERE id = OLD.id; END".to_string(),
            ),
            enabled: true,
            sql_definition: None,
        };

        let sql = generate_create_trigger_sql(&trigger);
        assert!(sql.contains("INSTEAD OF DELETE"));
        assert!(sql.contains("FOR EACH ROW"));
    }

    /// Create a minimal SelectStmt for testing purposes
    fn create_minimal_select_stmt() -> vibesql_ast::SelectStmt {
        vibesql_ast::SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![vibesql_ast::SelectItem::Wildcard { alias: None }],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        }
    }

    #[test]
    fn test_generate_create_view_sql_with_definition() {
        use vibesql_catalog::ViewDefinition;

        // Create a view with sql_definition set
        let view = ViewDefinition::new_with_sql(
            "active_users".to_string(),
            Some(vec!["id".to_string(), "name".to_string()]),
            create_minimal_select_stmt(),
            false,
            "CREATE VIEW active_users (id, name) AS SELECT id, name FROM users WHERE active = 1"
                .to_string(),
        );

        let sql = generate_create_view_sql(&view);
        assert_eq!(
            sql,
            "CREATE VIEW active_users (id, name) AS SELECT id, name FROM users WHERE active = 1"
        );
    }

    #[test]
    fn test_generate_create_view_sql_fallback() {
        use vibesql_catalog::ViewDefinition;

        // Create a view without sql_definition (fallback path)
        let view = ViewDefinition::new(
            "test_view".to_string(),
            Some(vec!["col1".to_string()]),
            create_minimal_select_stmt(),
            false,
        );

        let sql = generate_create_view_sql(&view);
        // Should contain CREATE VIEW and the view name
        assert!(sql.contains("CREATE VIEW test_view"));
        // With column list specified
        assert!(sql.contains("(col1)"));
    }

    /// Issue #5796 (alterdropcol 7.2): the implicit PRIMARY KEY autoindex of a
    /// WITHOUT ROWID table must be hidden from sqlite_master (in SQLite the PK
    /// *is* the table b-tree), while a UNIQUE-constraint autoindex on the same
    /// table and a PK autoindex on an ordinary rowid table stay listed.
    #[test]
    fn test_without_rowid_pk_autoindex_hidden_from_sqlite_master() {
        let mut catalog = Catalog::new();

        let make_cols = || {
            vec![
                ColumnSchema::new("a".to_string(), DataType::Integer, true),
                ColumnSchema::new("b".to_string(), DataType::Integer, true),
            ]
        };

        // WITHOUT ROWID table with PK(a) and UNIQUE(b). The PK materializes an
        // internal index outside the autoindex namespace (issue #5882), so the
        // UNIQUE constraint is the first real index and gets the `_1` ordinal.
        let wr_pk_index = format!("{}t_wr", vibesql_catalog::WITHOUT_ROWID_PK_INDEX_PREFIX);
        let mut wr =
            TableSchema::with_primary_key("t_wr".to_string(), make_cols(), vec!["a".to_string()]);
        wr.without_rowid = true;
        catalog.create_table(wr).unwrap();
        catalog
            .add_index(IndexMetadata::new(
                wr_pk_index.clone(),
                "t_wr".to_string(),
                IndexType::BTree,
                vec![IndexedColumn::new_column("a".to_string(), SortOrder::Ascending)],
                true,
            ))
            .unwrap();
        catalog
            .add_index(IndexMetadata::new(
                "sqlite_autoindex_t_wr_1".to_string(),
                "t_wr".to_string(),
                IndexType::BTree,
                vec![IndexedColumn::new_column("b".to_string(), SortOrder::Ascending)],
                true,
            ))
            .unwrap();

        // Ordinary rowid table with a (non-INTEGER-PK-alias) PK autoindex.
        let rowid = TableSchema::with_primary_key(
            "t_rowid".to_string(),
            make_cols(),
            vec!["a".to_string()],
        );
        catalog.create_table(rowid).unwrap();
        catalog
            .add_index(IndexMetadata::new(
                "sqlite_autoindex_t_rowid_1".to_string(),
                "t_rowid".to_string(),
                IndexType::BTree,
                vec![IndexedColumn::new_column("a".to_string(), SortOrder::Ascending)],
                true,
            ))
            .unwrap();

        let result = execute_sqlite_schema_query(&catalog).unwrap();
        let index_names: Vec<String> = result
            .rows
            .iter()
            .filter(|r| matches!(&r.values[0], SqlValue::Varchar(s) if s.as_str() == "index"))
            .map(|r| match &r.values[1] {
                SqlValue::Varchar(s) => s.to_string(),
                other => panic!("Expected VARCHAR index name, got {other:?}"),
            })
            .collect();

        assert!(
            !index_names.contains(&wr_pk_index),
            "WITHOUT ROWID PK internal index must be hidden, got {index_names:?}"
        );
        assert!(
            index_names.contains(&"sqlite_autoindex_t_wr_1".to_string()),
            "UNIQUE autoindex on WITHOUT ROWID table must stay listed as _1, got {index_names:?}"
        );
        assert!(
            !index_names.contains(&"sqlite_autoindex_t_wr_2".to_string()),
            "UNIQUE autoindex must not be bumped to _2 by the hidden PK, got {index_names:?}"
        );
        assert!(
            index_names.contains(&"sqlite_autoindex_t_rowid_1".to_string()),
            "rowid-table PK autoindex must stay listed, got {index_names:?}"
        );
    }

    fn parse_update(sql: &str) -> vibesql_ast::UpdateStmt {
        match vibesql_parser::Parser::parse_sql(sql).unwrap() {
            vibesql_ast::Statement::Update(stmt) => stmt,
            other => panic!("Expected UPDATE statement, got {other:?}"),
        }
    }

    fn writable_schema_test_db() -> vibesql_storage::Database {
        let mut db = vibesql_storage::Database::new();
        for name in ["t1", "t2"] {
            let mut schema = TableSchema::new(
                name.to_string(),
                vec![ColumnSchema::new("a".to_string(), DataType::Integer, true)],
            );
            schema.set_sql_source(format!("CREATE TABLE {name}(a)"));
            db.create_table_with_identifier(
                schema,
                vibesql_catalog::TableIdentifier::new(name, false),
            )
            .unwrap();
        }
        db
    }

    /// Issue #5796 (alterdropcol 8.x): under writable_schema, UPDATE
    /// sqlite_schema SET sql = '...' rewrites the stored CREATE TABLE source
    /// text of the matching table rows.
    #[test]
    fn test_writable_schema_update_rewrites_sql_source_with_where() {
        let mut db = writable_schema_test_db();
        let stmt = parse_update(
            "UPDATE sqlite_schema SET sql = 'CREATE TABLE t2(a, b)' WHERE name = 't2'",
        );

        let matched = execute_sqlite_schema_update(&stmt, &mut db).unwrap();
        assert_eq!(matched, 1);
        assert_eq!(
            db.catalog.get_table("t2").unwrap().sql_source.as_deref(),
            Some("CREATE TABLE t2(a, b)")
        );
        // Non-matching row untouched.
        assert_eq!(
            db.catalog.get_table("t1").unwrap().sql_source.as_deref(),
            Some("CREATE TABLE t1(a)")
        );
    }

    /// A WHERE-less UPDATE (alterdropcol 8.0 form) matches every schema row.
    #[test]
    fn test_writable_schema_update_without_where_matches_all_tables() {
        let mut db = writable_schema_test_db();
        let stmt = parse_update("UPDATE sqlite_schema SET sql = 'CREATE TABLE x(a)'");

        let matched = execute_sqlite_schema_update(&stmt, &mut db).unwrap();
        assert_eq!(matched, 2);
        for name in ["t1", "t2"] {
            assert_eq!(
                db.catalog.get_table(name).unwrap().sql_source.as_deref(),
                Some("CREATE TABLE x(a)"),
                "table {name} must be rewritten"
            );
        }
    }

    /// Assignments to columns other than `sql` are outside the supported
    /// writable_schema subset and must be rejected, not silently ignored.
    #[test]
    fn test_writable_schema_update_rejects_non_sql_column() {
        let mut db = writable_schema_test_db();
        let stmt = parse_update("UPDATE sqlite_schema SET name = 'oops'");

        let err = execute_sqlite_schema_update(&stmt, &mut db).unwrap_err();
        assert!(matches!(err, ExecutorError::UnsupportedFeature(_)), "got {err:?}");
    }

    /// The UPDATE executor keeps rejecting schema-table writes when
    /// writable_schema is OFF (the default) and routes them to the
    /// writable_schema path when ON.
    #[test]
    fn test_update_executor_gates_sqlite_schema_on_writable_schema_pragma() {
        let mut db = writable_schema_test_db();
        let stmt = parse_update(
            "UPDATE sqlite_schema SET sql = 'CREATE TABLE t1(a, b)' WHERE name = 't1'",
        );

        // Default: read-only error.
        let err = crate::UpdateExecutor::execute(&stmt, &mut db).unwrap_err();
        assert!(matches!(err, ExecutorError::SqliteSystemTableReadOnly { .. }), "got {err:?}");
        assert_eq!(
            db.catalog.get_table("t1").unwrap().sql_source.as_deref(),
            Some("CREATE TABLE t1(a)")
        );

        // With PRAGMA writable_schema=ON: the rewrite goes through.
        db.set_writable_schema(true);
        let matched = crate::UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(matched, 1);
        assert_eq!(
            db.catalog.get_table("t1").unwrap().sql_source.as_deref(),
            Some("CREATE TABLE t1(a, b)")
        );
    }
}
