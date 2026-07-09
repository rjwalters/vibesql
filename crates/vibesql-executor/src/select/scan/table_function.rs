//! Table-valued function (TVF) execution for the FROM clause.
//!
//! Implements the non-correlated forms of SQLite's JSON1 table-valued
//! functions `json_each(json[, path])` and `json_tree(json[, path])`. These
//! appear in FROM position and expand a JSON document into a relation with the
//! canonical eight-column contract:
//!
//! ```text
//! key, value, type, atom, id, parent, fullkey, path
//! ```
//!
//! - `json_each` yields one row per *immediate* child of the (possibly
//!   path-navigated) value; a scalar value yields exactly one row.
//! - `json_tree` performs a depth-first walk emitting one row per node,
//!   including the root.
//!
//! This handler is modeled on [`super::values::execute_values`]: it evaluates
//! the argument expression(s), materializes the resulting rows, attaches a
//! fixed derived-table schema, and returns a [`FromResult`].
//!
//! ## Scope
//!
//! **Non-correlated only** (ADR-0005 Bucket A): the argument expressions are
//! literals, bind parameters, or otherwise outer-independent — plus the case
//! where the TVF is the correlated child of a subquery that already threads an
//! outer row (e.g. `EXISTS(SELECT 1 FROM json_each(t.j,'$.items') ...)`). The
//! bare comma-lateral form `FROM t, json_each(t.j)` is ADR-0005 step 4 (LATERAL)
//! and is *not* handled here.
//!
//! Reference: <https://www.sqlite.org/json1.html#jeach>

use crate::{
    errors::ExecutorError,
    evaluator::{
        functions::sqlite_compat::json_funcs::{
            json_node_to_json_text, json_node_to_sql_value, json_node_type_name, navigate,
            parse_json_relaxed, parse_sqlite_json_path,
        },
        CombinedExpressionEvaluator,
    },
    schema::CombinedSchema,
};
use vibesql_types::{DataType, SqlValue};

/// The eight columns exposed by `json_each` / `json_tree`, in order.
const TVF_COLUMNS: [&str; 8] = ["key", "value", "type", "atom", "id", "parent", "fullkey", "path"];

/// Execute a JSON table-valued function (`json_each` / `json_tree`) in FROM
/// position, returning the expanded 8-column relation.
///
/// # Arguments
///
/// * `name` - The function name, already normalized to lowercase
///   (`"json_each"` or `"json_tree"`).
/// * `args` - The argument expressions: `args[0]` is the JSON value, optional
///   `args[1]` is a JSON path.
/// * `alias` - Optional table alias (`FROM json_each(x) AS je`). When absent the
///   function name is used as the table name so `json_each.value` resolves.
/// * `column_aliases` - Optional column renaming (`AS je(k, v)`).
/// * `database` - Database reference for expression evaluation.
/// * `cte_results` - CTE context so arguments can reference enclosing WITH names.
/// * `outer_row` / `outer_schema` - Outer correlation context. When present the
///   arguments resolve against the outer row (correlated-subquery-child form).
pub(crate) fn execute_table_function(
    name: &str,
    args: &[vibesql_ast::Expression],
    alias: Option<&String>,
    column_aliases: Option<&Vec<String>>,
    database: &vibesql_storage::Database,
    cte_results: &std::collections::HashMap<String, crate::select::cte::CteResult>,
    outer_row: Option<&vibesql_storage::Row>,
    outer_schema: Option<&CombinedSchema>,
) -> Result<super::FromResult, ExecutorError> {
    let is_each = match name {
        "json_each" => true,
        "json_tree" => false,
        other => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "table function '{}' in FROM is not supported (only json_each/json_tree)",
                other
            )));
        }
    };

    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::SqliteCompatError(format!(
            "{}() requires 1 or 2 arguments",
            name
        )));
    }

    // Evaluate the argument expressions. The empty schema means the arguments
    // themselves reference no *local* FROM columns; correlated column
    // references resolve against the threaded outer row/schema (Bucket A:
    // TVF-as-correlated-subquery-child).
    let empty_schema = CombinedSchema::empty();
    let empty_row = vibesql_storage::Row::new(vec![]);

    let mut evaluator = match (outer_row, outer_schema) {
        (Some(orow), Some(oschema)) => {
            CombinedExpressionEvaluator::with_database_and_outer_context(
                &empty_schema,
                database,
                orow,
                oschema,
            )
        }
        _ => CombinedExpressionEvaluator::with_database(&empty_schema, database),
    };
    if !cte_results.is_empty() {
        evaluator = evaluator.with_cte_context(cte_results);
    }

    let json_value = evaluator.eval(&args[0], &empty_row).map_err(|e| {
        ExecutorError::TypeError(format!("Error evaluating {}() argument: {}", name, e))
    })?;

    let path_value = match args.get(1) {
        Some(expr) => Some(evaluator.eval(expr, &empty_row).map_err(|e| {
            ExecutorError::TypeError(format!("Error evaluating {}() path argument: {}", name, e))
        })?),
        None => None,
    };

    // Build the fixed 8-column schema up front so even the zero-row cases (NULL
    // input, unmatched path, empty container) carry the correct shape.
    let schema = build_schema(name, alias, column_aliases)?;

    // A NULL json argument yields zero rows (SQLite: `json_each(NULL)` -> 0
    // rows). Same for a NULL path.
    let json_str = match &json_value {
        SqlValue::Null => return Ok(super::FromResult::from_rows(schema, vec![])),
        other => sqlvalue_as_json_text(other),
    };
    let json_str = match json_str {
        Some(s) => s,
        None => return Ok(super::FromResult::from_rows(schema, vec![])),
    };

    let root = parse_json_relaxed(&json_str)
        .map_err(|_| ExecutorError::SqliteCompatError("malformed JSON".to_string()))?;

    // Resolve the optional path argument. The path is applied to the root; the
    // referenced node becomes the expansion root. The `base_path` string is the
    // JSONPath prefix used to build `fullkey`/`path` for emitted rows.
    let (start_node, base_path) = match path_value {
        Some(SqlValue::Null) => {
            return Ok(super::FromResult::from_rows(schema, vec![]));
        }
        Some(pv) => {
            let path_str = match sqlvalue_as_json_text(&pv) {
                Some(s) => s,
                None => return Ok(super::FromResult::from_rows(schema, vec![])),
            };
            let segments =
                parse_sqlite_json_path(&path_str).map_err(ExecutorError::SqliteCompatError)?;
            match navigate(&root, &segments) {
                Some(node) => (node, path_str),
                // Path does not resolve -> zero rows.
                None => return Ok(super::FromResult::from_rows(schema, vec![])),
            }
        }
        None => (&root, "$".to_string()),
    };

    let mut rows: Vec<vibesql_storage::Row> = Vec::new();
    let mut next_id: i64 = 0;
    if is_each {
        expand_each(start_node, &base_path, &mut rows, &mut next_id);
    } else {
        // json_tree: emit the root node itself, then recurse.
        // The root's `key` is NULL, its `fullkey`/`path` are the base path.
        expand_tree_node(
            start_node,
            SqlValue::Null,
            &base_path,
            &base_path,
            None,
            &mut rows,
            &mut next_id,
        );
    }

    Ok(super::FromResult::from_rows(schema, rows))
}

/// Build the derived-table schema (8 fixed columns, optionally renamed).
pub(crate) fn build_schema(
    name: &str,
    alias: Option<&String>,
    column_aliases: Option<&Vec<String>>,
) -> Result<CombinedSchema, ExecutorError> {
    let column_names: Vec<String> = match column_aliases {
        Some(aliases) => {
            if aliases.len() != TVF_COLUMNS.len() {
                return Err(ExecutorError::ColumnCountMismatch {
                    expected: TVF_COLUMNS.len(),
                    provided: aliases.len(),
                });
            }
            aliases.clone()
        }
        None => TVF_COLUMNS.iter().map(|s| s.to_string()).collect(),
    };

    // Column types mirror SQLite: key/value/atom are dynamic (declared NULL so
    // the derived schema treats them permissively), type/fullkey/path are text,
    // id/parent are integers.
    let column_types = vec![
        DataType::Null,                         // key
        DataType::Null,                         // value
        DataType::Varchar { max_length: None }, // type
        DataType::Null,                         // atom
        DataType::Bigint,                       // id
        DataType::Bigint,                       // parent
        DataType::Varchar { max_length: None }, // fullkey
        DataType::Varchar { max_length: None }, // path
    ];

    // Table name defaults to the function name so `json_each.value` resolves
    // when no explicit alias is given (SQLite behavior).
    let table_name = alias.map(|a| a.to_string()).unwrap_or_else(|| name.to_string());
    Ok(CombinedSchema::from_derived_table(table_name, column_names, column_types))
}

/// Coerce a non-NULL SQL value into the JSON *text* it represents for TVF input
/// / path arguments. Text values are used verbatim; numeric/boolean scalars are
/// rendered as their JSON scalar token. Returns `None` for values that cannot be
/// a JSON document (which the caller treats as zero rows).
fn sqlvalue_as_json_text(v: &SqlValue) -> Option<String> {
    match v {
        SqlValue::Varchar(s) | SqlValue::Character(s) => Some(s.as_str().to_string()),
        SqlValue::Integer(i) | SqlValue::Bigint(i) => Some(i.to_string()),
        SqlValue::Smallint(i) => Some(i.to_string()),
        SqlValue::Unsigned(u) => Some(u.to_string()),
        SqlValue::Real(f) | SqlValue::Double(f) | SqlValue::Numeric(f) => {
            Some(SqlValue::Real(*f).to_string())
        }
        SqlValue::Float(f) => Some(SqlValue::Real(*f as f64).to_string()),
        SqlValue::Boolean(b) => Some(if *b { "1" } else { "0" }.to_string()),
        _ => None,
    }
}

/// Is this JSON node a container (object or array)?
fn is_container(node: &serde_json::Value) -> bool {
    matches!(node, serde_json::Value::Object(_) | serde_json::Value::Array(_))
}

/// The `value` column rendering for a node: scalars use SQLite's `->>`-style
/// scalar value; containers use their minified JSON text.
///
/// A container's `value` carries SQLite's JSON "J" subtype (per the 2024-02-16
/// json_tree/json_each regression fix, sqlite.org forumpost/ecb94cd210): when
/// such a value is fed to a JSON construction function it embeds as a
/// sub-document rather than being quoted as a string (json101-5.10 vs 5.11).
/// VibeSQL does not carry a distinct subtype tag on `SqlValue`, so it signals
/// the JSON subtype on TEXT container values by emitting them as
/// [`SqlValue::Character`] instead of [`SqlValue::Varchar`]. Both variants are
/// interoperable text everywhere else (display, comparison, `typeof`), so the
/// marker is invisible to all other consumers; only the JSON construction
/// functions read it (see `json_funcs::sql_value_is_json_subtyped`).
fn node_value_column(node: &serde_json::Value) -> SqlValue {
    if is_container(node) {
        SqlValue::Character(json_node_to_json_text(node).into())
    } else {
        json_node_to_sql_value(node)
    }
}

/// The `atom` column: the scalar value for leaves, SQL NULL for containers.
fn node_atom_column(node: &serde_json::Value) -> SqlValue {
    if is_container(node) {
        SqlValue::Null
    } else {
        json_node_to_sql_value(node)
    }
}

/// Assemble one 8-column output row.
fn make_row(
    key: SqlValue,
    node: &serde_json::Value,
    id: i64,
    parent: Option<i64>,
    fullkey: &str,
    path: &str,
) -> vibesql_storage::Row {
    vibesql_storage::Row::new(vec![
        key,
        node_value_column(node),
        SqlValue::Varchar(json_node_type_name(node).into()),
        node_atom_column(node),
        SqlValue::Bigint(id),
        parent.map(SqlValue::Bigint).unwrap_or(SqlValue::Null),
        SqlValue::Varchar(fullkey.into()),
        SqlValue::Varchar(path.into()),
    ])
}

/// `json_each` expansion: one row per immediate child of `node`. A scalar (or
/// container navigated to via a path that lands on a scalar) yields exactly one
/// row for the node itself.
fn expand_each(
    node: &serde_json::Value,
    base_path: &str,
    rows: &mut Vec<vibesql_storage::Row>,
    next_id: &mut i64,
) {
    match node {
        serde_json::Value::Array(arr) => {
            for (i, child) in arr.iter().enumerate() {
                let fullkey = format!("{}[{}]", base_path, i);
                let id = *next_id;
                *next_id += 1;
                rows.push(make_row(
                    SqlValue::Bigint(i as i64),
                    child,
                    id,
                    None,
                    &fullkey,
                    base_path,
                ));
            }
        }
        serde_json::Value::Object(map) => {
            for (k, child) in map.iter() {
                let fullkey = format!("{}{}", base_path, dot_key(k));
                let id = *next_id;
                *next_id += 1;
                rows.push(make_row(
                    SqlValue::Varchar(k.as_str().into()),
                    child,
                    id,
                    None,
                    &fullkey,
                    base_path,
                ));
            }
        }
        // Scalar: a single row for the node itself, key NULL, fullkey == path.
        scalar => {
            let id = *next_id;
            *next_id += 1;
            rows.push(make_row(SqlValue::Null, scalar, id, None, base_path, base_path));
        }
    }
}

/// `json_tree` recursive expansion. Emits a row for `node` (with the given
/// `key`, `fullkey`, `path`, and `parent`), then, if `node` is a container,
/// recurses into each child.
fn expand_tree_node(
    node: &serde_json::Value,
    key: SqlValue,
    fullkey: &str,
    path: &str,
    parent: Option<i64>,
    rows: &mut Vec<vibesql_storage::Row>,
    next_id: &mut i64,
) {
    let id = *next_id;
    *next_id += 1;
    rows.push(make_row(key, node, id, parent, fullkey, path));

    match node {
        serde_json::Value::Array(arr) => {
            for (i, child) in arr.iter().enumerate() {
                let child_fullkey = format!("{}[{}]", fullkey, i);
                expand_tree_node(
                    child,
                    SqlValue::Bigint(i as i64),
                    &child_fullkey,
                    fullkey,
                    Some(id),
                    rows,
                    next_id,
                );
            }
        }
        serde_json::Value::Object(map) => {
            for (k, child) in map.iter() {
                let child_fullkey = format!("{}{}", fullkey, dot_key(k));
                expand_tree_node(
                    child,
                    SqlValue::Varchar(k.as_str().into()),
                    &child_fullkey,
                    fullkey,
                    Some(id),
                    rows,
                    next_id,
                );
            }
        }
        _ => {}
    }
}

/// Render an object member key as a JSONPath component. Simple identifiers use
/// `.key`; anything else is quoted `."key"` (matching SQLite's fullkey output).
fn dot_key(key: &str) -> String {
    if is_simple_key(key) {
        format!(".{}", key)
    } else {
        format!(".\"{}\"", key.replace('"', "\\\""))
    }
}

/// A "simple" object key needs no quoting in a JSONPath. SQLite leaves a label
/// unquoted only when the first char is ASCII **alphabetic** and every char is
/// ASCII **alphanumeric** — underscore is NOT permitted anywhere and always
/// forces quoting (e.g. `a_b` -> `$."a_b"`), matching sqlite3's fullkey/path.
fn is_simple_key(key: &str) -> bool {
    let mut chars = key.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn each_rows(json: &str) -> Vec<vibesql_storage::Row> {
        let root = parse_json_relaxed(json).unwrap();
        let mut rows = Vec::new();
        let mut id = 0;
        expand_each(&root, "$", &mut rows, &mut id);
        rows
    }

    fn tree_rows(json: &str) -> Vec<vibesql_storage::Row> {
        let root = parse_json_relaxed(json).unwrap();
        let mut rows = Vec::new();
        let mut id = 0;
        expand_tree_node(&root, SqlValue::Null, "$", "$", None, &mut rows, &mut id);
        rows
    }

    // Column index constants for readability.
    const KEY: usize = 0;
    const VALUE: usize = 1;
    const TYPE: usize = 2;
    const ATOM: usize = 3;
    const ID: usize = 4;
    const PARENT: usize = 5;
    const FULLKEY: usize = 6;
    const PATH: usize = 7;

    fn s(v: &SqlValue) -> String {
        match v {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str().to_string(),
            SqlValue::Integer(i) | SqlValue::Bigint(i) => i.to_string(),
            SqlValue::Real(f) => SqlValue::Real(*f).to_string(),
            SqlValue::Null => "NULL".to_string(),
            other => format!("{:?}", other),
        }
    }

    #[test]
    fn each_scalar_integer_all_columns() {
        // json_each('123') -> single row, key NULL, fullkey/path "$".
        let rows = each_rows("123");
        assert_eq!(rows.len(), 1);
        let r = &rows[0].values;
        assert_eq!(r[KEY], SqlValue::Null, "key");
        assert_eq!(r[VALUE], SqlValue::Integer(123), "value");
        assert_eq!(s(&r[TYPE]), "integer", "type");
        assert_eq!(r[ATOM], SqlValue::Integer(123), "atom");
        assert_eq!(r[ID], SqlValue::Bigint(0), "id");
        assert_eq!(r[PARENT], SqlValue::Null, "parent");
        assert_eq!(s(&r[FULLKEY]), "$", "fullkey");
        assert_eq!(s(&r[PATH]), "$", "path");
    }

    #[test]
    fn each_scalar_null_value() {
        // json_each('null') -> single row, value/atom NULL, type "null".
        let rows = each_rows("null");
        assert_eq!(rows.len(), 1);
        let r = &rows[0].values;
        assert_eq!(r[KEY], SqlValue::Null);
        assert_eq!(r[VALUE], SqlValue::Null, "value");
        assert_eq!(s(&r[TYPE]), "null", "type");
        assert_eq!(r[ATOM], SqlValue::Null, "atom");
        assert_eq!(s(&r[FULLKEY]), "$");
        assert_eq!(s(&r[PATH]), "$");
    }

    #[test]
    fn each_array_key_is_index_and_fullkey() {
        let rows = each_rows("[1,2,3]");
        assert_eq!(rows.len(), 3);
        for (i, row) in rows.iter().enumerate() {
            let r = &row.values;
            assert_eq!(r[KEY], SqlValue::Bigint(i as i64), "array key = index");
            assert_eq!(r[VALUE], SqlValue::Integer((i + 1) as i64));
            assert_eq!(s(&r[TYPE]), "integer");
            assert_eq!(r[ATOM], SqlValue::Integer((i + 1) as i64));
            assert_eq!(r[PARENT], SqlValue::Null, "each: parent always NULL");
            assert_eq!(s(&r[FULLKEY]), format!("$[{}]", i));
            assert_eq!(s(&r[PATH]), "$", "each array path = base");
        }
    }

    #[test]
    fn each_object_container_child_atom_null() {
        // {"a":1,"b":[2,3]} -> two rows; container child has value=JSON text, atom NULL.
        let rows = each_rows(r#"{"a":1,"b":[2,3]}"#);
        assert_eq!(rows.len(), 2);
        // "a": scalar
        let a = &rows[0].values;
        assert_eq!(s(&a[KEY]), "a");
        assert_eq!(a[VALUE], SqlValue::Integer(1));
        assert_eq!(s(&a[TYPE]), "integer");
        assert_eq!(a[ATOM], SqlValue::Integer(1));
        assert_eq!(s(&a[FULLKEY]), "$.a");
        assert_eq!(s(&a[PATH]), "$");
        // "b": array container
        let b = &rows[1].values;
        assert_eq!(s(&b[KEY]), "b");
        assert_eq!(s(&b[VALUE]), "[2,3]", "container value = minified JSON");
        assert_eq!(s(&b[TYPE]), "array");
        assert_eq!(b[ATOM], SqlValue::Null, "container atom = NULL");
        assert_eq!(s(&b[FULLKEY]), "$.b");
    }

    #[test]
    fn each_empty_container_zero_rows() {
        assert_eq!(each_rows("[]").len(), 0);
        assert_eq!(each_rows("{}").len(), 0);
    }

    #[test]
    fn tree_scalar_single_root_row() {
        // json_tree('123') -> single root row.
        let rows = tree_rows("123");
        assert_eq!(rows.len(), 1);
        let r = &rows[0].values;
        assert_eq!(r[KEY], SqlValue::Null);
        assert_eq!(r[VALUE], SqlValue::Integer(123));
        assert_eq!(s(&r[FULLKEY]), "$");
        assert_eq!(s(&r[PATH]), "$");
        assert_eq!(r[PARENT], SqlValue::Null);
    }

    #[test]
    fn tree_object_root_then_member_parent_ids() {
        // json_tree(json_set('{}','$.x',123,'$.x',456)) == json_tree('{"x":456}').
        let rows = tree_rows(r#"{"x":456}"#);
        assert_eq!(rows.len(), 2);
        // Root: object, key NULL, atom NULL.
        let root = &rows[0].values;
        assert_eq!(root[KEY], SqlValue::Null);
        assert_eq!(s(&root[VALUE]), r#"{"x":456}"#);
        assert_eq!(s(&root[TYPE]), "object");
        assert_eq!(root[ATOM], SqlValue::Null);
        assert_eq!(root[ID], SqlValue::Bigint(0));
        assert_eq!(root[PARENT], SqlValue::Null);
        assert_eq!(s(&root[FULLKEY]), "$");
        assert_eq!(s(&root[PATH]), "$");
        // Member x -> 456.
        let x = &rows[1].values;
        assert_eq!(s(&x[KEY]), "x");
        assert_eq!(x[VALUE], SqlValue::Integer(456));
        assert_eq!(s(&x[TYPE]), "integer");
        assert_eq!(x[ATOM], SqlValue::Integer(456), "atom = leaf value");
        assert_eq!(x[ID], SqlValue::Bigint(1));
        assert_eq!(x[PARENT], SqlValue::Bigint(0), "parent = root id");
        assert_eq!(s(&x[FULLKEY]), "$.x");
        assert_eq!(s(&x[PATH]), "$", "member path = parent container");
    }

    #[test]
    fn tree_nested_array_pre_order_and_paths() {
        // json_tree('[1,[20,21],3]'): depth-first pre-order with parent links.
        let rows = tree_rows("[1,[20,21],3]");
        // root, [0]=1, [1]=array, [1][0]=20, [1][1]=21, [2]=3  => 6 rows.
        assert_eq!(rows.len(), 6);
        let fullkeys: Vec<String> = rows.iter().map(|r| s(&r.values[FULLKEY])).collect();
        assert_eq!(fullkeys, vec!["$", "$[0]", "$[1]", "$[1][0]", "$[1][1]", "$[2]"]);
        // The inner array's children point back to the inner array's id.
        let inner_array_id = rows[2].values[ID].clone();
        assert_eq!(rows[3].values[PARENT], inner_array_id, "$[1][0] parent = $[1]");
        assert_eq!(rows[4].values[PARENT], inner_array_id, "$[1][1] parent = $[1]");
        // path of $[1][0] is the inner array's fullkey.
        assert_eq!(s(&rows[3].values[PATH]), "$[1]");
        // ids are stable pre-order 0..n.
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.values[ID], SqlValue::Bigint(i as i64), "pre-order id");
        }
    }

    #[test]
    fn tree_booleans_type_and_atom() {
        // Booleans: type "true"/"false", atom 1/0.
        let rows = tree_rows("[true,false,null]");
        // root + 3 children.
        assert_eq!(rows.len(), 4);
        let t = &rows[1].values;
        assert_eq!(s(&t[TYPE]), "true");
        assert_eq!(t[ATOM], SqlValue::Integer(1));
        let f = &rows[2].values;
        assert_eq!(s(&f[TYPE]), "false");
        assert_eq!(f[ATOM], SqlValue::Integer(0));
        let n = &rows[3].values;
        assert_eq!(s(&n[TYPE]), "null");
        assert_eq!(n[ATOM], SqlValue::Null);
    }

    #[test]
    fn each_with_base_path_prefix() {
        // Simulate json_each(json,'$.items'): expansion of the navigated array
        // uses the path as the base so fullkey/path carry the prefix.
        let root = parse_json_relaxed(r#"{"items":[3,5]}"#).unwrap();
        let node = navigate(&root, &parse_sqlite_json_path("$.items").unwrap()).unwrap();
        let mut rows = Vec::new();
        let mut id = 0;
        expand_each(node, "$.items", &mut rows, &mut id);
        assert_eq!(rows.len(), 2);
        assert_eq!(s(&rows[0].values[FULLKEY]), "$.items[0]");
        assert_eq!(s(&rows[0].values[PATH]), "$.items");
        assert_eq!(rows[0].values[VALUE], SqlValue::Integer(3));
        assert_eq!(rows[1].values[VALUE], SqlValue::Integer(5));
    }

    #[test]
    fn quoted_key_fullkey() {
        // Keys with special characters are quoted in fullkey.
        let rows = each_rows(r#"{"a.b":1}"#);
        assert_eq!(rows.len(), 1);
        assert_eq!(s(&rows[0].values[FULLKEY]), r#"$."a.b""#);
    }

    #[test]
    fn is_simple_key_underscore_forces_quoting() {
        // SQLite leaves a label unquoted only when the first char is ASCII
        // alphabetic and every char is ASCII alphanumeric. Underscore always
        // forces quoting, matching sqlite3 3.51 fullkey/path.
        assert!(is_simple_key("abc"));
        assert!(is_simple_key("a1"));
        assert!(is_simple_key("a"));
        // Underscore anywhere -> not simple (must be quoted).
        assert!(!is_simple_key("a_b"), "a_b must be quoted");
        assert!(!is_simple_key("_x"), "_x must be quoted (leading underscore)");
        assert!(!is_simple_key("ab_"), "ab_ must be quoted (trailing underscore)");
        // Leading digit and other punctuation also require quoting.
        assert!(!is_simple_key("1a"));
        assert!(!is_simple_key("a-b"));
        assert!(!is_simple_key(""));
    }

    #[test]
    fn each_underscore_keys_quoted_fullkey_and_path() {
        // json_each('{"a_b":1,"_x":2,"ab_":3}'): underscore keys are quoted in
        // fullkey; path is the base container "$" for object members.
        let rows = each_rows(r#"{"a_b":1,"_x":2,"ab_":3}"#);
        assert_eq!(rows.len(), 3);
        let expected_fullkeys = [r#"$."a_b""#, r#"$."_x""#, r#"$."ab_""#];
        for (row, fk) in rows.iter().zip(expected_fullkeys) {
            assert_eq!(s(&row.values[FULLKEY]), fk, "each fullkey");
            assert_eq!(s(&row.values[PATH]), "$", "each member path = base");
        }
    }

    #[test]
    fn tree_underscore_keys_quoted_fullkey_and_path() {
        // json_tree('{"a_b":1,"_x":2,"ab_":3}'): root + 3 members. Underscore
        // keys are quoted in fullkey; each member's path is the root container.
        let rows = tree_rows(r#"{"a_b":1,"_x":2,"ab_":3}"#);
        assert_eq!(rows.len(), 4);
        // Root object.
        assert_eq!(s(&rows[0].values[FULLKEY]), "$");
        assert_eq!(s(&rows[0].values[PATH]), "$");
        let expected_fullkeys = [r#"$."a_b""#, r#"$."_x""#, r#"$."ab_""#];
        for (row, fk) in rows[1..].iter().zip(expected_fullkeys) {
            assert_eq!(s(&row.values[FULLKEY]), fk, "tree fullkey");
            assert_eq!(s(&row.values[PATH]), "$", "tree member path = parent container");
        }
    }

    #[test]
    fn tree_nested_underscore_key_path_carries_quoting() {
        // Nested container under an underscore key: the child's path is the
        // parent's (quoted) fullkey, so quoting propagates into path.
        let rows = tree_rows(r#"{"a_b":[7]}"#);
        // root, "a_b" (array container), "a_b"[0] = 7  => 3 rows.
        assert_eq!(rows.len(), 3);
        assert_eq!(s(&rows[1].values[FULLKEY]), r#"$."a_b""#);
        assert_eq!(s(&rows[1].values[PATH]), "$");
        // The array element's fullkey/path carry the quoted parent label.
        assert_eq!(s(&rows[2].values[FULLKEY]), r#"$."a_b"[0]"#);
        assert_eq!(s(&rows[2].values[PATH]), r#"$."a_b""#);
    }
}
