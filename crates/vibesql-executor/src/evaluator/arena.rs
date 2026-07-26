//! Arena-based expression evaluator for zero-allocation prepared statement execution.
//!
//! This module provides `ArenaExpressionEvaluator` which evaluates arena-allocated
//! expressions (`vibesql_ast::arena::Expression`) with inline placeholder resolution.
//!
//! # Performance
//!
//! Unlike the regular evaluator that works with owned AST types, this evaluator:
//! - Works directly with arena-allocated AST references
//! - Resolves placeholders inline without intermediate allocations
//! - Avoids cloning expressions during evaluation
//!
//! # Usage
//!
//! ```text
//! let params = &[SqlValue::Integer(42), SqlValue::Varchar(arcstr::ArcStr::from("hello"))];
//! let evaluator = ArenaExpressionEvaluator::new(schema, params);
//! let result = evaluator.eval(&arena_expr, &row)?;
//! ```

use std::{
    cell::RefCell,
    collections::HashMap,
    hash::{Hash, Hasher},
};

use ahash::AHasher;
use vibesql_ast::arena::{
    self as arena_ast, ArenaInterner, Expression as ArenaExpression,
    ExtendedExpr as ArenaExtendedExpr, Symbol,
};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Maximum expression evaluation depth to prevent stack overflow.
const MAX_ARENA_EXPRESSION_DEPTH: usize = 128;

/// Arena-based expression evaluator for prepared statement execution.
///
/// This evaluator works with arena-allocated expressions and resolves
/// placeholders inline from a provided parameters slice.
pub struct ArenaExpressionEvaluator<'a, 'arena> {
    /// Combined schema for column resolution
    schema: &'a CombinedSchema,
    /// Parameters for placeholder resolution
    params: &'a [SqlValue],
    /// Database reference for subquery execution
    database: Option<&'a vibesql_storage::Database>,
    /// SQL mode for operator semantics
    sql_mode: vibesql_types::SqlMode,
    /// Cache for column lookups to avoid repeated schema traversals
    column_cache: RefCell<HashMap<u64, usize>>,
    /// Current depth in expression tree (for preventing stack overflow)
    depth: usize,
    /// Interner for resolving symbols to strings
    interner: &'arena ArenaInterner<'arena>,
}

impl<'a, 'arena> ArenaExpressionEvaluator<'a, 'arena> {
    /// Create a new arena expression evaluator.
    ///
    /// # Arguments
    ///
    /// * `schema` - Combined schema for column resolution
    /// * `params` - Slice of parameter values for placeholder resolution
    /// * `interner` - Interner for resolving symbols to strings
    pub fn new(
        schema: &'a CombinedSchema,
        params: &'a [SqlValue],
        interner: &'arena ArenaInterner<'arena>,
    ) -> Self {
        ArenaExpressionEvaluator {
            schema,
            params,
            database: None,
            sql_mode: vibesql_types::SqlMode::default(),
            column_cache: RefCell::new(HashMap::new()),
            depth: 0,
            interner,
        }
    }

    /// Create a new arena expression evaluator with database reference.
    ///
    /// # Arguments
    ///
    /// * `schema` - Combined schema for column resolution
    /// * `params` - Slice of parameter values for placeholder resolution
    /// * `database` - Database reference for subquery execution
    /// * `interner` - Interner for resolving symbols to strings
    pub fn with_database(
        schema: &'a CombinedSchema,
        params: &'a [SqlValue],
        database: &'a vibesql_storage::Database,
        interner: &'arena ArenaInterner<'arena>,
    ) -> Self {
        ArenaExpressionEvaluator {
            schema,
            params,
            database: Some(database),
            sql_mode: database.sql_mode(),
            column_cache: RefCell::new(HashMap::new()),
            depth: 0,
            interner,
        }
    }

    /// Resolve a symbol to its string value.
    #[inline]
    fn resolve(&self, symbol: Symbol) -> &'arena str {
        self.interner.resolve(symbol)
    }

    /// Does this arena expression's result embed as a JSON sub-document when
    /// passed to another JSON function? True when it is a direct call to a JSON
    /// function whose output is a well-formed JSON document — JSON text or a JSONB
    /// blob (which decodes back to the same document when embedded). This
    /// *embedding* signal is distinct from `subtype()` reporting: a JSONB blob
    /// embeds correctly yet reports `subtype()` 0 (see
    /// `special.rs::expr_has_json_subtype`, `json_subtype.rs`, and the module note
    /// in `json_funcs.rs`).
    fn arena_expr_has_json_subtype(&self, expr: &ArenaExpression<'arena>) -> bool {
        if let ArenaExpression::Extended(ArenaExtendedExpr::Function { name, .. }) = expr {
            matches!(
                self.resolve(*name).to_ascii_lowercase().as_str(),
                "json"
                    | "json_array"
                    | "json_object"
                    | "json_insert"
                    | "json_replace"
                    | "json_set"
                    | "json_remove"
                    | "json_patch"
                    // JSONB functions emit a real BLOB (Stage 1, #6035) that
                    // decodes back to the same JSON document when embedded — so
                    // they embed correctly here, though `subtype()` on them is 0.
                    | "jsonb"
                    | "jsonb_array"
                    | "jsonb_object"
                    | "jsonb_insert"
                    | "jsonb_replace"
                    | "jsonb_set"
                    | "jsonb_remove"
                    | "jsonb_patch"
            )
        } else {
            false
        }
    }

    /// Is this arena argument expression eligible to carry the *runtime* JSON
    /// subtype marker? A bare read of a column declared with a real string type
    /// (CHAR/VARCHAR) is not eligible — its container-shaped text quotes rather
    /// than embedding (issue #6007). Arena mirror of
    /// [`crate::evaluator::json_subtype::expr_runtime_json_subtype_eligible`].
    fn arena_expr_runtime_json_subtype_eligible(&self, expr: &ArenaExpression<'arena>) -> bool {
        if let ArenaExpression::ColumnRef { table, column, .. } = expr {
            let column_str = self.resolve(*column);
            let table_str = table.map(|t| self.resolve(t));
            let declared_string = self
                .schema
                .get_column_index(table_str, column_str)
                .and_then(|idx| self.schema.get_column_type_by_index(idx))
                .map(crate::evaluator::json_subtype::data_type_is_string)
                .unwrap_or(false);
            !declared_string
        } else {
            true
        }
    }

    /// Structurally determine whether an arena expression evaluates to a
    /// JSON-subtyped value in the current row. Arena mirror of
    /// [`crate::evaluator::json_subtype`]; see that module for the rules.
    fn arena_expr_json_subtype(
        &self,
        expr: &ArenaExpression<'arena>,
        row: &Row,
    ) -> Result<bool, ExecutorError> {
        use vibesql_ast::BinaryOperator;
        Ok(match expr {
            ArenaExpression::BinaryOp { op: BinaryOperator::JsonExtract, .. } => {
                !matches!(self.eval(expr, row)?, SqlValue::Null)
            }
            ArenaExpression::BinaryOp { op: BinaryOperator::JsonExtractText, .. } => false,
            ArenaExpression::Extended(ArenaExtendedExpr::Function { name, args, .. }) => {
                let canon = self.resolve(*name).to_ascii_lowercase();
                // `json_quote` always returns valid JSON text and SQLite tags it
                // with subtype 74 unconditionally, so for `subtype()` recovery it
                // is an unconditional producer. (It is deliberately not an
                // *embedding* producer in `arena_expr_has_json_subtype`, so its
                // quoted output keeps quoting when passed to another JSON func.)
                if self.arena_expr_has_json_subtype(expr) || canon == "json_quote" {
                    !matches!(self.eval(expr, row)?, SqlValue::Null)
                } else if matches!(canon.as_str(), "json_extract" | "jsonb_extract") {
                    crate::evaluator::json_subtype::value_is_json_container(&self.eval(expr, row)?)
                } else if matches!(canon.as_str(), "if" | "iif") {
                    match self.arena_selected_conditional_branch(args, row)? {
                        Some(branch) => self.arena_expr_json_subtype(branch, row)?,
                        None => false,
                    }
                } else if matches!(canon.as_str(), "coalesce" | "ifnull") {
                    let mut result = false;
                    for arg in args.iter() {
                        if !matches!(self.eval(arg, row)?, SqlValue::Null) {
                            result = self.arena_expr_json_subtype(arg, row)?;
                            break;
                        }
                    }
                    result
                } else if canon == "nullif" && args.len() == 2 {
                    self.arena_expr_json_subtype(&args[0], row)?
                } else {
                    false
                }
            }
            ArenaExpression::Extended(ArenaExtendedExpr::Case {
                operand,
                when_clauses,
                else_result,
            }) => {
                match self.arena_selected_case_branch(
                    operand.as_deref(),
                    when_clauses,
                    else_result.as_deref(),
                    row,
                )? {
                    Some(branch) => self.arena_expr_json_subtype(branch, row)?,
                    None => false,
                }
            }
            // `CAST(inner AS <text type>)` preserves the JSON subtype of `inner`
            // (`CAST(json('[1,2]') AS TEXT)` is still subtype 74). A non-text cast
            // drops the subtype. Arena mirror of the standard evaluator.
            ArenaExpression::Extended(ArenaExtendedExpr::Cast { expr: inner, data_type })
                if crate::evaluator::json_subtype::data_type_is_string(data_type) =>
            {
                self.arena_expr_json_subtype(inner, row)?
            }
            // Scalar subquery with a single projected expression: recurse into
            // that projected expression, so `subtype((SELECT json('[1,2]')))`
            // reports the subtype of `json('[1,2]')`. Best-effort: an evaluation
            // error on the projected expression yields no subtype rather than
            // propagating.
            ArenaExpression::Extended(ArenaExtendedExpr::ScalarSubquery(select)) => {
                match self.arena_single_projected_expression(select) {
                    Some(projected) => {
                        self.arena_expr_json_subtype(projected, row).unwrap_or(false)
                    }
                    None => false,
                }
            }
            _ => {
                self.arena_expr_runtime_json_subtype_eligible(expr)
                    && crate::evaluator::functions::sqlite_compat::json_funcs::sql_value_is_json_subtyped(
                        &self.eval(expr, row)?,
                    )
            }
        })
    }

    /// Arena mirror of the `if`/`iif` branch-selection used by subtype recovery.
    fn arena_selected_conditional_branch<'e>(
        &self,
        args: &'e [ArenaExpression<'arena>],
        row: &Row,
    ) -> Result<Option<&'e ArenaExpression<'arena>>, ExecutorError> {
        use crate::evaluator::operators::is_truthy;
        let mut i = 0;
        while i + 1 < args.len() {
            if is_truthy(&self.eval(&args[i], row)?) {
                return Ok(Some(&args[i + 1]));
            }
            i += 2;
        }
        if !args.len().is_multiple_of(2) {
            Ok(Some(&args[args.len() - 1]))
        } else {
            Ok(None)
        }
    }

    /// Arena mirror of the CASE branch-selection used by subtype recovery.
    fn arena_selected_case_branch<'e>(
        &self,
        operand: Option<&'e ArenaExpression<'arena>>,
        when_clauses: &'e [arena_ast::CaseWhen<'arena>],
        else_result: Option<&'e ArenaExpression<'arena>>,
        row: &Row,
    ) -> Result<Option<&'e ArenaExpression<'arena>>, ExecutorError> {
        use crate::evaluator::operators::is_truthy;
        let operand_val = match operand {
            Some(op) => Some(self.eval(op, row)?),
            None => None,
        };
        for wc in when_clauses.iter() {
            for cond in wc.conditions.iter() {
                let matched = match &operand_val {
                    Some(ov) => {
                        let cv = self.eval(cond, row)?;
                        crate::evaluator::core::values_are_equal(ov, &cv)
                    }
                    None => is_truthy(&self.eval(cond, row)?),
                };
                if matched {
                    return Ok(Some(&wc.result));
                }
            }
        }
        Ok(else_result)
    }

    /// Return the single projected expression of an arena scalar subquery, or
    /// `None` when the projection is not exactly one plain expression (a
    /// wildcard, a set operation, or a multi-column projection all disqualify).
    /// Arena mirror of
    /// [`crate::evaluator::json_subtype`]'s `single_projected_expression`.
    fn arena_single_projected_expression<'e>(
        &self,
        select: &'e arena_ast::SelectStmt<'arena>,
    ) -> Option<&'e ArenaExpression<'arena>> {
        if select.set_operation.is_some() {
            return None;
        }
        match select.select_list.as_slice() {
            [arena_ast::SelectItem::Expression { expr, .. }] => Some(expr),
            _ => None,
        }
    }

    /// Evaluate an arena-allocated expression against a row.
    ///
    /// # Arguments
    ///
    /// * `expr` - Arena-allocated expression to evaluate
    /// * `row` - Row data for column resolution
    ///
    /// # Returns
    ///
    /// The evaluated SqlValue result.
    pub fn eval(
        &self,
        expr: &ArenaExpression<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= MAX_ARENA_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: MAX_ARENA_EXPRESSION_DEPTH,
            });
        }

        self.eval_impl(expr, row)
    }

    /// Internal evaluation implementation.
    fn eval_impl(
        &self,
        expr: &ArenaExpression<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        match expr {
            // Literals - direct return without allocation
            ArenaExpression::Literal(val) => Ok(val.clone()),

            // Placeholder - inline resolution from params slice
            ArenaExpression::Placeholder(idx) => self.params.get(*idx).cloned().ok_or_else(|| {
                ExecutorError::UnsupportedExpression(format!(
                    "Parameter index {} out of bounds (available: {})",
                    idx,
                    self.params.len()
                ))
            }),

            // Numbered placeholder ($1, $2, etc.) - 1-indexed
            ArenaExpression::NumberedPlaceholder(num) => {
                let idx = num.saturating_sub(1);
                self.params.get(idx).cloned().ok_or_else(|| {
                    ExecutorError::UnsupportedExpression(format!(
                        "Parameter ${} out of bounds (available: {})",
                        num,
                        self.params.len()
                    ))
                })
            }

            // Named placeholder - not supported in this evaluator
            ArenaExpression::NamedPlaceholder(name) => {
                Err(ExecutorError::UnsupportedExpression(format!(
                    "Named placeholder '{}' not supported in arena evaluator",
                    self.resolve(*name)
                )))
            }

            // Column reference
            ArenaExpression::ColumnRef { schema, table, column, .. } => {
                let column_str = self.resolve(*column);
                let table_str = table.map(|t| self.resolve(t));

                // Handle schema qualifier (three-part names like schema.table.column)
                if let Some(schema_sym) = schema {
                    let schema_str = self.resolve(*schema_sym);
                    if !schema_str.eq_ignore_ascii_case("main") {
                        // SQLite returns "no such column: schema.table.column" for unknown schemas
                        return Err(ExecutorError::ColumnNotFound {
                            column_name: format!(
                                "{}.{}.{}",
                                schema_str,
                                table_str.unwrap_or(""),
                                column_str
                            ),
                            table_name: table_str
                                .map(|t| t.to_string())
                                .unwrap_or_else(|| "unknown".to_string()),
                            searched_tables: self.schema.table_names(),
                            available_columns: self.get_available_columns(),
                        });
                    }
                }

                // Special case: "*" is a wildcard used in COUNT(*)
                if column_str == "*" {
                    return Ok(SqlValue::Null);
                }

                if let Some(col_index) = self.get_column_index_cached(table_str, column_str) {
                    return row
                        .get(col_index)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                }

                // SQLite compatibility: Handle ROWID pseudo-column.
                // ROWID, _rowid_, and oid are aliases that return the row's unique
                // identifier. This mirrors the resolution already implemented in
                // `combined/eval.rs` and `expressions/eval.rs`. Real columns take
                // precedence (checked above), so we only reach here for the
                // pseudo-column. WITHOUT ROWID tables and VIEWs do NOT have the
                // rowid pseudo-column (#4953, #5492); TVF-derived FROM items have no
                // tracked row-id and therefore fall back to NULL (#6019).
                let column_lower = column_str.to_ascii_lowercase();
                if column_lower == "rowid" || column_lower == "_rowid_" || column_lower == "oid" {
                    let table_id = table_str.map(vibesql_catalog::TableIdentifier::from);

                    // WITHOUT ROWID tables (#4953) and VIEWs (#5492) both lack the
                    // rowid pseudo-column and must error.
                    if let Some(ref table_id) = table_id {
                        if let Some((_, table_schema)) = self.schema.table_schemas.get(table_id) {
                            if table_schema.without_rowid || table_schema.is_view {
                                return Err(ExecutorError::ColumnNotFound {
                                    column_name: column_str.to_string(),
                                    table_name: table_id.display().to_string(),
                                    searched_tables: vec![table_id.display().to_string()],
                                    available_columns: table_schema
                                        .columns
                                        .iter()
                                        .map(|c| c.name.clone())
                                        .collect(),
                                });
                            }
                        }
                    } else {
                        // Unqualified rowid - if any table in scope is WITHOUT ROWID
                        // or a VIEW (neither has a rowid), error.
                        for (tid, (_, table_schema)) in &self.schema.table_schemas {
                            if table_schema.without_rowid || table_schema.is_view {
                                return Err(ExecutorError::ColumnNotFound {
                                    column_name: column_str.to_string(),
                                    table_name: tid.display().to_string(),
                                    searched_tables: self.schema.table_names(),
                                    available_columns: table_schema
                                        .columns
                                        .iter()
                                        .map(|c| c.name.clone())
                                        .collect(),
                                });
                            }
                        }
                    }

                    // Issue #4536: an INTEGER PRIMARY KEY column is an alias for
                    // rowid; return that column's value.
                    if let Some(ref table_id) = table_id {
                        if let Some((start_idx, table_schema)) =
                            self.schema.table_schemas.get(table_id)
                        {
                            if let Some(ipk_col_idx) = table_schema.rowid_alias_column {
                                let combined_idx = start_idx + ipk_col_idx;
                                return row.get(combined_idx).cloned().ok_or(
                                    ExecutorError::ColumnIndexOutOfBounds { index: combined_idx },
                                );
                            }
                        }
                    } else {
                        for (start_idx, table_schema) in self.schema.table_schemas.values() {
                            if let Some(ipk_col_idx) = table_schema.rowid_alias_column {
                                let combined_idx = start_idx + ipk_col_idx;
                                return row.get(combined_idx).cloned().ok_or(
                                    ExecutorError::ColumnIndexOutOfBounds { index: combined_idx },
                                );
                            }
                        }
                    }

                    // No IPK alias - fall back to tracked row-id (handles single-table
                    // and multi-table/JOIN rows, #4370).
                    if let Some(row_id) = row.get_row_id_for_table(table_str) {
                        return Ok(SqlValue::Bigint(row_id as i64));
                    }
                    // ROWID not available (e.g. a TVF-derived FROM item) - return NULL,
                    // matching SQLite's behavior for derived tables.
                    return Ok(SqlValue::Null);
                }

                Err(ExecutorError::ColumnNotFound {
                    column_name: column_str.to_string(),
                    table_name: table_str
                        .map(|t| t.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    searched_tables: self.schema.table_names(),
                    available_columns: self.get_available_columns(),
                })
            }

            // Binary operation
            ArenaExpression::BinaryOp { op, left, right } => {
                self.eval_binary_op(*op, left, right, row)
            }

            // Unary operation
            ArenaExpression::UnaryOp { op, expr: inner } => {
                let val = self.eval_with_depth(inner, row)?;
                super::expressions::operators::eval_unary_op(op, &val)
            }

            // IS NULL / IS NOT NULL
            ArenaExpression::IsNull { expr: inner, negated } => {
                let val = self.eval_with_depth(inner, row)?;
                let is_null = matches!(val, SqlValue::Null);
                Ok(SqlValue::Boolean(if *negated { !is_null } else { is_null }))
            }

            // IS DISTINCT FROM / IS NOT DISTINCT FROM
            ArenaExpression::IsDistinctFrom { left, right, negated } => {
                let left_val = self.eval_with_depth(left, row)?;
                let right_val = self.eval_with_depth(right, row)?;
                let is_distinct = super::core::values_are_distinct(&left_val, &right_val);
                Ok(SqlValue::Boolean(if *negated { !is_distinct } else { is_distinct }))
            }

            // IS TRUE / IS FALSE / IS UNKNOWN
            // SQLite compatibility: integers are treated as booleans (0=FALSE, non-zero=TRUE)
            ArenaExpression::IsTruthValue { expr, truth_value, negated } => {
                let val = self.eval_with_depth(expr, row)?;
                // SQLite compatibility: coerce any value to a boolean via the
                // shared truthiness rule (numeric prefix != 0), covering TEXT/BLOB
                // (`'3' IS TRUE` -> 1, `'abc' IS TRUE` -> 0). NULL is UNKNOWN.
                // See crate::evaluator::operators::is_truthy.
                let is_null = matches!(val, SqlValue::Null);
                let result = match truth_value {
                    vibesql_ast::arena::TruthValue::True => {
                        !is_null && crate::evaluator::operators::is_truthy(&val)
                    }
                    vibesql_ast::arena::TruthValue::False => {
                        !is_null && !crate::evaluator::operators::is_truthy(&val)
                    }
                    vibesql_ast::arena::TruthValue::Unknown => is_null,
                };
                Ok(SqlValue::Boolean(if *negated { !result } else { result }))
            }

            // Wildcard (*)
            ArenaExpression::Wildcard => Ok(SqlValue::Null),

            // Current date/time functions - use scalar function path
            ArenaExpression::CurrentDate => super::functions::eval_scalar_function(
                "CURRENT_DATE",
                &[],
                &None,
                &self.sql_mode,
                super::SchemaExprContext::None,
            ),
            ArenaExpression::CurrentTime { .. } => super::functions::eval_scalar_function(
                "CURRENT_TIME",
                &[],
                &None,
                &self.sql_mode,
                super::SchemaExprContext::None,
            ),
            ArenaExpression::CurrentTimestamp { .. } => super::functions::eval_scalar_function(
                "CURRENT_TIMESTAMP",
                &[],
                &None,
                &self.sql_mode,
                super::SchemaExprContext::None,
            ),

            // DEFAULT keyword
            ArenaExpression::Default => Err(ExecutorError::UnsupportedExpression(
                "DEFAULT keyword is only valid in INSERT VALUES and UPDATE SET clauses".to_string(),
            )),

            // Conjunction and Disjunction - evaluate children
            ArenaExpression::Conjunction(children) => {
                let mut result = SqlValue::Boolean(true);
                for child in children.iter() {
                    let val = self.eval_with_depth(child, row)?;
                    match val {
                        SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                        SqlValue::Null => result = SqlValue::Null,
                        SqlValue::Boolean(true) => {}
                        _ => {
                            return Err(ExecutorError::TypeError(format!(
                                "Conjunction requires boolean operands, got {:?}",
                                val
                            )))
                        }
                    }
                }
                Ok(result)
            }

            ArenaExpression::Disjunction(children) => {
                let mut result = SqlValue::Boolean(false);
                for child in children.iter() {
                    let val = self.eval_with_depth(child, row)?;
                    match val {
                        SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
                        SqlValue::Null => result = SqlValue::Null,
                        SqlValue::Boolean(false) => {}
                        _ => {
                            return Err(ExecutorError::TypeError(format!(
                                "Disjunction requires boolean operands, got {:?}",
                                val
                            )))
                        }
                    }
                }
                Ok(result)
            }

            // Cold-path extended variants
            ArenaExpression::Extended(ext) => self.eval_extended(ext, row),
        }
    }

    /// Evaluate an extended expression (cold path variants).
    fn eval_extended(
        &self,
        ext: &ArenaExtendedExpr<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        match ext {
            // Function call
            ArenaExtendedExpr::Function { name, args, character_unit } => {
                let evaluated_args = args
                    .iter()
                    .map(|arg| self.eval_with_depth(arg, row))
                    .collect::<Result<Vec<SqlValue>, _>>()?;
                let name_str = self.resolve(*name);

                // JSON construction/mutation functions honor the JSON subtype:
                // an argument that is itself a JSON-producing function call must
                // embed as a sub-document rather than a quoted string. Compute
                // those per-argument flags from the arena AST and route to the
                // subtype-aware implementations. (Mirrors special.rs for the
                // non-arena evaluator.)
                let upper = name_str.to_uppercase();
                // subtype(X): runtime JSON subtype probe, computed structurally
                // from the argument expression + its evaluated value. Mirrors
                // crate::evaluator::json_subtype for the arena AST.
                if upper == "SUBTYPE" {
                    if args.len() != 1 {
                        return Err(ExecutorError::WrongNumberOfArguments {
                            function_name: "subtype".to_string(),
                        });
                    }
                    let is_json = self.arena_expr_json_subtype(&args[0], row)?;
                    return Ok(SqlValue::Integer(if is_json {
                        crate::evaluator::json_subtype::JSON_SUBTYPE_TAG
                    } else {
                        0
                    }));
                }
                if matches!(
                    upper.as_str(),
                    "JSON_ARRAY"
                        | "JSON_OBJECT"
                        | "JSON_INSERT"
                        | "JSON_REPLACE"
                        | "JSON_SET"
                        | "JSONB_ARRAY"
                        | "JSONB_OBJECT"
                        | "JSONB_INSERT"
                        | "JSONB_REPLACE"
                        | "JSONB_SET"
                ) {
                    use super::functions::sqlite_compat::json_funcs;
                    // AST-derived subtype OR a runtime container marker on an
                    // eligible (non-string-column) argument (issue #6007).
                    let mut subtypes: Vec<bool> = Vec::with_capacity(args.len());
                    for (i, a) in args.iter().enumerate() {
                        let is_json = self.arena_expr_has_json_subtype(a)
                            || (self.arena_expr_runtime_json_subtype_eligible(a)
                                && json_funcs::sql_value_is_json_subtyped(&evaluated_args[i]));
                        subtypes.push(is_json);
                    }
                    // `json_*` emit JSON text; `jsonb_*` emit a JSONB Blob
                    // (Stage 1 of #6008). Same node + subtype handling; only the
                    // output encoding differs.
                    return match upper.as_str() {
                        "JSON_ARRAY" => json_funcs::json_array(&evaluated_args, &subtypes),
                        "JSONB_ARRAY" => json_funcs::jsonb_array(&evaluated_args, &subtypes),
                        "JSON_OBJECT" => json_funcs::json_object(&evaluated_args, &subtypes),
                        "JSONB_OBJECT" => json_funcs::jsonb_object(&evaluated_args, &subtypes),
                        "JSON_INSERT" => json_funcs::json_insert(&evaluated_args, &subtypes),
                        "JSONB_INSERT" => json_funcs::jsonb_insert(&evaluated_args, &subtypes),
                        "JSON_REPLACE" => json_funcs::json_replace(&evaluated_args, &subtypes),
                        "JSONB_REPLACE" => json_funcs::jsonb_replace(&evaluated_args, &subtypes),
                        "JSON_SET" => json_funcs::json_set(&evaluated_args, &subtypes),
                        "JSONB_SET" => json_funcs::jsonb_set(&evaluated_args, &subtypes),
                        _ => unreachable!(),
                    };
                }

                let char_unit = character_unit.as_ref().map(|cu| match cu {
                    arena_ast::CharacterUnit::Characters => vibesql_ast::CharacterUnit::Characters,
                    arena_ast::CharacterUnit::Octets => vibesql_ast::CharacterUnit::Octets,
                });
                super::functions::eval_scalar_function(
                    name_str,
                    &evaluated_args,
                    &char_unit,
                    &self.sql_mode,
                    super::SchemaExprContext::None,
                )
            }

            // Aggregate function - should be pre-computed
            ArenaExtendedExpr::AggregateFunction { name, .. } => {
                Err(ExecutorError::UnsupportedExpression(format!(
                    "Aggregate function '{}' must be pre-computed before arena evaluation",
                    self.resolve(*name)
                )))
            }

            // CASE expression
            ArenaExtendedExpr::Case { operand, when_clauses, else_result } => {
                self.eval_case(operand.as_deref(), when_clauses, else_result.as_deref(), row)
            }

            // BETWEEN predicate
            ArenaExtendedExpr::Between { expr: inner, low, high, negated, symmetric } => {
                let val = self.eval_with_depth(inner, row)?;
                let low_val = self.eval_with_depth(low, row)?;
                let high_val = self.eval_with_depth(high, row)?;
                super::core::eval_between_static(
                    &val,
                    &low_val,
                    &high_val,
                    *negated,
                    *symmetric,
                    self.sql_mode.clone(),
                )
            }

            // IN list
            ArenaExtendedExpr::InList { expr: inner, values, negated } => {
                let val = self.eval_with_depth(inner, row)?;
                if matches!(val, SqlValue::Null) {
                    return Ok(SqlValue::Null);
                }
                let mut found = false;
                let mut has_null = false;
                for list_val in values.iter() {
                    let v = self.eval_with_depth(list_val, row)?;
                    if matches!(v, SqlValue::Null) {
                        has_null = true;
                        continue;
                    }
                    let eq = super::core::eval_binary_op_static(
                        &val,
                        &vibesql_ast::BinaryOperator::Equal,
                        &v,
                        self.sql_mode.clone(),
                    )?;
                    if matches!(eq, SqlValue::Boolean(true)) {
                        found = true;
                        break;
                    }
                }
                if found {
                    Ok(SqlValue::Boolean(!*negated))
                } else if has_null {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(*negated))
                }
            }

            // LIKE pattern matching
            ArenaExtendedExpr::Like { expr: inner, pattern, negated, escape } => {
                let val = self.eval_with_depth(inner, row)?;
                let pattern_val = self.eval_with_depth(pattern, row)?;
                let escape_char = if let Some(escape_expr) = escape {
                    match self.eval_with_depth(escape_expr, row)? {
                        SqlValue::Varchar(s) | SqlValue::Character(s) => {
                            let mut chars = s.chars();
                            match (chars.next(), chars.next()) {
                                (Some(c), None) => Some(c), // Exactly one character
                                _ => {
                                    // Empty string or multi-character string: error per SQLite
                                    return Err(ExecutorError::SqliteCompatError(
                                        "ESCAPE expression must be a single character".to_string(),
                                    ));
                                }
                            }
                        }
                        SqlValue::Null => return Ok(SqlValue::Null),
                        _ => {
                            return Err(ExecutorError::SqliteCompatError(
                                "ESCAPE expression must be a single character".to_string(),
                            ))
                        }
                    }
                } else {
                    None
                };
                self.eval_like(&val, &pattern_val, *negated, escape_char)
            }

            // GLOB pattern matching (SQLite)
            ArenaExtendedExpr::Glob { expr: inner, pattern, negated, .. } => {
                let val = self.eval_with_depth(inner, row)?;
                let pattern_val = self.eval_with_depth(pattern, row)?;
                self.eval_glob(&val, &pattern_val, *negated)
            }

            // CAST expression - delegate to casting module
            ArenaExtendedExpr::Cast { expr: inner, data_type } => {
                let val = self.eval_with_depth(inner, row)?;
                super::casting::cast_value(&val, data_type, &self.sql_mode)
            }

            // Subqueries - not supported without conversion to owned types
            ArenaExtendedExpr::ScalarSubquery(_)
            | ArenaExtendedExpr::In { .. }
            | ArenaExtendedExpr::Exists { .. }
            | ArenaExtendedExpr::QuantifiedComparison { .. } => {
                Err(ExecutorError::UnsupportedExpression(
                    "Subqueries in arena expressions require conversion to owned types".to_string(),
                ))
            }

            // Window function - should be pre-computed
            ArenaExtendedExpr::WindowFunction { .. } => Err(ExecutorError::UnsupportedExpression(
                "Window functions must be pre-computed before arena evaluation".to_string(),
            )),

            // POSITION function
            ArenaExtendedExpr::Position { substring, string, .. } => {
                let substr = self.eval_with_depth(substring, row)?;
                let s = self.eval_with_depth(string, row)?;
                self.eval_position(&substr, &s)
            }

            // TRIM function
            ArenaExtendedExpr::Trim { position, removal_char, string } => {
                let s = self.eval_with_depth(string, row)?;
                let remove = match removal_char {
                    Some(expr) => Some(self.eval_with_depth(expr, row)?),
                    None => None,
                };
                self.eval_trim(*position, remove.as_ref(), &s)
            }

            // EXTRACT function - simplified implementation
            ArenaExtendedExpr::Extract { field, expr: inner } => {
                let val = self.eval_with_depth(inner, row)?;
                self.eval_extract(*field, &val)
            }

            // INTERVAL expression - simplified implementation
            ArenaExtendedExpr::Interval { value, .. } => {
                // For now, just evaluate the value expression
                self.eval_with_depth(value, row)
            }

            // Pseudo-variables, session variables, etc. - not supported
            ArenaExtendedExpr::PseudoVariable { .. }
            | ArenaExtendedExpr::SessionVariable { .. }
            | ArenaExtendedExpr::DuplicateKeyValue { .. }
            | ArenaExtendedExpr::NextValue { .. }
            | ArenaExtendedExpr::MatchAgainst { .. }
            | ArenaExtendedExpr::RowValueConstructor { .. } => {
                Err(ExecutorError::UnsupportedExpression(
                    "Advanced expression types not supported in arena evaluator".to_string(),
                ))
            }
        }
    }

    /// Evaluate with depth tracking.
    fn eval_with_depth(
        &self,
        expr: &ArenaExpression<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        // Create a new evaluator with incremented depth
        let child = ArenaExpressionEvaluator {
            schema: self.schema,
            params: self.params,
            database: self.database,
            sql_mode: self.sql_mode.clone(),
            column_cache: RefCell::new(HashMap::new()), // Don't share cache across depth
            depth: self.depth + 1,
            interner: self.interner,
        };
        child.eval(expr, row)
    }

    /// Evaluate a binary operation with short-circuit semantics.
    fn eval_binary_op(
        &self,
        op: vibesql_ast::BinaryOperator,
        left: &ArenaExpression<'arena>,
        right: &ArenaExpression<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        use vibesql_ast::BinaryOperator;

        // Short-circuit evaluation for AND/OR
        match op {
            BinaryOperator::And => {
                let left_val = self.eval_with_depth(left, row)?;
                // Short-circuit: if left is false, return false immediately
                if matches!(left_val, SqlValue::Boolean(false)) {
                    return Ok(SqlValue::Boolean(false));
                }
                let right_val = self.eval_with_depth(right, row)?;
                // NULL AND FALSE = FALSE
                if matches!(left_val, SqlValue::Null)
                    && matches!(right_val, SqlValue::Boolean(false))
                {
                    return Ok(SqlValue::Boolean(false));
                }
                super::core::eval_binary_op_static(
                    &left_val,
                    &op,
                    &right_val,
                    self.sql_mode.clone(),
                )
            }
            BinaryOperator::Or => {
                let left_val = self.eval_with_depth(left, row)?;
                // Short-circuit: if left is true, return true immediately
                if matches!(left_val, SqlValue::Boolean(true)) {
                    return Ok(SqlValue::Boolean(true));
                }
                let right_val = self.eval_with_depth(right, row)?;
                // NULL OR TRUE = TRUE
                if matches!(left_val, SqlValue::Null)
                    && matches!(right_val, SqlValue::Boolean(true))
                {
                    return Ok(SqlValue::Boolean(true));
                }
                super::core::eval_binary_op_static(
                    &left_val,
                    &op,
                    &right_val,
                    self.sql_mode.clone(),
                )
            }
            _ => {
                // Non-short-circuit: evaluate both sides
                let left_val = self.eval_with_depth(left, row)?;
                let right_val = self.eval_with_depth(right, row)?;
                super::core::eval_binary_op_static(
                    &left_val,
                    &op,
                    &right_val,
                    self.sql_mode.clone(),
                )
            }
        }
    }

    /// Evaluate a CASE expression.
    fn eval_case(
        &self,
        operand: Option<&ArenaExpression<'arena>>,
        when_clauses: &bumpalo::collections::Vec<'arena, arena_ast::CaseWhen<'arena>>,
        else_result: Option<&ArenaExpression<'arena>>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        // Simple CASE: CASE operand WHEN value THEN result ...
        if let Some(op_expr) = operand {
            let op_val = self.eval_with_depth(op_expr, row)?;
            for when_clause in when_clauses.iter() {
                for condition in when_clause.conditions.iter() {
                    let cond_val = self.eval_with_depth(condition, row)?;
                    if super::core::values_are_equal(&op_val, &cond_val) {
                        return self.eval_with_depth(&when_clause.result, row);
                    }
                }
            }
        } else {
            // Searched CASE: CASE WHEN condition THEN result ...
            for when_clause in when_clauses.iter() {
                for condition in when_clause.conditions.iter() {
                    let cond_val = self.eval_with_depth(condition, row)?;
                    // In SQL, truthiness is:
                    // - Boolean(true) => true
                    // - Integer/Bigint non-zero => true
                    // - Double/Float non-zero => true
                    // - Everything else (Null, zero, strings) => false
                    let is_truthy = match cond_val {
                        SqlValue::Boolean(b) => b,
                        SqlValue::Integer(n) => n != 0,
                        SqlValue::Bigint(n) => n != 0,
                        SqlValue::Double(n) => n != 0.0,
                        SqlValue::Float(n) => n != 0.0,
                        _ => false,
                    };
                    if is_truthy {
                        return self.eval_with_depth(&when_clause.result, row);
                    }
                }
            }
        }

        // No match - return ELSE or NULL
        match else_result {
            Some(else_expr) => self.eval_with_depth(else_expr, row),
            None => Ok(SqlValue::Null),
        }
    }

    /// Evaluate LIKE pattern matching.
    fn eval_like(
        &self,
        value: &SqlValue,
        pattern: &SqlValue,
        negated: bool,
        escape_char: Option<char>,
    ) -> Result<SqlValue, ExecutorError> {
        // Get case_sensitive_like setting from database (default: false = case-insensitive)
        let case_sensitive = self.database.map(|db| db.case_sensitive_like()).unwrap_or(false);

        // Issue #4913: SQLite coerces numeric types to strings for LIKE comparison
        let text = match value {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
            SqlValue::Integer(i) => arcstr::ArcStr::from(i.to_string()),
            SqlValue::Bigint(i) => arcstr::ArcStr::from(i.to_string()),
            SqlValue::Smallint(i) => arcstr::ArcStr::from(i.to_string()),
            SqlValue::Unsigned(u) => arcstr::ArcStr::from(u.to_string()),
            // Render floats through the SqlValue Display impl (SQLite %!.15g
            // scientific rendering), not raw f64/f32 `to_string()` (fixed-point).
            // Display already matches SQLite 3.51; this keeps stored/CTE REAL
            // values consistent with inline literals for LIKE/GLOB (fixes atof-3.1).
            f @ (SqlValue::Float(_)
            | SqlValue::Double(_)
            | SqlValue::Real(_)
            | SqlValue::Numeric(_)) => arcstr::ArcStr::from(f.to_string()),
            SqlValue::Boolean(b) => arcstr::ArcStr::from(if *b { "1" } else { "0" }),
            SqlValue::Blob(b) => arcstr::ArcStr::from(String::from_utf8_lossy(b).into_owned()),
            _ => {
                return Err(ExecutorError::TypeError(format!(
                    "LIKE requires string operands, got {:?} and {:?}",
                    value, pattern
                )))
            }
        };

        let pattern_str = match pattern {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
            SqlValue::Integer(i) => arcstr::ArcStr::from(i.to_string()),
            SqlValue::Bigint(i) => arcstr::ArcStr::from(i.to_string()),
            SqlValue::Smallint(i) => arcstr::ArcStr::from(i.to_string()),
            SqlValue::Unsigned(u) => arcstr::ArcStr::from(u.to_string()),
            // Render floats through the SqlValue Display impl (SQLite %!.15g
            // scientific rendering), not raw f64/f32 `to_string()` (fixed-point).
            // Display already matches SQLite 3.51; this keeps stored/CTE REAL
            // values consistent with inline literals for LIKE/GLOB (fixes atof-3.1).
            f @ (SqlValue::Float(_)
            | SqlValue::Double(_)
            | SqlValue::Real(_)
            | SqlValue::Numeric(_)) => arcstr::ArcStr::from(f.to_string()),
            SqlValue::Boolean(b) => arcstr::ArcStr::from(if *b { "1" } else { "0" }),
            SqlValue::Blob(b) => arcstr::ArcStr::from(String::from_utf8_lossy(b).into_owned()),
            _ => {
                return Err(ExecutorError::TypeError(format!(
                    "LIKE requires string operands, got {:?} and {:?}",
                    value, pattern
                )))
            }
        };

        let matches = super::pattern::like_match(&text, &pattern_str, case_sensitive, escape_char);
        Ok(SqlValue::Boolean(if negated { !matches } else { matches }))
    }

    /// Evaluate GLOB pattern matching (SQLite).
    fn eval_glob(
        &self,
        value: &SqlValue,
        pattern: &SqlValue,
        negated: bool,
    ) -> Result<SqlValue, ExecutorError> {
        // Issue #4913: SQLite coerces numeric types to strings for GLOB comparison
        let text = match value {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
            SqlValue::Integer(i) => arcstr::ArcStr::from(i.to_string()),
            SqlValue::Bigint(i) => arcstr::ArcStr::from(i.to_string()),
            SqlValue::Smallint(i) => arcstr::ArcStr::from(i.to_string()),
            SqlValue::Unsigned(u) => arcstr::ArcStr::from(u.to_string()),
            // Render floats through the SqlValue Display impl (SQLite %!.15g
            // scientific rendering), not raw f64/f32 `to_string()` (fixed-point).
            // Display already matches SQLite 3.51; this keeps stored/CTE REAL
            // values consistent with inline literals for LIKE/GLOB (fixes atof-3.1).
            f @ (SqlValue::Float(_)
            | SqlValue::Double(_)
            | SqlValue::Real(_)
            | SqlValue::Numeric(_)) => arcstr::ArcStr::from(f.to_string()),
            SqlValue::Boolean(b) => arcstr::ArcStr::from(if *b { "1" } else { "0" }),
            SqlValue::Blob(b) => arcstr::ArcStr::from(String::from_utf8_lossy(b).into_owned()),
            _ => {
                return Err(ExecutorError::TypeError(format!(
                    "GLOB requires string operands, got {:?} and {:?}",
                    value, pattern
                )))
            }
        };

        let pattern_str = match pattern {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
            SqlValue::Integer(i) => arcstr::ArcStr::from(i.to_string()),
            SqlValue::Bigint(i) => arcstr::ArcStr::from(i.to_string()),
            SqlValue::Smallint(i) => arcstr::ArcStr::from(i.to_string()),
            SqlValue::Unsigned(u) => arcstr::ArcStr::from(u.to_string()),
            // Render floats through the SqlValue Display impl (SQLite %!.15g
            // scientific rendering), not raw f64/f32 `to_string()` (fixed-point).
            // Display already matches SQLite 3.51; this keeps stored/CTE REAL
            // values consistent with inline literals for LIKE/GLOB (fixes atof-3.1).
            f @ (SqlValue::Float(_)
            | SqlValue::Double(_)
            | SqlValue::Real(_)
            | SqlValue::Numeric(_)) => arcstr::ArcStr::from(f.to_string()),
            SqlValue::Boolean(b) => arcstr::ArcStr::from(if *b { "1" } else { "0" }),
            SqlValue::Blob(b) => arcstr::ArcStr::from(String::from_utf8_lossy(b).into_owned()),
            _ => {
                return Err(ExecutorError::TypeError(format!(
                    "GLOB requires string operands, got {:?} and {:?}",
                    value, pattern
                )))
            }
        };

        let matches = super::pattern::glob_match(&text, &pattern_str);
        Ok(SqlValue::Boolean(if negated { !matches } else { matches }))
    }

    /// Evaluate POSITION function.
    fn eval_position(
        &self,
        substring: &SqlValue,
        string: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        match (substring, string) {
            (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
            (SqlValue::Varchar(sub), SqlValue::Varchar(s))
            | (SqlValue::Character(sub), SqlValue::Varchar(s))
            | (SqlValue::Varchar(sub), SqlValue::Character(s))
            | (SqlValue::Character(sub), SqlValue::Character(s)) => {
                let pos = s.find(&**sub).map(|i| i + 1).unwrap_or(0);
                Ok(SqlValue::Integer(pos as i64))
            }
            _ => Err(ExecutorError::TypeError(format!(
                "POSITION requires string operands, got {:?}",
                substring
            ))),
        }
    }

    /// Evaluate TRIM function.
    fn eval_trim(
        &self,
        position: Option<arena_ast::TrimPosition>,
        removal_char: Option<&SqlValue>,
        string: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        match string {
            SqlValue::Null => Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                let remove_chars: &str = match removal_char {
                    Some(SqlValue::Varchar(r)) | Some(SqlValue::Character(r)) => r,
                    Some(SqlValue::Null) => return Ok(SqlValue::Null),
                    None => " ",
                    _ => {
                        return Err(ExecutorError::TypeError(format!(
                            "TRIM removal character must be string, got {:?}",
                            removal_char
                        )))
                    }
                };
                let result = match position {
                    Some(arena_ast::TrimPosition::Leading) => {
                        s.trim_start_matches(|c| remove_chars.contains(c))
                    }
                    Some(arena_ast::TrimPosition::Trailing) => {
                        s.trim_end_matches(|c| remove_chars.contains(c))
                    }
                    Some(arena_ast::TrimPosition::Both) | None => {
                        s.trim_matches(|c| remove_chars.contains(c))
                    }
                };
                Ok(SqlValue::Varchar(arcstr::ArcStr::from(result)))
            }
            _ => Err(ExecutorError::TypeError(format!(
                "TRIM requires string operand, got {:?}",
                string
            ))),
        }
    }

    /// Evaluate EXTRACT function.
    fn eval_extract(
        &self,
        field: arena_ast::IntervalUnit,
        value: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        use arena_ast::IntervalUnit;

        match value {
            SqlValue::Null => Ok(SqlValue::Null),
            SqlValue::Date(d) => {
                let result: i64 = match field {
                    IntervalUnit::Year => d.year as i64,
                    IntervalUnit::Month => d.month as i64,
                    IntervalUnit::Day => d.day as i64,
                    IntervalUnit::Quarter => (d.month as i64 - 1) / 3 + 1,
                    _ => {
                        return Err(ExecutorError::UnsupportedExpression(format!(
                            "EXTRACT {:?} from DATE not supported",
                            field
                        )))
                    }
                };
                Ok(SqlValue::Integer(result))
            }
            SqlValue::Time(t) => {
                let result: i64 = match field {
                    IntervalUnit::Hour => t.hour as i64,
                    IntervalUnit::Minute => t.minute as i64,
                    IntervalUnit::Second => t.second as i64,
                    _ => {
                        return Err(ExecutorError::UnsupportedExpression(format!(
                            "EXTRACT {:?} from TIME not supported",
                            field
                        )))
                    }
                };
                Ok(SqlValue::Integer(result))
            }
            SqlValue::Timestamp(ts) => {
                let result: i64 = match field {
                    IntervalUnit::Year => ts.date.year as i64,
                    IntervalUnit::Month => ts.date.month as i64,
                    IntervalUnit::Day => ts.date.day as i64,
                    IntervalUnit::Hour => ts.time.hour as i64,
                    IntervalUnit::Minute => ts.time.minute as i64,
                    IntervalUnit::Second => ts.time.second as i64,
                    IntervalUnit::Quarter => (ts.date.month as i64 - 1) / 3 + 1,
                    _ => {
                        return Err(ExecutorError::UnsupportedExpression(format!(
                            "EXTRACT {:?} from TIMESTAMP not supported",
                            field
                        )))
                    }
                };
                Ok(SqlValue::Integer(result))
            }
            _ => Err(ExecutorError::TypeError(format!(
                "EXTRACT requires date/time/timestamp operand, got {:?}",
                value
            ))),
        }
    }

    /// Get column index with caching.
    fn get_column_index_cached(&self, table: Option<&str>, column: &str) -> Option<usize> {
        // Compute hash for cache key
        let mut hasher = AHasher::default();
        table.hash(&mut hasher);
        column.hash(&mut hasher);
        let key = hasher.finish();

        // Check cache
        if let Some(&idx) = self.column_cache.borrow().get(&key) {
            return Some(idx);
        }

        // Look up in schema
        if let Some(idx) = self.schema.get_column_index(table, column) {
            self.column_cache.borrow_mut().insert(key, idx);
            return Some(idx);
        }

        None
    }

    /// Get available columns for error messages.
    fn get_available_columns(&self) -> Vec<String> {
        let mut columns = Vec::new();
        for (_start, schema) in self.schema.table_schemas.values() {
            columns.extend(schema.columns.iter().map(|c| c.name.clone()));
        }
        columns
    }
}

#[cfg(test)]
mod tests {
    use bumpalo::Bump;
    use vibesql_ast::arena::ArenaInterner;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use super::*;

    fn make_schema() -> CombinedSchema {
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(255) },
                true,
            ),
        ];
        let table_schema = TableSchema::new("test".to_string(), columns);
        CombinedSchema::from_table("test".to_string(), table_schema)
    }

    #[test]
    fn test_eval_literal() {
        let arena = Bump::new();
        let interner = ArenaInterner::new(&arena);
        let schema = make_schema();
        let params = vec![];
        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);
        let row =
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);

        let expr = ArenaExpression::Literal(SqlValue::Integer(42));
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_eval_placeholder() {
        let arena = Bump::new();
        let interner = ArenaInterner::new(&arena);
        let schema = make_schema();
        let params = vec![SqlValue::Integer(100), SqlValue::Varchar(arcstr::ArcStr::from("test"))];
        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);
        let row =
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);

        // First placeholder (index 0)
        let expr = ArenaExpression::Placeholder(0);
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(100));

        // Second placeholder (index 1)
        let expr = ArenaExpression::Placeholder(1);
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("test")));
    }

    #[test]
    fn test_eval_column_ref() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);
        let schema = make_schema();
        let params = vec![];

        // Intern the column names (uppercased to match schema lookup)
        let id_sym = interner.intern("ID");
        let name_sym = interner.intern("NAME");

        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);
        let row =
            Row::new(vec![SqlValue::Integer(42), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]);

        let expr = ArenaExpression::ColumnRef {
            schema: None,
            table: None,
            column: id_sym,
            schema_quoted: false,
            table_quoted: false,
            column_quoted: false,
        };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));

        let expr = ArenaExpression::ColumnRef {
            schema: None,
            table: None,
            column: name_sym,
            schema_quoted: false,
            table_quoted: false,
            column_quoted: false,
        };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
    }

    #[test]
    fn test_eval_is_null() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);
        let schema = make_schema();
        let params = vec![];

        // Intern column names (uppercased to match schema lookup)
        let name_sym = interner.intern("NAME");
        let id_sym = interner.intern("ID");

        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);
        let row = Row::new(vec![SqlValue::Integer(1), SqlValue::Null]);

        let expr = ArenaExpression::IsNull {
            expr: arena.alloc(ArenaExpression::ColumnRef {
                schema: None,
                table: None,
                column: name_sym,
                schema_quoted: false,
                table_quoted: false,
                column_quoted: false,
            }),
            negated: false,
        };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));

        let expr = ArenaExpression::IsNull {
            expr: arena.alloc(ArenaExpression::ColumnRef {
                schema: None,
                table: None,
                column: id_sym,
                schema_quoted: false,
                table_quoted: false,
                column_quoted: false,
            }),
            negated: false,
        };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Boolean(false));
    }
}
