//! Special expression forms (CASE, Function calls)

use super::super::{core::ExpressionEvaluator, functions::eval_scalar_function};
use crate::errors::ExecutorError;

impl ExpressionEvaluator<'_> {
    /// Evaluate CASE expression
    pub(super) fn eval_case(
        &self,
        operand: &Option<Box<vibesql_ast::Expression>>,
        when_clauses: &[vibesql_ast::CaseWhen],
        else_result: &Option<Box<vibesql_ast::Expression>>,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        match operand {
            // Simple CASE: CASE operand WHEN value THEN result ...
            Some(operand_expr) => {
                // Row-value operand or WHEN values (e.g.
                // `CASE (2,2) WHEN (1,1) THEN ... END`): compare with row-value
                // equality by delegating to the binary `=` dispatch, which
                // handles tuple-vs-tuple and tuple-vs-subquery forms.
                let row_value_case = matches!(
                    operand_expr.as_ref(),
                    vibesql_ast::Expression::RowValueConstructor(elems) if elems.len() > 1
                ) || when_clauses.iter().any(|wc| {
                    wc.conditions.iter().any(|c| {
                        matches!(
                            c,
                            vibesql_ast::Expression::RowValueConstructor(elems) if elems.len() > 1
                        )
                    })
                });
                if row_value_case {
                    for when_clause in when_clauses {
                        for condition_expr in &when_clause.conditions {
                            let eq_expr = vibesql_ast::Expression::BinaryOp {
                                left: operand_expr.clone(),
                                op: vibesql_ast::BinaryOperator::Equal,
                                right: Box::new(condition_expr.clone()),
                            };
                            if matches!(
                                self.eval(&eq_expr, row)?,
                                vibesql_types::SqlValue::Boolean(true)
                            ) {
                                return self.eval(&when_clause.result, row);
                            }
                        }
                    }
                    return match else_result {
                        Some(else_expr) => self.eval(else_expr, row),
                        None => Ok(vibesql_types::SqlValue::Null),
                    };
                }

                let operand_value = self.eval(operand_expr, row)?;

                for when_clause in when_clauses {
                    // Check if ANY condition matches (OR logic)
                    for condition_expr in &when_clause.conditions {
                        let when_value = self.eval(condition_expr, row)?;

                        // Affinity-aware equality (not the permissive
                        // `values_are_equal` used for hash-join keys): a bare
                        // literal operand carries no affinity, so `CASE 55
                        // WHEN '55' THEN ...` must not match (e_expr-23.1.6).
                        if self.affinity_aware_equal(
                            operand_expr,
                            operand_value.clone(),
                            condition_expr,
                            when_value,
                        )? {
                            return self.eval(&when_clause.result, row);
                        }
                    }
                }
            }

            // Searched CASE: CASE WHEN condition THEN result ...
            None => {
                for when_clause in when_clauses {
                    // Check if ANY condition is TRUE (OR logic)
                    for condition_expr in &when_clause.conditions {
                        let condition_result = self.eval(condition_expr, row)?;

                        // Delegate to the shared SQLite truthiness helper
                        // (numerics non-zero, strings/blobs via the
                        // leading-numeric parse, NULL falsy). (#5856)
                        let is_truthy = super::super::operators::is_truthy(&condition_result);
                        if is_truthy {
                            return self.eval(&when_clause.result, row);
                        }
                    }
                }
            }
        }

        match else_result {
            Some(else_expr) => self.eval(else_expr, row),
            None => Ok(vibesql_types::SqlValue::Null),
        }
    }

    /// Evaluate COALESCE function with lazy evaluation
    /// COALESCE(val1, val2, ...) - returns first non-NULL value
    /// This uses lazy evaluation to short-circuit on first non-NULL value,
    /// avoiding evaluation of expensive expressions.
    pub(super) fn eval_coalesce_lazy(
        &self,
        args: &[vibesql_ast::Expression],
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // SQLite requires coalesce to have at least 2 arguments
        if args.len() < 2 {
            return Err(ExecutorError::WrongNumberOfArguments {
                function_name: "coalesce".to_string(),
            });
        }

        // Lazy evaluation: return first non-NULL value without evaluating remaining args
        for arg in args {
            let val = self.eval(arg, row)?;
            if !matches!(val, vibesql_types::SqlValue::Null) {
                return Ok(val);
            }
        }

        // All arguments were NULL
        Ok(vibesql_types::SqlValue::Null)
    }

    /// Evaluate NULLIF function with lazy evaluation
    /// NULLIF(val1, val2) - returns NULL if val1 = val2, otherwise val1
    /// This uses lazy evaluation to avoid unnecessary comparisons.
    pub(super) fn eval_nullif_lazy(
        &self,
        args: &[vibesql_ast::Expression],
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        if args.len() != 2 {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "NULLIF requires exactly 2 arguments, got {}",
                args.len()
            )));
        }

        // Evaluate first argument (required)
        let val1 = self.eval(&args[0], row)?;

        // If first is NULL, return NULL immediately without evaluating second
        if matches!(val1, vibesql_types::SqlValue::Null) {
            return Ok(val1);
        }

        // Evaluate second argument
        let val2 = self.eval(&args[1], row)?;

        // If either is NULL, comparison is undefined - return val1
        if matches!(val2, vibesql_types::SqlValue::Null) {
            return Ok(val1);
        }

        // NULLIF never applies column affinity, unlike `=`/CASE/IS: SQLite
        // implements it as a plain scalar function (func.c `nullifFunc`) that
        // compares the two already-evaluated `sqlite3_value`s directly via
        // `sqlite3MemCompare` with no affinity conversion, even when an
        // argument is a real affinity-carrying column reference. So a TEXT
        // column holding '1' is NOT NULLIF-equal to the integer literal 1
        // even though `x = 1` is TRUE for that same column (verified against
        // SQLite: `CREATE TABLE t(x TEXT); INSERT INTO t VALUES(1); SELECT
        // NULLIF(x, 1) FROM t` -> '1', not NULL). Use the strict (no
        // cross-type guessing) comparator directly — not
        // `affinity_aware_equal` (which is for `=`/CASE/IS) and not the
        // permissive `values_are_equal` used for hash-join keys.
        if matches!(
            self.eval_binary_op(&val1, &vibesql_ast::BinaryOperator::Equal, &val2)?,
            vibesql_types::SqlValue::Boolean(true)
        ) {
            Ok(vibesql_types::SqlValue::Null)
        } else {
            Ok(val1)
        }
    }

    /// Evaluate a JSON construction/mutation function that honors the JSON
    /// subtype (json_array, json_object, json_insert, json_replace, json_set).
    ///
    /// For each argument we evaluate its value and separately compute a subtype
    /// flag: `true` when the argument expression is a direct call to a JSON
    /// function whose output is always well-formed JSON. Subtype-flagged TEXT
    /// arguments embed as JSON sub-documents; everything else encodes as a fresh
    /// JSON scalar. See the module note in `json_funcs.rs`.
    fn eval_json_subtype_function(
        &self,
        name: &str,
        args: &[vibesql_ast::Expression],
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        use super::super::functions::sqlite_compat::json_funcs;
        use crate::evaluator::json_subtype::{
            data_type_is_string, expr_runtime_json_subtype_eligible,
        };

        // Resolve whether a bare column reference is declared with a real string
        // type (CHAR/VARCHAR). Such reads must never pick up the runtime JSON
        // subtype marker (issue #6007): a `CHAR(n)` column holding
        // container-shaped text quotes, while a dynamically-typed json_each /
        // json_tree `value` column stays eligible (json101-5.10).
        let column_is_declared_string = |_table: Option<&str>, column: &str| -> bool {
            self.schema
                .get_column(column)
                .map(|c| data_type_is_string(&c.data_type))
                .unwrap_or(false)
        };

        let mut values = Vec::with_capacity(args.len());
        let mut subtypes = Vec::with_capacity(args.len());
        for arg in args {
            let value = self.eval(arg, row)?;
            // AST-derived subtype OR a runtime container marker on an eligible
            // (non-string-column) argument.
            let is_json = expr_has_json_subtype(arg)
                || (expr_runtime_json_subtype_eligible(arg, &column_is_declared_string)
                    && json_funcs::sql_value_is_json_subtyped(&value));
            subtypes.push(is_json);
            values.push(value);
        }

        // The `json_*` names emit JSON text; the `jsonb_*` names emit SQLite's
        // binary JSONB representation as a Blob (Stage 1 of #6008). Both build
        // the same JSON node with identical subtype handling; only the output
        // encoding differs.
        match name.to_uppercase().as_str() {
            "JSON_ARRAY" => json_funcs::json_array(&values, &subtypes),
            "JSONB_ARRAY" => json_funcs::jsonb_array(&values, &subtypes),
            "JSON_OBJECT" => json_funcs::json_object(&values, &subtypes),
            "JSONB_OBJECT" => json_funcs::jsonb_object(&values, &subtypes),
            "JSON_INSERT" => json_funcs::json_insert(&values, &subtypes),
            "JSONB_INSERT" => json_funcs::jsonb_insert(&values, &subtypes),
            "JSON_REPLACE" => json_funcs::json_replace(&values, &subtypes),
            "JSONB_REPLACE" => json_funcs::jsonb_replace(&values, &subtypes),
            "JSON_SET" => json_funcs::json_set(&values, &subtypes),
            "JSONB_SET" => json_funcs::jsonb_set(&values, &subtypes),
            _ => unreachable!("eval_json_subtype_function called with {name}"),
        }
    }

    /// Evaluate `subtype(X)` — SQLite's runtime JSON subtype probe. Delegates
    /// the structural subtype rules to [`crate::evaluator::json_subtype`].
    pub(crate) fn eval_subtype(
        &self,
        args: &[vibesql_ast::Expression],
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        use crate::evaluator::json_subtype::data_type_is_string;
        let column_is_declared_string = |_table: Option<&str>, column: &str| -> bool {
            self.schema
                .get_column(column)
                .map(|c| data_type_is_string(&c.data_type))
                .unwrap_or(false)
        };
        crate::evaluator::json_subtype::eval_subtype(
            args,
            &|e| self.eval(e, row),
            &column_is_declared_string,
        )
    }

    /// Evaluate function call
    pub(super) fn eval_function(
        &self,
        name: &str,
        args: &[vibesql_ast::Expression],
        character_unit: &Option<vibesql_ast::CharacterUnit>,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Handle special functions with lazy evaluation
        match name.to_uppercase().as_str() {
            "COALESCE" => return self.eval_coalesce_lazy(args, row),
            "NULLIF" => return self.eval_nullif_lazy(args, row),
            // subtype(X): runtime JSON subtype probe, computed structurally from
            // the argument expression + its evaluated value.
            "SUBTYPE" => return self.eval_subtype(args, row),
            // JSON construction/mutation functions that honor the JSON subtype:
            // an argument that is itself a call to a JSON-producing function
            // embeds as a sub-document rather than a quoted string. We compute
            // those per-argument subtype flags here (from the AST) and pass them
            // alongside the evaluated values.
            "JSON_ARRAY" | "JSON_OBJECT" | "JSON_INSERT" | "JSON_REPLACE" | "JSON_SET"
            | "JSONB_ARRAY" | "JSONB_OBJECT" | "JSONB_INSERT" | "JSONB_REPLACE" | "JSONB_SET" => {
                return self.eval_json_subtype_function(name, args, row);
            }
            // Handle LAST_INSERT_ROWID() and LAST_INSERT_ID() - require database access
            "LAST_INSERT_ROWID" | "LAST_INSERT_ID" => {
                if !args.is_empty() {
                    return Err(ExecutorError::UnsupportedFeature(format!(
                        "{}() takes no arguments",
                        name.to_uppercase()
                    )));
                }
                if let Some(db) = self.database {
                    return Ok(vibesql_types::SqlValue::Integer(db.last_insert_rowid()));
                } else {
                    // No database context available, return 0
                    return Ok(vibesql_types::SqlValue::Integer(0));
                }
            }
            // Handle changes() - returns number of rows modified by last INSERT/UPDATE/DELETE
            // This is a SQLite-compatible function for tracking DML row counts
            "CHANGES" => {
                if !args.is_empty() {
                    return Err(ExecutorError::UnsupportedFeature(
                        "changes() takes no arguments".to_string(),
                    ));
                }
                if let Some(db) = self.database {
                    return Ok(vibesql_types::SqlValue::Integer(db.last_changes_count() as i64));
                } else {
                    // No database context available, return 0
                    return Ok(vibesql_types::SqlValue::Integer(0));
                }
            }
            // Handle total_changes() - returns cumulative rows modified since connection opened
            // This is a SQLite-compatible function for tracking total DML row counts
            "TOTAL_CHANGES" => {
                if !args.is_empty() {
                    return Err(ExecutorError::UnsupportedFeature(
                        "total_changes() takes no arguments".to_string(),
                    ));
                }
                if let Some(db) = self.database {
                    return Ok(vibesql_types::SqlValue::Integer(db.total_changes_count() as i64));
                } else {
                    // No database context available, return 0
                    return Ok(vibesql_types::SqlValue::Integer(0));
                }
            }
            // Handle sqlite_search_count() - TCL test compatibility diagnostic
            // Returns the number of rows examined during query execution
            "SQLITE_SEARCH_COUNT" => {
                if !args.is_empty() {
                    return Err(ExecutorError::UnsupportedFeature(
                        "sqlite_search_count() takes no arguments".to_string(),
                    ));
                }
                if let Some(db) = self.database {
                    return Ok(vibesql_types::SqlValue::Bigint(db.search_count() as i64));
                } else {
                    return Ok(vibesql_types::SqlValue::Bigint(0));
                }
            }
            // Handle sqlite_search_count_reset() - Reset search count to 0
            // Returns 0 and resets the counter
            "SQLITE_SEARCH_COUNT_RESET" => {
                if !args.is_empty() {
                    return Err(ExecutorError::UnsupportedFeature(
                        "sqlite_search_count_reset() takes no arguments".to_string(),
                    ));
                }
                if let Some(db) = self.database {
                    db.reset_search_count();
                }
                return Ok(vibesql_types::SqlValue::Bigint(0));
            }
            _ => {}
        }

        // Check for user-defined functions (Phase 5)
        // Note: UDF execution requires mutable database access, which we don't have here.
        // For now, we'll check if the function exists and return a helpful error.
        // Full UDF support will require refactoring the evaluator to support mutable access.
        if let Some(db) = self.database {
            if db.catalog.function_exists(name) {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "User-defined function '{}' found but cannot be executed in this context. \
                         UDF execution from SELECT expressions requires mutable database access. \
                         This is a known limitation that will be addressed in a future phase.",
                    name
                )));
            }
        }

        // Check for wildcard expressions in function arguments
        // Scalar functions don't accept wildcards (only aggregate functions like COUNT(*) do)
        // SQLite returns "wrong number of arguments to function X()" for this case
        for arg in args {
            if matches!(arg, vibesql_ast::Expression::Wildcard) {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.to_string(),
                });
            }
        }

        // Standard function call: evaluate all arguments eagerly
        let mut arg_values = Vec::new();
        for arg in args {
            arg_values.push(self.eval(arg, row)?);
        }

        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
        let enable_regexp = self.database.map(|db| db.enable_regexp_functions()).unwrap_or(false);
        eval_scalar_function(
            name,
            &arg_values,
            character_unit,
            &sql_mode,
            self.schema_context,
            enable_regexp,
        )
    }
}

/// Does this expression's result embed as a JSON sub-document when passed to
/// another JSON function?
///
/// True when the expression is a direct call to a JSON function whose result is a
/// well-formed JSON document — either JSON *text* (`json`, `json_array`,
/// `json_object`, and the text insert/replace/set/remove/patch mutation
/// functions) or a JSONB *blob* (`jsonb*`), which decodes back to the same JSON
/// document when embedded. Such results embed as JSON sub-documents when passed to
/// another JSON function. This *embedding* signal is distinct from `subtype()`
/// reporting (`json_subtype.rs`): a JSONB blob embeds correctly here yet reports
/// `subtype()` 0. Producers with a *conditional* subtype (json_extract /
/// json_quote / `->`) are intentionally excluded — see the module note in
/// `json_funcs.rs`.
pub(crate) fn expr_has_json_subtype(expr: &vibesql_ast::Expression) -> bool {
    if let vibesql_ast::Expression::Function { name, .. } = expr {
        matches!(
            name.canonical(),
            "json"
                | "json_array"
                | "json_object"
                | "json_insert"
                | "json_replace"
                | "json_set"
                | "json_remove"
                | "json_patch"
                // JSONB functions emit real `SqlValue::Blob` output (Stage 1,
                // #6035); that blob decodes back to the same JSON document when
                // embedded as an argument to another JSON function, so they carry
                // the JSON subtype for *embedding* purposes here. (This is a
                // distinct mechanism from `subtype()` reporting in
                // `json_subtype.rs`, where the BLOB producers are 0.)
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
