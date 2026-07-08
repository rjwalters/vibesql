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

                        if super::super::core::ExpressionEvaluator::values_are_equal(
                            &operand_value,
                            &when_value,
                        ) {
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

        // Check equality and return accordingly
        if super::super::core::ExpressionEvaluator::values_are_equal(&val1, &val2) {
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
        let mut values = Vec::with_capacity(args.len());
        let mut subtypes = Vec::with_capacity(args.len());
        for arg in args {
            values.push(self.eval(arg, row)?);
            subtypes.push(expr_has_json_subtype(arg));
        }

        use super::super::functions::sqlite_compat::json_funcs;
        // The `jsonb_*` names are text-mode aliases (accept-and-convert): they
        // delegate to the identical `json_*` implementation. See the Phase 4
        // JSONB note in `json_funcs.rs`.
        match name.to_uppercase().as_str() {
            "JSON_ARRAY" | "JSONB_ARRAY" => json_funcs::json_array(&values, &subtypes),
            "JSON_OBJECT" | "JSONB_OBJECT" => json_funcs::json_object(&values, &subtypes),
            "JSON_INSERT" | "JSONB_INSERT" => json_funcs::json_insert(&values, &subtypes),
            "JSON_REPLACE" | "JSONB_REPLACE" => json_funcs::json_replace(&values, &subtypes),
            "JSON_SET" | "JSONB_SET" => json_funcs::json_set(&values, &subtypes),
            _ => unreachable!("eval_json_subtype_function called with {name}"),
        }
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
        eval_scalar_function(name, &arg_values, character_unit, &sql_mode, self.schema_context)
    }
}

/// Does this expression carry SQLite's JSON subtype?
///
/// True when the expression is a direct call to a JSON function whose result is
/// always well-formed JSON text (json, json_array, json_object, and the
/// insert/replace/set/remove/patch mutation functions). Such results embed as
/// JSON sub-documents when passed to another JSON function. Producers with a
/// *conditional* subtype (json_extract / json_quote / `->`) are intentionally
/// excluded — see the module note in `json_funcs.rs`.
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
                // JSONB accept-and-convert aliases produce the same JSON text and
                // so carry the JSON subtype too.
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
