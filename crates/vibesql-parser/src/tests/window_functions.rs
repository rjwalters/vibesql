//! Tests for window function parsing (OVER clause)

use super::*;

#[test]
fn test_empty_over_clause() {
    let sql = "SELECT ROW_NUMBER() OVER () FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::WindowFunction { function, over } => {
                        match function {
                            vibesql_ast::WindowFunctionSpec::Ranking { name, .. } => {
                                assert_eq!(name, "ROW_NUMBER");
                            }
                            _ => panic!("Expected Ranking window function"),
                        }
                        assert!(over.partition_by.is_none());
                        assert!(over.order_by.is_none());
                        assert!(over.frame.is_none());
                    }
                    _ => panic!("Expected WindowFunction expression"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_partition_by_single() {
    let sql = "SELECT RANK() OVER (PARTITION BY dept) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.partition_by.is_some());
                    let partition = over.partition_by.as_ref().unwrap();
                    assert_eq!(partition.len(), 1);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_partition_by_multiple() {
    let sql = "SELECT RANK() OVER (PARTITION BY dept, region) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.partition_by.is_some());
                    let partition = over.partition_by.as_ref().unwrap();
                    assert_eq!(partition.len(), 2);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_order_by_single() {
    let sql = "SELECT RANK() OVER (ORDER BY salary DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.order_by.is_some());
                    let order = over.order_by.as_ref().unwrap();
                    assert_eq!(order.len(), 1);
                    assert_eq!(order[0].direction, vibesql_ast::OrderDirection::Desc);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_order_by_multiple() {
    let sql = "SELECT RANK() OVER (ORDER BY dept ASC, salary DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.order_by.is_some());
                    let order = over.order_by.as_ref().unwrap();
                    assert_eq!(order.len(), 2);
                    assert_eq!(order[0].direction, vibesql_ast::OrderDirection::Asc);
                    assert_eq!(order[1].direction, vibesql_ast::OrderDirection::Desc);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_partition_and_order() {
    let sql = "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.partition_by.is_some());
                    assert!(over.order_by.is_some());
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_rows_unbounded_preceding() {
    let sql = "SELECT SUM(x) OVER (ROWS UNBOUNDED PRECEDING) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.frame.is_some());
                    let frame = over.frame.as_ref().unwrap();
                    assert_eq!(frame.unit, vibesql_ast::FrameUnit::Rows);
                    assert_eq!(frame.start, vibesql_ast::FrameBound::UnboundedPreceding);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_rows_between_and() {
    let sql = "SELECT SUM(x) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.frame.is_some());
                    let frame = over.frame.as_ref().unwrap();
                    assert_eq!(frame.unit, vibesql_ast::FrameUnit::Rows);

                    match &frame.start {
                        vibesql_ast::FrameBound::Preceding(_) => {}
                        _ => panic!("Expected Preceding frame start"),
                    }

                    assert!(frame.end.is_some());
                    assert_eq!(frame.end.as_ref().unwrap(), &vibesql_ast::FrameBound::CurrentRow);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_range_frame() {
    let sql = "SELECT SUM(x) OVER (RANGE UNBOUNDED PRECEDING) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.frame.is_some());
                    let frame = over.frame.as_ref().unwrap();
                    assert_eq!(frame.unit, vibesql_ast::FrameUnit::Range);
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_aggregate_as_window_function() {
    let sql = "SELECT SUM(salary) OVER (PARTITION BY dept) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { function, .. } => match function {
                    vibesql_ast::WindowFunctionSpec::Aggregate { name, args, .. } => {
                        assert_eq!(name, "SUM");
                        assert_eq!(args.len(), 1);
                    }
                    _ => panic!("Expected Aggregate window function"),
                },
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_ranking_functions() {
    let sql = "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let sql = "SELECT RANK() OVER (ORDER BY score DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let sql = "SELECT DENSE_RANK() OVER (ORDER BY score DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let sql = "SELECT NTILE(4) OVER (ORDER BY score DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());
}

#[test]
fn test_value_functions() {
    let sql = "SELECT LAG(salary, 1) OVER (ORDER BY hire_date) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let sql = "SELECT LEAD(salary, 2) OVER (ORDER BY hire_date) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());
}

// window1 6.3: FILTER is only valid on aggregate window functions. A ranking or
// value window function with a FILTER clause is a parse error, matching SQLite's
// "FILTER clause may only be used with aggregate window functions".
#[test]
fn test_filter_on_non_aggregate_window_rejected() {
    for sql in [
        "SELECT lag(x) FILTER (WHERE (x%2)=0) OVER w FROM t1 WINDOW w AS (ORDER BY x)",
        "SELECT row_number() FILTER (WHERE x>0) OVER (ORDER BY x) FROM t1",
        "SELECT rank() FILTER (WHERE x>0) OVER (ORDER BY x) FROM t1",
    ] {
        let err = Parser::parse_sql(sql).expect_err(sql);
        assert_eq!(
            err.message, "FILTER clause may only be used with aggregate window functions",
            "sql: {sql}"
        );
    }

    // FILTER on an *aggregate* window function stays valid.
    assert!(
        Parser::parse_sql("SELECT sum(x) FILTER (WHERE x>0) OVER (ORDER BY x) FROM t1").is_ok()
    );
}

#[test]
fn test_complex_window_spec() {
    let sql = "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    assert!(over.partition_by.is_some());
                    assert!(over.order_by.is_some());
                    assert!(over.frame.is_some());

                    let frame = over.frame.as_ref().unwrap();
                    assert!(frame.end.is_some());
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_multiple_window_functions() {
    let sql = "SELECT ROW_NUMBER() OVER (ORDER BY id), RANK() OVER (ORDER BY score DESC) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 2);

            for item in &select.select_list {
                match item {
                    vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                        vibesql_ast::Expression::WindowFunction { .. } => {}
                        _ => panic!("Expected WindowFunction"),
                    },
                    _ => panic!("Expected Expression"),
                }
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_unbounded_following() {
    let sql = "SELECT SUM(x) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    let frame = over.frame.as_ref().unwrap();
                    assert_eq!(frame.start, vibesql_ast::FrameBound::CurrentRow);
                    assert_eq!(
                        frame.end.as_ref().unwrap(),
                        &vibesql_ast::FrameBound::UnboundedFollowing
                    );
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_n_following() {
    let sql = "SELECT AVG(price) OVER (ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok());

    let stmt = result.unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::WindowFunction { over, .. } => {
                    let frame = over.frame.as_ref().unwrap();

                    match &frame.start {
                        vibesql_ast::FrameBound::Preceding(_) => {}
                        _ => panic!("Expected Preceding"),
                    }

                    match &frame.end {
                        Some(vibesql_ast::FrameBound::Following(_)) => {}
                        _ => panic!("Expected Following"),
                    }
                }
                _ => panic!("Expected WindowFunction"),
            },
            _ => panic!("Expected Expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_window_function_without_over_clause_errors() {
    // Window-only functions MUST have an OVER clause
    // They cannot be used as regular scalar functions

    // Ranking functions
    let sql = "SELECT row_number() FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "row_number() without OVER should error");
    assert!(result.unwrap_err().message.contains("misuse of window function"));

    let sql = "SELECT rank() FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "rank() without OVER should error");

    let sql = "SELECT dense_rank() FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "dense_rank() without OVER should error");

    let sql = "SELECT ntile(4) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "ntile() without OVER should error");

    // Value functions
    let sql = "SELECT nth_value(x, 1) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "nth_value() without OVER should error");

    let sql = "SELECT lag(x, 1) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "lag() without OVER should error");

    let sql = "SELECT lead(x, 1) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "lead() without OVER should error");

    let sql = "SELECT first_value(x) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "first_value() without OVER should error");

    let sql = "SELECT last_value(x) FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "last_value() without OVER should error");
}

#[test]
fn test_window_function_argument_count_validation() {
    // Zero-argument functions should reject any arguments
    let sql = "SELECT row_number(x) OVER () FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "row_number(x) should error");
    assert!(result.unwrap_err().message.contains("wrong number of arguments"));

    let sql = "SELECT rank(x) OVER () FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "rank(x) should error");

    let sql = "SELECT dense_rank(x) OVER () FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "dense_rank(x) should error");

    // Single-argument functions should require exactly one argument
    let sql = "SELECT ntile() OVER () FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "ntile() with no args should error");

    let sql = "SELECT ntile(1, 2) OVER () FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "ntile(1, 2) should error");

    let sql = "SELECT first_value() OVER () FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "first_value() with no args should error");

    // NTH_VALUE requires exactly 2 arguments
    let sql = "SELECT nth_value(x) OVER () FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "nth_value(x) with 1 arg should error");

    let sql = "SELECT nth_value(x, 1, 2) OVER () FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "nth_value(x, 1, 2) with 3 args should error");

    // LAG/LEAD accept 1-3 arguments
    let sql = "SELECT lag() OVER () FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "lag() with no args should error");

    let sql = "SELECT lag(x, 1, 0, extra) OVER () FROM t";
    let result = Parser::parse_sql(sql);
    assert!(result.is_err(), "lag() with 4 args should error");

    // Valid calls should succeed
    let sql = "SELECT row_number() OVER () FROM t";
    assert!(Parser::parse_sql(sql).is_ok(), "row_number() should work");

    let sql = "SELECT ntile(4) OVER () FROM t";
    assert!(Parser::parse_sql(sql).is_ok(), "ntile(4) should work");

    let sql = "SELECT nth_value(x, 1) OVER () FROM t";
    assert!(Parser::parse_sql(sql).is_ok(), "nth_value(x, 1) should work");

    let sql = "SELECT lag(x) OVER () FROM t";
    assert!(Parser::parse_sql(sql).is_ok(), "lag(x) should work");

    let sql = "SELECT lag(x, 1) OVER () FROM t";
    assert!(Parser::parse_sql(sql).is_ok(), "lag(x, 1) should work");

    let sql = "SELECT lag(x, 1, 0) OVER () FROM t";
    assert!(Parser::parse_sql(sql).is_ok(), "lag(x, 1, 0) should work");
}

// window6.test 9.4/9.5: UNBOUNDED FOLLOWING may not be used as a frame *start*
// bound. SQLite reports `near "FOLLOWING": syntax error`.
#[test]
fn test_frame_start_unbounded_following_rejected() {
    for sql in [
        "SELECT count() OVER (ORDER BY x RANGE UNBOUNDED FOLLOWING) FROM c",
        "SELECT count() OVER (ORDER BY x RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) FROM c",
    ] {
        let err = Parser::parse_sql(sql).expect_err("UNBOUNDED FOLLOWING start must error");
        assert!(
            err.message.contains("near \"FOLLOWING\": syntax error"),
            "unexpected error for {sql:?}: {err:?}"
        );
    }
}

// window6.test 9.6: UNBOUNDED PRECEDING may not be used as a frame *end* bound.
// SQLite reports `near "PRECEDING": syntax error`.
#[test]
fn test_frame_end_unbounded_preceding_rejected() {
    let sql =
        "SELECT count() OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) FROM c";
    let err = Parser::parse_sql(sql).expect_err("UNBOUNDED PRECEDING end must error");
    assert!(err.message.contains("near \"PRECEDING\": syntax error"), "unexpected error: {err:?}");
}

// A full-partition frame `BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
// must still parse — the bound restrictions only reject the reversed forms.
#[test]
fn test_frame_unbounded_preceding_to_following_ok() {
    let sql =
        "SELECT sum(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM c";
    assert!(
        Parser::parse_sql(sql).is_ok(),
        "UNBOUNDED PRECEDING..UNBOUNDED FOLLOWING should parse"
    );
}

// window6.test 9.3: DISTINCT is not permitted on a window aggregate.
#[test]
fn test_distinct_window_aggregate_rejected() {
    let sql = "SELECT count(DISTINCT x) OVER (ORDER BY x) FROM c";
    let err = Parser::parse_sql(sql).expect_err("DISTINCT window aggregate must error");
    assert!(
        err.message.contains("DISTINCT is not supported for window functions"),
        "unexpected error: {err:?}"
    );
}

// -----------------------------------------------------------------------------
// window6.test: WINDOW / OVER / FILTER are fallback identifiers (keyword1-style).
// These regression tests cover the reserved-keyword-as-identifier gaps fixed for
// issue #6191 (window6 filescope-err.1 / 4.1 / 5.0 / 5.1 / 1.5.4) without
// regressing the genuine WINDOW / OVER / FILTER clauses.
// -----------------------------------------------------------------------------

// window6 iteration 5 file-scope setup: `window`/`over`/`filter` are legal
// column type names (`CREATE TABLE over(following, preceding window)`).
#[test]
fn test_window_over_filter_as_type_names() {
    for ty in ["window", "over", "filter"] {
        let sql = format!("CREATE TABLE t(a, b {ty})");
        assert!(Parser::parse_sql(&sql).is_ok(), "`{ty}` should be a legal type name");
    }
}

// window6 5.0: `SELECT sum(x) over FROM over` — OVER is a column alias (not a
// window clause) because it is not followed by `(` or a window name.
#[test]
fn test_over_used_as_column_alias() {
    let sql = "SELECT sum(x) over FROM over";
    let stmt = Parser::parse_sql(sql).expect("OVER-as-alias should parse");
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                assert!(
                    matches!(expr, vibesql_ast::Expression::AggregateFunction { .. }),
                    "expected a plain aggregate, not a window function"
                );
                assert_eq!(alias.as_deref(), Some("over"));
            }
            _ => panic!("expected an expression select item"),
        },
        _ => panic!("expected a SELECT"),
    }
}

// window6 1.5.4: `SELECT sum(x) filter FROM t` — FILTER is a column alias because
// it is not followed by `(`.
#[test]
fn test_filter_used_as_column_alias() {
    let sql = "SELECT sum(x) filter FROM t";
    let stmt = Parser::parse_sql(sql).expect("FILTER-as-alias should parse");
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                assert!(matches!(
                    expr,
                    vibesql_ast::Expression::AggregateFunction { filter: None, .. }
                ));
                assert_eq!(alias.as_deref(), Some("filter"));
            }
            _ => panic!("expected an expression select item"),
        },
        _ => panic!("expected a SELECT"),
    }
}

// window6 5.1: `OVER over` references a named window whose name is the keyword
// `over`; the genuine WINDOW clause defines it.
#[test]
fn test_over_keyword_window_name() {
    let sql = "SELECT sum(x) over over FROM over WINDOW over AS ()";
    assert!(Parser::parse_sql(sql).is_ok(), "keyword window name after OVER should parse");
}

// window6 4.1: `SELECT * FROM t4 window, t4` — WINDOW is a table alias, not the
// start of a WINDOW clause (which would be `WINDOW <name> AS (...)`).
#[test]
fn test_window_used_as_table_alias() {
    let sql = "SELECT * FROM t4 window, t4";
    assert!(Parser::parse_sql(sql).is_ok(), "WINDOW-as-table-alias should parse");
}

// Guard: a genuine trailing WINDOW clause is still recognized (not swallowed as
// an alias) when it is a real `WINDOW <name> AS (...)`.
#[test]
fn test_real_window_clause_still_parses() {
    let sql = "SELECT sum(x) OVER w FROM t1 WINDOW w AS (ORDER BY y)";
    let stmt = Parser::parse_sql(sql).expect("real WINDOW clause should parse");
    match stmt {
        vibesql_ast::Statement::Select(select) => {
            let defs = select.window_definitions.expect("WINDOW clause must be captured");
            assert!(!defs.is_empty(), "WINDOW clause must define at least one window");
        }
        _ => panic!("expected a SELECT"),
    }
}

// Guard: a genuine FILTER clause is still recognized when followed by `(`.
#[test]
fn test_real_filter_clause_still_parses() {
    let sql = "SELECT sum(x) FILTER (WHERE x > 0) FROM t";
    let stmt = Parser::parse_sql(sql).expect("real FILTER clause should parse");
    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => {
                assert!(matches!(
                    expr,
                    vibesql_ast::Expression::AggregateFunction { filter: Some(_), .. }
                ));
            }
            _ => panic!("expected an expression select item"),
        },
        _ => panic!("expected a SELECT"),
    }
}
