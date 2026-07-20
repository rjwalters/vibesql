use super::*;

// ========================================================================
// JOIN Operation Tests
// ========================================================================

#[test]
fn test_parse_simple_join() {
    let result = Parser::parse_sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.from.is_some());
            match select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Join {
                    join_type,
                    left,
                    right,
                    condition,
                    natural,
                    ..
                } => {
                    // Default JOIN is INNER JOIN
                    assert_eq!(*join_type, vibesql_ast::JoinType::Inner);
                    assert!(!*natural);

                    // Left should be users table
                    match **left {
                        vibesql_ast::FromClause::Table { ref name, .. } if name == "users" => {} /* Success */
                        _ => panic!("Expected left table to be 'users'"),
                    }

                    // Right should be orders table
                    match **right {
                        vibesql_ast::FromClause::Table { ref name, .. } if name == "orders" => {} /* Success */
                        _ => panic!("Expected right table to be 'orders'"),
                    }

                    // Should have ON condition
                    assert!(condition.is_some());
                }
                _ => panic!("Expected JOIN in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_inner_join() {
    let result =
        Parser::parse_sql("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::Inner);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_comma_separated_from() {
    let result = Parser::parse_sql("SELECT * FROM tab0, tab1 AS cor0;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert!(select.from.is_some());
            match select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Join {
                    join_type,
                    left,
                    right,
                    condition,
                    natural,
                    ..
                } => {
                    // Comma should be parsed as CROSS JOIN
                    assert_eq!(*join_type, vibesql_ast::JoinType::Cross);
                    assert!(!*natural);

                    // Left should be tab0 table
                    match **left {
                        vibesql_ast::FromClause::Table { ref name, alias: None, .. }
                            if name == "tab0" => {} /* Success */
                        _ => panic!("Expected left table to be 'tab0'"),
                    }

                    // Right should be tab1 table with alias cor0
                    match **right {
                        vibesql_ast::FromClause::Table {
                            ref name, alias: Some(ref alias), ..
                        } if name == "tab1" && alias == "cor0" => {} // Success
                        _ => panic!("Expected right table to be 'tab1' with alias 'cor0'"),
                    }

                    // Should have no condition (CROSS JOIN)
                    assert!(condition.is_none());
                }
                _ => panic!("Expected JOIN in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_left_join() {
    let result =
        Parser::parse_sql("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::LeftOuter);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_left_outer_join() {
    let result = Parser::parse_sql(
        "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id;",
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::LeftOuter);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_right_join() {
    let result =
        Parser::parse_sql("SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::RightOuter);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_multiple_joins() {
    let result = Parser::parse_sql(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id;"
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Should have nested JOINs
            match select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Join { left, .. } => {
                    // Left should also be a JOIN
                    match **left {
                        vibesql_ast::FromClause::Join { .. } => {} // Success - nested JOIN
                        _ => panic!("Expected nested JOIN"),
                    }
                }
                _ => panic!("Expected JOIN"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

// ========================================================================
// USING Clause Tests (#4241)
// ========================================================================

#[test]
fn test_parse_join_using_single_column() {
    let result = Parser::parse_sql("SELECT * FROM t1 JOIN t2 USING (id);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, using_columns, natural, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::Inner);
                assert!(!*natural);
                assert!(using_columns.is_some());
                let cols = using_columns.as_ref().unwrap();
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0], "id");
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_join_using_multiple_columns() {
    let result = Parser::parse_sql("SELECT * FROM t1 LEFT JOIN t2 USING (id, name, value);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, using_columns, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::LeftOuter);
                assert!(using_columns.is_some());
                let cols = using_columns.as_ref().unwrap();
                assert_eq!(cols.len(), 3);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_full_join_using() {
    let result = Parser::parse_sql("SELECT * FROM t1 FULL OUTER JOIN t2 USING (id);");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, using_columns, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::FullOuter);
                assert!(using_columns.is_some());
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

// ========================================================================
// Parenthesized JOIN Expression Tests (#4241)
// ========================================================================

#[test]
fn test_parse_parenthesized_join() {
    let result =
        Parser::parse_sql("SELECT * FROM t1 JOIN (t2 JOIN t3 ON t2.id = t3.id) ON t1.id = t2.id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Join { left, right, .. } => {
                    // Left should be t1
                    match **left {
                        vibesql_ast::FromClause::Table { ref name, .. } if name == "t1" => {}
                        _ => panic!("Expected left table to be t1"),
                    }
                    // Right should be a JOIN
                    match **right {
                        vibesql_ast::FromClause::Join { .. } => {} // Success
                        _ => panic!("Expected right to be a JOIN"),
                    }
                }
                _ => panic!("Expected JOIN"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_nested_parenthesized_join_with_using() {
    // This is the exact case from issue #4241
    let result = Parser::parse_sql(
        "SELECT * FROM t3 FULL JOIN (t4 FULL JOIN (t5 FULL JOIN t6 USING (id)) USING(id)) USING(id);",
    );
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Join { join_type, using_columns, right, .. } => {
                    assert_eq!(*join_type, vibesql_ast::JoinType::FullOuter);
                    assert!(using_columns.is_some());
                    // Right side should be another JOIN
                    match right.as_ref() {
                        vibesql_ast::FromClause::Join { join_type: ref inner_type, .. } => {
                            assert_eq!(*inner_type, vibesql_ast::JoinType::FullOuter);
                        }
                        _ => panic!("Expected nested JOIN"),
                    }
                }
                _ => panic!("Expected FULL JOIN"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

// ========================================================================
// NATURAL JOIN Tests (#4312)
// ========================================================================

#[test]
fn test_parse_natural_join() {
    let result = Parser::parse_sql("SELECT * FROM t1 NATURAL JOIN t2;");
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join {
                join_type, natural, condition, using_columns, ..
            } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::Inner);
                assert!(*natural);
                // NATURAL JOIN should have no ON or USING clause
                assert!(condition.is_none());
                assert!(using_columns.is_none());
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_natural_left_join() {
    let result = Parser::parse_sql("SELECT * FROM t1 NATURAL LEFT JOIN t2;");
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, natural, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::LeftOuter);
                assert!(*natural);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_natural_cross_join() {
    // NATURAL CROSS JOIN is valid SQL - the NATURAL modifier applies the
    // natural join condition (matching on common column names)
    let result = Parser::parse_sql("SELECT * FROM t1 NATURAL CROSS JOIN t2;");
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join {
                join_type, natural, condition, using_columns, ..
            } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::Cross);
                // NATURAL flag should be preserved
                assert!(*natural);
                // No explicit ON condition (natural join derives condition from common columns)
                assert!(condition.is_none());
                assert!(using_columns.is_none());
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_natural_cross_join_complex() {
    // Test case from issue #4312
    let result = Parser::parse_sql(
        "SELECT DISTINCT t1.c0, t3.c0 FROM t2 NATURAL CROSS JOIN t1 RIGHT JOIN t3 ON t1.c0;",
    );
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
}

#[test]
fn test_parse_outer_left_natural_join() {
    // SQLite allows OUTER before LEFT in "OUTER LEFT NATURAL JOIN" (#4574)
    let result = Parser::parse_sql("SELECT * FROM t1 OUTER LEFT NATURAL JOIN t2;");
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, natural, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::LeftOuter);
                assert!(*natural);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_left_natural_join() {
    // NATURAL can appear after LEFT: "LEFT NATURAL JOIN" (#4574)
    let result = Parser::parse_sql("SELECT * FROM t1 LEFT NATURAL JOIN t2;");
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, natural, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::LeftOuter);
                assert!(*natural);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_left_outer_natural_join() {
    // NATURAL can appear after LEFT OUTER: "LEFT OUTER NATURAL JOIN" (#4574)
    let result = Parser::parse_sql("SELECT * FROM t1 LEFT OUTER NATURAL JOIN t2;");
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, natural, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::LeftOuter);
                assert!(*natural);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_natural_left_outer_join() {
    // Standard form: "NATURAL LEFT OUTER JOIN"
    let result = Parser::parse_sql("SELECT * FROM t1 NATURAL LEFT OUTER JOIN t2;");
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, natural, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::LeftOuter);
                assert!(*natural);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

// ========================================================================
// Legacy Comma-Join ON Syntax Tests (#4369)
// ========================================================================

#[test]
fn test_parse_legacy_comma_join_on() {
    // SQLite legacy syntax: FROM t1, t2 ON condition behaves like INNER JOIN
    let result = Parser::parse_sql("SELECT * FROM t1, t2 ON t1.a = t2.b;");
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, condition, natural, .. } => {
                // Legacy comma-join with ON should be treated as INNER JOIN
                assert_eq!(*join_type, vibesql_ast::JoinType::Inner);
                assert!(!*natural);
                assert!(condition.is_some(), "Expected ON condition");
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_legacy_comma_join_on_multiple_tables() {
    // Multiple comma-joins with ON clauses
    let result = Parser::parse_sql("SELECT * FROM t1, t2 ON t1.a = t2.b, t3 ON t2.c = t3.d;");
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            match select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Join { join_type, left, condition, .. } => {
                    // Outermost join (t1,t2) JOIN t3
                    assert_eq!(*join_type, vibesql_ast::JoinType::Inner);
                    assert!(condition.is_some());

                    // Inner join should also be Inner with condition
                    match left.as_ref() {
                        vibesql_ast::FromClause::Join {
                            join_type: inner_type,
                            condition: inner_cond,
                            ..
                        } => {
                            assert_eq!(*inner_type, vibesql_ast::JoinType::Inner);
                            assert!(inner_cond.is_some());
                        }
                        _ => panic!("Expected nested JOIN"),
                    }
                }
                _ => panic!("Expected JOIN"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_comma_without_on_still_cross_join() {
    // Plain comma without ON should still be CROSS JOIN
    let result = Parser::parse_sql("SELECT * FROM t1, t2;");
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, condition, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::Cross);
                assert!(condition.is_none());
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_legacy_comma_join_using(/* issue #6192 */) {
    // SQLite treats "," exactly like CROSS/INNER JOIN, so a USING clause is
    // legal after a comma-join and turns the cartesian product into an inner
    // join. Previously the parser accepted `FROM t1, t2 ON ...` but rejected
    // `FROM t1, t2 USING (...)` with a spurious `near "USING": syntax error`
    // (e_select-0.1.2 / 1.4 / 1.5 / 1.6 / 1.7 comma variants).
    let result = Parser::parse_sql("SELECT * FROM t1, t2 USING (a);");
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join {
                join_type,
                condition,
                using_columns,
                natural,
                ..
            } => {
                // Comma-join with USING behaves like INNER JOIN ... USING.
                assert_eq!(*join_type, vibesql_ast::JoinType::Inner);
                assert!(!*natural);
                assert!(condition.is_none(), "USING join carries no ON condition");
                let cols = using_columns.as_ref().expect("Expected USING columns");
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0], "a");
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_legacy_comma_join_using_multiple_columns(/* issue #6192 */) {
    let result = Parser::parse_sql("SELECT * FROM t3, t4 USING (a, c);");
    assert!(result.is_ok(), "Parse failed: {:?}", result.err());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            vibesql_ast::FromClause::Join { join_type, using_columns, .. } => {
                assert_eq!(*join_type, vibesql_ast::JoinType::Inner);
                let cols = using_columns.as_ref().expect("Expected USING columns");
                assert_eq!(cols.len(), 2);
                assert_eq!(cols[0], "a");
                assert_eq!(cols[1], "c");
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_join_using_contextual_keyword_column(/* issue #5945 */) {
    // Contextual keywords (`m`, `key`, `level`) must be accepted as unquoted
    // column names in a JOIN ... USING (...) list, matching SQLite.
    for name in ["m", "key", "level"] {
        let sql = format!("SELECT * FROM t1 JOIN t2 USING ({name});");
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "USING ({name}) should parse: {:?}", result.err());
        match result.unwrap() {
            vibesql_ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
                vibesql_ast::FromClause::Join { using_columns, .. } => {
                    let cols = using_columns.as_ref().expect("USING columns present");
                    assert_eq!(cols, &vec![name.to_string()]);
                }
                _ => panic!("Expected JOIN"),
            },
            _ => panic!("Expected SELECT"),
        }
    }
}
