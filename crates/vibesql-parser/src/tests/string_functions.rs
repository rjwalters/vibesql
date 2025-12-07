use super::*;

// ========================================================================
// SUBSTRING Function Tests
// ========================================================================

#[test]
fn test_substring_comma_syntax() {
    let result = Parser::parse_sql("SELECT SUBSTRING('hello', 2, 3)");
    assert!(result.is_ok(), "Comma syntax should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert_eq!(select.select_list.len(), 1);

        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Function { name, args, character_unit: _ } = expr {
                assert_eq!(name.to_uppercase(), "SUBSTRING");
                assert_eq!(args.len(), 3); // string, start, length

                // Check string argument
                if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s)) =
                    &args[0]
                {
                    assert_eq!(s.as_str(), "hello");
                } else {
                    panic!("Expected string literal");
                }

                // Check start argument
                if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(n)) =
                    &args[1]
                {
                    assert_eq!(*n, 2);
                } else {
                    panic!("Expected integer literal for start");
                }

                // Check length argument
                if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(n)) =
                    &args[2]
                {
                    assert_eq!(*n, 3);
                } else {
                    panic!("Expected integer literal for length");
                }
            } else {
                panic!("Expected Function expression");
            }
        } else {
            panic!("Expected Expression SelectItem");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_substring_comma_syntax_no_length() {
    let result = Parser::parse_sql("SELECT SUBSTRING('hello', 2)");
    assert!(result.is_ok(), "Comma syntax without length should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Function { name, args, character_unit: _ } = expr {
                assert_eq!(name.to_uppercase(), "SUBSTRING");
                assert_eq!(args.len(), 2); // string, start (no length)
            } else {
                panic!("Expected Function expression");
            }
        }
    }
}

#[test]
fn test_substring_from_syntax() {
    let result = Parser::parse_sql("SELECT SUBSTRING('hello' FROM 2)");
    assert!(result.is_ok(), "FROM syntax should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert_eq!(select.select_list.len(), 1);

        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Function { name, args, character_unit: _ } = expr {
                assert_eq!(name.to_uppercase(), "SUBSTRING");
                assert_eq!(args.len(), 2); // string, start

                // Check string argument
                if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s)) =
                    &args[0]
                {
                    assert_eq!(s.as_str(), "hello");
                } else {
                    panic!("Expected string literal");
                }

                // Check start argument
                if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(n)) =
                    &args[1]
                {
                    assert_eq!(*n, 2);
                } else {
                    panic!("Expected integer literal for start");
                }
            } else {
                panic!("Expected Function expression");
            }
        } else {
            panic!("Expected Expression SelectItem");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_substring_from_for_syntax() {
    let result = Parser::parse_sql("SELECT SUBSTRING('hello' FROM 2 FOR 3)");
    assert!(result.is_ok(), "FROM/FOR syntax should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert_eq!(select.select_list.len(), 1);

        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Function { name, args, character_unit: _ } = expr {
                assert_eq!(name.to_uppercase(), "SUBSTRING");
                assert_eq!(args.len(), 3); // string, start, length

                // Check string argument
                if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s)) =
                    &args[0]
                {
                    assert_eq!(s.as_str(), "hello");
                } else {
                    panic!("Expected string literal");
                }

                // Check start argument
                if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(n)) =
                    &args[1]
                {
                    assert_eq!(*n, 2);
                } else {
                    panic!("Expected integer literal for start");
                }

                // Check length argument
                if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(n)) =
                    &args[2]
                {
                    assert_eq!(*n, 3);
                } else {
                    panic!("Expected integer literal for length");
                }
            } else {
                panic!("Expected Function expression");
            }
        } else {
            panic!("Expected Expression SelectItem");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_substring_syntaxes_equivalent() {
    // Parse both syntaxes
    let comma_result = Parser::parse_sql("SELECT SUBSTRING('hello', 2, 3)");
    let from_for_result = Parser::parse_sql("SELECT SUBSTRING('hello' FROM 2 FOR 3)");

    assert!(comma_result.is_ok());
    assert!(from_for_result.is_ok());

    let comma_stmt = comma_result.unwrap();
    let from_for_stmt = from_for_result.unwrap();

    // Extract the function expressions
    let comma_expr = if let vibesql_ast::Statement::Select(select) = comma_stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            expr.clone()
        } else {
            panic!("Expected expression");
        }
    } else {
        panic!("Expected SELECT");
    };

    let from_for_expr = if let vibesql_ast::Statement::Select(select) = from_for_stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            expr.clone()
        } else {
            panic!("Expected expression");
        }
    } else {
        panic!("Expected SELECT");
    };

    // Both should be Function expressions with same args
    if let (
        vibesql_ast::Expression::Function { name: name1, args: args1, character_unit: _ },
        vibesql_ast::Expression::Function { name: name2, args: args2, character_unit: _ },
    ) = (comma_expr, from_for_expr)
    {
        assert_eq!(name1.to_uppercase(), name2.to_uppercase());
        assert_eq!(args1.len(), args2.len());
        assert_eq!(args1.len(), 3);
    } else {
        panic!("Both should be Function expressions");
    }
}

// ========================================================================
// TRIM Function Tests
// ========================================================================

#[test]
fn test_trim_from_without_char() {
    let result = Parser::parse_sql("SELECT TRIM(FROM '  foo  ')");
    assert!(result.is_ok(), "TRIM(FROM string) should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        assert_eq!(select.select_list.len(), 1);

        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Trim { position, removal_char, string } = expr {
                // Position should be None (defaults to BOTH)
                assert_eq!(*position, None);
                // removal_char should be None (defaults to space)
                assert!(removal_char.is_none());
                // String should be the literal
                if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(s)) =
                    string.as_ref()
                {
                    assert_eq!(s.as_str(), "  foo  ");
                } else {
                    panic!("Expected string literal");
                }
            } else {
                panic!("Expected Trim expression");
            }
        } else {
            panic!("Expected Expression SelectItem");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_trim_both_from_without_char() {
    let result = Parser::parse_sql("SELECT TRIM(BOTH FROM '  foo  ')");
    assert!(result.is_ok(), "TRIM(BOTH FROM string) should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Trim { position, removal_char, .. } = expr {
                // Position should be BOTH
                assert_eq!(*position, Some(vibesql_ast::TrimPosition::Both));
                // removal_char should be None (defaults to space)
                assert!(removal_char.is_none());
            } else {
                panic!("Expected Trim expression");
            }
        }
    }
}

#[test]
fn test_trim_leading_from_without_char() {
    let result = Parser::parse_sql("SELECT TRIM(LEADING FROM '  foo')");
    assert!(result.is_ok(), "TRIM(LEADING FROM string) should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Trim { position, removal_char, .. } = expr {
                // Position should be LEADING
                assert_eq!(*position, Some(vibesql_ast::TrimPosition::Leading));
                // removal_char should be None (defaults to space)
                assert!(removal_char.is_none());
            } else {
                panic!("Expected Trim expression");
            }
        }
    }
}

#[test]
fn test_trim_trailing_from_without_char() {
    let result = Parser::parse_sql("SELECT TRIM(TRAILING FROM 'foo  ')");
    assert!(result.is_ok(), "TRIM(TRAILING FROM string) should parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Trim { position, removal_char, .. } = expr {
                // Position should be TRAILING
                assert_eq!(*position, Some(vibesql_ast::TrimPosition::Trailing));
                // removal_char should be None (defaults to space)
                assert!(removal_char.is_none());
            } else {
                panic!("Expected Trim expression");
            }
        }
    }
}

#[test]
fn test_trim_with_char_still_works() {
    // Verify existing functionality is preserved
    let result = Parser::parse_sql("SELECT TRIM(BOTH 'x' FROM 'xfoox')");
    assert!(result.is_ok(), "TRIM(BOTH 'x' FROM string) should still parse: {:?}", result);

    let stmt = result.unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = &select.select_list[0] {
            if let vibesql_ast::Expression::Trim { position, removal_char, .. } = expr {
                // Position should be BOTH
                assert_eq!(*position, Some(vibesql_ast::TrimPosition::Both));
                // removal_char should be Some('x')
                assert!(removal_char.is_some());
                if let Some(boxed_expr) = removal_char {
                    if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(c)) =
                        boxed_expr.as_ref()
                    {
                        assert_eq!(c.as_str(), "x");
                    } else {
                        panic!("Expected string literal for removal char");
                    }
                }
            } else {
                panic!("Expected Trim expression");
            }
        }
    }
}
