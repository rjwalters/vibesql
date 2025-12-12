//! Tests for stored procedures and functions (CREATE/DROP/CALL)

use crate::Parser;

#[test]
fn test_create_procedure_simple() {
    let sql = "CREATE PROCEDURE add_user(IN user_id INT) BEGIN SELECT * FROM users; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateProcedure(stmt)) = result {
        assert_eq!(stmt.procedure_name, "add_user");
        assert_eq!(stmt.parameters.len(), 1);
    } else {
        panic!("Expected CreateProcedure statement");
    }
}

#[test]
fn test_create_function_simple() {
    let sql = "CREATE FUNCTION double_it(x INT) RETURNS INT BEGIN RETURN x * 2; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateFunction(stmt)) = result {
        assert_eq!(stmt.function_name, "double_it");
        assert_eq!(stmt.parameters.len(), 1);
    } else {
        panic!("Expected CreateFunction statement");
    }
}

#[test]
fn test_drop_procedure() {
    let sql = "DROP PROCEDURE IF EXISTS add_user;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::DropProcedure(stmt)) = result {
        assert_eq!(stmt.procedure_name, "add_user");
        assert!(stmt.if_exists);
    } else {
        panic!("Expected DropProcedure statement");
    }
}

#[test]
fn test_drop_function() {
    let sql = "DROP FUNCTION IF EXISTS double_it;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::DropFunction(stmt)) = result {
        assert_eq!(stmt.function_name, "double_it");
        assert!(stmt.if_exists);
    } else {
        panic!("Expected DropFunction statement");
    }
}

#[test]
fn test_drop_procedure_without_if_exists() {
    let sql = "DROP PROCEDURE add_user;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::DropProcedure(stmt)) = result {
        assert_eq!(stmt.procedure_name, "add_user");
        assert!(!stmt.if_exists);
    } else {
        panic!("Expected DropProcedure statement");
    }
}

#[test]
fn test_call_statement() {
    let sql = "CALL add_user(123);";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::Call(stmt)) = result {
        assert_eq!(stmt.procedure_name, "add_user");
        assert_eq!(stmt.arguments.len(), 1);
    } else {
        panic!("Expected Call statement");
    }
}

#[test]
fn test_create_procedure_multiple_parameters() {
    let sql = "CREATE PROCEDURE update_status(IN user_id INT, IN new_status VARCHAR(20), OUT success BOOLEAN) BEGIN UPDATE users SET status = new_status WHERE id = user_id; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateProcedure(stmt)) = result {
        assert_eq!(stmt.procedure_name, "update_status");
        assert_eq!(stmt.parameters.len(), 3);
    } else {
        panic!("Expected CreateProcedure statement");
    }
}

#[test]
fn test_create_function_with_parameters() {
    let sql = "CREATE FUNCTION add_prices(price1 DECIMAL(10,2), price2 DECIMAL(10,2)) RETURNS DECIMAL(10,2) BEGIN RETURN price1 + price2; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateFunction(stmt)) = result {
        assert_eq!(stmt.function_name, "add_prices");
        assert_eq!(stmt.parameters.len(), 2);
    } else {
        panic!("Expected CreateFunction statement");
    }
}

#[test]
fn test_procedure_with_if_statement() {
    let sql = "CREATE PROCEDURE check_status(IN id INT) BEGIN IF id > 0 THEN SELECT 'positive'; END IF; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateProcedure(stmt)) = result {
        assert_eq!(stmt.procedure_name, "check_status");
    } else {
        panic!("Expected CreateProcedure statement");
    }
}

#[test]
fn test_procedure_with_while_loop() {
    let sql = "CREATE PROCEDURE count_up(IN max_count INT) BEGIN DECLARE i INT DEFAULT 0; WHILE i < max_count DO SET i = i + 1; END WHILE; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateProcedure(stmt)) = result {
        assert_eq!(stmt.procedure_name, "count_up");
    } else {
        panic!("Expected CreateProcedure statement");
    }
}

#[test]
fn test_procedure_with_declare() {
    let sql = "CREATE PROCEDURE test_declare() BEGIN DECLARE var1 INT DEFAULT 10; DECLARE var2 VARCHAR(50) DEFAULT 'hello'; SELECT var1, var2; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateProcedure(stmt)) = result {
        assert_eq!(stmt.procedure_name, "test_declare");
    } else {
        panic!("Expected CreateProcedure statement");
    }
}

#[test]
fn test_call_with_multiple_arguments() {
    let sql = "CALL update_status(123, 'active', NULL);";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::Call(stmt)) = result {
        assert_eq!(stmt.procedure_name, "update_status");
        assert_eq!(stmt.arguments.len(), 3);
    } else {
        panic!("Expected Call statement");
    }
}

#[test]
fn test_procedure_with_set_statement() {
    let sql = "CREATE PROCEDURE add_one() BEGIN DECLARE counter INT DEFAULT 0; SET counter = counter + 1; SELECT counter; END;";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    if let Ok(vibesql_ast::Statement::CreateProcedure(stmt)) = result {
        assert_eq!(stmt.procedure_name, "add_one");
    } else {
        panic!("Expected CreateProcedure statement");
    }
}
