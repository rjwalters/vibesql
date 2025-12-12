//! Tests for SET statement parsing

use crate::{Lexer, Parser};

#[test]
fn test_parse_set_catalog() {
    let sql = "SET CATALOG my_catalog";
    let tokens = Lexer::new(sql).tokenize().unwrap();
    eprintln!("Tokens: {:?}", tokens);
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        vibesql_ast::Statement::SetCatalog(set_stmt) => {
            assert_eq!(set_stmt.catalog_name, "my_catalog");
        }
        _ => panic!("Expected SetCatalog statement"),
    }
}

#[test]
fn test_parse_set_names_simple() {
    let sql = "SET NAMES 'UTF8'";
    let tokens = Lexer::new(sql).tokenize().unwrap();
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        vibesql_ast::Statement::SetNames(set_stmt) => {
            assert_eq!(set_stmt.charset_name, "UTF8");
            assert_eq!(set_stmt.collation, None);
        }
        _ => panic!("Expected SetNames statement"),
    }
}

#[test]
fn test_parse_set_names_with_collation() {
    let sql = "SET NAMES 'UTF8' COLLATION 'en_US'";
    let tokens = Lexer::new(sql).tokenize().unwrap();
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        vibesql_ast::Statement::SetNames(set_stmt) => {
            assert_eq!(set_stmt.charset_name, "UTF8");
            assert_eq!(set_stmt.collation, Some("en_US".to_string()));
        }
        _ => panic!("Expected SetNames statement"),
    }
}

#[test]
fn test_parse_set_time_zone_local() {
    let sql = "SET TIME ZONE LOCAL";
    let tokens = Lexer::new(sql).tokenize().unwrap();
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        vibesql_ast::Statement::SetTimeZone(set_stmt) => match set_stmt.zone {
            vibesql_ast::TimeZoneSpec::Local => {}
            _ => panic!("Expected Local time zone"),
        },
        _ => panic!("Expected SetTimeZone statement"),
    }
}

#[test]
fn test_parse_set_time_zone_interval() {
    let sql = "SET TIME ZONE INTERVAL '+05:00' HOUR TO MINUTE";
    let tokens = Lexer::new(sql).tokenize().unwrap();
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_statement().unwrap();

    match stmt {
        vibesql_ast::Statement::SetTimeZone(set_stmt) => match set_stmt.zone {
            vibesql_ast::TimeZoneSpec::Interval(ref interval) => {
                assert_eq!(interval, "+05:00");
            }
            _ => panic!("Expected Interval time zone"),
        },
        _ => panic!("Expected SetTimeZone statement"),
    }
}
