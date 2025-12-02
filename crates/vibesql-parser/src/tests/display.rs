//! Tests for Display trait implementations.

use crate::{keywords::Keyword, lexer::LexerError, token::MultiCharOperator, token::Token};

#[test]
fn test_keyword_display_select() {
    assert_eq!(format!("{}", Keyword::Select), "SELECT");
}

#[test]
fn test_keyword_display_distinct() {
    assert_eq!(format!("{}", Keyword::Distinct), "DISTINCT");
}

#[test]
fn test_keyword_display_from() {
    assert_eq!(format!("{}", Keyword::From), "FROM");
}

#[test]
fn test_keyword_display_where() {
    assert_eq!(format!("{}", Keyword::Where), "WHERE");
}

#[test]
fn test_keyword_display_insert() {
    assert_eq!(format!("{}", Keyword::Insert), "INSERT");
}

#[test]
fn test_keyword_display_into() {
    assert_eq!(format!("{}", Keyword::Into), "INTO");
}

#[test]
fn test_keyword_display_update() {
    assert_eq!(format!("{}", Keyword::Update), "UPDATE");
}

#[test]
fn test_keyword_display_delete() {
    assert_eq!(format!("{}", Keyword::Delete), "DELETE");
}

#[test]
fn test_keyword_display_create() {
    assert_eq!(format!("{}", Keyword::Create), "CREATE");
}

#[test]
fn test_keyword_display_table() {
    assert_eq!(format!("{}", Keyword::Table), "TABLE");
}

#[test]
fn test_keyword_display_drop() {
    assert_eq!(format!("{}", Keyword::Drop), "DROP");
}

#[test]
fn test_keyword_display_alter() {
    assert_eq!(format!("{}", Keyword::Alter), "ALTER");
}

#[test]
fn test_keyword_display_and() {
    assert_eq!(format!("{}", Keyword::And), "AND");
}

#[test]
fn test_keyword_display_or() {
    assert_eq!(format!("{}", Keyword::Or), "OR");
}

#[test]
fn test_keyword_display_not() {
    assert_eq!(format!("{}", Keyword::Not), "NOT");
}

#[test]
fn test_keyword_display_null() {
    assert_eq!(format!("{}", Keyword::Null), "NULL");
}

#[test]
fn test_keyword_display_true() {
    assert_eq!(format!("{}", Keyword::True), "TRUE");
}

#[test]
fn test_keyword_display_false() {
    assert_eq!(format!("{}", Keyword::False), "FALSE");
}

#[test]
fn test_keyword_display_as() {
    assert_eq!(format!("{}", Keyword::As), "AS");
}

#[test]
fn test_keyword_display_join() {
    assert_eq!(format!("{}", Keyword::Join), "JOIN");
}

#[test]
fn test_keyword_display_left() {
    assert_eq!(format!("{}", Keyword::Left), "LEFT");
}

#[test]
fn test_keyword_display_right() {
    assert_eq!(format!("{}", Keyword::Right), "RIGHT");
}

#[test]
fn test_keyword_display_inner() {
    assert_eq!(format!("{}", Keyword::Inner), "INNER");
}

#[test]
fn test_keyword_display_outer() {
    assert_eq!(format!("{}", Keyword::Outer), "OUTER");
}

#[test]
fn test_keyword_display_on() {
    assert_eq!(format!("{}", Keyword::On), "ON");
}

#[test]
fn test_keyword_display_group() {
    assert_eq!(format!("{}", Keyword::Group), "GROUP");
}

#[test]
fn test_keyword_display_by() {
    assert_eq!(format!("{}", Keyword::By), "BY");
}

#[test]
fn test_keyword_display_having() {
    assert_eq!(format!("{}", Keyword::Having), "HAVING");
}

#[test]
fn test_keyword_display_order() {
    assert_eq!(format!("{}", Keyword::Order), "ORDER");
}

#[test]
fn test_keyword_display_asc() {
    assert_eq!(format!("{}", Keyword::Asc), "ASC");
}

#[test]
fn test_keyword_display_desc() {
    assert_eq!(format!("{}", Keyword::Desc), "DESC");
}

#[test]
fn test_keyword_display_limit() {
    assert_eq!(format!("{}", Keyword::Limit), "LIMIT");
}

#[test]
fn test_keyword_display_offset() {
    assert_eq!(format!("{}", Keyword::Offset), "OFFSET");
}

#[test]
fn test_keyword_display_set() {
    assert_eq!(format!("{}", Keyword::Set), "SET");
}

#[test]
fn test_keyword_display_values() {
    assert_eq!(format!("{}", Keyword::Values), "VALUES");
}

#[test]
fn test_keyword_display_in() {
    assert_eq!(format!("{}", Keyword::In), "IN");
}

#[test]
fn test_token_display_keyword() {
    let token = Token::Keyword(Keyword::Select);
    assert_eq!(format!("{}", token), "Keyword(SELECT)");
}

#[test]
fn test_token_display_identifier() {
    let token = Token::Identifier("users".to_string());
    assert_eq!(format!("{}", token), "Identifier(users)");
}

#[test]
fn test_token_display_number() {
    let token = Token::Number("42".to_string());
    assert_eq!(format!("{}", token), "Number(42)");
}

#[test]
fn test_token_display_string() {
    let token = Token::String("hello".to_string());
    assert_eq!(format!("{}", token), "String('hello')");
}

#[test]
fn test_token_display_symbol() {
    let token = Token::Symbol('+');
    assert_eq!(format!("{}", token), "Symbol(+)");
}

#[test]
fn test_token_display_operator() {
    let token = Token::Operator(MultiCharOperator::LessEqual);
    assert_eq!(format!("{}", token), "Operator(<=)");
}

#[test]
fn test_token_display_semicolon() {
    let token = Token::Semicolon;
    assert_eq!(format!("{}", token), "Semicolon");
}

#[test]
fn test_token_display_comma() {
    let token = Token::Comma;
    assert_eq!(format!("{}", token), "Comma");
}

#[test]
fn test_token_display_lparen() {
    let token = Token::LParen;
    assert_eq!(format!("{}", token), "LParen");
}

#[test]
fn test_token_display_rparen() {
    let token = Token::RParen;
    assert_eq!(format!("{}", token), "RParen");
}

#[test]
fn test_token_display_eof() {
    let token = Token::Eof;
    assert_eq!(format!("{}", token), "Eof");
}

#[test]
fn test_lexer_error_display() {
    let error = LexerError { message: "Unexpected character".to_string(), position: 42 };
    assert_eq!(format!("{}", error), "Lexer error at position 42: Unexpected character");
}
