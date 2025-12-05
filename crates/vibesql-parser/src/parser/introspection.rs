//! Database introspection statement parsing (SHOW, DESCRIBE)

use super::{ParseError, Parser};
use crate::keywords::Keyword;
use crate::token::Token;

impl Parser {
    /// Helper to parse a string literal
    fn parse_string(&mut self) -> Result<String, ParseError> {
        match self.peek() {
            Token::String(s) => {
                let string_val = s.clone();
                self.advance();
                Ok(string_val)
            }
            _ => Err(ParseError {
                message: format!("Expected string literal, found {:?}", self.peek()),
            }),
        }
    }

    /// Parse SHOW statement
    pub fn parse_show_statement(&mut self) -> Result<vibesql_ast::Statement, ParseError> {
        self.expect_keyword(Keyword::Show)?;

        // Determine which SHOW variant
        match self.peek() {
            Token::Keyword(Keyword::Tables) => {
                Ok(vibesql_ast::Statement::ShowTables(self.parse_show_tables()?))
            }
            Token::Keyword(Keyword::Databases) => {
                Ok(vibesql_ast::Statement::ShowDatabases(self.parse_show_databases()?))
            }
            Token::Keyword(Keyword::Full) => {
                // SHOW FULL COLUMNS - handle FULL modifier for SHOW COLUMNS
                Ok(vibesql_ast::Statement::ShowColumns(self.parse_show_columns()?))
            }
            Token::Keyword(Keyword::Columns) | Token::Keyword(Keyword::Fields) => {
                Ok(vibesql_ast::Statement::ShowColumns(self.parse_show_columns()?))
            }
            Token::Keyword(Keyword::Index)
            | Token::Keyword(Keyword::Indexes)
            | Token::Keyword(Keyword::Keys) => {
                Ok(vibesql_ast::Statement::ShowIndex(self.parse_show_index()?))
            }
            Token::Keyword(Keyword::Create) => {
                self.advance(); // consume CREATE
                if self.peek_keyword(Keyword::Table) {
                    Ok(vibesql_ast::Statement::ShowCreateTable(self.parse_show_create_table()?))
                } else {
                    Err(ParseError { message: "Expected TABLE after SHOW CREATE".to_string() })
                }
            }
            _ => Err(ParseError {
                message: format!(
                    "Expected TABLES, DATABASES, COLUMNS, INDEX, or CREATE after SHOW, found {:?}",
                    self.peek()
                ),
            }),
        }
    }

    /// Parse SHOW TABLES [FROM database] [LIKE pattern | WHERE expr]
    fn parse_show_tables(&mut self) -> Result<vibesql_ast::ShowTablesStmt, ParseError> {
        self.expect_keyword(Keyword::Tables)?;

        // Optional FROM database
        let database = if self.peek_keyword(Keyword::From) {
            self.advance();
            Some(self.parse_identifier()?)
        } else {
            None
        };

        // Optional LIKE pattern
        let like_pattern = if self.peek_keyword(Keyword::Like) {
            self.advance();
            Some(self.parse_string()?)
        } else {
            None
        };

        // Optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(vibesql_ast::ShowTablesStmt { database, like_pattern, where_clause })
    }

    /// Parse SHOW DATABASES [LIKE pattern | WHERE expr]
    fn parse_show_databases(&mut self) -> Result<vibesql_ast::ShowDatabasesStmt, ParseError> {
        self.expect_keyword(Keyword::Databases)?;

        // Optional LIKE pattern
        let like_pattern = if self.peek_keyword(Keyword::Like) {
            self.advance();
            Some(self.parse_string()?)
        } else {
            None
        };

        // Optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(vibesql_ast::ShowDatabasesStmt { like_pattern, where_clause })
    }

    /// Parse SHOW [FULL] COLUMNS FROM table [FROM database] [LIKE pattern | WHERE expr]
    fn parse_show_columns(&mut self) -> Result<vibesql_ast::ShowColumnsStmt, ParseError> {
        // Check for FULL modifier
        let full = if self.peek_keyword(Keyword::Full) {
            self.advance();
            true
        } else {
            false
        };

        // COLUMNS or FIELDS (synonyms)
        if !self.peek_keyword(Keyword::Columns) && !self.peek_keyword(Keyword::Fields) {
            return Err(ParseError { message: "Expected COLUMNS or FIELDS".to_string() });
        }
        self.advance();

        // FROM table_name
        self.expect_keyword(Keyword::From)?;
        let table_name = self.parse_identifier()?;

        // Optional FROM database (second FROM)
        let database = if self.peek_keyword(Keyword::From) {
            self.advance();
            Some(self.parse_identifier()?)
        } else {
            None
        };

        // Optional LIKE pattern
        let like_pattern = if self.peek_keyword(Keyword::Like) {
            self.advance();
            Some(self.parse_string()?)
        } else {
            None
        };

        // Optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(vibesql_ast::ShowColumnsStmt { table_name, database, full, like_pattern, where_clause })
    }

    /// Parse SHOW INDEX FROM table [FROM database]
    fn parse_show_index(&mut self) -> Result<vibesql_ast::ShowIndexStmt, ParseError> {
        // INDEX, INDEXES, or KEYS (synonyms)
        self.advance(); // consume the keyword

        // FROM table_name
        self.expect_keyword(Keyword::From)?;
        let table_name = self.parse_identifier()?;

        // Optional FROM database
        let database = if self.peek_keyword(Keyword::From) {
            self.advance();
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(vibesql_ast::ShowIndexStmt { table_name, database })
    }

    /// Parse SHOW CREATE TABLE table_name
    fn parse_show_create_table(&mut self) -> Result<vibesql_ast::ShowCreateTableStmt, ParseError> {
        self.expect_keyword(Keyword::Table)?;
        let table_name = self.parse_identifier()?;

        Ok(vibesql_ast::ShowCreateTableStmt { table_name })
    }

    /// Parse DESCRIBE table [column_pattern]
    pub fn parse_describe_statement(&mut self) -> Result<vibesql_ast::DescribeStmt, ParseError> {
        self.expect_keyword(Keyword::Describe)?;

        let table_name = self.parse_identifier()?;

        // Optional column name or pattern
        let column_pattern = if matches!(self.peek(), Token::Identifier(_) | Token::String(_)) {
            Some(match self.peek() {
                Token::String(_) => self.parse_string()?,
                _ => self.parse_identifier()?,
            })
        } else {
            None
        };

        Ok(vibesql_ast::DescribeStmt { table_name, column_pattern })
    }

    /// Parse EXPLAIN [ANALYZE] statement
    ///
    /// Syntax: EXPLAIN [ANALYZE] [FORMAT {TEXT | JSON}] statement
    pub fn parse_explain_statement(&mut self) -> Result<vibesql_ast::ExplainStmt, ParseError> {
        self.expect_keyword(Keyword::Explain)?;

        // Check for ANALYZE option
        let analyze = if matches!(self.peek(), Token::Keyword(Keyword::Analyze)) {
            self.advance();
            true
        } else {
            false
        };

        // Check for FORMAT option (optional)
        let format = if matches!(self.peek(), Token::Identifier(ref s) if s.to_uppercase() == "FORMAT")
        {
            self.advance(); // consume FORMAT
            match self.peek() {
                Token::Identifier(ref s) if s.to_uppercase() == "TEXT" => {
                    self.advance();
                    vibesql_ast::ExplainFormat::Text
                }
                Token::Identifier(ref s) if s.to_uppercase() == "JSON" => {
                    self.advance();
                    vibesql_ast::ExplainFormat::Json
                }
                _ => {
                    return Err(ParseError {
                        message: format!(
                            "Expected TEXT or JSON after FORMAT, found {:?}",
                            self.peek()
                        ),
                    });
                }
            }
        } else {
            vibesql_ast::ExplainFormat::Text
        };

        // Parse the inner statement (SELECT, INSERT, UPDATE, DELETE)
        let statement = match self.peek() {
            Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::With) => {
                let select_stmt = self.parse_select_statement()?;
                vibesql_ast::Statement::Select(Box::new(select_stmt))
            }
            Token::Keyword(Keyword::Insert) => {
                let insert_stmt = self.parse_insert_statement()?;
                vibesql_ast::Statement::Insert(insert_stmt)
            }
            Token::Keyword(Keyword::Update) => {
                let update_stmt = self.parse_update_statement()?;
                vibesql_ast::Statement::Update(update_stmt)
            }
            Token::Keyword(Keyword::Delete) => {
                let delete_stmt = self.parse_delete_statement()?;
                vibesql_ast::Statement::Delete(delete_stmt)
            }
            _ => {
                return Err(ParseError {
                    message: format!(
                        "EXPLAIN requires SELECT, INSERT, UPDATE, or DELETE statement, found {:?}",
                        self.peek()
                    ),
                });
            }
        };

        Ok(vibesql_ast::ExplainStmt { statement: Box::new(statement), format, analyze })
    }
}
