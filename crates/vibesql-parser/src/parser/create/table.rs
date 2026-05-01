//! CREATE TABLE parsing

use vibesql_types::DataType;

use super::super::*;

impl Parser {
    /// Parse CREATE TABLE statement
    /// Supports CREATE [TEMP|TEMPORARY] TABLE syntax (SQLite compatibility)
    pub(in crate::parser) fn parse_create_table_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateTableStmt, ParseError> {
        self.expect_keyword(Keyword::Create)?;

        // Check for TEMP or TEMPORARY keyword (SQLite compatibility)
        // Temporary tables are stored in a separate "temp" schema
        let temporary = if self.peek_keyword(Keyword::Temp) || self.peek_keyword(Keyword::Temporary)
        {
            self.advance();
            true
        } else {
            false
        };

        self.expect_keyword(Keyword::Table)?;

        // Check for optional IF NOT EXISTS clause
        let if_not_exists = if self.peek_keyword(Keyword::If) {
            self.advance(); // consume IF
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse table name with quoted flag (supports schema.table)
        let table = self.parse_table_ref()?;

        // Check for CREATE TABLE ... AS SELECT syntax
        if self.peek_keyword(Keyword::As) {
            self.advance(); // consume AS
                            // Parse the SELECT statement
            let select_stmt = self.parse_select_statement()?;

            // Expect semicolon or EOF
            if matches!(self.peek(), Token::Semicolon) {
                self.advance();
            }

            return Ok(vibesql_ast::CreateTableStmt {
                temporary,
                if_not_exists,
                table_name: table.full_name(),
                columns: Vec::new(),
                table_constraints: Vec::new(),
                table_options: Vec::new(),
                quoted: table.is_any_quoted(),
                as_query: Some(Box::new(select_stmt)),
                without_rowid: false, // CREATE TABLE AS SELECT cannot be WITHOUT ROWID
            });
        }

        // Parse column definitions and table constraints
        self.expect_token(Token::LParen)?;
        let mut columns = Vec::new();
        let mut table_constraints = Vec::new();

        loop {
            // Check if this is a table-level constraint (including CONSTRAINT keyword)
            if self.peek_keyword(Keyword::Constraint)
                || self.peek_keyword(Keyword::Primary)
                || self.peek_keyword(Keyword::Foreign)
                || self.peek_keyword(Keyword::Unique)
                || self.peek_keyword(Keyword::Check)
                || self.peek_keyword(Keyword::Fulltext)
            {
                table_constraints.push(self.parse_table_constraint()?);
                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                    continue;
                } else {
                    break;
                }
            }

            // Parse column name
            let name = match self.peek() {
                Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                    let c = col.clone();
                    self.advance();
                    c
                }
                // Allow unreserved keywords (like TIMESTAMP, DATE, TIME, INTERVAL) as column names
                // SQL:1999: normalize to lowercase when used as unquoted identifier
                Token::Keyword { keyword: kw, .. } if kw.can_be_identifier() => {
                    let col_name = format!("{}", kw).to_lowercase();
                    self.advance();
                    col_name
                }
                _ => return Err(ParseError { message: "Expected column name".to_string() }),
            };

            // Parse data type (optional for SQLite compatibility)
            // SQLite allows typeless columns with BLOB affinity
            // Also check for generated column syntax: AS(expression) or GENERATED ALWAYS AS(expression)
            let (data_type, is_exact_integer_type, mut generated_expr) =
                if self.peek_keyword(Keyword::As) {
                    // Generated column (short form): AS(expression)
                    self.advance(); // consume AS
                    self.expect_token(Token::LParen)?;
                    let expr = self.parse_expression()?;
                    self.expect_token(Token::RParen)?;
                    // Skip optional STORED or VIRTUAL keyword
                    if self.peek_keyword(Keyword::Stored) || self.peek_keyword(Keyword::Virtual) {
                        self.advance();
                    }
                    // Generated columns have the type inferred from the expression
                    // For now, use BLOB affinity (will be refined by type inference)
                    (DataType::BinaryLargeObject, false, Some(Box::new(expr)))
                } else if self.is_column_constraint_or_separator() {
                    // No data type specified - use BLOB affinity (SQLite default)
                    (DataType::BinaryLargeObject, false, None)
                } else {
                    let (dt, is_int) = self.parse_data_type_with_integer_flag()?;
                    (dt, is_int, None)
                };

            // Check for GENERATED ALWAYS AS (expression) [STORED|VIRTUAL] after data type
            if generated_expr.is_none() && self.peek_keyword(Keyword::Generated) {
                self.advance(); // consume GENERATED
                self.expect_keyword(Keyword::Always)?;
                self.expect_keyword(Keyword::As)?;
                self.expect_token(Token::LParen)?;
                let expr = self.parse_expression()?;
                self.expect_token(Token::RParen)?;
                // Skip optional STORED or VIRTUAL keyword
                if self.peek_keyword(Keyword::Stored) || self.peek_keyword(Keyword::Virtual) {
                    self.advance();
                }
                generated_expr = Some(Box::new(expr));
            }

            // Also check for short form AS (expression) after data type
            if generated_expr.is_none() && self.peek_keyword(Keyword::As) {
                self.advance(); // consume AS
                self.expect_token(Token::LParen)?;
                let expr = self.parse_expression()?;
                self.expect_token(Token::RParen)?;
                // Skip optional STORED or VIRTUAL keyword
                if self.peek_keyword(Keyword::Stored) || self.peek_keyword(Keyword::Virtual) {
                    self.advance();
                }
                generated_expr = Some(Box::new(expr));
            }

            // Parse optional DEFAULT clause (before COMMENT, per MySQL standard).
            // SQLite also allows DEFAULT to appear interleaved with column constraints
            // (e.g. `idx UNIQUE DEFAULT NULL`), so `parse_column_constraints` below may
            // also consume a DEFAULT clause and return it via its second tuple element.
            let mut default_value = if self.peek_keyword(Keyword::Default) {
                self.advance(); // consume DEFAULT
                Some(Box::new(self.parse_expression()?))
            } else {
                None
            };

            // Parse optional COMMENT clause (after DEFAULT, per MySQL standard)
            let comment = if self.peek_keyword(Keyword::Comment) {
                self.advance(); // consume COMMENT
                match self.peek() {
                    Token::String(s) => {
                        let c = s.clone();
                        self.advance();
                        Some(c)
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected string literal after COMMENT".to_string(),
                        })
                    }
                }
            } else {
                None
            };

            // Parse column constraints (which may include NOT NULL and an interleaved DEFAULT)
            let (constraints, mid_default) = self.parse_column_constraints()?;
            if let Some(d) = mid_default {
                if default_value.is_some() {
                    return Err(ParseError {
                        message: "Multiple DEFAULT clauses for the same column".to_string(),
                    });
                }
                default_value = Some(d);
            }

            // Determine nullability based on constraints
            let nullable = !constraints
                .iter()
                .any(|c| matches!(&c.kind, vibesql_ast::ColumnConstraintKind::NotNull));

            columns.push(vibesql_ast::ColumnDef {
                name,
                data_type,
                nullable,
                constraints,
                default_value,
                comment,
                generated_expr,
                is_exact_integer_type,
            });

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        self.expect_token(Token::RParen)?;

        // Parse optional table options (MySQL extensions)
        let table_options = self.parse_table_options()?;

        // Parse optional WITH OIDS / WITHOUT OIDS clause (PostgreSQL)
        // or WITHOUT ROWID clause (SQLite)
        let mut without_rowid = false;
        if self.peek_keyword(Keyword::With) {
            self.advance(); // consume WITH
            self.expect_keyword(Keyword::Oids)?;
        } else if self.peek_keyword(Keyword::Without) {
            self.advance(); // consume WITHOUT
            if self.peek_keyword(Keyword::Oids) {
                self.advance(); // consume OIDS
            } else if self.peek_keyword(Keyword::Rowid) {
                self.advance(); // consume ROWID (SQLite WITHOUT ROWID tables)
                without_rowid = true;
            } else {
                return Err(ParseError {
                    message: "Expected OIDS or ROWID after WITHOUT".to_string(),
                });
            }
        }

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::CreateTableStmt {
            temporary,
            if_not_exists,
            table_name: table.full_name(),
            columns,
            table_constraints,
            table_options,
            quoted: table.is_any_quoted(),
            as_query: None,
            without_rowid,
        })
    }

    /// Check if the current token indicates a column constraint or column separator
    /// rather than a data type. This enables SQLite-style typeless column definitions.
    ///
    /// Returns true if the next token is:
    /// - A column separator: `,` or `)`
    /// - A column constraint keyword: `NULL`, `NOT`, `PRIMARY`, `UNIQUE`, `CHECK`, `REFERENCES`,
    ///   `AUTO_INCREMENT`, `AUTOINCREMENT`, `KEY`, `CONSTRAINT`
    /// - A column modifier: `DEFAULT`, `COMMENT`
    fn is_column_constraint_or_separator(&self) -> bool {
        match self.peek() {
            // Column separators
            Token::Comma | Token::RParen => true,

            // Column constraint keywords
            Token::Keyword { keyword: Keyword::Null, .. } => true,
            Token::Keyword { keyword: Keyword::Not, .. } => true,
            Token::Keyword { keyword: Keyword::Primary, .. } => true,
            Token::Keyword { keyword: Keyword::Unique, .. } => true,
            Token::Keyword { keyword: Keyword::Check, .. } => true,
            Token::Keyword { keyword: Keyword::References, .. } => true,
            Token::Keyword { keyword: Keyword::AutoIncrement, .. } => true,
            Token::Keyword { keyword: Keyword::Key, .. } => true,
            Token::Keyword { keyword: Keyword::Constraint, .. } => true,

            // Column modifiers (appear after data type normally)
            Token::Keyword { keyword: Keyword::Default, .. } => true,
            Token::Keyword { keyword: Keyword::Comment, .. } => true,
            Token::Keyword { keyword: Keyword::Collate, .. } => true,

            // Generated column keywords (SQLite)
            Token::Keyword { keyword: Keyword::Generated, .. } => true,
            Token::Keyword { keyword: Keyword::As, .. } => true,

            // Not a constraint/separator - likely a data type
            _ => false,
        }
    }
}
