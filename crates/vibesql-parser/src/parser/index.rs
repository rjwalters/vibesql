//! Parser for CREATE INDEX, DROP INDEX, REINDEX, and PRAGMA statements

use super::{ParseError, Parser};
use crate::{keywords::Keyword, token::Token};

impl Parser {
    /// Parse CREATE INDEX statement
    ///
    /// Syntax:
    ///   CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON table_name (column_list)
    ///   CREATE FULLTEXT INDEX [IF NOT EXISTS] index_name ON table_name (column_list)
    ///   CREATE SPATIAL INDEX [IF NOT EXISTS] index_name ON table_name (column_list)
    ///   CREATE INDEX [IF NOT EXISTS] index_name ON table_name USING ivfflat (column [ops]) [WITH
    /// (lists = N)]
    pub(super) fn parse_create_index_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateIndexStmt, ParseError> {
        // Expect CREATE keyword
        self.expect_keyword(Keyword::Create)?;

        // Check for FULLTEXT keyword
        if self.peek_keyword(Keyword::Fulltext) {
            self.advance(); // consume FULLTEXT

            // Expect INDEX keyword
            self.expect_keyword(Keyword::Index)?;

            return self.parse_create_index_columns(vibesql_ast::IndexType::Fulltext);
        }

        // Check for SPATIAL keyword
        if self.peek_keyword(Keyword::Spatial) {
            self.advance(); // consume SPATIAL

            // Expect INDEX keyword
            self.expect_keyword(Keyword::Index)?;

            return self.parse_create_index_columns(vibesql_ast::IndexType::Spatial);
        }

        // Check for optional UNIQUE keyword
        let unique = if self.peek_keyword(Keyword::Unique) {
            self.advance(); // consume UNIQUE
            true
        } else {
            false
        };

        // Expect INDEX keyword
        self.expect_keyword(Keyword::Index)?;

        // For UNIQUE indexes, we don't support USING clause yet
        if unique {
            return self.parse_create_index_columns(vibesql_ast::IndexType::BTree { unique: true });
        }

        // Check if this uses USING clause for index method specification
        // First, parse IF NOT EXISTS and index name, then check for USING
        self.parse_create_index_with_using_clause()
    }

    /// Parse CREATE INDEX with potential USING clause
    fn parse_create_index_with_using_clause(
        &mut self,
    ) -> Result<vibesql_ast::CreateIndexStmt, ParseError> {
        // Check for optional IF NOT EXISTS clause
        let if_not_exists = if self.peek_keyword(Keyword::If) {
            self.advance(); // consume IF
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse index name
        let index_name = self.parse_identifier()?;

        // Expect ON keyword
        self.expect_keyword(Keyword::On)?;

        // Parse table name
        let table_name = self.parse_identifier()?;

        // Check for USING clause (PostgreSQL-style index method specification)
        if self.peek_keyword(Keyword::Using) {
            self.advance(); // consume USING

            // Parse index method
            if self.peek_keyword(Keyword::Ivfflat) {
                self.advance(); // consume IVFFLAT
                return self.parse_ivfflat_index(if_not_exists, index_name, table_name);
            } else if self.peek_keyword(Keyword::Hnsw) {
                self.advance(); // consume HNSW
                return self.parse_hnsw_index(if_not_exists, index_name, table_name);
            } else {
                // Unknown index method - could extend for BTREE, HASH, etc.
                return Err(ParseError {
                    message: "Unsupported index method. Supported: IVFFLAT, HNSW".to_string(),
                });
            }
        }

        // No USING clause - parse as standard B-tree index
        // Expect opening parenthesis
        self.expect_token(Token::LParen)?;

        // Parse column list
        let columns = self.parse_index_column_list()?;

        // Expect closing parenthesis
        self.expect_token(Token::RParen)?;

        // Optional WHERE clause for partial indexes (SQLite syntax).
        // Storage support is pending; we only capture the expression so
        // downstream validation can reject window functions in the predicate.
        let where_clause = self.parse_optional_index_where_clause()?;

        Ok(vibesql_ast::CreateIndexStmt {
            if_not_exists,
            index_name,
            table_name,
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns,
            where_clause,
        })
    }

    /// Parse IVFFlat index specifics
    ///
    /// Syntax: USING ivfflat (column [vector_l2_ops|vector_cosine_ops|vector_ip_ops]) [WITH (lists
    /// = N)]
    fn parse_ivfflat_index(
        &mut self,
        if_not_exists: bool,
        index_name: String,
        table_name: String,
    ) -> Result<vibesql_ast::CreateIndexStmt, ParseError> {
        // Expect opening parenthesis
        self.expect_token(Token::LParen)?;

        // Parse column name
        let column_name = self.parse_identifier()?;

        // Parse optional operator class (vector_l2_ops, vector_cosine_ops, vector_ip_ops)
        let metric = if let Token::Identifier(ident) = self.peek().clone() {
            let upper = ident.to_uppercase();
            match upper.as_str() {
                "VECTOR_L2_OPS" => {
                    self.advance();
                    vibesql_ast::VectorDistanceMetric::L2
                }
                "VECTOR_COSINE_OPS" => {
                    self.advance();
                    vibesql_ast::VectorDistanceMetric::Cosine
                }
                "VECTOR_IP_OPS" => {
                    self.advance();
                    vibesql_ast::VectorDistanceMetric::InnerProduct
                }
                _ => vibesql_ast::VectorDistanceMetric::L2, // Default to L2
            }
        } else {
            vibesql_ast::VectorDistanceMetric::L2 // Default to L2
        };

        // Expect closing parenthesis
        self.expect_token(Token::RParen)?;

        // Parse optional WITH clause for index parameters
        let lists = if self.peek_keyword(Keyword::With) {
            self.advance(); // consume WITH
            self.expect_token(Token::LParen)?;

            // Parse lists = N
            let mut lists_value = 100u32; // Default
            loop {
                if self.peek_keyword(Keyword::Lists) {
                    self.advance(); // consume LISTS
                    self.expect_token(Token::Symbol('='))?;
                    lists_value = self.parse_positive_integer()? as u32;
                } else if let Token::Identifier(ident) = self.peek().clone() {
                    if ident.to_uppercase() == "LISTS" {
                        self.advance();
                        self.expect_token(Token::Symbol('='))?;
                        lists_value = self.parse_positive_integer()? as u32;
                    } else {
                        return Err(ParseError {
                            message: format!("Unknown IVFFlat parameter: {}", ident),
                        });
                    }
                } else {
                    break;
                }

                // Check for comma to continue
                if self.peek() == &Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }

            self.expect_token(Token::RParen)?;
            lists_value
        } else {
            100 // Default number of lists
        };

        // Validate lists parameter
        if lists < 1 {
            return Err(ParseError {
                message: "IVFFlat 'lists' parameter must be at least 1".to_string(),
            });
        }

        let columns = vec![vibesql_ast::IndexColumn::Column {
            column_name,
            direction: vibesql_ast::OrderDirection::Asc, // Not meaningful for vector indexes
            prefix_length: None,
        }];

        Ok(vibesql_ast::CreateIndexStmt {
            if_not_exists,
            index_name,
            table_name,
            index_type: vibesql_ast::IndexType::IVFFlat { metric, lists },
            columns,
            where_clause: None,
        })
    }

    /// Parse HNSW index specifics
    ///
    /// Syntax: USING hnsw (column [vector_l2_ops|vector_cosine_ops|vector_ip_ops]) [WITH (m = N,
    /// ef_construction = N)]
    fn parse_hnsw_index(
        &mut self,
        if_not_exists: bool,
        index_name: String,
        table_name: String,
    ) -> Result<vibesql_ast::CreateIndexStmt, ParseError> {
        // Expect opening parenthesis
        self.expect_token(Token::LParen)?;

        // Parse column name
        let column_name = self.parse_identifier()?;

        // Parse optional operator class (vector_l2_ops, vector_cosine_ops, vector_ip_ops)
        let metric = if let Token::Identifier(ident) = self.peek().clone() {
            let upper = ident.to_uppercase();
            match upper.as_str() {
                "VECTOR_L2_OPS" => {
                    self.advance();
                    vibesql_ast::VectorDistanceMetric::L2
                }
                "VECTOR_COSINE_OPS" => {
                    self.advance();
                    vibesql_ast::VectorDistanceMetric::Cosine
                }
                "VECTOR_IP_OPS" => {
                    self.advance();
                    vibesql_ast::VectorDistanceMetric::InnerProduct
                }
                _ => vibesql_ast::VectorDistanceMetric::Cosine, // Default to Cosine for HNSW
            }
        } else {
            vibesql_ast::VectorDistanceMetric::Cosine // Default to Cosine for HNSW
        };

        // Expect closing parenthesis
        self.expect_token(Token::RParen)?;

        // Parse optional WITH clause for index parameters
        let mut m = 16u32; // Default m
        let mut ef_construction = 64u32; // Default ef_construction

        if self.peek_keyword(Keyword::With) {
            self.advance(); // consume WITH
            self.expect_token(Token::LParen)?;

            loop {
                // Check for M parameter
                if self.peek_keyword(Keyword::M) {
                    self.advance(); // consume M
                    self.expect_token(Token::Symbol('='))?;
                    m = self.parse_positive_integer()? as u32;
                }
                // Check for EF_CONSTRUCTION parameter
                else if self.peek_keyword(Keyword::EfConstruction) {
                    self.advance(); // consume EF_CONSTRUCTION
                    self.expect_token(Token::Symbol('='))?;
                    ef_construction = self.parse_positive_integer()? as u32;
                }
                // Also accept lowercase/mixed case identifiers
                else if let Token::Identifier(ident) = self.peek().clone() {
                    let upper = ident.to_uppercase();
                    match upper.as_str() {
                        "M" => {
                            self.advance();
                            self.expect_token(Token::Symbol('='))?;
                            m = self.parse_positive_integer()? as u32;
                        }
                        "EF_CONSTRUCTION" => {
                            self.advance();
                            self.expect_token(Token::Symbol('='))?;
                            ef_construction = self.parse_positive_integer()? as u32;
                        }
                        _ => {
                            return Err(ParseError {
                                message: format!("Unknown HNSW parameter: {}. Valid parameters: m, ef_construction", ident),
                            });
                        }
                    }
                } else {
                    break;
                }

                // Check for comma to continue
                if self.peek() == &Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }

            self.expect_token(Token::RParen)?;
        }

        // Validate parameters
        if m < 2 {
            return Err(ParseError {
                message: "HNSW 'm' parameter must be at least 2".to_string(),
            });
        }
        if ef_construction < 1 {
            return Err(ParseError {
                message: "HNSW 'ef_construction' parameter must be at least 1".to_string(),
            });
        }

        let columns = vec![vibesql_ast::IndexColumn::Column {
            column_name,
            direction: vibesql_ast::OrderDirection::Asc, // Not meaningful for vector indexes
            prefix_length: None,
        }];

        Ok(vibesql_ast::CreateIndexStmt {
            if_not_exists,
            index_name,
            table_name,
            index_type: vibesql_ast::IndexType::Hnsw { metric, m, ef_construction },
            columns,
            where_clause: None,
        })
    }

    /// Parse the optional `WHERE <expr>` clause that follows the column list
    /// in a CREATE INDEX statement (SQLite partial-index syntax).
    ///
    /// Returns `Ok(None)` when no WHERE keyword is present so callers can use
    /// this helper unconditionally.
    fn parse_optional_index_where_clause(
        &mut self,
    ) -> Result<Option<Box<vibesql_ast::Expression>>, ParseError> {
        if self.peek_keyword(Keyword::Where) {
            self.advance(); // consume WHERE
            let expr = self.parse_expression()?;
            Ok(Some(Box::new(expr)))
        } else {
            Ok(None)
        }
    }

    /// Parse a positive integer value
    fn parse_positive_integer(&mut self) -> Result<i64, ParseError> {
        match self.peek() {
            Token::Number(n) => {
                let value = n
                    .parse::<i64>()
                    .map_err(|_| ParseError { message: "Invalid integer value".to_string() })?;
                self.advance();
                if value < 1 {
                    return Err(ParseError {
                        message: "Value must be a positive integer".to_string(),
                    });
                }
                Ok(value)
            }
            _ => Err(ParseError { message: "Expected positive integer".to_string() }),
        }
    }

    /// Parse index column list (helper for standard indexes)
    ///
    /// Supports both simple column references and expression indexes:
    /// - Column: `col1`, `col1 ASC`, `col1(10)` (with prefix length)
    /// - Expression: `(lower(name))`, `(a + b) DESC`, `0`, `0 LIKE col`
    ///
    /// SQLite allows expressions without parentheses, so we must detect when
    /// the token cannot be a column name (e.g., numeric literals) and parse
    /// as an expression instead.
    /// Reject `NULLS FIRST` / `NULLS LAST` modifiers in positions where SQLite
    /// accepts them syntactically but rejects them semantically (CREATE INDEX
    /// column specs, PRIMARY KEY / UNIQUE constraint column specs, ON CONFLICT
    /// upsert targets).
    ///
    /// Emits SQLite's canonical error string `unsupported use of NULLS FIRST`
    /// or `unsupported use of NULLS LAST` so the TCL test suite's
    /// error-message assertions match (nulls1.test 3.1.*).
    pub(in crate::parser) fn reject_nulls_in_index_position(
        &mut self,
    ) -> Result<(), ParseError> {
        if self.peek_keyword(Keyword::Nulls) {
            self.advance(); // consume NULLS
            let position = if self.peek_keyword(Keyword::First) {
                "FIRST"
            } else if self.peek_keyword(Keyword::Last) {
                "LAST"
            } else {
                return Err(ParseError {
                    message: "Expected FIRST or LAST after NULLS".to_string(),
                });
            };
            return Err(ParseError {
                message: format!("unsupported use of NULLS {}", position),
            });
        }
        Ok(())
    }

    fn parse_index_column_list(&mut self) -> Result<Vec<vibesql_ast::IndexColumn>, ParseError> {
        let mut columns = Vec::new();
        loop {
            // Check if this is an expression index (starts with parenthesis)
            if self.peek() == &Token::LParen {
                // This could be either:
                // 1. An expression index: (lower(name))
                // 2. A prefix length on the previous column: name(10)
                //
                // Since we're at the start of a new column spec, it must be an expression
                self.advance(); // consume LParen

                // Parse the expression
                let expr = self.parse_expression()?;

                self.expect_token(Token::RParen)?;

                // Check for optional ASC/DESC
                let direction = if self.peek_keyword(crate::keywords::Keyword::Asc) {
                    self.advance();
                    vibesql_ast::OrderDirection::Asc
                } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
                    self.advance();
                    vibesql_ast::OrderDirection::Desc
                } else {
                    vibesql_ast::OrderDirection::Asc
                };

                self.reject_nulls_in_index_position()?;

                columns.push(vibesql_ast::IndexColumn::new_expression(expr, direction));
            } else if matches!(
                self.peek(),
                Token::Number(_) | Token::Symbol('-') | Token::Symbol('+')
            ) {
                // Numeric literal or unary operator - cannot be a column name, must be an expression
                // SQLite allows: CREATE INDEX i ON t(0), CREATE INDEX i ON t(0 LIKE col)
                // SQLite also allows: CREATE INDEX i ON t(-b=b), CREATE INDEX i ON t(-a)
                let expr = self.parse_expression()?;

                // Check for optional ASC/DESC
                let direction = if self.peek_keyword(crate::keywords::Keyword::Asc) {
                    self.advance();
                    vibesql_ast::OrderDirection::Asc
                } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
                    self.advance();
                    vibesql_ast::OrderDirection::Desc
                } else {
                    vibesql_ast::OrderDirection::Asc
                };

                self.reject_nulls_in_index_position()?;

                columns.push(vibesql_ast::IndexColumn::new_expression(expr, direction));
            } else {
                // Save position before parsing identifier - we may need to backtrack
                // if this turns out to be an expression like abs(b) rather than a column name
                let saved_position = self.position;

                // Parse column name (use parse_alias_name to allow SQLite-style single-quoted
                // identifiers)
                // SQLite allows: CREATE INDEX i1xy ON t1(`x`,'y' ASC); -- 'y' is a column name
                let column_name = self.parse_alias_name()?;

                // Check if this is actually an expression (has arithmetic operator after identifier)
                // Examples: b+1, a*2, x-y are expressions, not column names
                if matches!(
                    self.peek(),
                    Token::Symbol('+')
                        | Token::Symbol('-')
                        | Token::Symbol('*')
                        | Token::Symbol('/')
                        | Token::Symbol('%')
                        | Token::Symbol('|')
                        | Token::Symbol('&')
                        | Token::Symbol('<')
                        | Token::Symbol('>')
                        | Token::Symbol('=')
                ) || matches!(self.peek(), Token::Keyword { keyword: kw, .. } if matches!(kw,
                    crate::keywords::Keyword::And
                    | crate::keywords::Keyword::Or
                    | crate::keywords::Keyword::Is
                    | crate::keywords::Keyword::Like
                    | crate::keywords::Keyword::Glob
                    | crate::keywords::Keyword::Between
                    | crate::keywords::Keyword::In
                )) {
                    // This is an expression - backtrack and parse fully
                    self.position = saved_position;
                    let expr = self.parse_expression()?;

                    let direction = if self.peek_keyword(crate::keywords::Keyword::Asc) {
                        self.advance();
                        vibesql_ast::OrderDirection::Asc
                    } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
                        self.advance();
                        vibesql_ast::OrderDirection::Desc
                    } else {
                        vibesql_ast::OrderDirection::Asc
                    };

                    self.reject_nulls_in_index_position()?;

                    columns.push(vibesql_ast::IndexColumn::new_expression(expr, direction));

                    if self.peek() == &Token::Comma {
                        self.advance();
                    } else {
                        break;
                    }
                    continue;
                }

                // Check for optional prefix length: column_name(length)
                // BUT: if the token after ( is not a number, this is likely a function call
                // like abs(b), not a prefix length like name(10). In that case, backtrack
                // and parse as an expression.
                let prefix_length = if self.peek() == &Token::LParen {
                    // Peek ahead to check if this is a prefix length (number) or function args
                    // A prefix length is: (number) - so we need Number followed by RParen
                    if matches!(self.peek_at_offset(1), Token::Number(_))
                        && matches!(self.peek_at_offset(2), Token::RParen)
                    {
                        self.advance(); // consume LParen

                        // Parse the integer length
                        let length = match self.peek() {
                            Token::Number(n) => {
                                let value = n.parse::<i64>().map_err(|_| ParseError {
                                    message: "Invalid integer for column prefix length".to_string(),
                                })?;
                                self.advance();

                                // Validate prefix length range (MySQL compatibility)
                                if value < 1 {
                                    return Err(ParseError {
                                        message: format!(
                                            "Key part '{}' length cannot be 0",
                                            column_name
                                        ),
                                    });
                                }
                                // MySQL InnoDB limit: 3072 bytes for index prefix length
                                if value > 3072 {
                                    return Err(ParseError {
                                        message:
                                            "Specified key was too long; max key length is 3072 bytes"
                                                .to_string(),
                                    });
                                }

                                value
                            }
                            _ => {
                                return Err(ParseError {
                                    message: "Expected integer for column prefix length"
                                        .to_string(),
                                })
                            }
                        };

                        self.expect_token(Token::RParen)?;
                        Some(length as u64)
                    } else {
                        // Not a prefix length - this is a function call like abs(b)
                        // Backtrack and parse as an expression
                        self.position = saved_position;
                        let expr = self.parse_expression()?;

                        // Check for optional ASC/DESC
                        let direction = if self.peek_keyword(crate::keywords::Keyword::Asc) {
                            self.advance();
                            vibesql_ast::OrderDirection::Asc
                        } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
                            self.advance();
                            vibesql_ast::OrderDirection::Desc
                        } else {
                            vibesql_ast::OrderDirection::Asc
                        };

                        self.reject_nulls_in_index_position()?;

                        columns.push(vibesql_ast::IndexColumn::new_expression(expr, direction));

                        if self.peek() == &Token::Comma {
                            self.advance(); // consume comma
                        } else {
                            break;
                        }
                        continue;
                    }
                } else {
                    None
                };

                // Check for optional COLLATE clause (SQLite compatibility)
                // Syntax: column_name COLLATE collation_name
                if self.peek_keyword(crate::keywords::Keyword::Collate) {
                    self.advance(); // consume COLLATE
                                    // Parse collation name (e.g., NOCASE, BINARY, RTRIM)
                    let _collation = self.parse_identifier()?;
                    // Note: We parse and ignore the collation for now
                    // Full collation support would require storing it in IndexColumn
                }

                // Check for optional ASC/DESC
                let direction = if self.peek_keyword(crate::keywords::Keyword::Asc) {
                    self.advance(); // consume ASC
                    vibesql_ast::OrderDirection::Asc
                } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
                    self.advance(); // consume DESC
                    vibesql_ast::OrderDirection::Desc
                } else {
                    vibesql_ast::OrderDirection::Asc // Default
                };

                // SQLite's grammar accepts NULLS FIRST/LAST in index column
                // specifications syntactically, then rejects them with a
                // specific error message. Match that behavior so error-message
                // assertions in the TCL test suite (nulls1.test) pass.
                self.reject_nulls_in_index_position()?;

                columns.push(vibesql_ast::IndexColumn::Column {
                    column_name,
                    direction,
                    prefix_length,
                });
            }

            if self.peek() == &Token::Comma {
                self.advance(); // consume comma
            } else {
                break;
            }
        }
        Ok(columns)
    }

    /// Helper function to parse the common parts of CREATE INDEX after type has been determined
    fn parse_create_index_columns(
        &mut self,
        index_type: vibesql_ast::IndexType,
    ) -> Result<vibesql_ast::CreateIndexStmt, ParseError> {
        // Check for optional IF NOT EXISTS clause
        let if_not_exists = if self.peek_keyword(Keyword::If) {
            self.advance(); // consume IF
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse index name
        let index_name = self.parse_identifier()?;

        // Expect ON keyword
        self.expect_keyword(Keyword::On)?;

        // Parse table name
        let table_name = self.parse_identifier()?;

        // Expect opening parenthesis
        self.expect_token(Token::LParen)?;

        // Parse column list (supports both column references and expression indexes)
        let columns = self.parse_index_column_list()?;

        // Expect closing parenthesis
        self.expect_token(Token::RParen)?;

        // Optional WHERE clause for partial indexes (e.g. UNIQUE partial indexes
        // in SQLite). FULLTEXT/SPATIAL indexes do not support WHERE in their
        // standard syntax, but accepting the clause here keeps the validation
        // path unified — downstream layers can still reject unsupported
        // combinations.
        let where_clause = self.parse_optional_index_where_clause()?;

        Ok(vibesql_ast::CreateIndexStmt {
            if_not_exists,
            index_name,
            table_name,
            index_type,
            columns,
            where_clause,
        })
    }

    /// Parse DROP INDEX statement
    ///
    /// Syntax:
    ///   DROP INDEX [IF EXISTS] index_name
    pub(super) fn parse_drop_index_statement(
        &mut self,
    ) -> Result<vibesql_ast::DropIndexStmt, ParseError> {
        // Expect DROP keyword
        self.expect_keyword(Keyword::Drop)?;

        // Expect INDEX keyword
        self.expect_keyword(Keyword::Index)?;

        // Check for optional IF EXISTS clause
        let if_exists = if self.peek_keyword(Keyword::If) {
            self.advance(); // consume IF
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse index name
        let index_name = self.parse_identifier()?;

        Ok(vibesql_ast::DropIndexStmt { if_exists, index_name })
    }

    /// Parse REINDEX statement
    ///
    /// Syntax:
    ///   REINDEX [database_name | table_name | index_name]
    pub(super) fn parse_reindex_statement(
        &mut self,
    ) -> Result<vibesql_ast::ReindexStmt, ParseError> {
        // Expect REINDEX keyword
        self.expect_keyword(Keyword::Reindex)?;

        // Check for optional target (database, table, or index name)
        let target = if self.peek() == &Token::Semicolon || self.peek() == &Token::Eof {
            // No target specified - reindex all
            None
        } else {
            // Parse optional identifier (could be database, table, or index name)
            Some(self.parse_identifier()?)
        };

        Ok(vibesql_ast::ReindexStmt { target })
    }

    /// Parse VACUUM statement (SQLite compatibility)
    ///
    /// Syntax:
    ///   VACUUM [schema_name] [INTO 'filename']
    pub(super) fn parse_vacuum_statement(&mut self) -> Result<vibesql_ast::VacuumStmt, ParseError> {
        // Expect VACUUM keyword
        self.expect_keyword(Keyword::Vacuum)?;

        // Check for optional schema name (e.g. VACUUM main)
        let schema_name = match self.peek() {
            Token::Semicolon | Token::Eof => None,
            Token::Keyword { keyword: Keyword::Into, .. } => None,
            _ => Some(self.parse_identifier()?),
        };

        // Check for optional INTO 'filename'
        let into_file = if matches!(self.peek(), Token::Keyword { keyword: Keyword::Into, .. }) {
            self.advance(); // consume INTO
            match self.peek().clone() {
                Token::String(s) => {
                    self.advance();
                    Some(s)
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected string literal after VACUUM INTO".to_string(),
                    })
                }
            }
        } else {
            None
        };

        Ok(vibesql_ast::VacuumStmt { schema_name, into_file })
    }

    pub(super) fn parse_analyze_statement(
        &mut self,
    ) -> Result<vibesql_ast::AnalyzeStmt, ParseError> {
        // Expect ANALYZE keyword
        self.expect_keyword(Keyword::Analyze)?;

        // Check for optional table name
        let table_name = if self.peek() == &Token::Semicolon || self.peek() == &Token::Eof {
            // No table specified - analyze all tables
            None
        } else {
            // Parse table name
            Some(self.parse_identifier()?)
        };

        // Check for optional column list (only if table name is present)
        let columns = if table_name.is_some() && self.peek() == &Token::LParen {
            self.advance(); // consume '('

            let mut cols = Vec::new();
            loop {
                cols.push(self.parse_identifier()?);

                if self.peek() == &Token::Comma {
                    self.advance(); // consume ','
                } else {
                    break;
                }
            }

            self.expect_token(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        Ok(vibesql_ast::AnalyzeStmt { table_name, columns })
    }

    /// Parse PRAGMA statement
    ///
    /// SQLite-specific statement for database configuration and introspection.
    /// Syntax variations:
    /// - PRAGMA pragma_name;                 -- Query pragma value
    /// - PRAGMA pragma_name = value;         -- Set pragma value
    /// - PRAGMA pragma_name(value);          -- Set pragma value (function syntax)
    /// - PRAGMA database.pragma_name;        -- Database-qualified pragma
    /// - PRAGMA database.pragma_name = value;
    pub(super) fn parse_pragma_statement(&mut self) -> Result<vibesql_ast::PragmaStmt, ParseError> {
        // Expect PRAGMA keyword
        self.expect_keyword(Keyword::Pragma)?;

        // Parse the pragma name (may be qualified with database.pragma_name).
        // Schema names can collide with reserved keywords (e.g. PRAGMA temp.foreign_key_check),
        // so accept a keyword in either position too.
        let first_ident = self.parse_identifier_or_keyword()?;

        // Check for dot (database.pragma_name syntax)
        let (database, name) = if self.peek() == &Token::Symbol('.') {
            self.advance(); // consume '.'
            let pragma_name = self.parse_identifier_or_keyword()?;
            (Some(first_ident), pragma_name)
        } else {
            (None, first_ident)
        };

        // Check for optional value assignment or function-style argument
        let value = if self.peek() == &Token::Symbol('=') {
            self.advance(); // consume '='
            Some(self.parse_pragma_value()?)
        } else if self.peek() == &Token::LParen {
            self.advance(); // consume '('
            let val = self.parse_pragma_value()?;
            self.expect_token(Token::RParen)?;
            Some(val)
        } else {
            None
        };

        Ok(vibesql_ast::PragmaStmt { database, name, value })
    }

    /// Parse a PRAGMA value (identifier, string, or number)
    fn parse_pragma_value(&mut self) -> Result<vibesql_ast::PragmaValue, ParseError> {
        match self.peek().clone() {
            Token::Identifier(ident) => {
                self.advance();
                Ok(vibesql_ast::PragmaValue::Identifier(ident))
            }
            Token::DelimitedIdentifier(ident) => {
                // Accept double-quoted/bracketed/backtick identifiers as PRAGMA
                // arguments, e.g. `PRAGMA table_info("weird name")` or
                // `PRAGMA table_info="""1"` (a table named `"1`).
                self.advance();
                Ok(vibesql_ast::PragmaValue::Identifier(ident))
            }
            Token::Keyword { keyword: kw, .. } => {
                // Allow keywords like ON, OFF, TRUE, FALSE as identifiers
                self.advance();
                Ok(vibesql_ast::PragmaValue::Identifier(kw.to_string()))
            }
            Token::String(s) => {
                self.advance();
                Ok(vibesql_ast::PragmaValue::String(s))
            }
            Token::Number(n) => {
                self.advance();
                Ok(vibesql_ast::PragmaValue::Number(n))
            }
            Token::Symbol('-') => {
                // Handle negative numbers
                self.advance(); // consume '-'
                match self.peek().clone() {
                    Token::Number(n) => {
                        self.advance();
                        Ok(vibesql_ast::PragmaValue::SignedNumber(format!("-{}", n)))
                    }
                    _ => Err(ParseError {
                        message: "Expected number after minus sign in PRAGMA value".to_string(),
                    }),
                }
            }
            Token::Symbol('+') => {
                // Handle explicit positive numbers
                self.advance(); // consume '+'
                match self.peek().clone() {
                    Token::Number(n) => {
                        self.advance();
                        Ok(vibesql_ast::PragmaValue::Number(n))
                    }
                    _ => Err(ParseError {
                        message: "Expected number after plus sign in PRAGMA value".to_string(),
                    }),
                }
            }
            _ => Err(ParseError { message: self.peek().syntax_error() }),
        }
    }
}
