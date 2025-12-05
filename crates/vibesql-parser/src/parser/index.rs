//! Parser for CREATE INDEX, DROP INDEX, and REINDEX statements

use super::{ParseError, Parser};
use crate::{keywords::Keyword, token::Token};

impl Parser {
    /// Parse CREATE INDEX statement
    ///
    /// Syntax:
    ///   CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON table_name (column_list)
    ///   CREATE FULLTEXT INDEX [IF NOT EXISTS] index_name ON table_name (column_list)
    ///   CREATE SPATIAL INDEX [IF NOT EXISTS] index_name ON table_name (column_list)
    ///   CREATE INDEX [IF NOT EXISTS] index_name ON table_name USING ivfflat (column [ops]) [WITH (lists = N)]
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

        Ok(vibesql_ast::CreateIndexStmt {
            if_not_exists,
            index_name,
            table_name,
            index_type: vibesql_ast::IndexType::BTree { unique: false },
            columns,
        })
    }

    /// Parse IVFFlat index specifics
    ///
    /// Syntax: USING ivfflat (column [vector_l2_ops|vector_cosine_ops|vector_ip_ops]) [WITH (lists = N)]
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

        let columns = vec![vibesql_ast::IndexColumn {
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
        })
    }

    /// Parse HNSW index specifics
    ///
    /// Syntax: USING hnsw (column [vector_l2_ops|vector_cosine_ops|vector_ip_ops]) [WITH (m = N, ef_construction = N)]
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

        let columns = vec![vibesql_ast::IndexColumn {
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
        })
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
    fn parse_index_column_list(&mut self) -> Result<Vec<vibesql_ast::IndexColumn>, ParseError> {
        let mut columns = Vec::new();
        loop {
            // Parse column name
            let column_name = self.parse_identifier()?;

            // Check for optional prefix length: column_name(length)
            let prefix_length = if self.peek() == &Token::LParen {
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
                                message: format!("Key part '{}' length cannot be 0", column_name),
                            });
                        }
                        // MySQL InnoDB limit: 3072 bytes for index prefix length
                        if value > 3072 {
                            return Err(ParseError {
                                message: "Specified key was too long; max key length is 3072 bytes"
                                    .to_string(),
                            });
                        }

                        value
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected integer for column prefix length".to_string(),
                        })
                    }
                };

                self.expect_token(Token::RParen)?;
                Some(length as u64)
            } else {
                None
            };

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

            columns.push(vibesql_ast::IndexColumn { column_name, direction, prefix_length });

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

        // Parse column list
        let mut columns = Vec::new();
        loop {
            // Parse column name
            let column_name = self.parse_identifier()?;

            // Check for optional prefix length: column_name(length)
            let prefix_length = if self.peek() == &Token::LParen {
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
                                message: format!("Key part '{}' length cannot be 0", column_name),
                            });
                        }
                        // MySQL InnoDB limit: 3072 bytes for index prefix length
                        // This is the maximum for utf8mb4 with innodb_large_prefix enabled
                        if value > 3072 {
                            return Err(ParseError {
                                message: "Specified key was too long; max key length is 3072 bytes"
                                    .to_string(),
                            });
                        }

                        value
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected integer for column prefix length".to_string(),
                        })
                    }
                };

                self.expect_token(Token::RParen)?;
                Some(length as u64)
            } else {
                None
            };

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

            columns.push(vibesql_ast::IndexColumn { column_name, direction, prefix_length });

            if self.peek() == &Token::Comma {
                self.advance(); // consume comma
            } else {
                break;
            }
        }

        // Expect closing parenthesis
        self.expect_token(Token::RParen)?;

        Ok(vibesql_ast::CreateIndexStmt {
            if_not_exists,
            index_name,
            table_name,
            index_type,
            columns,
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
}
