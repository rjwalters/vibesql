use super::{ParseError, Parser};
use crate::{keywords::Keyword, token::Token};

impl Parser {
    /// Parse MySQL table options for CREATE TABLE
    pub fn parse_table_options(&mut self) -> Result<Vec<vibesql_ast::TableOption>, ParseError> {
        let mut options = Vec::new();

        loop {
            // Check for table option keywords
            let option = if self.try_consume_keyword(Keyword::KeyBlockSize) {
                self.parse_key_block_size_option()?
            } else if self.try_consume_keyword(Keyword::Connection) {
                self.parse_connection_option()?
            } else if self.try_consume_keyword(Keyword::InsertMethod) {
                self.parse_insert_method_option()?
            } else if self.try_consume_keyword(Keyword::Union) {
                self.parse_union_option()?
            } else if self.try_consume_keyword(Keyword::RowFormat) {
                self.parse_row_format_option()?
            } else if self.try_consume_keyword(Keyword::DelayKeyWrite) {
                self.parse_delay_key_write_option()?
            } else if self.try_consume_keyword(Keyword::TableChecksum)
                || self.try_consume_keyword(Keyword::Checksum)
            {
                self.parse_table_checksum_option()?
            } else if self.try_consume_keyword(Keyword::StatsSamplePages) {
                self.parse_stats_sample_pages_option()?
            } else if self.try_consume_keyword(Keyword::Password) {
                self.parse_password_option()?
            } else if self.try_consume_keyword(Keyword::AvgRowLength) {
                self.parse_avg_row_length_option()?
            } else if self.try_consume_keyword(Keyword::MinRows) {
                self.parse_min_rows_option()?
            } else if self.try_consume_keyword(Keyword::MaxRows) {
                self.parse_max_rows_option()?
            } else if self.try_consume_keyword(Keyword::SecondaryEngine) {
                self.parse_secondary_engine_option()?
            } else if self.try_consume_keyword(Keyword::Collate) {
                self.parse_collate_option()?
            } else if self.try_consume_keyword(Keyword::Comment) {
                self.parse_comment_option()?
            } else if self.try_consume_keyword(Keyword::Storage) {
                self.parse_storage_option()?
            } else {
                // No more table options
                break;
            };

            options.push(option);

            // Table options can be separated by commas (MySQL style) or just spaces
            self.try_consume(&Token::Comma);
        }

        Ok(options)
    }

    fn parse_key_block_size_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse value
        let value = self.parse_numeric_value()?;
        Ok(vibesql_ast::TableOption::KeyBlockSize(value))
    }

    fn parse_connection_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse string value
        match self.peek() {
            Token::String(s) => {
                let val = s.clone();
                self.advance();
                Ok(vibesql_ast::TableOption::Connection(Some(val)))
            }
            _ => Err(ParseError { message: "Expected string value for CONNECTION".to_string() }),
        }
    }

    fn parse_insert_method_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        let method = if self.try_consume_keyword(Keyword::First) {
            vibesql_ast::InsertMethod::First
        } else if self.try_consume_keyword(Keyword::Last) {
            vibesql_ast::InsertMethod::Last
        } else if self.try_consume_keyword(Keyword::No) {
            vibesql_ast::InsertMethod::No
        } else {
            return Err(ParseError {
                message: "Expected FIRST, LAST, or NO for INSERT_METHOD".to_string(),
            });
        };
        Ok(vibesql_ast::TableOption::InsertMethod(method))
    }

    fn parse_union_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Optional parentheses
        self.try_consume(&Token::LParen);
        let mut tables = Vec::new();
        if !self.try_consume(&Token::RParen) {
            loop {
                match self.peek() {
                    Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                        tables.push(id.clone());
                        self.advance();
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected table name in UNION".to_string(),
                        })
                    }
                }
                if self.try_consume(&Token::Comma) {
                    continue;
                } else {
                    break;
                }
            }
            self.expect_token(Token::RParen)?;
        }
        Ok(vibesql_ast::TableOption::Union(Some(tables)))
    }

    fn parse_row_format_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        let format = if self.try_consume_keyword(Keyword::Default) {
            vibesql_ast::RowFormat::Default
        } else if self.try_consume_keyword(Keyword::Dynamic) {
            vibesql_ast::RowFormat::Dynamic
        } else if self.try_consume_keyword(Keyword::Fixed) {
            vibesql_ast::RowFormat::Fixed
        } else if self.try_consume_keyword(Keyword::Compressed) {
            vibesql_ast::RowFormat::Compressed
        } else if self.try_consume_keyword(Keyword::Redundant) {
            vibesql_ast::RowFormat::Redundant
        } else if self.try_consume_keyword(Keyword::Compact) {
            vibesql_ast::RowFormat::Compact
        } else {
            return Err(ParseError { message: "Expected row format value".to_string() });
        };
        Ok(vibesql_ast::TableOption::RowFormat(Some(format)))
    }

    fn parse_delay_key_write_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse numeric value
        let value = self.parse_numeric_value()?;
        Ok(vibesql_ast::TableOption::DelayKeyWrite(value))
    }

    fn parse_table_checksum_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse numeric value
        let value = self.parse_numeric_value()?;
        Ok(vibesql_ast::TableOption::TableChecksum(value))
    }

    fn parse_stats_sample_pages_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse numeric value
        let value = self.parse_numeric_value()?;
        Ok(vibesql_ast::TableOption::StatsSamplePages(value))
    }

    fn parse_password_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse string value
        match self.peek() {
            Token::String(s) => {
                let val = s.clone();
                self.advance();
                Ok(vibesql_ast::TableOption::Password(Some(val)))
            }
            _ => Err(ParseError { message: "Expected string value for PASSWORD".to_string() }),
        }
    }

    fn parse_avg_row_length_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse numeric value
        let value = self.parse_numeric_value()?;
        Ok(vibesql_ast::TableOption::AvgRowLength(value))
    }

    fn parse_min_rows_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse numeric value
        let value = self.parse_numeric_value()?;
        Ok(vibesql_ast::TableOption::MinRows(value))
    }

    fn parse_max_rows_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse numeric value
        let value = self.parse_numeric_value()?;
        Ok(vibesql_ast::TableOption::MaxRows(value))
    }

    fn parse_secondary_engine_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse identifier or NULL
        let value = if self.try_consume_keyword(Keyword::Null) {
            Some("NULL".to_string())
        } else if let Token::Identifier(id) = self.peek() {
            let val = id.clone();
            self.advance();
            Some(val)
        } else {
            None
        };
        Ok(vibesql_ast::TableOption::SecondaryEngine(value))
    }

    fn parse_collate_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse collation name
        match self.peek() {
            Token::Identifier(id) => {
                let val = id.clone();
                self.advance();
                Ok(vibesql_ast::TableOption::Collate(Some(val)))
            }
            _ => Err(ParseError { message: "Expected collation name".to_string() }),
        }
    }

    fn parse_comment_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse string value
        match self.peek() {
            Token::String(s) => {
                let val = s.clone();
                self.advance();
                Ok(vibesql_ast::TableOption::Comment(Some(val)))
            }
            _ => Err(ParseError { message: "Expected string value for COMMENT".to_string() }),
        }
    }

    /// Parse STORAGE = {ROW | COLUMNAR} option (VibeSQL extension)
    fn parse_storage_option(&mut self) -> Result<vibesql_ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse storage format value
        let format = if self.try_consume_keyword(Keyword::Row) {
            vibesql_ast::StorageFormat::Row
        } else if self.try_consume_keyword(Keyword::Columnar) {
            vibesql_ast::StorageFormat::Columnar
        } else {
            return Err(ParseError { message: "Expected ROW or COLUMNAR for STORAGE".to_string() });
        };
        Ok(vibesql_ast::TableOption::Storage(format))
    }

    /// Parse a numeric value that can be integer or float (converts float to int by truncation)
    pub(super) fn parse_numeric_value(&mut self) -> Result<Option<i64>, ParseError> {
        if let Token::Number(n) = self.peek() {
            // Try parsing as i64 first, then as f64 and truncate
            let val = if let Ok(int_val) = n.parse::<i64>() {
                int_val
            } else if let Ok(float_val) = n.parse::<f64>() {
                float_val as i64
            } else {
                return Err(ParseError { message: "Invalid numeric value".to_string() });
            };
            self.advance();
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }
}
