//! Data type parsing

use super::super::*;

impl Parser {
    /// Parse data type
    pub(in crate::parser) fn parse_data_type(
        &mut self,
    ) -> Result<vibesql_types::DataType, ParseError> {
        let type_upper = match self.peek() {
            Token::Identifier(type_name) => type_name.to_uppercase(),
            Token::Keyword(Keyword::Date) => "DATE".to_string(),
            Token::Keyword(Keyword::Time) => "TIME".to_string(),
            Token::Keyword(Keyword::Timestamp) => "TIMESTAMP".to_string(),
            Token::Keyword(Keyword::Interval) => "INTERVAL".to_string(),
            Token::Keyword(Keyword::Character) => "CHARACTER".to_string(),
            Token::Keyword(Keyword::Boolean) => "BOOLEAN".to_string(),
            // MySQL-specific types that are keywords
            Token::Keyword(Keyword::Set) => "SET".to_string(),
            Token::Keyword(Keyword::Year) => "YEAR".to_string(),
            Token::Keyword(Keyword::Fixed) => "FIXED".to_string(),
            _ => return Err(ParseError { message: "Expected data type".to_string() }),
        };
        self.advance();

        match type_upper.as_str() {
            "INTEGER" | "INT" => Ok(vibesql_types::DataType::Integer),
            "SIGNED" => Ok(vibesql_types::DataType::Integer), /* MySQL-specific: SIGNED is equivalent to */
            // INTEGER (signed 32-bit integer)
            "UNSIGNED" => Ok(vibesql_types::DataType::Unsigned), /* MySQL-specific: UNSIGNED is 64-bit */
            // unsigned integer
            "SMALLINT" => Ok(vibesql_types::DataType::Smallint),
            "BIGINT" | "LONG" => Ok(vibesql_types::DataType::Bigint),
            "BOOLEAN" | "BOOL" => Ok(vibesql_types::DataType::Boolean),
            "BIT" => {
                // Parse BIT or BIT(n)
                // MySQL BIT type - stores bit values from 1 to 64 bits
                // Syntax: BIT[(length)]
                // Default length is 1 if not specified
                let length = if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume (
                    let len = match self.peek() {
                        Token::Number(n) => {
                            let parsed = n.parse::<usize>().map_err(|_| ParseError {
                                message: "Invalid BIT length".to_string(),
                            })?;
                            self.advance();
                            Some(parsed)
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected number after BIT(".to_string(),
                            })
                        }
                    };
                    self.expect_token(Token::RParen)?;
                    len
                } else {
                    None // No length specified, default to 1 (handled by storage layer)
                };
                Ok(vibesql_types::DataType::Bit { length })
            }
            "FLOAT" => {
                // Parse FLOAT(precision) or FLOAT
                // SQL:1999 allows FLOAT with optional precision parameter
                if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume (
                    let precision = match self.peek() {
                        Token::Number(n) => {
                            let p = n.parse::<u8>().map_err(|_| ParseError {
                                message: "Invalid FLOAT precision".to_string(),
                            })?;
                            self.advance();
                            p
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected precision after FLOAT(".to_string(),
                            })
                        }
                    };
                    self.expect_token(Token::RParen)?;
                    Ok(vibesql_types::DataType::Float { precision })
                } else {
                    // FLOAT without parameters defaults to 53-bit precision (IEEE 754 double)
                    Ok(vibesql_types::DataType::Float { precision: 53 })
                }
            }
            "REAL" => Ok(vibesql_types::DataType::Real),
            "DOUBLE" => {
                // Check for DOUBLE PRECISION
                if let Token::Identifier(next) = self.peek() {
                    if next.to_uppercase() == "PRECISION" {
                        self.advance();
                        return Ok(vibesql_types::DataType::DoublePrecision);
                    }
                }
                // Just DOUBLE without PRECISION - treat as DOUBLE PRECISION
                Ok(vibesql_types::DataType::DoublePrecision)
            }
            "NUMERIC" | "DECIMAL" | "DEC" => {
                // Parse NUMERIC(precision, scale) or NUMERIC(precision)
                // NUMERIC, DECIMAL, and DEC are all aliases per SQL:1999
                // All map to DataType::Numeric internally
                if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume (

                    let precision = match self.peek() {
                        Token::Number(n) => {
                            let p = n.parse::<u8>().map_err(|_| ParseError {
                                message: "Invalid NUMERIC precision".to_string(),
                            })?;
                            self.advance();
                            p
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected precision after NUMERIC(".to_string(),
                            })
                        }
                    };

                    let scale = if matches!(self.peek(), Token::Comma) {
                        self.advance(); // consume comma
                        match self.peek() {
                            Token::Number(n) => {
                                let s = n.parse::<u8>().map_err(|_| ParseError {
                                    message: "Invalid NUMERIC scale".to_string(),
                                })?;
                                self.advance();
                                s
                            }
                            _ => {
                                return Err(ParseError {
                                    message: "Expected scale after NUMERIC(precision,".to_string(),
                                })
                            }
                        }
                    } else {
                        0 // Default scale is 0
                    };

                    self.expect_token(Token::RParen)?;

                    // DEC, DECIMAL, and NUMERIC all map to DataType::Numeric
                    Ok(vibesql_types::DataType::Numeric { precision, scale })
                } else {
                    // NUMERIC/DECIMAL/DEC without parameters - use defaults (38, 0) per SQL
                    // standard
                    Ok(vibesql_types::DataType::Numeric { precision: 38, scale: 0 })
                }
            }
            "DATE" => Ok(vibesql_types::DataType::Date),
            "NAME" => Ok(vibesql_types::DataType::Name),
            "TIME" => {
                // Parse optional WITH TIME ZONE or WITHOUT TIME ZONE
                let with_timezone = self.parse_timezone_modifier()?;
                Ok(vibesql_types::DataType::Time { with_timezone })
            }
            "TIMESTAMP" => {
                // Parse optional WITH TIME ZONE or WITHOUT TIME ZONE
                let with_timezone = self.parse_timezone_modifier()?;
                Ok(vibesql_types::DataType::Timestamp { with_timezone })
            }
            "DATETIME" => {
                // MySQL/SQLite DATETIME type - treated as alias for TIMESTAMP
                //
                // DESIGN NOTE: DATETIME is semantically equivalent to TIMESTAMP and is
                // internally represented as DataType::Timestamp. This means:
                // - DATETIME and TIMESTAMP are functionally identical at runtime
                // - During persistence (save/load), DATETIME becomes TIMESTAMP
                // - This behavior is intentional for simplicity and consistency
                //
                // See issue #1626 for discussion of alternatives.
                let with_timezone = self.parse_timezone_modifier()?;
                Ok(vibesql_types::DataType::Timestamp { with_timezone })
            }
            "YEAR" => {
                // MySQL YEAR type - stores years from 1901-2155
                // Treated as a user-defined type for compatibility
                Ok(vibesql_types::DataType::UserDefined { type_name: "YEAR".to_string() })
            }
            "INTERVAL" => {
                // Parse INTERVAL start_field [TO end_field]
                let start_field = self.parse_interval_field()?;

                // Check for TO keyword (multi-field interval)
                let end_field = match self.peek() {
                    Token::Keyword(Keyword::To) => {
                        self.advance(); // consume TO keyword
                        Some(self.parse_interval_field()?)
                    }
                    Token::Identifier(word) if word.to_uppercase() == "TO" => {
                        self.advance(); // consume TO identifier (backward compat)
                        Some(self.parse_interval_field()?)
                    }
                    _ => None,
                };

                Ok(vibesql_types::DataType::Interval { start_field, end_field })
            }
            "VARCHAR" => {
                // Parse VARCHAR or VARCHAR(n) or VARCHAR(n CHARACTERS) or VARCHAR(n OCTETS)
                // Length is optional - if not specified, defaults to None (unlimited)
                let max_length = if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume LParen
                    let len = match self.peek() {
                        Token::Number(n) => {
                            let len = n.parse::<usize>().map_err(|_| ParseError {
                                message: "Invalid VARCHAR length".to_string(),
                            })?;
                            self.advance();
                            len
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected number after VARCHAR(".to_string(),
                            })
                        }
                    };

                    // Check for CHARACTERS or OCTETS modifier
                    // For MVP, we accept both but treat them the same (as character count)
                    if self.try_consume_keyword(Keyword::Characters)
                        || self.try_consume_keyword(Keyword::Octets)
                    {
                        // Modifier consumed, continue
                    }

                    self.expect_token(Token::RParen)?;
                    Some(len)
                } else {
                    None // No length specified, use default
                };
                Ok(vibesql_types::DataType::Varchar { max_length })
            }
            "CHAR" | "CHARACTER" => {
                // Check for VARYING keyword (CHARACTER VARYING = VARCHAR)
                // Also support deprecated VARING identifier (SQL:1999 conformance tests)
                let is_varying = self.try_consume_keyword(Keyword::Varying);
                let is_varing = if !is_varying {
                    // Check for VARING as identifier (deprecated SQL:1999 variant)
                    if let Token::Identifier(next) = self.peek() {
                        if next.to_uppercase() == "VARING" {
                            self.advance(); // consume VARING
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                };

                if is_varying || is_varing {
                    // Parse as VARCHAR (CHARACTER VARYING or CHAR VARING)
                    let max_length = if self.peek() == &Token::LParen {
                        self.advance();
                        let len = match self.peek() {
                            Token::Number(n) => {
                                let parsed = n.parse::<usize>().map_err(|_| ParseError {
                                    message: "Invalid VARCHAR length".to_string(),
                                })?;
                                self.advance();
                                Some(parsed)
                            }
                            _ => {
                                return Err(ParseError {
                                    message: "Expected number after CHARACTER VARYING(".to_string(),
                                })
                            }
                        };

                        // Check for CHARACTERS or OCTETS modifier
                        if self.try_consume_keyword(Keyword::Characters)
                            || self.try_consume_keyword(Keyword::Octets)
                        {
                            // Modifier consumed, continue
                        }

                        self.expect_token(Token::RParen)?;
                        len
                    } else {
                        None // No length specified, use default
                    };
                    return Ok(vibesql_types::DataType::Varchar { max_length });
                }

                // Otherwise parse as CHAR
                // Length is optional - if not specified, defaults to 1 per SQL:1999
                let length = if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume (
                    let len = match self.peek() {
                        Token::Number(n) => {
                            let parsed = n.parse::<usize>().map_err(|_| ParseError {
                                message: "Invalid CHAR length".to_string(),
                            })?;
                            self.advance();
                            parsed
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected number after CHAR(".to_string(),
                            })
                        }
                    };

                    // Check for CHARACTERS or OCTETS modifier
                    if self.try_consume_keyword(Keyword::Characters)
                        || self.try_consume_keyword(Keyword::Octets)
                    {
                        // Modifier consumed, continue
                    }

                    self.expect_token(Token::RParen)?;
                    len
                } else {
                    1 // Default length is 1 per SQL:1999 standard
                };

                Ok(vibesql_types::DataType::Character { length })
            }
            "NCHAR" => {
                // NCHAR is SQL standard national character type
                // NCHAR(n) maps to CHAR(n)
                // NCHAR VARYING(n) maps to VARCHAR(n)
                // Both are for Unicode/national character sets

                // Check for VARYING keyword (NCHAR VARYING = VARCHAR)
                let is_varying = self.try_consume_keyword(Keyword::Varying);

                if is_varying {
                    // Parse as VARCHAR (NCHAR VARYING)
                    let max_length = if self.peek() == &Token::LParen {
                        self.advance();
                        let len = match self.peek() {
                            Token::Number(n) => {
                                let parsed = n.parse::<usize>().map_err(|_| ParseError {
                                    message: "Invalid NCHAR VARYING length".to_string(),
                                })?;
                                self.advance();
                                Some(parsed)
                            }
                            _ => {
                                return Err(ParseError {
                                    message: "Expected number after NCHAR VARYING(".to_string(),
                                })
                            }
                        };

                        // Check for CHARACTERS or OCTETS modifier
                        if self.try_consume_keyword(Keyword::Characters)
                            || self.try_consume_keyword(Keyword::Octets)
                        {
                            // Modifier consumed, continue
                        }

                        self.expect_token(Token::RParen)?;
                        len
                    } else {
                        None // No length specified, use default
                    };
                    return Ok(vibesql_types::DataType::Varchar { max_length });
                }

                // Otherwise parse as NCHAR (fixed-length)
                // Length is optional - if not specified, defaults to 1 per SQL standard
                let length = if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume (
                    let len = match self.peek() {
                        Token::Number(n) => {
                            let parsed = n.parse::<usize>().map_err(|_| ParseError {
                                message: "Invalid NCHAR length".to_string(),
                            })?;
                            self.advance();
                            parsed
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected number after NCHAR(".to_string(),
                            })
                        }
                    };

                    // Check for CHARACTERS or OCTETS modifier
                    if self.try_consume_keyword(Keyword::Characters)
                        || self.try_consume_keyword(Keyword::Octets)
                    {
                        // Modifier consumed, continue
                    }

                    self.expect_token(Token::RParen)?;
                    len
                } else {
                    1 // Default length is 1 per SQL standard
                };

                Ok(vibesql_types::DataType::Character { length })
            }
            "NVARCHAR" => {
                // NVARCHAR is SQL Server/MySQL alias for NCHAR VARYING
                // Both map to VARCHAR internally (variable-length national character)
                // This is a convenience alias that behaves identically to NCHAR VARYING
                let max_length = if self.peek() == &Token::LParen {
                    self.advance();
                    let len = match self.peek() {
                        Token::Number(n) => {
                            let parsed = n.parse::<usize>().map_err(|_| ParseError {
                                message: "Invalid NVARCHAR length".to_string(),
                            })?;
                            self.advance();
                            Some(parsed)
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected number after NVARCHAR(".to_string(),
                            })
                        }
                    };

                    // Check for CHARACTERS or OCTETS modifier
                    if self.try_consume_keyword(Keyword::Characters)
                        || self.try_consume_keyword(Keyword::Octets)
                    {
                        // Modifier consumed, continue
                    }

                    self.expect_token(Token::RParen)?;
                    len
                } else {
                    None // No length specified, use default
                };
                Ok(vibesql_types::DataType::Varchar { max_length })
            }
            "TEXT" => {
                // TEXT is SQLite-style unlimited VARCHAR
                // Maps to VARCHAR without length constraint (unlimited)
                Ok(vibesql_types::DataType::Varchar { max_length: None })
            }
            "BINARY" | "VARBINARY" => {
                // MySQL BINARY and VARBINARY types with optional size
                // Syntax: BINARY[(n)] or VARBINARY[(n)]
                // Parse and discard the size - stored as UserDefined type
                if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume (

                    // Parse the size parameter
                    match self.peek() {
                        Token::Number(n) => {
                            // Validate it's a valid number (we don't store it currently)
                            let _ = n.parse::<usize>().map_err(|_| ParseError {
                                message: format!("Invalid {} size", type_upper),
                            })?;
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: format!("Expected size after {}(", type_upper),
                            })
                        }
                    }

                    self.expect_token(Token::RParen)?;
                }

                Ok(vibesql_types::DataType::UserDefined { type_name: type_upper })
            }
            "ENUM" | "SET" => {
                // MySQL ENUM and SET types take a list of values in parentheses
                // For now, we parse and ignore the values - just recognize the type
                // The syntax is: ENUM('value1','value2',...) or SET('value1','value2',...)
                if matches!(self.peek(), Token::LParen) {
                    self.expect_token(Token::LParen)?; // consume and validate (

                    // Skip values until we find the closing paren
                    // Values are typically string literals, separated by commas
                    let mut paren_depth = 1;
                    while paren_depth > 0 && !matches!(self.peek(), Token::Eof) {
                        match self.peek() {
                            Token::LParen => {
                                paren_depth += 1;
                                self.advance();
                            }
                            Token::RParen => {
                                paren_depth -= 1;
                                if paren_depth > 0 {
                                    self.advance();
                                } else {
                                    // Found the closing paren - consume it and break
                                    self.expect_token(Token::RParen)?;
                                    break;
                                }
                            }
                            _ => {
                                self.advance();
                            }
                        }
                    }
                }

                Ok(vibesql_types::DataType::UserDefined { type_name: type_upper })
            }
            "NATIONAL" => {
                // NATIONAL VARCHAR, NATIONAL CHARACTER, NATIONAL CHAR
                // These are SQL standard national character types for Unicode data
                // NATIONAL VARCHAR(n) -> maps to NVARCHAR(n) -> VARCHAR(n)
                // NATIONAL CHARACTER(n) -> maps to NCHAR(n) -> CHAR(n)
                // NATIONAL CHAR(n) -> maps to NCHAR(n) -> CHAR(n)

                // Look ahead to determine which national type follows
                let next = match self.peek() {
                    Token::Identifier(word) => word.to_uppercase(),
                    Token::Keyword(Keyword::Character) => "CHARACTER".to_string(),
                    _ => {
                        return Err(ParseError {
                            message: "Expected VARCHAR, CHARACTER, or CHAR after NATIONAL"
                                .to_string(),
                        })
                    }
                };

                match next.as_str() {
                    "VARCHAR" => {
                        self.advance(); // consume VARCHAR

                        // Parse as NVARCHAR - same logic as lines 371-405
                        let max_length = if self.peek() == &Token::LParen {
                            self.advance();
                            let len = match self.peek() {
                                Token::Number(n) => {
                                    let parsed = n.parse::<usize>().map_err(|_| ParseError {
                                        message: "Invalid NATIONAL VARCHAR length".to_string(),
                                    })?;
                                    self.advance();
                                    Some(parsed)
                                }
                                _ => {
                                    return Err(ParseError {
                                        message: "Expected number after NATIONAL VARCHAR("
                                            .to_string(),
                                    })
                                }
                            };

                            // Check for CHARACTERS or OCTETS modifier
                            if self.try_consume_keyword(Keyword::Characters)
                                || self.try_consume_keyword(Keyword::Octets)
                            {
                                // Modifier consumed, continue
                            }

                            self.expect_token(Token::RParen)?;
                            len
                        } else {
                            None // No length specified, use default
                        };
                        Ok(vibesql_types::DataType::Varchar { max_length })
                    }
                    "CHARACTER" | "CHAR" => {
                        self.advance(); // consume CHARACTER or CHAR

                        // Parse as NCHAR (fixed-length) - same logic as lines 337-370
                        let length = if matches!(self.peek(), Token::LParen) {
                            self.advance(); // consume (
                            let len = match self.peek() {
                                Token::Number(n) => {
                                    let parsed = n.parse::<usize>().map_err(|_| ParseError {
                                        message: "Invalid NATIONAL CHARACTER length".to_string(),
                                    })?;
                                    self.advance();
                                    parsed
                                }
                                _ => {
                                    return Err(ParseError {
                                        message: "Expected number after NATIONAL CHARACTER("
                                            .to_string(),
                                    })
                                }
                            };

                            // Check for CHARACTERS or OCTETS modifier
                            if self.try_consume_keyword(Keyword::Characters)
                                || self.try_consume_keyword(Keyword::Octets)
                            {
                                // Modifier consumed, continue
                            }

                            self.expect_token(Token::RParen)?;
                            len
                        } else {
                            1 // Default length is 1 per SQL standard
                        };

                        Ok(vibesql_types::DataType::Character { length })
                    }
                    _ => Err(ParseError {
                        message: format!(
                            "Expected VARCHAR, CHARACTER, or CHAR after NATIONAL, got: {}",
                            next
                        ),
                    }),
                }
            }
            "VECTOR" => {
                // Parse VECTOR(dimensions)
                // Syntax: VECTOR(n) where n is the dimension count (e.g., VECTOR(1536) for OpenAI embeddings)
                if !matches!(self.peek(), Token::LParen) {
                    return Err(ParseError {
                        message: "VECTOR type requires dimension specification: VECTOR(n)"
                            .to_string(),
                    });
                }
                self.advance(); // consume (
                let dimensions = match self.peek() {
                    Token::Number(n) => {
                        let d = n.parse::<u32>().map_err(|_| ParseError {
                            message: "Invalid VECTOR dimension (must be positive integer)"
                                .to_string(),
                        })?;
                        if d == 0 {
                            return Err(ParseError {
                                message: "VECTOR dimension must be greater than 0".to_string(),
                            });
                        }
                        self.advance();
                        d
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected dimension count after VECTOR(".to_string(),
                        })
                    }
                };
                self.expect_token(Token::RParen)?;
                Ok(vibesql_types::DataType::Vector { dimensions })
            }
            _ => {
                // Check if this is a known non-standard type that we should support
                if Self::is_supported_extension_type(&type_upper) {
                    Ok(vibesql_types::DataType::UserDefined { type_name: type_upper })
                } else {
                    Err(ParseError { message: format!("Unknown data type: {}", type_upper) })
                }
            }
        }
    }

    /// Check if a type name is a supported extension type (non-SQL:1999)
    /// These types are outside the SQL:1999 standard but should parse gracefully
    /// as user-defined types, including:
    /// - Spatial/geometric types from SQL/MM standard
    /// - MySQL-specific types
    /// - Other database extensions
    fn is_supported_extension_type(type_name: &str) -> bool {
        matches!(
            type_name,
            // 2D basic types (SQL/MM standard)
            "POINT" | "LINESTRING" | "POLYGON" |
            // Multi types (SQL/MM standard)
            "MULTIPOINT" | "MULTILINESTRING" | "MULTIPOLYGON" |
            // Collection types (SQL/MM standard)
            "GEOMETRY" | "GEOMETRYCOLLECTION" |
            // MySQL numeric types
            "TINYINT" | "MEDIUMINT" | "SERIAL" | "FIXED" |
            // MySQL temporal types
            "YEAR" |
            // MySQL string types
            "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" |
            "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" |
            "BINARY" | "VARBINARY" |
            // MySQL JSON type
            "JSON" |
            // MySQL enumeration types
            "ENUM" | "SET" |
            // Other common extension types
            "UUID"
        )
    }

    /// Parse interval field (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)
    pub(in crate::parser) fn parse_interval_field(
        &mut self,
    ) -> Result<vibesql_types::IntervalField, ParseError> {
        let field_upper = match self.peek() {
            Token::Identifier(field) => field.to_uppercase(),
            Token::Keyword(Keyword::Year) => "YEAR".to_string(),
            Token::Keyword(Keyword::Month) => "MONTH".to_string(),
            Token::Keyword(Keyword::Day) => "DAY".to_string(),
            Token::Keyword(Keyword::Hour) => "HOUR".to_string(),
            Token::Keyword(Keyword::Minute) => "MINUTE".to_string(),
            Token::Keyword(Keyword::Second) => "SECOND".to_string(),
            _ => {
                return Err(ParseError {
                    message: "Expected interval field (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)"
                        .to_string(),
                })
            }
        };
        self.advance();

        match field_upper.as_str() {
            "YEAR" => Ok(vibesql_types::IntervalField::Year),
            "MONTH" => Ok(vibesql_types::IntervalField::Month),
            "DAY" => Ok(vibesql_types::IntervalField::Day),
            "HOUR" => Ok(vibesql_types::IntervalField::Hour),
            "MINUTE" => Ok(vibesql_types::IntervalField::Minute),
            "SECOND" => Ok(vibesql_types::IntervalField::Second),
            _ => Err(ParseError { message: format!("Unknown interval field: {}", field_upper) }),
        }
    }

    /// Parse optional timezone modifier (WITH TIME ZONE or WITHOUT TIME ZONE)
    /// Returns true if WITH TIME ZONE, false if WITHOUT TIME ZONE or no modifier
    pub(in crate::parser) fn parse_timezone_modifier(&mut self) -> Result<bool, ParseError> {
        // Check for WITH keyword
        if matches!(self.peek(), Token::Keyword(Keyword::With)) {
            self.advance(); // consume WITH

            // Expect TIME keyword
            self.expect_keyword(Keyword::Time)?;

            // Expect ZONE keyword
            self.expect_keyword(Keyword::Zone)?;
            return Ok(true); // WITH TIME ZONE
        }

        // Check for WITHOUT keyword
        if matches!(self.peek(), Token::Keyword(Keyword::Without)) {
            self.advance(); // consume WITHOUT

            // Expect TIME keyword
            self.expect_keyword(Keyword::Time)?;

            // Expect ZONE keyword
            self.expect_keyword(Keyword::Zone)?;
            return Ok(false); // WITHOUT TIME ZONE
        }

        // No timezone modifier - default to WITHOUT TIME ZONE
        Ok(false)
    }
}
