//! Data type parsing

use super::super::*;

impl Parser {
    /// Parse data type
    pub(in crate::parser) fn parse_data_type(
        &mut self,
    ) -> Result<vibesql_types::DataType, ParseError> {
        let (data_type, _is_exact_integer) = self.parse_data_type_with_integer_flag()?;
        Ok(data_type)
    }

    /// Parse data type with INTEGER flag for SQLite rowid alias detection.
    /// Returns (DataType, is_exact_integer_type) where is_exact_integer_type is true
    /// only when the original type declaration was exactly "INTEGER" (case-insensitive).
    /// This is needed because in SQLite, only `INTEGER PRIMARY KEY` is a rowid alias,
    /// not `INT PRIMARY KEY`.
    pub(in crate::parser) fn parse_data_type_with_integer_flag(
        &mut self,
    ) -> Result<(vibesql_types::DataType, bool), ParseError> {
        // A delimited (quoted/bracketed) type name is taken verbatim and treated
        // as an opaque SQLite-style type whose storage is governed by affinity
        // only. This covers `"col.1" [char.3]` and `f "VARCHAR (+1,-10, 5)"`
        // (table-8.9, table-8.10), where the quoted text is the full type name.
        if let Token::DelimitedIdentifier(name) = self.peek() {
            let type_name = name.clone();
            self.advance();
            // A delimited identifier may still be followed by a size specifier,
            // e.g. `"DECIMAL"(10,2)`. Consume and discard it (affinity only).
            self.consume_optional_type_arg_list()?;
            return Ok((vibesql_types::DataType::UserDefined { type_name }, false));
        }

        // Get the type name from the token. Note that identifiers are already
        // normalized to lowercase by the lexer, so we use uppercase for matching
        // but store the lowercase form.
        let type_name = match self.peek() {
            Token::Identifier(name) => name.clone(),
            Token::Keyword { keyword: Keyword::Date, .. } => "date".to_string(),
            Token::Keyword { keyword: Keyword::Time, .. } => "time".to_string(),
            Token::Keyword { keyword: Keyword::Timestamp, .. } => "timestamp".to_string(),
            Token::Keyword { keyword: Keyword::Interval, .. } => "interval".to_string(),
            Token::Keyword { keyword: Keyword::Character, .. } => "character".to_string(),
            Token::Keyword { keyword: Keyword::Boolean, .. } => "boolean".to_string(),
            // MySQL-specific types that are keywords
            Token::Keyword { keyword: Keyword::Set, .. } => "set".to_string(),
            Token::Keyword { keyword: Keyword::Year, .. } => "year".to_string(),
            Token::Keyword { keyword: Keyword::Fixed, .. } => "fixed".to_string(),
            // SQLite type aliases - VARYING can start multi-word types like VARYING CHARACTER
            Token::Keyword { keyword: Keyword::Varying, .. } => "varying".to_string(),
            // SQLite ANY type - represents any type, stored with no affinity
            Token::Keyword { keyword: Keyword::Any, .. } => "any".to_string(),
            // SQLite fallback keywords are legal type names (keyword1.test:
            // `CREATE TABLE abort(abort abort)`). Truly-reserved words
            // (PRIMARY, NOT, CROSS, ...) are not in the fallback set and still
            // fail with "Expected data type", matching SQLite. The name is
            // lowercased like any unquoted identifier and resolves through the
            // affinity-based UserDefined catch-all below.
            Token::Keyword { keyword, .. } if keyword.is_sqlite_fallback_keyword() => {
                keyword.to_string().to_lowercase()
            }
            // SQLite accepts arbitrary type names, including the TYPE keyword
            // itself (misc1-7.1: `CREATE TABLE error1(a TYPE PRIMARY KEY, ...)`).
            // TYPE is a VibeSQL-only keyword (CREATE TYPE, GRANT) not in the
            // SQLite fallback set above, so it needs its own arm; it falls
            // through to the UserDefined catch-all below and is stored by
            // affinity (#5804).
            Token::Keyword { keyword: Keyword::Type, .. } => "type".to_string(),
            // The window-function contextual keywords WINDOW, OVER, and FILTER are
            // fallback identifiers in SQLite and are legal type names (window6.test
            // iteration 5: `CREATE TABLE over(following, preceding window)` uses
            // `window` as a column type). They are not in the general SQLite
            // fallback set above, so each needs its own arm; all three fall through
            // to the UserDefined affinity catch-all below.
            Token::Keyword { keyword: Keyword::Window, .. } => "window".to_string(),
            Token::Keyword { keyword: Keyword::Over, .. } => "over".to_string(),
            Token::Keyword { keyword: Keyword::Filter, .. } => "filter".to_string(),
            _ => return Err(ParseError { message: "Expected data type".to_string() }),
        };
        self.advance();

        // Use uppercase for matching to support case-insensitive type names
        let type_upper = type_name.to_uppercase();
        match type_upper.as_str() {
            "INTEGER" => Ok((vibesql_types::DataType::Integer, true)),
            "INT" => Ok((vibesql_types::DataType::Integer, false)),
            "SIGNED" => Ok((vibesql_types::DataType::Integer, false)), // MySQL SIGNED = INTEGER
            "UNSIGNED" => Ok((vibesql_types::DataType::Unsigned, false)), // MySQL UNSIGNED = 64-bit
            "SMALLINT" => Ok((vibesql_types::DataType::Smallint, false)),
            "BIGINT" | "LONG" => Ok((vibesql_types::DataType::Bigint, false)),
            "BOOLEAN" | "BOOL" => Ok((vibesql_types::DataType::Boolean, false)),
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
                Ok((vibesql_types::DataType::Bit { length }, false))
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
                    Ok((vibesql_types::DataType::Float { precision }, false))
                } else {
                    // FLOAT without parameters defaults to 53-bit precision (IEEE 754 double)
                    Ok((vibesql_types::DataType::Float { precision: 53 }, false))
                }
            }
            "REAL" => Ok((vibesql_types::DataType::Real, false)),
            "DOUBLE" => {
                // Check for DOUBLE PRECISION
                if let Token::Identifier(next) = self.peek() {
                    if next.to_uppercase() == "PRECISION" {
                        self.advance();
                        return Ok((vibesql_types::DataType::DoublePrecision, false));
                    }
                }
                // Just DOUBLE without PRECISION - treat as DOUBLE PRECISION
                Ok((vibesql_types::DataType::DoublePrecision, false))
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
                    Ok((vibesql_types::DataType::Numeric { precision, scale }, false))
                } else {
                    // NUMERIC/DECIMAL/DEC without parameters - use defaults (38, 0) per SQL
                    // standard
                    Ok((vibesql_types::DataType::Numeric { precision: 38, scale: 0 }, false))
                }
            }
            "DATE" => Ok((vibesql_types::DataType::Date, false)),
            "NAME" => Ok((vibesql_types::DataType::Name, false)),
            "TIME" => {
                // Parse optional WITH TIME ZONE or WITHOUT TIME ZONE
                let with_timezone = self.parse_timezone_modifier()?;
                Ok((vibesql_types::DataType::Time { with_timezone }, false))
            }
            "TIMESTAMP" => {
                // Parse optional WITH TIME ZONE or WITHOUT TIME ZONE
                let with_timezone = self.parse_timezone_modifier()?;
                Ok((vibesql_types::DataType::Timestamp { with_timezone }, false))
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
                Ok((vibesql_types::DataType::Timestamp { with_timezone }, false))
            }
            "YEAR" => {
                // MySQL YEAR type - stores years from 1901-2155
                // Treated as a user-defined type for compatibility
                Ok((vibesql_types::DataType::UserDefined { type_name: "year".to_string() }, false))
            }
            "INTERVAL" => {
                // Parse INTERVAL start_field [TO end_field]
                let start_field = self.parse_interval_field()?;

                // Check for TO keyword (multi-field interval)
                let end_field = match self.peek() {
                    Token::Keyword { keyword: Keyword::To, .. } => {
                        self.advance(); // consume TO keyword
                        Some(self.parse_interval_field()?)
                    }
                    Token::Identifier(word) if word.to_uppercase() == "TO" => {
                        self.advance(); // consume TO identifier (backward compat)
                        Some(self.parse_interval_field()?)
                    }
                    _ => None,
                };

                Ok((vibesql_types::DataType::Interval { start_field, end_field }, false))
            }
            "VARCHAR" => {
                // Parse VARCHAR or VARCHAR(n) or VARCHAR(n CHARACTERS) or VARCHAR(n OCTETS)
                // Length is optional - if not specified, defaults to None (unlimited).
                //
                // SQLite accepts arbitrary, possibly-signed, multi-argument size
                // specifiers on any type name (e.g. VARCHAR(1,10), VARCHAR(+1,-10);
                // table-8.10). The arguments are ignored for affinity, so we keep
                // the first non-negative argument as the length (when present) and
                // tolerantly skip the rest.
                let max_length = if matches!(self.peek(), Token::LParen) {
                    let first = self.consume_optional_type_arg_list()?;
                    first.and_then(|n| usize::try_from(n).ok())
                } else {
                    None // No length specified, use default
                };
                Ok((vibesql_types::DataType::Varchar { max_length }, false))
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
                    return Ok((vibesql_types::DataType::Varchar { max_length }, false));
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

                Ok((vibesql_types::DataType::Character { length }, false))
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
                    return Ok((vibesql_types::DataType::Varchar { max_length }, false));
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

                Ok((vibesql_types::DataType::Character { length }, false))
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
                Ok((vibesql_types::DataType::Varchar { max_length }, false))
            }
            "TEXT" => {
                // TEXT is SQLite-style unlimited VARCHAR
                // SQLite allows TEXT(n) for compatibility but ignores the size
                // Maps to VARCHAR without length constraint (unlimited)
                if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume (
                                    // Parse and ignore the size parameter (SQLite compatibility)
                    match self.peek() {
                        Token::Number(n) => {
                            let _ = n.parse::<usize>().map_err(|_| ParseError {
                                message: "Invalid TEXT size".to_string(),
                            })?;
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected size after TEXT(".to_string(),
                            })
                        }
                    }
                    self.expect_token(Token::RParen)?;
                }
                Ok((vibesql_types::DataType::Varchar { max_length: None }, false))
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

                Ok((vibesql_types::DataType::UserDefined { type_name }, false))
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

                Ok((vibesql_types::DataType::UserDefined { type_name }, false))
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
                    Token::Keyword { keyword: Keyword::Character, .. } => "CHARACTER".to_string(),
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
                        Ok((vibesql_types::DataType::Varchar { max_length }, false))
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

                        Ok((vibesql_types::DataType::Character { length }, false))
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
                // Syntax: VECTOR(n) where n is the dimension count (e.g., VECTOR(1536) for OpenAI
                // embeddings)
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
                Ok((vibesql_types::DataType::Vector { dimensions }, false))
            }
            // BLOB and CLOB types - SQL:1999 large object types
            // These need explicit handling to map to BinaryLargeObject/CharacterLargeObject
            // instead of falling through to UserDefined
            "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
                Ok((vibesql_types::DataType::BinaryLargeObject, false))
            }
            "CLOB" => Ok((vibesql_types::DataType::CharacterLargeObject, false)),
            // SQLite ANY type: intentionally NOT special-cased to BLOB/no-affinity here.
            // A plain (non-STRICT) column declared ANY does not match the INT/CHAR/
            // CLOB/TEXT/BLOB/REAL/FLOA/DOUB substring rules, so per SQLite's column
            // affinity algorithm (https://www.sqlite.org/datatype3.html#affinity) it
            // falls through to rule 5 and gets NUMERIC affinity — e.g. inserting the
            // REAL literal 6.0 into an ANY column stores it as INTEGER 6, matching
            // `typeof()`/`quote()` on real SQLite (verified against sqlite3 3.51.0;
            // window1.test 29.2, #6191). STRICT-table `ANY` columns (no affinity at
            // all) are validated separately via `StrictType::Any` in
            // vibesql-executor/src/strict.rs and are unaffected by this fallthrough.
            _ => {
                // SQLite compatibility: Accept ANY string as a type name.
                // SQLite uses type affinity rules to determine storage, but accepts
                // any type name including typos like "IMTEGES" or "INTEGES".
                //
                // Multi-word types are common in SQLite:
                // - LARGE BLOB, NATIVE CHARACTER(n), VARYING CHARACTER(n)
                // - UNSIGNED BIG INT, LONG VARCHAR, etc.
                //
                // Continue consuming identifiers and type-related keywords to build
                // the complete type name, then apply SQLite's affinity rules.
                let mut full_type_name = type_name.clone();

                loop {
                    match self.peek() {
                        // Continue with identifier tokens
                        Token::Identifier(next) => {
                            full_type_name.push(' ');
                            full_type_name.push_str(next);
                            self.advance();
                        }
                        // Also allow CHARACTER keyword in multi-word types
                        Token::Keyword { keyword: Keyword::Character, .. } => {
                            full_type_name.push_str(" character");
                            self.advance();
                        }
                        // Stop at length specifier, constraint, or delimiter
                        _ => break,
                    }
                }

                // Handle optional length/precision specifier for multi-word and
                // unrecognized type names, e.g. NATIVE CHARACTER(70), VARYING
                // CHARACTER(255), or Oracle/SQLite-style NUMBER(5,10). SQLite accepts
                // any type name with an optional, possibly-signed, multi-argument size
                // specifier and stores it by affinity only, so we discard the numbers.
                self.consume_optional_type_arg_list()?;

                Ok((vibesql_types::DataType::UserDefined { type_name: full_type_name }, false))
            }
        }
    }

    /// Consume an optional SQLite-style type size specifier and return the
    /// first numeric argument (signed), if any.
    ///
    /// SQLite permits any type name to carry a parenthesized list of one or
    /// more optionally-signed numeric arguments, e.g. `VARCHAR(1,10)`,
    /// `VARCHAR(+1,-10)`, `NUMBER(5,10)` (table-8.10). The arguments do not
    /// affect storage (affinity is derived from the type *name*), so callers
    /// generally discard the result; `Varchar` keeps the first argument as a
    /// best-effort declared length when it is non-negative.
    ///
    /// CHARACTERS / OCTETS unit modifiers (after a single argument) are also
    /// accepted and discarded. If the next token is not `(`, this is a no-op
    /// and returns `None`.
    pub(in crate::parser) fn consume_optional_type_arg_list(
        &mut self,
    ) -> Result<Option<i64>, ParseError> {
        if !matches!(self.peek(), Token::LParen) {
            return Ok(None);
        }
        self.advance(); // consume (

        let mut first: Option<i64> = None;

        loop {
            // Optional leading sign on the numeric argument.
            let mut negate = false;
            loop {
                match self.peek() {
                    Token::Symbol('+') => {
                        self.advance();
                    }
                    Token::Symbol('-') => {
                        negate = !negate;
                        self.advance();
                    }
                    _ => break,
                }
            }

            match self.peek() {
                Token::Number(n) => {
                    let value = n.parse::<i64>().map_err(|_| ParseError {
                        message: format!("Invalid numeric type argument: {}", n),
                    })?;
                    let value = if negate { -value } else { value };
                    if first.is_none() {
                        first = Some(value);
                    }
                    self.advance();
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected numeric type argument".to_string(),
                    })
                }
            }

            // CHARACTERS / OCTETS unit modifier (single-argument character types).
            if self.try_consume_keyword(Keyword::Characters)
                || self.try_consume_keyword(Keyword::Octets)
            {
                // Modifier consumed, continue.
            }

            if matches!(self.peek(), Token::Comma) {
                self.advance(); // consume , and parse the next argument
                continue;
            }
            break;
        }

        self.expect_token(Token::RParen)?;
        Ok(first)
    }

    /// Parse interval field (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)
    pub(in crate::parser) fn parse_interval_field(
        &mut self,
    ) -> Result<vibesql_types::IntervalField, ParseError> {
        let field_upper = match self.peek() {
            Token::Identifier(field) => field.to_uppercase(),
            Token::Keyword { keyword: Keyword::Year, .. } => "YEAR".to_string(),
            Token::Keyword { keyword: Keyword::Month, .. } => "MONTH".to_string(),
            Token::Keyword { keyword: Keyword::Day, .. } => "DAY".to_string(),
            Token::Keyword { keyword: Keyword::Hour, .. } => "HOUR".to_string(),
            Token::Keyword { keyword: Keyword::Minute, .. } => "MINUTE".to_string(),
            Token::Keyword { keyword: Keyword::Second, .. } => "SECOND".to_string(),
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
        if matches!(self.peek(), Token::Keyword { keyword: Keyword::With, .. }) {
            self.advance(); // consume WITH

            // Expect TIME keyword
            self.expect_keyword(Keyword::Time)?;

            // Expect ZONE keyword
            self.expect_keyword(Keyword::Zone)?;
            return Ok(true); // WITH TIME ZONE
        }

        // Check for WITHOUT keyword
        if matches!(self.peek(), Token::Keyword { keyword: Keyword::Without, .. }) {
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
