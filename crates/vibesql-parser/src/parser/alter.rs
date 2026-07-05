//! ALTER TABLE parser

use vibesql_ast::*;

use crate::{keywords::Keyword, parser::ParseError, token::Token};

/// Parse ALTER TABLE statement
pub fn parse_alter_table(parser: &mut crate::Parser) -> Result<AlterTableStmt, ParseError> {
    // ALTER TABLE
    parser.expect_keyword(Keyword::Alter)?;
    parser.expect_keyword(Keyword::Table)?;

    let table_name = parser.parse_identifier()?;

    // Dispatch based on operation
    match parser.peek() {
        Token::Keyword { keyword: Keyword::Add, .. } => {
            parser.advance();
            match parser.peek() {
                Token::Keyword { keyword: Keyword::Column, .. } => {
                    parser.advance();
                    parse_add_column(parser, table_name)
                }
                // SQL:1999 allows adding constraints with or without CONSTRAINT keyword
                Token::Keyword {
                    keyword:
                        Keyword::Constraint
                        | Keyword::Check
                        | Keyword::Unique
                        | Keyword::Primary
                        | Keyword::Foreign,
                    ..
                } => parse_add_constraint(parser, table_name),
                // SQL:1999 allows ADD COLUMN without the COLUMN keyword
                // If we see an identifier, treat it as a bare column addition
                Token::Identifier(_) => parse_add_column(parser, table_name),
                _ => Err(ParseError {
                    message: "Expected COLUMN, constraint keyword, or column name after ADD"
                        .to_string(),
                }),
            }
        }
        Token::Keyword { keyword: Keyword::Drop, .. } => {
            parser.advance();
            match parser.peek() {
                Token::Keyword { keyword: Keyword::Column, .. } => {
                    parser.advance();
                    parse_drop_column(parser, table_name)
                }
                Token::Keyword { keyword: Keyword::Constraint, .. } => {
                    parser.advance();
                    parse_drop_constraint(parser, table_name)
                }
                _ => Err(ParseError {
                    message: "Expected COLUMN or CONSTRAINT after DROP".to_string(),
                }),
            }
        }
        Token::Keyword { keyword: Keyword::Alter, .. } => {
            parser.advance();
            parser.expect_keyword(Keyword::Column)?;
            parse_alter_column(parser, table_name)
        }
        Token::Keyword { keyword: Keyword::Rename, .. } => {
            parser.advance();
            parse_rename(parser, table_name)
        }
        Token::Keyword { keyword: Keyword::Modify, .. } => {
            parser.advance();
            parse_modify_column(parser, table_name)
        }
        Token::Keyword { keyword: Keyword::Change, .. } => {
            parser.advance();
            parse_change_column(parser, table_name)
        }
        _ => Err(ParseError {
            message: "Expected ADD, DROP, ALTER, RENAME, MODIFY, or CHANGE after table name"
                .to_string(),
        }),
    }
}

/// Parse ADD COLUMN
fn parse_add_column(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    let column_name = parser.parse_identifier()?;
    let type_start = parser.current_position();
    let (data_type, is_exact_integer_type) = parser.parse_data_type_with_integer_flag()?;
    // Capture the verbatim declared type text for STRICT-table validation
    // (issue #5837): ALTER TABLE <strict> ADD COLUMN must reject non-strict
    // datatypes just like CREATE TABLE.
    let type_source = parser.source_between(type_start, parser.current_position());

    // Parse optional DEFAULT clause
    let default_value = if parser.peek_keyword(Keyword::Default) {
        parser.advance(); // consume DEFAULT
        Some(Box::new(parser.parse_expression()?))
    } else {
        None
    };

    // Parse column constraints
    let mut nullable = true;
    let mut constraints = Vec::new();

    loop {
        match parser.peek() {
            Token::Keyword { keyword: Keyword::Not, .. } => {
                parser.advance();
                parser.expect_keyword(Keyword::Null)?;
                nullable = false;
            }
            Token::Keyword { keyword: Keyword::Primary, .. } => {
                parser.advance();
                parser.expect_keyword(Keyword::Key)?;
                constraints.push(ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::PrimaryKey { on_conflict: None },
                });
            }
            Token::Keyword { keyword: Keyword::Unique, .. } => {
                parser.advance();
                constraints.push(ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::Unique { on_conflict: None },
                });
            }
            Token::Keyword { keyword: Keyword::References, .. } => {
                parser.advance();
                let ref_table = parser.parse_identifier()?;
                // Column specification is optional in SQLite - defaults to PK
                let ref_column = if parser.peek() == &crate::token::Token::LParen {
                    parser.advance(); // consume LParen
                    let col = parser.parse_identifier()?;
                    parser.expect_token(crate::token::Token::RParen)?;
                    Some(col)
                } else {
                    None
                };
                constraints.push(ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::References {
                        table: ref_table,
                        column: ref_column,
                        on_delete: None,
                        on_update: None,
                        deferral: None,
                    },
                });
            }
            _ => break,
        }
    }

    let column_def = ColumnDef {
        name: column_name,
        data_type,
        nullable,
        constraints,
        default_value,
        comment: None,
        generated_expr: None,
        is_exact_integer_type,
        type_source,
    };

    Ok(AlterTableStmt::AddColumn(AddColumnStmt { table_name, column_def }))
}

/// Parse DROP COLUMN
fn parse_drop_column(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    let if_exists =
        parser.try_consume_keyword(Keyword::If) && parser.try_consume_keyword(Keyword::Exists);

    // Accept a reserved keyword as the column name (e.g. `DROP COLUMN sql`).
    // SQLite permits keyword column names here; matching that lets validations
    // such as the `sqlite_schema` guard run instead of failing at parse time.
    let column_name = parser.parse_identifier_or_keyword()?;

    Ok(AlterTableStmt::DropColumn(DropColumnStmt { table_name, column_name, if_exists }))
}

/// Parse ALTER COLUMN
fn parse_alter_column(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    let column_name = parser.parse_identifier()?;

    match parser.peek() {
        Token::Keyword { keyword: Keyword::Set, .. } => {
            parser.advance();
            match parser.peek() {
                Token::Keyword { keyword: Keyword::Default, .. } => {
                    parser.advance();
                    // Parse the default expression
                    let default = parser.parse_expression()?;
                    Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::SetDefault {
                        table_name,
                        column_name,
                        default,
                    }))
                }
                Token::Keyword { keyword: Keyword::Not, .. } => {
                    parser.advance();
                    parser.expect_keyword(Keyword::Null)?;
                    Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::SetNotNull {
                        table_name,
                        column_name,
                    }))
                }
                _ => Err(ParseError {
                    message: "Expected DEFAULT or NOT NULL after SET".to_string(),
                }),
            }
        }
        Token::Keyword { keyword: Keyword::Drop, .. } => {
            parser.advance();
            match parser.peek() {
                Token::Keyword { keyword: Keyword::Default, .. } => {
                    parser.advance();
                    Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::DropDefault {
                        table_name,
                        column_name,
                    }))
                }
                Token::Keyword { keyword: Keyword::Not, .. } => {
                    parser.advance();
                    parser.expect_keyword(Keyword::Null)?;
                    Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::DropNotNull {
                        table_name,
                        column_name,
                    }))
                }
                _ => Err(ParseError {
                    message: "Expected DEFAULT or NOT NULL after DROP".to_string(),
                }),
            }
        }
        _ => Err(ParseError { message: "Expected SET or DROP after column name".to_string() }),
    }
}

/// Parse ADD CONSTRAINT
fn parse_add_constraint(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    // Use the existing parse_table_constraint method which handles all constraint types
    let constraint = parser.parse_table_constraint()?;

    Ok(AlterTableStmt::AddConstraint(AddConstraintStmt { table_name, constraint }))
}

/// Parse DROP CONSTRAINT
fn parse_drop_constraint(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    let constraint_name = parser.parse_identifier()?;

    Ok(AlterTableStmt::DropConstraint(DropConstraintStmt { table_name, constraint_name }))
}

/// Parse the part of an `ALTER TABLE <t> RENAME ...` statement that follows the
/// `RENAME` keyword. Distinguishes three forms:
/// - `RENAME TO <new_table>`            -> RenameTable
/// - `RENAME COLUMN <old> TO <new>`     -> RenameColumn
/// - `RENAME <old> TO <new>`            -> RenameColumn (bare column form)
fn parse_rename(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    match parser.peek() {
        // RENAME TO <new_table>
        Token::Keyword { keyword: Keyword::To, .. } => {
            parser.advance();
            let new_table_name = parser.parse_identifier()?;
            Ok(AlterTableStmt::RenameTable(RenameTableStmt { table_name, new_table_name }))
        }
        // RENAME COLUMN <old> TO <new>
        Token::Keyword { keyword: Keyword::Column, .. } => {
            parser.advance();
            parse_rename_column(parser, table_name)
        }
        // RENAME <old> TO <new> (bare column form)
        _ => parse_rename_column(parser, table_name),
    }
}

/// Parse the `<old> TO <new>` tail of a RENAME COLUMN statement.
fn parse_rename_column(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    let old_column_name = parser.parse_identifier()?;
    parser.expect_keyword(Keyword::To)?;
    let new_column_name = parser.parse_identifier()?;

    Ok(AlterTableStmt::RenameColumn(RenameColumnStmt {
        table_name,
        old_column_name,
        new_column_name,
    }))
}

/// Parse MODIFY COLUMN
fn parse_modify_column(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    // MODIFY [COLUMN] column_name new_definition
    if parser.peek_keyword(Keyword::Column) {
        parser.advance(); // consume optional COLUMN keyword
    }

    let column_name = parser.parse_identifier()?;
    let (data_type, is_exact_integer_type) = parser.parse_data_type_with_integer_flag()?;

    // Parse optional DEFAULT clause
    let default_value = if parser.peek_keyword(Keyword::Default) {
        parser.advance();
        Some(Box::new(parser.parse_expression()?))
    } else {
        None
    };

    // Parse column constraints
    let mut nullable = true;
    let mut constraints = Vec::new();

    loop {
        match parser.peek() {
            Token::Keyword { keyword: Keyword::Not, .. } => {
                parser.advance();
                parser.expect_keyword(Keyword::Null)?;
                nullable = false;
            }
            Token::Keyword { keyword: Keyword::Primary, .. } => {
                parser.advance();
                parser.expect_keyword(Keyword::Key)?;
                constraints.push(ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::PrimaryKey { on_conflict: None },
                });
            }
            Token::Keyword { keyword: Keyword::Unique, .. } => {
                parser.advance();
                constraints.push(ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::Unique { on_conflict: None },
                });
            }
            Token::Keyword { keyword: Keyword::References, .. } => {
                parser.advance();
                let ref_table = parser.parse_identifier()?;
                // Column specification is optional in SQLite - defaults to PK
                let ref_column = if parser.peek() == &crate::token::Token::LParen {
                    parser.advance(); // consume LParen
                    let col = parser.parse_identifier()?;
                    parser.expect_token(crate::token::Token::RParen)?;
                    Some(col)
                } else {
                    None
                };
                constraints.push(ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::References {
                        table: ref_table,
                        column: ref_column,
                        on_delete: None,
                        on_update: None,
                        deferral: None,
                    },
                });
            }
            _ => break,
        }
    }

    let new_column_def = ColumnDef {
        name: column_name.clone(),
        data_type,
        nullable,
        constraints,
        default_value,
        comment: None,
        generated_expr: None,
        is_exact_integer_type,
        type_source: None,
    };

    Ok(AlterTableStmt::ModifyColumn(ModifyColumnStmt { table_name, column_name, new_column_def }))
}

/// Parse CHANGE COLUMN
fn parse_change_column(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    // CHANGE [COLUMN] old_column_name new_column_name new_definition
    if parser.peek_keyword(Keyword::Column) {
        parser.advance(); // consume optional COLUMN keyword
    }

    let old_column_name = parser.parse_identifier()?;
    let new_column_name = parser.parse_identifier()?;
    let (data_type, is_exact_integer_type) = parser.parse_data_type_with_integer_flag()?;

    // Parse optional DEFAULT clause
    let default_value = if parser.peek_keyword(Keyword::Default) {
        parser.advance();
        Some(Box::new(parser.parse_expression()?))
    } else {
        None
    };

    // Parse column constraints
    let mut nullable = true;
    let mut constraints = Vec::new();

    loop {
        match parser.peek() {
            Token::Keyword { keyword: Keyword::Not, .. } => {
                parser.advance();
                parser.expect_keyword(Keyword::Null)?;
                nullable = false;
            }
            Token::Keyword { keyword: Keyword::Primary, .. } => {
                parser.advance();
                parser.expect_keyword(Keyword::Key)?;
                constraints.push(ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::PrimaryKey { on_conflict: None },
                });
            }
            Token::Keyword { keyword: Keyword::Unique, .. } => {
                parser.advance();
                constraints.push(ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::Unique { on_conflict: None },
                });
            }
            Token::Keyword { keyword: Keyword::References, .. } => {
                parser.advance();
                let ref_table = parser.parse_identifier()?;
                // Column specification is optional in SQLite - defaults to PK
                let ref_column = if parser.peek() == &crate::token::Token::LParen {
                    parser.advance(); // consume LParen
                    let col = parser.parse_identifier()?;
                    parser.expect_token(crate::token::Token::RParen)?;
                    Some(col)
                } else {
                    None
                };
                constraints.push(ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::References {
                        table: ref_table,
                        column: ref_column,
                        on_delete: None,
                        on_update: None,
                        deferral: None,
                    },
                });
            }
            _ => break,
        }
    }

    let new_column_def = ColumnDef {
        name: new_column_name,
        data_type,
        nullable,
        constraints,
        default_value,
        comment: None,
        generated_expr: None,
        is_exact_integer_type,
        type_source: None,
    };

    Ok(AlterTableStmt::ChangeColumn(ChangeColumnStmt {
        table_name,
        old_column_name,
        new_column_def,
    }))
}
