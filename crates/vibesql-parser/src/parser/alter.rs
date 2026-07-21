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
                // SQLite allows DROP COLUMN without the COLUMN keyword:
                // `ALTER TABLE t DROP <col>` is a synonym for
                // `ALTER TABLE t DROP COLUMN <col>`. If we see a bare column
                // name, treat it as a column drop (mirrors the ADD case above).
                Token::Identifier(_) => parse_drop_column(parser, table_name),
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
    // The data type is optional in SQLite: `ALTER TABLE t ADD COLUMN x` (no
    // type) creates a column with BLOB/no-affinity, exactly like a typeless
    // column in CREATE TABLE. When the token following the column name is a
    // column constraint keyword, a DEFAULT clause, or end-of-statement, there
    // is no type to parse — default to BLOB affinity.
    let (data_type, is_exact_integer_type, type_source) = if is_add_column_type_absent(parser) {
        (vibesql_types::DataType::BinaryLargeObject, false, None)
    } else {
        let (dt, is_int) = parser.parse_data_type_with_integer_flag()?;
        // Capture the verbatim declared type text for STRICT-table validation
        // (issue #5837): ALTER TABLE <strict> ADD COLUMN must reject non-strict
        // datatypes just like CREATE TABLE.
        let src = parser.source_between(type_start, parser.current_position());
        (dt, is_int, src)
    };

    // Parse optional generated-column clause, mirroring the CREATE TABLE parser
    // (create/table.rs). Supports both `GENERATED ALWAYS AS (expr)` and the
    // short form `AS (expr)`, each optionally followed by STORED or VIRTUAL.
    // Without this the GENERATED/AS tokens were left unconsumed and
    // `generated_expr` stayed `None`, so the executor stored a plain column and
    // the added column silently returned NULL instead of the computed value
    // (issue #5861). SQLite defaults a generated column to VIRTUAL; the STORED
    // flag is threaded to the executor so a STORED add on a populated table can
    // be rejected the way sqlite3 3.51.0 does. The typeless short forms (`ADD
    // COLUMN y AS (x+1)` / `... GENERATED ...`) are already routed here because
    // `is_add_column_type_absent` treats a leading GENERATED/AS as "no data
    // type".
    let mut generated = parse_optional_generated_clause(parser)?;

    // Parse optional DEFAULT clause
    let mut default_value = if parser.peek_keyword(Keyword::Default) {
        parser.advance(); // consume DEFAULT
        Some(Box::new(parser.parse_expression()?))
    } else {
        None
    };

    // A generated clause may also follow DEFAULT (`DEFAULT 7 GENERATED ALWAYS AS
    // (...)`). SQLite rejects the DEFAULT + generated combination regardless of
    // order, so parse the trailing generated clause here to feed the
    // mutual-exclusion guard below instead of leaving the tokens unconsumed.
    if generated.is_none() {
        generated = parse_optional_generated_clause(parser)?;
    }

    // SQLite rejects a generated column that also declares DEFAULT with
    // `cannot use DEFAULT on a generated column` (verified against sqlite3
    // 3.51.0). Match that instead of silently accepting the invalid clause.
    if generated.is_some() && default_value.is_some() {
        return Err(ParseError { message: "cannot use DEFAULT on a generated column".to_string() });
    }

    let (generated_expr, generated_stored) = match generated {
        Some((expr, stored)) => (Some(expr), stored),
        None => (None, false),
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
            // A column-level CHECK constraint on the added column. Without this
            // arm the loop broke at CHECK and the `CHECK(<expr>)` tokens were
            // silently discarded, so the constraint was neither stored nor
            // validated against existing rows (alter3-9.*). Mirror the CREATE
            // TABLE column-constraint parser (create/constraints.rs), preserving
            // the verbatim expression text for SQLite-compatible messages.
            Token::Keyword { keyword: Keyword::Check, .. } => {
                parser.advance(); // consume CHECK
                let lparen_pos = parser.current_position();
                parser.expect_token(Token::LParen)?;
                let expr = parser.parse_expression()?;
                let rparen_pos = parser.current_position();
                parser.expect_token(Token::RParen)?;
                let source_text = parser.source_inside_delimiters(lparen_pos, rparen_pos);
                constraints.push(ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::Check { expr: Box::new(expr), source_text },
                });
            }
            // A DEFAULT clause may appear among the other column constraints in
            // any order (e.g. `ADD c NOT NULL DEFAULT 10`). SQLite accepts column
            // constraints in any order; parsing DEFAULT only before the loop
            // dropped a trailing `DEFAULT` and left the column with no default
            // (alter3-2.4). The first DEFAULT seen wins.
            Token::Keyword { keyword: Keyword::Default, .. } => {
                parser.advance();
                let expr = parser.parse_expression()?;
                if default_value.is_none() {
                    default_value = Some(Box::new(expr));
                }
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
        generated_expr,
        is_exact_integer_type,
        type_source,
    };

    Ok(AlterTableStmt::AddColumn(AddColumnStmt { table_name, column_def, generated_stored }))
}

/// Parse an optional generated-column clause: `GENERATED ALWAYS AS (expr)` or the
/// short form `AS (expr)`, each optionally followed by `STORED` or `VIRTUAL`.
/// Returns the generated expression and whether it was declared `STORED` (SQLite
/// defaults a generated column to VIRTUAL when neither keyword is present).
fn parse_optional_generated_clause(
    parser: &mut crate::Parser,
) -> Result<Option<(Box<Expression>, bool)>, ParseError> {
    if parser.peek_keyword(Keyword::Generated) {
        parser.advance(); // consume GENERATED
        parser.expect_keyword(Keyword::Always)?;
        parser.expect_keyword(Keyword::As)?;
    } else if parser.peek_keyword(Keyword::As) {
        parser.advance(); // consume AS (short form)
    } else {
        return Ok(None);
    }

    parser.expect_token(Token::LParen)?;
    let expr = parser.parse_expression()?;
    parser.expect_token(Token::RParen)?;

    let mut stored = false;
    if parser.peek_keyword(Keyword::Stored) {
        parser.advance();
        stored = true;
    } else if parser.peek_keyword(Keyword::Virtual) {
        parser.advance();
    }

    Ok(Some((Box::new(expr), stored)))
}

/// Determine whether the token following an ADD COLUMN column name signals
/// that no data type was declared. SQLite permits typeless columns (BLOB
/// affinity), so `ALTER TABLE t ADD COLUMN x`, `... ADD COLUMN x NOT NULL`,
/// `... ADD COLUMN x DEFAULT 0`, etc. are all valid with no explicit type.
fn is_add_column_type_absent(parser: &crate::Parser) -> bool {
    matches!(
        parser.peek(),
        // End of the statement — a bare `ADD COLUMN x`.
        Token::Eof
            | Token::Semicolon
            | Token::Comma
            | Token::RParen
            // Column constraint / modifier keywords that follow the (absent) type.
            | Token::Keyword { keyword: Keyword::Not, .. }
            | Token::Keyword { keyword: Keyword::Null, .. }
            | Token::Keyword { keyword: Keyword::Primary, .. }
            | Token::Keyword { keyword: Keyword::Unique, .. }
            | Token::Keyword { keyword: Keyword::Check, .. }
            | Token::Keyword { keyword: Keyword::References, .. }
            | Token::Keyword { keyword: Keyword::Default, .. }
            | Token::Keyword { keyword: Keyword::Collate, .. }
            | Token::Keyword { keyword: Keyword::Constraint, .. }
            | Token::Keyword { keyword: Keyword::Key, .. }
            | Token::Keyword { keyword: Keyword::AutoIncrement, .. }
            | Token::Keyword { keyword: Keyword::Generated, .. }
            | Token::Keyword { keyword: Keyword::As, .. }
    )
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
    let old_column_name = parser.parse_column_name()?;
    parser.expect_keyword(Keyword::To)?;
    let new_column_name = parser.parse_column_name()?;

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
