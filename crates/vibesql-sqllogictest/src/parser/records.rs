//! Record type definitions for sqllogictest.

use std::fmt;

use crate::ColumnType;
use super::location::Location;
use super::retry_parser::RetryConfig;
// SortMode and ResultMode are used in Display impl via ControlItem trait
#[allow(unused_imports)]
use super::directive_parser::{Control, Condition, Connection, SortMode, ResultMode, ControlItem};
use super::record_parser::{StatementExpect, QueryExpect};

/// A single directive in a sqllogictest file.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Record<T: ColumnType> {
    /// An include copies all records from another files.
    Include {
        loc: Location,
        /// A glob pattern
        filename: String,
    },
    /// A statement is an SQL command that is to be evaluated but from which we do not expect to
    /// get results (other than success or failure).
    Statement {
        loc: Location,
        conditions: Vec<Condition>,
        connection: Connection,
        /// The SQL command.
        sql: String,
        expected: StatementExpect,
        /// Optional retry configuration
        retry: Option<RetryConfig>,
    },
    /// A query is an SQL command from which we expect to receive results. The result set might be
    /// empty.
    Query {
        loc: Location,
        conditions: Vec<Condition>,
        connection: Connection,
        /// The SQL command.
        sql: String,
        expected: QueryExpect<T>,
        /// Optional retry configuration
        retry: Option<RetryConfig>,
    },
    /// A system command is an external command that is to be executed by the shell. Currently it
    /// must succeed and the output is ignored.
    #[non_exhaustive]
    System {
        loc: Location,
        conditions: Vec<Condition>,
        /// The external command.
        command: String,
        stdout: Option<String>,
        /// Optional retry configuration
        retry: Option<RetryConfig>,
    },
    /// A sleep period.
    Sleep {
        loc: Location,
        duration: std::time::Duration,
    },
    /// Subtest.
    Subtest {
        loc: Location,
        name: String,
    },
    /// A halt record merely causes sqllogictest to ignore the rest of the test script.
    /// For debugging use only.
    Halt {
        loc: Location,
    },
    /// Control statements.
    Control(Control),
    /// Set the maximum number of result values that will be accepted
    /// for a query.  If the number of result values exceeds this number,
    /// then an MD5 hash is computed of all values, and the resulting hash
    /// is the only result.
    ///
    /// If the threshold is 0, then hashing is never used.
    HashThreshold {
        loc: Location,
        threshold: u64,
    },
    /// Condition statements, including `onlyif` and `skipif`.
    Condition(Condition),
    /// Connection statements to specify the connection to use for the following statement.
    Connection(Connection),
    /// Dialect switch directive - changes SQL dialect mode for subsequent tests.
    /// Executes SET SQL_MODE = '<mode>' on the database.
    Dialect {
        loc: Location,
        /// The dialect mode: "mysql", "sqlite", "postgresql", etc.
        mode: String,
    },
    Comment(Vec<String>),
    Newline,
    /// Internally injected record which should not occur in the test file.
    Injected(Injected),
}

impl<T: ColumnType> Record<T> {
    /// Unparses the record to its string representation in the test file.
    ///
    /// # Panics
    /// If the record is an internally injected record which should not occur in the test file.
    pub fn unparse(&self, w: &mut impl std::io::Write) -> std::io::Result<()> {
        write!(w, "{self}")
    }
}

/// As is the standard for Display, does not print any trailing
/// newline except for records that always end with a blank line such
/// as Query and Statement.
impl<T: ColumnType> std::fmt::Display for Record<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Record::Include { loc: _, filename } => {
                write!(f, "include {filename}")
            }
            Record::Statement {
                loc: _,
                conditions: _,
                connection: _,
                sql,
                expected,
                retry,
            } => {
                super::record_parser::fmt_statement(f, sql, expected, retry)
            }
            Record::Query {
                loc: _,
                conditions: _,
                connection: _,
                sql,
                expected,
                retry,
            } => {
                super::record_parser::fmt_query(f, sql, expected, retry)
            }
            Record::System {
                loc: _,
                conditions: _,
                command,
                stdout,
                retry,
            } => {
                super::record_parser::fmt_system(f, command, stdout, retry)
            }
            Record::Sleep { loc: _, duration } => {
                write!(f, "sleep {}", humantime::format_duration(*duration))
            }
            Record::Subtest { loc: _, name } => {
                write!(f, "subtest {name}")
            }
            Record::Halt { loc: _ } => {
                write!(f, "halt")
            }
            Record::Control(c) => match c {
                Control::SortMode(m) => write!(f, "control sortmode {}", m.as_str()),
                Control::ResultMode(m) => write!(f, "control resultmode {}", m.as_str()),
                Control::Substitution(s) => write!(f, "control substitution {}", s.as_str()),
            },
            Record::Condition(cond) => match cond {
                Condition::OnlyIf { label } => write!(f, "onlyif {label}"),
                Condition::SkipIf { label } => write!(f, "skipif {label}"),
            },
            Record::Connection(conn) => {
                if let Connection::Named(conn) = conn {
                    write!(f, "connection {}", conn)?;
                }
                Ok(())
            }
            Record::Dialect { loc: _, mode } => {
                write!(f, "dialect {mode}")
            }
            Record::HashThreshold { loc: _, threshold } => {
                write!(f, "hash-threshold {threshold}")
            }
            Record::Comment(comment) => {
                let mut iter = comment.iter();
                write!(f, "#{}", iter.next().unwrap().trim_end())?;
                for line in iter {
                    write!(f, "\n#{}", line.trim_end())?;
                }
                Ok(())
            }
            Record::Newline => Ok(()), // Display doesn't end with newline
            Record::Injected(p) => panic!("unexpected injected record: {p:?}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[non_exhaustive]
pub enum Injected {
    /// Pseudo control command to indicate the begin of an include statement. Automatically
    /// injected by sqllogictest parser.
    BeginInclude(String),
    /// Pseudo control command to indicate the end of an include statement. Automatically injected
    /// by sqllogictest parser.
    EndInclude(String),
}
