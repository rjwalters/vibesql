//! Parsing and handling of directives and control statements.

use std::fmt;

use itertools::Itertools;

use crate::ParseErrorKind;

/// A single directive in a sqllogictest file that controls behavior.
#[derive(Debug, PartialEq, Eq, Clone)]
#[non_exhaustive]
pub enum Control {
    /// Control sort mode.
    SortMode(SortMode),
    /// control result mode.
    ResultMode(ResultMode),
    /// Control whether or not to substitute variables in the SQL.
    Substitution(bool),
}

/// Whether to apply sorting before checking the results of a query.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SortMode {
    /// The default option. The results appear in exactly the order in which they were received
    /// from the database engine.
    NoSort,
    /// Gathers all output from the database engine then sorts it by rows.
    RowSort,
    /// It works like rowsort except that it does not honor row groupings. Each individual result
    /// value is sorted on its own.
    ValueSort,
}

/// Whether the results should be parsed as value-wise or row-wise
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ResultMode {
    /// Results are in a single column
    ValueWise,
    /// The default option where results are in columns separated by spaces
    RowWise,
}

impl fmt::Display for ResultMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// The condition to run a query.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Condition {
    /// The statement or query is evaluated only if the label is seen.
    OnlyIf { label: String },
    /// The statement or query is not evaluated if the label is seen.
    SkipIf { label: String },
}

impl Condition {
    /// Evaluate condition on given `label`, returns whether to skip this record.
    pub(crate) fn should_skip<'a>(&'a self, labels: impl IntoIterator<Item = &'a str>) -> bool {
        match self {
            Condition::OnlyIf { label } => !labels.into_iter().contains(&label.as_str()),
            Condition::SkipIf { label } => labels.into_iter().contains(&label.as_str()),
        }
    }
}

/// The connection to use for the following statement.
#[derive(Default, Debug, PartialEq, Eq, Hash, Clone)]
pub enum Connection {
    /// The default connection if not specified or if the name is "default".
    #[default]
    Default,
    /// A named connection.
    Named(String),
}

impl Connection {
    /// Creates a new connection from a name.
    pub(crate) fn new(name: impl AsRef<str>) -> Self {
        match name.as_ref() {
            "default" => Self::Default,
            name => Self::Named(name.to_owned()),
        }
    }
}

// Control item trait and implementations

pub trait ControlItem: Sized {
    /// Try to parse from string.
    fn try_from_str(s: &str) -> Result<Self, ParseErrorKind>;

    /// Convert to string.
    fn as_str(&self) -> &'static str;
}

impl ControlItem for bool {
    fn try_from_str(s: &str) -> Result<Self, ParseErrorKind> {
        match s {
            "on" => Ok(true),
            "off" => Ok(false),
            _ => Err(ParseErrorKind::InvalidControl(s.to_string())),
        }
    }

    fn as_str(&self) -> &'static str {
        if *self {
            "on"
        } else {
            "off"
        }
    }
}

impl ControlItem for SortMode {
    fn try_from_str(s: &str) -> Result<Self, ParseErrorKind> {
        match s {
            "nosort" => Ok(Self::NoSort),
            "rowsort" => Ok(Self::RowSort),
            "valuesort" => Ok(Self::ValueSort),
            _ => Err(ParseErrorKind::InvalidSortMode(s.to_string())),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::NoSort => "nosort",
            Self::RowSort => "rowsort",
            Self::ValueSort => "valuesort",
        }
    }
}

impl ControlItem for ResultMode {
    fn try_from_str(s: &str) -> Result<Self, ParseErrorKind> {
        match s {
            "rowwise" => Ok(Self::RowWise),
            "valuewise" => Ok(Self::ValueWise),
            _ => Err(ParseErrorKind::InvalidSortMode(s.to_string())),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::RowWise => "rowwise",
            Self::ValueWise => "valuewise",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_condition_should_skip_onlyif() {
        let cond = Condition::OnlyIf { label: "postgres".to_string() };
        assert!(!cond.should_skip(vec!["postgres"]));
        assert!(cond.should_skip(vec!["sqlite"]));
        assert!(cond.should_skip(vec![]));
    }

    #[test]
    fn test_condition_should_skip_skipif() {
        let cond = Condition::SkipIf { label: "postgres".to_string() };
        assert!(cond.should_skip(vec!["postgres"]));
        assert!(!cond.should_skip(vec!["sqlite"]));
        assert!(!cond.should_skip(vec![]));
    }

    #[test]
    fn test_connection_new() {
        assert_eq!(Connection::new("default"), Connection::Default);
        assert_eq!(Connection::new("conn1"), Connection::Named("conn1".to_string()));
    }
}
