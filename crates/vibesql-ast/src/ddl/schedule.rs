//! Scheduled functions and cron jobs
//!
//! This module contains AST nodes for VibeSQL's scheduled function execution feature.
//! Supports one-time scheduled tasks and recurring cron jobs.

use crate::Expression;

/// SCHEDULE AFTER statement - schedule a task to run after a time interval
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleAfterStmt {
    /// The interval expression (e.g., INTERVAL '5 minutes')
    pub interval: Box<Expression>,
    /// The SQL statement to execute
    pub statement: Box<crate::Statement>,
}

/// SCHEDULE AT statement - schedule a task to run at a specific time
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduleAtStmt {
    /// The timestamp expression (e.g., TIMESTAMP '2024-12-25 00:00:00')
    pub timestamp: Box<Expression>,
    /// The SQL statement to execute
    pub statement: Box<crate::Statement>,
}

/// CREATE CRON statement - create a recurring cron job
#[derive(Debug, Clone, PartialEq)]
pub struct CreateCronStmt {
    /// The name of the cron job
    pub cron_name: String,
    /// The cron schedule expression (5-field format)
    pub schedule: String,
    /// The SQL statement to execute
    pub statement: Box<crate::Statement>,
}

/// DROP CRON statement - remove a cron job
#[derive(Debug, Clone, PartialEq)]
pub struct DropCronStmt {
    pub cron_name: String,
    pub if_exists: bool,
}

/// ALTER CRON statement - modify a cron job
#[derive(Debug, Clone, PartialEq)]
pub struct AlterCronStmt {
    pub cron_name: String,
    pub schedule: Option<String>,
    pub statement: Option<Box<crate::Statement>>,
    pub enabled: Option<bool>,
}

/// CANCEL SCHEDULE statement - remove a scheduled task
#[derive(Debug, Clone, PartialEq)]
pub struct CancelScheduleStmt {
    /// The schedule ID (UUID)
    pub schedule_id: String,
}
