//! Storage models for scheduled tasks and cron jobs
//!
//! These types represent the persistent state of scheduled executions.

use chrono::{DateTime, Utc};
use std::fmt;

/// Status of a scheduled task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScheduleStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl fmt::Display for ScheduleStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScheduleStatus::Pending => write!(f, "pending"),
            ScheduleStatus::Running => write!(f, "running"),
            ScheduleStatus::Completed => write!(f, "completed"),
            ScheduleStatus::Failed => write!(f, "failed"),
            ScheduleStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// A one-time scheduled task record
#[derive(Debug, Clone)]
pub struct ScheduleRecord {
    pub id: String,
    pub sql: String,
    pub params: Option<Vec<u8>>,
    pub run_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub status: ScheduleStatus,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// A recurring cron job record
#[derive(Debug, Clone)]
pub struct CronRecord {
    pub name: String,
    pub schedule: String,
    pub sql: String,
    pub params: Option<Vec<u8>>,
    pub enabled: bool,
    pub last_run: Option<DateTime<Utc>>,
    pub next_run: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Execution history for scheduled tasks
#[derive(Debug, Clone)]
pub struct ExecutionHistoryRecord {
    pub id: Option<i64>,
    pub schedule_id: Option<String>,
    pub cron_name: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub status: ScheduleStatus,
    pub error: Option<String>,
    pub rows_affected: Option<i64>,
}
