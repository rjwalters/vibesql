//! Schedule execution engine with retry logic
//!
//! Handles background execution of scheduled tasks and cron jobs with automatic retry.

use crate::Session;
use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::Mutex;
use tracing::{error, info, warn};
use vibesql_parser::Parser;

use super::storage::{ExecutionHistoryRecord, ScheduleRecord, ScheduleStatus};

/// Configuration for schedule executor
#[derive(Debug, Clone)]
pub struct ScheduleExecutorConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial retry backoff duration
    pub initial_backoff: StdDuration,
    /// Maximum retry backoff duration
    pub max_backoff: StdDuration,
    /// Exponential backoff multiplier
    pub backoff_multiplier: f64,
}

impl Default for ScheduleExecutorConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: StdDuration::from_secs(5),
            max_backoff: StdDuration::from_secs(300),
            backoff_multiplier: 2.0,
        }
    }
}

/// Executes scheduled tasks with retry logic
pub struct ScheduleExecutor {
    config: ScheduleExecutorConfig,
}

impl ScheduleExecutor {
    pub fn new(config: ScheduleExecutorConfig) -> Self {
        Self { config }
    }

    /// Execute a scheduled task with retry logic
    pub async fn execute_schedule(
        &self,
        schedule: &ScheduleRecord,
        session: Arc<Mutex<Session>>,
    ) -> Result<ExecutionHistoryRecord> {
        let started_at = Utc::now();

        // Parse the SQL statement
        let statement = match Parser::parse_sql(&schedule.sql) {
            Ok(stmt) => stmt,
            Err(e) => {
                let error_msg = e.to_string();
                return Ok(ExecutionHistoryRecord {
                    id: None,
                    schedule_id: Some(schedule.id.clone()),
                    cron_name: None,
                    started_at,
                    completed_at: Some(Utc::now()),
                    status: ScheduleStatus::Failed,
                    error: Some(error_msg),
                    rows_affected: None,
                });
            }
        };

        // Execute with retries
        #[allow(unused_assignments)] // Initial None is never read, but keeps the code clear
        let mut last_error: Option<String> = None;
        let mut attempt = 0;

        loop {
            attempt += 1;
            let backoff = self.calculate_backoff(attempt - 1);

            match self.execute_statement(&statement, &session).await {
                Ok(rows_affected) => {
                    info!(
                        schedule_id = %schedule.id,
                        rows_affected = rows_affected,
                        attempts = attempt,
                        "Schedule executed successfully"
                    );
                    return Ok(ExecutionHistoryRecord {
                        id: None,
                        schedule_id: Some(schedule.id.clone()),
                        cron_name: None,
                        started_at,
                        completed_at: Some(Utc::now()),
                        status: ScheduleStatus::Completed,
                        error: None,
                        rows_affected: Some(rows_affected as i64),
                    });
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    warn!(
                        schedule_id = %schedule.id,
                        attempt = attempt,
                        error = %e,
                        "Schedule execution failed, will retry"
                    );

                    if attempt >= self.config.max_retries {
                        error!(
                            schedule_id = %schedule.id,
                            attempts = attempt,
                            error = %last_error.as_ref().unwrap(),
                            "Schedule execution failed after all retries"
                        );
                        return Ok(ExecutionHistoryRecord {
                            id: None,
                            schedule_id: Some(schedule.id.clone()),
                            cron_name: None,
                            started_at,
                            completed_at: Some(Utc::now()),
                            status: ScheduleStatus::Failed,
                            error: last_error,
                            rows_affected: None,
                        });
                    }

                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// Calculate exponential backoff duration
    fn calculate_backoff(&self, attempt: u32) -> StdDuration {
        let backoff_secs = self.config.initial_backoff.as_secs_f64()
            * self.config.backoff_multiplier.powi(attempt as i32);

        let max_secs = self.config.max_backoff.as_secs_f64();
        let capped_secs = backoff_secs.min(max_secs);

        StdDuration::from_secs_f64(capped_secs)
    }

    /// Execute a single statement
    async fn execute_statement(
        &self,
        _statement: &vibesql_ast::Statement,
        _session: &Arc<Mutex<Session>>,
    ) -> Result<usize> {
        // This will be implemented when we have access to the Executor
        // For now, return a placeholder
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_calculation() {
        let config = ScheduleExecutorConfig {
            initial_backoff: StdDuration::from_secs(5),
            max_backoff: StdDuration::from_secs(300),
            backoff_multiplier: 2.0,
            ..Default::default()
        };

        let executor = ScheduleExecutor::new(config);

        // First retry: 5 seconds
        assert_eq!(executor.calculate_backoff(0), StdDuration::from_secs(5));

        // Second retry: 10 seconds
        assert_eq!(executor.calculate_backoff(1), StdDuration::from_secs(10));

        // Third retry: 20 seconds
        assert_eq!(executor.calculate_backoff(2), StdDuration::from_secs(20));

        // Should cap at max_backoff (300 seconds)
        let very_high = executor.calculate_backoff(100);
        assert_eq!(very_high, StdDuration::from_secs(300));
    }
}
