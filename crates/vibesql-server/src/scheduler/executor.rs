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

        // Validate SQL parses before retrying (don't retry parse errors)
        if let Err(e) = Parser::parse_sql(&schedule.sql) {
            return Ok(ExecutionHistoryRecord {
                id: None,
                schedule_id: Some(schedule.id.clone()),
                cron_name: None,
                started_at,
                completed_at: Some(Utc::now()),
                status: ScheduleStatus::Failed,
                error: Some(e.to_string()),
                rows_affected: None,
            });
        }

        // Execute with retries
        #[allow(unused_assignments)] // Initial None is never read, but keeps the code clear
        let mut last_error: Option<String> = None;
        let mut attempt = 0;

        loop {
            attempt += 1;
            let backoff = self.calculate_backoff(attempt - 1);

            match self.execute_statement(&schedule.sql, &session).await {
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

    /// Execute a single SQL statement via the session
    async fn execute_statement(&self, sql: &str, session: &Arc<Mutex<Session>>) -> Result<usize> {
        let mut session_guard = session.lock().await;
        let result = session_guard.execute(sql).await?;
        Ok(result.rows_affected() as usize)
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

    #[tokio::test]
    async fn test_execute_schedule_insert() {
        // Create a session with a table
        let mut session = Session::new_standalone("testdb".to_string(), "testuser".to_string());
        session.execute("CREATE TABLE schedule_test (id INT, value VARCHAR(100))").await.unwrap();
        let session = Arc::new(Mutex::new(session));

        // Create executor
        let executor = ScheduleExecutor::new(ScheduleExecutorConfig::default());

        // Create a schedule record for INSERT
        let schedule = ScheduleRecord {
            id: "test-schedule-1".to_string(),
            sql: "INSERT INTO schedule_test VALUES (1, 'scheduled')".to_string(),
            params: None,
            run_at: Utc::now(),
            created_at: Utc::now(),
            status: ScheduleStatus::Pending,
            attempts: 0,
            last_error: None,
            completed_at: None,
        };

        // Execute the schedule
        let result = executor.execute_schedule(&schedule, session.clone()).await.unwrap();

        // Verify success
        assert_eq!(result.status, ScheduleStatus::Completed);
        assert!(result.error.is_none());
        assert_eq!(result.rows_affected, Some(1));

        // Verify data was inserted
        let session_guard = session.lock().await;
        let _select_result = Session::new_standalone("testdb".to_string(), "testuser".to_string());
        drop(session_guard);

        let mut verify_session = session.lock().await;
        let verify = verify_session.execute("SELECT * FROM schedule_test WHERE id = 1").await.unwrap();
        match verify {
            crate::session::ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[tokio::test]
    async fn test_execute_schedule_update() {
        // Create a session with a table and initial data
        let mut session = Session::new_standalone("testdb".to_string(), "testuser".to_string());
        session.execute("CREATE TABLE update_test (id INT, value VARCHAR(100))").await.unwrap();
        session.execute("INSERT INTO update_test VALUES (1, 'original')").await.unwrap();
        let session = Arc::new(Mutex::new(session));

        // Create executor
        let executor = ScheduleExecutor::new(ScheduleExecutorConfig::default());

        // Create a schedule record for UPDATE
        let schedule = ScheduleRecord {
            id: "test-schedule-2".to_string(),
            sql: "UPDATE update_test SET value = 'updated' WHERE id = 1".to_string(),
            params: None,
            run_at: Utc::now(),
            created_at: Utc::now(),
            status: ScheduleStatus::Pending,
            attempts: 0,
            last_error: None,
            completed_at: None,
        };

        // Execute the schedule
        let result = executor.execute_schedule(&schedule, session.clone()).await.unwrap();

        // Verify success
        assert_eq!(result.status, ScheduleStatus::Completed);
        assert!(result.error.is_none());
        assert_eq!(result.rows_affected, Some(1));
    }

    #[tokio::test]
    async fn test_execute_schedule_delete() {
        // Create a session with a table and initial data
        let mut session = Session::new_standalone("testdb".to_string(), "testuser".to_string());
        session.execute("CREATE TABLE delete_test (id INT, value VARCHAR(100))").await.unwrap();
        session.execute("INSERT INTO delete_test VALUES (1, 'to_delete')").await.unwrap();
        session.execute("INSERT INTO delete_test VALUES (2, 'to_keep')").await.unwrap();
        let session = Arc::new(Mutex::new(session));

        // Create executor
        let executor = ScheduleExecutor::new(ScheduleExecutorConfig::default());

        // Create a schedule record for DELETE
        let schedule = ScheduleRecord {
            id: "test-schedule-3".to_string(),
            sql: "DELETE FROM delete_test WHERE id = 1".to_string(),
            params: None,
            run_at: Utc::now(),
            created_at: Utc::now(),
            status: ScheduleStatus::Pending,
            attempts: 0,
            last_error: None,
            completed_at: None,
        };

        // Execute the schedule
        let result = executor.execute_schedule(&schedule, session.clone()).await.unwrap();

        // Verify success
        assert_eq!(result.status, ScheduleStatus::Completed);
        assert!(result.error.is_none());
        assert_eq!(result.rows_affected, Some(1));
    }

    #[tokio::test]
    async fn test_execute_schedule_select() {
        // Create a session with a table and data
        let mut session = Session::new_standalone("testdb".to_string(), "testuser".to_string());
        session.execute("CREATE TABLE select_test (id INT, value VARCHAR(100))").await.unwrap();
        session.execute("INSERT INTO select_test VALUES (1, 'row1')").await.unwrap();
        session.execute("INSERT INTO select_test VALUES (2, 'row2')").await.unwrap();
        let session = Arc::new(Mutex::new(session));

        // Create executor
        let executor = ScheduleExecutor::new(ScheduleExecutorConfig::default());

        // Create a schedule record for SELECT
        let schedule = ScheduleRecord {
            id: "test-schedule-4".to_string(),
            sql: "SELECT * FROM select_test".to_string(),
            params: None,
            run_at: Utc::now(),
            created_at: Utc::now(),
            status: ScheduleStatus::Pending,
            attempts: 0,
            last_error: None,
            completed_at: None,
        };

        // Execute the schedule
        let result = executor.execute_schedule(&schedule, session.clone()).await.unwrap();

        // Verify success - SELECT returns rows as rows_affected
        assert_eq!(result.status, ScheduleStatus::Completed);
        assert!(result.error.is_none());
        assert_eq!(result.rows_affected, Some(2));
    }

    #[tokio::test]
    async fn test_execute_schedule_invalid_sql() {
        // Create a session
        let session = Session::new_standalone("testdb".to_string(), "testuser".to_string());
        let session = Arc::new(Mutex::new(session));

        // Create executor
        let executor = ScheduleExecutor::new(ScheduleExecutorConfig::default());

        // Create a schedule record with invalid SQL
        let schedule = ScheduleRecord {
            id: "test-schedule-5".to_string(),
            sql: "INVALID SQL SYNTAX HERE".to_string(),
            params: None,
            run_at: Utc::now(),
            created_at: Utc::now(),
            status: ScheduleStatus::Pending,
            attempts: 0,
            last_error: None,
            completed_at: None,
        };

        // Execute the schedule
        let result = executor.execute_schedule(&schedule, session.clone()).await.unwrap();

        // Verify failure (parse error - no retries)
        assert_eq!(result.status, ScheduleStatus::Failed);
        assert!(result.error.is_some());
        assert!(result.rows_affected.is_none());
    }

    #[tokio::test]
    async fn test_execute_schedule_table_not_found() {
        // Create a session without the table
        let session = Session::new_standalone("testdb".to_string(), "testuser".to_string());
        let session = Arc::new(Mutex::new(session));

        // Create executor with minimal retries for faster test
        let executor = ScheduleExecutor::new(ScheduleExecutorConfig {
            max_retries: 1,
            initial_backoff: StdDuration::from_millis(10),
            ..Default::default()
        });

        // Create a schedule record for a non-existent table
        let schedule = ScheduleRecord {
            id: "test-schedule-6".to_string(),
            sql: "INSERT INTO nonexistent_table VALUES (1, 'test')".to_string(),
            params: None,
            run_at: Utc::now(),
            created_at: Utc::now(),
            status: ScheduleStatus::Pending,
            attempts: 0,
            last_error: None,
            completed_at: None,
        };

        // Execute the schedule
        let result = executor.execute_schedule(&schedule, session.clone()).await.unwrap();

        // Verify failure (execution error after retries)
        assert_eq!(result.status, ScheduleStatus::Failed);
        assert!(result.error.is_some());
        assert!(result.rows_affected.is_none());
    }

    #[tokio::test]
    async fn test_execute_schedule_create_table() {
        // Create a session
        let session = Session::new_standalone("testdb".to_string(), "testuser".to_string());
        let session = Arc::new(Mutex::new(session));

        // Create executor
        let executor = ScheduleExecutor::new(ScheduleExecutorConfig::default());

        // Create a schedule record for CREATE TABLE
        let schedule = ScheduleRecord {
            id: "test-schedule-7".to_string(),
            sql: "CREATE TABLE scheduled_table (id INT, name VARCHAR(100))".to_string(),
            params: None,
            run_at: Utc::now(),
            created_at: Utc::now(),
            status: ScheduleStatus::Pending,
            attempts: 0,
            last_error: None,
            completed_at: None,
        };

        // Execute the schedule
        let result = executor.execute_schedule(&schedule, session.clone()).await.unwrap();

        // Verify success - DDL returns 0 rows_affected
        assert_eq!(result.status, ScheduleStatus::Completed);
        assert!(result.error.is_none());
        assert_eq!(result.rows_affected, Some(0));

        // Verify table was created by inserting into it
        let mut session_guard = session.lock().await;
        let insert_result = session_guard.execute("INSERT INTO scheduled_table VALUES (1, 'test')").await;
        assert!(insert_result.is_ok());
    }
}
