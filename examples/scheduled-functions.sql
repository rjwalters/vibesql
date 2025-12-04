-- Scheduled Functions Examples
-- VibeSQL supports scheduled execution of SQL statements for background tasks,
-- cleanup operations, and recurring jobs.

--------------------------------------------------------------------------------
-- ONE-TIME SCHEDULED TASKS (SCHEDULE AFTER / SCHEDULE AT)
--------------------------------------------------------------------------------

-- Schedule a task to run after a time interval
-- Use SCHEDULE AFTER for relative delays (e.g., "run in 5 minutes")
SCHEDULE AFTER INTERVAL '5 minutes'
  DELETE FROM sessions WHERE expires_at < NOW();

-- Schedule session cleanup in 1 hour
SCHEDULE AFTER INTERVAL '1 hour'
  UPDATE users SET status = 'inactive'
  WHERE last_seen < NOW() - INTERVAL '30 days';

-- Schedule a task for a specific timestamp
-- Use SCHEDULE AT for absolute times
SCHEDULE AT TIMESTAMP '2024-12-25 00:00:00'
  INSERT INTO notifications (user_id, message, created_at)
  SELECT id, 'Happy Holidays!', NOW() FROM users WHERE subscribed = TRUE;

-- Schedule end-of-day report generation
SCHEDULE AT TIMESTAMP '2024-12-31 23:59:59'
  INSERT INTO reports (type, data, generated_at)
  VALUES ('year_end', (SELECT json_agg(row_to_json(t)) FROM yearly_summary t), NOW());

--------------------------------------------------------------------------------
-- RECURRING CRON JOBS (CREATE CRON)
--------------------------------------------------------------------------------

-- Create a recurring job using standard 5-field cron syntax
-- Format: minute hour day-of-month month day-of-week
--
-- Field values:
--   *      = any value
--   */n    = every n intervals
--   n      = specific value
--   n-m    = range
--   n,m    = list

-- Daily cleanup at midnight (0:00)
CREATE CRON cleanup_expired_sessions
  SCHEDULE '0 0 * * *'
  AS DELETE FROM sessions WHERE expires_at < NOW();

-- Hourly stats aggregation
CREATE CRON aggregate_hourly_stats
  SCHEDULE '0 * * * *'
  AS INSERT INTO hourly_stats (hour, page_views, unique_visitors)
     SELECT
       date_trunc('hour', NOW() - INTERVAL '1 hour'),
       COUNT(*),
       COUNT(DISTINCT user_id)
     FROM page_views
     WHERE created_at >= NOW() - INTERVAL '1 hour';

-- Weekly archive on Sunday at 3 AM
CREATE CRON archive_old_logs
  SCHEDULE '0 3 * * 0'
  AS INSERT INTO logs_archive
     SELECT * FROM logs WHERE created_at < NOW() - INTERVAL '30 days';

-- Monthly report on first day of month at 6 AM
CREATE CRON generate_monthly_report
  SCHEDULE '0 6 1 * *'
  AS INSERT INTO reports (type, period, data, generated_at)
     VALUES (
       'monthly',
       date_trunc('month', NOW() - INTERVAL '1 month'),
       (SELECT json_agg(row_to_json(t)) FROM monthly_metrics t
        WHERE month = date_trunc('month', NOW() - INTERVAL '1 month')),
       NOW()
     );

-- Every 15 minutes - cache refresh
CREATE CRON refresh_materialized_cache
  SCHEDULE '*/15 * * * *'
  AS DELETE FROM product_cache WHERE updated_at < NOW() - INTERVAL '15 minutes';

-- Every 6 hours - data sync
CREATE CRON sync_external_data
  SCHEDULE '0 */6 * * *'
  AS UPDATE sync_status SET last_sync = NOW() WHERE source = 'external';

--------------------------------------------------------------------------------
-- MANAGING CRON JOBS (ALTER CRON / DROP CRON)
--------------------------------------------------------------------------------

-- Disable a cron job (stops execution without removing it)
ALTER CRON cleanup_expired_sessions SET enabled = FALSE;

-- Re-enable a cron job
ALTER CRON cleanup_expired_sessions SET enabled = TRUE;

-- Change the schedule
ALTER CRON aggregate_hourly_stats SET SCHEDULE '30 * * * *';

-- Change the SQL statement
ALTER CRON refresh_materialized_cache
  SET STATEMENT
    DELETE FROM product_cache WHERE updated_at < NOW() - INTERVAL '30 minutes';

-- Drop a cron job
DROP CRON archive_old_logs;

-- Drop with IF EXISTS (no error if doesn't exist)
DROP CRON IF EXISTS nonexistent_job;

--------------------------------------------------------------------------------
-- CANCELING ONE-TIME SCHEDULES
--------------------------------------------------------------------------------

-- One-time schedules return a schedule_id that can be used to cancel them
-- Cancel by schedule ID (UUID format)
CANCEL SCHEDULE 'a1b2c3d4-e5f6-7890-abcd-ef1234567890';

--------------------------------------------------------------------------------
-- VIEWING SCHEDULED TASKS
--------------------------------------------------------------------------------

-- View all pending one-time scheduled tasks
SELECT * FROM vibesql_schedules;

-- View scheduled tasks with details
SELECT
  id,
  sql,
  run_at,
  status,
  attempts,
  last_error,
  created_at
FROM vibesql_schedules
WHERE status = 'pending'
ORDER BY run_at;

-- View all cron jobs
SELECT * FROM vibesql_crons;

-- View cron jobs with next execution time
SELECT
  name,
  schedule,
  sql,
  enabled,
  last_run,
  next_run,
  created_at
FROM vibesql_crons
ORDER BY next_run;

-- View execution history
SELECT
  schedule_id,
  cron_name,
  started_at,
  completed_at,
  status,
  error,
  rows_affected
FROM vibesql_execution_history
ORDER BY started_at DESC
LIMIT 100;

--------------------------------------------------------------------------------
-- USE CASES
--------------------------------------------------------------------------------

-- 1. Session Management: Clean up expired sessions
CREATE CRON session_cleanup
  SCHEDULE '*/5 * * * *'
  AS DELETE FROM sessions WHERE expires_at < NOW();

-- 2. Data Archival: Move old data to archive tables
CREATE CRON archive_orders
  SCHEDULE '0 2 * * *'
  AS BEGIN;
     INSERT INTO orders_archive SELECT * FROM orders WHERE created_at < NOW() - INTERVAL '1 year';
     DELETE FROM orders WHERE created_at < NOW() - INTERVAL '1 year';
     COMMIT;
     END;

-- 3. Cache Invalidation: Clear stale cache entries
CREATE CRON invalidate_cache
  SCHEDULE '*/10 * * * *'
  AS DELETE FROM cache WHERE expires_at < NOW();

-- 4. Report Generation: Daily sales report
CREATE CRON daily_sales_report
  SCHEDULE '0 8 * * *'
  AS INSERT INTO daily_reports (date, total_sales, order_count)
     SELECT
       CURRENT_DATE - INTERVAL '1 day',
       SUM(total),
       COUNT(*)
     FROM orders
     WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
       AND created_at < CURRENT_DATE;

-- 5. Notification Scheduling: Send reminders
SCHEDULE AFTER INTERVAL '24 hours'
  INSERT INTO notification_queue (user_id, type, message)
  SELECT user_id, 'reminder', 'Your trial expires tomorrow!'
  FROM subscriptions
  WHERE trial_end = CURRENT_DATE + INTERVAL '1 day';

--------------------------------------------------------------------------------
-- CRON EXPRESSION REFERENCE
--------------------------------------------------------------------------------

-- Format: minute hour day-of-month month day-of-week
--
-- Examples:
--   '0 0 * * *'     - Daily at midnight
--   '*/15 * * * *'  - Every 15 minutes
--   '0 */2 * * *'   - Every 2 hours
--   '0 9 * * 1-5'   - 9 AM on weekdays (Mon-Fri)
--   '0 0 1 * *'     - First day of each month at midnight
--   '0 0 * * 0'     - Every Sunday at midnight
--   '30 4 1,15 * *' - 4:30 AM on 1st and 15th of each month
