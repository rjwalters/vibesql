# Scheduled Functions

VibeSQL supports scheduled execution of SQL statements, enabling background tasks, cleanup operations, and recurring jobs without external schedulers like cron or task queues.

## Overview

There are two types of scheduled tasks:

1. **One-time tasks** - Execute once after a delay or at a specific time
   - `SCHEDULE AFTER` - Run after a time interval
   - `SCHEDULE AT` - Run at a specific timestamp

2. **Recurring jobs** - Execute repeatedly on a cron schedule
   - `CREATE CRON` - Create a recurring job
   - `ALTER CRON` - Modify an existing job
   - `DROP CRON` - Remove a job

## One-Time Scheduled Tasks

### SCHEDULE AFTER

Schedule a task to execute after a specified time interval.

```sql
SCHEDULE AFTER INTERVAL '<interval>'
  <sql_statement>;
```

**Examples:**

```sql
-- Clean up expired sessions in 5 minutes
SCHEDULE AFTER INTERVAL '5 minutes'
  DELETE FROM sessions WHERE expires_at < NOW();

-- Mark inactive users in 1 hour
SCHEDULE AFTER INTERVAL '1 hour'
  UPDATE users SET status = 'inactive'
  WHERE last_seen < NOW() - INTERVAL '30 days';

-- Archive old data in 24 hours
SCHEDULE AFTER INTERVAL '24 hours'
  INSERT INTO orders_archive SELECT * FROM orders WHERE created_at < NOW() - INTERVAL '1 year';
```

### SCHEDULE AT

Schedule a task to execute at a specific timestamp.

```sql
SCHEDULE AT TIMESTAMP '<timestamp>'
  <sql_statement>;
```

**Examples:**

```sql
-- Send holiday notification at midnight on Christmas
SCHEDULE AT TIMESTAMP '2024-12-25 00:00:00'
  INSERT INTO notifications (user_id, message, created_at)
  SELECT id, 'Happy Holidays!', NOW() FROM users WHERE subscribed = TRUE;

-- Generate end-of-year report
SCHEDULE AT TIMESTAMP '2024-12-31 23:59:59'
  INSERT INTO reports (type, data, generated_at)
  VALUES ('year_end', (SELECT json_agg(row_to_json(t)) FROM yearly_summary t), NOW());
```

### Canceling One-Time Tasks

One-time scheduled tasks can be canceled using their schedule ID:

```sql
CANCEL SCHEDULE '<schedule_id>';
```

The schedule ID is a UUID returned when the task is created and can be found in the `vibesql_schedules` system table.

## Recurring Cron Jobs

### CREATE CRON

Create a recurring job using standard cron syntax.

```sql
CREATE CRON <job_name>
  SCHEDULE '<cron_expression>'
  AS <sql_statement>;
```

**Examples:**

```sql
-- Daily cleanup at midnight
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

-- Every 15 minutes - cache refresh
CREATE CRON refresh_cache
  SCHEDULE '*/15 * * * *'
  AS DELETE FROM cache WHERE expires_at < NOW();

-- Weekly archive on Sundays at 3 AM
CREATE CRON archive_logs
  SCHEDULE '0 3 * * 0'
  AS INSERT INTO logs_archive SELECT * FROM logs WHERE created_at < NOW() - INTERVAL '30 days';
```

### ALTER CRON

Modify an existing cron job.

```sql
-- Disable a job
ALTER CRON <job_name> SET enabled = FALSE;

-- Enable a job
ALTER CRON <job_name> SET enabled = TRUE;

-- Change the schedule
ALTER CRON <job_name> SET SCHEDULE '<new_cron_expression>';

-- Change the SQL statement
ALTER CRON <job_name> SET STATEMENT <new_sql_statement>;
```

**Examples:**

```sql
-- Temporarily disable the archive job
ALTER CRON archive_logs SET enabled = FALSE;

-- Change cleanup to run every 6 hours instead of daily
ALTER CRON cleanup_expired_sessions SET SCHEDULE '0 */6 * * *';

-- Update the cache refresh query
ALTER CRON refresh_cache
  SET STATEMENT DELETE FROM cache WHERE updated_at < NOW() - INTERVAL '30 minutes';

-- Re-enable the archive job
ALTER CRON archive_logs SET enabled = TRUE;
```

### DROP CRON

Remove a cron job.

```sql
DROP CRON <job_name>;
DROP CRON IF EXISTS <job_name>;
```

**Examples:**

```sql
-- Remove the archive job
DROP CRON archive_logs;

-- Safe removal (no error if doesn't exist)
DROP CRON IF EXISTS nonexistent_job;
```

## Cron Expression Syntax

VibeSQL uses the standard 5-field cron format:

```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of month (1-31)
│ │ │ ┌───────────── month (1-12)
│ │ │ │ ┌───────────── day of week (0-6, Sunday=0)
│ │ │ │ │
* * * * *
```

### Special Characters

| Character | Description | Example |
|-----------|-------------|---------|
| `*` | Any value | `* * * * *` (every minute) |
| `*/n` | Every n intervals | `*/15 * * * *` (every 15 minutes) |
| `n` | Specific value | `0 9 * * *` (9:00 AM) |
| `n-m` | Range | `0 9-17 * * *` (9 AM to 5 PM, on the hour) |
| `n,m` | List | `0 9,12,18 * * *` (9 AM, noon, 6 PM) |

### Common Examples

| Expression | Description |
|------------|-------------|
| `0 0 * * *` | Daily at midnight |
| `*/15 * * * *` | Every 15 minutes |
| `0 */2 * * *` | Every 2 hours |
| `0 9 * * 1-5` | 9 AM on weekdays (Mon-Fri) |
| `0 0 1 * *` | First day of each month at midnight |
| `0 0 * * 0` | Every Sunday at midnight |
| `30 4 1,15 * *` | 4:30 AM on 1st and 15th of each month |
| `0 */6 * * *` | Every 6 hours |
| `0 8 * * 1` | Every Monday at 8 AM |

## System Tables

VibeSQL provides system tables to monitor and manage scheduled tasks.

### vibesql_schedules

One-time scheduled tasks.

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR | Unique schedule ID (UUID) |
| `sql` | TEXT | SQL statement to execute |
| `params` | BYTEA | Serialized parameters (optional) |
| `run_at` | TIMESTAMP | Scheduled execution time |
| `created_at` | TIMESTAMP | When the schedule was created |
| `status` | VARCHAR | pending, running, completed, failed, cancelled |
| `attempts` | INTEGER | Number of execution attempts |
| `last_error` | TEXT | Error message from last failed attempt |
| `completed_at` | TIMESTAMP | When execution completed |

```sql
-- View all pending schedules
SELECT id, sql, run_at, status FROM vibesql_schedules WHERE status = 'pending';

-- View failed schedules with errors
SELECT id, sql, last_error, attempts FROM vibesql_schedules WHERE status = 'failed';
```

### vibesql_crons

Recurring cron jobs.

| Column | Type | Description |
|--------|------|-------------|
| `name` | VARCHAR | Unique cron job name |
| `schedule` | VARCHAR | Cron expression |
| `sql` | TEXT | SQL statement to execute |
| `params` | BYTEA | Serialized parameters (optional) |
| `enabled` | BOOLEAN | Whether the job is active |
| `last_run` | TIMESTAMP | Last execution time |
| `next_run` | TIMESTAMP | Next scheduled execution |
| `created_at` | TIMESTAMP | When the cron was created |

```sql
-- View all cron jobs
SELECT name, schedule, enabled, next_run FROM vibesql_crons ORDER BY next_run;

-- View disabled cron jobs
SELECT name, schedule FROM vibesql_crons WHERE enabled = FALSE;
```

### vibesql_execution_history

Execution audit trail for all scheduled tasks.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Auto-incrementing history ID |
| `schedule_id` | VARCHAR | Schedule ID (for one-time tasks) |
| `cron_name` | VARCHAR | Cron name (for recurring jobs) |
| `started_at` | TIMESTAMP | When execution started |
| `completed_at` | TIMESTAMP | When execution completed |
| `status` | VARCHAR | completed, failed |
| `error` | TEXT | Error message if failed |
| `rows_affected` | BIGINT | Number of rows affected by DML |

```sql
-- View recent executions
SELECT * FROM vibesql_execution_history ORDER BY started_at DESC LIMIT 20;

-- View failed executions
SELECT cron_name, schedule_id, error, started_at
FROM vibesql_execution_history
WHERE status = 'failed'
ORDER BY started_at DESC;

-- View execution stats by cron job
SELECT
  cron_name,
  COUNT(*) as total_runs,
  COUNT(*) FILTER (WHERE status = 'completed') as successful,
  COUNT(*) FILTER (WHERE status = 'failed') as failed,
  AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration_secs
FROM vibesql_execution_history
WHERE cron_name IS NOT NULL
GROUP BY cron_name;
```

## Execution Behavior

### Retry Logic

Failed scheduled tasks are automatically retried with exponential backoff:

- **Max retries**: 3 attempts (configurable)
- **Initial backoff**: 5 seconds
- **Backoff multiplier**: 2x
- **Maximum backoff**: 300 seconds (5 minutes)

Parse errors (invalid SQL) are not retried.

### Execution Guarantees

- Tasks are executed in the context of a new session
- Each task runs in its own transaction
- Failed tasks are marked with error details for debugging
- Execution history is preserved for auditing

### Scheduler Polling

The scheduler polls for due tasks every 10 seconds (configurable). Tasks scheduled within this window will execute at the next poll interval.

## Common Use Cases

### 1. Session Cleanup

```sql
CREATE CRON session_cleanup
  SCHEDULE '*/5 * * * *'
  AS DELETE FROM sessions WHERE expires_at < NOW();
```

### 2. Data Archival

```sql
CREATE CRON archive_old_orders
  SCHEDULE '0 2 * * *'
  AS BEGIN;
     INSERT INTO orders_archive SELECT * FROM orders WHERE created_at < NOW() - INTERVAL '1 year';
     DELETE FROM orders WHERE created_at < NOW() - INTERVAL '1 year';
     COMMIT;
     END;
```

### 3. Cache Invalidation

```sql
CREATE CRON invalidate_stale_cache
  SCHEDULE '*/10 * * * *'
  AS DELETE FROM cache WHERE expires_at < NOW();
```

### 4. Report Generation

```sql
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
```

### 5. Notification Scheduling

```sql
-- Schedule a reminder for trial expiration
SCHEDULE AFTER INTERVAL '24 hours'
  INSERT INTO notification_queue (user_id, type, message)
  SELECT user_id, 'reminder', 'Your trial expires tomorrow!'
  FROM subscriptions
  WHERE trial_end = CURRENT_DATE + INTERVAL '1 day';
```

### 6. Metrics Aggregation

```sql
CREATE CRON hourly_metrics
  SCHEDULE '5 * * * *'
  AS INSERT INTO hourly_metrics (hour, requests, errors, avg_latency)
     SELECT
       date_trunc('hour', NOW() - INTERVAL '1 hour'),
       COUNT(*),
       COUNT(*) FILTER (WHERE status >= 500),
       AVG(latency_ms)
     FROM request_logs
     WHERE timestamp >= NOW() - INTERVAL '1 hour';
```

## Troubleshooting

### Task Not Executing

1. Check if the schedule is in `pending` status:
   ```sql
   SELECT * FROM vibesql_schedules WHERE status = 'pending';
   ```

2. Verify the cron job is enabled:
   ```sql
   SELECT name, enabled, next_run FROM vibesql_crons;
   ```

3. Check execution history for errors:
   ```sql
   SELECT * FROM vibesql_execution_history WHERE status = 'failed' ORDER BY started_at DESC;
   ```

### SQL Parse Errors

Parse errors are not retried. Check the `last_error` field:

```sql
SELECT id, sql, last_error FROM vibesql_schedules WHERE status = 'failed';
```

### High Retry Count

If a task repeatedly fails, check:
- The SQL syntax is valid
- Referenced tables and columns exist
- The user has required permissions

```sql
SELECT id, sql, attempts, last_error
FROM vibesql_schedules
WHERE attempts > 1
ORDER BY attempts DESC;
```

### Cron Expression Issues

Test your cron expression by checking `next_run`:

```sql
SELECT name, schedule, next_run FROM vibesql_crons WHERE name = 'my_job';
```

## TypeScript SDK

See the [TypeScript client examples](../packages/vibesql-client-ts/examples/scheduled-tasks/) for code examples using the SDK.

```typescript
import { VibeSqlClient } from '@vibesql/client';

const db = new VibeSqlClient({ host: 'localhost', ... });
await db.connect();

// Schedule one-time task
await db.query(`
  SCHEDULE AFTER INTERVAL '1 hour'
  DELETE FROM sessions WHERE expires_at < NOW()
`);

// Create recurring job
await db.query(`
  CREATE CRON daily_cleanup
  SCHEDULE '0 0 * * *'
  AS DELETE FROM logs WHERE created_at < NOW() - INTERVAL '30 days'
`);

// List cron jobs
const crons = await db.query('SELECT * FROM vibesql_crons');
```

## Related Documentation

- [CLI Guide](CLI_GUIDE.md) - Running scheduled functions from the CLI
- [HTTP API](../crates/vibesql-server/HTTP_API.md) - REST API for executing SQL
