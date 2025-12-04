/**
 * Scheduled Tasks Example
 * Demonstrates scheduling one-time tasks and recurring cron jobs
 */

import { VibeSqlClient } from '../../src/client';

interface ScheduleRecord {
  id: string;
  sql: string;
  run_at: Date;
  status: string;
  attempts: number;
  last_error: string | null;
  created_at: Date;
}

interface CronRecord {
  name: string;
  schedule: string;
  sql: string;
  enabled: boolean;
  last_run: Date | null;
  next_run: Date | null;
  created_at: Date;
}

interface ExecutionHistory {
  id: number;
  schedule_id: string | null;
  cron_name: string | null;
  started_at: Date;
  completed_at: Date | null;
  status: string;
  error: string | null;
  rows_affected: number | null;
}

async function main() {
  const db = new VibeSqlClient({
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    database: process.env.DB_NAME || 'mydb',
    user: process.env.DB_USER || 'postgres',
    password: process.env.DB_PASSWORD,
  });

  try {
    console.log('Connecting to database...');
    await db.connect();
    console.log('Connected!\n');

    // -------------------------------------------------------------------------
    // ONE-TIME SCHEDULED TASKS
    // -------------------------------------------------------------------------

    console.log('=== One-Time Scheduled Tasks ===\n');

    // Schedule a cleanup task to run in 5 minutes
    console.log('Scheduling session cleanup in 5 minutes...');
    await db.query(`
      SCHEDULE AFTER INTERVAL '5 minutes'
      DELETE FROM sessions WHERE expires_at < NOW()
    `);
    console.log('Scheduled!\n');

    // Schedule a task for a specific time
    console.log('Scheduling holiday notification...');
    await db.query(`
      SCHEDULE AT TIMESTAMP '2024-12-25 00:00:00'
      INSERT INTO notifications (user_id, message, created_at)
      SELECT id, 'Happy Holidays!', NOW()
      FROM users WHERE subscribed = TRUE
    `);
    console.log('Scheduled!\n');

    // Schedule user status update in 1 hour
    console.log('Scheduling inactive user update in 1 hour...');
    await db.query(`
      SCHEDULE AFTER INTERVAL '1 hour'
      UPDATE users SET status = 'inactive'
      WHERE last_seen < NOW() - INTERVAL '30 days'
    `);
    console.log('Scheduled!\n');

    // -------------------------------------------------------------------------
    // RECURRING CRON JOBS
    // -------------------------------------------------------------------------

    console.log('=== Recurring Cron Jobs ===\n');

    // Create a daily cleanup job (runs at midnight)
    console.log('Creating daily session cleanup cron...');
    await db.query(`
      CREATE CRON cleanup_sessions
      SCHEDULE '0 0 * * *'
      AS DELETE FROM sessions WHERE expires_at < NOW()
    `);
    console.log('Created!\n');

    // Create an hourly stats aggregation job
    console.log('Creating hourly stats aggregation cron...');
    await db.query(`
      CREATE CRON aggregate_stats
      SCHEDULE '0 * * * *'
      AS INSERT INTO hourly_stats (hour, page_views, unique_visitors)
         SELECT
           date_trunc('hour', NOW() - INTERVAL '1 hour'),
           COUNT(*),
           COUNT(DISTINCT user_id)
         FROM page_views
         WHERE created_at >= NOW() - INTERVAL '1 hour'
    `);
    console.log('Created!\n');

    // Create a weekly archive job (runs Sundays at 3 AM)
    console.log('Creating weekly archive cron...');
    await db.query(`
      CREATE CRON archive_old_logs
      SCHEDULE '0 3 * * 0'
      AS INSERT INTO logs_archive
         SELECT * FROM logs WHERE created_at < NOW() - INTERVAL '30 days'
    `);
    console.log('Created!\n');

    // -------------------------------------------------------------------------
    // MANAGING CRON JOBS
    // -------------------------------------------------------------------------

    console.log('=== Managing Cron Jobs ===\n');

    // Disable a cron job
    console.log('Disabling archive_old_logs cron...');
    await db.query(`ALTER CRON archive_old_logs SET enabled = FALSE`);
    console.log('Disabled!\n');

    // Change the schedule
    console.log('Changing aggregate_stats schedule to every 30 minutes...');
    await db.query(`ALTER CRON aggregate_stats SET SCHEDULE '*/30 * * * *'`);
    console.log('Updated!\n');

    // Re-enable the cron job
    console.log('Re-enabling archive_old_logs cron...');
    await db.query(`ALTER CRON archive_old_logs SET enabled = TRUE`);
    console.log('Enabled!\n');

    // -------------------------------------------------------------------------
    // VIEWING SCHEDULED TASKS
    // -------------------------------------------------------------------------

    console.log('=== Viewing Scheduled Tasks ===\n');

    // List all pending one-time schedules
    console.log('Pending one-time schedules:');
    const schedules = await db.query<ScheduleRecord>(`
      SELECT id, sql, run_at, status, attempts, created_at
      FROM vibesql_schedules
      WHERE status = 'pending'
      ORDER BY run_at
    `);
    for (const schedule of schedules) {
      console.log(`  - [${schedule.id}] runs at ${schedule.run_at}`);
      console.log(`    SQL: ${schedule.sql.substring(0, 50)}...`);
    }
    console.log();

    // List all cron jobs
    console.log('Configured cron jobs:');
    const crons = await db.query<CronRecord>(`
      SELECT name, schedule, enabled, next_run
      FROM vibesql_crons
      ORDER BY name
    `);
    for (const cron of crons) {
      const status = cron.enabled ? 'enabled' : 'disabled';
      console.log(`  - ${cron.name} (${status})`);
      console.log(`    Schedule: ${cron.schedule}`);
      console.log(`    Next run: ${cron.next_run || 'N/A'}`);
    }
    console.log();

    // View recent execution history
    console.log('Recent execution history:');
    const history = await db.query<ExecutionHistory>(`
      SELECT cron_name, schedule_id, started_at, status, rows_affected
      FROM vibesql_execution_history
      ORDER BY started_at DESC
      LIMIT 5
    `);
    for (const entry of history) {
      const source = entry.cron_name || entry.schedule_id;
      console.log(`  - ${source}: ${entry.status} (${entry.rows_affected} rows)`);
    }
    console.log();

    // -------------------------------------------------------------------------
    // CLEANUP
    // -------------------------------------------------------------------------

    console.log('=== Cleanup ===\n');

    // Drop the cron jobs we created
    console.log('Dropping example cron jobs...');
    await db.query(`DROP CRON IF EXISTS cleanup_sessions`);
    await db.query(`DROP CRON IF EXISTS aggregate_stats`);
    await db.query(`DROP CRON IF EXISTS archive_old_logs`);
    console.log('Cleaned up!\n');

  } catch (error) {
    console.error('Error:', error instanceof Error ? error.message : error);
  } finally {
    console.log('Closing connection...');
    await db.close();
  }
}

main().catch(console.error);
