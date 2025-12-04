/**
 * Transaction support for VibeSQL Drizzle adapter
 *
 * VibeSQL supports transactions through BEGIN/COMMIT/ROLLBACK statements.
 * This module provides helpers for transaction management with Drizzle.
 */

import type { SqliteRemoteDatabase } from 'drizzle-orm/sqlite-proxy';
import type { VibeSQLClient } from './types.js';

/**
 * Transaction options
 */
export interface TransactionOptions {
  /**
   * Transaction mode (SQLite-specific)
   * - DEFERRED: Lock acquired on first read/write
   * - IMMEDIATE: Write lock acquired immediately
   * - EXCLUSIVE: Exclusive lock, no other connections can read
   */
  mode?: 'DEFERRED' | 'IMMEDIATE' | 'EXCLUSIVE';
}

/**
 * Transaction callback function type
 */
export type TransactionCallback<T> = (tx: TransactionContext) => Promise<T>;

/**
 * Transaction context providing access to the underlying client for raw queries
 */
export interface TransactionContext {
  /**
   * Execute a raw SQL query within the transaction
   */
  query(sql: string, params?: unknown[]): Promise<{
    columns: string[];
    rows: unknown[];
    rowsAffected?: number;
  }>;
}

/**
 * Execute a callback within a database transaction.
 *
 * The transaction will be automatically committed if the callback succeeds,
 * or rolled back if it throws an error.
 *
 * @example
 * ```typescript
 * import { createDrizzle, withTransaction } from '@vibesql/drizzle';
 *
 * const vibesql = new VibeSQL();
 * await vibesql.connect();
 *
 * await withTransaction(vibesql, async (tx) => {
 *   await tx.query('INSERT INTO users (name) VALUES (?)', ['Alice']);
 *   await tx.query('INSERT INTO accounts (user_id) VALUES (last_insert_rowid())');
 * });
 * ```
 *
 * @param client - VibeSQL client instance
 * @param callback - Transaction callback function
 * @param options - Transaction options
 * @returns Result of the callback function
 */
export async function withTransaction<T>(
  client: VibeSQLClient,
  callback: TransactionCallback<T>,
  options?: TransactionOptions
): Promise<T> {
  const mode = options?.mode ?? 'DEFERRED';

  // Begin transaction
  await client.query(`BEGIN ${mode} TRANSACTION`);

  const context: TransactionContext = {
    query: (sql, params) => client.query(sql, params),
  };

  try {
    const result = await callback(context);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  }
}

/**
 * Create a transaction wrapper for the Drizzle database instance.
 *
 * This allows executing Drizzle queries within a transaction context.
 * Note: This is a simpler approach than Drizzle's built-in transactions
 * since sqlite-proxy doesn't support nested sessions.
 *
 * @example
 * ```typescript
 * import { createDrizzle, createTransactionHelper } from '@vibesql/drizzle';
 *
 * const vibesql = new VibeSQL();
 * const db = createDrizzle(vibesql);
 * const transaction = createTransactionHelper(vibesql);
 *
 * await transaction(async () => {
 *   await db.insert(users).values({ name: 'Alice' });
 *   await db.insert(accounts).values({ userId: 1, balance: 0 });
 * });
 * ```
 *
 * @param client - VibeSQL client instance
 * @returns Transaction helper function
 */
export function createTransactionHelper(client: VibeSQLClient) {
  return async function <T>(
    callback: () => Promise<T>,
    options?: TransactionOptions
  ): Promise<T> {
    const mode = options?.mode ?? 'DEFERRED';

    await client.query(`BEGIN ${mode} TRANSACTION`);

    try {
      const result = await callback();
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  };
}
