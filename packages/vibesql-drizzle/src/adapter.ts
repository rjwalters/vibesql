/**
 * VibeSQL adapter for Drizzle ORM using sqlite-proxy driver
 */

import { drizzle } from 'drizzle-orm/sqlite-proxy';
import type { SqliteRemoteDatabase, SqliteRemoteResult } from 'drizzle-orm/sqlite-proxy';
import type {
  VibeSQLClient,
  DrizzleAdapterOptions,
  ProxyQueryMethod,
  ProxyQueryResult,
  BatchQueryItem,
} from './types.js';

/**
 * Convert VibeSQL row objects to sqlite-proxy array format.
 * Drizzle sqlite-proxy expects rows as arrays of values.
 */
function rowsToArrays(columns: string[], rows: unknown[]): unknown[][] {
  return rows.map((row) => {
    if (Array.isArray(row)) {
      return row;
    }
    const rowObj = row as Record<string, unknown>;
    return columns.map((col) => rowObj[col]);
  });
}

/**
 * Create a Drizzle database instance connected to VibeSQL.
 *
 * Uses Drizzle's sqlite-proxy driver which allows custom query execution
 * backends. This bridges VibeSQL's query interface to Drizzle ORM.
 *
 * @example
 * ```typescript
 * import { VibeSQL } from '@vibesql/client';
 * import { createDrizzle } from '@vibesql/drizzle';
 * import { sqliteTable, text, integer } from 'drizzle-orm/sqlite-core';
 *
 * // Define schema
 * const users = sqliteTable('users', {
 *   id: integer('id').primaryKey({ autoIncrement: true }),
 *   name: text('name').notNull(),
 *   email: text('email').notNull(),
 * });
 *
 * // Create client
 * const vibesql = new VibeSQL();
 * await vibesql.connect();
 *
 * // Create Drizzle instance
 * const db = createDrizzle(vibesql);
 *
 * // Type-safe queries
 * const allUsers = await db.select().from(users);
 * ```
 *
 * @param client - VibeSQL client instance
 * @param options - Optional configuration
 * @returns Drizzle database instance
 */
export function createDrizzle<TSchema extends Record<string, unknown> = Record<string, never>>(
  client: VibeSQLClient,
  options?: DrizzleAdapterOptions
): SqliteRemoteDatabase<TSchema> {
  const logger = options?.logger;

  // Create the query callback for sqlite-proxy
  const queryCallback = async (
    sql: string,
    params: unknown[],
    method: ProxyQueryMethod
  ): Promise<ProxyQueryResult> => {
    // Log query if logger is enabled
    if (logger) {
      if (typeof logger === 'object' && logger.logQuery) {
        logger.logQuery(sql, params);
      } else if (logger === true) {
        console.log('[Drizzle Query]', sql, params);
      }
    }

    // Execute query via VibeSQL
    const result = await client.query(sql, params);

    // For write operations, return empty rows
    if (method === 'run') {
      return { rows: [] };
    }

    // Convert row objects to arrays
    const arrayRows = rowsToArrays(result.columns, result.rows);

    // For 'get', we only return first row (but still as array of arrays)
    if (method === 'get') {
      return { rows: arrayRows.length > 0 ? [arrayRows[0]] : [] };
    }

    return { rows: arrayRows };
  };

  // Create batch callback if provided or use default sequential execution
  const batchCallback = options?.batchCallback ?? (async (queries: BatchQueryItem[]) => {
    const results: ProxyQueryResult[] = [];
    for (const query of queries) {
      const result = await queryCallback(query.sql, query.params, query.method);
      results.push(result);
    }
    return results;
  });

  // Create and return Drizzle instance with sqlite-proxy driver
  return drizzle<TSchema>(queryCallback, batchCallback, {
    logger: false, // We handle logging ourselves
  });
}

/**
 * Get the underlying VibeSQL client from a Drizzle instance.
 *
 * Note: This is only available if you used createDrizzle to create the instance.
 */
export function getClient(db: SqliteRemoteDatabase): VibeSQLClient | undefined {
  // Store reference on the db object for retrieval
  return (db as unknown as { _vibesqlClient?: VibeSQLClient })._vibesqlClient;
}
