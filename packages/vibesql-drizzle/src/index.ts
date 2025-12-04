/**
 * @vibesql/drizzle - Drizzle ORM adapter for VibeSQL
 *
 * This package provides a Drizzle ORM adapter using the sqlite-proxy driver,
 * enabling type-safe queries against VibeSQL with real-time subscription support.
 *
 * @example
 * ```typescript
 * import { VibeSQL } from '@vibesql/client';
 * import { createDrizzle } from '@vibesql/drizzle';
 * import { sqliteTable, text, integer } from 'drizzle-orm/sqlite-core';
 *
 * // Define schema (standard Drizzle)
 * const users = sqliteTable('users', {
 *   id: integer('id').primaryKey({ autoIncrement: true }),
 *   name: text('name').notNull(),
 *   email: text('email').notNull(),
 * });
 *
 * // Create VibeSQL client and Drizzle instance
 * const vibesql = new VibeSQL();
 * await vibesql.connect();
 * const db = createDrizzle(vibesql);
 *
 * // Type-safe queries
 * const allUsers = await db.select().from(users);
 *
 * // Mutations
 * await db.insert(users).values({ name: 'Alice', email: 'alice@example.com' });
 * ```
 *
 * @packageDocumentation
 */

// Main adapter
export { createDrizzle, getClient } from './adapter.js';

// Types
export type {
  VibeSQLClient,
  DrizzleAdapterOptions,
  ProxyQueryMethod,
  ProxyQueryResult,
  BatchQueryItem,
  AsyncBatchRemoteCallback,
  RemoteCallback,
} from './types.js';

// Transaction support
export {
  withTransaction,
  createTransactionHelper,
} from './transaction.js';
export type {
  TransactionOptions,
  TransactionCallback,
  TransactionContext,
} from './transaction.js';

// Subscription helpers
export {
  extractQuery,
  subscribeToDrizzleQuery,
} from './subscription.js';
export type {
  DrizzleSubscriptionConfig,
  DrizzleSubscription,
  DrizzleQueryResult,
  VibeSQLSubscriptionClient,
  Subscription,
  SubscriptionUpdate,
  SubscriptionError,
} from './subscription.js';
