/**
 * Subscription helpers for bridging Drizzle queries to VibeSQL subscriptions
 *
 * This module provides utilities to convert Drizzle queries into SQL strings
 * that can be used with VibeSQL's real-time subscription system.
 */

/**
 * VibeSQL subscription update data (mirrors @vibesql/client type)
 */
export interface SubscriptionUpdate {
  subscriptionId: string;
  columns: string[];
  rows: unknown[];
  operation: 'insert' | 'update' | 'delete' | 'full-sync';
  timestamp: number;
}

/**
 * VibeSQL subscription error (mirrors @vibesql/client type)
 */
export interface SubscriptionError {
  subscriptionId: string;
  code: string;
  message: string;
  timestamp: number;
}

/**
 * VibeSQL subscription interface (mirrors @vibesql/client type)
 */
export interface Subscription {
  onData(callback: (update: SubscriptionUpdate) => void): void;
  onError(callback: (error: SubscriptionError) => void): void;
  getId(): string;
  getSql(): string;
  getParams(): unknown[] | undefined;
}

/**
 * VibeSQL client interface for subscription methods
 */
export interface VibeSQLSubscriptionClient {
  subscribe(sql: string, params?: unknown[]): Subscription;
  unsubscribe(subscription: Subscription): Promise<void>;
}

/**
 * Extract SQL string and parameters from a Drizzle query.
 *
 * This allows you to build type-safe queries with Drizzle and then
 * subscribe to them using VibeSQL's subscription system.
 *
 * @example
 * ```typescript
 * import { createDrizzle, extractQuery } from '@vibesql/drizzle';
 * import { eq } from 'drizzle-orm';
 *
 * const db = createDrizzle(vibesql);
 * const query = db.select().from(users).where(eq(users.active, true));
 * const { sql, params } = extractQuery(query);
 *
 * // Use with VibeSQL subscription
 * const subscription = vibesql.subscribe(sql, params);
 * ```
 *
 * @param query - Drizzle query object with toSQL method
 * @returns Object containing SQL string and parameters array
 */
export function extractQuery<T extends { toSQL(): { sql: string; params: unknown[] } }>(
  query: T
): { sql: string; params: unknown[] } {
  const compiled = query.toSQL();
  return {
    sql: compiled.sql,
    params: compiled.params,
  };
}

/**
 * Subscription configuration
 */
export interface DrizzleSubscriptionConfig<T> {
  /**
   * Callback when new data is received
   */
  onData?: (data: T[]) => void;

  /**
   * Callback when an error occurs
   */
  onError?: (error: Error) => void;

  /**
   * Transform function to convert raw rows to typed objects
   */
  transform?: (rows: unknown[]) => T[];
}

/**
 * Managed Drizzle subscription that wraps VibeSQL subscription
 */
export interface DrizzleSubscription<T> {
  /**
   * Current data from the subscription
   */
  data: T[];

  /**
   * Current loading state
   */
  loading: boolean;

  /**
   * Current error, if any
   */
  error: Error | null;

  /**
   * Underlying VibeSQL subscription
   */
  subscription: Subscription;

  /**
   * Unsubscribe and clean up
   */
  unsubscribe(): Promise<void>;
}

/**
 * Create a managed subscription from a Drizzle query.
 *
 * This provides a simpler API for subscribing to query results
 * with automatic data management.
 *
 * @example
 * ```typescript
 * import { createDrizzle, subscribeToDrizzleQuery } from '@vibesql/drizzle';
 *
 * const db = createDrizzle(vibesql);
 * const query = db.select().from(users).where(eq(users.active, true));
 *
 * const sub = subscribeToDrizzleQuery(vibesql, query, {
 *   onData: (users) => console.log('Active users:', users),
 *   onError: (err) => console.error('Error:', err),
 * });
 *
 * // Later: unsubscribe
 * await sub.unsubscribe();
 * ```
 *
 * @param client - VibeSQL client instance
 * @param query - Drizzle query to subscribe to
 * @param config - Subscription configuration
 * @returns Managed subscription object
 */
export function subscribeToDrizzleQuery<T>(
  client: VibeSQLSubscriptionClient,
  query: { toSQL(): { sql: string; params: unknown[] } },
  config?: DrizzleSubscriptionConfig<T>
): DrizzleSubscription<T> {
  const { sql, params } = extractQuery(query);

  let data: T[] = [];
  let loading = true;
  let error: Error | null = null;

  const subscription = client.subscribe(sql, params);

  subscription.onData((update: SubscriptionUpdate) => {
    loading = false;

    // Transform rows if transform function provided
    const transformedRows = config?.transform
      ? config.transform(update.rows)
      : (update.rows as T[]);

    if (update.operation === 'full-sync') {
      data = transformedRows;
    } else if (update.operation === 'insert') {
      data = [...data, ...transformedRows];
    } else if (update.operation === 'delete') {
      // Assume rows have an 'id' field for deletion matching
      const deletedIds = new Set(
        transformedRows.map((r) => (r as Record<string, unknown>).id)
      );
      data = data.filter((r) => !deletedIds.has((r as Record<string, unknown>).id));
    } else if (update.operation === 'update') {
      data = data.map((row) => {
        const updated = transformedRows.find(
          (r) => (r as Record<string, unknown>).id === (row as Record<string, unknown>).id
        );
        return updated ? { ...row, ...updated } : row;
      });
    }

    config?.onData?.(data);
  });

  subscription.onError((err) => {
    loading = false;
    error = new Error(err.message);
    config?.onError?.(error);
  });

  return {
    get data() {
      return data;
    },
    get loading() {
      return loading;
    },
    get error() {
      return error;
    },
    subscription,
    async unsubscribe() {
      await client.unsubscribe(subscription);
    },
  };
}

/**
 * Type helper for inferring the result type of a Drizzle select query.
 *
 * @example
 * ```typescript
 * const query = db.select().from(users);
 * type User = DrizzleQueryResult<typeof query>;
 * // User is now typed as the row type from the query
 * ```
 */
export type DrizzleQueryResult<T extends { _: { result: unknown } }> = T['_']['result'];
