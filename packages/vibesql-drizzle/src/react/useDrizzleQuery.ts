import { useEffect, useState, useRef, useCallback, useMemo } from 'react';
import type {
  VibeSQLSubscriptionClient,
  Subscription,
  SubscriptionUpdate,
  SubscriptionError,
} from '../subscription.js';

/**
 * Result type for useDrizzleQuery hook
 */
export interface UseDrizzleQueryResult<T> {
  /** Current query data */
  data: T[];
  /** Loading state - true until first data arrives */
  loading: boolean;
  /** Error state, if any error occurred */
  error: Error | null;
  /** Underlying subscription instance */
  subscription: Subscription | null;
  /** Manually refetch the data */
  refetch: () => void;
}

/**
 * Options for useDrizzleQuery hook
 */
export interface UseDrizzleQueryOptions<T> {
  /**
   * Transform function to convert raw rows to typed objects.
   * If not provided, rows are returned as-is.
   */
  transform?: (rows: unknown[]) => T[];

  /**
   * Whether the query is enabled. Set to false to disable the subscription.
   * Useful for conditional queries.
   * @default true
   */
  enabled?: boolean;

  /**
   * Callback fired when new data is received
   */
  onData?: (data: T[]) => void;

  /**
   * Callback fired when an error occurs
   */
  onError?: (error: Error) => void;
}

/**
 * React hook for subscribing to Drizzle query results with real-time updates.
 *
 * This hook bridges Drizzle's type-safe queries with VibeSQL's real-time
 * subscription system, providing automatic updates when data changes.
 *
 * @example
 * ```tsx
 * import { useDrizzleQuery } from '@vibesql/drizzle/react';
 * import { eq } from 'drizzle-orm';
 *
 * function ActiveUsers() {
 *   const db = useDrizzleDb(); // Your Drizzle instance
 *   const query = db.select().from(users).where(eq(users.active, true));
 *
 *   const { data, loading, error } = useDrizzleQuery(vibesql, query);
 *
 *   if (loading) return <div>Loading...</div>;
 *   if (error) return <div>Error: {error.message}</div>;
 *
 *   return (
 *     <ul>
 *       {data.map((user) => (
 *         <li key={user.id}>{user.name}</li>
 *       ))}
 *     </ul>
 *   );
 * }
 * ```
 *
 * @param client - VibeSQL client instance
 * @param query - Drizzle query with toSQL() method
 * @param options - Hook options
 * @returns Query result with data, loading, and error states
 */
export function useDrizzleQuery<T>(
  client: VibeSQLSubscriptionClient | null,
  query: { toSQL(): { sql: string; params: unknown[] } },
  options?: UseDrizzleQueryOptions<T>
): UseDrizzleQueryResult<T> {
  const { transform, enabled = true, onData, onError } = options ?? {};

  // Extract SQL from query - memoize to avoid unnecessary resubscriptions
  const { sql, params } = useMemo(() => query.toSQL(), [query]);
  const paramsKey = JSON.stringify(params);

  const [data, setData] = useState<T[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  const [refetchTrigger, setRefetchTrigger] = useState(0);
  const subscriptionRef = useRef<Subscription | null>(null);

  const refetch = useCallback(() => {
    setRefetchTrigger((prev) => prev + 1);
  }, []);

  useEffect(() => {
    if (!client || !enabled) {
      setLoading(false);
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const subscription = client.subscribe(sql, params);
      subscriptionRef.current = subscription;

      subscription.onData((update: SubscriptionUpdate) => {
        // Transform rows if transform function provided
        const transformedRows = transform
          ? transform(update.rows)
          : (update.rows as T[]);

        setLoading(false);

        setData((prev) => {
          let newData: T[];

          switch (update.operation) {
            case 'full-sync':
              newData = transformedRows;
              break;

            case 'insert':
              newData = [...prev, ...transformedRows];
              break;

            case 'delete': {
              // Assume rows have an 'id' field for deletion matching
              const deletedIds = new Set(
                transformedRows.map((r) => (r as Record<string, unknown>).id)
              );
              newData = prev.filter(
                (r) => !deletedIds.has((r as Record<string, unknown>).id)
              );
              break;
            }

            case 'update': {
              newData = prev.map((row) => {
                const updated = transformedRows.find(
                  (r) =>
                    (r as Record<string, unknown>).id ===
                    (row as Record<string, unknown>).id
                );
                return updated ? { ...row, ...updated } : row;
              });
              break;
            }

            default:
              newData = prev;
          }

          // Call onData callback
          onData?.(newData);
          return newData;
        });
      });

      subscription.onError((err: SubscriptionError) => {
        const subscriptionError = new Error(err.message);
        setError(subscriptionError);
        setLoading(false);
        onError?.(subscriptionError);
      });

      return () => {
        if (subscriptionRef.current) {
          client.unsubscribe(subscriptionRef.current).catch((err) => {
            console.error('Failed to unsubscribe:', err);
          });
          subscriptionRef.current = null;
        }
      };
    } catch (err) {
      const subscriptionError =
        err instanceof Error ? err : new Error(String(err));
      setError(subscriptionError);
      setLoading(false);
      onError?.(subscriptionError);
    }
  }, [client, sql, paramsKey, enabled, transform, onData, onError, refetchTrigger]);

  return {
    data,
    loading,
    error,
    subscription: subscriptionRef.current,
    refetch,
  };
}
