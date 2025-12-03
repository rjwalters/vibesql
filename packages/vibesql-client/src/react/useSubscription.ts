import { useEffect, useState, useContext, useRef } from 'react';
import { SubscriptionUpdate, SubscriptionError } from '../types.js';
import { Subscription } from '../subscription.js';
import { VibeSQLContext } from './provider.js';

interface UseSubscriptionResult {
  data: any[];
  error: SubscriptionError | null;
  loading: boolean;
  subscription: Subscription | null;
}

/**
 * Hook for subscribing to real-time query updates
 * 
 * @example
 * ```tsx
 * function UserList() {
 *   const { data, error, loading } = useSubscription(
 *     'SELECT * FROM users WHERE active = $1',
 *     [true]
 *   );
 *   
 *   if (loading) return <div>Loading...</div>;
 *   if (error) return <div>Error: {error.message}</div>;
 *   return (
 *     <ul>
 *       {data.map(user => (
 *         <li key={user.id}>{user.name}</li>
 *       ))}
 *     </ul>
 *   );
 * }
 * ```
 */
export function useSubscription(
  sql: string,
  params?: any[]
): UseSubscriptionResult {
  const client = useContext(VibeSQLContext);
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<SubscriptionError | null>(null);
  const subscriptionRef = useRef<Subscription | null>(null);

  useEffect(() => {
    if (!client) {
      setError({
        subscriptionId: '',
        code: 'NO_CLIENT',
        message: 'VibeSQL client not available',
        timestamp: Date.now(),
      });
      setLoading(false);
      return;
    }

    try {
      const subscription = client.subscribe(sql, params);
      subscriptionRef.current = subscription;
      setLoading(true);
      setError(null);

      subscription.onData((update: SubscriptionUpdate) => {
        if (update.operation === 'full-sync') {
          setData(update.rows);
        } else if (update.operation === 'insert') {
          setData((prev) => [...prev, ...update.rows]);
        } else if (update.operation === 'delete') {
          // Remove rows matching the deleted rows
          const deletedIds = new Set(update.rows.map((r) => r.id));
          setData((prev) => prev.filter((r) => !deletedIds.has(r.id)));
        } else if (update.operation === 'update') {
          setData((prev) =>
            prev.map((row) => {
              const updated = update.rows.find((r) => r.id === row.id);
              return updated ? { ...row, ...updated } : row;
            })
          );
        }

        setLoading(false);
      });

      subscription.onError((err: SubscriptionError) => {
        setError(err);
        setLoading(false);
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
      setError({
        subscriptionId: '',
        code: 'SUBSCRIBE_ERROR',
        message: err instanceof Error ? err.message : String(err),
        timestamp: Date.now(),
      });
      setLoading(false);
    }
  }, [client, sql, JSON.stringify(params)]);

  return {
    data,
    error,
    loading,
    subscription: subscriptionRef.current,
  };
}
