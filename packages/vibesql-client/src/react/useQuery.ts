import { useEffect, useState, useContext } from 'react';
import { QueryResult } from '../types.js';
import { VibeSQLContext } from './provider.js';

/**
 * Hook for executing one-time queries
 * 
 * @example
 * ```tsx
 * function UserList() {
 *   const { data, error, loading } = useQuery('SELECT * FROM users');
 *   
 *   if (loading) return <div>Loading...</div>;
 *   if (error) return <div>Error: {error.message}</div>;
 *   return <pre>{JSON.stringify(data, null, 2)}</pre>;
 * }
 * ```
 */
export function useQuery(sql: string, params?: any[]) {
  const client = useContext(VibeSQLContext);
  const [data, setData] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!client) {
      setError(new Error('VibeSQL client not available'));
      setLoading(false);
      return;
    }

    let isMounted = true;

    const executeQuery = async () => {
      try {
        setLoading(true);
        setError(null);
        const result = await client.query(sql, params);
        if (isMounted) {
          setData(result);
        }
      } catch (err) {
        if (isMounted) {
          setError(err instanceof Error ? err : new Error(String(err)));
        }
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    };

    executeQuery();

    return () => {
      isMounted = false;
    };
  }, [client, sql, JSON.stringify(params)]);

  return { data, loading, error };
}
