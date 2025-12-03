/**
 * React Integration - Hooks for VibeSql Client
 */

import { useState, useEffect, useRef } from 'react';
import { VibeSqlClient } from '../client';
import { Subscription } from '../types/index';

/**
 * Hook result type
 */
export interface UseSubscriptionResult<T> {
  data: T[] | null;
  error: Error | null;
  isLoading: boolean;
}

/**
 * useSubscription hook
 * Subscribes to a query and returns real-time updates
 */
export function useSubscription<T = any>(
  client: VibeSqlClient,
  query: string,
  params: any[] = []
): UseSubscriptionResult<T> {
  const [data, setData] = useState<T[] | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const subscriptionRef = useRef<Subscription | null>(null);

  useEffect(() => {
    if (!client.connected) {
      setError(new Error('Client not connected'));
      setIsLoading(false);
      return;
    }

    let isMounted = true;

    try {
      subscriptionRef.current = client.subscribe<T>(query, params, {
        onData: rows => {
          if (isMounted) {
            setData(rows);
            setError(null);
            setIsLoading(false);
          }
        },

        onError: err => {
          if (isMounted) {
            setError(err);
            setIsLoading(false);
          }
        },
      });
    } catch (err) {
      if (isMounted) {
        setError(
          err instanceof Error
            ? err
            : new Error(String(err))
        );
        setIsLoading(false);
      }
    }

    return () => {
      isMounted = false;
      if (subscriptionRef.current) {
        subscriptionRef.current.unsubscribe();
        subscriptionRef.current = null;
      }
    };
  }, [client, query, JSON.stringify(params)]);

  return { data, error, isLoading };
}

/**
 * useQuery hook
 * Executes a one-time query
 */
export function useQuery<T = any>(
  client: VibeSqlClient,
  query: string,
  params: any[] = []
): UseSubscriptionResult<T> {
  const [data, setData] = useState<T[] | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    if (!client.connected) {
      setError(new Error('Client not connected'));
      setIsLoading(false);
      return;
    }

    let isMounted = true;

    (async () => {
      try {
        const result = await client.query<T>(query, params);
        if (isMounted) {
          setData(result);
          setError(null);
          setIsLoading(false);
        }
      } catch (err) {
        if (isMounted) {
          setError(
            err instanceof Error
              ? err
              : new Error(String(err))
          );
          setIsLoading(false);
        }
      }
    })();

    return () => {
      isMounted = false;
    };
  }, [client, query, JSON.stringify(params)]);

  return { data, error, isLoading };
}
