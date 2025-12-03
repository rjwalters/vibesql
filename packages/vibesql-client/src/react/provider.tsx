import React, { createContext, useEffect, useState, ReactNode } from 'react';
import { VibeSQL } from '../client.js';
import { VibeQLClientConfig } from '../types.js';

/**
 * React context for VibeSQL client
 */
export const VibeSQLContext = createContext<VibeSQL | null>(null);

interface VibeSQLProviderProps {
  config: VibeQLClientConfig;
  children: ReactNode;
}

/**
 * Provider component for VibeSQL client
 * 
 * @example
 * ```tsx
 * function App() {
 *   const config: VibeQLClientConfig = {
 *     host: 'localhost',
 *     port: 5432,
 *   };
 *
 *   return (
 *     <VibeSQLProvider config={config}>
 *       <YourComponents />
 *     </VibeSQLProvider>
 *   );
 * }
 * ```
 */
export function VibeSQLProvider({ config, children }: VibeSQLProviderProps) {
  const [client] = useState(() => new VibeSQL(config));
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    const connect = async () => {
      try {
        await client.connect();
        setIsConnected(true);
      } catch (err) {
        setError(err instanceof Error ? err : new Error(String(err)));
      }
    };

    connect();

    const handleStateChange = (state: any) => {
      setIsConnected(state === 'connected');
    };

    const handleError = (err: Error) => {
      setError(err);
    };

    client.on('stateChange', handleStateChange);
    client.on('error', handleError);

    return () => {
      client.removeListener('stateChange', handleStateChange);
      client.removeListener('error', handleError);
      client.disconnect().catch((err) => {
        console.error('Failed to disconnect:', err);
      });
    };
  }, [client]);

  return (
    <VibeSQLContext.Provider value={isConnected ? client : null}>
      {children}
    </VibeSQLContext.Provider>
  );
}
