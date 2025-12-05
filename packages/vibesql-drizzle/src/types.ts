/**
 * Types for the VibeSQL Drizzle adapter
 */

/**
 * Result row from sqlite-proxy query
 */
export interface ProxyRow {
  [key: string]: unknown;
}

/**
 * Query result for sqlite-proxy interface
 */
export interface ProxyQueryResult {
  rows: unknown[][];
}

/**
 * Query method types for sqlite-proxy
 * - 'run': INSERT/UPDATE/DELETE (returns empty rows)
 * - 'all': SELECT returning all rows
 * - 'get': SELECT returning first row
 * - 'values': SELECT returning raw arrays
 */
export type ProxyQueryMethod = 'run' | 'all' | 'values' | 'get';

/**
 * Remote callback signature for sqlite-proxy
 */
export type RemoteCallback = (
  sql: string,
  params: unknown[],
  method: ProxyQueryMethod
) => Promise<ProxyQueryResult>;

/**
 * Batch query item for batch operations
 */
export interface BatchQueryItem {
  sql: string;
  params: unknown[];
  method: ProxyQueryMethod;
}

/**
 * Batch callback signature for sqlite-proxy batch operations
 */
export type AsyncBatchRemoteCallback = (
  queries: BatchQueryItem[]
) => Promise<ProxyQueryResult[]>;

/**
 * Configuration options for the Drizzle adapter
 */
export interface DrizzleAdapterOptions {
  /**
   * Custom batch callback for optimized batch operations.
   * If not provided, batch operations will execute queries sequentially.
   */
  batchCallback?: AsyncBatchRemoteCallback;

  /**
   * Logger function for debugging queries
   */
  logger?: boolean | {
    logQuery: (query: string, params: unknown[]) => void;
  };
}

/**
 * VibeSQL client interface (subset needed for adapter)
 */
export interface VibeSQLClient {
  query(sql: string, params?: unknown[]): Promise<{
    columns: string[];
    rows: unknown[];
    rowsAffected?: number;
  }>;
}
