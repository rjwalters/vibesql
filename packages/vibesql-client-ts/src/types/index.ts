/**
 * VibeSql Client - Public Type Definitions
 */

/**
 * Storage configuration options
 */
export interface StorageOptions {
  /**
   * Base URL for the storage HTTP API
   * If not set, derived from host and httpPort
   */
  baseUrl?: string;

  /**
   * Custom headers to include with storage requests
   */
  headers?: Record<string, string>;
}

/**
 * Client configuration options
 */
export interface VibeSqlClientOptions {
  // Connection settings
  host: string;
  port?: number;
  database: string;
  user: string;
  password?: string;

  // HTTP API port (for storage and REST API)
  httpPort?: number;

  // Connection pool settings
  pool?: {
    min?: number;
    max?: number;
    idleTimeout?: number;
  };

  // Reconnection settings
  reconnect?: {
    enabled?: boolean;
    maxRetries?: number;
    baseDelay?: number;
    maxDelay?: number;
  };

  // TLS settings
  ssl?: boolean | TlsOptions;

  // Storage settings
  storage?: StorageOptions;
}

/**
 * TLS configuration options
 */
export interface TlsOptions {
  rejectUnauthorized?: boolean;
  ca?: string | Buffer;
  cert?: string | Buffer;
  key?: string | Buffer;
}

/**
 * Subscription callbacks
 */
export interface SubscriptionCallbacks<T> {
  onData: (rows: T[]) => void;
  onDelta?: (delta: UpdateDelta<T>) => void;
  onError?: (error: Error) => void;
}

/**
 * Subscription update delta
 */
export type UpdateDelta<T> =
  | { type: 'insert'; row: T }
  | { type: 'delete'; row: T }
  | { type: 'update'; oldRow: T; newRow: T };

/**
 * Subscription handle
 */
export interface Subscription {
  unsubscribe(): void;
}

/**
 * Error types
 */
export class VibeSqlError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'VibeSqlError';
  }
}

export class ConnectionError extends VibeSqlError {
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionError';
  }
}

export class QueryError extends VibeSqlError {
  constructor(message: string) {
    super(message);
    this.name = 'QueryError';
  }
}

export class SubscriptionError extends VibeSqlError {
  constructor(message: string) {
    super(message);
    this.name = 'SubscriptionError';
  }
}
