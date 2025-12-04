/**
 * VibeSql Client
 * Main entry point for the client library
 */

import {
  VibeSqlClientOptions,
  SubscriptionCallbacks,
  Subscription,
  ConnectionError,
} from './types/index';
import { Connection } from './connection';
import { SubscriptionManager } from './subscription/manager';
import { HttpStorageClient, type StorageClient } from './storage/index';

/**
 * Main VibeSql client class
 */
export class VibeSqlClient {
  private connection: Connection;
  private subscriptionManager: SubscriptionManager;
  private isConnected = false;
  private reconnectAttempts = 0;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private _storage: StorageClient;

  /**
   * Storage client for blob operations
   *
   * @example
   * ```typescript
   * // Upload a file
   * const file = new File(['hello'], 'hello.txt', { type: 'text/plain' });
   * const storageId = await db.storage.upload(file);
   *
   * // Get URL
   * const url = db.storage.getUrl(storageId);
   *
   * // Download
   * const blob = await db.storage.download(storageId);
   *
   * // Get metadata
   * const meta = await db.storage.getMetadata(storageId);
   *
   * // Delete
   * await db.storage.delete(storageId);
   * ```
   */
  get storage(): StorageClient {
    return this._storage;
  }

  constructor(private options: VibeSqlClientOptions) {
    this.connection = new Connection(options);
    this.subscriptionManager = new SubscriptionManager(this.connection);

    // Initialize storage client with base URL from options
    const storageBaseUrl = this.buildStorageBaseUrl();
    this._storage = new HttpStorageClient({
      baseUrl: storageBaseUrl,
      headers: this.options.storage?.headers,
    });

    // Setup reconnection handler
    if (this.options.reconnect?.enabled !== false) {
      this.connection.on('close', () => {
        this.isConnected = false;
        this.handleDisconnect();
      });
    }
  }

  /**
   * Connect to the database
   */
  async connect(): Promise<void> {
    if (this.isConnected) {
      return;
    }

    try {
      await this.connection.connect();
      this.isConnected = true;
      this.reconnectAttempts = 0;

      // Start subscription listener
      this.subscriptionManager.start();
    } catch (error) {
      this.isConnected = false;
      throw new ConnectionError(
        `Failed to connect: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Execute a query
   */
  async query<T = any>(sql: string, params?: any[]): Promise<T[]> {
    if (!this.isConnected) {
      throw new ConnectionError('Not connected');
    }

    try {
      return await this.connection.query<T>(sql, params);
    } catch (error) {
      throw error;
    }
  }

  /**
   * Subscribe to a query for real-time updates
   */
  subscribe<T = any>(
    sql: string,
    params: any[],
    callbacks: SubscriptionCallbacks<T>
  ): Subscription {
    if (!this.isConnected) {
      throw new ConnectionError('Not connected');
    }

    return this.subscriptionManager.subscribe<T>(sql, params, callbacks);
  }

  /**
   * Close the connection
   */
  async close(): Promise<void> {
    this.subscriptionManager.stop();

    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    await this.connection.close();
    this.isConnected = false;
  }

  /**
   * Check if connected
   */
  get connected(): boolean {
    return this.isConnected;
  }

  /**
   * Handle disconnection with automatic reconnection
   */
  private async handleDisconnect(): Promise<void> {
    const {
      maxRetries = 10,
      baseDelay = 1000,
      maxDelay = 30000,
    } = this.options.reconnect || {};

    while (this.reconnectAttempts < maxRetries) {
      const delay = Math.min(
        baseDelay * Math.pow(2, this.reconnectAttempts),
        maxDelay
      );

      console.log(
        `Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts + 1}/${maxRetries})...`
      );

      await this.sleep(delay);

      try {
        await this.connect();
        console.log('Reconnected successfully');

        // Restore subscriptions
        await this.subscriptionManager.restoreSubscriptions();
        return;
      } catch (error) {
        this.reconnectAttempts += 1;
        console.warn(
          `Reconnection attempt ${this.reconnectAttempts} failed:`,
          error instanceof Error ? error.message : String(error)
        );
      }
    }

    console.error(`Failed to reconnect after ${maxRetries} attempts`);
  }

  /**
   * Sleep for a given duration
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => {
      this.reconnectTimeout = setTimeout(resolve, ms);
    });
  }

  /**
   * Build the base URL for storage API from client options
   */
  private buildStorageBaseUrl(): string {
    if (this.options.storage?.baseUrl) {
      return this.options.storage.baseUrl;
    }

    // Build URL from connection options
    const protocol = this.options.ssl ? 'https' : 'http';
    const host = this.options.host;
    const port = this.options.httpPort ?? 8080;

    return `${protocol}://${host}:${port}`;
  }
}
