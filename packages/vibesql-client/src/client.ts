import { EventEmitter } from 'events';
import { randomUUID } from 'crypto';
import {
  VibeQLClientConfig,
  QueryResult,
  MessageType,
  SubscriptionUpdate,
  SubscriptionError,
} from './types.js';
import { Connection } from './connection.js';
import { ProtocolCodec } from './protocol.js';
import { Subscription, SubscriptionManager } from './subscription.js';

/**
 * Main VibeSQL client for executing queries and managing subscriptions
 */
export class VibeSQL extends EventEmitter {
  private connection: Connection;
  private subscriptionManager: SubscriptionManager;
  private pendingQueries = new Map<string, { resolve: Function; reject: Function }>();

  constructor(config: VibeQLClientConfig = {}) {
    super();
    this.connection = new Connection(config);
    this.subscriptionManager = new SubscriptionManager();

    this.setupConnectionHandlers();
  }

  /**
   * Connect to VibeSQL server
   */
  async connect(): Promise<void> {
    await this.connection.connect();
  }

  /**
   * Disconnect from server
   */
  async disconnect(): Promise<void> {
    this.subscriptionManager.clear();
    await this.connection.disconnect();
  }

  /**
   * Execute a one-time query
   */
  async query(sql: string, params?: any[]): Promise<QueryResult> {
    if (!this.connection.isConnected()) {
      throw new Error('Not connected to server');
    }

    const queryId = randomUUID();
    const frame = ProtocolCodec.encodeQuery(sql, params);

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingQueries.delete(queryId);
        reject(new Error('Query timeout'));
      }, 30000);

      this.pendingQueries.set(queryId, {
        resolve: (result: QueryResult) => {
          clearTimeout(timeout);
          resolve(result);
        },
        reject: (error: Error) => {
          clearTimeout(timeout);
          reject(error);
        },
      });

      try {
        this.connection.send(frame);
      } catch (error) {
        clearTimeout(timeout);
        this.pendingQueries.delete(queryId);
        reject(error);
      }
    });
  }

  /**
   * Subscribe to real-time query updates
   */
  subscribe(sql: string, params?: any[]): Subscription {
    if (!this.connection.isConnected()) {
      throw new Error('Not connected to server');
    }

    const subscriptionId = randomUUID();
    const subscription = this.subscriptionManager.create(
      subscriptionId,
      sql,
      params
    );

    subscription._setSubscribing();

    const frame = ProtocolCodec.encodeSubscribe(subscriptionId, sql, params);

    try {
      this.connection.send(frame);
      subscription._setActive();
    } catch (error) {
      subscription._setError();
      throw error;
    }

    return subscription;
  }

  /**
   * Unsubscribe from updates
   */
  async unsubscribe(subscription: Subscription): Promise<void> {
    const id = subscription.getId();

    if (!this.connection.isConnected()) {
      this.subscriptionManager.remove(id);
      subscription.unsubscribe();
      return;
    }

    const frame = ProtocolCodec.encodeUnsubscribe(id);

    return new Promise((resolve, reject) => {
      try {
        this.connection.send(frame);
        this.subscriptionManager.remove(id);
        subscription.unsubscribe();
        resolve();
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Get a subscription by ID
   */
  getSubscription(id: string): Subscription | undefined {
    return this.subscriptionManager.get(id);
  }

  /**
   * Get all active subscriptions
   */
  getSubscriptions(): Subscription[] {
    return this.subscriptionManager.getActive();
  }

  /**
   * Setup connection event handlers
   */
  private setupConnectionHandlers(): void {
    this.connection.on('frame', (frame: any) => {
      this.handleFrame(frame);
    });

    this.connection.on('stateChange', (state: any) => {
      this.emit('stateChange', state);

      // On reconnect, restore subscriptions
      if (state === 'connected') {
        this.restoreSubscriptions();
      }
    });

    this.connection.on('error', (error: Error) => {
      this.emit('error', error);
    });

    this.connection.on('maxReconnectAttemptsReached', () => {
      this.emit('maxReconnectAttemptsReached');
    });
  }

  /**
   * Handle incoming frames from server
   */
  private handleFrame(frame: any): void {
    const { type, payload } = frame;

    try {
      switch (type) {
        case MessageType.QUERY_RESULT:
          this.handleQueryResult(payload);
          break;

        case MessageType.SUBSCRIPTION_DATA:
          this.handleSubscriptionData(payload);
          break;

        case MessageType.SUBSCRIPTION_ERROR:
          this.handleSubscriptionError(payload);
          break;

        case MessageType.PONG:
          // Handle pong if needed
          break;

        case MessageType.ERROR:
          this.handleError(payload);
          break;

        default:
          console.warn(`Unknown message type: ${type}`);
      }
    } catch (error) {
      console.error('Error handling frame:', error);
    }
  }

  /**
   * Handle query result
   */
  private handleQueryResult(payload: Buffer): void {
    const result = ProtocolCodec.decodeQueryResult(payload);
    // TODO: Match result to pending query using ID from payload
    // For now, resolve first pending query
    const pending = this.pendingQueries.entries().next();
    if (!pending.done) {
      const [queryId, { resolve }] = pending.value;
      this.pendingQueries.delete(queryId);
      resolve(result);
    }
  }

  /**
   * Handle subscription data update
   */
  private handleSubscriptionData(payload: Buffer): void {
    const update = ProtocolCodec.decodeSubscriptionData(payload);
    const subscription = this.subscriptionManager.get(update.subscriptionId);

    if (subscription) {
      subscription._emitData(update);
      this.emit('subscriptionData', update);
    }
  }

  /**
   * Handle subscription error
   */
  private handleSubscriptionError(payload: Buffer): void {
    const error = ProtocolCodec.decodeSubscriptionError(payload);
    const subscription = this.subscriptionManager.get(error.subscriptionId);

    if (subscription) {
      subscription._setError();
      subscription._emitError(error);
      this.emit('subscriptionError', error);
    }
  }

  /**
   * Handle general error
   */
  private handleError(payload: Buffer): void {
    const errorMsg = payload.toString('utf-8');
    const error = new Error(errorMsg);
    this.emit('error', error);
  }

  /**
   * Restore subscriptions after reconnect
   */
  private restoreSubscriptions(): void {
    const subscriptions = this.subscriptionManager.getAll();

    for (const subscription of subscriptions) {
      try {
        const id = subscription.getId();
        const sql = subscription.getSql();
        const params = subscription.getParams();

        const frame = ProtocolCodec.encodeSubscribe(id, sql, params);
        this.connection.send(frame);

        this.emit('subscriptionRestored', id);
      } catch (error) {
        console.error('Failed to restore subscription:', error);
      }
    }
  }
}
