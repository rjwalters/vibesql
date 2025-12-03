/**
 * Subscription Manager
 * Manages real-time query subscriptions
 */

import { randomUUID } from 'crypto';
import {
  SubscriptionCallbacks,
  UpdateDelta,
  Subscription,
  SubscriptionError,
} from '../types/index';
import { Connection } from '../connection';
import { encodeSubscribe, encodeUnsubscribe } from '../protocol/encoder';
import {
  SubscriptionDataMessage,
  SubscriptionErrorMessage,
  QueryRow,
} from '../protocol/messages';

/**
 * Active subscription
 */
interface ActiveSubscription<T> {
  id: Buffer;
  query: string;
  params: any[];
  callbacks: SubscriptionCallbacks<T>;
  currentRows?: T[];
  previousRows?: T[];
}

/**
 * Subscription Manager
 */
export class SubscriptionManager {
  private subscriptions = new Map<string, ActiveSubscription<any>>();
  private isRunning = false;

  constructor(private connection: Connection) {
    // Listen for subscription messages from connection
    this.connection.on('subscriptionData', (msg: SubscriptionDataMessage) => {
      this.handleSubscriptionData(msg);
    });

    this.connection.on('subscriptionError', (msg: SubscriptionErrorMessage) => {
      this.handleSubscriptionError(msg);
    });
  }

  /**
   * Start the subscription listener
   */
  start(): void {
    this.isRunning = true;
  }

  /**
   * Stop the subscription listener
   */
  stop(): void {
    this.isRunning = false;

    // Unsubscribe from all active subscriptions
    for (const [, subscription] of this.subscriptions) {
      try {
        const unsubscribeMsg = encodeUnsubscribe(subscription.id);
        // Send to connection
      } catch (error) {
        // Ignore errors during shutdown
      }
    }

    this.subscriptions.clear();
  }

  /**
   * Subscribe to a query
   */
  subscribe<T = any>(
    query: string,
    params: any[],
    callbacks: SubscriptionCallbacks<T>
  ): Subscription {
    if (!this.isRunning) {
      throw new SubscriptionError('Subscription manager not running');
    }

    const subscriptionId = Buffer.from(randomUUID().replace(/-/g, ''), 'hex');
    const subscriptionKey = subscriptionId.toString('hex');

    const subscription: ActiveSubscription<T> = {
      id: subscriptionId,
      query,
      params,
      callbacks,
      currentRows: [],
      previousRows: [],
    };

    this.subscriptions.set(subscriptionKey, subscription);

    // Send subscribe message
    try {
      const subscribeMsg = encodeSubscribe(subscriptionId, query, params);
      // Send to connection
    } catch (error) {
      this.subscriptions.delete(subscriptionKey);
      throw new SubscriptionError(
        `Failed to subscribe: ${error instanceof Error ? error.message : String(error)}`
      );
    }

    // Return subscription handle
    return {
      unsubscribe: () => {
        this.unsubscribe(subscriptionKey);
      },
    };
  }

  /**
   * Unsubscribe from a subscription
   */
  private unsubscribe(subscriptionKey: string): void {
    const subscription = this.subscriptions.get(subscriptionKey);
    if (!subscription) {
      return;
    }

    try {
      const unsubscribeMsg = encodeUnsubscribe(subscription.id);
      // Send to connection
    } catch (error) {
      // Ignore errors
    }

    this.subscriptions.delete(subscriptionKey);
  }

  /**
   * Restore subscriptions after reconnect
   */
  async restoreSubscriptions(): Promise<void> {
    const subscriptionList = Array.from(this.subscriptions.values());

    for (const subscription of subscriptionList) {
      try {
        const subscribeMsg = encodeSubscribe(
          subscription.id,
          subscription.query,
          subscription.params
        );
        // Send to connection
      } catch (error) {
        // Log error but continue with other subscriptions
        console.error('Failed to restore subscription:', error);
      }
    }
  }

  /**
   * Handle subscription data message
   */
  private handleSubscriptionData(msg: SubscriptionDataMessage): void {
    const subscriptionKey = msg.subscriptionId.toString('hex');
    const subscription = this.subscriptions.get(subscriptionKey);

    if (!subscription) {
      console.warn(
        `Received data for unknown subscription: ${subscriptionKey}`
      );
      return;
    }

    try {
      // Parse rows
      const newRows = msg.rows as any[];

      if (msg.updateType === 'full') {
        // Full result set
        subscription.currentRows = newRows;
        subscription.callbacks.onData(newRows);
      } else if (msg.updateType === 'delta_insert') {
        // Incremental insert
        if (!subscription.currentRows) {
          subscription.currentRows = [];
        }
        subscription.currentRows.push(...newRows);

        if (subscription.callbacks.onDelta) {
          for (const row of newRows) {
            subscription.callbacks.onDelta({
              type: 'insert',
              row,
            });
          }
        }

        subscription.callbacks.onData(subscription.currentRows);
      } else if (msg.updateType === 'delta_delete') {
        // Incremental delete
        if (subscription.currentRows && newRows.length > 0) {
          const rowToDelete = newRows[0];
          const index = this.findRowIndex(
            subscription.currentRows,
            rowToDelete
          );
          if (index >= 0) {
            const removed = subscription.currentRows.splice(index, 1);

            if (subscription.callbacks.onDelta) {
              subscription.callbacks.onDelta({
                type: 'delete',
                row: removed[0],
              });
            }
          }
        }

        subscription.callbacks.onData(subscription.currentRows || []);
      } else if (msg.updateType === 'delta_update') {
        // Incremental update
        if (subscription.currentRows && newRows.length >= 2) {
          const oldRow = newRows[0];
          const newRow = newRows[1];
          const index = this.findRowIndex(subscription.currentRows, oldRow);

          if (index >= 0) {
            subscription.currentRows[index] = newRow;

            if (subscription.callbacks.onDelta) {
              subscription.callbacks.onDelta({
                type: 'update',
                oldRow,
                newRow,
              });
            }
          }
        }

        subscription.callbacks.onData(subscription.currentRows || []);
      }
    } catch (error) {
      const errorMsg =
        error instanceof Error ? error.message : String(error);
      if (subscription.callbacks.onError) {
        subscription.callbacks.onError(new SubscriptionError(errorMsg));
      }
    }
  }

  /**
   * Handle subscription error message
   */
  private handleSubscriptionError(msg: SubscriptionErrorMessage): void {
    const subscriptionKey = msg.subscriptionId.toString('hex');
    const subscription = this.subscriptions.get(subscriptionKey);

    if (!subscription) {
      return;
    }

    if (subscription.callbacks.onError) {
      subscription.callbacks.onError(new SubscriptionError(msg.error));
    }
  }

  /**
   * Find row index in current rows
   * Simple comparison - in production would use primary key
   */
  private findRowIndex(rows: any[], targetRow: any): number {
    for (let i = 0; i < rows.length; i++) {
      if (this.rowsEqual(rows[i], targetRow)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Compare two rows for equality
   */
  private rowsEqual(row1: any, row2: any): boolean {
    const keys1 = Object.keys(row1);
    const keys2 = Object.keys(row2);

    if (keys1.length !== keys2.length) {
      return false;
    }

    for (const key of keys1) {
      if (row1[key] !== row2[key]) {
        return false;
      }
    }

    return true;
  }
}
