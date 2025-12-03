import { EventEmitter } from 'events';
import {
  SubscriptionState,
  SubscriptionListeners,
  SubscriptionUpdate,
  SubscriptionError,
} from './types.js';

/**
 * Manages a single subscription to a query
 */
export class Subscription extends EventEmitter {
  private id: string;
  private sql: string;
  private params?: any[];
  private state: SubscriptionState = SubscriptionState.IDLE;
  private listeners: SubscriptionListeners = {};

  constructor(id: string, sql: string, params?: any[]) {
    super();
    this.id = id;
    this.sql = sql;
    this.params = params;
  }

  /**
   * Get subscription ID
   */
  getId(): string {
    return this.id;
  }

  /**
   * Get subscription SQL
   */
  getSql(): string {
    return this.sql;
  }

  /**
   * Get subscription parameters
   */
  getParams(): any[] | undefined {
    return this.params;
  }

  /**
   * Get current subscription state
   */
  getState(): SubscriptionState {
    return this.state;
  }

  /**
   * Attach data listener
   */
  onData(callback: (update: SubscriptionUpdate) => void): Subscription {
    this.listeners.data = callback;
    this.on('data', callback);
    return this;
  }

  /**
   * Attach error listener
   */
  onError(callback: (error: SubscriptionError) => void): Subscription {
    this.listeners.error = callback;
    this.on('error', callback);
    return this;
  }

  /**
   * Attach close listener
   */
  onClose(callback: () => void): Subscription {
    this.listeners.close = callback;
    this.on('close', callback);
    return this;
  }

  /**
   * Set multiple listeners at once
   */
  on(eventName: string | symbol, listener: (...args: any[]) => void): this {
    if (eventName === 'data') {
      this.listeners.data = listener;
    } else if (eventName === 'error') {
      this.listeners.error = listener;
    } else if (eventName === 'close') {
      this.listeners.close = listener;
    }
    return super.on(eventName, listener);
  }

  /**
   * Remove data listener
   */
  offData(): Subscription {
    if (this.listeners.data) {
      this.removeListener('data', this.listeners.data);
      this.listeners.data = undefined;
    }
    return this;
  }

  /**
   * Remove error listener
   */
  offError(): Subscription {
    if (this.listeners.error) {
      this.removeListener('error', this.listeners.error);
      this.listeners.error = undefined;
    }
    return this;
  }

  /**
   * Remove close listener
   */
  offClose(): Subscription {
    if (this.listeners.close) {
      this.removeListener('close', this.listeners.close);
      this.listeners.close = undefined;
    }
    return this;
  }

  /**
   * Unsubscribe from updates
   */
  unsubscribe(): void {
    this.setState(SubscriptionState.UNSUBSCRIBED);
    this.removeAllListeners();
    this.listeners = {};
  }

  /**
   * Internal: set state when subscribing starts
   */
  _setSubscribing(): void {
    this.setState(SubscriptionState.SUBSCRIBING);
  }

  /**
   * Internal: set state when subscription is active
   */
  _setActive(): void {
    this.setState(SubscriptionState.ACTIVE);
  }

  /**
   * Internal: set state on error
   */
  _setError(): void {
    this.setState(SubscriptionState.ERROR);
  }

  /**
   * Internal: emit data update
   */
  _emitData(update: SubscriptionUpdate): void {
    this.emit('data', update);
  }

  /**
   * Internal: emit error
   */
  _emitError(error: SubscriptionError): void {
    this.emit('error', error);
  }

  /**
   * Internal: emit close
   */
  _emitClose(): void {
    this.setState(SubscriptionState.UNSUBSCRIBED);
    this.emit('close');
  }

  /**
   * Set state and emit stateChange event
   */
  private setState(newState: SubscriptionState): void {
    if (this.state !== newState) {
      this.state = newState;
      this.emit('stateChange', newState);
    }
  }
}

/**
 * Manages all subscriptions
 */
export class SubscriptionManager {
  private subscriptions = new Map<string, Subscription>();

  /**
   * Create a new subscription
   */
  create(id: string, sql: string, params?: any[]): Subscription {
    const subscription = new Subscription(id, sql, params);
    this.subscriptions.set(id, subscription);
    return subscription;
  }

  /**
   * Get subscription by ID
   */
  get(id: string): Subscription | undefined {
    return this.subscriptions.get(id);
  }

  /**
   * Check if subscription exists
   */
  has(id: string): boolean {
    return this.subscriptions.has(id);
  }

  /**
   * Remove subscription
   */
  remove(id: string): boolean {
    return this.subscriptions.delete(id);
  }

  /**
   * Get all subscriptions
   */
  getAll(): Subscription[] {
    return Array.from(this.subscriptions.values());
  }

  /**
   * Get all active subscriptions
   */
  getActive(): Subscription[] {
    return Array.from(this.subscriptions.values()).filter(
      (sub) => sub.getState() === SubscriptionState.ACTIVE
    );
  }

  /**
   * Clear all subscriptions
   */
  clear(): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions.clear();
  }
}
