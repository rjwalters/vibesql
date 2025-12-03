/**
 * Wire protocol message types
 */
export enum MessageType {
  // Client → Server
  QUERY = 0x00,
  SUBSCRIBE = 0x01,
  UNSUBSCRIBE = 0x02,
  PING = 0x03,

  // Server → Client
  QUERY_RESULT = 0x10,
  SUBSCRIPTION_DATA = 0x11,
  SUBSCRIPTION_ERROR = 0x12,
  PONG = 0x13,
  ERROR = 0x14,
}

/**
 * Query execution result
 */
export interface QueryResult {
  columns: string[];
  rows: any[];
  rowsAffected?: number;
}

/**
 * Subscription update data
 */
export interface SubscriptionUpdate {
  subscriptionId: string;
  columns: string[];
  rows: any[];
  operation: 'insert' | 'update' | 'delete' | 'full-sync';
  timestamp: number;
}

/**
 * Subscription error details
 */
export interface SubscriptionError {
  subscriptionId: string;
  code: string;
  message: string;
  timestamp: number;
}

/**
 * Client configuration
 */
export interface VibeQLClientConfig {
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  password?: string;
  ssl?: boolean;
  reconnectInterval?: number;
  maxReconnectAttempts?: number;
  queryTimeout?: number;
}

/**
 * Connection state
 */
export enum ConnectionState {
  DISCONNECTED = 'disconnected',
  CONNECTING = 'connecting',
  CONNECTED = 'connected',
  RECONNECTING = 'reconnecting',
  ERROR = 'error',
}

/**
 * Subscription state
 */
export enum SubscriptionState {
  IDLE = 'idle',
  SUBSCRIBING = 'subscribing',
  ACTIVE = 'active',
  ERROR = 'error',
  UNSUBSCRIBED = 'unsubscribed',
}

/**
 * Subscription event listeners
 */
export interface SubscriptionListeners {
  data?: (update: SubscriptionUpdate) => void;
  error?: (error: SubscriptionError) => void;
  close?: () => void;
}

/**
 * Wire protocol frame
 */
export interface ProtocolFrame {
  type: MessageType;
  payload: Buffer;
}
