/**
 * VibeSQL TypeScript SDK
 * Real-time reactive query subscriptions
 */

export { VibeSQL } from './client.js';
export { Connection } from './connection.js';
export { Subscription, SubscriptionManager } from './subscription.js';
export { ProtocolCodec } from './protocol.js';

export type {
  MessageType,
  QueryResult,
  SubscriptionUpdate,
  SubscriptionError,
  VibeQLClientConfig,
  ConnectionState,
  SubscriptionState,
  SubscriptionListeners,
  ProtocolFrame,
} from './types.js';

export {
  MessageType,
  ConnectionState,
  SubscriptionState,
} from './types.js';
