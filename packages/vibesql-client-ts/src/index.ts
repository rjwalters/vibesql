/**
 * VibeSql Client - Main Entry Point
 */

// Core client
export { VibeSqlClient } from './client';

// Types
export type {
  VibeSqlClientOptions,
  TlsOptions,
  SubscriptionCallbacks,
  UpdateDelta,
  Subscription,
} from './types/index';

// Errors
export {
  VibeSqlError,
  ConnectionError,
  QueryError,
  SubscriptionError,
} from './types/index';

// Protocol types (for advanced users)
export type {
  BackendMessage,
  FrontendMessage,
  QueryRow,
  ColumnDescription,
  SubscriptionDataMessage,
  SubscriptionErrorMessage,
} from './protocol/messages';
