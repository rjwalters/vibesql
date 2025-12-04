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
  StorageOptions,
} from './types/index';

// Errors
export {
  VibeSqlError,
  ConnectionError,
  QueryError,
  SubscriptionError,
} from './types/index';

// Storage types and errors
export type {
  StorageClient,
  StorageMetadata,
  UploadOptions,
} from './storage/index';

export {
  StorageError,
  BlobNotFoundError,
  UploadError,
  HttpStorageClient,
} from './storage/index';

// Protocol types (for advanced users)
export type {
  BackendMessage,
  FrontendMessage,
  QueryRow,
  ColumnDescription,
  SubscriptionDataMessage,
  SubscriptionErrorMessage,
} from './protocol/messages';
