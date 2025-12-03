/**
 * PostgreSQL Wire Protocol Messages
 * Based on PostgreSQL 12+ protocol specification
 */

/**
 * Message type codes for PostgreSQL wire protocol
 */
export const MessageCodes = {
  // Frontend (client -> server)
  StartupMessage: 0,
  PasswordMessage: 'p'.charCodeAt(0),
  Query: 'Q'.charCodeAt(0),
  Terminate: 'X'.charCodeAt(0),

  // Backend (server -> client)
  AuthenticationOk: 'R'.charCodeAt(0),
  AuthenticationMD5Password: 'R'.charCodeAt(0),
  AuthenticationSASL: 'R'.charCodeAt(0),
  BackendKeyData: 'K'.charCodeAt(0),
  RowDescription: 'T'.charCodeAt(0),
  DataRow: 'D'.charCodeAt(0),
  CommandComplete: 'C'.charCodeAt(0),
  ReadyForQuery: 'Z'.charCodeAt(0),
  ErrorResponse: 'E'.charCodeAt(0),
  NoticeResponse: 'N'.charCodeAt(0),
  ParameterStatus: 'S'.charCodeAt(0),

  // VibeSql extensions for subscriptions
  Subscribe: 0xf0,
  Unsubscribe: 0xf1,
  SubscriptionData: 0xf2,
  SubscriptionError: 0xf3,
};

/**
 * Query result row
 */
export interface QueryRow {
  [key: string]: any;
}

/**
 * Column description from RowDescription message
 */
export interface ColumnDescription {
  name: string;
  tableOid: number;
  columnAttrNum: number;
  dataTypeOid: number;
  dataTypeSize: number;
  typeModifier: number;
  formatCode: 'text' | 'binary';
}

/**
 * Authentication type codes
 */
export const AuthenticationTypes = {
  OK: 0,
  KerberosV5: 2,
  CleartextPassword: 3,
  MD5Password: 5,
  SCMCredential: 6,
  GSS: 7,
  SSPI: 9,
  SASL: 10,
};

/**
 * Transaction status from ReadyForQuery
 */
export type TransactionStatus = 'idle' | 'in-transaction' | 'failed-transaction';

/**
 * Parse message (extended query protocol)
 */
export interface ParseMessage {
  type: 'Parse';
  name: string; // prepared statement name (empty for unnamed)
  query: string;
  parameterOids?: number[];
}

/**
 * Bind message (extended query protocol)
 */
export interface BindMessage {
  type: 'Bind';
  portalName: string;
  statementName: string;
  parameterFormatCodes?: number[];
  parameters?: (Buffer | null)[];
  resultFormatCodes?: number[];
}

/**
 * Execute message (extended query protocol)
 */
export interface ExecuteMessage {
  type: 'Execute';
  portalName: string;
  maxRows?: number;
}

/**
 * Flush message (extended query protocol)
 */
export interface FlushMessage {
  type: 'Flush';
}

/**
 * Sync message (extended query protocol)
 */
export interface SyncMessage {
  type: 'Sync';
}

/**
 * Subscribe message (VibeSql extension)
 */
export interface SubscribeMessage {
  type: 'Subscribe';
  subscriptionId: Buffer;
  query: string;
  params: (Buffer | null)[];
}

/**
 * Unsubscribe message (VibeSql extension)
 */
export interface UnsubscribeMessage {
  type: 'Unsubscribe';
  subscriptionId: Buffer;
}

/**
 * Subscription data message (VibeSql extension)
 */
export interface SubscriptionDataMessage {
  type: 'SubscriptionData';
  subscriptionId: Buffer;
  updateType: 'full' | 'delta_insert' | 'delta_update' | 'delta_delete';
  rows: QueryRow[];
  columns?: ColumnDescription[];
}

/**
 * Subscription error message (VibeSql extension)
 */
export interface SubscriptionErrorMessage {
  type: 'SubscriptionError';
  subscriptionId: Buffer;
  error: string;
}

/**
 * Data row message
 */
export interface DataRowMessage {
  type: 'DataRow';
  columns: (Buffer | null)[];
}

/**
 * Row description message
 */
export interface RowDescriptionMessage {
  type: 'RowDescription';
  columns: ColumnDescription[];
}

/**
 * Command complete message
 */
export interface CommandCompleteMessage {
  type: 'CommandComplete';
  command: string;
}

/**
 * Ready for query message
 */
export interface ReadyForQueryMessage {
  type: 'ReadyForQuery';
  status: TransactionStatus;
}

/**
 * Error response message
 */
export interface ErrorResponseMessage {
  type: 'ErrorResponse';
  fields: Map<string, string>;
}

/**
 * Authentication ok message
 */
export interface AuthenticationOkMessage {
  type: 'AuthenticationOk';
}

/**
 * Backend key data message
 */
export interface BackendKeyDataMessage {
  type: 'BackendKeyData';
  processId: number;
  secretKey: number;
}

/**
 * Parameter status message
 */
export interface ParameterStatusMessage {
  type: 'ParameterStatus';
  parameterName: string;
  parameterValue: string;
}

/**
 * All possible backend messages
 */
export type BackendMessage =
  | AuthenticationOkMessage
  | BackendKeyDataMessage
  | ParameterStatusMessage
  | RowDescriptionMessage
  | DataRowMessage
  | CommandCompleteMessage
  | ReadyForQueryMessage
  | ErrorResponseMessage
  | SubscriptionDataMessage
  | SubscriptionErrorMessage;

/**
 * All possible frontend messages
 */
export type FrontendMessage =
  | ParseMessage
  | BindMessage
  | ExecuteMessage
  | FlushMessage
  | SyncMessage
  | SubscribeMessage
  | UnsubscribeMessage;
