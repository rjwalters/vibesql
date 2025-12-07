/**
 * Low-Level Connection Handling
 * Manages TCP connections and protocol communication
 */

import { EventEmitter } from 'events';
import { VibeSqlClientOptions, QueryError, ConnectionError } from './types/index';
import { MessageParser } from './protocol/parser';
import {
  encodeStartupMessage,
  encodePasswordMessage,
  encodeQuery,
  encodeTerminate,
} from './protocol/encoder';
import {
  BackendMessage,
  ReadyForQueryMessage,
  ErrorResponseMessage,
  DataRowMessage,
  RowDescriptionMessage,
  CommandCompleteMessage,
  QueryRow,
  ColumnDescription,
} from './protocol/messages';

/**
 * Query result set
 */
export interface QueryResult {
  columns: ColumnDescription[];
  rows: QueryRow[];
}

/**
 * Internal connection state
 */
export class Connection extends EventEmitter {
  private socket: any; // Would be net.Socket in Node.js
  private parser: MessageParser;
  private isConnected = false;
  private isAuthenticated = false;
  private currentQuery: QueryResult | null = null;
  private queryPromise: Promise<void> | null = null;
  private queryResolve: (() => void) | null = null;
  private queryReject: ((error: Error) => void) | null = null;
  private readyForQuery = false;

  constructor(
    private options: VibeSqlClientOptions,
    socket?: any
  ) {
    super();
    this.parser = new MessageParser();
    this.socket = socket;
  }

  /**
   * Check if connection is established
   */
  get connected(): boolean {
    return this.isConnected && this.isAuthenticated;
  }

  /**
   * Connect to the database
   */
  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }

    try {
      // In Node.js, we would use net.Socket
      // For this implementation, we'll provide a template
      if (!this.socket) {
        this.socket = this.createSocket();
      }

      this.setupSocketListeners();

      // Send startup message
      const startupMsg = encodeStartupMessage(
        this.options.database,
        this.options.user
      );
      this.socket.write(startupMsg);

      // Wait for authentication
      await this.waitForAuthentication();
      this.isConnected = true;
    } catch (error) {
      this.isConnected = false;
      this.isAuthenticated = false;
      throw new ConnectionError(
        `Failed to connect: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Execute a query
   */
  async query<T = any>(sql: string, params?: any[]): Promise<T[]> {
    if (!this.connected) {
      throw new ConnectionError('Not connected');
    }

    try {
      // Simple text protocol (not extended protocol for now)
      // In production, would use extended protocol with parameters
      const query = params ? this.bindParameters(sql, params) : sql;

      this.currentQuery = null;
      const queryMsg = encodeQuery(query);
      this.socket.write(queryMsg);

      await this.waitForQueryCompletion();

      if (!this.currentQuery) {
        return [];
      }

      const queryResult = this.currentQuery as QueryResult;  // Type assertion after null check
      return queryResult.rows.map((row: QueryRow) =>
        this.parseRow<T>(row, queryResult.columns)
      );
    } catch (error) {
      throw new QueryError(
        `Query failed: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  /**
   * Close the connection
   */
  async close(): Promise<void> {
    if (this.socket) {
      try {
        const terminateMsg = encodeTerminate();
        this.socket.write(terminateMsg);
      } catch (error) {
        // Ignore errors during close
      }

      if (this.socket.destroy) {
        this.socket.destroy();
      }
    }

    this.isConnected = false;
    this.isAuthenticated = false;
  }

  /**
   * Create a socket (to be implemented based on environment)
   */
  private createSocket(): any {
    // This would be implemented differently for Node.js vs browser
    throw new Error('Socket creation not implemented. Override in subclass.');
  }

  /**
   * Setup socket event listeners
   */
  private setupSocketListeners(): void {
    this.socket.on('data', (data: Buffer) => {
      this.parser.addData(data);
      this.processMessages();
    });

    this.socket.on('error', (error: Error) => {
      this.emit('error', error);
      this.isConnected = false;
      this.isAuthenticated = false;
    });

    this.socket.on('close', () => {
      this.emit('close');
      this.isConnected = false;
      this.isAuthenticated = false;
    });
  }

  /**
   * Process parsed messages
   */
  private processMessages(): void {
    const messages = this.parser.getMessages();

    for (const message of messages) {
      switch (message.type) {
        case 'AuthenticationOk':
          this.isAuthenticated = true;
          this.emit('authenticated');
          break;

        case 'RowDescription':
          if (!this.currentQuery) {
            this.currentQuery = {
              columns: (message as RowDescriptionMessage).columns,
              rows: [],
            };
          }
          break;

        case 'DataRow':
          if (this.currentQuery) {
            const row: QueryRow = {};
            const dataMsg = message as DataRowMessage;
            for (let i = 0; i < this.currentQuery.columns.length; i++) {
              const column = this.currentQuery.columns[i];
              const value = dataMsg.columns[i];
              row[column.name] =
                value === null ? null : value.toString('utf8');
            }
            this.currentQuery.rows.push(row);
          }
          break;

        case 'CommandComplete':
          // Command completed
          break;

        case 'ReadyForQuery':
          this.readyForQuery = true;
          if (this.queryResolve) {
            this.queryResolve();
            this.queryResolve = null;
            this.queryReject = null;
          }
          break;

        case 'ErrorResponse':
          const errorMsg = message as ErrorResponseMessage;
          const errorText = errorMsg.fields.get('M') || 'Unknown error';
          if (this.queryReject) {
            this.queryReject(new QueryError(errorText));
            this.queryReject = null;
            this.queryResolve = null;
          } else {
            this.emit('error', new QueryError(errorText));
          }
          break;

        default:
          // Handle other message types
          break;
      }
    }
  }

  /**
   * Wait for authentication to complete
   */
  private waitForAuthentication(): Promise<void> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new ConnectionError('Authentication timeout'));
      }, 5000);

      this.once('authenticated', () => {
        clearTimeout(timeout);
        resolve();
      });

      this.once('error', error => {
        clearTimeout(timeout);
        reject(error);
      });
    });
  }

  /**
   * Wait for query to complete
   */
  private waitForQueryCompletion(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.readyForQuery = false;

      this.queryResolve = () => {
        this.queryResolve = null;
        this.queryReject = null;
        resolve();
      };

      this.queryReject = (error: Error) => {
        this.queryResolve = null;
        this.queryReject = null;
        reject(error);
      };

      const timeout = setTimeout(() => {
        this.queryResolve = null;
        this.queryReject = null;
        reject(new QueryError('Query timeout'));
      }, 30000);

      this.queryResolve = () => {
        clearTimeout(timeout);
        if (this.queryResolve) {
          this.queryResolve();
        }
      };
    });
  }

  /**
   * Bind parameters to query (simple implementation)
   */
  private bindParameters(sql: string, params: any[]): string {
    let query = sql;
    for (let i = 0; i < params.length; i++) {
      const value = params[i];
      let paramValue: string;

      if (value === null) {
        paramValue = 'NULL';
      } else if (typeof value === 'string') {
        paramValue = `'${value.replace(/'/g, "''")}'`;
      } else if (typeof value === 'number') {
        paramValue = String(value);
      } else if (typeof value === 'boolean') {
        paramValue = value ? 'TRUE' : 'FALSE';
      } else if (value instanceof Date) {
        paramValue = `'${value.toISOString()}'`;
      } else {
        paramValue = `'${String(value).replace(/'/g, "''")}'`;
      }

      query = query.replace(`$${i + 1}`, paramValue);
    }
    return query;
  }

  /**
   * Parse a row into a typed object
   */
  private parseRow<T>(row: QueryRow, columns: ColumnDescription[]): T {
    const result: any = {};

    for (const column of columns) {
      const value = row[column.name];

      if (value === null) {
        result[column.name] = null;
      } else if (value === undefined) {
        result[column.name] = undefined;
      } else {
        // Simple type parsing
        result[column.name] = this.parseValue(value, column.dataTypeOid);
      }
    }

    return result as T;
  }

  /**
   * Parse a value based on its type OID
   */
  private parseValue(value: any, typeOid: number): any {
    const INT4_OID = 23;
    const INT8_OID = 20;
    const FLOAT8_OID = 701;
    const BOOLEAN_OID = 16;
    const TIMESTAMP_OID = 1114;
    const TIMESTAMPTZ_OID = 1184;

    switch (typeOid) {
      case INT4_OID:
      case INT8_OID:
        return parseInt(String(value), 10);
      case FLOAT8_OID:
        return parseFloat(String(value));
      case BOOLEAN_OID:
        return String(value) === 't';
      case TIMESTAMP_OID:
      case TIMESTAMPTZ_OID:
        return new Date(value);
      default:
        return value;
    }
  }
}
