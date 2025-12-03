/**
 * Protocol Message Parser
 * Parses PostgreSQL wire protocol messages from buffers
 */

import {
  MessageCodes,
  BackendMessage,
  ColumnDescription,
  QueryRow,
  TransactionStatus,
  RowDescriptionMessage,
  DataRowMessage,
  CommandCompleteMessage,
  ReadyForQueryMessage,
  ErrorResponseMessage,
  AuthenticationOkMessage,
  BackendKeyDataMessage,
  ParameterStatusMessage,
  SubscriptionDataMessage,
  SubscriptionErrorMessage,
} from './messages';

/**
 * Message parser state machine
 */
export class MessageParser {
  private buffer: Buffer = Buffer.alloc(0);
  private messages: BackendMessage[] = [];

  /**
   * Add data to the buffer
   */
  addData(data: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, data]);
    this.parseMessages();
  }

  /**
   * Get parsed messages
   */
  getMessages(): BackendMessage[] {
    const messages = this.messages;
    this.messages = [];
    return messages;
  }

  /**
   * Parse complete messages from buffer
   */
  private parseMessages(): void {
    while (this.buffer.length >= 5) {
      const messageType = this.buffer[0];
      const length = this.buffer.readUInt32BE(1);

      // Check if we have the complete message
      if (this.buffer.length < 1 + length) {
        break;
      }

      const messageData = this.buffer.slice(1, 1 + length);
      this.buffer = this.buffer.slice(1 + length);

      try {
        const message = this.parseMessage(messageType, messageData);
        if (message) {
          this.messages.push(message);
        }
      } catch (error) {
        console.error('Error parsing message:', error);
      }
    }
  }

  /**
   * Parse a single message
   */
  private parseMessage(type: number, data: Buffer): BackendMessage | null {
    switch (type) {
      case MessageCodes.AuthenticationOk:
        return this.parseAuthenticationOk(data);
      case MessageCodes.BackendKeyData:
        return this.parseBackendKeyData(data);
      case MessageCodes.ParameterStatus:
        return this.parseParameterStatus(data);
      case MessageCodes.RowDescription:
        return this.parseRowDescription(data);
      case MessageCodes.DataRow:
        return this.parseDataRow(data);
      case MessageCodes.CommandComplete:
        return this.parseCommandComplete(data);
      case MessageCodes.ReadyForQuery:
        return this.parseReadyForQuery(data);
      case MessageCodes.ErrorResponse:
        return this.parseErrorResponse(data);
      case MessageCodes.SubscriptionData:
        return this.parseSubscriptionData(data);
      case MessageCodes.SubscriptionError:
        return this.parseSubscriptionError(data);
      default:
        console.warn(`Unknown message type: ${type}`);
        return null;
    }
  }

  /**
   * Parse authentication ok message
   */
  private parseAuthenticationOk(_data: Buffer): AuthenticationOkMessage {
    return { type: 'AuthenticationOk' };
  }

  /**
   * Parse backend key data message
   */
  private parseBackendKeyData(data: Buffer): BackendKeyDataMessage {
    return {
      type: 'BackendKeyData',
      processId: data.readUInt32BE(0),
      secretKey: data.readUInt32BE(4),
    };
  }

  /**
   * Parse parameter status message
   */
  private parseParameterStatus(data: Buffer): ParameterStatusMessage {
    let offset = 0;
    const parameterName = this.readCString(data, offset);
    offset += parameterName.length + 1;
    const parameterValue = this.readCString(data, offset);

    return {
      type: 'ParameterStatus',
      parameterName,
      parameterValue,
    };
  }

  /**
   * Parse row description message
   */
  private parseRowDescription(data: Buffer): RowDescriptionMessage {
    let offset = 0;
    const columnCount = data.readUInt16BE(offset);
    offset += 2;

    const columns: ColumnDescription[] = [];
    for (let i = 0; i < columnCount; i++) {
      const name = this.readCString(data, offset);
      offset += name.length + 1;

      const column: ColumnDescription = {
        name,
        tableOid: data.readUInt32BE(offset),
        columnAttrNum: data.readInt16BE(offset + 4),
        dataTypeOid: data.readUInt32BE(offset + 6),
        dataTypeSize: data.readInt16BE(offset + 10),
        typeModifier: data.readInt32BE(offset + 12),
        formatCode: data.readUInt16BE(offset + 16) === 0 ? 'text' : 'binary',
      };
      offset += 18;
      columns.push(column);
    }

    return {
      type: 'RowDescription',
      columns,
    };
  }

  /**
   * Parse data row message
   */
  private parseDataRow(data: Buffer): DataRowMessage {
    let offset = 0;
    const columnCount = data.readUInt16BE(offset);
    offset += 2;

    const columns: (Buffer | null)[] = [];
    for (let i = 0; i < columnCount; i++) {
      const length = data.readInt32BE(offset);
      offset += 4;

      if (length === -1) {
        columns.push(null);
      } else {
        columns.push(data.slice(offset, offset + length));
        offset += length;
      }
    }

    return {
      type: 'DataRow',
      columns,
    };
  }

  /**
   * Parse command complete message
   */
  private parseCommandComplete(data: Buffer): CommandCompleteMessage {
    const command = this.readCString(data, 0);
    return {
      type: 'CommandComplete',
      command,
    };
  }

  /**
   * Parse ready for query message
   */
  private parseReadyForQuery(data: Buffer): ReadyForQueryMessage {
    const statusChar = String.fromCharCode(data[0]);
    let status: TransactionStatus = 'idle';
    if (statusChar === 'T') {
      status = 'in-transaction';
    } else if (statusChar === 'E') {
      status = 'failed-transaction';
    }

    return {
      type: 'ReadyForQuery',
      status,
    };
  }

  /**
   * Parse error response message
   */
  private parseErrorResponse(data: Buffer): ErrorResponseMessage {
    const fields = new Map<string, string>();
    let offset = 0;

    while (offset < data.length) {
      const fieldType = String.fromCharCode(data[offset]);
      if (fieldType === '\0') {
        break;
      }
      offset += 1;

      const value = this.readCString(data, offset);
      fields.set(fieldType, value);
      offset += value.length + 1;
    }

    return {
      type: 'ErrorResponse',
      fields,
    };
  }

  /**
   * Parse subscription data message (VibeSql extension)
   */
  private parseSubscriptionData(data: Buffer): SubscriptionDataMessage {
    let offset = 0;

    const subscriptionId = data.slice(offset, offset + 16);
    offset += 16;

    const updateTypeCode = data.readUInt8(offset);
    offset += 1;

    let updateType: 'full' | 'delta_insert' | 'delta_update' | 'delta_delete' =
      'full';
    if (updateTypeCode === 1) {
      updateType = 'delta_insert';
    } else if (updateTypeCode === 2) {
      updateType = 'delta_update';
    } else if (updateTypeCode === 3) {
      updateType = 'delta_delete';
    }

    const rowCount = data.readUInt32BE(offset);
    offset += 4;

    const columnCount = data.readUInt16BE(offset);
    offset += 2;

    const columns: ColumnDescription[] = [];
    for (let i = 0; i < columnCount; i++) {
      const name = this.readCString(data, offset);
      offset += name.length + 1;

      const column: ColumnDescription = {
        name,
        tableOid: data.readUInt32BE(offset),
        columnAttrNum: data.readInt16BE(offset + 4),
        dataTypeOid: data.readUInt32BE(offset + 6),
        dataTypeSize: data.readInt16BE(offset + 10),
        typeModifier: data.readInt32BE(offset + 12),
        formatCode: data.readUInt16BE(offset + 16) === 0 ? 'text' : 'binary',
      };
      offset += 18;
      columns.push(column);
    }

    const rows: QueryRow[] = [];
    for (let i = 0; i < rowCount; i++) {
      const row: QueryRow = {};
      for (let j = 0; j < columnCount; j++) {
        const length = data.readInt32BE(offset);
        offset += 4;

        if (length === -1) {
          row[columns[j].name] = null;
        } else {
          const value = data
            .slice(offset, offset + length)
            .toString('utf8');
          row[columns[j].name] = this.parseColumnValue(
            value,
            columns[j].dataTypeOid
          );
          offset += length;
        }
      }
      rows.push(row);
    }

    return {
      type: 'SubscriptionData',
      subscriptionId,
      updateType,
      rows,
      columns,
    };
  }

  /**
   * Parse subscription error message (VibeSql extension)
   */
  private parseSubscriptionError(data: Buffer): SubscriptionErrorMessage {
    const subscriptionId = data.slice(0, 16);
    const error = this.readCString(data, 16);

    return {
      type: 'SubscriptionError',
      subscriptionId,
      error,
    };
  }

  /**
   * Read a null-terminated string from buffer at offset
   */
  private readCString(data: Buffer, offset: number): string {
    let end = offset;
    while (end < data.length && data[end] !== 0) {
      end += 1;
    }
    return data.slice(offset, end).toString('utf8');
  }

  /**
   * Parse column value based on data type OID
   * Note: This is simplified. A production implementation would handle all PostgreSQL types
   */
  private parseColumnValue(value: string, typeOid: number): any {
    // Common PostgreSQL type OIDs
    const INT4_OID = 23;
    const INT8_OID = 20;
    const FLOAT8_OID = 701;
    const BOOLEAN_OID = 16;
    const TEXT_OID = 25;
    const VARCHAR_OID = 1043;
    const TIMESTAMP_OID = 1114;
    const TIMESTAMPTZ_OID = 1184;

    switch (typeOid) {
      case INT4_OID:
      case INT8_OID:
        return parseInt(value, 10);
      case FLOAT8_OID:
        return parseFloat(value);
      case BOOLEAN_OID:
        return value === 't';
      case TIMESTAMP_OID:
      case TIMESTAMPTZ_OID:
        return new Date(value);
      case TEXT_OID:
      case VARCHAR_OID:
      default:
        return value;
    }
  }
}
