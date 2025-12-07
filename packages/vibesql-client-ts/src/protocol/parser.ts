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
  SubscriptionPartialDataMessage,
  PartialRow,
} from './messages';
import { parseColumnValue, PgTypeOid } from './typeConversion';

// Re-export for backwards compatibility
export { parseColumnValue, PgTypeOid };
// Legacy alias for backwards compatibility with existing tests
export const TYPE_OIDS = PgTypeOid;

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
      case MessageCodes.SubscriptionPartialData:
        return this.parseSubscriptionPartialData(data);
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
          row[columns[j].name] = parseColumnValue(
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
   * Parse subscription partial data message (VibeSql extension)
   * Wire format:
   *   subscription_id: [u8; 16]
   *   update_type: u8 (always 4 for SelectiveUpdate)
   *   row_count: i32
   *   For each row:
   *     total_columns: u16
   *     bitmap: [u8; ceil(total_columns/8)]
   *     For each present column (bit=1):
   *       value_len: i32 (-1 for NULL, else byte count)
   *       value: [u8; value_len] (if len >= 0)
   */
  private parseSubscriptionPartialData(
    data: Buffer
  ): SubscriptionPartialDataMessage {
    // Skip the 4-byte length field included in the data buffer
    let offset = 4;

    const subscriptionId = data.slice(offset, offset + 16);
    offset += 16;

    // Skip update_type (always 4 for SelectiveUpdate)
    offset += 1;

    const rowCount = data.readInt32BE(offset);
    offset += 4;

    const rows: PartialRow[] = [];

    for (let i = 0; i < rowCount; i++) {
      const totalColumns = data.readUInt16BE(offset);
      offset += 2;

      // Calculate bitmap size (ceil(totalColumns / 8))
      const bitmapSize = Math.ceil(totalColumns / 8);
      const bitmap = data.slice(offset, offset + bitmapSize);
      offset += bitmapSize;

      // Parse which columns are present based on bitmap
      const presentColumns: number[] = [];
      for (let col = 0; col < totalColumns; col++) {
        const byteIndex = Math.floor(col / 8);
        const bitIndex = 7 - (col % 8); // MSB first
        if ((bitmap[byteIndex] & (1 << bitIndex)) !== 0) {
          presentColumns.push(col);
        }
      }

      // Parse values for present columns
      const values: (any | null)[] = [];
      for (const _colIndex of presentColumns) {
        const valueLen = data.readInt32BE(offset);
        offset += 4;

        if (valueLen === -1) {
          values.push(null);
        } else {
          const value = data.slice(offset, offset + valueLen).toString('utf8');
          values.push(value);
          offset += valueLen;
        }
      }

      rows.push({
        totalColumns,
        presentColumns,
        values,
      });
    }

    return {
      type: 'SubscriptionPartialData',
      subscriptionId,
      rows,
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
}
