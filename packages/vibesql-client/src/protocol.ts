import {
  MessageType,
  QueryResult,
  SubscriptionUpdate,
  SubscriptionError,
  ProtocolFrame,
} from './types.js';

/**
 * Wire protocol encoding/decoding utilities
 * 
 * Frame format:
 * [1 byte: type] [4 bytes: payload length] [N bytes: payload]
 */

const FRAME_HEADER_SIZE = 5;

export class ProtocolCodec {
  /**
   * Encode a query message
   */
  static encodeQuery(sql: string, params?: any[]): Buffer {
    const payload = Buffer.alloc(1 + 4 + Buffer.byteLength(sql) + 4);
    let offset = 0;

    // Write query
    const sqlBuf = Buffer.from(sql, 'utf-8');
    payload.writeUInt32BE(sqlBuf.length, offset);
    offset += 4;
    sqlBuf.copy(payload, offset);
    offset += sqlBuf.length;

    // Write params length
    const paramCount = params?.length ?? 0;
    payload.writeUInt32BE(paramCount, offset);

    return ProtocolCodec.wrapFrame(MessageType.QUERY, payload);
  }

  /**
   * Encode a subscribe message
   */
  static encodeSubscribe(
    subscriptionId: string,
    sql: string,
    params?: any[]
  ): Buffer {
    const idBuf = Buffer.from(subscriptionId, 'utf-8');
    const sqlBuf = Buffer.from(sql, 'utf-8');

    // Allocate buffer: 4 + idLen + 4 + sqlLen + 4
    const payload = Buffer.alloc(4 + idBuf.length + 4 + sqlBuf.length + 4);
    let offset = 0;

    // Write subscription ID
    payload.writeUInt32BE(idBuf.length, offset);
    offset += 4;
    idBuf.copy(payload, offset);
    offset += idBuf.length;

    // Write SQL
    payload.writeUInt32BE(sqlBuf.length, offset);
    offset += 4;
    sqlBuf.copy(payload, offset);
    offset += sqlBuf.length;

    // Write param count
    const paramCount = params?.length ?? 0;
    payload.writeUInt32BE(paramCount, offset);

    return ProtocolCodec.wrapFrame(MessageType.SUBSCRIBE, payload);
  }

  /**
   * Encode an unsubscribe message
   */
  static encodeUnsubscribe(subscriptionId: string): Buffer {
    const idBuf = Buffer.from(subscriptionId, 'utf-8');
    const payload = Buffer.alloc(4 + idBuf.length);
    payload.writeUInt32BE(idBuf.length, 0);
    idBuf.copy(payload, 4);

    return ProtocolCodec.wrapFrame(MessageType.UNSUBSCRIBE, payload);
  }

  /**
   * Encode a ping message
   */
  static encodePing(): Buffer {
    return ProtocolCodec.wrapFrame(MessageType.PING, Buffer.alloc(0));
  }

  /**
   * Wrap payload in frame with type and length
   */
  private static wrapFrame(type: MessageType, payload: Buffer): Buffer {
    const frame = Buffer.alloc(FRAME_HEADER_SIZE + payload.length);
    frame[0] = type;
    frame.writeUInt32BE(payload.length, 1);
    payload.copy(frame, FRAME_HEADER_SIZE);
    return frame;
  }

  /**
   * Decode a frame header and return frame info
   */
  static decodeFrameHeader(buffer: Buffer): ProtocolFrame | null {
    if (buffer.length < FRAME_HEADER_SIZE) {
      return null;
    }

    const type = buffer[0] as MessageType;
    const payloadLength = buffer.readUInt32BE(1);

    if (buffer.length < FRAME_HEADER_SIZE + payloadLength) {
      return null;
    }

    const payload = buffer.subarray(FRAME_HEADER_SIZE, FRAME_HEADER_SIZE + payloadLength);

    return { type, payload };
  }

  /**
   * Decode a query result message
   */
  static decodeQueryResult(payload: Buffer): QueryResult {
    let offset = 0;

    // Read columns
    const columnCount = payload.readUInt32BE(offset);
    offset += 4;

    const columns: string[] = [];
    for (let i = 0; i < columnCount; i++) {
      const colLen = payload.readUInt32BE(offset);
      offset += 4;
      const colName = payload.toString('utf-8', offset, offset + colLen);
      columns.push(colName);
      offset += colLen;
    }

    // Read rows
    const rowCount = payload.readUInt32BE(offset);
    offset += 4;

    const rows: any[] = [];
    for (let i = 0; i < rowCount; i++) {
      const row: any = {};
      for (const col of columns) {
        // Simplified: assume string values for now
        const valueLen = payload.readUInt32BE(offset);
        offset += 4;
        if (valueLen > 0) {
          row[col] = payload.toString('utf-8', offset, offset + valueLen);
          offset += valueLen;
        } else {
          row[col] = null;
        }
      }
      rows.push(row);
    }

    // Read rows affected (optional)
    let rowsAffected: number | undefined;
    if (offset < payload.length) {
      rowsAffected = payload.readUInt32BE(offset);
    }

    return { columns, rows, rowsAffected };
  }

  /**
   * Decode a subscription data message
   */
  static decodeSubscriptionData(payload: Buffer): SubscriptionUpdate {
    let offset = 0;

    // Read subscription ID
    const idLen = payload.readUInt32BE(offset);
    offset += 4;
    const subscriptionId = payload.toString('utf-8', offset, offset + idLen);
    offset += idLen;

    // Read columns
    const columnCount = payload.readUInt32BE(offset);
    offset += 4;

    const columns: string[] = [];
    for (let i = 0; i < columnCount; i++) {
      const colLen = payload.readUInt32BE(offset);
      offset += 4;
      const colName = payload.toString('utf-8', offset, offset + colLen);
      columns.push(colName);
      offset += colLen;
    }

    // Read rows
    const rowCount = payload.readUInt32BE(offset);
    offset += 4;

    const rows: any[] = [];
    for (let i = 0; i < rowCount; i++) {
      const row: any = {};
      for (const col of columns) {
        const valueLen = payload.readUInt32BE(offset);
        offset += 4;
        if (valueLen > 0) {
          row[col] = payload.toString('utf-8', offset, offset + valueLen);
          offset += valueLen;
        } else {
          row[col] = null;
        }
      }
      rows.push(row);
    }

    // Read operation type
    const opByte = payload[offset];
    offset += 1;
    const operations = ['insert', 'update', 'delete', 'full-sync'] as const;
    const operation = operations[opByte] || 'full-sync';

    // Read timestamp
    const timestamp = Number(payload.readBigInt64BE(offset));

    return { subscriptionId, columns, rows, operation, timestamp };
  }

  /**
   * Decode a subscription error message
   */
  static decodeSubscriptionError(payload: Buffer): SubscriptionError {
    let offset = 0;

    // Read subscription ID
    const idLen = payload.readUInt32BE(offset);
    offset += 4;
    const subscriptionId = payload.toString('utf-8', offset, offset + idLen);
    offset += idLen;

    // Read error code
    const codeLen = payload.readUInt32BE(offset);
    offset += 4;
    const code = payload.toString('utf-8', offset, offset + codeLen);
    offset += codeLen;

    // Read error message
    const msgLen = payload.readUInt32BE(offset);
    offset += 4;
    const message = payload.toString('utf-8', offset, offset + msgLen);
    offset += msgLen;

    // Read timestamp
    const timestamp = Number(payload.readBigInt64BE(offset));

    return { subscriptionId, code, message, timestamp };
  }
}
