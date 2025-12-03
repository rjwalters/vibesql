/**
 * Protocol Message Encoder
 * Encodes messages into PostgreSQL wire protocol format
 */

import { MessageCodes } from './messages';

/**
 * Encode a startup message
 */
export function encodeStartupMessage(
  database: string,
  user: string,
  options?: Record<string, string>
): Buffer {
  const params: Record<string, string> = {
    user,
    database,
    ...options,
  };

  let length = 8; // 4 bytes for length + 4 bytes for protocol version
  const paramPairs: string[] = [];

  for (const [key, value] of Object.entries(params)) {
    paramPairs.push(`${key}\0${value}\0`);
    length += key.length + 1 + value.length + 1;
  }

  length += 1; // null terminator

  const buf = Buffer.alloc(length);
  let offset = 0;

  // Message length
  buf.writeUInt32BE(length, offset);
  offset += 4;

  // Protocol version 3.0
  buf.writeUInt16BE(3, offset);
  offset += 2;
  buf.writeUInt16BE(0, offset);
  offset += 2;

  // Parameters
  for (const pair of paramPairs) {
    buf.write(pair, offset);
    offset += pair.length;
  }

  // Final null terminator
  buf[offset] = 0;

  return buf;
}

/**
 * Encode a password message
 */
export function encodePasswordMessage(password: string): Buffer {
  const passwordBuf = Buffer.from(password, 'utf8');
  const buf = Buffer.alloc(1 + 4 + passwordBuf.length + 1);

  let offset = 0;
  buf[offset] = MessageCodes.PasswordMessage;
  offset += 1;

  // Message length (excluding type byte)
  buf.writeUInt32BE(4 + passwordBuf.length + 1, offset);
  offset += 4;

  // Password
  passwordBuf.copy(buf, offset);
  offset += passwordBuf.length;

  // Null terminator
  buf[offset] = 0;

  return buf;
}

/**
 * Encode a query message
 */
export function encodeQuery(query: string): Buffer {
  const queryBuf = Buffer.from(query, 'utf8');
  const buf = Buffer.alloc(1 + 4 + queryBuf.length + 1);

  let offset = 0;
  buf[offset] = MessageCodes.Query;
  offset += 1;

  // Message length (excluding type byte)
  buf.writeUInt32BE(4 + queryBuf.length + 1, offset);
  offset += 4;

  // Query
  queryBuf.copy(buf, offset);
  offset += queryBuf.length;

  // Null terminator
  buf[offset] = 0;

  return buf;
}

/**
 * Encode a parse message (extended query protocol)
 */
export function encodeParse(
  statementName: string,
  query: string,
  parameterOids: number[] = []
): Buffer {
  const nameBuf = Buffer.from(statementName, 'utf8');
  const queryBuf = Buffer.from(query, 'utf8');

  let length = 4 + nameBuf.length + 1 + queryBuf.length + 1 + 2;
  length += parameterOids.length * 4;

  const buf = Buffer.alloc(1 + length);
  let offset = 0;

  buf[offset] = 'P'.charCodeAt(0);
  offset += 1;

  // Length
  buf.writeUInt32BE(length, offset);
  offset += 4;

  // Statement name
  nameBuf.copy(buf, offset);
  offset += nameBuf.length;
  buf[offset] = 0;
  offset += 1;

  // Query
  queryBuf.copy(buf, offset);
  offset += queryBuf.length;
  buf[offset] = 0;
  offset += 1;

  // Number of parameter types
  buf.writeUInt16BE(parameterOids.length, offset);
  offset += 2;

  // Parameter type OIDs
  for (const oid of parameterOids) {
    buf.writeUInt32BE(oid, offset);
    offset += 4;
  }

  return buf;
}

/**
 * Encode a bind message
 */
export function encodeBind(
  portalName: string,
  statementName: string,
  params: (string | number | boolean | null)[] = [],
  resultFormatCodes: number[] = []
): Buffer {
  const portalBuf = Buffer.from(portalName, 'utf8');
  const statementBuf = Buffer.from(statementName, 'utf8');

  // Convert parameters to buffers
  const paramBuffers: (Buffer | null)[] = params.map(param => {
    if (param === null) {
      return null;
    }
    if (typeof param === 'string') {
      return Buffer.from(param, 'utf8');
    }
    if (typeof param === 'number') {
      return Buffer.from(String(param), 'utf8');
    }
    if (typeof param === 'boolean') {
      return Buffer.from(param ? 't' : 'f', 'utf8');
    }
    return null;
  });

  let length = 4 + portalBuf.length + 1 + statementBuf.length + 1 + 2;
  length += 2; // parameter format codes count
  length += 2; // result format codes count
  length += resultFormatCodes.length * 2;
  length += 2; // parameter values count

  for (const paramBuf of paramBuffers) {
    length += 4; // value length
    if (paramBuf) {
      length += paramBuf.length;
    }
  }

  const buf = Buffer.alloc(1 + length);
  let offset = 0;

  buf[offset] = 'B'.charCodeAt(0);
  offset += 1;

  // Length
  buf.writeUInt32BE(length, offset);
  offset += 4;

  // Portal name
  portalBuf.copy(buf, offset);
  offset += portalBuf.length;
  buf[offset] = 0;
  offset += 1;

  // Statement name
  statementBuf.copy(buf, offset);
  offset += statementBuf.length;
  buf[offset] = 0;
  offset += 1;

  // Parameter format codes count (0 = all text)
  buf.writeUInt16BE(0, offset);
  offset += 2;

  // Parameter count
  buf.writeUInt16BE(paramBuffers.length, offset);
  offset += 2;

  // Parameters
  for (const paramBuf of paramBuffers) {
    if (paramBuf === null) {
      buf.writeInt32BE(-1, offset);
      offset += 4;
    } else {
      buf.writeInt32BE(paramBuf.length, offset);
      offset += 4;
      paramBuf.copy(buf, offset);
      offset += paramBuf.length;
    }
  }

  // Result format codes count
  buf.writeUInt16BE(resultFormatCodes.length, offset);
  offset += 2;

  // Result format codes
  for (const code of resultFormatCodes) {
    buf.writeUInt16BE(code, offset);
    offset += 2;
  }

  return buf;
}

/**
 * Encode an execute message
 */
export function encodeExecute(
  portalName: string,
  maxRows: number = 0
): Buffer {
  const portalBuf = Buffer.from(portalName, 'utf8');
  const length = 4 + portalBuf.length + 1 + 4;

  const buf = Buffer.alloc(1 + length);
  let offset = 0;

  buf[offset] = 'E'.charCodeAt(0);
  offset += 1;

  // Length
  buf.writeUInt32BE(length, offset);
  offset += 4;

  // Portal name
  portalBuf.copy(buf, offset);
  offset += portalBuf.length;
  buf[offset] = 0;
  offset += 1;

  // Max rows
  buf.writeUInt32BE(maxRows, offset);

  return buf;
}

/**
 * Encode a flush message
 */
export function encodeFlush(): Buffer {
  const buf = Buffer.alloc(5);
  buf[0] = 'H'.charCodeAt(0);
  buf.writeUInt32BE(4, 1);
  return buf;
}

/**
 * Encode a sync message
 */
export function encodeSync(): Buffer {
  const buf = Buffer.alloc(5);
  buf[0] = 'S'.charCodeAt(0);
  buf.writeUInt32BE(4, 1);
  return buf;
}

/**
 * Encode a terminate message
 */
export function encodeTerminate(): Buffer {
  const buf = Buffer.alloc(5);
  buf[0] = MessageCodes.Terminate;
  buf.writeUInt32BE(4, 1);
  return buf;
}

/**
 * Encode a subscribe message (VibeSql extension)
 */
export function encodeSubscribe(
  subscriptionId: Buffer,
  query: string,
  params: (string | number | boolean | null)[] = []
): Buffer {
  const queryBuf = Buffer.from(query, 'utf8');

  // Convert parameters to buffers
  const paramBuffers: (Buffer | null)[] = params.map(param => {
    if (param === null) {
      return null;
    }
    if (typeof param === 'string') {
      return Buffer.from(param, 'utf8');
    }
    if (typeof param === 'number') {
      return Buffer.from(String(param), 'utf8');
    }
    if (typeof param === 'boolean') {
      return Buffer.from(param ? 't' : 'f', 'utf8');
    }
    return null;
  });

  let length = 4 + 16 + queryBuf.length + 1 + 2; // subscription id + query + null + param count
  for (const paramBuf of paramBuffers) {
    length += 4; // value length
    if (paramBuf) {
      length += paramBuf.length;
    }
  }

  const buf = Buffer.alloc(1 + length);
  let offset = 0;

  buf[offset] = MessageCodes.Subscribe;
  offset += 1;

  // Length
  buf.writeUInt32BE(length, offset);
  offset += 4;

  // Subscription ID (16 bytes)
  subscriptionId.copy(buf, offset);
  offset += 16;

  // Query
  queryBuf.copy(buf, offset);
  offset += queryBuf.length;
  buf[offset] = 0;
  offset += 1;

  // Parameter count
  buf.writeUInt16BE(paramBuffers.length, offset);
  offset += 2;

  // Parameters
  for (const paramBuf of paramBuffers) {
    if (paramBuf === null) {
      buf.writeInt32BE(-1, offset);
      offset += 4;
    } else {
      buf.writeInt32BE(paramBuf.length, offset);
      offset += 4;
      paramBuf.copy(buf, offset);
      offset += paramBuf.length;
    }
  }

  return buf;
}

/**
 * Encode an unsubscribe message (VibeSql extension)
 */
export function encodeUnsubscribe(subscriptionId: Buffer): Buffer {
  const length = 4 + 16;
  const buf = Buffer.alloc(1 + length);
  let offset = 0;

  buf[offset] = MessageCodes.Unsubscribe;
  offset += 1;

  // Length
  buf.writeUInt32BE(length, offset);
  offset += 4;

  // Subscription ID
  subscriptionId.copy(buf, offset);

  return buf;
}
