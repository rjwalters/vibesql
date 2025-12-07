/**
 * Tests for SubscriptionPartialData (0xF7) message parsing and handling
 */

import { describe, it, expect } from 'vitest';
import { MessageParser, parseColumnValue, TYPE_OIDS } from '../src/protocol/parser';
import { MessageCodes, SubscriptionPartialDataMessage } from '../src/protocol/messages';

describe('SubscriptionPartialData Message Parsing', () => {
  /**
   * Helper to create a valid 0xF7 message buffer
   */
  function createPartialDataMessage(options: {
    subscriptionId: Buffer;
    rows: Array<{
      totalColumns: number;
      presentColumns: number[];
      values: (string | null)[];
    }>;
  }): Buffer {
    const parts: Buffer[] = [];

    // Subscription ID (16 bytes)
    parts.push(options.subscriptionId);

    // Update type: 4 = SelectiveUpdate
    parts.push(Buffer.from([4]));

    // Row count (i32)
    const rowCountBuf = Buffer.alloc(4);
    rowCountBuf.writeInt32BE(options.rows.length, 0);
    parts.push(rowCountBuf);

    // Each row
    for (const row of options.rows) {
      // Total columns (u16)
      const totalColBuf = Buffer.alloc(2);
      totalColBuf.writeUInt16BE(row.totalColumns, 0);
      parts.push(totalColBuf);

      // Bitmap (ceil(totalColumns/8) bytes)
      const bitmapSize = Math.ceil(row.totalColumns / 8);
      const bitmap = Buffer.alloc(bitmapSize, 0);
      for (const colIndex of row.presentColumns) {
        const byteIndex = Math.floor(colIndex / 8);
        const bitIndex = 7 - (colIndex % 8);
        bitmap[byteIndex] |= 1 << bitIndex;
      }
      parts.push(bitmap);

      // Values for present columns
      for (const value of row.values) {
        if (value === null) {
          const lenBuf = Buffer.alloc(4);
          lenBuf.writeInt32BE(-1, 0);
          parts.push(lenBuf);
        } else {
          const valueBuf = Buffer.from(value, 'utf8');
          const lenBuf = Buffer.alloc(4);
          lenBuf.writeInt32BE(valueBuf.length, 0);
          parts.push(lenBuf);
          parts.push(valueBuf);
        }
      }
    }

    const body = Buffer.concat(parts);

    // Create full message: type (1) + length (4) + body
    const length = 4 + body.length; // Length includes itself but not type byte
    const header = Buffer.alloc(5);
    header[0] = MessageCodes.SubscriptionPartialData;
    header.writeUInt32BE(length, 1);

    return Buffer.concat([header, body]);
  }

  it('should parse a simple partial update with one column', () => {
    const subscriptionId = Buffer.alloc(16);
    subscriptionId.write('0123456789abcdef', 0, 'hex');

    const messageBuffer = createPartialDataMessage({
      subscriptionId,
      rows: [
        {
          totalColumns: 3,
          presentColumns: [1], // Only column 1 is present
          values: ['updated_value'],
        },
      ],
    });

    const parser = new MessageParser();
    parser.addData(messageBuffer);
    const messages = parser.getMessages();

    expect(messages).toHaveLength(1);
    expect(messages[0].type).toBe('SubscriptionPartialData');

    const msg = messages[0] as SubscriptionPartialDataMessage;
    expect(msg.subscriptionId.toString('hex')).toBe(
      subscriptionId.toString('hex')
    );
    expect(msg.rows).toHaveLength(1);
    expect(msg.rows[0].totalColumns).toBe(3);
    expect(msg.rows[0].presentColumns).toEqual([1]);
    expect(msg.rows[0].values).toEqual(['updated_value']);
  });

  it('should parse partial update with multiple columns', () => {
    const subscriptionId = Buffer.alloc(16, 0xab);

    const messageBuffer = createPartialDataMessage({
      subscriptionId,
      rows: [
        {
          totalColumns: 5,
          presentColumns: [0, 2, 4], // First, third, fifth columns
          values: ['val0', 'val2', 'val4'],
        },
      ],
    });

    const parser = new MessageParser();
    parser.addData(messageBuffer);
    const messages = parser.getMessages();

    expect(messages).toHaveLength(1);
    const msg = messages[0] as SubscriptionPartialDataMessage;
    expect(msg.rows[0].presentColumns).toEqual([0, 2, 4]);
    expect(msg.rows[0].values).toEqual(['val0', 'val2', 'val4']);
  });

  it('should handle NULL values in partial updates', () => {
    const subscriptionId = Buffer.alloc(16, 0x12);

    const messageBuffer = createPartialDataMessage({
      subscriptionId,
      rows: [
        {
          totalColumns: 3,
          presentColumns: [0, 1, 2],
          values: ['first', null, 'third'],
        },
      ],
    });

    const parser = new MessageParser();
    parser.addData(messageBuffer);
    const messages = parser.getMessages();

    expect(messages).toHaveLength(1);
    const msg = messages[0] as SubscriptionPartialDataMessage;
    expect(msg.rows[0].values).toEqual(['first', null, 'third']);
  });

  it('should parse multiple rows in a single message', () => {
    const subscriptionId = Buffer.alloc(16, 0x34);

    const messageBuffer = createPartialDataMessage({
      subscriptionId,
      rows: [
        {
          totalColumns: 2,
          presentColumns: [0],
          values: ['row1_col0'],
        },
        {
          totalColumns: 2,
          presentColumns: [1],
          values: ['row2_col1'],
        },
        {
          totalColumns: 2,
          presentColumns: [0, 1],
          values: ['row3_col0', 'row3_col1'],
        },
      ],
    });

    const parser = new MessageParser();
    parser.addData(messageBuffer);
    const messages = parser.getMessages();

    expect(messages).toHaveLength(1);
    const msg = messages[0] as SubscriptionPartialDataMessage;
    expect(msg.rows).toHaveLength(3);

    expect(msg.rows[0].presentColumns).toEqual([0]);
    expect(msg.rows[0].values).toEqual(['row1_col0']);

    expect(msg.rows[1].presentColumns).toEqual([1]);
    expect(msg.rows[1].values).toEqual(['row2_col1']);

    expect(msg.rows[2].presentColumns).toEqual([0, 1]);
    expect(msg.rows[2].values).toEqual(['row3_col0', 'row3_col1']);
  });

  it('should handle rows with more than 8 columns (multi-byte bitmap)', () => {
    const subscriptionId = Buffer.alloc(16, 0x56);

    const messageBuffer = createPartialDataMessage({
      subscriptionId,
      rows: [
        {
          totalColumns: 16,
          presentColumns: [0, 7, 8, 15], // Spans both bytes of bitmap
          values: ['c0', 'c7', 'c8', 'c15'],
        },
      ],
    });

    const parser = new MessageParser();
    parser.addData(messageBuffer);
    const messages = parser.getMessages();

    expect(messages).toHaveLength(1);
    const msg = messages[0] as SubscriptionPartialDataMessage;
    expect(msg.rows[0].totalColumns).toBe(16);
    expect(msg.rows[0].presentColumns).toEqual([0, 7, 8, 15]);
    expect(msg.rows[0].values).toEqual(['c0', 'c7', 'c8', 'c15']);
  });

  it('should handle empty values (zero-length strings)', () => {
    const subscriptionId = Buffer.alloc(16, 0x78);

    const messageBuffer = createPartialDataMessage({
      subscriptionId,
      rows: [
        {
          totalColumns: 2,
          presentColumns: [0, 1],
          values: ['', 'non-empty'],
        },
      ],
    });

    const parser = new MessageParser();
    parser.addData(messageBuffer);
    const messages = parser.getMessages();

    expect(messages).toHaveLength(1);
    const msg = messages[0] as SubscriptionPartialDataMessage;
    expect(msg.rows[0].values).toEqual(['', 'non-empty']);
  });

  it('should handle all columns present (same as full update)', () => {
    const subscriptionId = Buffer.alloc(16, 0x9a);

    const messageBuffer = createPartialDataMessage({
      subscriptionId,
      rows: [
        {
          totalColumns: 4,
          presentColumns: [0, 1, 2, 3],
          values: ['a', 'b', 'c', 'd'],
        },
      ],
    });

    const parser = new MessageParser();
    parser.addData(messageBuffer);
    const messages = parser.getMessages();

    expect(messages).toHaveLength(1);
    const msg = messages[0] as SubscriptionPartialDataMessage;
    expect(msg.rows[0].presentColumns).toEqual([0, 1, 2, 3]);
    expect(msg.rows[0].values).toEqual(['a', 'b', 'c', 'd']);
  });

  it('should handle fragmented message data', () => {
    const subscriptionId = Buffer.alloc(16, 0xbc);

    const fullMessage = createPartialDataMessage({
      subscriptionId,
      rows: [
        {
          totalColumns: 2,
          presentColumns: [0, 1],
          values: ['hello', 'world'],
        },
      ],
    });

    // Split the message into fragments
    const fragment1 = fullMessage.slice(0, 10);
    const fragment2 = fullMessage.slice(10, 20);
    const fragment3 = fullMessage.slice(20);

    const parser = new MessageParser();

    // Add first fragment - should not produce message yet
    parser.addData(fragment1);
    expect(parser.getMessages()).toHaveLength(0);

    // Add second fragment - should not produce message yet
    parser.addData(fragment2);
    expect(parser.getMessages()).toHaveLength(0);

    // Add final fragment - should now produce the complete message
    parser.addData(fragment3);
    const messages = parser.getMessages();

    expect(messages).toHaveLength(1);
    expect(messages[0].type).toBe('SubscriptionPartialData');
  });
});

describe('parseColumnValue Type Conversion', () => {
  it('should convert integer types to numbers', () => {
    expect(parseColumnValue('42', TYPE_OIDS.INT4)).toBe(42);
    expect(parseColumnValue('-123', TYPE_OIDS.INT8)).toBe(-123);
    expect(parseColumnValue('0', TYPE_OIDS.INT2)).toBe(0);
  });

  it('should convert float types to numbers', () => {
    expect(parseColumnValue('3.14159', TYPE_OIDS.FLOAT8)).toBeCloseTo(3.14159);
    expect(parseColumnValue('-2.5', TYPE_OIDS.FLOAT4)).toBeCloseTo(-2.5);
  });

  it('should preserve NUMERIC as string for precision', () => {
    // NUMERIC/DECIMAL types are returned as strings to preserve precision
    expect(parseColumnValue('1.23e10', TYPE_OIDS.NUMERIC)).toBe('1.23e10');
    expect(parseColumnValue('123456789012345678901234567890', TYPE_OIDS.NUMERIC)).toBe('123456789012345678901234567890');
  });

  it('should convert boolean type', () => {
    expect(parseColumnValue('t', TYPE_OIDS.BOOLEAN)).toBe(true);
    expect(parseColumnValue('f', TYPE_OIDS.BOOLEAN)).toBe(false);
  });

  it('should convert timestamp types to Date objects', () => {
    const timestamp = parseColumnValue('2024-01-15 10:30:00', TYPE_OIDS.TIMESTAMP);
    expect(timestamp).toBeInstanceOf(Date);
    expect(timestamp.getFullYear()).toBe(2024);

    const timestamptz = parseColumnValue('2024-01-15 10:30:00+00', TYPE_OIDS.TIMESTAMPTZ);
    expect(timestamptz).toBeInstanceOf(Date);

    const date = parseColumnValue('2024-01-15', TYPE_OIDS.DATE);
    expect(date).toBeInstanceOf(Date);
  });

  it('should return strings as-is for text types', () => {
    expect(parseColumnValue('hello world', TYPE_OIDS.TEXT)).toBe('hello world');
    expect(parseColumnValue('test', TYPE_OIDS.VARCHAR)).toBe('test');
  });

  it('should return strings for unknown type OIDs', () => {
    expect(parseColumnValue('unknown', 99999)).toBe('unknown');
  });
});
