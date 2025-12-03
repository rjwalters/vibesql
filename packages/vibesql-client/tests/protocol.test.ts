import { describe, it, expect } from 'vitest';
import { ProtocolCodec } from '../src/protocol.js';
import { MessageType } from '../src/types.js';

describe('ProtocolCodec', () => {
  describe('encodeQuery', () => {
    it('should encode simple query', () => {
      const frame = ProtocolCodec.encodeQuery('SELECT * FROM users');

      expect(frame[0]).toBe(MessageType.QUERY);
      expect(frame.length).toBeGreaterThan(5); // Header + payload
    });

    it('should encode query with parameters', () => {
      const frame = ProtocolCodec.encodeQuery('SELECT * FROM users WHERE id = $1', [1]);

      expect(frame[0]).toBe(MessageType.QUERY);
      expect(frame.length).toBeGreaterThan(5);
    });
  });

  describe('encodeSubscribe', () => {
    it('should encode subscription', () => {
      const frame = ProtocolCodec.encodeSubscribe(
        'sub-123',
        'SELECT * FROM users'
      );

      expect(frame[0]).toBe(MessageType.SUBSCRIBE);
      expect(frame.length).toBeGreaterThan(5);
    });

    it('should encode subscription with parameters', () => {
      const frame = ProtocolCodec.encodeSubscribe(
        'sub-123',
        'SELECT * FROM users WHERE active = $1',
        [true]
      );

      expect(frame[0]).toBe(MessageType.SUBSCRIBE);
    });
  });

  describe('encodeUnsubscribe', () => {
    it('should encode unsubscribe', () => {
      const frame = ProtocolCodec.encodeUnsubscribe('sub-123');

      expect(frame[0]).toBe(MessageType.UNSUBSCRIBE);
      expect(frame.length).toBeGreaterThan(5);
    });
  });

  describe('encodePing', () => {
    it('should encode ping', () => {
      const frame = ProtocolCodec.encodePing();

      expect(frame[0]).toBe(MessageType.PING);
      expect(frame.length).toBe(5); // Just header, no payload
    });
  });

  describe('decodeFrameHeader', () => {
    it('should decode complete frame', () => {
      const ping = ProtocolCodec.encodePing();
      const decoded = ProtocolCodec.decodeFrameHeader(ping);

      expect(decoded).not.toBeNull();
      expect(decoded?.type).toBe(MessageType.PING);
      expect(decoded?.payload.length).toBe(0);
    });

    it('should return null for incomplete frame', () => {
      const partial = Buffer.from([MessageType.PING, 0, 0, 0]);
      const decoded = ProtocolCodec.decodeFrameHeader(partial);

      expect(decoded).toBeNull();
    });
  });

  describe('decodeQueryResult', () => {
    it('should decode query result with rows', () => {
      // Build a minimal query result payload
      const payload = Buffer.alloc(1024);
      let offset = 0;

      // Column count: 2
      payload.writeUInt32BE(2, offset);
      offset += 4;

      // Column 1: "id"
      const col1 = Buffer.from('id');
      payload.writeUInt32BE(col1.length, offset);
      offset += 4;
      col1.copy(payload, offset);
      offset += col1.length;

      // Column 2: "name"
      const col2 = Buffer.from('name');
      payload.writeUInt32BE(col2.length, offset);
      offset += 4;
      col2.copy(payload, offset);
      offset += col2.length;

      // Row count: 1
      payload.writeUInt32BE(1, offset);
      offset += 4;

      // Row 1, Column 1: "1"
      const val1 = Buffer.from('1');
      payload.writeUInt32BE(val1.length, offset);
      offset += 4;
      val1.copy(payload, offset);
      offset += val1.length;

      // Row 1, Column 2: "John"
      const val2 = Buffer.from('John');
      payload.writeUInt32BE(val2.length, offset);
      offset += 4;
      val2.copy(payload, offset);
      offset += val2.length;

      // Rows affected
      payload.writeUInt32BE(1, offset);

      const trimmed = payload.subarray(0, offset + 4);
      const result = ProtocolCodec.decodeQueryResult(trimmed);

      expect(result.columns).toEqual(['id', 'name']);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toEqual({ id: '1', name: 'John' });
    });
  });

  describe('decodeSubscriptionData', () => {
    it('should decode subscription data', () => {
      const payload = Buffer.alloc(1024);
      let offset = 0;

      // Subscription ID: "sub-123"
      const subId = Buffer.from('sub-123');
      payload.writeUInt32BE(subId.length, offset);
      offset += 4;
      subId.copy(payload, offset);
      offset += subId.length;

      // Column count: 1
      payload.writeUInt32BE(1, offset);
      offset += 4;

      // Column 1: "id"
      const col = Buffer.from('id');
      payload.writeUInt32BE(col.length, offset);
      offset += 4;
      col.copy(payload, offset);
      offset += col.length;

      // Row count: 1
      payload.writeUInt32BE(1, offset);
      offset += 4;

      // Row value: "42"
      const val = Buffer.from('42');
      payload.writeUInt32BE(val.length, offset);
      offset += 4;
      val.copy(payload, offset);
      offset += val.length;

      // Operation: insert (0)
      payload[offset] = 0;
      offset += 1;

      // Timestamp
      payload.writeBigInt64BE(BigInt(Date.now()), offset);

      const trimmed = payload.subarray(0, offset + 8);
      const update = ProtocolCodec.decodeSubscriptionData(trimmed);

      expect(update.subscriptionId).toBe('sub-123');
      expect(update.columns).toEqual(['id']);
      expect(update.rows).toHaveLength(1);
      expect(update.operation).toBe('insert');
    });
  });
});
