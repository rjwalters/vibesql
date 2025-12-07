/**
 * Tests for type conversion utilities
 */

import { describe, it, expect } from 'vitest';
import { parseColumnValue, PG_TYPE_OIDS } from '../src/protocol/typeConversion';

describe('parseColumnValue', () => {
  describe('integer types', () => {
    it('should parse INT4 values', () => {
      expect(parseColumnValue('42', PG_TYPE_OIDS.INT4)).toBe(42);
      expect(parseColumnValue('-123', PG_TYPE_OIDS.INT4)).toBe(-123);
      expect(parseColumnValue('0', PG_TYPE_OIDS.INT4)).toBe(0);
    });

    it('should parse INT8 values', () => {
      expect(parseColumnValue('9223372036854775807', PG_TYPE_OIDS.INT8)).toBe(9223372036854775807);
      expect(parseColumnValue('-9223372036854775808', PG_TYPE_OIDS.INT8)).toBe(-9223372036854775808);
    });
  });

  describe('float types', () => {
    it('should parse FLOAT8 values', () => {
      expect(parseColumnValue('3.14159', PG_TYPE_OIDS.FLOAT8)).toBe(3.14159);
      expect(parseColumnValue('-2.5', PG_TYPE_OIDS.FLOAT8)).toBe(-2.5);
      expect(parseColumnValue('0.0', PG_TYPE_OIDS.FLOAT8)).toBe(0);
    });
  });

  describe('boolean type', () => {
    it('should parse BOOLEAN values', () => {
      expect(parseColumnValue('t', PG_TYPE_OIDS.BOOLEAN)).toBe(true);
      expect(parseColumnValue('f', PG_TYPE_OIDS.BOOLEAN)).toBe(false);
    });
  });

  describe('text types', () => {
    it('should return TEXT values as-is', () => {
      expect(parseColumnValue('hello world', PG_TYPE_OIDS.TEXT)).toBe('hello world');
      expect(parseColumnValue('', PG_TYPE_OIDS.TEXT)).toBe('');
    });

    it('should return VARCHAR values as-is', () => {
      expect(parseColumnValue('test string', PG_TYPE_OIDS.VARCHAR)).toBe('test string');
    });
  });

  describe('timestamp types', () => {
    it('should parse TIMESTAMP values as Date', () => {
      const result = parseColumnValue('2024-01-15 10:30:00', PG_TYPE_OIDS.TIMESTAMP);
      expect(result).toBeInstanceOf(Date);
      expect(result.getFullYear()).toBe(2024);
      expect(result.getMonth()).toBe(0); // January is 0
      expect(result.getDate()).toBe(15);
    });

    it('should parse TIMESTAMPTZ values as Date', () => {
      const result = parseColumnValue('2024-06-20T14:30:00Z', PG_TYPE_OIDS.TIMESTAMPTZ);
      expect(result).toBeInstanceOf(Date);
    });
  });

  describe('unknown types', () => {
    it('should return unknown type OIDs as strings', () => {
      expect(parseColumnValue('some value', 99999)).toBe('some value');
    });
  });
});

describe('Type conversion consistency', () => {
  it('should produce consistent types between initial and partial updates', () => {
    // Simulate what happens with initial full update
    const initialIntValue = parseColumnValue('42', PG_TYPE_OIDS.INT4);

    // Simulate what happens with partial update (now uses same conversion)
    const partialIntValue = parseColumnValue('43', PG_TYPE_OIDS.INT4);

    // Both should be numbers
    expect(typeof initialIntValue).toBe('number');
    expect(typeof partialIntValue).toBe('number');
    expect(initialIntValue).toBe(42);
    expect(partialIntValue).toBe(43);
  });

  it('should handle boolean type consistency', () => {
    const initialBool = parseColumnValue('t', PG_TYPE_OIDS.BOOLEAN);
    const partialBool = parseColumnValue('f', PG_TYPE_OIDS.BOOLEAN);

    expect(typeof initialBool).toBe('boolean');
    expect(typeof partialBool).toBe('boolean');
    expect(initialBool).toBe(true);
    expect(partialBool).toBe(false);
  });
});
