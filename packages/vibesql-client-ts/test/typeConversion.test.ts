/**
 * Tests for PostgreSQL type conversion
 */

import { describe, it, expect } from 'vitest';
import {
  parseColumnValue,
  PgTypeOid,
  isNumericType,
  isArrayType,
  getArrayElementType,
  PgInterval,
} from '../src/protocol/typeConversion';

describe('parseColumnValue', () => {
  describe('Boolean type', () => {
    it('should parse "t" as true', () => {
      expect(parseColumnValue('t', PgTypeOid.BOOLEAN)).toBe(true);
    });

    it('should parse "true" as true', () => {
      expect(parseColumnValue('true', PgTypeOid.BOOLEAN)).toBe(true);
    });

    it('should parse "1" as true', () => {
      expect(parseColumnValue('1', PgTypeOid.BOOLEAN)).toBe(true);
    });

    it('should parse "f" as false', () => {
      expect(parseColumnValue('f', PgTypeOid.BOOLEAN)).toBe(false);
    });

    it('should parse "false" as false', () => {
      expect(parseColumnValue('false', PgTypeOid.BOOLEAN)).toBe(false);
    });

    it('should parse "0" as false', () => {
      expect(parseColumnValue('0', PgTypeOid.BOOLEAN)).toBe(false);
    });
  });

  describe('Integer types', () => {
    it('should parse INT2 values', () => {
      expect(parseColumnValue('42', PgTypeOid.INT2)).toBe(42);
      expect(parseColumnValue('-32768', PgTypeOid.INT2)).toBe(-32768);
      expect(parseColumnValue('32767', PgTypeOid.INT2)).toBe(32767);
    });

    it('should parse INT4 values', () => {
      expect(parseColumnValue('123456', PgTypeOid.INT4)).toBe(123456);
      expect(parseColumnValue('-2147483648', PgTypeOid.INT4)).toBe(-2147483648);
      expect(parseColumnValue('2147483647', PgTypeOid.INT4)).toBe(2147483647);
    });

    it('should parse INT8 values within safe integer range', () => {
      expect(parseColumnValue('9007199254740991', PgTypeOid.INT8)).toBe(
        9007199254740991
      );
      expect(parseColumnValue('-9007199254740991', PgTypeOid.INT8)).toBe(
        -9007199254740991
      );
    });

    it('should return string for INT8 values outside safe integer range', () => {
      // Values beyond Number.MAX_SAFE_INTEGER should be returned as strings
      expect(parseColumnValue('9007199254740993', PgTypeOid.INT8)).toBe(
        '9007199254740993'
      );
    });
  });

  describe('Floating point types', () => {
    it('should parse FLOAT4 values', () => {
      expect(parseColumnValue('3.14', PgTypeOid.FLOAT4)).toBeCloseTo(3.14);
      expect(parseColumnValue('-2.5', PgTypeOid.FLOAT4)).toBeCloseTo(-2.5);
      expect(parseColumnValue('1.23e10', PgTypeOid.FLOAT4)).toBeCloseTo(
        1.23e10
      );
    });

    it('should parse FLOAT8 values', () => {
      expect(parseColumnValue('3.141592653589793', PgTypeOid.FLOAT8)).toBeCloseTo(
        3.141592653589793
      );
      expect(parseColumnValue('-1.7976931348623157e308', PgTypeOid.FLOAT8)).toBe(
        -1.7976931348623157e308
      );
    });

    it('should parse special float values', () => {
      expect(parseColumnValue('Infinity', PgTypeOid.FLOAT8)).toBe(Infinity);
      expect(parseColumnValue('-Infinity', PgTypeOid.FLOAT8)).toBe(-Infinity);
      expect(parseColumnValue('NaN', PgTypeOid.FLOAT8)).toBeNaN();
    });
  });

  describe('NUMERIC type', () => {
    it('should return NUMERIC values as strings to preserve precision', () => {
      expect(parseColumnValue('123.456789012345678901234567890', PgTypeOid.NUMERIC)).toBe(
        '123.456789012345678901234567890'
      );
      expect(parseColumnValue('-99999999999999999999.99999999', PgTypeOid.NUMERIC)).toBe(
        '-99999999999999999999.99999999'
      );
    });
  });

  describe('Text types', () => {
    it('should return TEXT as string', () => {
      expect(parseColumnValue('hello world', PgTypeOid.TEXT)).toBe('hello world');
    });

    it('should return VARCHAR as string', () => {
      expect(parseColumnValue('varchar value', PgTypeOid.VARCHAR)).toBe(
        'varchar value'
      );
    });

    it('should return CHAR as string', () => {
      expect(parseColumnValue('A', PgTypeOid.CHAR)).toBe('A');
    });

    it('should return BPCHAR as string', () => {
      expect(parseColumnValue('padded    ', PgTypeOid.BPCHAR)).toBe('padded    ');
    });

    it('should return NAME as string', () => {
      expect(parseColumnValue('identifier', PgTypeOid.NAME)).toBe('identifier');
    });
  });

  describe('Date type', () => {
    it('should parse DATE to Date object at midnight UTC', () => {
      const result = parseColumnValue('2024-03-15', PgTypeOid.DATE) as Date;
      expect(result).toBeInstanceOf(Date);
      expect(result.getUTCFullYear()).toBe(2024);
      expect(result.getUTCMonth()).toBe(2); // March is 2 (0-indexed)
      expect(result.getUTCDate()).toBe(15);
      expect(result.getUTCHours()).toBe(0);
      expect(result.getUTCMinutes()).toBe(0);
      expect(result.getUTCSeconds()).toBe(0);
    });

    it('should handle edge case dates', () => {
      const result = parseColumnValue('2000-01-01', PgTypeOid.DATE) as Date;
      expect(result.getUTCFullYear()).toBe(2000);
      expect(result.getUTCMonth()).toBe(0);
      expect(result.getUTCDate()).toBe(1);
    });
  });

  describe('Time types', () => {
    it('should return TIME as string', () => {
      expect(parseColumnValue('14:30:00', PgTypeOid.TIME)).toBe('14:30:00');
      expect(parseColumnValue('23:59:59.999999', PgTypeOid.TIME)).toBe(
        '23:59:59.999999'
      );
    });

    it('should return TIMETZ as string', () => {
      expect(parseColumnValue('14:30:00+05:30', PgTypeOid.TIMETZ)).toBe(
        '14:30:00+05:30'
      );
      expect(parseColumnValue('08:00:00-08', PgTypeOid.TIMETZ)).toBe(
        '08:00:00-08'
      );
    });
  });

  describe('Timestamp types', () => {
    it('should parse TIMESTAMP to Date', () => {
      const result = parseColumnValue(
        '2024-03-15 14:30:00',
        PgTypeOid.TIMESTAMP
      ) as Date;
      expect(result).toBeInstanceOf(Date);
    });

    it('should parse TIMESTAMPTZ to Date', () => {
      const result = parseColumnValue(
        '2024-03-15 14:30:00+00',
        PgTypeOid.TIMESTAMPTZ
      ) as Date;
      expect(result).toBeInstanceOf(Date);
    });

    it('should handle ISO format timestamps', () => {
      const result = parseColumnValue(
        '2024-03-15T14:30:00.000Z',
        PgTypeOid.TIMESTAMPTZ
      ) as Date;
      expect(result.toISOString()).toBe('2024-03-15T14:30:00.000Z');
    });
  });

  describe('Interval type', () => {
    it('should parse simple interval with time', () => {
      const result = parseColumnValue('00:30:00', PgTypeOid.INTERVAL) as PgInterval;
      expect(result.hours).toBe(0);
      expect(result.minutes).toBe(30);
      expect(result.seconds).toBe(0);
    });

    it('should parse interval with days', () => {
      const result = parseColumnValue('3 days', PgTypeOid.INTERVAL) as PgInterval;
      expect(result.days).toBe(3);
    });

    it('should parse complex interval', () => {
      const result = parseColumnValue(
        '1 year 2 mons 3 days 04:05:06',
        PgTypeOid.INTERVAL
      ) as PgInterval;
      expect(result.years).toBe(1);
      expect(result.months).toBe(2);
      expect(result.days).toBe(3);
      expect(result.hours).toBe(4);
      expect(result.minutes).toBe(5);
      expect(result.seconds).toBe(6);
    });

    it('should handle negative intervals', () => {
      const result = parseColumnValue('-3 days', PgTypeOid.INTERVAL) as PgInterval;
      expect(result.days).toBe(-3);
    });

    it('should handle verbose format with @', () => {
      const result = parseColumnValue(
        '@ 1 year 2 mons',
        PgTypeOid.INTERVAL
      ) as PgInterval;
      expect(result.years).toBe(1);
      expect(result.months).toBe(2);
    });
  });

  describe('JSON types', () => {
    it('should parse JSON objects', () => {
      const result = parseColumnValue('{"key": "value", "num": 42}', PgTypeOid.JSON);
      expect(result).toEqual({ key: 'value', num: 42 });
    });

    it('should parse JSONB objects', () => {
      const result = parseColumnValue(
        '{"nested": {"array": [1, 2, 3]}}',
        PgTypeOid.JSONB
      );
      expect(result).toEqual({ nested: { array: [1, 2, 3] } });
    });

    it('should parse JSON arrays', () => {
      const result = parseColumnValue('[1, "two", true, null]', PgTypeOid.JSON);
      expect(result).toEqual([1, 'two', true, null]);
    });

    it('should parse JSON primitives', () => {
      expect(parseColumnValue('"hello"', PgTypeOid.JSON)).toBe('hello');
      expect(parseColumnValue('42', PgTypeOid.JSON)).toBe(42);
      expect(parseColumnValue('true', PgTypeOid.JSON)).toBe(true);
      expect(parseColumnValue('null', PgTypeOid.JSON)).toBe(null);
    });

    it('should return string for invalid JSON', () => {
      expect(parseColumnValue('not valid json', PgTypeOid.JSON)).toBe(
        'not valid json'
      );
    });
  });

  describe('UUID type', () => {
    it('should return UUID as string', () => {
      expect(
        parseColumnValue('550e8400-e29b-41d4-a716-446655440000', PgTypeOid.UUID)
      ).toBe('550e8400-e29b-41d4-a716-446655440000');
    });

    it('should handle uppercase UUIDs', () => {
      expect(
        parseColumnValue('550E8400-E29B-41D4-A716-446655440000', PgTypeOid.UUID)
      ).toBe('550E8400-E29B-41D4-A716-446655440000');
    });
  });

  describe('BYTEA type', () => {
    it('should parse hex format bytea', () => {
      const result = parseColumnValue('\\xDEADBEEF', PgTypeOid.BYTEA) as Uint8Array;
      expect(result).toBeInstanceOf(Uint8Array);
      expect(result.length).toBe(4);
      expect(result[0]).toBe(0xde);
      expect(result[1]).toBe(0xad);
      expect(result[2]).toBe(0xbe);
      expect(result[3]).toBe(0xef);
    });

    it('should handle empty bytea', () => {
      const result = parseColumnValue('\\x', PgTypeOid.BYTEA) as Uint8Array;
      expect(result).toBeInstanceOf(Uint8Array);
      expect(result.length).toBe(0);
    });

    it('should parse bytea with lowercase hex', () => {
      const result = parseColumnValue('\\xcafe', PgTypeOid.BYTEA) as Uint8Array;
      expect(result.length).toBe(2);
      expect(result[0]).toBe(0xca);
      expect(result[1]).toBe(0xfe);
    });
  });

  describe('Array types', () => {
    it('should parse empty arrays', () => {
      expect(parseColumnValue('{}', PgTypeOid.INT4_ARRAY)).toEqual([]);
      expect(parseColumnValue('{}', PgTypeOid.TEXT_ARRAY)).toEqual([]);
    });

    it('should parse integer arrays', () => {
      expect(parseColumnValue('{1,2,3}', PgTypeOid.INT4_ARRAY)).toEqual([1, 2, 3]);
      expect(parseColumnValue('{-1,0,1}', PgTypeOid.INT2_ARRAY)).toEqual([
        -1, 0, 1,
      ]);
    });

    it('should parse float arrays', () => {
      const result = parseColumnValue('{1.5,2.5,3.5}', PgTypeOid.FLOAT8_ARRAY) as number[];
      expect(result[0]).toBeCloseTo(1.5);
      expect(result[1]).toBeCloseTo(2.5);
      expect(result[2]).toBeCloseTo(3.5);
    });

    it('should parse text arrays', () => {
      expect(parseColumnValue('{hello,world}', PgTypeOid.TEXT_ARRAY)).toEqual([
        'hello',
        'world',
      ]);
    });

    it('should parse text arrays with quoted values', () => {
      expect(
        parseColumnValue('{"hello world","foo bar"}', PgTypeOid.TEXT_ARRAY)
      ).toEqual(['hello world', 'foo bar']);
    });

    it('should parse boolean arrays', () => {
      expect(parseColumnValue('{t,f,t}', PgTypeOid.BOOLEAN_ARRAY)).toEqual([
        true,
        false,
        true,
      ]);
    });

    it('should handle NULL values in arrays', () => {
      expect(parseColumnValue('{1,NULL,3}', PgTypeOid.INT4_ARRAY)).toEqual([
        1,
        null,
        3,
      ]);
      expect(parseColumnValue('{hello,NULL,world}', PgTypeOid.TEXT_ARRAY)).toEqual([
        'hello',
        null,
        'world',
      ]);
    });

    it('should parse UUID arrays', () => {
      expect(
        parseColumnValue(
          '{550e8400-e29b-41d4-a716-446655440000,550e8400-e29b-41d4-a716-446655440001}',
          PgTypeOid.UUID_ARRAY
        )
      ).toEqual([
        '550e8400-e29b-41d4-a716-446655440000',
        '550e8400-e29b-41d4-a716-446655440001',
      ]);
    });

    it('should parse JSON arrays with simple values', () => {
      // JSON arrays with primitive values
      const result = parseColumnValue('{1,2,3}', PgTypeOid.JSON_ARRAY) as unknown[];
      expect(result).toEqual([1, 2, 3]);
    });

    it('should parse JSON arrays with quoted JSON objects', () => {
      // PostgreSQL escapes quotes with backslash in array literals
      // The input is: {"{\\"a\\":1}","{\\"b\\":2}"}
      // Which represents array elements: {"a":1} and {"b":2}
      const input = '{"{\\"a\\":1}","{\\"b\\":2}"}';
      const result = parseColumnValue(input, PgTypeOid.JSON_ARRAY) as unknown[];
      expect(result).toEqual([{ a: 1 }, { b: 2 }]);
    });

    it('should handle escaped characters in arrays', () => {
      expect(
        parseColumnValue('{"hello\\"world","back\\\\slash"}', PgTypeOid.TEXT_ARRAY)
      ).toEqual(['hello"world', 'back\\slash']);
    });
  });

  describe('Unknown types', () => {
    it('should return unknown types as strings', () => {
      const unknownOid = 99999;
      expect(parseColumnValue('some value', unknownOid)).toBe('some value');
    });
  });
});

describe('Helper functions', () => {
  describe('isNumericType', () => {
    it('should return true for numeric types', () => {
      expect(isNumericType(PgTypeOid.INT2)).toBe(true);
      expect(isNumericType(PgTypeOid.INT4)).toBe(true);
      expect(isNumericType(PgTypeOid.INT8)).toBe(true);
      expect(isNumericType(PgTypeOid.FLOAT4)).toBe(true);
      expect(isNumericType(PgTypeOid.FLOAT8)).toBe(true);
      expect(isNumericType(PgTypeOid.NUMERIC)).toBe(true);
    });

    it('should return false for non-numeric types', () => {
      expect(isNumericType(PgTypeOid.TEXT)).toBe(false);
      expect(isNumericType(PgTypeOid.BOOLEAN)).toBe(false);
      expect(isNumericType(PgTypeOid.DATE)).toBe(false);
    });
  });

  describe('isArrayType', () => {
    it('should return true for array types', () => {
      expect(isArrayType(PgTypeOid.INT4_ARRAY)).toBe(true);
      expect(isArrayType(PgTypeOid.TEXT_ARRAY)).toBe(true);
      expect(isArrayType(PgTypeOid.UUID_ARRAY)).toBe(true);
    });

    it('should return false for non-array types', () => {
      expect(isArrayType(PgTypeOid.INT4)).toBe(false);
      expect(isArrayType(PgTypeOid.TEXT)).toBe(false);
    });
  });

  describe('getArrayElementType', () => {
    it('should return element type for array types', () => {
      expect(getArrayElementType(PgTypeOid.INT4_ARRAY)).toBe(PgTypeOid.INT4);
      expect(getArrayElementType(PgTypeOid.TEXT_ARRAY)).toBe(PgTypeOid.TEXT);
      expect(getArrayElementType(PgTypeOid.UUID_ARRAY)).toBe(PgTypeOid.UUID);
    });

    it('should return undefined for non-array types', () => {
      expect(getArrayElementType(PgTypeOid.INT4)).toBeUndefined();
      expect(getArrayElementType(PgTypeOid.TEXT)).toBeUndefined();
    });
  });
});
