/**
 * Type Conversion Utilities
 * Converts PostgreSQL wire protocol values to JavaScript types
 */

// Common PostgreSQL type OIDs
export const PG_TYPE_OIDS = {
  INT4: 23,
  INT8: 20,
  FLOAT8: 701,
  BOOLEAN: 16,
  TEXT: 25,
  VARCHAR: 1043,
  TIMESTAMP: 1114,
  TIMESTAMPTZ: 1184,
} as const;

/**
 * Parse a string value from the wire protocol into the appropriate JavaScript type
 * based on the PostgreSQL type OID
 */
export function parseColumnValue(value: string, typeOid: number): any {
  switch (typeOid) {
    case PG_TYPE_OIDS.INT4:
    case PG_TYPE_OIDS.INT8:
      return parseInt(value, 10);
    case PG_TYPE_OIDS.FLOAT8:
      return parseFloat(value);
    case PG_TYPE_OIDS.BOOLEAN:
      return value === 't';
    case PG_TYPE_OIDS.TIMESTAMP:
    case PG_TYPE_OIDS.TIMESTAMPTZ:
      return new Date(value);
    case PG_TYPE_OIDS.TEXT:
    case PG_TYPE_OIDS.VARCHAR:
    default:
      return value;
  }
}
