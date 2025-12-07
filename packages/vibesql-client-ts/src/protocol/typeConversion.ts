/**
 * PostgreSQL Type Conversion
 * Converts PostgreSQL wire protocol values to JavaScript types
 */

/**
 * PostgreSQL type OIDs
 * @see https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
 */
export const PgTypeOid = {
  // Boolean
  BOOLEAN: 16,

  // Binary
  BYTEA: 17,

  // Numeric types
  INT2: 21,
  INT4: 23,
  INT8: 20,
  FLOAT4: 700,
  FLOAT8: 701,
  NUMERIC: 1700,

  // Text types
  TEXT: 25,
  VARCHAR: 1043,
  CHAR: 18,
  BPCHAR: 1042, // blank-padded char
  NAME: 19,

  // Date/Time types
  DATE: 1082,
  TIME: 1083,
  TIMETZ: 1266,
  TIMESTAMP: 1114,
  TIMESTAMPTZ: 1184,
  INTERVAL: 1186,

  // JSON types
  JSON: 114,
  JSONB: 3802,

  // UUID
  UUID: 2950,

  // Array types (common ones)
  BOOLEAN_ARRAY: 1000,
  BYTEA_ARRAY: 1001,
  INT2_ARRAY: 1005,
  INT4_ARRAY: 1007,
  INT8_ARRAY: 1016,
  FLOAT4_ARRAY: 1021,
  FLOAT8_ARRAY: 1022,
  NUMERIC_ARRAY: 1231,
  TEXT_ARRAY: 1009,
  VARCHAR_ARRAY: 1015,
  DATE_ARRAY: 1182,
  TIME_ARRAY: 1183,
  TIMESTAMP_ARRAY: 1115,
  TIMESTAMPTZ_ARRAY: 1185,
  UUID_ARRAY: 2951,
  JSON_ARRAY: 199,
  JSONB_ARRAY: 3807,
} as const;

/**
 * Set of numeric type OIDs for quick lookup
 */
const NUMERIC_TYPES: Set<number> = new Set([
  PgTypeOid.INT2,
  PgTypeOid.INT4,
  PgTypeOid.INT8,
  PgTypeOid.FLOAT4,
  PgTypeOid.FLOAT8,
  PgTypeOid.NUMERIC,
]);

/**
 * Set of array type OIDs and their element type OIDs
 */
const ARRAY_TYPE_TO_ELEMENT: Map<number, number> = new Map([
  [PgTypeOid.BOOLEAN_ARRAY, PgTypeOid.BOOLEAN],
  [PgTypeOid.BYTEA_ARRAY, PgTypeOid.BYTEA],
  [PgTypeOid.INT2_ARRAY, PgTypeOid.INT2],
  [PgTypeOid.INT4_ARRAY, PgTypeOid.INT4],
  [PgTypeOid.INT8_ARRAY, PgTypeOid.INT8],
  [PgTypeOid.FLOAT4_ARRAY, PgTypeOid.FLOAT4],
  [PgTypeOid.FLOAT8_ARRAY, PgTypeOid.FLOAT8],
  [PgTypeOid.NUMERIC_ARRAY, PgTypeOid.NUMERIC],
  [PgTypeOid.TEXT_ARRAY, PgTypeOid.TEXT],
  [PgTypeOid.VARCHAR_ARRAY, PgTypeOid.VARCHAR],
  [PgTypeOid.DATE_ARRAY, PgTypeOid.DATE],
  [PgTypeOid.TIME_ARRAY, PgTypeOid.TIME],
  [PgTypeOid.TIMESTAMP_ARRAY, PgTypeOid.TIMESTAMP],
  [PgTypeOid.TIMESTAMPTZ_ARRAY, PgTypeOid.TIMESTAMPTZ],
  [PgTypeOid.UUID_ARRAY, PgTypeOid.UUID],
  [PgTypeOid.JSON_ARRAY, PgTypeOid.JSON],
  [PgTypeOid.JSONB_ARRAY, PgTypeOid.JSONB],
]);

/**
 * Parse a PostgreSQL array literal into a JavaScript array
 * PostgreSQL array format: {elem1,elem2,elem3} or {"elem 1","elem 2"}
 */
function parseArrayLiteral(
  value: string,
  elementTypeOid: number
): (unknown | null)[] {
  // Handle empty array
  if (value === '{}') {
    return [];
  }

  // Remove outer braces
  const inner = value.slice(1, -1);

  const elements: (unknown | null)[] = [];
  let current = '';
  let inQuotes = false;
  let escaped = false;

  for (let i = 0; i < inner.length; i++) {
    const char = inner[i];

    if (escaped) {
      current += char;
      escaped = false;
      continue;
    }

    if (char === '\\') {
      escaped = true;
      continue;
    }

    if (char === '"') {
      inQuotes = !inQuotes;
      continue;
    }

    if (char === ',' && !inQuotes) {
      elements.push(parseArrayElement(current, elementTypeOid));
      current = '';
      continue;
    }

    current += char;
  }

  // Push the last element
  if (current.length > 0 || elements.length > 0) {
    elements.push(parseArrayElement(current, elementTypeOid));
  }

  return elements;
}

/**
 * Parse a single array element
 */
function parseArrayElement(
  value: string,
  elementTypeOid: number
): unknown | null {
  // PostgreSQL represents NULL as unquoted "NULL" in arrays
  if (value === 'NULL') {
    return null;
  }

  return parseColumnValue(value, elementTypeOid);
}

/**
 * Parse a PostgreSQL bytea hex format value
 * Format: \xHEXDIGITS
 */
function parseBytea(value: string): Uint8Array {
  // PostgreSQL sends bytea in hex format: \xDEADBEEF
  if (value.startsWith('\\x')) {
    const hex = value.slice(2);
    const bytes = new Uint8Array(hex.length / 2);
    for (let i = 0; i < hex.length; i += 2) {
      bytes[i / 2] = parseInt(hex.slice(i, i + 2), 16);
    }
    return bytes;
  }

  // Fallback: escape format (older PostgreSQL)
  // This is a simplified implementation
  const bytes: number[] = [];
  for (let i = 0; i < value.length; i++) {
    if (value[i] === '\\' && i + 3 < value.length) {
      // Octal escape \NNN
      const octal = value.slice(i + 1, i + 4);
      if (/^[0-7]{3}$/.test(octal)) {
        bytes.push(parseInt(octal, 8));
        i += 3;
        continue;
      }
      // Escaped backslash \\
      if (value[i + 1] === '\\') {
        bytes.push(92); // backslash
        i += 1;
        continue;
      }
    }
    bytes.push(value.charCodeAt(i));
  }
  return new Uint8Array(bytes);
}

/**
 * Parse a PostgreSQL interval value
 * Returns an object with the interval components
 */
export interface PgInterval {
  years?: number;
  months?: number;
  days?: number;
  hours?: number;
  minutes?: number;
  seconds?: number;
}

function parseInterval(value: string): PgInterval {
  const result: PgInterval = {};

  // PostgreSQL interval format examples:
  // "1 year 2 mons 3 days 04:05:06"
  // "00:00:00"
  // "1 day"
  // "@ 1 year 2 mons -3 days 4 hours 5 mins 6 secs"

  // Remove leading @ if present (verbose format)
  let str = value.replace(/^@\s*/, '');

  // Parse years
  const yearsMatch = str.match(/(-?\d+)\s+years?/i);
  if (yearsMatch) {
    result.years = parseInt(yearsMatch[1], 10);
  }

  // Parse months
  const monthsMatch = str.match(/(-?\d+)\s+(?:mons?|months?)/i);
  if (monthsMatch) {
    result.months = parseInt(monthsMatch[1], 10);
  }

  // Parse days
  const daysMatch = str.match(/(-?\d+)\s+days?/i);
  if (daysMatch) {
    result.days = parseInt(daysMatch[1], 10);
  }

  // Parse time portion (HH:MM:SS or HH:MM:SS.microseconds)
  const timeMatch = str.match(/(-?)(\d+):(\d+):(\d+(?:\.\d+)?)/);
  if (timeMatch) {
    const sign = timeMatch[1] === '-' ? -1 : 1;
    result.hours = sign * parseInt(timeMatch[2], 10);
    result.minutes = sign * parseInt(timeMatch[3], 10);
    result.seconds = sign * parseFloat(timeMatch[4]);
  } else {
    // Check for individual time components (verbose format)
    const hoursMatch = str.match(/(-?\d+)\s+hours?/i);
    if (hoursMatch) {
      result.hours = parseInt(hoursMatch[1], 10);
    }

    const minsMatch = str.match(/(-?\d+)\s+(?:mins?|minutes?)/i);
    if (minsMatch) {
      result.minutes = parseInt(minsMatch[1], 10);
    }

    const secsMatch = str.match(/(-?[\d.]+)\s+(?:secs?|seconds?)/i);
    if (secsMatch) {
      result.seconds = parseFloat(secsMatch[1]);
    }
  }

  return result;
}

/**
 * Parse column value based on PostgreSQL data type OID
 *
 * @param value - The string value from the wire protocol
 * @param typeOid - The PostgreSQL type OID
 * @returns The converted JavaScript value
 */
export function parseColumnValue(value: string, typeOid: number): unknown {
  // Check for array types first
  const elementTypeOid = ARRAY_TYPE_TO_ELEMENT.get(typeOid);
  if (elementTypeOid !== undefined) {
    return parseArrayLiteral(value, elementTypeOid);
  }

  switch (typeOid) {
    // Boolean
    case PgTypeOid.BOOLEAN:
      return value === 't' || value === 'true' || value === '1';

    // Binary
    case PgTypeOid.BYTEA:
      return parseBytea(value);

    // Integer types
    case PgTypeOid.INT2:
    case PgTypeOid.INT4:
      return parseInt(value, 10);

    // BigInt for INT8 to preserve precision for large values
    // Note: parseInt is used for compatibility, but may lose precision for very large values
    case PgTypeOid.INT8: {
      const num = parseInt(value, 10);
      // If the number is within safe integer range, return as number
      // Otherwise, return the string to preserve precision
      if (Number.isSafeInteger(num)) {
        return num;
      }
      // For very large values, return as string to preserve precision
      // Users can convert to BigInt if needed: BigInt(value)
      return value;
    }

    // Floating point types
    case PgTypeOid.FLOAT4:
    case PgTypeOid.FLOAT8:
      return parseFloat(value);

    // NUMERIC/DECIMAL - return as string to preserve precision
    case PgTypeOid.NUMERIC:
      return value;

    // Text types
    case PgTypeOid.TEXT:
    case PgTypeOid.VARCHAR:
    case PgTypeOid.CHAR:
    case PgTypeOid.BPCHAR:
    case PgTypeOid.NAME:
      return value;

    // Date (returns Date object at midnight UTC)
    case PgTypeOid.DATE: {
      // PostgreSQL date format: YYYY-MM-DD
      const [year, month, day] = value.split('-').map((n) => parseInt(n, 10));
      return new Date(Date.UTC(year, month - 1, day));
    }

    // Time without timezone (returns string in HH:MM:SS format)
    case PgTypeOid.TIME:
      return value;

    // Time with timezone (returns string in HH:MM:SS+TZ format)
    case PgTypeOid.TIMETZ:
      return value;

    // Timestamp types
    case PgTypeOid.TIMESTAMP:
    case PgTypeOid.TIMESTAMPTZ:
      return new Date(value);

    // Interval
    case PgTypeOid.INTERVAL:
      return parseInterval(value);

    // JSON types
    case PgTypeOid.JSON:
    case PgTypeOid.JSONB:
      try {
        return JSON.parse(value);
      } catch {
        // If JSON parsing fails, return as string
        return value;
      }

    // UUID (returned as string, already in canonical format)
    case PgTypeOid.UUID:
      return value;

    // Default: return as string
    default:
      return value;
  }
}

/**
 * Check if a type OID represents a numeric type
 */
export function isNumericType(typeOid: number): boolean {
  return NUMERIC_TYPES.has(typeOid);
}

/**
 * Check if a type OID represents an array type
 */
export function isArrayType(typeOid: number): boolean {
  return ARRAY_TYPE_TO_ELEMENT.has(typeOid);
}

/**
 * Get the element type OID for an array type
 */
export function getArrayElementType(typeOid: number): number | undefined {
  return ARRAY_TYPE_TO_ELEMENT.get(typeOid);
}
