/**
 * Measurement formatting utility with proper significant figures and uncertainty display.
 *
 * When displaying measurements with uncertainty (mean ± stddev), the uncertainty
 * determines the precision of the displayed value. This follows standard scientific
 * practice where:
 *
 * 1. Round the uncertainty to 1-2 significant figures
 * 2. Round the value to match the precision of the uncertainty
 * 3. Display both with the same unit
 *
 * Examples:
 *   formatMeasurement(0.01234, 0.00056) => "12.34 ± 0.56 ms"
 *   formatMeasurement(0.01234, 0.00056, { showUncertainty: false }) => "12.3 ms"
 *   formatMeasurement(0.01234) => "12.34 ms" (no stddev, uses default precision)
 */

export interface MeasurementOptions {
  /** Whether to display ± uncertainty notation (default: true if stddev provided) */
  showUncertainty?: boolean;
  /** Number of significant figures for uncertainty: 1 or 2 (default: 2) */
  uncertaintyDigits?: 1 | 2;
  /** Format style: 'plusminus' for "12.3 ± 0.4", 'paren' for "12.3(4)" */
  format?: 'plusminus' | 'paren';
  /** Default decimal places when no stddev is provided (default: 2) */
  defaultPrecision?: number;
}

export interface FormattedMeasurement {
  /** Formatted value with unit (e.g., "12.34 ms") */
  value: string;
  /** Formatted uncertainty with unit, if available (e.g., "0.56 ms") */
  uncertainty?: string;
  /** Combined display string (e.g., "12.34 ± 0.56 ms") */
  combined: string;
  /** The unit used (e.g., "ms", "µs", "s") */
  unit: string;
  /** Raw numeric value in the display unit */
  numericValue: number;
  /** Raw numeric uncertainty in the display unit, if available */
  numericUncertainty?: number;
}

/** Time unit thresholds and multipliers */
interface TimeUnit {
  name: string;
  threshold: number; // Values >= this (in seconds) use this unit
  multiplier: number; // Multiply seconds by this to get unit value
}

const TIME_UNITS: TimeUnit[] = [
  { name: 's', threshold: 1, multiplier: 1 },
  { name: 'ms', threshold: 0.001, multiplier: 1000 },
  { name: 'µs', threshold: 0.000001, multiplier: 1_000_000 },
  { name: 'ns', threshold: 0, multiplier: 1_000_000_000 },
];

/**
 * Select the appropriate time unit for a value in seconds.
 */
function selectTimeUnit(seconds: number): TimeUnit {
  const absValue = Math.abs(seconds);
  for (const unit of TIME_UNITS) {
    if (absValue >= unit.threshold) {
      return unit;
    }
  }
  return TIME_UNITS[TIME_UNITS.length - 1]; // ns for very small values
}

/**
 * Round a number to a specified number of significant figures.
 */
function roundToSigFigs(value: number, sigFigs: number): number {
  if (value === 0) return 0;
  const magnitude = Math.floor(Math.log10(Math.abs(value)));
  const scale = Math.pow(10, sigFigs - magnitude - 1);
  return Math.round(value * scale) / scale;
}

/**
 * Get the position of the least significant digit (as power of 10).
 * E.g., 0.056 has LSD at 10^-3, so returns -3.
 */
function getLSDPosition(value: number, sigFigs: number): number {
  if (value === 0) return 0;
  const magnitude = Math.floor(Math.log10(Math.abs(value)));
  return magnitude - sigFigs + 1;
}

/**
 * Format a time measurement with proper significant figures based on uncertainty.
 *
 * @param seconds - The mean value in seconds
 * @param stddev - Optional standard deviation in seconds
 * @param options - Formatting options
 * @returns Formatted measurement object
 */
export function formatMeasurement(
  seconds: number,
  stddev?: number,
  options: MeasurementOptions = {}
): FormattedMeasurement | null {
  // Handle invalid/failed measurements
  if (seconds < 0 || !Number.isFinite(seconds)) {
    return null;
  }

  const {
    showUncertainty = false,  // Default to NOT showing ± notation for cleaner display
    uncertaintyDigits = 2,
    format = 'plusminus',
    defaultPrecision = 2,
  } = options;

  // Select unit based on the mean value
  const unit = selectTimeUnit(seconds);
  const scaledValue = seconds * unit.multiplier;
  const scaledStddev = stddev !== undefined ? stddev * unit.multiplier : undefined;

  let decimalPlaces: number;
  let roundedValue: number;
  let roundedStddev: number | undefined;

  if (scaledStddev !== undefined && scaledStddev > 0 && showUncertainty) {
    // Round stddev to specified significant figures
    roundedStddev = roundToSigFigs(scaledStddev, uncertaintyDigits);

    // Determine decimal places from the rounded stddev
    const lsdPosition = getLSDPosition(roundedStddev, uncertaintyDigits);
    decimalPlaces = Math.max(0, -lsdPosition);

    // Round value to match stddev precision
    const roundingFactor = Math.pow(10, decimalPlaces);
    roundedValue = Math.round(scaledValue * roundingFactor) / roundingFactor;
  } else if (scaledStddev !== undefined && scaledStddev > 0) {
    // Have stddev but not showing it - still use it to determine precision
    const roundedStddevForPrecision = roundToSigFigs(scaledStddev, uncertaintyDigits);
    const lsdPosition = getLSDPosition(roundedStddevForPrecision, uncertaintyDigits);
    decimalPlaces = Math.max(0, -lsdPosition);

    const roundingFactor = Math.pow(10, decimalPlaces);
    roundedValue = Math.round(scaledValue * roundingFactor) / roundingFactor;
  } else {
    // No stddev - use default precision
    decimalPlaces = defaultPrecision;
    roundedValue = parseFloat(scaledValue.toFixed(decimalPlaces));
  }

  // Format the strings
  const valueStr = roundedValue.toFixed(decimalPlaces);
  const uncertaintyStr = roundedStddev !== undefined
    ? roundedStddev.toFixed(decimalPlaces)
    : undefined;

  // Build combined string
  let combined: string;
  if (showUncertainty && uncertaintyStr !== undefined) {
    if (format === 'paren') {
      // Parenthetical notation: extract just the uncertain digits
      // e.g., "12.34(56)" where 56 represents ±0.56
      const uncertainDigits = uncertaintyStr.replace(/^0*\.?0*/, '');
      combined = `${valueStr}(${uncertainDigits}) ${unit.name}`;
    } else {
      combined = `${valueStr} ± ${uncertaintyStr} ${unit.name}`;
    }
  } else {
    combined = `${valueStr} ${unit.name}`;
  }

  return {
    value: `${valueStr} ${unit.name}`,
    uncertainty: uncertaintyStr !== undefined ? `${uncertaintyStr} ${unit.name}` : undefined,
    combined,
    unit: unit.name,
    numericValue: roundedValue,
    numericUncertainty: roundedStddev,
  };
}

/**
 * Format a time measurement as a simple string (backward compatible).
 *
 * @param seconds - The mean value in seconds
 * @param stddev - Optional standard deviation in seconds
 * @param options - Formatting options
 * @returns Formatted string or null for invalid values
 */
export function formatTime(
  seconds: number,
  stddev?: number,
  options: MeasurementOptions = {}
): string | null {
  const result = formatMeasurement(seconds, stddev, options);
  return result?.combined ?? null;
}

/**
 * Format a size in bytes with appropriate units.
 */
export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

/**
 * Format memory in KB with appropriate units.
 */
export function formatMemory(kb: number): string {
  if (kb < 1024) return `${kb.toFixed(0)} KB`;
  if (kb < 1024 * 1024) return `${(kb / 1024).toFixed(1)} MB`;
  return `${(kb / (1024 * 1024)).toFixed(2)} GB`;
}

/**
 * Format transactions per second with appropriate units.
 */
export function formatTps(tps: number): string {
  if (tps >= 1_000_000) return `${(tps / 1_000_000).toFixed(2)}M TPS`;
  if (tps >= 1_000) return `${(tps / 1_000).toFixed(2)}K TPS`;
  return `${tps.toFixed(2)} TPS`;
}

/**
 * Format a speedup ratio with appropriate precision.
 * Uses the uncertainty in both measurements to determine display precision.
 */
export function formatSpeedup(
  ratio: number,
  options: { precision?: number } = {}
): string {
  const { precision = 2 } = options;
  return `${ratio.toFixed(precision)}x`;
}
