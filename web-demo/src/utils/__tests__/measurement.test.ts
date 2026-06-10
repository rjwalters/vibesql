import { describe, it, expect } from 'vitest'
import { formatTime, formatMeasurement, formatBytes, formatMemory, formatTps } from '../measurement'

describe('formatMeasurement', () => {
  it('should return null for negative values', () => {
    expect(formatMeasurement(-1)).toBeNull()
  })

  it('should return null for NaN', () => {
    expect(formatMeasurement(NaN)).toBeNull()
  })

  it('should format milliseconds correctly', () => {
    // 9.03ms with 0.094ms stddev
    const result = formatMeasurement(0.00903277215333333, 9.444017288855609e-5)
    expect(result).not.toBeNull()
    expect(result!.unit).toBe('ms')
    // Should use precision based on stddev (2 sig figs of stddev)
    expect(result!.combined).toMatch(/ms$/)
  })

  it('should format microseconds correctly', () => {
    // 888µs with 11.8µs stddev
    const result = formatMeasurement(0.0008882716580273318, 1.1825102506590448e-5)
    expect(result).not.toBeNull()
    expect(result!.unit).toBe('µs')
  })

  it('should format seconds correctly', () => {
    const result = formatMeasurement(1.5, 0.05)
    expect(result).not.toBeNull()
    expect(result!.unit).toBe('s')
  })

  it('should include uncertainty when showUncertainty is true', () => {
    const result = formatMeasurement(0.01, 0.001, { showUncertainty: true })
    expect(result).not.toBeNull()
    expect(result!.combined).toContain('±')
  })

  it('should use parenthetical format when specified', () => {
    const result = formatMeasurement(0.01234, 0.00056, {
      showUncertainty: true,
      format: 'paren',
    })
    expect(result).not.toBeNull()
    expect(result!.combined).toMatch(/\(\d+\)/)
  })

  it('should use default precision when no stddev provided', () => {
    const result = formatMeasurement(0.01234)
    expect(result).not.toBeNull()
    // Default precision is 2 decimal places
    expect(result!.combined).toBe('12.34 ms')
  })
})

describe('formatTime', () => {
  it('should return null for negative values', () => {
    expect(formatTime(-1)).toBeNull()
  })

  it('should return formatted string for valid values', () => {
    const result = formatTime(0.01)
    expect(result).toBe('10.00 ms')
  })

  it('should pass stddev to underlying function', () => {
    // When stddev is provided, precision should be based on it
    const result = formatTime(0.01234, 0.00056)
    expect(result).not.toBeNull()
    // Should not be exactly "12.34 ms" since stddev affects precision
    expect(result).toMatch(/ms$/)
  })

  it('should show uncertainty when requested', () => {
    const result = formatTime(0.01, 0.001, { showUncertainty: true })
    expect(result).toContain('±')
  })
})

describe('formatBytes', () => {
  it('should format bytes', () => {
    expect(formatBytes(500)).toBe('500 B')
  })

  it('should format kilobytes', () => {
    expect(formatBytes(2048)).toBe('2.0 KB')
  })

  it('should format megabytes', () => {
    expect(formatBytes(1024 * 1024 * 2)).toBe('2.00 MB')
  })

  it('should format gigabytes', () => {
    expect(formatBytes(1024 * 1024 * 1024 * 1.5)).toBe('1.50 GB')
  })
})

describe('formatMemory', () => {
  it('should format KB', () => {
    expect(formatMemory(512)).toBe('512 KB')
  })

  it('should format MB', () => {
    expect(formatMemory(2048)).toBe('2.0 MB')
  })

  it('should format GB', () => {
    expect(formatMemory(1024 * 1024 * 1.5)).toBe('1.50 GB')
  })
})

describe('formatTps', () => {
  it('should format small TPS', () => {
    expect(formatTps(500)).toBe('500.00 TPS')
  })

  it('should format K TPS', () => {
    expect(formatTps(5000)).toBe('5.00K TPS')
  })

  it('should format M TPS', () => {
    expect(formatTps(5000000)).toBe('5.00M TPS')
  })
})

describe('real benchmark data formatting', () => {
  // Test with actual data from benchmark_results.json
  const testCases = [
    {
      name: 'Q1 VibeSQL',
      mean: 0.00903277215333333,
      stddev: 9.444017288855609e-5,
      expectedUnit: 'ms',
    },
    {
      name: 'Q1 SQLite',
      mean: 0.032574188865,
      stddev: 0.0006630421339375958,
      expectedUnit: 'ms',
    },
    {
      name: 'Q2 SQLite',
      mean: 0.0008882716580273318,
      stddev: 1.1825102506590448e-5,
      expectedUnit: 'µs',
    },
    {
      name: 'Q5 VibeSQL',
      mean: 0.052424130369999995,
      stddev: 0.0022248999399577165,
      expectedUnit: 'ms',
    },
  ]

  for (const tc of testCases) {
    it(`should format ${tc.name} with correct unit`, () => {
      const result = formatMeasurement(tc.mean, tc.stddev)
      expect(result).not.toBeNull()
      expect(result!.unit).toBe(tc.expectedUnit)
    })

    it(`should format ${tc.name} with stddev-based precision`, () => {
      const result = formatMeasurement(tc.mean, tc.stddev)
      expect(result).not.toBeNull()
      // The value should have limited precision based on stddev
      const numericStr = result!.value.replace(/ \w+$/, '')
      const decimalPlaces = numericStr.includes('.') ? numericStr.split('.')[1].length : 0
      // With 2 sig figs in stddev, we should have reasonable precision
      expect(decimalPlaces).toBeLessThanOrEqual(4)
    })
  }
})
