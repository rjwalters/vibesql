/**
 * Benchmark statistics utility
 *
 * Loads and provides access to benchmark data for use in translations
 * and dynamic content rendering.
 */

interface TPCCResult {
  benchmarks: Array<{
    name: string
    stats: {
      tps: number
      mean: number
    }
  }>
}

interface FootprintResult {
  benchmarks: Array<{
    database: string
    binary_size_bytes: number
    startup_time_ms: number
    wasm_size_gzip_bytes?: number | null
  }>
}

interface SysbenchResult {
  benchmarks: Array<{
    name: string
    stats: {
      mean: number
    }
  }>
}

interface DashboardData {
  summary: {
    conformance: {
      tests_passing: number
      tests_total: number
    }
  }
  conformance: {
    files: {
      total: number
    }
  }
  benchmarks: {
    tpcc: {
      latest: {
        transactions: {
          mixed: {
            vibesql: { tps: number }
            sqlite: { tps: number }
            duckdb: { tps: number }
          }
        }
      }
    }
  }
}

export interface DynamicBenchmarkStats {
  // TPC-C
  tpccVibesqlTps: number
  tpccSqliteTps: number
  tpccDuckdbTps: number
  tpccVibesqlVsSqlite: string
  tpccVibesqlVsDuckdb: string
  tpccSqliteVsDuckdb: string

  // Footprint
  vibesqlBinaryMb: string
  sqliteBinaryMb: string
  duckdbBinaryMb: string
  vibesqlStartupMs: string
  sqliteStartupMs: string
  duckdbStartupMs: string
  wasmSizeGzipMb: string

  // Conformance
  testCasesTotal: string
  testFilesTotal: number

  // Sysbench Embedded
  sysbenchPointVibesqlUs: string
  sysbenchPointSqliteUs: string
  sysbenchPointRatio: string
  sysbenchIndexVibesqlUs: string
  sysbenchIndexSqliteUs: string
  sysbenchIndexRatio: string
  sysbenchNonIndexVibesqlUs: string
  sysbenchNonIndexSqliteUs: string
  sysbenchDeleteVibesqlUs: string
  sysbenchDeleteSqliteUs: string
}

// Cached stats
let cachedStats: DynamicBenchmarkStats | null = null

/**
 * Format a number with thousand separators
 */
function formatNumber(n: number): string {
  return n.toLocaleString('en-US')
}

/**
 * Format bytes to MB with one decimal
 */
function formatMb(bytes: number): string {
  return (bytes / (1024 * 1024)).toFixed(1)
}

/**
 * Calculate speedup ratio and format as string
 */
function formatSpeedup(faster: number, slower: number): string {
  const ratio = faster / slower
  if (ratio >= 10) {
    return `~${Math.round(ratio)}x`
  }
  return `${ratio.toFixed(1)}x`
}

/**
 * Format seconds to microseconds with appropriate precision
 */
function formatUs(seconds: number): string {
  const us = seconds * 1_000_000
  if (us < 1) {
    return us.toFixed(2)
  } else if (us < 10) {
    return us.toFixed(1)
  }
  return us.toFixed(0)
}

/**
 * Load benchmark stats from JSON files
 */
export async function loadBenchmarkStats(): Promise<DynamicBenchmarkStats> {
  if (cachedStats) {
    return cachedStats
  }

  try {
    // Load all data sources in parallel
    const [tpccRes, footprintRes, dashboardRes, sysbenchRes] = await Promise.all([
      fetch('/benchmarks/tpcc_results.json'),
      fetch('/benchmarks/footprint_results.json'),
      fetch('/data/dashboard.json'),
      fetch('/benchmarks/sysbench_results.json'),
    ])

    const tpcc: TPCCResult = await tpccRes.json()
    const footprint: FootprintResult = await footprintRes.json()
    const dashboard: DashboardData = await dashboardRes.json()
    const sysbench: SysbenchResult = await sysbenchRes.json()

    // Extract TPC-C stats
    const vibesqlTpcc = tpcc.benchmarks.find(b => b.name.includes('vibesql'))
    const sqliteTpcc = tpcc.benchmarks.find(b => b.name.includes('sqlite'))
    const duckdbTpcc = tpcc.benchmarks.find(b => b.name.includes('duckdb'))

    const tpccVibesqlTps = vibesqlTpcc?.stats.tps ?? 0
    const tpccSqliteTps = sqliteTpcc?.stats.tps ?? 0
    const tpccDuckdbTps = duckdbTpcc?.stats.tps ?? 0

    // Extract footprint stats
    const vibesqlFp = footprint.benchmarks.find(b => b.database === 'vibesql')
    const sqliteFp = footprint.benchmarks.find(b => b.database === 'sqlite')
    const duckdbFp = footprint.benchmarks.find(b => b.database === 'duckdb')

    // Extract sysbench embedded stats (non-server benchmarks)
    const getSysbenchMean = (suffix: string): number => {
      const b = sysbench.benchmarks.find(b => b.name === `sysbench_${suffix}`)
      return b?.stats.mean ?? 0
    }
    const pointVibesql = getSysbenchMean('point_select_vibesql')
    const pointSqlite = getSysbenchMean('point_select_sqlite')
    const indexVibesql = getSysbenchMean('update_index_vibesql')
    const indexSqlite = getSysbenchMean('update_index_sqlite')
    const nonIndexVibesql = getSysbenchMean('update_non_index_vibesql')
    const nonIndexSqlite = getSysbenchMean('update_non_index_sqlite')
    const deleteVibesql = getSysbenchMean('delete_vibesql')
    const deleteSqlite = getSysbenchMean('delete_sqlite')

    cachedStats = {
      // TPC-C
      tpccVibesqlTps: Math.round(tpccVibesqlTps),
      tpccSqliteTps: Math.round(tpccSqliteTps),
      tpccDuckdbTps: Math.round(tpccDuckdbTps),
      tpccVibesqlVsSqlite: formatSpeedup(tpccVibesqlTps, tpccSqliteTps),
      tpccVibesqlVsDuckdb: formatSpeedup(tpccVibesqlTps, tpccDuckdbTps),
      tpccSqliteVsDuckdb: formatSpeedup(tpccSqliteTps, tpccDuckdbTps),

      // Footprint
      vibesqlBinaryMb: formatMb(vibesqlFp?.binary_size_bytes ?? 0),
      sqliteBinaryMb: formatMb(sqliteFp?.binary_size_bytes ?? 0),
      duckdbBinaryMb: formatMb(duckdbFp?.binary_size_bytes ?? 0),
      vibesqlStartupMs: (vibesqlFp?.startup_time_ms ?? 0).toFixed(0),
      sqliteStartupMs: (sqliteFp?.startup_time_ms ?? 0).toFixed(0),
      duckdbStartupMs: (duckdbFp?.startup_time_ms ?? 0).toFixed(0),
      wasmSizeGzipMb: formatMb(vibesqlFp?.wasm_size_gzip_bytes ?? 0),

      // Conformance
      testCasesTotal: formatNumber(dashboard.summary.conformance.tests_total),
      testFilesTotal: dashboard.conformance.files.total,

      // Sysbench Embedded
      sysbenchPointVibesqlUs: formatUs(pointVibesql),
      sysbenchPointSqliteUs: formatUs(pointSqlite),
      sysbenchPointRatio: pointSqlite > 0 ? formatSpeedup(pointVibesql, pointSqlite) : 'N/A',
      sysbenchIndexVibesqlUs: formatUs(indexVibesql),
      sysbenchIndexSqliteUs: formatUs(indexSqlite),
      sysbenchIndexRatio: indexSqlite > 0 ? formatSpeedup(indexVibesql, indexSqlite) : 'N/A',
      sysbenchNonIndexVibesqlUs: formatUs(nonIndexVibesql),
      sysbenchNonIndexSqliteUs: formatUs(nonIndexSqlite),
      sysbenchDeleteVibesqlUs: formatUs(deleteVibesql),
      sysbenchDeleteSqliteUs: formatUs(deleteSqlite),
    }

    return cachedStats
  } catch (error) {
    console.error('Failed to load benchmark stats:', error)
    // Return defaults if loading fails
    return getDefaultStats()
  }
}

/**
 * Get cached stats synchronously (returns defaults if not loaded)
 */
export function getBenchmarkStats(): DynamicBenchmarkStats {
  return cachedStats ?? getDefaultStats()
}

/**
 * Default stats (used before data is loaded or on error)
 */
function getDefaultStats(): DynamicBenchmarkStats {
  return {
    tpccVibesqlTps: 12769,
    tpccSqliteTps: 3168,
    tpccDuckdbTps: 382,
    tpccVibesqlVsSqlite: '4.0x',
    tpccVibesqlVsDuckdb: '~33x',
    tpccSqliteVsDuckdb: '~8x',
    vibesqlBinaryMb: '19.5',
    sqliteBinaryMb: '4.5',
    duckdbBinaryMb: '42.5',
    vibesqlStartupMs: '20',
    sqliteStartupMs: '8',
    duckdbStartupMs: '19',
    wasmSizeGzipMb: '1.5',
    testCasesTotal: '7,420,713',
    testFilesTotal: 622,
    // Sysbench Embedded defaults
    sysbenchPointVibesqlUs: '2.2',
    sysbenchPointSqliteUs: '0.33',
    sysbenchPointRatio: '6.6x',
    sysbenchIndexVibesqlUs: '5.3',
    sysbenchIndexSqliteUs: '1.5',
    sysbenchIndexRatio: '3.5x',
    sysbenchNonIndexVibesqlUs: '2.3',
    sysbenchNonIndexSqliteUs: '1.2',
    sysbenchDeleteVibesqlUs: '3.7',
    sysbenchDeleteSqliteUs: '1.4',
  }
}

/**
 * Clear cached stats (useful for testing or forcing reload)
 */
export function clearBenchmarkStatsCache(): void {
  cachedStats = null
}
