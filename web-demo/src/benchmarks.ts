/**
 * Benchmark results page
 *
 * Loads and displays performance benchmark data comparing VibeSQL to SQLite, DuckDB, and MySQL.
 * Supports multiple benchmark suites: TPC-H, TPC-C, and Sysbench.
 */

import './styles/main.css';
import { initTheme } from './theme';
import { NavigationComponent } from './components/Navigation';

// Chart.js is loaded via CDN in benchmarks.html
declare const Chart: any;

/**
 * Benchmark suite types
 */
type BenchmarkSuite = 'tpch' | 'tpcc' | 'sysbench' | 'footprint';

/**
 * Suite configuration
 */
interface SuiteConfig {
  id: BenchmarkSuite;
  name: string;
  dataFile: string;
  opsLabel: string;
  descriptions: Record<string, string>;
  methodology: string;
}

/**
 * Suite configurations
 */
const SUITE_CONFIGS: Record<BenchmarkSuite, SuiteConfig> = {
  tpch: {
    id: 'tpch',
    name: 'TPC-H',
    dataFile: 'benchmark_results.json',
    opsLabel: 'TPC-H queries',
    descriptions: {
      'q1': 'Pricing Summary Report - Aggregate pricing with GROUP BY and ORDER BY',
      'q2': 'Minimum Cost Supplier - 3-table JOIN with ORDER BY and LIMIT',
      'q3': 'Shipping Priority - 3-table JOIN with aggregation',
      'q4': 'Order Priority Checking - Correlated EXISTS subquery',
      'q5': 'Local Supplier Volume - 6-table JOIN with complex filtering',
      'q6': 'Forecasting Revenue Change - WHERE filters with BETWEEN and SUM',
      'q7': 'Volume Shipping - 6-table JOIN with SUBSTR and date filtering',
      'q8': 'National Market Share - 7-table JOIN with CASE expressions',
      'q9': 'Product Type Profit Measure - 4-table JOIN with aggregation',
      'q10': 'Returned Item Reporting - 4-table JOIN with TOP-N LIMIT',
      'q11': 'Important Stock Identification - Subquery in HAVING clause',
      'q12': 'Shipping Modes Priority - CASE aggregation with date logic',
      'q13': 'Customer Distribution - LEFT OUTER JOIN with subquery',
      'q14': 'Promotion Effect - Conditional aggregation with CASE',
      'q15': 'Top Supplier - Nested subqueries with MAX',
      'q16': 'Parts/Supplier Relationship - NOT IN subquery with DISTINCT',
      'q17': 'Small-Quantity-Order Revenue - Correlated subquery in WHERE',
      'q18': 'Large Volume Customer - GROUP BY with HAVING',
      'q19': 'Discounted Revenue - Complex OR conditions',
      'q20': 'Potential Part Promotion - IN subquery with GROUP BY/HAVING',
      'q21': 'Suppliers Who Kept Orders Waiting - Multi-table EXISTS',
      'q22': 'Global Sales Opportunity - SUBSTR with NOT EXISTS subquery',
    },
    methodology: `
      <h3 class="text-lg font-semibold text-foreground mb-2">TPC-H Decision Support Benchmark</h3>
      <p class="text-muted mb-4">
        These benchmarks use the industry-standard <strong>TPC-H benchmark suite</strong>,
        which simulates real-world decision support workloads with complex analytical queries
        involving aggregations, joins, subqueries, and sorting.
      </p>

      <ul class="space-y-2 text-muted">
        <li><strong>Hardware:</strong> GitHub Actions runners (ubuntu-latest, 2-core CPU)</li>
        <li><strong>Benchmark Framework:</strong> Criterion.rs (Rust native benchmarking)</li>
        <li><strong>Scale Factor:</strong> SF 0.01 (~60,000 rows across 6 tables)</li>
        <li><strong>Data:</strong> Deterministic TPC-H compliant dataset</li>
        <li><strong>Databases Tested:</strong> VibeSQL, SQLite (via rusqlite), DuckDB (via duckdb-rs), MySQL 8.0 (via mysql crate)</li>
        <li><strong>Execution Mode:</strong> All databases run in-memory (no disk I/O)</li>
        <li><strong>Measurement:</strong> Native Rust API calls (no Python/FFI overhead)</li>
      </ul>

      <p class="mt-4 text-muted">
        All benchmarks measure end-to-end query execution time including parsing,
        planning, execution, and result materialization. This represents <strong>real-world
        SQL engine performance</strong> for analytical workloads. Results are automatically
        updated on every commit to the main branch.
      </p>

      <p class="mt-2 text-muted text-sm">
        <strong>Note:</strong> TPC-H queries test different aspects of SQL performance:
        simple aggregations (Q1, Q6), complex joins (Q2-Q5, Q7-Q10), subqueries (Q11-Q15),
        and advanced analytics (Q16-Q22). Hover over query names in the table above for descriptions.
      </p>
    `,
  },
  tpcc: {
    id: 'tpcc',
    name: 'TPC-C',
    dataFile: 'tpcc_results.json',
    opsLabel: 'TPC-C transactions',
    descriptions: {
      'new_order': 'New Order - Complex transaction with inventory checks and order creation',
      'payment': 'Payment - Update customer balance and warehouse/district totals',
      'order_status': 'Order Status - Read-only query for customer order history',
      'delivery': 'Delivery - Batch processing of pending orders',
      'stock_level': 'Stock Level - Count items below threshold in recent orders',
    },
    methodology: `
      <h3 class="text-lg font-semibold text-foreground mb-2">TPC-C Online Transaction Processing Benchmark</h3>
      <p class="text-muted mb-4">
        The <strong>TPC-C benchmark</strong> simulates a complete order-entry environment
        with a mix of complex transactions including order entry, payment processing,
        order status queries, delivery processing, and stock level monitoring.
      </p>

      <ul class="space-y-2 text-muted">
        <li><strong>Workload:</strong> OLTP (Online Transaction Processing)</li>
        <li><strong>Transaction Mix:</strong> 45% New Order, 43% Payment, 4% Order Status, 4% Delivery, 4% Stock Level</li>
        <li><strong>Warehouses:</strong> 1 warehouse (scaled for in-memory testing)</li>
        <li><strong>Concurrency:</strong> Single-threaded baseline measurements</li>
        <li><strong>ACID Compliance:</strong> Full transaction isolation testing</li>
      </ul>

      <p class="mt-4 text-muted">
        TPC-C measures transactions per minute (tpmC) and tests the database's ability to handle
        concurrent transactions with complex business logic. This benchmark is critical for
        evaluating <strong>transactional workload performance</strong>.
      </p>

      <p class="mt-2 text-muted text-sm">
        <strong>Note:</strong> Results show average transaction latency. Lower is better.
        TPC-C is particularly demanding for write-heavy workloads with strict consistency requirements.
      </p>
    `,
  },
  sysbench: {
    id: 'sysbench',
    name: 'Sysbench',
    dataFile: 'sysbench_results.json',
    opsLabel: 'Sysbench operations',
    descriptions: {
      'point_select': 'Point Select - Single row lookup by primary key',
      'range_select': 'Range Select - Fetch rows within a key range',
      'simple_update': 'Simple Update - Update single row by primary key',
      'index_update': 'Index Update - Update indexed column',
      'delete_insert': 'Delete/Insert - Remove and re-insert row',
      'read_write': 'Read/Write Mix - Combined read and write operations',
    },
    methodology: `
      <h3 class="text-lg font-semibold text-foreground mb-2">Sysbench Micro-Benchmarks</h3>
      <p class="text-muted mb-4">
        <strong>Sysbench</strong> provides focused micro-benchmarks that isolate specific
        database operations. These tests measure raw performance for fundamental operations
        without the complexity of full transaction workloads.
      </p>

      <ul class="space-y-2 text-muted">
        <li><strong>Workload Types:</strong> Point queries, range scans, updates, inserts, deletes</li>
        <li><strong>Table Size:</strong> 10,000 rows per table</li>
        <li><strong>Index Types:</strong> Primary key and secondary indexes</li>
        <li><strong>Operations:</strong> Single-statement operations (no multi-statement transactions)</li>
        <li><strong>Measurement:</strong> Operations per second and latency percentiles</li>
      </ul>

      <p class="mt-4 text-muted">
        Sysbench micro-benchmarks help identify <strong>bottlenecks in specific operations</strong>
        and are useful for comparing raw SQL engine performance without application-level complexity.
      </p>

      <p class="mt-2 text-muted text-sm">
        <strong>Note:</strong> Point selects and simple updates are the most common operations
        in typical web applications. Range selects test scan performance for reporting queries.
      </p>
    `,
  },
  footprint: {
    id: 'footprint',
    name: 'Footprint',
    dataFile: 'footprint_results.json',
    opsLabel: 'databases compared',
    descriptions: {
      'binary_size': 'Binary Size - Size of the compiled database binary on disk',
      'wasm_size': 'WASM Size - Size of the WebAssembly module for browser deployment',
      'startup_time': 'Startup Time - Time to cold-start and execute first query',
      'peak_memory': 'Peak Memory - Maximum resident set size during initialization',
    },
    methodology: `
      <h3 class="text-lg font-semibold text-foreground mb-2">Binary Footprint Benchmarks</h3>
      <p class="text-muted mb-4">
        <strong>Footprint benchmarks</strong> measure the resource efficiency of database binaries,
        comparing binary size, WASM bundle size, cold startup time, and peak memory usage during initialization.
      </p>

      <ul class="space-y-2 text-muted">
        <li><strong>Binary Size:</strong> Size of the compiled native binary in bytes (stripped release build)</li>
        <li><strong>WASM Size:</strong> Size of the WebAssembly module (raw and gzip-compressed)</li>
        <li><strong>Startup Time:</strong> Time from process start to first query result (CREATE TABLE, INSERT, SELECT)</li>
        <li><strong>Peak Memory:</strong> Maximum resident set size (RSS) during cold startup</li>
        <li><strong>Measurement:</strong> macOS /usr/bin/time -l for memory, perf_counter for timing</li>
        <li><strong>Runs:</strong> 5-10 iterations with standard deviation reported</li>
      </ul>

      <p class="mt-4 text-muted">
        Footprint benchmarks are critical for <strong>embedded, edge, and web deployments</strong> where
        binary size, WASM bundle size, startup latency, and memory consumption directly impact user experience and costs.
        WASM gzip sizes are particularly relevant for web delivery, as browsers automatically decompress gzip content.
      </p>

      <p class="mt-2 text-muted text-sm">
        <strong>Note:</strong> VibeSQL is designed for minimal footprint while maintaining SQL:1999 compliance.
        Smaller binaries mean faster downloads, quicker cold starts, and lower memory pressure.
      </p>
    `,
  },
};

interface BenchmarkStats {
  mean: number;
  stddev: number;
  min: number;
  max: number;
  rounds: number;
}

interface Benchmark {
  name: string;
  stats: BenchmarkStats;
}

interface BenchmarkResults {
  benchmarks: Benchmark[];
  datetime: string;
  machine_info?: {
    system?: string;
    python_version?: string;
  };
}

/**
 * Footprint benchmark interfaces (different format from TPC benchmarks)
 */
interface FootprintBenchmark {
  database: string;
  binary_path: string;
  binary_size_bytes: number;
  startup_time_ms: number;
  startup_time_stddev_ms: number;
  peak_memory_kb: number;
  peak_memory_stddev_kb: number;
  version: string;
  available: boolean;
  error: string | null;
  // WASM-specific fields (only for VibeSQL)
  wasm_size_bytes?: number | null;
  wasm_size_gzip_bytes?: number | null;
  wasm_size_brotli_bytes?: number | null;
}

interface FootprintResults {
  benchmarks: FootprintBenchmark[];
  datetime: string;
  machine_info?: {
    system?: string;
    release?: string;
    machine?: string;
    processor?: string;
    python_version?: string;
  };
  num_runs: number;
}

/**
 * Format time in appropriate units
 * Returns null for failed/timeout queries (negative values)
 */
function formatTime(seconds: number): string | null {
  if (seconds < 0) {
    return null; // Failed or timeout
  }
  if (seconds < 0.001) {
    return `${(seconds * 1_000_000).toFixed(2)} µs`;
  } else if (seconds < 1) {
    return `${(seconds * 1000).toFixed(2)} ms`;
  } else {
    return `${seconds.toFixed(2)} s`;
  }
}

/**
 * Current benchmark suite state
 */
let currentSuite: BenchmarkSuite = 'tpch';
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let currentChart: any = null;

/**
 * Parse benchmark name to extract database and operation info
 */
function parseBenchmarkName(name: string, suite: BenchmarkSuite): { operation: string; database: string; queryNum?: string; description?: string } {
  const config = SUITE_CONFIGS[suite];
  const parts = name.split('_');
  const database = parts[parts.length - 1]; // Last part is database name

  // Check if this is a TPC-H query
  if (name.startsWith('tpch_')) {
    // Extract query number (q1, q2, etc.)
    const queryNum = parts[1]; // e.g., "q1"
    const description = config.descriptions[queryNum];

    // Operation name is everything except "tpch_", query number, and database
    // e.g., "tpch_q1_pricing_summary_report_vibesql" -> "pricing_summary_report"
    const operation = parts.slice(2, -1).join('_');

    return { operation, database, queryNum, description };
  }

  // TPC-C format: "tpcc_new_order_vibesql"
  if (name.startsWith('tpcc_')) {
    const operation = parts.slice(1, -1).join('_');
    const description = config.descriptions[operation];
    return { operation, database, description };
  }

  // Sysbench format: "sysbench_point_select_vibesql"
  if (name.startsWith('sysbench_')) {
    const operation = parts.slice(1, -1).join('_');
    const description = config.descriptions[operation];
    return { operation, database, description };
  }

  // Legacy format
  const operation = parts.slice(1, -1).join('_');
  return { operation, database };
}

/**
 * Group benchmarks by operation
 */
function groupBenchmarksByOperation(benchmarks: Benchmark[], suite: BenchmarkSuite): Map<string, Map<string, Benchmark>> {
  const grouped = new Map<string, Map<string, Benchmark>>();

  for (const bench of benchmarks) {
    const { operation, database } = parseBenchmarkName(bench.name, suite);

    if (!grouped.has(operation)) {
      grouped.set(operation, new Map());
    }

    grouped.get(operation)!.set(database, bench);
  }

  return grouped;
}

/**
 * Calculate speedup factor
 */
function calculateSpeedup(vibesql: number, sqlite: number): number {
  return sqlite / vibesql;
}

/**
 * Render results table
 */
function renderResultsTable(data: BenchmarkResults, suite: BenchmarkSuite): void {
  const tbody = document.getElementById('results-tbody');
  if (!tbody) return;

  const config = SUITE_CONFIGS[suite];
  const grouped = groupBenchmarksByOperation(data.benchmarks, suite);

  tbody.innerHTML = '';

  let totalSpeedup = 0;
  let comparisonCount = 0;

  for (const [operation, databases] of grouped.entries()) {
    const vibesql = databases.get('vibesql');
    const sqlite = databases.get('sqlite');
    const duckdb = databases.get('duckdb');
    const mysql = databases.get('mysql');

    if (!vibesql && !sqlite && !duckdb && !mysql) continue;

    const row = document.createElement('tr');
    row.className = 'hover:bg-card/50 transition-colors';

    // Operation name (with tooltip)
    const opCell = document.createElement('td');
    opCell.className = 'px-4 py-3 font-medium text-foreground';

    // Get the first benchmark to extract query info
    const firstBench = vibesql || sqlite || duckdb;
    if (firstBench) {
      const parsed = parseBenchmarkName(firstBench.name, suite);
      if (parsed.queryNum && parsed.description) {
        // TPC-H query - show query number and add tooltip
        opCell.innerHTML = `
          <span class="cursor-help" title="${parsed.description}">
            ${config.name} ${parsed.queryNum.toUpperCase()}
          </span>
        `;
      } else if (parsed.description) {
        // TPC-C or Sysbench - show operation name with tooltip
        opCell.innerHTML = `
          <span class="cursor-help" title="${parsed.description}">
            ${operation.replace(/_/g, ' ').toUpperCase()}
          </span>
        `;
      } else {
        // Legacy format
        opCell.textContent = operation.replace(/_/g, ' ').toUpperCase();
      }
    } else {
      opCell.textContent = operation.replace(/_/g, ' ').toUpperCase();
    }
    row.appendChild(opCell);

    // vibesql time
    const vibesqlCell = document.createElement('td');
    vibesqlCell.className = 'px-4 py-3 text-right text-muted';
    const vibesqlTime = vibesql ? formatTime(vibesql.stats.mean) : null;
    if (vibesqlTime) {
      vibesqlCell.textContent = vibesqlTime;
    } else if (vibesql && vibesql.stats.mean < 0) {
      vibesqlCell.innerHTML = '<span class="text-red-500" title="Query failed (timeout or error)">FAILED</span>';
    } else {
      vibesqlCell.textContent = 'N/A';
    }
    row.appendChild(vibesqlCell);

    // SQLite time
    const sqliteCell = document.createElement('td');
    sqliteCell.className = 'px-4 py-3 text-right text-muted';
    const sqliteTime = sqlite ? formatTime(sqlite.stats.mean) : null;
    if (sqliteTime) {
      sqliteCell.textContent = sqliteTime;
    } else if (sqlite && sqlite.stats.mean < 0) {
      sqliteCell.innerHTML = '<span class="text-red-500" title="Query failed (timeout or error)">FAILED</span>';
    } else {
      sqliteCell.textContent = 'N/A';
    }
    row.appendChild(sqliteCell);

    // DuckDB time
    const duckdbCell = document.createElement('td');
    duckdbCell.className = 'px-4 py-3 text-right text-muted';
    const duckdbTime = duckdb ? formatTime(duckdb.stats.mean) : null;
    if (duckdbTime) {
      duckdbCell.textContent = duckdbTime;
    } else if (duckdb && duckdb.stats.mean < 0) {
      duckdbCell.innerHTML = '<span class="text-red-500" title="Query failed (timeout or error)">FAILED</span>';
    } else {
      duckdbCell.textContent = 'N/A';
    }
    row.appendChild(duckdbCell);

    // MySQL time
    const mysqlCell = document.createElement('td');
    mysqlCell.className = 'px-4 py-3 text-right text-muted';
    const mysqlTime = mysql ? formatTime(mysql.stats.mean) : null;
    if (mysqlTime) {
      mysqlCell.textContent = mysqlTime;
    } else if (mysql && mysql.stats.mean < 0) {
      mysqlCell.innerHTML = '<span class="text-red-500" title="Query failed (timeout or error)">FAILED</span>';
    } else {
      mysqlCell.textContent = 'N/A';
    }
    row.appendChild(mysqlCell);

    // Speedup vs SQLite
    const speedupCell = document.createElement('td');
    speedupCell.className = 'px-4 py-3 text-right font-semibold';

    // Check if vibesql query failed
    const vibesqlFailed = vibesql && vibesql.stats.mean < 0;

    if (vibesqlFailed) {
      speedupCell.textContent = 'FAILED';
      speedupCell.className += ' text-red-600 dark:text-red-400';
    } else if (vibesql && sqlite && vibesql.stats.mean > 0 && sqlite.stats.mean > 0) {
      const speedup = calculateSpeedup(vibesql.stats.mean, sqlite.stats.mean);
      speedupCell.textContent = `${speedup.toFixed(2)}x`;

      if (speedup > 1) {
        speedupCell.className += ' text-green-600 dark:text-green-400';
      } else if (speedup < 1) {
        speedupCell.className += ' text-red-600 dark:text-red-400';
      } else {
        speedupCell.className += ' text-muted';
      }

      totalSpeedup += speedup;
      comparisonCount++;
    } else {
      speedupCell.textContent = 'N/A';
      speedupCell.className += ' text-muted';
    }

    row.appendChild(speedupCell);

    // Winner
    const winnerCell = document.createElement('td');
    winnerCell.className = 'px-4 py-3 text-center text-2xl';

    if (vibesqlFailed) {
      winnerCell.textContent = '❌';
      winnerCell.title = 'Query failed (timeout or error)';
    } else if (vibesql && sqlite && vibesql.stats.mean > 0 && sqlite.stats.mean > 0) {
      const speedup = calculateSpeedup(vibesql.stats.mean, sqlite.stats.mean);
      winnerCell.textContent = speedup > 1 ? '🚀' : speedup < 1 ? '🐌' : '🤝';
    } else {
      winnerCell.textContent = '-';
    }

    row.appendChild(winnerCell);
    tbody.appendChild(row);
  }

  // Update summary cards
  if (comparisonCount > 0) {
    const avgSpeedup = totalSpeedup / comparisonCount;
    const avgSpeedupEl = document.getElementById('avg-speedup');
    if (avgSpeedupEl) {
      if (avgSpeedup > 1) {
        avgSpeedupEl.textContent = `${avgSpeedup.toFixed(2)}x faster`;
        avgSpeedupEl.className = avgSpeedupEl.className.replace(
          'text-primary-light dark:text-primary-dark',
          'text-green-600 dark:text-green-400'
        );
      } else if (avgSpeedup < 1) {
        // Invert the ratio: if speedup = 0.11, we're 1/0.11 = 9.09x slower
        const slowerBy = 1 / avgSpeedup;
        avgSpeedupEl.textContent = `${slowerBy.toFixed(2)}x slower`;
        avgSpeedupEl.className = avgSpeedupEl.className.replace(
          'text-primary-light dark:text-primary-dark',
          'text-red-600 dark:text-red-400'
        );
      } else {
        avgSpeedupEl.textContent = `${avgSpeedup.toFixed(2)}x`;
      }
    }
  }

  const opsTestedEl = document.getElementById('ops-tested');
  if (opsTestedEl) {
    opsTestedEl.textContent = grouped.size.toString();
  }
}

/**
 * Render performance chart
 */
function renderChart(data: BenchmarkResults, suite: BenchmarkSuite): void {
  const canvas = document.getElementById('performance-chart') as HTMLCanvasElement;
  if (!canvas) return;

  const config = SUITE_CONFIGS[suite];

  // Destroy existing chart if any
  if (currentChart) {
    currentChart.destroy();
    currentChart = null;
  }

  const grouped = groupBenchmarksByOperation(data.benchmarks, suite);

  const labels: string[] = [];
  const vibesqlData: number[] = [];
  const sqliteData: number[] = [];
  const duckdbData: number[] = [];
  const mysqlData: number[] = [];

  for (const [operation, databases] of grouped.entries()) {
    const vibesql = databases.get('vibesql');
    const sqlite = databases.get('sqlite');
    const duckdb = databases.get('duckdb');
    const mysql = databases.get('mysql');

    // Skip failed queries (negative mean) in the chart
    const vibesqlValid = vibesql && vibesql.stats.mean > 0;
    const sqliteValid = sqlite && sqlite.stats.mean > 0;
    const duckdbValid = duckdb && duckdb.stats.mean > 0;
    const mysqlValid = mysql && mysql.stats.mean > 0;

    if (vibesqlValid || sqliteValid || duckdbValid || mysqlValid) {
      // Get label - prefer query number if available (TPC-H)
      let label = operation.replace(/_/g, ' ').toUpperCase();
      const firstBench = vibesql || sqlite || duckdb || mysql;
      if (firstBench) {
        const parsed = parseBenchmarkName(firstBench.name, suite);
        if (parsed.queryNum) {
          label = `${config.name} ${parsed.queryNum.toUpperCase()}`;
        }
      }

      labels.push(label);
      vibesqlData.push(vibesqlValid ? vibesql!.stats.mean * 1000 : 0); // Convert to ms
      sqliteData.push(sqliteValid ? sqlite!.stats.mean * 1000 : 0);
      duckdbData.push(duckdbValid ? duckdb!.stats.mean * 1000 : 0);
      mysqlData.push(mysqlValid ? mysql!.stats.mean * 1000 : 0);
    }
  }

  currentChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'VibeSQL',
          data: vibesqlData,
          backgroundColor: 'rgba(34, 197, 94, 0.5)',
          borderColor: 'rgba(34, 197, 94, 1)',
          borderWidth: 1,
        },
        {
          label: 'SQLite',
          data: sqliteData,
          backgroundColor: 'rgba(239, 68, 68, 0.5)',
          borderColor: 'rgba(239, 68, 68, 1)',
          borderWidth: 1,
        },
        {
          label: 'DuckDB',
          data: duckdbData,
          backgroundColor: 'rgba(59, 130, 246, 0.5)',
          borderColor: 'rgba(59, 130, 246, 1)',
          borderWidth: 1,
        },
        {
          label: 'MySQL',
          data: mysqlData,
          backgroundColor: 'rgba(249, 115, 22, 0.5)',
          borderColor: 'rgba(249, 115, 22, 1)',
          borderWidth: 1,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          type: 'logarithmic',
          beginAtZero: false,
          title: {
            display: true,
            text: 'Time (ms) - Log Scale',
          },
          ticks: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            callback: function (value: any) {
              // Only show specific tick marks: 0.01, 0.1, 1, 10, 100, 1000
              const allowedTicks = [0.01, 0.1, 1, 10, 100, 1000];
              if (allowedTicks.includes(value)) {
                return value;
              }
              return null;
            },
          },
        },
      },
      plugins: {
        legend: {
          display: true,
          position: 'top',
        },
        tooltip: {
          callbacks: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            label: function (context: any) {
              return `${context.dataset.label}: ${context.parsed.y.toFixed(2)} ms`;
            },
          },
        },
      },
    },
  });
}

/**
 * Format bytes as human-readable size
 */
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

/**
 * Format memory in KB as human-readable
 */
function formatMemory(kb: number): string {
  if (kb < 1024) return `${kb.toFixed(0)} KB`;
  return `${(kb / 1024).toFixed(1)} MB`;
}

/**
 * Render footprint results table (different format from TPC benchmarks)
 */
function renderFootprintTable(data: FootprintResults): void {
  const tbody = document.getElementById('results-tbody');
  const table = document.getElementById('results-table');
  if (!tbody || !table) return;

  // Update table headers for footprint view
  const thead = table.querySelector('thead tr');
  if (thead) {
    thead.innerHTML = `
      <th class="px-4 py-3">Database</th>
      <th class="px-4 py-3 text-right">Binary Size</th>
      <th class="px-4 py-3 text-right">WASM Size</th>
      <th class="px-4 py-3 text-right">WASM (gzip)</th>
      <th class="px-4 py-3 text-right">Startup Time</th>
      <th class="px-4 py-3 text-right">Peak Memory</th>
      <th class="px-4 py-3 text-right">Version</th>
      <th class="px-4 py-3 text-center">Best</th>
    `;
  }

  tbody.innerHTML = '';

  // Find best values for highlighting
  const availableBenchmarks = data.benchmarks.filter(b => b.available);
  const minBinarySize = Math.min(...availableBenchmarks.map(b => b.binary_size_bytes));
  const minStartupTime = Math.min(...availableBenchmarks.map(b => b.startup_time_ms));
  const minMemory = Math.min(...availableBenchmarks.map(b => b.peak_memory_kb));

  // Database display names and colors
  const dbDisplayNames: Record<string, string> = {
    'vibesql': 'VibeSQL',
    'sqlite': 'SQLite',
    'duckdb': 'DuckDB',
    'mysql': 'MySQL',
  };

  for (const benchmark of data.benchmarks) {
    if (!benchmark.available) continue;

    const row = document.createElement('tr');
    row.className = 'hover:bg-card/50 transition-colors';

    // Database name
    const dbCell = document.createElement('td');
    dbCell.className = 'px-4 py-3 font-medium text-foreground';
    dbCell.textContent = dbDisplayNames[benchmark.database] || benchmark.database;
    row.appendChild(dbCell);

    // Binary size
    const sizeCell = document.createElement('td');
    sizeCell.className = 'px-4 py-3 text-right';
    const isBestSize = benchmark.binary_size_bytes === minBinarySize;
    sizeCell.innerHTML = isBestSize
      ? `<span class="text-green-600 dark:text-green-400 font-semibold">${formatBytes(benchmark.binary_size_bytes)}</span>`
      : `<span class="text-muted">${formatBytes(benchmark.binary_size_bytes)}</span>`;
    row.appendChild(sizeCell);

    // WASM size (only for VibeSQL)
    const wasmCell = document.createElement('td');
    wasmCell.className = 'px-4 py-3 text-right';
    if (benchmark.wasm_size_bytes) {
      wasmCell.innerHTML = `<span class="text-muted">${formatBytes(benchmark.wasm_size_bytes)}</span>`;
    } else {
      wasmCell.innerHTML = '<span class="text-muted/50">-</span>';
    }
    row.appendChild(wasmCell);

    // WASM gzip size (only for VibeSQL)
    const wasmGzipCell = document.createElement('td');
    wasmGzipCell.className = 'px-4 py-3 text-right';
    if (benchmark.wasm_size_gzip_bytes) {
      // Show percentage of original WASM size
      const ratio = benchmark.wasm_size_bytes
        ? ((benchmark.wasm_size_gzip_bytes / benchmark.wasm_size_bytes) * 100).toFixed(0)
        : '';
      wasmGzipCell.innerHTML = `<span class="text-muted">${formatBytes(benchmark.wasm_size_gzip_bytes)}</span>`;
      if (ratio) {
        wasmGzipCell.title = `${ratio}% of uncompressed WASM`;
      }
    } else {
      wasmGzipCell.innerHTML = '<span class="text-muted/50">-</span>';
    }
    row.appendChild(wasmGzipCell);

    // Startup time
    const startupCell = document.createElement('td');
    startupCell.className = 'px-4 py-3 text-right';
    const isBestStartup = benchmark.startup_time_ms === minStartupTime;
    const startupText = `${benchmark.startup_time_ms.toFixed(2)} ms`;
    startupCell.innerHTML = isBestStartup
      ? `<span class="text-green-600 dark:text-green-400 font-semibold">${startupText}</span>`
      : `<span class="text-muted">${startupText}</span>`;
    row.appendChild(startupCell);

    // Peak memory
    const memCell = document.createElement('td');
    memCell.className = 'px-4 py-3 text-right';
    const isBestMem = benchmark.peak_memory_kb === minMemory;
    memCell.innerHTML = isBestMem
      ? `<span class="text-green-600 dark:text-green-400 font-semibold">${formatMemory(benchmark.peak_memory_kb)}</span>`
      : `<span class="text-muted">${formatMemory(benchmark.peak_memory_kb)}</span>`;
    row.appendChild(memCell);

    // Version
    const versionCell = document.createElement('td');
    versionCell.className = 'px-4 py-3 text-right text-muted text-xs';
    versionCell.textContent = benchmark.version;
    row.appendChild(versionCell);

    // Best indicator
    const bestCell = document.createElement('td');
    bestCell.className = 'px-4 py-3 text-center text-2xl';
    const bestCount = (isBestSize ? 1 : 0) + (isBestStartup ? 1 : 0) + (isBestMem ? 1 : 0);
    if (bestCount === 3) {
      bestCell.textContent = '🏆';
      bestCell.title = 'Best in all categories';
    } else if (bestCount >= 1) {
      bestCell.textContent = '⭐';
      bestCell.title = `Best in ${bestCount} category(ies)`;
    } else {
      bestCell.textContent = '-';
    }
    row.appendChild(bestCell);

    tbody.appendChild(row);
  }

  // Update summary cards for footprint
  const vibesql = data.benchmarks.find(b => b.database === 'vibesql');
  const sqlite = data.benchmarks.find(b => b.database === 'sqlite');

  const avgSpeedupEl = document.getElementById('avg-speedup');
  if (avgSpeedupEl && vibesql && sqlite && vibesql.available && sqlite.available) {
    const startupSpeedup = sqlite.startup_time_ms / vibesql.startup_time_ms;
    if (startupSpeedup > 1) {
      avgSpeedupEl.textContent = `${startupSpeedup.toFixed(2)}x faster startup`;
      avgSpeedupEl.className = 'text-xl font-bold text-green-600 dark:text-green-400';
    } else {
      const slower = 1 / startupSpeedup;
      avgSpeedupEl.textContent = `${slower.toFixed(2)}x slower startup`;
      avgSpeedupEl.className = 'text-xl font-bold text-red-600 dark:text-red-400';
    }
  }

  const opsTestedEl = document.getElementById('ops-tested');
  if (opsTestedEl) {
    opsTestedEl.textContent = availableBenchmarks.length.toString();
  }
}

/**
 * Render footprint comparison chart
 */
function renderFootprintChart(data: FootprintResults): void {
  const canvas = document.getElementById('performance-chart') as HTMLCanvasElement;
  if (!canvas) return;

  // Destroy existing chart if any
  if (currentChart) {
    currentChart.destroy();
    currentChart = null;
  }

  const availableBenchmarks = data.benchmarks.filter(b => b.available);

  // Database display names and colors
  const dbColors: Record<string, { bg: string; border: string }> = {
    'vibesql': { bg: 'rgba(34, 197, 94, 0.5)', border: 'rgba(34, 197, 94, 1)' },
    'sqlite': { bg: 'rgba(239, 68, 68, 0.5)', border: 'rgba(239, 68, 68, 1)' },
    'duckdb': { bg: 'rgba(59, 130, 246, 0.5)', border: 'rgba(59, 130, 246, 1)' },
    'mysql': { bg: 'rgba(249, 115, 22, 0.5)', border: 'rgba(249, 115, 22, 1)' },
  };

  const dbDisplayNames: Record<string, string> = {
    'vibesql': 'VibeSQL',
    'sqlite': 'SQLite',
    'duckdb': 'DuckDB',
    'mysql': 'MySQL',
  };

  const labels = ['Binary Size (MB)', 'WASM Size (MB)', 'WASM gzip (MB)', 'Startup Time (ms)', 'Peak Memory (MB)'];

  const datasets = availableBenchmarks.map(bench => ({
    label: dbDisplayNames[bench.database] || bench.database,
    data: [
      bench.binary_size_bytes / (1024 * 1024), // Convert to MB
      bench.wasm_size_bytes ? bench.wasm_size_bytes / (1024 * 1024) : 0, // WASM size
      bench.wasm_size_gzip_bytes ? bench.wasm_size_gzip_bytes / (1024 * 1024) : 0, // WASM gzip
      bench.startup_time_ms,
      bench.peak_memory_kb / 1024, // Convert to MB
    ],
    backgroundColor: dbColors[bench.database]?.bg || 'rgba(156, 163, 175, 0.5)',
    borderColor: dbColors[bench.database]?.border || 'rgba(156, 163, 175, 1)',
    borderWidth: 1,
  }));

  currentChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets,
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: 'Value (lower is better)',
          },
        },
      },
      plugins: {
        legend: {
          display: true,
          position: 'top',
        },
        tooltip: {
          callbacks: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            label: function (context: any) {
              const label = context.dataset.label;
              const value = context.parsed.y;
              const category = context.label;

              if (category.includes('Size')) {
                return `${label}: ${value.toFixed(2)} MB`;
              } else if (category.includes('Time')) {
                return `${label}: ${value.toFixed(2)} ms`;
              } else {
                return `${label}: ${value.toFixed(1)} MB`;
              }
            },
          },
        },
      },
    },
  });
}

/**
 * Update the methodology section
 */
function updateMethodology(suite: BenchmarkSuite): void {
  const methodologyEl = document.getElementById('methodology-content');
  if (methodologyEl) {
    methodologyEl.innerHTML = SUITE_CONFIGS[suite].methodology;
  }
}

/**
 * Update the ops label
 */
function updateOpsLabel(suite: BenchmarkSuite): void {
  const opsLabelEl = document.querySelector('#ops-tested + p');
  if (opsLabelEl) {
    opsLabelEl.textContent = SUITE_CONFIGS[suite].opsLabel;
  }
}

/**
 * Restore table headers for TPC benchmarks
 */
function restoreTPCTableHeaders(): void {
  const table = document.getElementById('results-table');
  if (!table) return;

  const thead = table.querySelector('thead tr');
  if (thead) {
    thead.innerHTML = `
      <th class="px-4 py-3">Operation</th>
      <th class="px-4 py-3 text-right">VibeSQL</th>
      <th class="px-4 py-3 text-right">SQLite</th>
      <th class="px-4 py-3 text-right">DuckDB</th>
      <th class="px-4 py-3 text-right">MySQL</th>
      <th class="px-4 py-3 text-right">Speedup</th>
      <th class="px-4 py-3 text-center">Winner</th>
    `;
  }
}

/**
 * Load and display benchmark data for a specific suite
 */
async function loadBenchmarkData(suite: BenchmarkSuite): Promise<void> {
  const config = SUITE_CONFIGS[suite];

  // Update methodology and ops label
  updateMethodology(suite);
  updateOpsLabel(suite);

  // Restore TPC headers if not footprint
  if (suite !== 'footprint') {
    restoreTPCTableHeaders();
  }

  try {
    const response = await fetch(`${import.meta.env.BASE_URL}benchmarks/${config.dataFile}`);

    if (!response.ok) {
      throw new Error(`Failed to load benchmark data: ${response.status}`);
    }

    // Handle footprint suite differently (different data format)
    if (suite === 'footprint') {
      const data: FootprintResults = await response.json();

      // Update last updated timestamp
      const lastUpdatedEl = document.getElementById('last-updated');
      if (lastUpdatedEl && data.datetime) {
        const date = new Date(data.datetime);
        lastUpdatedEl.textContent = date.toLocaleDateString();
        lastUpdatedEl.className = 'text-xl font-bold text-primary-light dark:text-primary-dark';
      }

      renderFootprintTable(data);
      renderFootprintChart(data);
      return;
    }

    // Standard TPC benchmark handling
    const data: BenchmarkResults = await response.json();

    // Update last updated timestamp
    const lastUpdatedEl = document.getElementById('last-updated');
    if (lastUpdatedEl && data.datetime) {
      const date = new Date(data.datetime);
      lastUpdatedEl.textContent = date.toLocaleDateString();
      lastUpdatedEl.className = 'text-xl font-bold text-primary-light dark:text-primary-dark';
    }

    renderResultsTable(data, suite);
    renderChart(data, suite);
  } catch (error) {
    console.error('Error loading benchmark data:', error);

    const tbody = document.getElementById('results-tbody');
    if (tbody) {
      tbody.innerHTML = `
        <tr>
          <td colspan="7" class="px-4 py-8 text-center text-muted">
            <p class="mb-2">No ${config.name} benchmark results available yet.</p>
            <p class="text-sm">Results will be generated when CI runs for this benchmark suite.</p>
          </td>
        </tr>
      `;
    }

    const avgSpeedupEl = document.getElementById('avg-speedup');
    if (avgSpeedupEl) {
      avgSpeedupEl.textContent = 'N/A';
      avgSpeedupEl.className = 'text-xl font-bold text-muted';
    }

    const opsTestedEl = document.getElementById('ops-tested');
    if (opsTestedEl) {
      opsTestedEl.textContent = '-';
    }

    // Destroy chart if exists
    if (currentChart) {
      currentChart.destroy();
      currentChart = null;
    }
  }
}

/**
 * Initialize tab switching
 */
function initTabs(): void {
  const tabs = document.querySelectorAll('.benchmark-tab');

  tabs.forEach((tab) => {
    tab.addEventListener('click', () => {
      const tabId = tab.id.replace('tab-', '') as BenchmarkSuite;

      // Update active state
      tabs.forEach((t) => {
        t.classList.remove('benchmark-tab--active');
        t.setAttribute('aria-selected', 'false');
      });
      tab.classList.add('benchmark-tab--active');
      tab.setAttribute('aria-selected', 'true');

      // Load new data
      currentSuite = tabId;
      loadBenchmarkData(tabId);
    });
  });
}

// Initialize page
document.addEventListener('DOMContentLoaded', () => {
  // Initialize theme system
  const theme = initTheme();

  // Initialize navigation component
  new NavigationComponent('benchmarks', theme);

  // Initialize tabs
  initTabs();

  // Load benchmark data for default suite (TPC-H)
  loadBenchmarkData(currentSuite);
});
