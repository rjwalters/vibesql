/**
 * Benchmark results page
 *
 * Loads and displays performance benchmark data comparing VibeSQL to SQLite.
 */

import './styles/main.css';
import { initTheme } from './theme';
import { NavigationComponent } from './components/Navigation';

// Chart.js is loaded via CDN in benchmarks.html
declare const Chart: any;

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
 * TPC-H Query descriptions
 */
const TPCH_DESCRIPTIONS: Record<string, string> = {
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
};

/**
 * Parse benchmark name to extract database and operation info
 */
function parseBenchmarkName(name: string): { operation: string; database: string; queryNum?: string; description?: string } {
  // TPC-H format: "tpch_q1_pricing_summary_report_vibesql"
  // Legacy format: "test_simple_select_1k_vibesql"
  const parts = name.split('_');
  const database = parts[parts.length - 1]; // Last part is database name

  // Check if this is a TPC-H query
  if (name.startsWith('tpch_')) {
    // Extract query number (q1, q2, etc.)
    const queryNum = parts[1]; // e.g., "q1"
    const description = TPCH_DESCRIPTIONS[queryNum];

    // Operation name is everything except "tpch_", query number, and database
    // e.g., "tpch_q1_pricing_summary_report_vibesql" -> "pricing_summary_report"
    const operation = parts.slice(2, -1).join('_');

    return { operation, database, queryNum, description };
  }

  // Legacy format
  const operation = parts.slice(1, -1).join('_');
  return { operation, database };
}

/**
 * Group benchmarks by operation
 */
function groupBenchmarksByOperation(benchmarks: Benchmark[]): Map<string, Map<string, Benchmark>> {
  const grouped = new Map<string, Map<string, Benchmark>>();

  for (const bench of benchmarks) {
    const { operation, database } = parseBenchmarkName(bench.name);

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
function renderResultsTable(data: BenchmarkResults) {
  const tbody = document.getElementById('results-tbody');
  if (!tbody) return;

  const grouped = groupBenchmarksByOperation(data.benchmarks);

  tbody.innerHTML = '';

  let totalSpeedup = 0;
  let comparisonCount = 0;

  for (const [operation, databases] of grouped.entries()) {
    const vibesql = databases.get('vibesql');
    const sqlite = databases.get('sqlite');
    const duckdb = databases.get('duckdb');

    if (!vibesql && !sqlite && !duckdb) continue;

    const row = document.createElement('tr');
    row.className = 'hover:bg-card/50 transition-colors';

    // Operation name (with tooltip for TPC-H queries)
    const opCell = document.createElement('td');
    opCell.className = 'px-4 py-3 font-medium text-foreground';

    // Get the first benchmark to extract query info
    const firstBench = vibesql || sqlite || duckdb;
    if (firstBench) {
      const parsed = parseBenchmarkName(firstBench.name);
      if (parsed.queryNum && parsed.description) {
        // TPC-H query - show query number and add tooltip
        opCell.innerHTML = `
          <span class="cursor-help" title="${parsed.description}">
            TPC-H ${parsed.queryNum.toUpperCase()}
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
function renderChart(data: BenchmarkResults) {
  const canvas = document.getElementById('performance-chart') as HTMLCanvasElement;
  if (!canvas) return;

  const grouped = groupBenchmarksByOperation(data.benchmarks);

  const labels: string[] = [];
  const vibesqlData: number[] = [];
  const sqliteData: number[] = [];
  const duckdbData: number[] = [];

  for (const [operation, databases] of grouped.entries()) {
    const vibesql = databases.get('vibesql');
    const sqlite = databases.get('sqlite');
    const duckdb = databases.get('duckdb');

    // Skip failed queries (negative mean) in the chart
    const vibesqlValid = vibesql && vibesql.stats.mean > 0;
    const sqliteValid = sqlite && sqlite.stats.mean > 0;
    const duckdbValid = duckdb && duckdb.stats.mean > 0;

    if (vibesqlValid || sqliteValid || duckdbValid) {
      // Get label - prefer TPC-H query number if available
      let label = operation.replace(/_/g, ' ').toUpperCase();
      const firstBench = vibesql || sqlite || duckdb;
      if (firstBench) {
        const parsed = parseBenchmarkName(firstBench.name);
        if (parsed.queryNum) {
          label = `TPC-H ${parsed.queryNum.toUpperCase()}`;
        }
      }

      labels.push(label);
      vibesqlData.push(vibesqlValid ? vibesql!.stats.mean * 1000 : 0); // Convert to ms
      sqliteData.push(sqliteValid ? sqlite!.stats.mean * 1000 : 0);
      duckdbData.push(duckdbValid ? duckdb!.stats.mean * 1000 : 0);
    }
  }

  new Chart(canvas, {
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
 * Load and display benchmark data
 */
async function loadBenchmarkData() {
  try {
    const response = await fetch(`${import.meta.env.BASE_URL}benchmarks/benchmark_results.json`);

    if (!response.ok) {
      throw new Error(`Failed to load benchmark data: ${response.status}`);
    }

    const data: BenchmarkResults = await response.json();

    // Update last updated timestamp
    const lastUpdatedEl = document.getElementById('last-updated');
    if (lastUpdatedEl && data.datetime) {
      const date = new Date(data.datetime);
      lastUpdatedEl.textContent = date.toLocaleDateString();
      lastUpdatedEl.className = 'text-xl font-bold text-primary-light dark:text-primary-dark';
    }

    renderResultsTable(data);
    renderChart(data);
  } catch (error) {
    console.error('Error loading benchmark data:', error);

    const tbody = document.getElementById('results-tbody');
    if (tbody) {
      tbody.innerHTML = `
        <tr>
          <td colspan="6" class="px-4 py-8 text-center text-red-500">
            ⚠️ Failed to load benchmark results. Please check back later.
          </td>
        </tr>
      `;
    }

    const avgSpeedupEl = document.getElementById('avg-speedup');
    if (avgSpeedupEl) {
      avgSpeedupEl.textContent = 'N/A';
      avgSpeedupEl.className = 'text-xl font-bold text-muted';
    }
  }
}

// Initialize page
document.addEventListener('DOMContentLoaded', () => {
  // Initialize theme system
  const theme = initTheme();

  // Initialize navigation component
  new NavigationComponent('benchmarks', theme);

  // Load benchmark data
  loadBenchmarkData();
});
