/**
 * Benchmark results page
 *
 * Loads and displays performance benchmark data comparing VibeSQL to SQLite, DuckDB, and MySQL.
 * Supports multiple benchmark suites: TPC-H, TPC-C, and Sysbench.
 */

import './styles/main.css';
import { initTheme } from './theme';
import { initLocale } from './locale';
import { NavigationComponent } from './components/Navigation';
import { formatTime, formatBytes, formatMemory, formatTps } from './utils/measurement';

// Chart.js is loaded via CDN in benchmarks.html
declare const Chart: any;

/**
 * Benchmark suite types
 */
type BenchmarkSuite = 'tpch' | 'tpcds' | 'tpcc' | 'sysbench-embedded' | 'sysbench-server' | 'footprint-embedded' | 'footprint-server';

// ============================================================================
// HTML Template Helpers
// ============================================================================

const HARDWARE = 'Mac Studio M3 Ultra (28 cores, 96GB RAM)';

// ============================================================================
// Shared Constants
// ============================================================================

/** Display names for databases */
const DB_DISPLAY_NAMES: Record<string, string> = {
  'vibesql': 'VibeSQL',
  'vibesql_server': 'VibeSQL Server',
  'sqlite': 'SQLite',
  'duckdb': 'DuckDB',
  'mysql': 'MySQL',
};

/** Color configurations for databases (Chart.js format) */
const DB_COLORS: Record<string, { bg: string; border: string }> = {
  'vibesql': { bg: 'rgba(34, 197, 94, 0.5)', border: 'rgba(34, 197, 94, 1)' },
  'vibesql_server': { bg: 'rgba(34, 197, 94, 0.5)', border: 'rgba(34, 197, 94, 1)' },
  'sqlite': { bg: 'rgba(239, 68, 68, 0.5)', border: 'rgba(239, 68, 68, 1)' },
  'duckdb': { bg: 'rgba(59, 130, 246, 0.5)', border: 'rgba(59, 130, 246, 1)' },
  'mysql': { bg: 'rgba(249, 115, 22, 0.5)', border: 'rgba(249, 115, 22, 1)' },
};

/** CSS classes for table cells */
const CELL_CLASSES = {
  base: 'px-4 py-3 text-right',
  muted: 'px-4 py-3 text-right text-gray-500 dark:text-gray-400',
  winner: 'px-4 py-3 text-right font-semibold text-green-600 dark:text-green-400',
  failed: 'text-red-500',
};

// ============================================================================
// Chart Helpers
// ============================================================================

/** Create a Chart.js dataset for a database */
function createDataset(dbKey: string, data: number[], labelOverride?: string): {
  label: string;
  data: number[];
  backgroundColor: string;
  borderColor: string;
  borderWidth: number;
} {
  const colors = DB_COLORS[dbKey] || { bg: 'rgba(156, 163, 175, 0.5)', border: 'rgba(156, 163, 175, 1)' };
  return {
    label: labelOverride || DB_DISPLAY_NAMES[dbKey] || dbKey,
    data,
    backgroundColor: colors.bg,
    borderColor: colors.border,
    borderWidth: 1,
  };
}

/** Common chart options for time-based benchmarks (log scale) */
function getLogScaleChartOptions(yAxisLabel: string): object {
  return {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
      y: {
        type: 'logarithmic',
        beginAtZero: false,
        title: { display: true, text: yAxisLabel },
        ticks: {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          callback: function (value: any) {
            const allowedTicks = [0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000];
            return allowedTicks.includes(value) ? value : null;
          },
        },
      },
    },
    plugins: {
      legend: { display: true, position: 'top' },
      tooltip: {
        callbacks: {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          label: (context: any) => `${context.dataset.label}: ${context.parsed.y.toFixed(2)} ms`,
        },
      },
    },
  };
}

/** Common chart options for linear scale benchmarks */
function getLinearChartOptions(yAxisLabel: string): object {
  return {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
      y: {
        beginAtZero: true,
        title: { display: true, text: yAxisLabel },
      },
    },
    plugins: {
      legend: { display: true, position: 'top' },
    },
  };
}

/** Generate a methodology detail list item */
const li = (label: string, value: string): string =>
  `<li><strong>${label}:</strong> ${value}</li>`;

/** Generate a bullet list for discussions */
const bullet = (label: string, desc: string): string =>
  `<li><strong>${label}:</strong> ${desc}</li>`;

/** Generate methodology section with title, description, details list, and optional notes */
const methodology = (
  title: string,
  description: string,
  details: string[],
  notes?: string[]
): string => `
  <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">${title}</h3>
  <p class="text-gray-500 dark:text-gray-400 mb-4">${description}</p>
  <ul class="space-y-2 text-gray-500 dark:text-gray-400">
    ${li('Hardware', HARDWARE)}
    ${details.join('\n    ')}
  </ul>
  ${notes?.map(note => `
  <p class="mt-4 text-gray-500 dark:text-gray-400 text-sm">${note}</p>`).join('') ?? ''}
`;

/** Generate discussion section with multiple subsections */
const discussion = (sections: { title: string; content: string }[]): string => `
  <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">Analysis &amp; Roadmap</h3>
  ${sections.map(({ title, content }) => `
  <h4 class="text-md font-medium text-gray-900 dark:text-gray-100 mt-4 mb-2">${title}</h4>
  ${content}`).join('')}
`;

/** Generate a paragraph */
const p = (text: string): string =>
  `<p class="text-gray-500 dark:text-gray-400 mb-2">${text}</p>`;

/** Generate a bullet list for discussion items */
const bullets = (items: string[]): string =>
  `<ul class="list-disc list-inside space-y-1 text-gray-500 dark:text-gray-400 text-sm ml-2">
    ${items.join('\n    ')}
  </ul>`;

// ============================================================================
// Suite Configuration
// ============================================================================

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
  discussion: string;
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
    methodology: methodology(
      'TPC-H Decision Support Benchmark',
      `These benchmarks use the industry-standard <strong>TPC-H benchmark suite</strong>,
        which simulates real-world decision support workloads with complex analytical queries
        involving aggregations, joins, subqueries, and sorting.`,
      [
        li('Benchmark Framework', 'Criterion.rs (Rust native benchmarking)'),
        li('Scale Factor', 'SF 0.01 (~60,000 rows across 8 tables)'),
        li('Data', 'Deterministic TPC-H compliant dataset'),
        li('Databases Tested', 'VibeSQL, SQLite (via rusqlite), DuckDB (via duckdb-rs)'),
        li('Execution Mode', 'All databases run in-memory (no disk I/O)'),
        li('Measurement', 'Native Rust API calls (no Python/FFI overhead)'),
      ],
      [
        `All benchmarks measure end-to-end query execution time including parsing,
        planning, execution, and result materialization. This represents <strong>real-world
        SQL engine performance</strong> for analytical workloads.`,
        `<strong>Note:</strong> TPC-H queries test different aspects of SQL performance:
        simple aggregations (Q1, Q6), complex joins (Q2-Q5, Q7-Q10), subqueries (Q11-Q15),
        and advanced analytics (Q16-Q22). Hover over query names in the table above for descriptions.`,
      ]
    ),
    discussion: discussion([
      {
        title: 'Where VibeSQL Excels',
        content: p(`VibeSQL shows strong performance on <strong>scan-heavy aggregation queries</strong> (Q1, Q6, Q14, Q15, Q20)
          where our columnar execution engine and SIMD-accelerated aggregations shine. These queries
          involve filtering large tables and computing aggregates without complex join patterns.`),
      },
      {
        title: 'Current Optimization Targets',
        content: p(`Multi-way join queries (Q3, Q5, Q7-Q10, Q18, Q19, Q21) currently show SQLite ahead. The primary bottleneck
          is our hash join implementation, which doesn't yet employ the same level of optimization as SQLite's
          decades-refined B-tree joins. Specific areas under active development:`) + bullets([
          bullet('Join ordering', 'Improved cardinality estimation for better join order selection'),
          bullet('Hash table sizing', 'Adaptive hash table growth and spill-to-disk for large joins'),
          bullet('Vectorized joins', 'Batch processing in the join inner loop to improve cache utilization'),
          bullet('Index-nested-loop joins', 'Leveraging B-tree indexes when beneficial'),
        ]),
      },
      {
        title: 'Path to Leadership',
        content: p(`VibeSQL's architecture is designed for modern hardware with features like columnar storage,
          vectorized execution, and lock-free concurrency. As these optimizations mature, we expect
          VibeSQL to achieve consistent leadership across all TPC-H queries. The fundamental design
          supports parallelism and SIMD that traditional row-store databases cannot easily retrofit.`),
      },
    ]),
  },
  tpcds: {
    id: 'tpcds',
    name: 'TPC-DS',
    dataFile: 'tpcds_results.json',
    opsLabel: 'TPC-DS queries',
    descriptions: {
      'q1': 'Q1 - Store returns analysis with date filtering',
      'q2': 'Q2 - Catalog and web sales comparison',
      'q3': 'Q3 - Brand sales by date',
      'q4': 'Q4 - Customer lifetime value comparison',
      'q5': 'Q5 - Web and catalog page sales',
      'q6': 'Q6 - State-based customer analysis',
      'q7': 'Q7 - Promotion effect analysis',
      'q8': 'Q8 - Customer zip code analysis',
      'q9': 'Q9 - Reason-based returns analysis',
      'q10': 'Q10 - Customer demographics from catalog',
      'q11': 'Q11 - Customer lifetime value (web vs store)',
      'q12': 'Q12 - Web sales by category',
      'q13': 'Q13 - Store sales demographics',
      'q14': 'Q14 - Cross-channel brand affinity',
      'q15': 'Q15 - Catalog sales by zip code',
      'q16': 'Q16 - Catalog returns analysis',
      'q17': 'Q17 - Store-catalog cross-sell',
      'q18': 'Q18 - Catalog sales demographics',
      'q19': 'Q19 - Store sales by manufacturer',
      'q20': 'Q20 - Catalog sales analysis',
      'q21': 'Q21 - Inventory analysis',
      'q22': 'Q22 - Inventory monthly rollup',
      'q23': 'Q23 - Frequent customer sales',
      'q24': 'Q24 - Store sales by color',
      'q25': 'Q25 - Store-catalog-web returns',
      'q26': 'Q26 - Promotion ROI analysis',
      'q27': 'Q27 - Store profit analysis',
      'q28': 'Q28 - Store sales price ranges',
      'q29': 'Q29 - Store-catalog order analysis',
      'q30': 'Q30 - Web returns by state',
      'q31': 'Q31 - Web-catalog geographic analysis',
      'q32': 'Q32 - Catalog excess inventory',
      'q33': 'Q33 - Manufacturer brand analysis',
      'q34': 'Q34 - Store sales by county',
      'q35': 'Q35 - Customer demographics analysis',
      'q36': 'Q36 - Store sales profit ranking',
      'q37': 'Q37 - Inventory planning',
      'q38': 'Q38 - Customer count analysis',
      'q39': 'Q39 - Inventory variance',
      'q40': 'Q40 - Catalog sales with warehouse',
      'q41': 'Q41 - Manufacturing item analysis',
      'q42': 'Q42 - Web sales by date',
      'q43': 'Q43 - Store sales by day of week',
      'q44': 'Q44 - Store sales top-N items',
      'q45': 'Q45 - Web sales by customer',
      'q46': 'Q46 - Store customer demographics',
      'q47': 'Q47 - Store sales monthly analysis',
      'q48': 'Q48 - Store sales price analysis',
      'q49': 'Q49 - Cross-channel returns analysis',
      'q50': 'Q50 - Store returns timing analysis',
      'q51': 'Q51 - Web-store sales window analysis',
      'q52': 'Q52 - Web sales by category hierarchy',
      'q53': 'Q53 - Store sales by manufacturer',
      'q54': 'Q54 - Cross-channel customer analysis',
      'q55': 'Q55 - Brand-manager sales analysis',
      'q56': 'Q56 - Multi-channel color analysis',
      'q57': 'Q57 - Catalog sales monthly analysis',
      'q58': 'Q58 - Cross-channel item sales',
      'q59': 'Q59 - Store sales weekly rollup',
      'q60': 'Q60 - Multi-channel category analysis',
      'q61': 'Q61 - Store promotion analysis',
      'q62': 'Q62 - Web sales shipping analysis',
      'q63': 'Q63 - Store manager sales analysis',
      'q64': 'Q64 - Cross-channel product analysis',
      'q65': 'Q65 - Store revenue analysis',
      'q66': 'Q66 - Web-catalog sales by warehouse',
      'q67': 'Q67 - Store sales ranking',
      'q68': 'Q68 - Store customer household analysis',
      'q69': 'Q69 - Customer demographics comparison',
      'q70': 'Q70 - Store sales rollup analysis',
      'q71': 'Q71 - Multi-channel time analysis',
      'q72': 'Q72 - Catalog warehouse analysis',
      'q73': 'Q73 - Store customer count analysis',
      'q74': 'Q74 - Store-web customer comparison',
      'q75': 'Q75 - Cross-channel brand returns',
      'q76': 'Q76 - Multi-channel null channel analysis',
      'q77': 'Q77 - Store-catalog-web profit analysis',
      'q78': 'Q78 - Cross-channel customer value',
      'q79': 'Q79 - Store customer spend analysis',
      'q80': 'Q80 - Multi-channel profit analysis',
      'q81': 'Q81 - Catalog returns by state',
      'q82': 'Q82 - Inventory demographics',
      'q83': 'Q83 - Cross-channel returns by week',
      'q84': 'Q84 - Store customer income analysis',
      'q85': 'Q85 - Web returns reason analysis',
      'q86': 'Q86 - Web sales rollup',
      'q87': 'Q87 - Cross-channel customer count',
      'q88': 'Q88 - Store sales time analysis',
      'q89': 'Q89 - Store sales by class',
      'q90': 'Q90 - Web sales time of day',
      'q91': 'Q91 - Call center returns analysis',
      'q92': 'Q92 - Web sales discount analysis',
      'q93': 'Q93 - Store returns reason analysis',
      'q94': 'Q94 - Web sales shipping exception',
      'q95': 'Q95 - Web sales duplicate analysis',
      'q96': 'Q96 - Store sales staff analysis',
      'q97': 'Q97 - Store-catalog sales distinct',
      'q98': 'Q98 - Store sales category analysis',
      'q99': 'Q99 - Multi-channel shipping analysis',
    },
    methodology: methodology(
      'TPC-DS Decision Support Benchmark',
      `<strong>TPC-DS</strong> is the successor to TPC-H, featuring 99 queries that model
        a modern decision support system with significantly more complex query patterns
        including multiple fact tables, snow-flake schema, and advanced SQL features.`,
      [
        li('Schema', '24 tables with star/snowflake schema design'),
        li('Query Count', '99 queries (currently 88/99 passing)'),
        li('Scale Factor', 'SF 0.01'),
        li('Query Types', 'Reporting, ad-hoc, data mining patterns'),
        li('SQL Features', 'Window functions, CTEs, complex subqueries, ROLLUP/CUBE'),
      ],
      [
        `TPC-DS queries are substantially more complex than TPC-H, testing advanced SQL features
        like window functions, common table expressions (WITH clause), and complex join patterns
        across multiple fact and dimension tables.`,
        `<strong>Note:</strong> Remaining unsupported queries require features like INTERSECT/EXCEPT or
        specific date arithmetic functions not yet implemented.`,
      ]
    ),
    discussion: discussion([
      {
        title: 'SQL:1999 Feature Coverage',
        content: p(`TPC-DS exercises the most demanding SQL features. VibeSQL passes <strong>88 of 99 queries</strong>,
          demonstrating broad coverage of SQL:1999 including ROLLUP, CUBE, GROUPING(), window functions with
          complex framing, and recursive CTEs. The remaining queries require INTERSECT/EXCEPT set operations.`),
      },
      {
        title: 'Complex Query Optimization',
        content: p('TPC-DS queries often join 10+ tables with correlated subqueries. Current focus areas:') + bullets([
          bullet('CTE materialization', 'Intelligent decision between materialized and inline CTEs'),
          bullet('Subquery decorrelation', 'Converting correlated subqueries to joins when beneficial'),
          bullet('Star schema optimization', 'Fact-dimension join ordering for analytical patterns'),
        ]),
      },
      {
        title: 'Toward 99/99',
        content: p(`INTERSECT and EXCEPT are planned additions that will enable the remaining queries. These set
          operations fit naturally into our existing query algebra and will be implemented as hash-based
          operators similar to our DISTINCT processing.`),
      },
    ]),
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
    methodology: methodology(
      'TPC-C Online Transaction Processing Benchmark',
      `The <strong>TPC-C benchmark</strong> simulates a complete order-entry environment
        with a mix of complex transactions including order entry, payment processing,
        order status queries, delivery processing, and stock level monitoring.`,
      [
        li('Workload', 'OLTP (Online Transaction Processing)'),
        li('Transaction Mix', '45% New Order, 43% Payment, 4% Order Status, 4% Delivery, 4% Stock Level'),
        li('Warehouses', '1 warehouse (scaled for in-memory testing)'),
        li('Concurrency', 'Single-threaded baseline measurements'),
        li('ACID Compliance', 'Full transaction isolation testing'),
      ],
      [
        `TPC-C measures transactions per minute (tpmC) and tests the database's ability to handle
        concurrent transactions with complex business logic. This benchmark is critical for
        evaluating <strong>transactional workload performance</strong>.`,
        `<strong>Note:</strong> Results show average transaction latency. Lower is better.
        TPC-C is particularly demanding for write-heavy workloads with strict consistency requirements.`,
      ]
    ),
    discussion: discussion([
      {
        title: '42x Faster Than SQLite',
        content: p(`VibeSQL achieves <strong>~79,000 transactions per second</strong> compared to SQLite's ~1,900 TPS,
          a 42x improvement. This dramatic speedup comes from our lock-free MVCC architecture that avoids
          SQLite's coarse-grained locking on every write operation.`),
      },
      {
        title: 'Why VibeSQL Dominates OLTP',
        content: bullets([
          bullet('Lock-free reads', 'MVCC allows readers and writers to proceed concurrently without blocking'),
          bullet('Optimistic concurrency', 'Transactions only conflict at commit time, not during execution'),
          bullet('In-memory B-tree', 'Purpose-built index structure optimized for in-memory workloads'),
          bullet('Prepared statement caching', 'Query plans are compiled once and reused'),
        ]),
      },
      {
        title: 'Scaling Further',
        content: p(`Current results are single-threaded. VibeSQL's architecture supports multi-threaded transaction
          processing, and we expect near-linear scaling as we add parallel execution support. Our goal is
          to achieve 500K+ TPS on modern multi-core hardware.`),
      },
    ]),
  },
  'sysbench-embedded': {
    id: 'sysbench-embedded',
    name: 'Sysbench (Embedded)',
    dataFile: 'sysbench_results.json',
    opsLabel: 'Sysbench operations',
    descriptions: {
      'point_select': 'Point Select - Single row lookup by primary key',
      'insert': 'Insert - Insert new rows into table',
      'update_index': 'Update Index - Update indexed column (k = k + 1)',
      'update_non_index': 'Update Non-Index - Update non-indexed column',
      'delete': 'Delete - Remove rows by primary key',
      'range_queries': 'Range Queries - Simple, SUM, ORDER BY, and DISTINCT range scans',
    },
    methodology: methodology(
      'Sysbench Micro-Benchmarks (Embedded)',
      `<strong>Sysbench</strong> provides focused micro-benchmarks that isolate specific
        database operations. These tests measure raw performance for fundamental operations
        without the complexity of full transaction workloads.`,
      [
        li('Mode', 'Embedded (in-process, zero network overhead)'),
        li('Workload Types', 'Point queries, range scans, updates, inserts, deletes'),
        li('Table Size', '10,000 rows per table'),
        li('Index Types', 'Primary key and secondary indexes'),
        li('Operations', 'Single-statement operations (no multi-statement transactions)'),
        li('Databases', 'VibeSQL, SQLite, DuckDB'),
      ],
      [
        `Embedded mode runs the database in-process with zero network overhead, ideal for
        single-process applications where minimal latency is critical.`,
      ]
    ),
    discussion: discussion([
      {
        title: 'Point Lookups: VibeSQL Leads',
        content: p(`VibeSQL's direct API achieves <strong>~137ns per point select</strong>, matching SQLite and vastly
          outperforming DuckDB (~140µs). Our B-tree implementation is optimized for single-row lookups with
          minimal pointer chasing and cache-friendly node layouts.`),
      },
      {
        title: 'Index Updates: 2x Faster',
        content: p(`VibeSQL's indexed updates run at <strong>~740ns vs SQLite's ~1.6µs</strong>. Our MVCC design
          allows in-place index updates without write-ahead logging overhead for each operation.`),
      },
      {
        title: 'Areas for Improvement',
        content: bullets([
          bullet('Bulk inserts', "SQLite's batch insert path is highly optimized; we're adding batched B-tree operations"),
          bullet('Non-indexed updates', 'Full table scans for non-indexed columns need predicate pushdown optimization'),
          bullet('Deletes', 'Our tombstone-based deletion has cleanup overhead; compaction improvements are planned'),
        ]),
      },
      {
        title: 'DuckDB Comparison',
        content: p(`DuckDB is optimized for analytical workloads, not micro-operations. Its 100-1000x slower
          results here reflect architectural choices (columnar storage, vectorized execution) that
          trade single-row latency for bulk throughput. VibeSQL targets both use cases.`),
      },
    ]),
  },
  'sysbench-server': {
    id: 'sysbench-server',
    name: 'Sysbench (Server)',
    dataFile: 'sysbench_results.json',
    opsLabel: 'Sysbench operations',
    descriptions: {
      'point_select': 'Point Select - Single row lookup by primary key',
      'insert': 'Insert - Insert new rows into table',
      'update_index': 'Update Index - Update indexed column (k = k + 1)',
      'update_non_index': 'Update Non-Index - Update non-indexed column',
      'delete': 'Delete - Remove rows by primary key',
      'range_queries': 'Range Queries - Simple, SUM, ORDER BY, and DISTINCT range scans',
    },
    methodology: methodology(
      'Sysbench Micro-Benchmarks (Server)',
      `<strong>Sysbench</strong> server benchmarks compare VibeSQL Server (PostgreSQL wire protocol)
        against MySQL, measuring performance for multi-client database deployments.`,
      [
        li('Mode', 'Server (PostgreSQL wire protocol)'),
        li('Workload Types', 'Point queries, range scans, updates, inserts, deletes'),
        li('Table Size', '10,000 rows per table'),
        li('Protocol Overhead', '~10-50µs per query for wire protocol handling'),
        li('Databases', 'VibeSQL Server, MySQL'),
      ],
      [
        `Server mode uses the PostgreSQL wire protocol, enabling multi-client access and
        compatibility with existing PostgreSQL tooling and drivers.`,
      ]
    ),
    discussion: discussion([
      {
        title: 'PostgreSQL Wire Protocol',
        content: p(`VibeSQL Server implements the PostgreSQL wire protocol, enabling compatibility with
          existing PostgreSQL drivers and tools. This adds ~10-50µs of protocol overhead per query
          compared to embedded mode, but enables multi-client deployments.`),
      },
      {
        title: 'MySQL Comparison',
        content: p(`Server benchmarks compare against MySQL to evaluate VibeSQL as a drop-in replacement
          for traditional client-server databases. Results vary by operation type, with VibeSQL
          showing advantages in read-heavy workloads.`),
      },
      {
        title: 'Server Roadmap',
        content: bullets([
          bullet('Connection pooling', 'Reduce connection establishment overhead for high-throughput scenarios'),
          bullet('Prepared statement caching', 'Server-side caching of query plans across connections'),
          bullet('Extended query protocol', 'Full PostgreSQL extended query protocol support for batch operations'),
        ]),
      },
    ]),
  },
  'footprint-embedded': {
    id: 'footprint-embedded',
    name: 'Footprint (Embedded)',
    dataFile: 'footprint_results.json',
    opsLabel: 'databases compared',
    descriptions: {
      'binary_size': 'Binary Size - Size of the compiled database binary on disk',
      'startup_time': 'Startup Time - Time to cold-start and execute first query',
      'peak_memory': 'Peak Memory - Maximum resident set size during initialization',
    },
    methodology: methodology(
      'Native Binary Footprint',
      `<strong>Embedded footprint benchmarks</strong> measure the resource efficiency of native database binaries,
        comparing binary size, cold startup time, and peak memory usage.`,
      [
        li('Binary Size', 'Size of the compiled native binary in bytes (stripped release build)'),
        li('Startup Time', 'Time from process start to first query result (CREATE TABLE, INSERT, SELECT)'),
        li('Peak Memory', 'Maximum resident set size (RSS) during cold startup'),
        li('Databases', 'VibeSQL, SQLite, DuckDB'),
      ],
      [
        `Native binary footprint is critical for <strong>embedded and edge deployments</strong> where
        binary size, startup latency, and memory consumption directly impact deployment feasibility.`,
      ]
    ),
    discussion: discussion([
      {
        title: 'Binary Size: Middle Ground',
        content: p(`VibeSQL at <strong>~17MB</strong> sits between SQLite (~5MB) and DuckDB (~45MB). This reflects
          our choice to include advanced features (window functions, CTEs, columnar execution) while
          keeping the binary manageable for embedded deployments.`),
      },
      {
        title: 'Startup: Fastest Cold Start',
        content: p(`VibeSQL achieves <strong>~7.7ms cold startup</strong>, slightly faster than SQLite (~8.2ms) and
          significantly faster than DuckDB (~14.6ms). Our minimal initialization path loads only
          essential metadata structures on startup.`),
      },
      {
        title: 'Memory Efficiency',
        content: p(`Peak memory during startup is ~7MB for VibeSQL vs ~3MB for SQLite and ~11MB for DuckDB.
          The difference from SQLite reflects our more sophisticated query optimizer and columnar
          execution infrastructure that's allocated upfront.`),
      },
      {
        title: 'Size Reduction Roadmap',
        content: bullets([
          bullet('Feature flags', 'Compile-time feature selection to exclude unused functionality'),
          bullet('LTO optimization', 'Whole-program link-time optimization for dead code elimination'),
          bullet('Modular builds', 'Separate core engine from optional features (e.g., window functions)'),
        ]),
      },
    ]),
  },
  'footprint-server': {
    id: 'footprint-server',
    name: 'Footprint (Server/WASM)',
    dataFile: 'footprint_results.json',
    opsLabel: 'deployment targets',
    descriptions: {
      'wasm_size': 'WASM Size - Size of the WebAssembly module for browser deployment',
      'wasm_gzip': 'WASM (gzip) - Compressed size for web delivery',
    },
    methodology: methodology(
      'WASM Footprint',
      `<strong>WASM footprint benchmarks</strong> measure the WebAssembly module size for browser deployment,
        critical for web applications where download size impacts user experience.`,
      [
        li('WASM Size', 'Size of the raw WebAssembly module'),
        li('WASM (gzip)', 'Compressed size for HTTP delivery (browsers auto-decompress)'),
        li('WASM (brotli)', 'Brotli-compressed size for optimal web delivery'),
      ],
      [
        `WASM sizes are critical for <strong>web deployments</strong> where download time directly impacts
        time-to-interactive. Gzip sizes are most relevant as browsers automatically decompress gzip content.`,
        `<strong>Note:</strong> VibeSQL WASM is designed for minimal download size while maintaining
        full SQL:1999 compliance in the browser.`,
      ]
    ),
    discussion: discussion([
      {
        title: 'WASM: 2.2MB Compressed',
        content: p(`VibeSQL's WebAssembly module compresses to <strong>~2.2MB gzipped</strong>, enabling fast
          initial page loads. This is a full SQL:1999 database with window functions, CTEs, and
          ACID transactions running entirely in the browser.`),
      },
      {
        title: "What's Included",
        content: bullets([
          '<li>Complete SQL parser and query optimizer</li>',
          '<li>B-tree storage engine with MVCC</li>',
          '<li>Window functions and advanced aggregations</li>',
          '<li>Common table expressions (WITH clause)</li>',
          '<li>Full ACID transaction support</li>',
        ]),
      },
      {
        title: 'Browser Deployment Benefits',
        content: p(`Running SQL in the browser eliminates round-trip latency to servers, enables offline-first
          applications, and keeps sensitive data on the user's device. VibeSQL's WASM build is
          designed for this use case with minimal dependencies and efficient memory usage.`),
      },
      {
        title: 'WASM Roadmap',
        content: bullets([
          bullet('Streaming compilation', 'Start executing while the module downloads'),
          bullet('IndexedDB persistence', 'Durable storage across browser sessions'),
          bullet('Worker thread support', 'Run queries off the main thread for responsive UIs'),
        ]),
      },
    ]),
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
  datetime?: string;
  machine_info?: {
    system?: string;
    python_version?: string;
    git_commit?: string;
  };
  metadata?: {
    suite?: string;
    timestamp?: string;
    git_commit?: string;
    table_size?: string;
  };
}

/**
 * TPC-C benchmark interfaces (different format - uses TPS instead of mean time)
 */
interface TPCCStats {
  tps: number;
  transactions: number;
  duration_ms: number;
  success_rate?: number;
}

interface TPCCBenchmark {
  name: string;
  stats: TPCCStats;
}

interface TPCCResults {
  benchmarks: TPCCBenchmark[];
  datetime?: string;
  metadata?: {
    suite: string;
    timestamp: string;
    git_commit: string;
    scale_factor: string;
  };
  machine_info?: {
    suite: string;
    git_commit: string;
    scale_factor: string;
  };
}

/**
 * TPC-DS benchmark interfaces (now with comparison data from SQLite and DuckDB)
 */
interface TPCDSStats {
  mean: number;
  total: number;
  rows: number;
  status: 'passed' | 'failed' | 'timeout';
  // Optional fields from Criterion data (present when comparison is enabled)
  stddev?: number;
  min?: number;
  max?: number;
  rounds?: number;
}

interface TPCDSBenchmark {
  name: string;
  stats: TPCDSStats;
}

interface TPCDSResults {
  benchmarks: TPCDSBenchmark[];
  metadata: {
    suite: string;
    timestamp: string;
    git_commit: string;
    scale_factor: string;
    total_queries: number;
    passed_queries: number;
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
 * Update the "Last Updated" display with date, time, and optional git commit
 */
function updateLastUpdated(timestamp: string, gitCommit?: string): void {
  const lastUpdatedEl = document.getElementById('last-updated');
  if (!lastUpdatedEl || !timestamp) return;

  const date = new Date(timestamp);
  const dateStr = date.toLocaleDateString();
  const timeStr = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

  if (gitCommit) {
    // Show date, time, and commit hash on two lines
    lastUpdatedEl.innerHTML = `${dateStr} ${timeStr}<br/><span class="text-sm font-mono text-gray-500 dark:text-gray-400">${gitCommit}</span>`;
  } else {
    lastUpdatedEl.textContent = `${dateStr} ${timeStr}`;
  }
  lastUpdatedEl.className = 'text-xl font-bold text-primary-light dark:text-primary-dark';
}

/**
 * Update a speedup display element with the calculated speedup value
 */
function updateSpeedupDisplay(elementId: string, avgSpeedup: number): void {
  const el = document.getElementById(elementId);
  if (!el) return;

  // Reset to default styling first
  el.className = 'text-3xl font-bold';

  if (avgSpeedup > 1) {
    el.textContent = `${avgSpeedup.toFixed(2)}x faster`;
    el.className += ' text-green-600 dark:text-green-400';
  } else if (avgSpeedup < 1 && avgSpeedup > 0) {
    const slowerBy = 1 / avgSpeedup;
    el.textContent = `${slowerBy.toFixed(2)}x slower`;
    el.className += ' text-red-600 dark:text-red-400';
  } else if (avgSpeedup === 0) {
    el.textContent = 'N/A';
    el.className += ' text-gray-500 dark:text-gray-400';
  } else {
    el.textContent = `${avgSpeedup.toFixed(2)}x`;
    el.className += ' text-primary-light dark:text-primary-dark';
  }
}

/**
 * Reset summary card headers to default state (for comparison benchmarks)
 */
function resetSummaryCardHeaders(): void {
  const sqliteHeader = document.querySelector('#avg-speedup-sqlite')?.parentElement?.querySelector('h3');
  if (sqliteHeader) sqliteHeader.textContent = 'vs SQLite';
  const sqliteLabelEl = document.getElementById('avg-speedup-sqlite-label');
  if (sqliteLabelEl) sqliteLabelEl.textContent = 'average speedup';

  const duckdbHeader = document.querySelector('#avg-speedup-duckdb')?.parentElement?.querySelector('h3');
  if (duckdbHeader) duckdbHeader.textContent = 'vs DuckDB';
  const duckdbLabelEl = document.getElementById('avg-speedup-duckdb-label');
  if (duckdbLabelEl) duckdbLabelEl.textContent = 'average speedup';
}

/**
 * Update both speedup summary cards (vs SQLite and vs DuckDB)
 */
function updateSpeedupSummary(
  sqliteSpeedup: { total: number; count: number },
  duckdbSpeedup: { total: number; count: number }
): void {
  // Reset headers to default first
  resetSummaryCardHeaders();
  // Update SQLite speedup
  if (sqliteSpeedup.count > 0) {
    updateSpeedupDisplay('avg-speedup-sqlite', sqliteSpeedup.total / sqliteSpeedup.count);
  } else {
    const sqliteEl = document.getElementById('avg-speedup-sqlite');
    if (sqliteEl) {
      sqliteEl.textContent = 'N/A';
      sqliteEl.className = 'text-3xl font-bold text-gray-500 dark:text-gray-400';
    }
  }

  // Update DuckDB speedup
  if (duckdbSpeedup.count > 0) {
    updateSpeedupDisplay('avg-speedup-duckdb', duckdbSpeedup.total / duckdbSpeedup.count);
  } else {
    const duckdbEl = document.getElementById('avg-speedup-duckdb');
    if (duckdbEl) {
      duckdbEl.textContent = 'N/A';
      duckdbEl.className = 'text-3xl font-bold text-gray-500 dark:text-gray-400';
    }
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

  // TPC-DS format: "tpcds_q1_vibesql" or "tpcds_q1_store_returns_vibesql"
  if (name.startsWith('tpcds_')) {
    // Extract query number (q1, q2, etc.)
    const queryNum = parts[1]; // e.g., "q1"
    const description = config.descriptions[queryNum];

    // Operation name is everything except "tpcds_", query number, and database
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

  const sqliteSpeedup = { total: 0, count: 0 };
  const duckdbSpeedup = { total: 0, count: 0 };

  for (const [operation, databases] of grouped.entries()) {
    const vibesql = databases.get('vibesql');
    const vibesqlServer = databases.get('vibesql_server');
    const sqlite = databases.get('sqlite');
    const duckdb = databases.get('duckdb');
    const mysql = databases.get('mysql');

    if (!vibesql && !vibesqlServer && !sqlite && !duckdb && !mysql) continue;

    const row = document.createElement('tr');
    row.className = 'hover:bg-gray-100 dark:bg-gray-700/50 transition-colors';

    // Operation name (with tooltip)
    const opCell = document.createElement('td');
    opCell.className = 'px-4 py-3 font-medium text-gray-900 dark:text-gray-100';

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

    // Collect valid times to find the winner
    const times: { name: string; mean: number; cell: HTMLTableCellElement }[] = [];

    // Helper to create a time cell
    const createTimeCell = (
      bench: Benchmark | undefined,
      name: string
    ): HTMLTableCellElement => {
      const cell = document.createElement('td');
      cell.className = 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';

      if (bench && bench.stats.mean > 0) {
        cell.textContent = formatTime(bench.stats.mean, bench.stats.stddev);
        times.push({ name, mean: bench.stats.mean, cell });
      } else if (bench && bench.stats.mean < 0) {
        cell.innerHTML = '<span class="text-red-500" title="Query failed (timeout or error)">FAILED</span>';
      } else {
        cell.textContent = 'N/A';
      }

      return cell;
    };

    // Create cells for each database
    const vibesqlCell = createTimeCell(vibesql, 'vibesql');
    row.appendChild(vibesqlCell);

    const sqliteCell = createTimeCell(sqlite, 'sqlite');
    row.appendChild(sqliteCell);

    const duckdbCell = createTimeCell(duckdb, 'duckdb');
    row.appendChild(duckdbCell);

    // MySQL time - only show for sysbench-server
    if (suite === 'sysbench-server') {
      const mysqlCell = createTimeCell(mysql, 'mysql');
      row.appendChild(mysqlCell);
    }

    // Find and highlight the winner (fastest time)
    if (times.length > 0) {
      const winner = times.reduce((min, t) => t.mean < min.mean ? t : min);
      winner.cell.className = CELL_CLASSES.winner;
    }

    // Track speedup for summary cards (VibeSQL vs SQLite and DuckDB)
    if (vibesql && vibesql.stats.mean > 0) {
      if (sqlite && sqlite.stats.mean > 0) {
        sqliteSpeedup.total += calculateSpeedup(vibesql.stats.mean, sqlite.stats.mean);
        sqliteSpeedup.count++;
      }
      if (duckdb && duckdb.stats.mean > 0) {
        duckdbSpeedup.total += calculateSpeedup(vibesql.stats.mean, duckdb.stats.mean);
        duckdbSpeedup.count++;
      }
    }

    tbody.appendChild(row);
  }

  // Update summary cards
  updateSpeedupSummary(sqliteSpeedup, duckdbSpeedup);

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

  // Build datasets array - MySQL only for sysbench-server
  const datasets = [
    createDataset('vibesql', vibesqlData),
    createDataset('sqlite', sqliteData),
    createDataset('duckdb', duckdbData),
  ];

  // Only add MySQL dataset for sysbench-server
  if (suite === 'sysbench-server') {
    datasets.push(createDataset('mysql', mysqlData));
  }

  currentChart = new Chart(canvas, {
    type: 'bar',
    data: { labels, datasets },
    options: getLogScaleChartOptions('Time (ms) - Log Scale'),
  });
}

// formatBytes and formatMemory are now imported from './utils/measurement'


/**
 * Render footprint embedded table (native binary metrics only)
 */
function renderFootprintEmbeddedTable(data: FootprintResults): void {
  const tbody = document.getElementById('results-tbody');
  const table = document.getElementById('results-table');
  if (!tbody || !table) return;

  // Update table headers for embedded footprint view
  const thead = table.querySelector('thead tr');
  if (thead) {
    thead.innerHTML = `
      <th class="px-4 py-3">Database</th>
      <th class="px-4 py-3 text-right">Binary Size</th>
      <th class="px-4 py-3 text-right">Startup Time</th>
      <th class="px-4 py-3 text-right">Peak Memory</th>
      <th class="px-4 py-3 text-right">Version</th>
      <th class="px-4 py-3 text-center">Best</th>
    `;
  }

  tbody.innerHTML = '';

  // Filter to available benchmarks
  const availableBenchmarks = data.benchmarks.filter(b => b.available);
  const minBinarySize = Math.min(...availableBenchmarks.map(b => b.binary_size_bytes));
  const minStartupTime = Math.min(...availableBenchmarks.map(b => b.startup_time_ms));
  const minMemory = Math.min(...availableBenchmarks.map(b => b.peak_memory_kb));

  for (const benchmark of availableBenchmarks) {
    const row = document.createElement('tr');
    row.className = 'hover:bg-gray-100 dark:bg-gray-700/50 transition-colors';

    // Database name
    const dbCell = document.createElement('td');
    dbCell.className = 'px-4 py-3 font-medium text-gray-900 dark:text-gray-100';
    dbCell.textContent = DB_DISPLAY_NAMES[benchmark.database] || benchmark.database;
    row.appendChild(dbCell);

    // Binary size
    const sizeCell = document.createElement('td');
    sizeCell.className = 'px-4 py-3 text-right';
    const isBestSize = benchmark.binary_size_bytes === minBinarySize;
    sizeCell.innerHTML = isBestSize
      ? `<span class="text-green-600 dark:text-green-400 font-semibold">${formatBytes(benchmark.binary_size_bytes)}</span>`
      : `<span class="text-gray-500 dark:text-gray-400">${formatBytes(benchmark.binary_size_bytes)}</span>`;
    row.appendChild(sizeCell);

    // Startup time
    const startupCell = document.createElement('td');
    startupCell.className = 'px-4 py-3 text-right';
    const isBestStartup = benchmark.startup_time_ms === minStartupTime;
    const startupText = `${benchmark.startup_time_ms.toFixed(2)} ms`;
    startupCell.innerHTML = isBestStartup
      ? `<span class="text-green-600 dark:text-green-400 font-semibold">${startupText}</span>`
      : `<span class="text-gray-500 dark:text-gray-400">${startupText}</span>`;
    row.appendChild(startupCell);

    // Peak memory
    const memCell = document.createElement('td');
    memCell.className = 'px-4 py-3 text-right';
    const isBestMem = benchmark.peak_memory_kb === minMemory;
    memCell.innerHTML = isBestMem
      ? `<span class="text-green-600 dark:text-green-400 font-semibold">${formatMemory(benchmark.peak_memory_kb)}</span>`
      : `<span class="text-gray-500 dark:text-gray-400">${formatMemory(benchmark.peak_memory_kb)}</span>`;
    row.appendChild(memCell);

    // Version
    const versionCell = document.createElement('td');
    versionCell.className = 'px-4 py-3 text-right text-gray-500 dark:text-gray-400 text-xs';
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

  // Update summary cards for footprint embedded
  const vibesql = data.benchmarks.find(b => b.database === 'vibesql');
  const sqlite = data.benchmarks.find(b => b.database === 'sqlite');
  const duckdb = data.benchmarks.find(b => b.database === 'duckdb');

  // SQLite comparison (startup time)
  const sqliteEl = document.getElementById('avg-speedup-sqlite');
  if (sqliteEl && vibesql && sqlite && vibesql.available && sqlite.available) {
    const startupSpeedup = sqlite.startup_time_ms / vibesql.startup_time_ms;
    if (startupSpeedup > 1) {
      sqliteEl.textContent = `${startupSpeedup.toFixed(2)}x faster`;
      sqliteEl.className = 'text-3xl font-bold text-green-600 dark:text-green-400';
    } else {
      const slower = 1 / startupSpeedup;
      sqliteEl.textContent = `${slower.toFixed(2)}x slower`;
      sqliteEl.className = 'text-3xl font-bold text-red-600 dark:text-red-400';
    }
  }
  const sqliteLabelEl = document.getElementById('avg-speedup-sqlite-label');
  if (sqliteLabelEl) sqliteLabelEl.textContent = 'startup time';

  // DuckDB comparison (startup time)
  const duckdbEl = document.getElementById('avg-speedup-duckdb');
  if (duckdbEl && vibesql && duckdb && vibesql.available && duckdb.available) {
    const startupSpeedup = duckdb.startup_time_ms / vibesql.startup_time_ms;
    if (startupSpeedup > 1) {
      duckdbEl.textContent = `${startupSpeedup.toFixed(2)}x faster`;
      duckdbEl.className = 'text-3xl font-bold text-green-600 dark:text-green-400';
    } else {
      const slower = 1 / startupSpeedup;
      duckdbEl.textContent = `${slower.toFixed(2)}x slower`;
      duckdbEl.className = 'text-3xl font-bold text-red-600 dark:text-red-400';
    }
  }
  const duckdbLabelEl = document.getElementById('avg-speedup-duckdb-label');
  if (duckdbLabelEl) duckdbLabelEl.textContent = 'startup time';

  const opsTestedEl = document.getElementById('ops-tested');
  if (opsTestedEl) {
    opsTestedEl.textContent = availableBenchmarks.length.toString();
  }
}

/**
 * Render footprint embedded chart
 */
function renderFootprintEmbeddedChart(data: FootprintResults): void {
  const canvas = document.getElementById('performance-chart') as HTMLCanvasElement;
  if (!canvas) return;

  if (currentChart) {
    currentChart.destroy();
    currentChart = null;
  }

  const availableBenchmarks = data.benchmarks.filter(b => b.available);

  const labels = ['Binary Size (MB)', 'Startup Time (ms)', 'Peak Memory (MB)'];

  const datasets = availableBenchmarks.map(bench => ({
    label: DB_DISPLAY_NAMES[bench.database] || bench.database,
    data: [
      bench.binary_size_bytes / (1024 * 1024),
      bench.startup_time_ms,
      bench.peak_memory_kb / 1024,
    ],
    backgroundColor: DB_COLORS[bench.database]?.bg || 'rgba(156, 163, 175, 0.5)',
    borderColor: DB_COLORS[bench.database]?.border || 'rgba(156, 163, 175, 1)',
    borderWidth: 1,
  }));

  currentChart = new Chart(canvas, {
    type: 'bar',
    data: { labels, datasets },
    options: getLinearChartOptions('Value (lower is better)'),
  });
}

/**
 * Render footprint server table (WASM metrics only)
 */
function renderFootprintServerTable(data: FootprintResults): void {
  const tbody = document.getElementById('results-tbody');
  const table = document.getElementById('results-table');
  if (!tbody || !table) return;

  // Update table headers for server/WASM footprint view
  const thead = table.querySelector('thead tr');
  if (thead) {
    thead.innerHTML = `
      <th class="px-4 py-3">Metric</th>
      <th class="px-4 py-3 text-right">Value</th>
      <th class="px-4 py-3 text-right">Notes</th>
    `;
  }

  tbody.innerHTML = '';

  // Find VibeSQL which has WASM data
  const vibesql = data.benchmarks.find(b => b.database === 'vibesql' && b.available);

  if (!vibesql || !vibesql.wasm_size_bytes) {
    const row = document.createElement('tr');
    row.innerHTML = `<td colspan="3" class="px-4 py-8 text-center text-gray-500 dark:text-gray-400">No WASM data available</td>`;
    tbody.appendChild(row);
    return;
  }

  const metrics = [
    {
      name: 'WASM Size (raw)',
      value: formatBytes(vibesql.wasm_size_bytes),
      note: 'Uncompressed WebAssembly module',
    },
    {
      name: 'WASM Size (gzip)',
      value: vibesql.wasm_size_gzip_bytes ? formatBytes(vibesql.wasm_size_gzip_bytes) : 'N/A',
      note: vibesql.wasm_size_gzip_bytes && vibesql.wasm_size_bytes
        ? `${((vibesql.wasm_size_gzip_bytes / vibesql.wasm_size_bytes) * 100).toFixed(0)}% of raw - typical HTTP delivery`
        : 'Standard HTTP compression',
    },
    {
      name: 'WASM Size (brotli)',
      value: vibesql.wasm_size_brotli_bytes ? formatBytes(vibesql.wasm_size_brotli_bytes) : 'N/A',
      note: vibesql.wasm_size_brotli_bytes && vibesql.wasm_size_bytes
        ? `${((vibesql.wasm_size_brotli_bytes / vibesql.wasm_size_bytes) * 100).toFixed(0)}% of raw - optimal compression`
        : 'Best compression for CDN delivery',
    },
  ];

  for (const metric of metrics) {
    const row = document.createElement('tr');
    row.className = 'hover:bg-gray-100 dark:bg-gray-700/50 transition-colors';

    const nameCell = document.createElement('td');
    nameCell.className = 'px-4 py-3 font-medium text-gray-900 dark:text-gray-100';
    nameCell.textContent = metric.name;
    row.appendChild(nameCell);

    const valueCell = document.createElement('td');
    valueCell.className = 'px-4 py-3 text-right text-primary-light dark:text-primary-dark font-semibold';
    valueCell.textContent = metric.value;
    row.appendChild(valueCell);

    const noteCell = document.createElement('td');
    noteCell.className = 'px-4 py-3 text-right text-gray-500 dark:text-gray-400 text-sm';
    noteCell.textContent = metric.note;
    row.appendChild(noteCell);

    tbody.appendChild(row);
  }

  // Update summary cards for WASM footprint
  const sqliteEl = document.getElementById('avg-speedup-sqlite');
  if (sqliteEl && vibesql.wasm_size_gzip_bytes) {
    sqliteEl.textContent = formatBytes(vibesql.wasm_size_gzip_bytes);
    sqliteEl.className = 'text-3xl font-bold text-primary-light dark:text-primary-dark';
  }
  // Update the header since this isn't a comparison
  const sqliteHeader = sqliteEl?.parentElement?.querySelector('h3');
  if (sqliteHeader) sqliteHeader.textContent = 'WASM (gzip)';
  const sqliteLabelEl = document.getElementById('avg-speedup-sqlite-label');
  if (sqliteLabelEl) sqliteLabelEl.textContent = 'download size';

  // Show raw WASM size in DuckDB slot
  const duckdbEl = document.getElementById('avg-speedup-duckdb');
  if (duckdbEl && vibesql.wasm_size_bytes) {
    duckdbEl.textContent = formatBytes(vibesql.wasm_size_bytes);
    duckdbEl.className = 'text-3xl font-bold text-primary-light dark:text-primary-dark';
  }
  const duckdbHeader = duckdbEl?.parentElement?.querySelector('h3');
  if (duckdbHeader) duckdbHeader.textContent = 'WASM (raw)';
  const duckdbLabelEl = document.getElementById('avg-speedup-duckdb-label');
  if (duckdbLabelEl) duckdbLabelEl.textContent = 'uncompressed';

  const opsTestedEl = document.getElementById('ops-tested');
  if (opsTestedEl) {
    opsTestedEl.textContent = '3'; // 3 metrics shown
  }

  // Update label below ops tested
  const opsLabelEl = document.getElementById('ops-tested-label');
  if (opsLabelEl) {
    opsLabelEl.textContent = 'size metrics';
  }
}

/**
 * Render footprint server chart (WASM sizes)
 */
function renderFootprintServerChart(data: FootprintResults): void {
  const canvas = document.getElementById('performance-chart') as HTMLCanvasElement;
  if (!canvas) return;

  if (currentChart) {
    currentChart.destroy();
    currentChart = null;
  }

  const vibesql = data.benchmarks.find(b => b.database === 'vibesql' && b.available);

  if (!vibesql || !vibesql.wasm_size_bytes) {
    return;
  }

  const labels = ['Raw', 'Gzip', 'Brotli'];
  const sizes = [
    vibesql.wasm_size_bytes / (1024 * 1024),
    (vibesql.wasm_size_gzip_bytes || 0) / (1024 * 1024),
    (vibesql.wasm_size_brotli_bytes || 0) / (1024 * 1024),
  ];

  currentChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'WASM Size (MB)',
        data: sizes,
        backgroundColor: [
          'rgba(156, 163, 175, 0.5)',
          'rgba(34, 197, 94, 0.5)',
          'rgba(59, 130, 246, 0.5)',
        ],
        borderColor: [
          'rgba(156, 163, 175, 1)',
          'rgba(34, 197, 94, 1)',
          'rgba(59, 130, 246, 1)',
        ],
        borderWidth: 1,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          beginAtZero: true,
          title: { display: true, text: 'Size (MB) - lower is better' },
        },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            label: function (context: any) {
              return `${context.parsed.y.toFixed(2)} MB`;
            },
          },
        },
      },
    },
  });
}

/**
 * Render Sysbench results table (filtered by embedded/server mode)
 */
function renderSysbenchTable(data: BenchmarkResults, suite: BenchmarkSuite): void {
  const tbody = document.getElementById('results-tbody');
  if (!tbody) return;

  const config = SUITE_CONFIGS[suite];
  const grouped = groupBenchmarksByOperation(data.benchmarks, suite);

  tbody.innerHTML = '';

  const sqliteSpeedup = { total: 0, count: 0 };
  const duckdbSpeedup = { total: 0, count: 0 };

  // Determine which databases to show based on mode
  const isServer = suite === 'sysbench-server';
  const primaryDb = isServer ? 'vibesql_server' : 'vibesql';
  const comparisonDb = isServer ? 'mysql' : 'sqlite';

  for (const [operation, databases] of grouped.entries()) {
    const primary = databases.get(primaryDb);
    const comparison = databases.get(comparisonDb);
    const duckdb = isServer ? null : databases.get('duckdb');

    if (!primary && !comparison) continue;

    const row = document.createElement('tr');
    row.className = 'hover:bg-gray-100 dark:bg-gray-700/50 transition-colors';

    // Operation name
    const opCell = document.createElement('td');
    opCell.className = 'px-4 py-3 font-medium text-gray-900 dark:text-gray-100';
    const description = config.descriptions[operation];
    if (description) {
      opCell.innerHTML = `<span class="cursor-help" title="${description}">${operation.replace(/_/g, ' ').toUpperCase()}</span>`;
    } else {
      opCell.textContent = operation.replace(/_/g, ' ').toUpperCase();
    }
    row.appendChild(opCell);

    // Collect valid times to find the winner (lower time = better)
    const times: { mean: number; cell: HTMLTableCellElement }[] = [];

    // Primary database time
    const primaryCell = document.createElement('td');
    primaryCell.className = 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    if (primary && primary.stats.mean > 0) {
      primaryCell.textContent = formatTime(primary.stats.mean, primary.stats.stddev) || 'N/A';
      times.push({ mean: primary.stats.mean, cell: primaryCell });
    } else {
      primaryCell.textContent = 'N/A';
    }
    row.appendChild(primaryCell);

    // Comparison database time
    const compCell = document.createElement('td');
    compCell.className = 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    if (comparison && comparison.stats.mean > 0) {
      compCell.textContent = formatTime(comparison.stats.mean, comparison.stats.stddev) || 'N/A';
      times.push({ mean: comparison.stats.mean, cell: compCell });
    } else {
      compCell.textContent = 'N/A';
    }
    row.appendChild(compCell);

    // DuckDB (only for embedded mode)
    if (!isServer) {
      const duckdbCell = document.createElement('td');
      duckdbCell.className = 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
      if (duckdb && duckdb.stats.mean > 0) {
        duckdbCell.textContent = formatTime(duckdb.stats.mean, duckdb.stats.stddev) || 'N/A';
        times.push({ mean: duckdb.stats.mean, cell: duckdbCell });
      } else {
        duckdbCell.textContent = 'N/A';
      }
      row.appendChild(duckdbCell);
    }

    // Find and highlight the winner (fastest time)
    if (times.length > 0) {
      const winner = times.reduce((min, t) => t.mean < min.mean ? t : min);
      winner.cell.className = CELL_CLASSES.winner;
    }

    // Track speedup for summary cards
    if (primary && primary.stats.mean > 0) {
      // For embedded: compare vs SQLite and DuckDB
      // For server: compare vs MySQL (stored in sqliteSpeedup for consistency)
      if (comparison && comparison.stats.mean > 0) {
        sqliteSpeedup.total += calculateSpeedup(primary.stats.mean, comparison.stats.mean);
        sqliteSpeedup.count++;
      }
      if (!isServer && duckdb && duckdb.stats.mean > 0) {
        duckdbSpeedup.total += calculateSpeedup(primary.stats.mean, duckdb.stats.mean);
        duckdbSpeedup.count++;
      }
    }

    tbody.appendChild(row);
  }

  // Update summary cards
  updateSpeedupSummary(sqliteSpeedup, duckdbSpeedup);

  // Update header labels for server mode (SQLite card shows MySQL comparison)
  if (isServer) {
    const sqliteHeader = document.querySelector('#avg-speedup-sqlite')?.parentElement?.querySelector('h3');
    if (sqliteHeader) sqliteHeader.textContent = 'vs MySQL';
  }

  const opsTestedEl = document.getElementById('ops-tested');
  if (opsTestedEl) {
    opsTestedEl.textContent = grouped.size.toString();
  }
}

/**
 * Render Sysbench chart (filtered by embedded/server mode)
 */
function renderSysbenchChart(data: BenchmarkResults, suite: BenchmarkSuite): void {
  const canvas = document.getElementById('performance-chart') as HTMLCanvasElement;
  if (!canvas) return;

  if (currentChart) {
    currentChart.destroy();
    currentChart = null;
  }

  const grouped = groupBenchmarksByOperation(data.benchmarks, suite);
  const isServer = suite === 'sysbench-server';

  const labels: string[] = [];
  const primaryData: number[] = [];
  const comparisonData: number[] = [];
  const duckdbData: number[] = [];

  const primaryDb = isServer ? 'vibesql_server' : 'vibesql';
  const comparisonDb = isServer ? 'mysql' : 'sqlite';

  for (const [operation, databases] of grouped.entries()) {
    const primary = databases.get(primaryDb);
    const comparison = databases.get(comparisonDb);
    const duckdb = isServer ? null : databases.get('duckdb');

    if (primary || comparison) {
      labels.push(operation.replace(/_/g, ' ').toUpperCase());
      primaryData.push(primary && primary.stats.mean > 0 ? primary.stats.mean * 1000 : 0);
      comparisonData.push(comparison && comparison.stats.mean > 0 ? comparison.stats.mean * 1000 : 0);
      if (!isServer) {
        duckdbData.push(duckdb && duckdb.stats.mean > 0 ? duckdb.stats.mean * 1000 : 0);
      }
    }
  }

  const datasets = [
    createDataset(isServer ? 'vibesql_server' : 'vibesql', primaryData),
    createDataset(isServer ? 'mysql' : 'sqlite', comparisonData),
  ];

  if (!isServer) {
    datasets.push(createDataset('duckdb', duckdbData));
  }

  currentChart = new Chart(canvas, {
    type: 'bar',
    data: { labels, datasets },
    options: getLogScaleChartOptions('Time (ms) - Log Scale'),
  });
}

// formatTps is imported from './utils/measurement' (aliased as formatTPS for compatibility)
const formatTPS = formatTps;

/**
 * Group TPC-C benchmarks by operation
 */
function groupTPCCBenchmarksByOperation(benchmarks: TPCCBenchmark[]): Map<string, Map<string, TPCCBenchmark>> {
  const grouped = new Map<string, Map<string, TPCCBenchmark>>();

  for (const bench of benchmarks) {
    const parts = bench.name.split('_');
    const database = parts[parts.length - 1]; // Last part is database name
    const operation = parts.slice(1, -1).join('_'); // Middle parts are operation

    if (!grouped.has(operation)) {
      grouped.set(operation, new Map());
    }

    grouped.get(operation)!.set(database, bench);
  }

  return grouped;
}

/**
 * Render TPC-C results table (uses TPS instead of execution time)
 */
function renderTPCCTable(data: TPCCResults): void {
  const tbody = document.getElementById('results-tbody');
  const table = document.getElementById('results-table');
  if (!tbody || !table) return;

  // Update table headers for TPC-C view (TPS = higher is better)
  const thead = table.querySelector('thead tr');
  if (thead) {
    thead.innerHTML = `
      <th class="px-4 py-3">Workload</th>
      <th class="px-4 py-3 text-right" title="Transactions per second (higher is better)">VibeSQL (TPS)</th>
      <th class="px-4 py-3 text-right" title="Transactions per second (higher is better)">SQLite (TPS)</th>
    `;
  }

  tbody.innerHTML = '';

  const grouped = groupTPCCBenchmarksByOperation(data.benchmarks);

  const sqliteSpeedup = { total: 0, count: 0 };
  const duckdbSpeedup = { total: 0, count: 0 }; // TPC-C doesn't have DuckDB, but needed for API

  for (const [operation, databases] of grouped.entries()) {
    const vibesql = databases.get('vibesql');
    const sqlite = databases.get('sqlite');

    if (!vibesql && !sqlite) continue;

    const row = document.createElement('tr');
    row.className = 'hover:bg-gray-100 dark:bg-gray-700/50 transition-colors';

    // Operation name
    const opCell = document.createElement('td');
    opCell.className = 'px-4 py-3 font-medium text-gray-900 dark:text-gray-100';
    const config = SUITE_CONFIGS['tpcc'];
    const description = config.descriptions[operation];
    if (description) {
      opCell.innerHTML = `<span class="cursor-help" title="${description}">${operation.replace(/_/g, ' ').toUpperCase()}</span>`;
    } else {
      opCell.textContent = operation.replace(/_/g, ' ').toUpperCase();
    }
    row.appendChild(opCell);

    // For TPS, higher is better - track which is the winner
    const vibesqlTps = vibesql?.stats.tps ?? 0;
    const sqliteTps = sqlite?.stats.tps ?? 0;
    const vibesqlWins = vibesqlTps > sqliteTps && vibesqlTps > 0;
    const sqliteWins = sqliteTps > vibesqlTps && sqliteTps > 0;

    // VibeSQL TPS
    const vibesqlCell = document.createElement('td');
    vibesqlCell.className = vibesqlWins
      ? 'px-4 py-3 text-right font-semibold text-green-600 dark:text-green-400'
      : 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    vibesqlCell.textContent = vibesql ? formatTPS(vibesql.stats.tps) : 'N/A';
    row.appendChild(vibesqlCell);

    // SQLite TPS
    const sqliteCell = document.createElement('td');
    sqliteCell.className = sqliteWins
      ? 'px-4 py-3 text-right font-semibold text-green-600 dark:text-green-400'
      : 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    sqliteCell.textContent = sqlite ? formatTPS(sqlite.stats.tps) : 'N/A';
    row.appendChild(sqliteCell);

    // Track speedup for summary card (TPS = higher is better, so vibesql/sqlite)
    if (vibesql && sqlite && vibesql.stats.tps > 0 && sqlite.stats.tps > 0) {
      const speedup = vibesql.stats.tps / sqlite.stats.tps;
      sqliteSpeedup.total += speedup;
      sqliteSpeedup.count++;
    }

    tbody.appendChild(row);
  }

  // Update summary cards
  updateSpeedupSummary(sqliteSpeedup, duckdbSpeedup);

  const opsTestedEl = document.getElementById('ops-tested');
  if (opsTestedEl) {
    opsTestedEl.textContent = grouped.size.toString();
  }

  // Update last updated timestamp
  const timestamp = data.metadata?.timestamp || data.datetime;
  const gitCommit = data.metadata?.git_commit || data.machine_info?.git_commit;
  if (timestamp) {
    updateLastUpdated(timestamp, gitCommit);
  }
}

/**
 * Render TPC-C comparison chart
 */
function renderTPCCChart(data: TPCCResults): void {
  const canvas = document.getElementById('performance-chart') as HTMLCanvasElement;
  if (!canvas) return;

  // Destroy existing chart if any
  if (currentChart) {
    currentChart.destroy();
    currentChart = null;
  }

  const grouped = groupTPCCBenchmarksByOperation(data.benchmarks);

  const labels: string[] = [];
  const vibesqlData: number[] = [];
  const sqliteData: number[] = [];

  for (const [operation, databases] of grouped.entries()) {
    const vibesql = databases.get('vibesql');
    const sqlite = databases.get('sqlite');

    labels.push(operation.replace(/_/g, ' ').toUpperCase());
    vibesqlData.push(vibesql ? vibesql.stats.tps / 1000 : 0); // Convert to K TPS
    sqliteData.push(sqlite ? sqlite.stats.tps / 1000 : 0);
  }

  currentChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        createDataset('vibesql', vibesqlData),
        createDataset('sqlite', sqliteData),
      ],
    },
    options: {
      ...getLinearChartOptions('Transactions per Second (K TPS) - Higher is Better'),
      plugins: {
        legend: { display: true, position: 'top' },
        tooltip: {
          callbacks: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            label: (context: any) => `${context.dataset.label}: ${(context.parsed.y * 1000).toLocaleString()} TPS`,
          },
        },
      },
    },
  });
}

/**
 * Parse TPC-DS benchmark name to extract query number
 */
function parseTPCDSBenchmarkName(name: string): { queryNum: string; database: string } {
  // Format: "tpcds_q1_vibesql" or "tpcds_q10_vibesql"
  const parts = name.split('_');
  const database = parts[parts.length - 1];
  const queryNum = parts[1]; // e.g., "q1", "q10"
  return { queryNum, database };
}

/**
 * Render TPC-DS results table (VibeSQL only, shows pass/fail status)
 */
function renderTPCDSTable(data: TPCDSResults): void {
  const tbody = document.getElementById('results-tbody');
  const table = document.getElementById('results-table');
  if (!tbody || !table) return;

  const config = SUITE_CONFIGS['tpcds'];

  // Update table headers for TPC-DS view
  const thead = table.querySelector('thead tr');
  if (thead) {
    thead.innerHTML = `
      <th class="px-4 py-3">Query</th>
      <th class="px-4 py-3 text-right">Execution Time</th>
      <th class="px-4 py-3 text-right">Rows</th>
      <th class="px-4 py-3 text-center">Status</th>
    `;
  }

  tbody.innerHTML = '';

  // Sort benchmarks by query number
  const sortedBenchmarks = [...data.benchmarks].sort((a, b) => {
    const aNum = parseInt(parseTPCDSBenchmarkName(a.name).queryNum.replace('q', ''));
    const bNum = parseInt(parseTPCDSBenchmarkName(b.name).queryNum.replace('q', ''));
    return aNum - bNum;
  });

  let passedCount = 0;

  for (const bench of sortedBenchmarks) {
    const { queryNum } = parseTPCDSBenchmarkName(bench.name);
    const isPassed = bench.stats.status === 'passed';
    if (isPassed) passedCount++;

    const row = document.createElement('tr');
    row.className = 'hover:bg-gray-100 dark:bg-gray-700/50 transition-colors';

    // Query name with description tooltip
    const queryCell = document.createElement('td');
    queryCell.className = 'px-4 py-3 font-medium text-gray-900 dark:text-gray-100';
    const description = config.descriptions[queryNum];
    if (description) {
      queryCell.innerHTML = `<span class="cursor-help" title="${description}">TPC-DS ${queryNum.toUpperCase()}</span>`;
    } else {
      queryCell.textContent = `TPC-DS ${queryNum.toUpperCase()}`;
    }
    row.appendChild(queryCell);

    // Execution time
    const timeCell = document.createElement('td');
    timeCell.className = 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    if (isPassed && bench.stats.mean > 0) {
      timeCell.textContent = formatTime(bench.stats.mean, bench.stats.stddev) || 'N/A';
    } else {
      timeCell.textContent = '-';
    }
    row.appendChild(timeCell);

    // Rows returned
    const rowsCell = document.createElement('td');
    rowsCell.className = 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    rowsCell.textContent = bench.stats.rows.toLocaleString();
    row.appendChild(rowsCell);

    // Status
    const statusCell = document.createElement('td');
    statusCell.className = 'px-4 py-3 text-center text-2xl';
    if (bench.stats.status === 'passed') {
      statusCell.textContent = '✅';
      statusCell.title = 'Query passed';
    } else if (bench.stats.status === 'timeout') {
      statusCell.textContent = '⏱️';
      statusCell.title = 'Query timed out';
    } else {
      statusCell.textContent = '❌';
      statusCell.title = 'Query failed';
    }
    row.appendChild(statusCell);

    tbody.appendChild(row);
  }

  // Update summary cards for TPC-DS (shows pass rate instead of speedup)
  const sqliteEl = document.getElementById('avg-speedup-sqlite');
  if (sqliteEl) {
    const passRate = (passedCount / sortedBenchmarks.length * 100).toFixed(0);
    sqliteEl.textContent = `${passRate}%`;
    sqliteEl.className = 'text-3xl font-bold text-green-600 dark:text-green-400';
  }
  const sqliteHeader = sqliteEl?.parentElement?.querySelector('h3');
  if (sqliteHeader) sqliteHeader.textContent = 'Pass Rate';
  const sqliteLabelEl = document.getElementById('avg-speedup-sqlite-label');
  if (sqliteLabelEl) sqliteLabelEl.textContent = 'queries passing';

  // Show passed/total in DuckDB slot
  const duckdbEl = document.getElementById('avg-speedup-duckdb');
  if (duckdbEl) {
    duckdbEl.textContent = `${passedCount}/${sortedBenchmarks.length}`;
    duckdbEl.className = 'text-3xl font-bold text-primary-light dark:text-primary-dark';
  }
  const duckdbHeader = duckdbEl?.parentElement?.querySelector('h3');
  if (duckdbHeader) duckdbHeader.textContent = 'Queries';
  const duckdbLabelEl = document.getElementById('avg-speedup-duckdb-label');
  if (duckdbLabelEl) duckdbLabelEl.textContent = 'passed / total';

  const opsTestedEl = document.getElementById('ops-tested');
  if (opsTestedEl) {
    opsTestedEl.textContent = `${passedCount}`;
  }

  // Update last updated timestamp
  if (data.metadata.timestamp) {
    updateLastUpdated(data.metadata.timestamp, data.metadata.git_commit);
  }
}

/**
 * Render TPC-DS performance chart
 */
function renderTPCDSChart(data: TPCDSResults): void {
  const canvas = document.getElementById('performance-chart') as HTMLCanvasElement;
  if (!canvas) return;

  // Destroy existing chart if any
  if (currentChart) {
    currentChart.destroy();
    currentChart = null;
  }

  // Sort benchmarks by query number and filter to passed queries
  const sortedBenchmarks = [...data.benchmarks]
    .filter(b => b.stats.status === 'passed' && b.stats.mean > 0)
    .sort((a, b) => {
      const aNum = parseInt(parseTPCDSBenchmarkName(a.name).queryNum.replace('q', ''));
      const bNum = parseInt(parseTPCDSBenchmarkName(b.name).queryNum.replace('q', ''));
      return aNum - bNum;
    });

  const labels = sortedBenchmarks.map(b => {
    const { queryNum } = parseTPCDSBenchmarkName(b.name);
    return queryNum.toUpperCase();
  });

  const vibesqlData = sortedBenchmarks.map(b => b.stats.mean * 1000); // Convert to ms

  currentChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [createDataset('vibesql', vibesqlData)],
    },
    options: {
      ...getLogScaleChartOptions('Execution Time (ms) - Log Scale'),
      scales: {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ...(getLogScaleChartOptions('') as any).scales,
        x: {
          ticks: {
            maxRotation: 90,
            minRotation: 45,
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
 * Update the discussion section
 */
function updateDiscussion(suite: BenchmarkSuite): void {
  const discussionEl = document.getElementById('discussion-content');
  if (discussionEl) {
    discussionEl.innerHTML = SUITE_CONFIGS[suite].discussion;
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
function restoreTPCTableHeaders(suite?: BenchmarkSuite): void {
  const table = document.getElementById('results-table');
  if (!table) return;

  const thead = table.querySelector('thead tr');
  if (thead) {
    // Sysbench embedded shows VibeSQL vs SQLite vs DuckDB
    if (suite === 'sysbench-embedded') {
      thead.innerHTML = `
        <th class="px-4 py-3">Operation</th>
        <th class="px-4 py-3 text-right">VibeSQL</th>
        <th class="px-4 py-3 text-right">SQLite</th>
        <th class="px-4 py-3 text-right">DuckDB</th>
      `;
    // Sysbench server shows VibeSQL Server vs MySQL
    } else if (suite === 'sysbench-server') {
      thead.innerHTML = `
        <th class="px-4 py-3">Operation</th>
        <th class="px-4 py-3 text-right" title="VibeSQL via PostgreSQL wire protocol">VibeSQL Server</th>
        <th class="px-4 py-3 text-right">MySQL</th>
      `;
    } else {
      // TPC-H, TPC-DS, TPC-C: VibeSQL vs SQLite vs DuckDB (embedded databases only)
      thead.innerHTML = `
        <th class="px-4 py-3">Operation</th>
        <th class="px-4 py-3 text-right">VibeSQL</th>
        <th class="px-4 py-3 text-right">SQLite</th>
        <th class="px-4 py-3 text-right">DuckDB</th>
      `;
    }
  }
}

/**
 * Load and display benchmark data for a specific suite
 */
async function loadBenchmarkData(suite: BenchmarkSuite): Promise<void> {
  const config = SUITE_CONFIGS[suite];

  // Update methodology, discussion, and ops label
  updateMethodology(suite);
  updateDiscussion(suite);
  updateOpsLabel(suite);

  // Restore TPC headers if not footprint
  if (!suite.startsWith('footprint')) {
    restoreTPCTableHeaders(suite);
  }

  try {
    const response = await fetch(`${import.meta.env.BASE_URL}benchmarks/${config.dataFile}`);

    if (!response.ok) {
      throw new Error(`Failed to load benchmark data: ${response.status}`);
    }

    // Handle footprint-embedded suite (native binary metrics)
    if (suite === 'footprint-embedded') {
      const data: FootprintResults = await response.json();

      if (data.datetime) {
        updateLastUpdated(data.datetime);
      }

      renderFootprintEmbeddedTable(data);
      renderFootprintEmbeddedChart(data);
      return;
    }

    // Handle footprint-server suite (WASM metrics)
    if (suite === 'footprint-server') {
      const data: FootprintResults = await response.json();

      if (data.datetime) {
        updateLastUpdated(data.datetime);
      }

      renderFootprintServerTable(data);
      renderFootprintServerChart(data);
      return;
    }

    // Handle TPC-C suite differently (uses TPS instead of mean execution time)
    if (suite === 'tpcc') {
      const data: TPCCResults = await response.json();
      renderTPCCTable(data);
      renderTPCCChart(data);
      return;
    }

    // Handle TPC-DS suite - now with comparison data (VibeSQL, SQLite, DuckDB)
    if (suite === 'tpcds') {
      const data: TPCDSResults = await response.json();

      // Check if comparison data exists (look for sqlite or duckdb entries)
      const hasComparison = data.benchmarks.some(b =>
        b.name.endsWith('_sqlite') || b.name.endsWith('_duckdb') || b.name.endsWith('_mysql')
      );

      if (hasComparison) {
        // Use standard comparison rendering (like TPC-H)
        const benchmarkResults: BenchmarkResults = {
          benchmarks: data.benchmarks.map(b => ({
            name: b.name,
            stats: {
              mean: b.stats.mean,
              stddev: b.stats.stddev ?? 0,
              min: b.stats.min ?? b.stats.mean * 0.95,
              max: b.stats.max ?? b.stats.mean * 1.05,
              rounds: b.stats.rounds ?? 100,
            }
          })),
          datetime: data.metadata.timestamp,
        };

        // Update last updated timestamp
        if (data.metadata.timestamp) {
          updateLastUpdated(data.metadata.timestamp, data.metadata.git_commit);
        }

        renderResultsTable(benchmarkResults, suite);
        renderChart(benchmarkResults, suite);
      } else {
        // Fall back to VibeSQL-only rendering
        renderTPCDSTable(data);
        renderTPCDSChart(data);
      }
      return;
    }

    // Handle Sysbench suites - filter by embedded vs server databases
    if (suite === 'sysbench-embedded' || suite === 'sysbench-server') {
      const data: BenchmarkResults = await response.json();

      // Update last updated timestamp
      const timestamp = data.metadata?.timestamp || data.datetime;
      const gitCommit = data.metadata?.git_commit || data.machine_info?.git_commit;
      if (timestamp) {
        updateLastUpdated(timestamp, gitCommit);
      }

      // Filter benchmarks based on mode
      const filteredData: BenchmarkResults = {
        ...data,
        benchmarks: data.benchmarks.filter(b => {
          if (suite === 'sysbench-embedded') {
            // Keep vibesql, sqlite, duckdb (exclude vibesql_server and mysql)
            return b.name.endsWith('_vibesql') ||
                   b.name.endsWith('_sqlite') ||
                   b.name.endsWith('_duckdb');
          } else {
            // Server mode: keep vibesql_server and mysql
            return b.name.endsWith('_vibesql_server') ||
                   b.name.endsWith('_mysql');
          }
        }),
      };

      renderSysbenchTable(filteredData, suite);
      renderSysbenchChart(filteredData, suite);
      return;
    }

    // Standard TPC benchmark handling (TPC-H with comparisons)
    const data: BenchmarkResults = await response.json();

    // Update last updated timestamp (support both datetime and metadata.timestamp formats)
    const timestamp = data.metadata?.timestamp || data.datetime;
    const gitCommit = data.metadata?.git_commit || data.machine_info?.git_commit;
    if (timestamp) {
      updateLastUpdated(timestamp, gitCommit);
    }

    renderResultsTable(data, suite);
    renderChart(data, suite);
  } catch (error) {
    console.error('Error loading benchmark data:', error);

    const tbody = document.getElementById('results-tbody');
    if (tbody) {
      tbody.innerHTML = `
        <tr>
          <td colspan="7" class="px-4 py-8 text-center text-gray-500 dark:text-gray-400">
            <p class="mb-2">No ${config.name} benchmark results available yet.</p>
            <p class="text-sm">Results will be generated when CI runs for this benchmark suite.</p>
          </td>
        </tr>
      `;
    }

    const sqliteEl = document.getElementById('avg-speedup-sqlite');
    if (sqliteEl) {
      sqliteEl.textContent = 'N/A';
      sqliteEl.className = 'text-3xl font-bold text-gray-500 dark:text-gray-400';
    }
    const duckdbEl = document.getElementById('avg-speedup-duckdb');
    if (duckdbEl) {
      duckdbEl.textContent = 'N/A';
      duckdbEl.className = 'text-3xl font-bold text-gray-500 dark:text-gray-400';
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
      // Map tab IDs to suite names
      const tabId = tab.id;
      let suite: BenchmarkSuite;

      // Handle the new tab ID format
      if (tabId === 'tab-sysbench-embedded') {
        suite = 'sysbench-embedded';
      } else if (tabId === 'tab-sysbench-server') {
        suite = 'sysbench-server';
      } else if (tabId === 'tab-footprint-embedded') {
        suite = 'footprint-embedded';
      } else if (tabId === 'tab-footprint-server') {
        suite = 'footprint-server';
      } else {
        suite = tabId.replace('tab-', '') as BenchmarkSuite;
      }

      // Update active state - clear all tabs
      tabs.forEach((t) => {
        t.classList.remove('benchmark-tab--active');
        t.setAttribute('aria-selected', 'false');
      });
      tab.classList.add('benchmark-tab--active');
      tab.setAttribute('aria-selected', 'true');

      // Load new data
      currentSuite = suite;
      loadBenchmarkData(suite);
    });
  });
}

// Initialize page
document.addEventListener('DOMContentLoaded', () => {
  // Initialize theme system
  const theme = initTheme();

  // Initialize locale system
  const locale = initLocale();

  // Initialize navigation component with theme and locale
  new NavigationComponent('benchmarks', theme, locale);

  // Initialize tabs
  initTabs();

  // Load benchmark data for default suite (TPC-H)
  loadBenchmarkData(currentSuite);
});
