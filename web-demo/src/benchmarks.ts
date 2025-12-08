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
import { initI18n, setI18nLocale, updateDOM, t } from './i18n';

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

/** Generate a bullet list item with i18n key */
const bulletI18n = (labelKey: string, descKey: string): string =>
  `<li><strong>${t(labelKey)}:</strong> ${t(descKey)}</li>`;

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
    ${li(t('bench-hardware'), HARDWARE)}
    ${details.join('\n    ')}
  </ul>
  ${notes?.map(note => `
  <p class="mt-4 text-gray-500 dark:text-gray-400 text-sm">${note}</p>`).join('') ?? ''}
`;

/** Generate discussion section with multiple subsections */
const discussion = (sections: { title: string; content: string }[]): string => `
  <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">${t('bench-analysis-roadmap')}</h3>
  ${sections.map(({ title, content }) => `
  <h4 class="text-md font-medium text-gray-900 dark:text-gray-100 mt-4 mb-2">${title}</h4>
  ${content}`).join('')}
`;

/** Generate a paragraph from i18n key */
const pI18n = (key: string): string =>
  `<p class="text-gray-500 dark:text-gray-400 mb-2">${t(key)}</p>`;

/** Generate a bullet list for discussion items */
const bullets = (items: string[]): string =>
  `<ul class="list-disc list-inside space-y-1 text-gray-500 dark:text-gray-400 text-sm ml-2">
    ${items.join('\n    ')}
  </ul>`;

/** Generate a bullet list from i18n keys */
const bulletsI18n = (itemKeys: { labelKey: string; descKey: string }[]): string =>
  `<ul class="list-disc list-inside space-y-1 text-gray-500 dark:text-gray-400 text-sm ml-2">
    ${itemKeys.map(k => bulletI18n(k.labelKey, k.descKey)).join('\n    ')}
  </ul>`;

// ============================================================================
// Suite Configuration
// ============================================================================

/**
 * Suite configuration - methodology and discussion are now functions
 * that generate content on-demand so i18n translations work correctly
 */
interface SuiteConfig {
  id: BenchmarkSuite;
  name: string;
  nameKey: string;  // i18n key for display name
  dataFile: string;
  opsLabel: string;
  opsLabelKey: string;  // i18n key for ops label
  descriptions: Record<string, string>;
  descriptionKeys?: Record<string, string>;  // i18n keys for descriptions
  getMethodology: () => string;  // Dynamic generation for i18n
  getDiscussion: () => string;   // Dynamic generation for i18n
}

/**
 * Suite configurations
 */
const SUITE_CONFIGS: Record<BenchmarkSuite, SuiteConfig> = {
  tpch: {
    id: 'tpch',
    name: 'TPC-H',
    nameKey: 'bench-tpch-name',
    dataFile: 'benchmark_results.json',
    opsLabel: 'TPC-H queries',
    opsLabelKey: 'bench-tpch-ops-label',
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
    descriptionKeys: {
      'q1': 'bench-tpch-q1', 'q2': 'bench-tpch-q2', 'q3': 'bench-tpch-q3', 'q4': 'bench-tpch-q4',
      'q5': 'bench-tpch-q5', 'q6': 'bench-tpch-q6', 'q7': 'bench-tpch-q7', 'q8': 'bench-tpch-q8',
      'q9': 'bench-tpch-q9', 'q10': 'bench-tpch-q10', 'q11': 'bench-tpch-q11', 'q12': 'bench-tpch-q12',
      'q13': 'bench-tpch-q13', 'q14': 'bench-tpch-q14', 'q15': 'bench-tpch-q15', 'q16': 'bench-tpch-q16',
      'q17': 'bench-tpch-q17', 'q18': 'bench-tpch-q18', 'q19': 'bench-tpch-q19', 'q20': 'bench-tpch-q20',
      'q21': 'bench-tpch-q21', 'q22': 'bench-tpch-q22',
    },
    getMethodology: () => methodology(
      t('bench-tpch-title'),
      t('bench-tpch-description'),
      [
        li(t('bench-benchmark-framework'), 'Criterion.rs (Rust native benchmarking)'),
        li(t('bench-scale-factor'), 'SF 0.01 (~60,000 rows across 8 tables)'),
        li(t('bench-data'), 'Deterministic TPC-H compliant dataset'),
        li(t('bench-databases-tested'), 'VibeSQL, SQLite (via rusqlite), DuckDB (via duckdb-rs)'),
        li(t('bench-execution-mode'), 'All databases run in-memory (no disk I/O)'),
        li(t('bench-measurement'), 'Native Rust API calls (no Python/FFI overhead)'),
      ],
      [t('bench-tpch-note-intro'), t('bench-tpch-note-queries')]
    ),
    getDiscussion: () => discussion([
      {
        title: t('bench-tpch-disc-excels-title'),
        content: pI18n('bench-tpch-disc-excels'),
      },
      {
        title: t('bench-tpch-disc-targets-title'),
        content: pI18n('bench-tpch-disc-targets') + bulletsI18n([
          { labelKey: 'bench-bullet-join-ordering', descKey: 'bench-tpch-disc-join-ordering' },
          { labelKey: 'bench-bullet-hash-sizing', descKey: 'bench-tpch-disc-hash-sizing' },
          { labelKey: 'bench-bullet-vectorized', descKey: 'bench-tpch-disc-vectorized' },
          { labelKey: 'bench-bullet-inl-joins', descKey: 'bench-tpch-disc-inl-joins' },
        ]),
      },
      {
        title: t('bench-tpch-disc-path-title'),
        content: pI18n('bench-tpch-disc-path'),
      },
    ]),
  },
  tpcds: {
    id: 'tpcds',
    name: 'TPC-DS',
    nameKey: 'bench-tpcds-name',
    dataFile: 'tpcds_results.json',
    opsLabel: 'TPC-DS queries',
    opsLabelKey: 'bench-tpcds-ops-label',
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
    getMethodology: () => methodology(
      t('bench-tpcds-title'),
      t('bench-tpcds-description'),
      [
        li(t('bench-schema'), '24 tables with star/snowflake schema design'),
        li(t('bench-query-count'), '99 queries (all 99 passing)'),
        li(t('bench-scale-factor'), 'SF 0.01'),
        li(t('bench-query-types'), 'Reporting, ad-hoc, data mining patterns'),
        li(t('bench-sql-features'), 'Window functions, CTEs, complex subqueries, ROLLUP/CUBE'),
      ],
      [t('bench-tpcds-note-intro'), t('bench-tpcds-note-remaining')]
    ),
    getDiscussion: () => discussion([
      {
        title: t('bench-tpcds-disc-coverage-title'),
        content: pI18n('bench-tpcds-disc-coverage'),
      },
      {
        title: t('bench-tpcds-disc-optimization-title'),
        content: pI18n('bench-tpcds-disc-optimization') + bulletsI18n([
          { labelKey: 'bench-bullet-cte-materialization', descKey: 'bench-tpcds-disc-cte' },
          { labelKey: 'bench-bullet-decorrelation', descKey: 'bench-tpcds-disc-decorrelation' },
          { labelKey: 'bench-bullet-star-optimization', descKey: 'bench-tpcds-disc-star' },
        ]),
      },
      {
        title: t('bench-tpcds-disc-toward-title'),
        content: pI18n('bench-tpcds-disc-toward'),
      },
    ]),
  },
  tpcc: {
    id: 'tpcc',
    name: 'TPC-C',
    nameKey: 'bench-tpcc-name',
    dataFile: 'tpcc_results.json',
    opsLabel: 'TPC-C transactions',
    opsLabelKey: 'bench-tpcc-ops-label',
    descriptions: {
      'new_order': 'New Order - Complex transaction with inventory checks and order creation',
      'payment': 'Payment - Update customer balance and warehouse/district totals',
      'order_status': 'Order Status - Read-only query for customer order history',
      'delivery': 'Delivery - Batch processing of pending orders',
      'stock_level': 'Stock Level - Count items below threshold in recent orders',
    },
    descriptionKeys: {
      'new_order': 'bench-tpcc-new-order',
      'payment': 'bench-tpcc-payment',
      'order_status': 'bench-tpcc-order-status',
      'delivery': 'bench-tpcc-delivery',
      'stock_level': 'bench-tpcc-stock-level',
    },
    getMethodology: () => methodology(
      t('bench-tpcc-title'),
      t('bench-tpcc-description'),
      [
        li(t('bench-workload'), 'OLTP (Online Transaction Processing)'),
        li(t('bench-transaction-mix'), '45% New Order, 43% Payment, 4% Order Status, 4% Delivery, 4% Stock Level'),
        li(t('bench-warehouses'), '1 warehouse (scaled for in-memory testing)'),
        li(t('bench-concurrency'), 'Single-threaded baseline measurements'),
        li(t('bench-acid-compliance'), 'Full transaction isolation testing'),
      ],
      [t('bench-tpcc-note-intro'), t('bench-tpcc-note-results')]
    ),
    getDiscussion: () => discussion([
      {
        title: t('bench-tpcc-disc-faster-title'),
        content: pI18n('bench-tpcc-disc-faster'),
      },
      {
        title: t('bench-tpcc-disc-dominates-title'),
        content: bulletsI18n([
          { labelKey: 'bench-bullet-lock-free', descKey: 'bench-tpcc-disc-lockfree' },
          { labelKey: 'bench-bullet-optimistic', descKey: 'bench-tpcc-disc-optimistic' },
          { labelKey: 'bench-bullet-btree', descKey: 'bench-tpcc-disc-btree' },
          { labelKey: 'bench-bullet-prepared', descKey: 'bench-tpcc-disc-prepared' },
        ]),
      },
      {
        title: t('bench-tpcc-disc-scaling-title'),
        content: pI18n('bench-tpcc-disc-scaling'),
      },
      {
        title: t('bench-tpcc-disc-duckdb-title'),
        content: pI18n('bench-tpcc-disc-duckdb'),
      },
    ]),
  },
  'sysbench-embedded': {
    id: 'sysbench-embedded',
    name: 'Sysbench (Embedded)',
    nameKey: 'bench-sysbench-embedded-name',
    dataFile: 'sysbench_results.json',
    opsLabel: 'Sysbench operations',
    opsLabelKey: 'bench-sysbench-embedded-ops-label',
    descriptions: {
      'point_select': 'Point Select - Single row lookup by primary key',
      'insert': 'Insert - Insert new rows into table',
      'update_index': 'Update Index - Update indexed column (k = k + 1)',
      'update_non_index': 'Update Non-Index - Update non-indexed column',
      'delete': 'Delete - Remove rows by primary key',
      'range_queries': 'Range Queries - Simple, SUM, ORDER BY, and DISTINCT range scans',
    },
    descriptionKeys: {
      'point_select': 'bench-sysbench-point-select',
      'insert': 'bench-sysbench-insert',
      'update_index': 'bench-sysbench-update-index',
      'update_non_index': 'bench-sysbench-update-non-index',
      'delete': 'bench-sysbench-delete',
      'range_queries': 'bench-sysbench-range-queries',
    },
    getMethodology: () => methodology(
      t('bench-sysbench-embedded-title'),
      t('bench-sysbench-embedded-description'),
      [
        li(t('bench-mode'), 'Embedded (in-process, zero network overhead)'),
        li(t('bench-workload-types'), 'Point queries, range scans, updates, inserts, deletes'),
        li(t('bench-table-size'), '10,000 rows per table'),
        li(t('bench-index-types'), 'Primary key and secondary indexes'),
        li(t('bench-operations'), 'Single-statement operations (no multi-statement transactions)'),
        li(t('bench-databases'), 'VibeSQL, SQLite, DuckDB'),
      ],
      [t('bench-sysbench-embedded-note')]
    ),
    getDiscussion: () => discussion([
      {
        title: t('bench-sysbench-emb-disc-point-title'),
        content: pI18n('bench-sysbench-emb-disc-point'),
      },
      {
        title: t('bench-sysbench-emb-disc-index-title'),
        content: pI18n('bench-sysbench-emb-disc-index'),
      },
      {
        title: t('bench-sysbench-emb-disc-improve-title'),
        content: bulletsI18n([
          { labelKey: 'bench-bullet-bulk-inserts', descKey: 'bench-sysbench-emb-disc-bulk' },
          { labelKey: 'bench-bullet-non-indexed', descKey: 'bench-sysbench-emb-disc-nonindex' },
          { labelKey: 'bench-bullet-deletes', descKey: 'bench-sysbench-emb-disc-deletes' },
        ]),
      },
      {
        title: t('bench-sysbench-emb-disc-architecture-title'),
        content: pI18n('bench-sysbench-emb-disc-architecture'),
      },
    ]),
  },
  'sysbench-server': {
    id: 'sysbench-server',
    name: 'Sysbench (Server)',
    nameKey: 'bench-sysbench-server-name',
    dataFile: 'sysbench_results.json',
    opsLabel: 'Sysbench operations',
    opsLabelKey: 'bench-sysbench-server-ops-label',
    descriptions: {
      'point_select': 'Point Select - Single row lookup by primary key',
      'insert': 'Insert - Insert new rows into table',
      'update_index': 'Update Index - Update indexed column (k = k + 1)',
      'update_non_index': 'Update Non-Index - Update non-indexed column',
      'delete': 'Delete - Remove rows by primary key',
      'range_queries': 'Range Queries - Simple, SUM, ORDER BY, and DISTINCT range scans',
    },
    descriptionKeys: {
      'point_select': 'bench-sysbench-point-select',
      'insert': 'bench-sysbench-insert',
      'update_index': 'bench-sysbench-update-index',
      'update_non_index': 'bench-sysbench-update-non-index',
      'delete': 'bench-sysbench-delete',
      'range_queries': 'bench-sysbench-range-queries',
    },
    getMethodology: () => methodology(
      t('bench-sysbench-server-title'),
      t('bench-sysbench-server-description'),
      [
        li(t('bench-mode'), 'Server (PostgreSQL wire protocol)'),
        li(t('bench-workload-types'), 'Point queries, range scans, updates, inserts, deletes'),
        li(t('bench-table-size'), '10,000 rows per table'),
        li(t('bench-protocol-overhead'), '~10-50µs per query for wire protocol handling'),
        li(t('bench-databases'), 'VibeSQL Server, MySQL'),
      ],
      [t('bench-sysbench-server-note')]
    ),
    getDiscussion: () => discussion([
      {
        title: t('bench-sysbench-srv-disc-protocol-title'),
        content: pI18n('bench-sysbench-srv-disc-protocol'),
      },
      {
        title: t('bench-sysbench-srv-disc-mysql-title'),
        content: pI18n('bench-sysbench-srv-disc-mysql'),
      },
      {
        title: t('bench-sysbench-srv-disc-roadmap-title'),
        content: bulletsI18n([
          { labelKey: 'bench-bullet-connection-pooling', descKey: 'bench-sysbench-srv-disc-pooling' },
          { labelKey: 'bench-bullet-stmt-caching', descKey: 'bench-sysbench-srv-disc-caching' },
          { labelKey: 'bench-bullet-extended-protocol', descKey: 'bench-sysbench-srv-disc-extended' },
        ]),
      },
    ]),
  },
  'footprint-embedded': {
    id: 'footprint-embedded',
    name: 'Footprint (Embedded)',
    nameKey: 'bench-footprint-embedded-name',
    dataFile: 'footprint_results.json',
    opsLabel: 'databases compared',
    opsLabelKey: 'bench-footprint-embedded-ops-label',
    descriptions: {
      'binary_size': 'Binary Size - Size of the compiled database binary on disk',
      'startup_time': 'Startup Time - Time to cold-start and execute first query',
      'peak_memory': 'Peak Memory - Maximum resident set size during initialization',
    },
    descriptionKeys: {
      'binary_size': 'bench-footprint-binary-size',
      'startup_time': 'bench-footprint-startup-time',
      'peak_memory': 'bench-footprint-peak-memory',
    },
    getMethodology: () => methodology(
      t('bench-footprint-embedded-title'),
      t('bench-footprint-embedded-description'),
      [
        li(t('bench-binary-size'), 'Size of the compiled native binary in bytes (stripped release build)'),
        li(t('bench-startup-time'), 'Time from process start to first query result (CREATE TABLE, INSERT, SELECT)'),
        li(t('bench-peak-memory'), 'Maximum resident set size (RSS) during cold startup'),
        li(t('bench-databases'), 'VibeSQL, SQLite, DuckDB'),
      ],
      [t('bench-footprint-embedded-note')]
    ),
    getDiscussion: () => discussion([
      {
        title: t('bench-footprint-emb-disc-size-title'),
        content: pI18n('bench-footprint-emb-disc-size'),
      },
      {
        title: t('bench-footprint-emb-disc-startup-title'),
        content: pI18n('bench-footprint-emb-disc-startup'),
      },
      {
        title: t('bench-footprint-emb-disc-memory-title'),
        content: pI18n('bench-footprint-emb-disc-memory'),
      },
      {
        title: t('bench-footprint-emb-disc-roadmap-title'),
        content: bulletsI18n([
          { labelKey: 'bench-bullet-feature-flags', descKey: 'bench-footprint-emb-disc-flags' },
          { labelKey: 'bench-bullet-lto', descKey: 'bench-footprint-emb-disc-lto' },
          { labelKey: 'bench-bullet-modular', descKey: 'bench-footprint-emb-disc-modular' },
        ]),
      },
    ]),
  },
  'footprint-server': {
    id: 'footprint-server',
    name: 'Footprint (Server/WASM)',
    nameKey: 'bench-footprint-server-name',
    dataFile: 'footprint_results.json',
    opsLabel: 'deployment targets',
    opsLabelKey: 'bench-footprint-server-ops-label',
    descriptions: {
      'wasm_size': 'WASM Size - Size of the WebAssembly module for browser deployment',
      'wasm_gzip': 'WASM (gzip) - Compressed size for web delivery',
    },
    descriptionKeys: {
      'wasm_size': 'bench-footprint-wasm-size',
      'wasm_gzip': 'bench-footprint-wasm-gzip',
    },
    getMethodology: () => methodology(
      t('bench-footprint-server-title'),
      t('bench-footprint-server-description'),
      [
        li(t('bench-wasm-size'), 'Size of the raw WebAssembly module'),
        li(t('bench-wasm-gzip'), 'Compressed size for HTTP delivery (browsers auto-decompress)'),
        li(t('bench-wasm-brotli'), 'Brotli-compressed size for optimal web delivery'),
      ],
      [t('bench-footprint-server-note'), t('bench-footprint-server-note2')]
    ),
    getDiscussion: () => discussion([
      {
        title: t('bench-footprint-srv-disc-wasm-title'),
        content: pI18n('bench-footprint-srv-disc-wasm'),
      },
      {
        title: t('bench-footprint-srv-disc-included-title'),
        content: bullets([
          `<li>${t('bench-footprint-srv-disc-parser')}</li>`,
          `<li>${t('bench-footprint-srv-disc-btree')}</li>`,
          `<li>${t('bench-footprint-srv-disc-window')}</li>`,
          `<li>${t('bench-footprint-srv-disc-cte')}</li>`,
          `<li>${t('bench-footprint-srv-disc-acid')}</li>`,
        ]),
      },
      {
        title: t('bench-footprint-srv-disc-benefits-title'),
        content: pI18n('bench-footprint-srv-disc-benefits'),
      },
      {
        title: t('bench-footprint-srv-disc-roadmap-title'),
        content: bulletsI18n([
          { labelKey: 'bench-bullet-streaming', descKey: 'bench-footprint-srv-disc-streaming' },
          { labelKey: 'bench-bullet-indexeddb', descKey: 'bench-footprint-srv-disc-indexeddb' },
          { labelKey: 'bench-bullet-worker', descKey: 'bench-footprint-srv-disc-worker' },
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
    vibesql_queries?: number;
    sqlite_queries?: number;
    duckdb_queries?: number;
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
    el.textContent = t('bench-faster', { value: avgSpeedup.toFixed(2) });
    el.className += ' text-green-600 dark:text-green-400';
  } else if (avgSpeedup < 1 && avgSpeedup > 0) {
    const slowerBy = 1 / avgSpeedup;
    el.textContent = t('bench-slower', { value: slowerBy.toFixed(2) });
    el.className += ' text-red-600 dark:text-red-400';
  } else if (avgSpeedup === 0) {
    el.textContent = t('bench-na');
    el.className += ' text-gray-500 dark:text-gray-400';
  } else {
    el.textContent = t('bench-speedup', { value: avgSpeedup.toFixed(2) });
    el.className += ' text-primary-light dark:text-primary-dark';
  }
}

/**
 * Reset summary card headers to default state (for comparison benchmarks)
 */
function resetSummaryCardHeaders(): void {
  const sqliteHeader = document.querySelector('#avg-speedup-sqlite')?.parentElement?.querySelector('h3');
  if (sqliteHeader) sqliteHeader.textContent = t('bench-vs-sqlite');
  const sqliteLabelEl = document.getElementById('avg-speedup-sqlite-label');
  if (sqliteLabelEl) sqliteLabelEl.textContent = t('bench-avg-speedup');

  const duckdbHeader = document.querySelector('#avg-speedup-duckdb')?.parentElement?.querySelector('h3');
  if (duckdbHeader) duckdbHeader.textContent = t('bench-vs-duckdb');
  const duckdbLabelEl = document.getElementById('avg-speedup-duckdb-label');
  if (duckdbLabelEl) duckdbLabelEl.textContent = t('bench-avg-speedup');
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
      sqliteEl.textContent = t('bench-na');
      sqliteEl.className = 'text-3xl font-bold text-gray-500 dark:text-gray-400';
    }
  }

  // Update DuckDB speedup
  if (duckdbSpeedup.count > 0) {
    updateSpeedupDisplay('avg-speedup-duckdb', duckdbSpeedup.total / duckdbSpeedup.count);
  } else {
    const duckdbEl = document.getElementById('avg-speedup-duckdb');
    if (duckdbEl) {
      duckdbEl.textContent = t('bench-na');
      duckdbEl.className = 'text-3xl font-bold text-gray-500 dark:text-gray-400';
    }
  }
}

/**
 * Current benchmark suite state
 */
let currentSuite: BenchmarkSuite = 'tpcc';
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
    row.className = 'hover:bg-gray-100 dark:hover:bg-gray-700/30 transition-colors';

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
        cell.innerHTML = `<span class="text-red-500" title="${t('bench-failed-title')}">${t('bench-failed')}</span>`;
      } else {
        cell.textContent = t('bench-na');
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
    row.className = 'hover:bg-gray-100 dark:hover:bg-gray-700/30 transition-colors';

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
      sqliteEl.textContent = t('bench-faster', { value: startupSpeedup.toFixed(2) });
      sqliteEl.className = 'text-3xl font-bold text-green-600 dark:text-green-400';
    } else {
      const slower = 1 / startupSpeedup;
      sqliteEl.textContent = t('bench-slower', { value: slower.toFixed(2) });
      sqliteEl.className = 'text-3xl font-bold text-red-600 dark:text-red-400';
    }
  }
  const sqliteLabelEl = document.getElementById('avg-speedup-sqlite-label');
  if (sqliteLabelEl) sqliteLabelEl.textContent = t('bench-startup-time-label');

  // DuckDB comparison (startup time)
  const duckdbEl = document.getElementById('avg-speedup-duckdb');
  if (duckdbEl && vibesql && duckdb && vibesql.available && duckdb.available) {
    const startupSpeedup = duckdb.startup_time_ms / vibesql.startup_time_ms;
    if (startupSpeedup > 1) {
      duckdbEl.textContent = t('bench-faster', { value: startupSpeedup.toFixed(2) });
      duckdbEl.className = 'text-3xl font-bold text-green-600 dark:text-green-400';
    } else {
      const slower = 1 / startupSpeedup;
      duckdbEl.textContent = t('bench-slower', { value: slower.toFixed(2) });
      duckdbEl.className = 'text-3xl font-bold text-red-600 dark:text-red-400';
    }
  }
  const duckdbLabelEl = document.getElementById('avg-speedup-duckdb-label');
  if (duckdbLabelEl) duckdbLabelEl.textContent = t('bench-startup-time-label');

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
    row.innerHTML = `<td colspan="3" class="px-4 py-8 text-center text-gray-500 dark:text-gray-400">${t('bench-no-wasm-data')}</td>`;
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
    row.className = 'hover:bg-gray-100 dark:hover:bg-gray-700/30 transition-colors';

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
  if (sqliteHeader) sqliteHeader.textContent = t('bench-wasm-gzip');
  const sqliteLabelEl = document.getElementById('avg-speedup-sqlite-label');
  if (sqliteLabelEl) sqliteLabelEl.textContent = t('bench-download-size');

  // Show raw WASM size in DuckDB slot
  const duckdbEl = document.getElementById('avg-speedup-duckdb');
  if (duckdbEl && vibesql.wasm_size_bytes) {
    duckdbEl.textContent = formatBytes(vibesql.wasm_size_bytes);
    duckdbEl.className = 'text-3xl font-bold text-primary-light dark:text-primary-dark';
  }
  const duckdbHeader = duckdbEl?.parentElement?.querySelector('h3');
  if (duckdbHeader) duckdbHeader.textContent = t('bench-wasm-size');
  const duckdbLabelEl = document.getElementById('avg-speedup-duckdb-label');
  if (duckdbLabelEl) duckdbLabelEl.textContent = t('bench-uncompressed');

  const opsTestedEl = document.getElementById('ops-tested');
  if (opsTestedEl) {
    opsTestedEl.textContent = '3'; // 3 metrics shown
  }

  // Update label below ops tested
  const opsLabelEl = document.getElementById('ops-tested-label');
  if (opsLabelEl) {
    opsLabelEl.textContent = t('bench-size-metrics');
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
    row.className = 'hover:bg-gray-100 dark:hover:bg-gray-700/30 transition-colors';

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
      primaryCell.textContent = formatTime(primary.stats.mean, primary.stats.stddev) || t('bench-na');
      times.push({ mean: primary.stats.mean, cell: primaryCell });
    } else {
      primaryCell.textContent = t('bench-na');
    }
    row.appendChild(primaryCell);

    // Comparison database time
    const compCell = document.createElement('td');
    compCell.className = 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    if (comparison && comparison.stats.mean > 0) {
      compCell.textContent = formatTime(comparison.stats.mean, comparison.stats.stddev) || t('bench-na');
      times.push({ mean: comparison.stats.mean, cell: compCell });
    } else {
      compCell.textContent = t('bench-na');
    }
    row.appendChild(compCell);

    // DuckDB (only for embedded mode)
    if (!isServer) {
      const duckdbCell = document.createElement('td');
      duckdbCell.className = 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
      if (duckdb && duckdb.stats.mean > 0) {
        duckdbCell.textContent = formatTime(duckdb.stats.mean, duckdb.stats.stddev) || t('bench-na');
        times.push({ mean: duckdb.stats.mean, cell: duckdbCell });
      } else {
        duckdbCell.textContent = t('bench-na');
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
      <th class="px-4 py-3 text-right" title="Transactions per second (higher is better)">DuckDB (TPS)</th>
    `;
  }

  tbody.innerHTML = '';

  const grouped = groupTPCCBenchmarksByOperation(data.benchmarks);

  const sqliteSpeedup = { total: 0, count: 0 };
  const duckdbSpeedup = { total: 0, count: 0 };

  for (const [operation, databases] of grouped.entries()) {
    const vibesql = databases.get('vibesql');
    const sqlite = databases.get('sqlite');
    const duckdb = databases.get('duckdb');

    if (!vibesql && !sqlite && !duckdb) continue;

    const row = document.createElement('tr');
    row.className = 'hover:bg-gray-100 dark:hover:bg-gray-700/30 transition-colors';

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
    const duckdbTps = duckdb?.stats.tps ?? 0;
    const maxTps = Math.max(vibesqlTps, sqliteTps, duckdbTps);
    const vibesqlWins = vibesqlTps === maxTps && vibesqlTps > 0;
    const sqliteWins = sqliteTps === maxTps && sqliteTps > 0 && !vibesqlWins;
    const duckdbWins = duckdbTps === maxTps && duckdbTps > 0 && !vibesqlWins && !sqliteWins;

    // VibeSQL TPS
    const vibesqlCell = document.createElement('td');
    vibesqlCell.className = vibesqlWins
      ? 'px-4 py-3 text-right font-semibold text-green-600 dark:text-green-400'
      : 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    vibesqlCell.textContent = vibesql ? formatTPS(vibesql.stats.tps) : t('bench-na');
    row.appendChild(vibesqlCell);

    // SQLite TPS
    const sqliteCell = document.createElement('td');
    sqliteCell.className = sqliteWins
      ? 'px-4 py-3 text-right font-semibold text-green-600 dark:text-green-400'
      : 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    sqliteCell.textContent = sqlite ? formatTPS(sqlite.stats.tps) : t('bench-na');
    row.appendChild(sqliteCell);

    // DuckDB TPS
    const duckdbCell = document.createElement('td');
    duckdbCell.className = duckdbWins
      ? 'px-4 py-3 text-right font-semibold text-green-600 dark:text-green-400'
      : 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    duckdbCell.textContent = duckdb ? formatTPS(duckdb.stats.tps) : t('bench-na');
    row.appendChild(duckdbCell);

    // Track speedup for summary cards (TPS = higher is better, so vibesql/other)
    if (vibesql && sqlite && vibesql.stats.tps > 0 && sqlite.stats.tps > 0) {
      const speedup = vibesql.stats.tps / sqlite.stats.tps;
      sqliteSpeedup.total += speedup;
      sqliteSpeedup.count++;
    }
    if (vibesql && duckdb && vibesql.stats.tps > 0 && duckdb.stats.tps > 0) {
      const speedup = vibesql.stats.tps / duckdb.stats.tps;
      duckdbSpeedup.total += speedup;
      duckdbSpeedup.count++;
    }

    tbody.appendChild(row);
  }

  // Update summary cards
  updateSpeedupSummary(sqliteSpeedup, duckdbSpeedup);

  // For TPC-C, show total transactions executed by VibeSQL (in millions)
  const opsTestedEl = document.getElementById('ops-tested');
  const opsLabelEl = document.getElementById('ops-tested-label');
  if (opsTestedEl) {
    // Get VibeSQL's total transactions from the first operation
    const firstOp = grouped.values().next().value;
    const vibesqlData = firstOp?.get('vibesql');
    if (vibesqlData && vibesqlData.stats.transactions) {
      const millionTxns = (vibesqlData.stats.transactions / 1_000_000).toFixed(1);
      opsTestedEl.textContent = `${millionTxns}M`;
    } else {
      opsTestedEl.textContent = grouped.size.toString();
    }
  }
  if (opsLabelEl) {
    opsLabelEl.textContent = t('bench-tpcc-transactions-label');
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
  const duckdbData: number[] = [];

  for (const [operation, databases] of grouped.entries()) {
    const vibesql = databases.get('vibesql');
    const sqlite = databases.get('sqlite');
    const duckdb = databases.get('duckdb');

    labels.push(operation.replace(/_/g, ' ').toUpperCase());
    vibesqlData.push(vibesql ? vibesql.stats.tps / 1000 : 0); // Convert to K TPS
    sqliteData.push(sqlite ? sqlite.stats.tps / 1000 : 0);
    duckdbData.push(duckdb ? duckdb.stats.tps / 1000 : 0);
  }

  currentChart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        createDataset('vibesql', vibesqlData),
        createDataset('sqlite', sqliteData),
        createDataset('duckdb', duckdbData),
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
 * Group TPC-DS benchmarks by query number
 */
function groupTPCDSBenchmarksByQuery(benchmarks: TPCDSBenchmark[]): Map<string, Map<string, TPCDSBenchmark>> {
  const grouped = new Map<string, Map<string, TPCDSBenchmark>>();

  for (const bench of benchmarks) {
    const { queryNum, database } = parseTPCDSBenchmarkName(bench.name);

    if (!grouped.has(queryNum)) {
      grouped.set(queryNum, new Map());
    }

    grouped.get(queryNum)!.set(database, bench);
  }

  return grouped;
}

/**
 * Render TPC-DS results table with comparison data (VibeSQL vs SQLite vs DuckDB)
 */
function renderTPCDSTable(data: TPCDSResults): void {
  const tbody = document.getElementById('results-tbody');
  const table = document.getElementById('results-table');
  if (!tbody || !table) return;

  const config = SUITE_CONFIGS['tpcds'];

  // Update table headers for TPC-DS comparison view
  const thead = table.querySelector('thead tr');
  if (thead) {
    thead.innerHTML = `
      <th class="px-4 py-3">${t('bench-table-query')}</th>
      <th class="px-4 py-3 text-right">${t('bench-table-vibesql')}</th>
      <th class="px-4 py-3 text-right">${t('bench-table-sqlite')}</th>
      <th class="px-4 py-3 text-right">${t('bench-table-duckdb')}</th>
    `;
  }

  tbody.innerHTML = '';

  const grouped = groupTPCDSBenchmarksByQuery(data.benchmarks);

  // Sort by query number
  const sortedQueries = [...grouped.entries()].sort((a, b) => {
    const aNum = parseInt(a[0].replace('q', ''));
    const bNum = parseInt(b[0].replace('q', ''));
    return aNum - bNum;
  });

  const sqliteSpeedup = { total: 0, count: 0 };
  const duckdbSpeedup = { total: 0, count: 0 };

  for (const [queryNum, databases] of sortedQueries) {
    const vibesql = databases.get('vibesql');
    const sqlite = databases.get('sqlite');
    const duckdb = databases.get('duckdb');

    const row = document.createElement('tr');
    row.className = 'hover:bg-gray-100 dark:hover:bg-gray-700/30 transition-colors';

    // Query name
    const queryCell = document.createElement('td');
    queryCell.className = 'px-4 py-3 font-medium text-gray-900 dark:text-gray-100';
    const description = config.descriptions[queryNum];
    if (description) {
      queryCell.innerHTML = `<span class="cursor-help" title="${description}">${queryNum.toUpperCase()}</span>`;
    } else {
      queryCell.textContent = queryNum.toUpperCase();
    }
    row.appendChild(queryCell);

    // Determine which engine is fastest for this query
    const times = [
      { name: 'vibesql', time: vibesql?.stats.mean || Infinity },
      { name: 'sqlite', time: sqlite?.stats.mean || Infinity },
      { name: 'duckdb', time: duckdb?.stats.mean || Infinity },
    ].filter(t => t.time !== Infinity);
    const fastest = times.length > 0 ? times.reduce((a, b) => a.time < b.time ? a : b).name : null;

    // VibeSQL time
    const vibesqlCell = document.createElement('td');
    const vibesqlIsFastest = fastest === 'vibesql';
    vibesqlCell.className = vibesqlIsFastest
      ? 'px-4 py-3 text-right font-semibold text-green-600 dark:text-green-400'
      : 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    vibesqlCell.textContent = vibesql && vibesql.stats.mean > 0
      ? formatTime(vibesql.stats.mean, vibesql.stats.stddev) || t('bench-na')
      : t('bench-na');
    row.appendChild(vibesqlCell);

    // SQLite time
    const sqliteCell = document.createElement('td');
    const sqliteIsFastest = fastest === 'sqlite';
    sqliteCell.className = sqliteIsFastest
      ? 'px-4 py-3 text-right font-semibold text-green-600 dark:text-green-400'
      : 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    sqliteCell.textContent = sqlite && sqlite.stats.mean > 0
      ? formatTime(sqlite.stats.mean) || t('bench-na')
      : t('bench-na');
    row.appendChild(sqliteCell);

    // DuckDB time
    const duckdbCell = document.createElement('td');
    const duckdbIsFastest = fastest === 'duckdb';
    duckdbCell.className = duckdbIsFastest
      ? 'px-4 py-3 text-right font-semibold text-green-600 dark:text-green-400'
      : 'px-4 py-3 text-right text-gray-500 dark:text-gray-400';
    duckdbCell.textContent = duckdb && duckdb.stats.mean > 0
      ? formatTime(duckdb.stats.mean) || t('bench-na')
      : t('bench-na');
    row.appendChild(duckdbCell);

    // Calculate speedup (lower time is better, so sqlite/vibesql for speedup)
    if (vibesql && sqlite && vibesql.stats.mean > 0 && sqlite.stats.mean > 0) {
      const speedup = sqlite.stats.mean / vibesql.stats.mean;
      sqliteSpeedup.total += speedup;
      sqliteSpeedup.count++;
    }
    if (vibesql && duckdb && vibesql.stats.mean > 0 && duckdb.stats.mean > 0) {
      const speedup = duckdb.stats.mean / vibesql.stats.mean;
      duckdbSpeedup.total += speedup;
      duckdbSpeedup.count++;
    }

    tbody.appendChild(row);
  }

  // Always show speedup ratios (comparison mode)
  resetSummaryCardHeaders();
  updateSpeedupSummary(sqliteSpeedup, duckdbSpeedup);

  const opsTestedEl = document.getElementById('ops-tested');
  if (opsTestedEl) {
    // Show VibeSQL query count from metadata, fallback to sorted queries length
    const vibesqlQueryCount = data.metadata.vibesql_queries ?? sortedQueries.length;
    opsTestedEl.textContent = `${vibesqlQueryCount}`;
  }

  // Update last updated timestamp
  if (data.metadata.timestamp) {
    updateLastUpdated(data.metadata.timestamp, data.metadata.git_commit);
  }
}

/**
 * Render TPC-DS performance chart with comparison data
 */
function renderTPCDSChart(data: TPCDSResults): void {
  const canvas = document.getElementById('performance-chart') as HTMLCanvasElement;
  if (!canvas) return;

  // Destroy existing chart if any
  if (currentChart) {
    currentChart.destroy();
    currentChart = null;
  }

  // Group by query and show all engines (always comparison mode)
  const grouped = groupTPCDSBenchmarksByQuery(data.benchmarks);

  const labels: string[] = [];
  const vibesqlData: number[] = [];
  const sqliteData: number[] = [];
  const duckdbData: number[] = [];

  // Sort by query number
  const sortedQueries = [...grouped.entries()].sort((a, b) => {
    const aNum = parseInt(a[0].replace('q', ''));
    const bNum = parseInt(b[0].replace('q', ''));
    return aNum - bNum;
  });

  for (const [queryNum, databases] of sortedQueries) {
    const vibesql = databases.get('vibesql');
    const sqlite = databases.get('sqlite');
    const duckdb = databases.get('duckdb');

    // Only include queries where at least one engine has data
    const vibesqlTime = vibesql?.stats.status === 'passed' && vibesql.stats.mean > 0 ? vibesql.stats.mean * 1000 : 0;
    const sqliteTime = sqlite?.stats.status === 'passed' && sqlite.stats.mean > 0 ? sqlite.stats.mean * 1000 : 0;
    const duckdbTime = duckdb?.stats.status === 'passed' && duckdb.stats.mean > 0 ? duckdb.stats.mean * 1000 : 0;

    if (vibesqlTime > 0 || sqliteTime > 0 || duckdbTime > 0) {
      labels.push(queryNum.toUpperCase());
      vibesqlData.push(vibesqlTime);
      sqliteData.push(sqliteTime);
      duckdbData.push(duckdbTime);
    }
  }

  // Always include all three datasets (show N/A bars when data is missing)
  const datasets = [
    createDataset('vibesql', vibesqlData),
    createDataset('sqlite', sqliteData),
    createDataset('duckdb', duckdbData),
  ];

  currentChart = new Chart(canvas, {
    type: 'bar',
    data: { labels, datasets },
    options: {
      ...getLogScaleChartOptions('Execution Time (ms) - Log Scale'),
      plugins: {
        legend: { display: true, position: 'top' },
        tooltip: {
          callbacks: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            label: (context: any) => {
              const value = context.parsed.y;
              if (value === 0) return `${context.dataset.label}: N/A`;
              return `${context.dataset.label}: ${value.toFixed(2)} ms`;
            },
          },
        },
      },
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
    methodologyEl.innerHTML = SUITE_CONFIGS[suite].getMethodology();
  }
}

/**
 * Update the discussion section
 */
function updateDiscussion(suite: BenchmarkSuite): void {
  const discussionEl = document.getElementById('discussion-content');
  if (discussionEl) {
    discussionEl.innerHTML = SUITE_CONFIGS[suite].getDiscussion();
  }
}

/**
 * Update the ops label
 */
function updateOpsLabel(suite: BenchmarkSuite): void {
  const opsLabelEl = document.querySelector('#ops-tested + p');
  if (opsLabelEl) {
    opsLabelEl.textContent = t(SUITE_CONFIGS[suite].opsLabelKey);
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
        <th class="px-4 py-3">${t('bench-table-operation')}</th>
        <th class="px-4 py-3 text-right">${t('bench-table-vibesql')}</th>
        <th class="px-4 py-3 text-right">${t('bench-table-sqlite')}</th>
        <th class="px-4 py-3 text-right">${t('bench-table-duckdb')}</th>
      `;
    // Sysbench server shows VibeSQL Server vs MySQL
    } else if (suite === 'sysbench-server') {
      thead.innerHTML = `
        <th class="px-4 py-3">${t('bench-table-operation')}</th>
        <th class="px-4 py-3 text-right" title="${t('bench-vibesql-server-title')}">${t('bench-table-vibesql-server')}</th>
        <th class="px-4 py-3 text-right">${t('bench-table-mysql')}</th>
      `;
    } else {
      // TPC-H, TPC-DS, TPC-C: VibeSQL vs SQLite vs DuckDB (embedded databases only)
      thead.innerHTML = `
        <th class="px-4 py-3">${t('bench-table-operation')}</th>
        <th class="px-4 py-3 text-right">${t('bench-table-vibesql')}</th>
        <th class="px-4 py-3 text-right">${t('bench-table-sqlite')}</th>
        <th class="px-4 py-3 text-right">${t('bench-table-duckdb')}</th>
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

        // Override ops-tested to show VibeSQL query count from metadata
        const opsTestedEl = document.getElementById('ops-tested');
        if (opsTestedEl && data.metadata.vibesql_queries) {
          opsTestedEl.textContent = `${data.metadata.vibesql_queries}`;
        }
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
      sqliteEl.textContent = t('bench-na');
      sqliteEl.className = 'text-3xl font-bold text-gray-500 dark:text-gray-400';
    }
    const duckdbEl = document.getElementById('avg-speedup-duckdb');
    if (duckdbEl) {
      duckdbEl.textContent = t('bench-na');
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

  // Initialize i18n with current locale
  initI18n(locale.current);
  updateDOM();
  document.title = document.querySelector('title')?.textContent || document.title;

  // Wire up locale changes to i18n
  locale.onChange(localeCode => {
    setI18nLocale(localeCode);
    updateDOM();
    document.documentElement.lang = localeCode;
    // Re-render the current suite to update dynamically generated i18n content
    loadBenchmarkData(currentSuite);
  });

  // Initialize navigation component with theme and locale
  new NavigationComponent('benchmarks', theme, locale);

  // Initialize tabs
  initTabs();

  // Load benchmark data for default suite (TPC-H)
  loadBenchmarkData(currentSuite);
});
