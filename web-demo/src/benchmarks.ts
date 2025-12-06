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
    discussion: `
      <h3 class="text-lg font-semibold text-foreground mb-2">Analysis &amp; Roadmap</h3>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Where VibeSQL Excels</h4>
      <p class="text-muted mb-2">
        VibeSQL shows strong performance on <strong>scan-heavy aggregation queries</strong> (Q1, Q6, Q14, Q15, Q20)
        where our columnar execution engine and SIMD-accelerated aggregations shine. These queries
        involve filtering large tables and computing aggregates without complex join patterns.
      </p>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Current Optimization Targets</h4>
      <p class="text-muted mb-2">
        Multi-way join queries (Q3, Q5, Q7-Q10, Q18, Q19, Q21) currently show SQLite ahead. The primary bottleneck
        is our hash join implementation, which doesn't yet employ the same level of optimization as SQLite's
        decades-refined B-tree joins. Specific areas under active development:
      </p>
      <ul class="list-disc list-inside space-y-1 text-muted text-sm ml-2">
        <li><strong>Join ordering:</strong> Improved cardinality estimation for better join order selection</li>
        <li><strong>Hash table sizing:</strong> Adaptive hash table growth and spill-to-disk for large joins</li>
        <li><strong>Vectorized joins:</strong> Batch processing in the join inner loop to improve cache utilization</li>
        <li><strong>Index-nested-loop joins:</strong> Leveraging B-tree indexes when beneficial</li>
      </ul>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Path to Leadership</h4>
      <p class="text-muted mb-2">
        VibeSQL's architecture is designed for modern hardware with features like columnar storage,
        vectorized execution, and lock-free concurrency. As these optimizations mature, we expect
        VibeSQL to achieve consistent leadership across all TPC-H queries. The fundamental design
        supports parallelism and SIMD that traditional row-store databases cannot easily retrofit.
      </p>
    `,
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
    methodology: `
      <h3 class="text-lg font-semibold text-foreground mb-2">TPC-DS Decision Support Benchmark</h3>
      <p class="text-muted mb-4">
        <strong>TPC-DS</strong> is the successor to TPC-H, featuring 99 queries that model
        a modern decision support system with significantly more complex query patterns
        including multiple fact tables, snow-flake schema, and advanced SQL features.
      </p>

      <ul class="space-y-2 text-muted">
        <li><strong>Schema:</strong> 24 tables with star/snowflake schema design</li>
        <li><strong>Query Count:</strong> 99 queries (currently 88/99 passing)</li>
        <li><strong>Scale Factor:</strong> SF 0.001 (development/testing scale)</li>
        <li><strong>Query Types:</strong> Reporting, ad-hoc, data mining patterns</li>
        <li><strong>SQL Features:</strong> Window functions, CTEs, complex subqueries, ROLLUP/CUBE</li>
      </ul>

      <p class="mt-4 text-muted">
        TPC-DS queries are substantially more complex than TPC-H, testing advanced SQL features
        like window functions, common table expressions (WITH clause), and complex join patterns
        across multiple fact and dimension tables.
      </p>

      <p class="mt-2 text-muted text-sm">
        <strong>Note:</strong> Remaining unsupported queries require features like INTERSECT/EXCEPT or
        specific date arithmetic functions not yet implemented.
      </p>
    `,
    discussion: `
      <h3 class="text-lg font-semibold text-foreground mb-2">Analysis &amp; Roadmap</h3>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">SQL:1999 Feature Coverage</h4>
      <p class="text-muted mb-2">
        TPC-DS exercises the most demanding SQL features. VibeSQL passes <strong>88 of 99 queries</strong>,
        demonstrating broad coverage of SQL:1999 including ROLLUP, CUBE, GROUPING(), window functions with
        complex framing, and recursive CTEs. The remaining queries require INTERSECT/EXCEPT set operations.
      </p>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Complex Query Optimization</h4>
      <p class="text-muted mb-2">
        TPC-DS queries often join 10+ tables with correlated subqueries. Current focus areas:
      </p>
      <ul class="list-disc list-inside space-y-1 text-muted text-sm ml-2">
        <li><strong>CTE materialization:</strong> Intelligent decision between materialized and inline CTEs</li>
        <li><strong>Subquery decorrelation:</strong> Converting correlated subqueries to joins when beneficial</li>
        <li><strong>Star schema optimization:</strong> Fact-dimension join ordering for analytical patterns</li>
      </ul>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Toward 99/99</h4>
      <p class="text-muted mb-2">
        INTERSECT and EXCEPT are planned additions that will enable the remaining queries. These set
        operations fit naturally into our existing query algebra and will be implemented as hash-based
        operators similar to our DISTINCT processing.
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
    discussion: `
      <h3 class="text-lg font-semibold text-foreground mb-2">Analysis &amp; Roadmap</h3>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">42x Faster Than SQLite</h4>
      <p class="text-muted mb-2">
        VibeSQL achieves <strong>~79,000 transactions per second</strong> compared to SQLite's ~1,900 TPS,
        a 42x improvement. This dramatic speedup comes from our lock-free MVCC architecture that avoids
        SQLite's coarse-grained locking on every write operation.
      </p>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Why VibeSQL Dominates OLTP</h4>
      <ul class="list-disc list-inside space-y-1 text-muted text-sm ml-2">
        <li><strong>Lock-free reads:</strong> MVCC allows readers and writers to proceed concurrently without blocking</li>
        <li><strong>Optimistic concurrency:</strong> Transactions only conflict at commit time, not during execution</li>
        <li><strong>In-memory B-tree:</strong> Purpose-built index structure optimized for in-memory workloads</li>
        <li><strong>Prepared statement caching:</strong> Query plans are compiled once and reused</li>
      </ul>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Scaling Further</h4>
      <p class="text-muted mb-2">
        Current results are single-threaded. VibeSQL's architecture supports multi-threaded transaction
        processing, and we expect near-linear scaling as we add parallel execution support. Our goal is
        to achieve 500K+ TPS on modern multi-core hardware.
      </p>
    `,
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
    methodology: `
      <h3 class="text-lg font-semibold text-foreground mb-2">Sysbench Micro-Benchmarks (Embedded)</h3>
      <p class="text-muted mb-4">
        <strong>Sysbench</strong> provides focused micro-benchmarks that isolate specific
        database operations. These tests measure raw performance for fundamental operations
        without the complexity of full transaction workloads.
      </p>

      <ul class="space-y-2 text-muted">
        <li><strong>Mode:</strong> Embedded (in-process, zero network overhead)</li>
        <li><strong>Workload Types:</strong> Point queries, range scans, updates, inserts, deletes</li>
        <li><strong>Table Size:</strong> 10,000 rows per table</li>
        <li><strong>Index Types:</strong> Primary key and secondary indexes</li>
        <li><strong>Operations:</strong> Single-statement operations (no multi-statement transactions)</li>
        <li><strong>Databases:</strong> VibeSQL, SQLite, DuckDB</li>
      </ul>

      <p class="mt-4 text-muted">
        Embedded mode runs the database in-process with zero network overhead, ideal for
        single-process applications where minimal latency is critical.
      </p>
    `,
    discussion: `
      <h3 class="text-lg font-semibold text-foreground mb-2">Analysis &amp; Roadmap</h3>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Point Lookups: VibeSQL Leads</h4>
      <p class="text-muted mb-2">
        VibeSQL's direct API achieves <strong>~137ns per point select</strong>, matching SQLite and vastly
        outperforming DuckDB (~140µs). Our B-tree implementation is optimized for single-row lookups with
        minimal pointer chasing and cache-friendly node layouts.
      </p>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Index Updates: 2x Faster</h4>
      <p class="text-muted mb-2">
        VibeSQL's indexed updates run at <strong>~740ns vs SQLite's ~1.6µs</strong>. Our MVCC design
        allows in-place index updates without write-ahead logging overhead for each operation.
      </p>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Areas for Improvement</h4>
      <ul class="list-disc list-inside space-y-1 text-muted text-sm ml-2">
        <li><strong>Bulk inserts:</strong> SQLite's batch insert path is highly optimized; we're adding batched B-tree operations</li>
        <li><strong>Non-indexed updates:</strong> Full table scans for non-indexed columns need predicate pushdown optimization</li>
        <li><strong>Deletes:</strong> Our tombstone-based deletion has cleanup overhead; compaction improvements are planned</li>
      </ul>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">DuckDB Comparison</h4>
      <p class="text-muted mb-2">
        DuckDB is optimized for analytical workloads, not micro-operations. Its 100-1000x slower
        results here reflect architectural choices (columnar storage, vectorized execution) that
        trade single-row latency for bulk throughput. VibeSQL targets both use cases.
      </p>
    `,
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
    methodology: `
      <h3 class="text-lg font-semibold text-foreground mb-2">Sysbench Micro-Benchmarks (Server)</h3>
      <p class="text-muted mb-4">
        <strong>Sysbench</strong> server benchmarks compare VibeSQL Server (PostgreSQL wire protocol)
        against MySQL, measuring performance for multi-client database deployments.
      </p>

      <ul class="space-y-2 text-muted">
        <li><strong>Mode:</strong> Server (PostgreSQL wire protocol)</li>
        <li><strong>Workload Types:</strong> Point queries, range scans, updates, inserts, deletes</li>
        <li><strong>Table Size:</strong> 10,000 rows per table</li>
        <li><strong>Protocol Overhead:</strong> ~10-50µs per query for wire protocol handling</li>
        <li><strong>Databases:</strong> VibeSQL Server, MySQL</li>
      </ul>

      <p class="mt-4 text-muted">
        Server mode uses the PostgreSQL wire protocol, enabling multi-client access and
        compatibility with existing PostgreSQL tooling and drivers.
      </p>
    `,
    discussion: `
      <h3 class="text-lg font-semibold text-foreground mb-2">Analysis &amp; Roadmap</h3>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">PostgreSQL Wire Protocol</h4>
      <p class="text-muted mb-2">
        VibeSQL Server implements the PostgreSQL wire protocol, enabling compatibility with
        existing PostgreSQL drivers and tools. This adds ~10-50µs of protocol overhead per query
        compared to embedded mode, but enables multi-client deployments.
      </p>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">MySQL Comparison</h4>
      <p class="text-muted mb-2">
        Server benchmarks compare against MySQL to evaluate VibeSQL as a drop-in replacement
        for traditional client-server databases. Results vary by operation type, with VibeSQL
        showing advantages in read-heavy workloads.
      </p>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Server Roadmap</h4>
      <ul class="list-disc list-inside space-y-1 text-muted text-sm ml-2">
        <li><strong>Connection pooling:</strong> Reduce connection establishment overhead for high-throughput scenarios</li>
        <li><strong>Prepared statement caching:</strong> Server-side caching of query plans across connections</li>
        <li><strong>Extended query protocol:</strong> Full PostgreSQL extended query protocol support for batch operations</li>
      </ul>
    `,
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
    methodology: `
      <h3 class="text-lg font-semibold text-foreground mb-2">Native Binary Footprint</h3>
      <p class="text-muted mb-4">
        <strong>Embedded footprint benchmarks</strong> measure the resource efficiency of native database binaries,
        comparing binary size, cold startup time, and peak memory usage.
      </p>

      <ul class="space-y-2 text-muted">
        <li><strong>Binary Size:</strong> Size of the compiled native binary in bytes (stripped release build)</li>
        <li><strong>Startup Time:</strong> Time from process start to first query result (CREATE TABLE, INSERT, SELECT)</li>
        <li><strong>Peak Memory:</strong> Maximum resident set size (RSS) during cold startup</li>
        <li><strong>Databases:</strong> VibeSQL, SQLite, DuckDB</li>
      </ul>

      <p class="mt-4 text-muted">
        Native binary footprint is critical for <strong>embedded and edge deployments</strong> where
        binary size, startup latency, and memory consumption directly impact deployment feasibility.
      </p>
    `,
    discussion: `
      <h3 class="text-lg font-semibold text-foreground mb-2">Analysis &amp; Roadmap</h3>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Binary Size: Middle Ground</h4>
      <p class="text-muted mb-2">
        VibeSQL at <strong>~17MB</strong> sits between SQLite (~5MB) and DuckDB (~45MB). This reflects
        our choice to include advanced features (window functions, CTEs, columnar execution) while
        keeping the binary manageable for embedded deployments.
      </p>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Startup: Fastest Cold Start</h4>
      <p class="text-muted mb-2">
        VibeSQL achieves <strong>~7.7ms cold startup</strong>, slightly faster than SQLite (~8.2ms) and
        significantly faster than DuckDB (~14.6ms). Our minimal initialization path loads only
        essential metadata structures on startup.
      </p>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Memory Efficiency</h4>
      <p class="text-muted mb-2">
        Peak memory during startup is ~7MB for VibeSQL vs ~3MB for SQLite and ~11MB for DuckDB.
        The difference from SQLite reflects our more sophisticated query optimizer and columnar
        execution infrastructure that's allocated upfront.
      </p>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Size Reduction Roadmap</h4>
      <ul class="list-disc list-inside space-y-1 text-muted text-sm ml-2">
        <li><strong>Feature flags:</strong> Compile-time feature selection to exclude unused functionality</li>
        <li><strong>LTO optimization:</strong> Whole-program link-time optimization for dead code elimination</li>
        <li><strong>Modular builds:</strong> Separate core engine from optional features (e.g., window functions)</li>
      </ul>
    `,
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
    methodology: `
      <h3 class="text-lg font-semibold text-foreground mb-2">WASM Footprint</h3>
      <p class="text-muted mb-4">
        <strong>WASM footprint benchmarks</strong> measure the WebAssembly module size for browser deployment,
        critical for web applications where download size impacts user experience.
      </p>

      <ul class="space-y-2 text-muted">
        <li><strong>WASM Size:</strong> Size of the raw WebAssembly module</li>
        <li><strong>WASM (gzip):</strong> Compressed size for HTTP delivery (browsers auto-decompress)</li>
        <li><strong>WASM (brotli):</strong> Brotli-compressed size for optimal web delivery</li>
      </ul>

      <p class="mt-4 text-muted">
        WASM sizes are critical for <strong>web deployments</strong> where download time directly impacts
        time-to-interactive. Gzip sizes are most relevant as browsers automatically decompress gzip content.
      </p>

      <p class="mt-2 text-muted text-sm">
        <strong>Note:</strong> VibeSQL WASM is designed for minimal download size while maintaining
        full SQL:1999 compliance in the browser.
      </p>
    `,
    discussion: `
      <h3 class="text-lg font-semibold text-foreground mb-2">Analysis &amp; Roadmap</h3>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">WASM: 2.2MB Compressed</h4>
      <p class="text-muted mb-2">
        VibeSQL's WebAssembly module compresses to <strong>~2.2MB gzipped</strong>, enabling fast
        initial page loads. This is a full SQL:1999 database with window functions, CTEs, and
        ACID transactions running entirely in the browser.
      </p>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">What's Included</h4>
      <ul class="list-disc list-inside space-y-1 text-muted text-sm ml-2">
        <li>Complete SQL parser and query optimizer</li>
        <li>B-tree storage engine with MVCC</li>
        <li>Window functions and advanced aggregations</li>
        <li>Common table expressions (WITH clause)</li>
        <li>Full ACID transaction support</li>
      </ul>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">Browser Deployment Benefits</h4>
      <p class="text-muted mb-2">
        Running SQL in the browser eliminates round-trip latency to servers, enables offline-first
        applications, and keeps sensitive data on the user's device. VibeSQL's WASM build is
        designed for this use case with minimal dependencies and efficient memory usage.
      </p>

      <h4 class="text-md font-medium text-foreground mt-4 mb-2">WASM Roadmap</h4>
      <ul class="list-disc list-inside space-y-1 text-muted text-sm ml-2">
        <li><strong>Streaming compilation:</strong> Start executing while the module downloads</li>
        <li><strong>IndexedDB persistence:</strong> Durable storage across browser sessions</li>
        <li><strong>Worker thread support:</strong> Run queries off the main thread for responsive UIs</li>
      </ul>
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
    lastUpdatedEl.innerHTML = `${dateStr} ${timeStr}<br/><span class="text-sm font-mono text-muted">${gitCommit}</span>`;
  } else {
    lastUpdatedEl.textContent = `${dateStr} ${timeStr}`;
  }
  lastUpdatedEl.className = 'text-xl font-bold text-primary-light dark:text-primary-dark';
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

  let totalSpeedup = 0;
  let comparisonCount = 0;

  for (const [operation, databases] of grouped.entries()) {
    const vibesql = databases.get('vibesql');
    const vibesqlServer = databases.get('vibesql_server');
    const sqlite = databases.get('sqlite');
    const duckdb = databases.get('duckdb');
    const mysql = databases.get('mysql');

    if (!vibesql && !vibesqlServer && !sqlite && !duckdb && !mysql) continue;

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
    const vibesqlTime = vibesql ? formatTime(vibesql.stats.mean, vibesql.stats.stddev) : null;
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
    const sqliteTime = sqlite ? formatTime(sqlite.stats.mean, sqlite.stats.stddev) : null;
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
    const duckdbTime = duckdb ? formatTime(duckdb.stats.mean, duckdb.stats.stddev) : null;
    if (duckdbTime) {
      duckdbCell.textContent = duckdbTime;
    } else if (duckdb && duckdb.stats.mean < 0) {
      duckdbCell.innerHTML = '<span class="text-red-500" title="Query failed (timeout or error)">FAILED</span>';
    } else {
      duckdbCell.textContent = 'N/A';
    }
    row.appendChild(duckdbCell);

    // MySQL time - only show for sysbench-server (server mode compares to MySQL)
    if (suite === 'sysbench-server') {
      const mysqlCell = document.createElement('td');
      mysqlCell.className = 'px-4 py-3 text-right text-muted';
      const mysqlTime = mysql ? formatTime(mysql.stats.mean, mysql.stats.stddev) : null;
      if (mysqlTime) {
        mysqlCell.textContent = mysqlTime;
      } else if (mysql && mysql.stats.mean < 0) {
        mysqlCell.innerHTML = '<span class="text-red-500" title="Query failed (timeout or error)">FAILED</span>';
      } else {
        mysqlCell.textContent = 'N/A';
      }
      row.appendChild(mysqlCell);
    }

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

  // Build datasets array - MySQL only for sysbench-server
  const datasets = [
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
  ];

  // Only add MySQL dataset for sysbench-server
  if (suite === 'sysbench-server') {
    datasets.push({
      label: 'MySQL',
      data: mysqlData,
      backgroundColor: 'rgba(249, 115, 22, 0.5)',
      borderColor: 'rgba(249, 115, 22, 1)',
      borderWidth: 1,
    });
  }

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

  const dbDisplayNames: Record<string, string> = {
    'vibesql': 'VibeSQL',
    'sqlite': 'SQLite',
    'duckdb': 'DuckDB',
  };

  for (const benchmark of availableBenchmarks) {
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

  // Update summary cards
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

  const dbColors: Record<string, { bg: string; border: string }> = {
    'vibesql': { bg: 'rgba(34, 197, 94, 0.5)', border: 'rgba(34, 197, 94, 1)' },
    'sqlite': { bg: 'rgba(239, 68, 68, 0.5)', border: 'rgba(239, 68, 68, 1)' },
    'duckdb': { bg: 'rgba(59, 130, 246, 0.5)', border: 'rgba(59, 130, 246, 1)' },
  };

  const dbDisplayNames: Record<string, string> = {
    'vibesql': 'VibeSQL',
    'sqlite': 'SQLite',
    'duckdb': 'DuckDB',
  };

  const labels = ['Binary Size (MB)', 'Startup Time (ms)', 'Peak Memory (MB)'];

  const datasets = availableBenchmarks.map(bench => ({
    label: dbDisplayNames[bench.database] || bench.database,
    data: [
      bench.binary_size_bytes / (1024 * 1024),
      bench.startup_time_ms,
      bench.peak_memory_kb / 1024,
    ],
    backgroundColor: dbColors[bench.database]?.bg || 'rgba(156, 163, 175, 0.5)',
    borderColor: dbColors[bench.database]?.border || 'rgba(156, 163, 175, 1)',
    borderWidth: 1,
  }));

  currentChart = new Chart(canvas, {
    type: 'bar',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          beginAtZero: true,
          title: { display: true, text: 'Value (lower is better)' },
        },
      },
      plugins: {
        legend: { display: true, position: 'top' },
      },
    },
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
    row.innerHTML = `<td colspan="3" class="px-4 py-8 text-center text-muted">No WASM data available</td>`;
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
    row.className = 'hover:bg-card/50 transition-colors';

    const nameCell = document.createElement('td');
    nameCell.className = 'px-4 py-3 font-medium text-foreground';
    nameCell.textContent = metric.name;
    row.appendChild(nameCell);

    const valueCell = document.createElement('td');
    valueCell.className = 'px-4 py-3 text-right text-primary-light dark:text-primary-dark font-semibold';
    valueCell.textContent = metric.value;
    row.appendChild(valueCell);

    const noteCell = document.createElement('td');
    noteCell.className = 'px-4 py-3 text-right text-muted text-sm';
    noteCell.textContent = metric.note;
    row.appendChild(noteCell);

    tbody.appendChild(row);
  }

  // Update summary cards
  const avgSpeedupEl = document.getElementById('avg-speedup');
  if (avgSpeedupEl && vibesql.wasm_size_gzip_bytes) {
    avgSpeedupEl.textContent = formatBytes(vibesql.wasm_size_gzip_bytes);
    avgSpeedupEl.className = 'text-xl font-bold text-primary-light dark:text-primary-dark';
  }

  const opsTestedEl = document.getElementById('ops-tested');
  if (opsTestedEl) {
    opsTestedEl.textContent = '3'; // 3 metrics shown
  }

  // Update label below ops tested
  const opsLabelEl = document.querySelector('#ops-tested + p');
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

  let totalSpeedup = 0;
  let comparisonCount = 0;

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
    row.className = 'hover:bg-card/50 transition-colors';

    // Operation name
    const opCell = document.createElement('td');
    opCell.className = 'px-4 py-3 font-medium text-foreground';
    const description = config.descriptions[operation];
    if (description) {
      opCell.innerHTML = `<span class="cursor-help" title="${description}">${operation.replace(/_/g, ' ').toUpperCase()}</span>`;
    } else {
      opCell.textContent = operation.replace(/_/g, ' ').toUpperCase();
    }
    row.appendChild(opCell);

    // Primary database time
    const primaryCell = document.createElement('td');
    primaryCell.className = 'px-4 py-3 text-right text-muted';
    primaryCell.textContent = primary ? formatTime(primary.stats.mean, primary.stats.stddev) || 'N/A' : 'N/A';
    row.appendChild(primaryCell);

    // Comparison database time
    const compCell = document.createElement('td');
    compCell.className = 'px-4 py-3 text-right text-muted';
    compCell.textContent = comparison ? formatTime(comparison.stats.mean, comparison.stats.stddev) || 'N/A' : 'N/A';
    row.appendChild(compCell);

    // DuckDB (only for embedded mode)
    if (!isServer) {
      const duckdbCell = document.createElement('td');
      duckdbCell.className = 'px-4 py-3 text-right text-muted';
      duckdbCell.textContent = duckdb ? formatTime(duckdb.stats.mean, duckdb.stats.stddev) || 'N/A' : 'N/A';
      row.appendChild(duckdbCell);
    }

    // Speedup
    const speedupCell = document.createElement('td');
    speedupCell.className = 'px-4 py-3 text-right font-semibold';

    if (primary && comparison && primary.stats.mean > 0 && comparison.stats.mean > 0) {
      const speedup = calculateSpeedup(primary.stats.mean, comparison.stats.mean);
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

    if (primary && comparison && primary.stats.mean > 0 && comparison.stats.mean > 0) {
      const speedup = calculateSpeedup(primary.stats.mean, comparison.stats.mean);
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
      const compLabel = isServer ? 'vs MySQL' : 'vs SQLite';
      if (avgSpeedup > 1) {
        avgSpeedupEl.textContent = `${avgSpeedup.toFixed(2)}x faster`;
        avgSpeedupEl.className = 'text-xl font-bold text-green-600 dark:text-green-400';
      } else if (avgSpeedup < 1) {
        const slowerBy = 1 / avgSpeedup;
        avgSpeedupEl.textContent = `${slowerBy.toFixed(2)}x slower`;
        avgSpeedupEl.className = 'text-xl font-bold text-red-600 dark:text-red-400';
      } else {
        avgSpeedupEl.textContent = `${avgSpeedup.toFixed(2)}x`;
        avgSpeedupEl.className = 'text-xl font-bold text-primary-light dark:text-primary-dark';
      }

      // Update the comparison label
      const compLabelEl = document.querySelector('#avg-speedup + p');
      if (compLabelEl) {
        compLabelEl.textContent = compLabel;
      }
    }
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
    {
      label: isServer ? 'VibeSQL Server' : 'VibeSQL',
      data: primaryData,
      backgroundColor: 'rgba(34, 197, 94, 0.5)',
      borderColor: 'rgba(34, 197, 94, 1)',
      borderWidth: 1,
    },
    {
      label: isServer ? 'MySQL' : 'SQLite',
      data: comparisonData,
      backgroundColor: isServer ? 'rgba(249, 115, 22, 0.5)' : 'rgba(239, 68, 68, 0.5)',
      borderColor: isServer ? 'rgba(249, 115, 22, 1)' : 'rgba(239, 68, 68, 1)',
      borderWidth: 1,
    },
  ];

  if (!isServer) {
    datasets.push({
      label: 'DuckDB',
      data: duckdbData,
      backgroundColor: 'rgba(59, 130, 246, 0.5)',
      borderColor: 'rgba(59, 130, 246, 1)',
      borderWidth: 1,
    });
  }

  currentChart = new Chart(canvas, {
    type: 'bar',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          type: 'logarithmic',
          beginAtZero: false,
          title: { display: true, text: 'Time (ms) - Log Scale' },
          ticks: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            callback: function (value: any) {
              const allowedTicks = [0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000];
              if (allowedTicks.includes(value)) {
                return value;
              }
              return null;
            },
          },
        },
      },
      plugins: {
        legend: { display: true, position: 'top' },
        tooltip: {
          callbacks: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            label: function (context: any) {
              return `${context.dataset.label}: ${context.parsed.y.toFixed(4)} ms`;
            },
          },
        },
      },
    },
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

  // Update table headers for TPC-C view
  const thead = table.querySelector('thead tr');
  if (thead) {
    thead.innerHTML = `
      <th class="px-4 py-3">Workload</th>
      <th class="px-4 py-3 text-right">VibeSQL</th>
      <th class="px-4 py-3 text-right">SQLite</th>
      <th class="px-4 py-3 text-right">Transactions</th>
      <th class="px-4 py-3 text-right">Duration</th>
      <th class="px-4 py-3 text-right">Speedup</th>
      <th class="px-4 py-3 text-center">Winner</th>
    `;
  }

  tbody.innerHTML = '';

  const grouped = groupTPCCBenchmarksByOperation(data.benchmarks);

  let totalSpeedup = 0;
  let comparisonCount = 0;

  for (const [operation, databases] of grouped.entries()) {
    const vibesql = databases.get('vibesql');
    const sqlite = databases.get('sqlite');

    if (!vibesql && !sqlite) continue;

    const row = document.createElement('tr');
    row.className = 'hover:bg-card/50 transition-colors';

    // Operation name
    const opCell = document.createElement('td');
    opCell.className = 'px-4 py-3 font-medium text-foreground';
    const config = SUITE_CONFIGS['tpcc'];
    const description = config.descriptions[operation];
    if (description) {
      opCell.innerHTML = `<span class="cursor-help" title="${description}">${operation.replace(/_/g, ' ').toUpperCase()}</span>`;
    } else {
      opCell.textContent = operation.replace(/_/g, ' ').toUpperCase();
    }
    row.appendChild(opCell);

    // VibeSQL TPS
    const vibesqlCell = document.createElement('td');
    vibesqlCell.className = 'px-4 py-3 text-right text-muted';
    vibesqlCell.textContent = vibesql ? formatTPS(vibesql.stats.tps) : 'N/A';
    row.appendChild(vibesqlCell);

    // SQLite TPS
    const sqliteCell = document.createElement('td');
    sqliteCell.className = 'px-4 py-3 text-right text-muted';
    sqliteCell.textContent = sqlite ? formatTPS(sqlite.stats.tps) : 'N/A';
    row.appendChild(sqliteCell);

    // Transactions (use vibesql or sqlite)
    const txCell = document.createElement('td');
    txCell.className = 'px-4 py-3 text-right text-muted';
    const txBench = vibesql || sqlite;
    txCell.textContent = txBench ? txBench.stats.transactions.toLocaleString() : 'N/A';
    row.appendChild(txCell);

    // Duration
    const durCell = document.createElement('td');
    durCell.className = 'px-4 py-3 text-right text-muted';
    durCell.textContent = txBench ? `${(txBench.stats.duration_ms / 1000).toFixed(0)}s` : 'N/A';
    row.appendChild(durCell);

    // Speedup (for TPS, higher is better, so speedup = vibesql / sqlite)
    const speedupCell = document.createElement('td');
    speedupCell.className = 'px-4 py-3 text-right font-semibold';

    if (vibesql && sqlite && vibesql.stats.tps > 0 && sqlite.stats.tps > 0) {
      const speedup = vibesql.stats.tps / sqlite.stats.tps;
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

    if (vibesql && sqlite && vibesql.stats.tps > 0 && sqlite.stats.tps > 0) {
      const speedup = vibesql.stats.tps / sqlite.stats.tps;
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
        avgSpeedupEl.className = 'text-xl font-bold text-green-600 dark:text-green-400';
      } else if (avgSpeedup < 1) {
        const slowerBy = 1 / avgSpeedup;
        avgSpeedupEl.textContent = `${slowerBy.toFixed(2)}x slower`;
        avgSpeedupEl.className = 'text-xl font-bold text-red-600 dark:text-red-400';
      } else {
        avgSpeedupEl.textContent = `${avgSpeedup.toFixed(2)}x`;
        avgSpeedupEl.className = 'text-xl font-bold text-primary-light dark:text-primary-dark';
      }
    }
  }

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
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: 'Transactions per Second (K TPS) - Higher is Better',
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
              return `${context.dataset.label}: ${(context.parsed.y * 1000).toLocaleString()} TPS`;
            },
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
    row.className = 'hover:bg-card/50 transition-colors';

    // Query name with description tooltip
    const queryCell = document.createElement('td');
    queryCell.className = 'px-4 py-3 font-medium text-foreground';
    const description = config.descriptions[queryNum];
    if (description) {
      queryCell.innerHTML = `<span class="cursor-help" title="${description}">TPC-DS ${queryNum.toUpperCase()}</span>`;
    } else {
      queryCell.textContent = `TPC-DS ${queryNum.toUpperCase()}`;
    }
    row.appendChild(queryCell);

    // Execution time
    const timeCell = document.createElement('td');
    timeCell.className = 'px-4 py-3 text-right text-muted';
    if (isPassed && bench.stats.mean > 0) {
      timeCell.textContent = formatTime(bench.stats.mean, bench.stats.stddev) || 'N/A';
    } else {
      timeCell.textContent = '-';
    }
    row.appendChild(timeCell);

    // Rows returned
    const rowsCell = document.createElement('td');
    rowsCell.className = 'px-4 py-3 text-right text-muted';
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

  // Update summary cards
  const avgSpeedupEl = document.getElementById('avg-speedup');
  if (avgSpeedupEl) {
    const passRate = (passedCount / sortedBenchmarks.length * 100).toFixed(0);
    avgSpeedupEl.textContent = `${passRate}% pass rate`;
    avgSpeedupEl.className = 'text-xl font-bold text-green-600 dark:text-green-400';
  }

  const opsTestedEl = document.getElementById('ops-tested');
  if (opsTestedEl) {
    opsTestedEl.textContent = `${passedCount}/${sortedBenchmarks.length}`;
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
      datasets: [
        {
          label: 'VibeSQL',
          data: vibesqlData,
          backgroundColor: 'rgba(34, 197, 94, 0.5)',
          borderColor: 'rgba(34, 197, 94, 1)',
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
            text: 'Execution Time (ms) - Log Scale',
          },
          ticks: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            callback: function (value: any) {
              const allowedTicks = [0.1, 1, 10, 100, 1000];
              if (allowedTicks.includes(value)) {
                return value;
              }
              return null;
            },
          },
        },
        x: {
          ticks: {
            maxRotation: 90,
            minRotation: 45,
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
        <th class="px-4 py-3 text-right">Speedup</th>
        <th class="px-4 py-3 text-center">Winner</th>
      `;
    // Sysbench server shows VibeSQL Server vs MySQL
    } else if (suite === 'sysbench-server') {
      thead.innerHTML = `
        <th class="px-4 py-3">Operation</th>
        <th class="px-4 py-3 text-right" title="VibeSQL via PostgreSQL wire protocol">VibeSQL Server</th>
        <th class="px-4 py-3 text-right">MySQL</th>
        <th class="px-4 py-3 text-right">Speedup</th>
        <th class="px-4 py-3 text-center">Winner</th>
      `;
    } else {
      // TPC-H, TPC-DS, TPC-C: VibeSQL vs SQLite vs DuckDB (embedded databases only)
      thead.innerHTML = `
        <th class="px-4 py-3">Operation</th>
        <th class="px-4 py-3 text-right">VibeSQL</th>
        <th class="px-4 py-3 text-right">SQLite</th>
        <th class="px-4 py-3 text-right">DuckDB</th>
        <th class="px-4 py-3 text-right">Speedup</th>
        <th class="px-4 py-3 text-center">Winner</th>
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
