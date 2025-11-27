/**
 * Performance Dashboard page
 *
 * Displays performance trends over time with:
 * - Hero cards showing key metrics
 * - Timeline chart for performance trends
 * - Tabbed benchmark suite details with sparklines
 * - Recent changes feed
 */

import './styles/main.css';
import { initTheme } from './theme';
import { NavigationComponent } from './components/Navigation';

// Chart.js is loaded via CDN in performance.html
// eslint-disable-next-line @typescript-eslint/no-explicit-any
declare const Chart: any;

// =============================================================================
// Type Definitions (matching dashboard.json schema from design doc)
// =============================================================================

interface DashboardSummary {
  conformance: {
    pass_rate: number;
    tests_passing: number;
    tests_total: number;
    files_passing: number;
    files_total: number;
  };
  tpch: {
    queries_passing: number;
    queries_total: number;
    geo_mean_ms: number;
    trend_7d_pct: number;
  };
}

interface TimelineEntry {
  date: string;
  commit: string;
  conformance_pass_rate: number;
  tpch_geo_mean_ms: number;
  tpch_passing: number;
  tpcc_tps?: number;
  events: string[];
}

interface QueryStats {
  avg_7d_ms: number;
  avg_30d_ms: number;
  best_ms: number;
  trend: 'improving' | 'stable' | 'regressing';
  trend_pct: number;
}

interface QueryLatest {
  vibesql_ms: number | null;
  sqlite_ms: number | null;
  duckdb_ms: number | null;
  status: 'passing' | 'failing' | 'timeout' | 'memory';
  timestamp: string;
  error?: string;
}

interface QueryHistoryEntry {
  date: string;
  ms: number;
}

interface QueryData {
  name: string;
  description: string;
  latest: QueryLatest;
  history: QueryHistoryEntry[];
  stats: QueryStats | null;
}

interface BenchmarkSuite {
  description: string;
  scale_factor?: number;
  queries?: Record<string, QueryData>;
  tests?: Record<string, QueryData>;
  latest?: {
    vibesql_tps?: number;
    sqlite_tps?: number;
    duckdb_tps?: number;
    mixed_latency_us?: number;
  };
  history?: Array<{ date: string; tps: number }>;
}

interface ChangeEntry {
  date: string;
  type: 'improvement' | 'regression' | 'fix' | 'new';
  category: string;
  query?: string;
  description: string;
  before_ms?: number;
  after_ms?: number;
  commit?: string;
  pr?: number;
}

interface MachineInfo {
  system: string;
  cpu: string;
  memory: string;
  notes?: string;
}

interface DashboardData {
  generated_at: string;
  version: string;
  summary: DashboardSummary;
  timeline: TimelineEntry[];
  benchmarks: {
    tpch?: BenchmarkSuite;
    tpcc?: BenchmarkSuite;
    tpcds?: BenchmarkSuite;
    sysbench?: BenchmarkSuite;
  };
  conformance?: {
    summary: {
      total_tests: number;
      passing: number;
      failing: number;
      pass_rate: number;
    };
    categories?: Record<string, { total: number; passing: number; pass_rate: number }>;
    history?: Array<{ date: string; pass_rate: number; passing: number }>;
  };
  changes: ChangeEntry[];
  machine_info?: MachineInfo;
}

// =============================================================================
// State Management
// =============================================================================

let dashboardData: DashboardData | null = null;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let timelineChart: any = null;
let selectedPeriod = 30;
let selectedSuite = 'tpch';

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Format milliseconds to human-readable string
 */
function formatMs(ms: number | null): string {
  if (ms === null || ms < 0) return '—';
  if (ms < 1) return `${(ms * 1000).toFixed(0)} µs`;
  if (ms < 1000) return `${ms.toFixed(1)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

/**
 * Format percentage with trend arrow
 */
function formatTrend(pct: number | null | undefined): string {
  if (pct === null || pct === undefined) return '—';
  const arrow = pct < 0 ? '↓' : pct > 0 ? '↑' : '';
  const color = pct < 0 ? 'text-green-600 dark:text-green-400' : pct > 0 ? 'text-red-600 dark:text-red-400' : 'text-muted';
  return `<span class="${color}">${arrow} ${Math.abs(pct).toFixed(1)}%</span>`;
}

/**
 * Format date for display
 */
function formatDate(dateStr: string): string {
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

/**
 * Get status badge HTML
 */
function getStatusBadge(status: string): string {
  const badges: Record<string, string> = {
    passing: '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">passing</span>',
    failing: '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200">failing</span>',
    timeout: '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200">timeout</span>',
    memory: '<span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200">memory</span>',
  };
  return badges[status] || badges.failing;
}

/**
 * Get change type icon
 */
function getChangeIcon(type: string): string {
  const icons: Record<string, string> = {
    improvement: '<span class="w-8 h-8 flex items-center justify-center rounded-full bg-green-100 dark:bg-green-900 text-green-600 dark:text-green-400">✓</span>',
    regression: '<span class="w-8 h-8 flex items-center justify-center rounded-full bg-yellow-100 dark:bg-yellow-900 text-yellow-600 dark:text-yellow-400">⚠</span>',
    fix: '<span class="w-8 h-8 flex items-center justify-center rounded-full bg-blue-100 dark:bg-blue-900 text-blue-600 dark:text-blue-400">★</span>',
    new: '<span class="w-8 h-8 flex items-center justify-center rounded-full bg-purple-100 dark:bg-purple-900 text-purple-600 dark:text-purple-400">+</span>',
  };
  return icons[type] || icons.improvement;
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(str: string): string {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

// =============================================================================
// Hero Cards
// =============================================================================

function renderHeroCards(): void {
  const container = document.getElementById('hero-cards');
  if (!container || !dashboardData) return;

  const { summary } = dashboardData;

  container.innerHTML = `
    <!-- Correctness Card -->
    <div class="bg-background rounded-lg shadow-lg p-6 border border-border">
      <h3 class="text-sm font-medium text-muted mb-2">Correctness</h3>
      <p class="text-3xl font-bold text-green-600 dark:text-green-400">
        ${summary.conformance.pass_rate.toFixed(2)}%
      </p>
      <p class="text-sm text-muted mt-1">
        ${summary.conformance.tests_passing.toLocaleString()} tests passing
      </p>
    </div>

    <!-- TPC-H Pass Card -->
    <div class="bg-background rounded-lg shadow-lg p-6 border border-border">
      <h3 class="text-sm font-medium text-muted mb-2">TPC-H Pass</h3>
      <p class="text-3xl font-bold text-primary-light dark:text-primary-dark">
        ${summary.tpch.queries_passing}/${summary.tpch.queries_total}
      </p>
      <p class="text-sm text-muted mt-1">queries passing</p>
    </div>

    <!-- Performance Card -->
    <div class="bg-background rounded-lg shadow-lg p-6 border border-border">
      <h3 class="text-sm font-medium text-muted mb-2">Performance</h3>
      <p class="text-3xl font-bold text-primary-light dark:text-primary-dark">
        ${formatMs(summary.tpch.geo_mean_ms)}
      </p>
      <p class="text-sm text-muted mt-1">geometric mean</p>
    </div>

    <!-- Trend Card -->
    <div class="bg-background rounded-lg shadow-lg p-6 border border-border">
      <h3 class="text-sm font-medium text-muted mb-2">7-Day Trend</h3>
      <p class="text-3xl font-bold">
        ${formatTrend(summary.tpch.trend_7d_pct)}
      </p>
      <p class="text-sm text-muted mt-1">vs previous week</p>
    </div>
  `;
}

// =============================================================================
// Timeline Chart
// =============================================================================

function renderTimelineChart(): void {
  const canvas = document.getElementById('timeline-chart') as HTMLCanvasElement;
  if (!canvas || !dashboardData) return;

  const { timeline } = dashboardData;

  // Filter timeline by selected period
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - selectedPeriod);

  const filteredTimeline = timeline.filter(entry => new Date(entry.date) >= cutoffDate);

  // Sort by date ascending for chart
  const sortedTimeline = [...filteredTimeline].sort((a, b) =>
    new Date(a.date).getTime() - new Date(b.date).getTime()
  );

  const labels = sortedTimeline.map(entry => entry.date);
  const data = sortedTimeline.map(entry => entry.tpch_geo_mean_ms);

  // Destroy existing chart if any
  if (timelineChart) {
    timelineChart.destroy();
  }

  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)';
  const textColor = isDark ? '#9ca3af' : '#6b7280';

  timelineChart = new Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'TPC-H Geometric Mean (ms)',
        data,
        borderColor: isDark ? '#60a5fa' : '#3b82f6',
        backgroundColor: isDark ? 'rgba(96, 165, 250, 0.1)' : 'rgba(59, 130, 246, 0.1)',
        borderWidth: 2,
        fill: true,
        tension: 0.3,
        pointRadius: 3,
        pointHoverRadius: 6,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        intersect: false,
        mode: 'index',
      },
      scales: {
        x: {
          type: 'time',
          time: {
            unit: selectedPeriod <= 7 ? 'day' : selectedPeriod <= 30 ? 'week' : 'month',
            displayFormats: {
              day: 'MMM d',
              week: 'MMM d',
              month: 'MMM yyyy',
            },
          },
          grid: { color: gridColor },
          ticks: { color: textColor },
        },
        y: {
          beginAtZero: false,
          title: {
            display: true,
            text: 'Time (ms)',
            color: textColor,
          },
          grid: { color: gridColor },
          ticks: { color: textColor },
        },
      },
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          callbacks: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            label: (context: any) => `${context.parsed.y.toFixed(2)} ms`,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            afterLabel: (context: any) => {
              const entry = sortedTimeline[context.dataIndex];
              if (entry.events && entry.events.length > 0) {
                return entry.events.map(e => `• ${e}`);
              }
              return '';
            },
          },
        },
      },
    },
  });

  // Render key events
  renderTimelineEvents(sortedTimeline);
}

function renderTimelineEvents(timeline: TimelineEntry[]): void {
  const container = document.getElementById('timeline-events');
  if (!container) return;

  const eventsWithDates = timeline
    .filter(entry => entry.events && entry.events.length > 0)
    .slice(-5); // Show last 5 events

  if (eventsWithDates.length === 0) {
    container.classList.add('hidden');
    return;
  }

  container.classList.remove('hidden');
  container.innerHTML = `
    <h4 class="font-medium text-foreground mb-2">Key Events:</h4>
    <ul class="space-y-1">
      ${eventsWithDates.map(entry => entry.events.map(event => `
        <li class="flex items-center gap-2">
          <span class="text-muted">${formatDate(entry.date)}:</span>
          <span class="text-foreground">${escapeHtml(event)}</span>
        </li>
      `).join('')).join('')}
    </ul>
  `;
}

function setupPeriodButtons(): void {
  const container = document.getElementById('timeline-period-buttons');
  if (!container) return;

  container.addEventListener('click', (e) => {
    const target = e.target as HTMLElement;
    if (target.tagName !== 'BUTTON') return;

    const period = parseInt(target.dataset.period || '30', 10);
    if (period === selectedPeriod) return;

    selectedPeriod = period;

    // Update button styles
    container.querySelectorAll('button').forEach(btn => {
      const isSelected = parseInt(btn.dataset.period || '30', 10) === period;
      btn.className = isSelected
        ? 'px-3 py-1 text-sm rounded border border-border bg-primary-light dark:bg-primary-dark text-white'
        : 'px-3 py-1 text-sm rounded border border-border bg-card hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors';
    });

    renderTimelineChart();
  });
}

// =============================================================================
// Benchmark Suite Tabs
// =============================================================================

function renderBenchmarkTabs(): void {
  const tabsContainer = document.getElementById('benchmark-tabs');
  if (!tabsContainer) return;

  tabsContainer.addEventListener('click', (e) => {
    const target = e.target as HTMLElement;
    if (target.getAttribute('role') !== 'tab') return;

    const suite = target.dataset.suite;
    if (!suite || suite === selectedSuite) return;

    selectedSuite = suite;

    // Update tab styles
    tabsContainer.querySelectorAll('[role="tab"]').forEach(tab => {
      const tabElement = tab as HTMLElement;
      const isSelected = tabElement.dataset.suite === suite;
      tab.setAttribute('aria-selected', isSelected.toString());
      tabElement.className = isSelected
        ? 'px-6 py-3 text-sm font-medium border-b-2 border-primary-light dark:border-primary-dark text-primary-light dark:text-primary-dark whitespace-nowrap'
        : 'px-6 py-3 text-sm font-medium border-b-2 border-transparent text-muted hover:text-foreground hover:border-gray-300 dark:hover:border-gray-600 whitespace-nowrap';
    });

    renderBenchmarkContent();
  });
}

function renderBenchmarkContent(): void {
  const container = document.getElementById('benchmark-content');
  if (!container || !dashboardData) return;

  const suite = dashboardData.benchmarks[selectedSuite as keyof typeof dashboardData.benchmarks];

  if (!suite) {
    container.innerHTML = `
      <div class="text-center py-8 text-muted">
        <p>No data available for this benchmark suite.</p>
        <p class="text-sm mt-2">Data will appear when the benchmark runs in CI.</p>
      </div>
    `;
    return;
  }

  // Render suite description
  let html = `
    <div class="mb-6">
      <h3 class="text-lg font-semibold text-foreground mb-2">${getSuiteName(selectedSuite)}</h3>
      <p class="text-muted">${escapeHtml(suite.description)}</p>
      ${suite.scale_factor ? `<p class="text-sm text-muted mt-1">Scale factor: ${suite.scale_factor}</p>` : ''}
    </div>
  `;

  // Render query table if available
  const queries = suite.queries || suite.tests;
  if (queries && Object.keys(queries).length > 0) {
    html += renderQueryTable(queries);
  } else if (suite.latest) {
    // Render TPC-C style summary
    html += renderTpccSummary(suite);
  }

  container.innerHTML = html;
}

function getSuiteName(suite: string): string {
  const names: Record<string, string> = {
    tpch: 'TPC-H Decision Support Queries',
    tpcc: 'TPC-C OLTP Transactions',
    tpcds: 'TPC-DS Decision Support Queries',
    sysbench: 'Sysbench OLTP Operations',
  };
  return names[suite] || suite.toUpperCase();
}

function renderQueryTable(queries: Record<string, QueryData>): string {
  const queryEntries = Object.entries(queries).sort(([a], [b]) => {
    // Sort by query number (Q1, Q2, etc.)
    const numA = parseInt(a.replace(/\D/g, ''), 10) || 0;
    const numB = parseInt(b.replace(/\D/g, ''), 10) || 0;
    return numA - numB;
  });

  return `
    <div class="overflow-x-auto">
      <table class="w-full text-sm">
        <thead class="text-xs uppercase bg-card text-muted border-b border-border">
          <tr>
            <th class="px-4 py-3 text-left">Query</th>
            <th class="px-4 py-3 text-right">Latest</th>
            <th class="px-4 py-3 text-right">7d Avg</th>
            <th class="px-4 py-3 text-right">Trend</th>
            <th class="px-4 py-3 text-right">vs SQLite</th>
            <th class="px-4 py-3 text-center">Status</th>
            <th class="px-4 py-3 text-center">Sparkline</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-border">
          ${queryEntries.map(([key, query]) => renderQueryRow(key, query)).join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderQueryRow(key: string, query: QueryData): string {
  const { latest, stats, history } = query;

  // Calculate vs SQLite ratio
  let vsRatio = '—';
  let ratioClass = 'text-muted';
  if (latest.vibesql_ms && latest.sqlite_ms && latest.vibesql_ms > 0 && latest.sqlite_ms > 0) {
    const ratio = latest.sqlite_ms / latest.vibesql_ms;
    vsRatio = `${ratio.toFixed(1)}x`;
    if (ratio > 1) {
      vsRatio += ' ✓';
      ratioClass = 'text-green-600 dark:text-green-400';
    } else if (ratio < 1) {
      ratioClass = 'text-red-600 dark:text-red-400';
    }
  }

  return `
    <tr class="hover:bg-card/50 transition-colors">
      <td class="px-4 py-3">
        <span class="font-medium text-foreground" title="${escapeHtml(query.description)}">
          ${escapeHtml(key.toUpperCase())}
        </span>
        <span class="block text-xs text-muted truncate max-w-[200px]" title="${escapeHtml(query.name)}">
          ${escapeHtml(query.name)}
        </span>
      </td>
      <td class="px-4 py-3 text-right font-mono text-foreground">
        ${formatMs(latest.vibesql_ms)}
      </td>
      <td class="px-4 py-3 text-right font-mono text-muted">
        ${stats ? formatMs(stats.avg_7d_ms) : '—'}
      </td>
      <td class="px-4 py-3 text-right">
        ${stats ? formatTrend(stats.trend_pct) : '—'}
      </td>
      <td class="px-4 py-3 text-right ${ratioClass} font-medium">
        ${vsRatio}
      </td>
      <td class="px-4 py-3 text-center">
        ${getStatusBadge(latest.status)}
      </td>
      <td class="px-4 py-3 text-center">
        ${renderSparkline(history)}
      </td>
    </tr>
  `;
}

function renderSparkline(history: QueryHistoryEntry[]): string {
  if (!history || history.length < 2) {
    return '<span class="text-muted">—</span>';
  }

  // Take last 10 data points
  const data = history.slice(-10);
  const values = data.map(h => h.ms);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  // SVG sparkline
  const width = 50;
  const height = 20;
  const padding = 2;

  const points = values.map((v, i) => {
    const x = padding + (i / (values.length - 1)) * (width - 2 * padding);
    const y = height - padding - ((v - min) / range) * (height - 2 * padding);
    return `${x},${y}`;
  }).join(' ');

  // Determine trend color
  const first = values[0];
  const last = values[values.length - 1];
  const color = last < first ? '#22c55e' : last > first ? '#ef4444' : '#6b7280';

  return `
    <svg width="${width}" height="${height}" class="inline-block">
      <polyline
        points="${points}"
        fill="none"
        stroke="${color}"
        stroke-width="1.5"
        stroke-linecap="round"
        stroke-linejoin="round"
      />
    </svg>
  `;
}

function renderTpccSummary(suite: BenchmarkSuite): string {
  if (!suite.latest) return '';

  return `
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      ${suite.latest.vibesql_tps !== undefined ? `
        <div class="bg-card rounded-lg p-4 border border-border">
          <h4 class="text-sm font-medium text-muted mb-1">VibeSQL TPS</h4>
          <p class="text-2xl font-bold text-foreground">${suite.latest.vibesql_tps.toLocaleString()}</p>
        </div>
      ` : ''}
      ${suite.latest.sqlite_tps !== undefined ? `
        <div class="bg-card rounded-lg p-4 border border-border">
          <h4 class="text-sm font-medium text-muted mb-1">SQLite TPS</h4>
          <p class="text-2xl font-bold text-foreground">${suite.latest.sqlite_tps.toLocaleString()}</p>
        </div>
      ` : ''}
      ${suite.latest.duckdb_tps !== undefined ? `
        <div class="bg-card rounded-lg p-4 border border-border">
          <h4 class="text-sm font-medium text-muted mb-1">DuckDB TPS</h4>
          <p class="text-2xl font-bold text-foreground">${suite.latest.duckdb_tps.toLocaleString()}</p>
        </div>
      ` : ''}
      ${suite.latest.mixed_latency_us !== undefined ? `
        <div class="bg-card rounded-lg p-4 border border-border">
          <h4 class="text-sm font-medium text-muted mb-1">Mixed Latency</h4>
          <p class="text-2xl font-bold text-foreground">${suite.latest.mixed_latency_us.toLocaleString()} µs</p>
        </div>
      ` : ''}
    </div>
  `;
}

// =============================================================================
// Recent Changes Feed
// =============================================================================

function renderChangesFeed(): void {
  const container = document.getElementById('changes-feed');
  if (!container || !dashboardData) return;

  const { changes } = dashboardData;

  if (!changes || changes.length === 0) {
    container.innerHTML = `
      <div class="text-center py-4 text-muted">
        No recent changes to display.
      </div>
    `;
    return;
  }

  // Show last 10 changes
  const recentChanges = changes.slice(0, 10);

  container.innerHTML = recentChanges.map(change => `
    <div class="flex items-start gap-3 py-2">
      ${getChangeIcon(change.type)}
      <div class="flex-1 min-w-0">
        <div class="flex items-baseline justify-between gap-2">
          <span class="font-medium text-foreground">${escapeHtml(change.description)}</span>
          <span class="text-xs text-muted whitespace-nowrap">${formatDate(change.date)}</span>
        </div>
        <div class="text-sm text-muted">
          ${change.category}${change.query ? ` / ${change.query}` : ''}
          ${change.before_ms && change.after_ms ? ` (${formatMs(change.before_ms)} → ${formatMs(change.after_ms)})` : ''}
        </div>
        <div class="text-xs text-muted mt-1">
          ${change.commit ? `<a href="https://github.com/rjwalters/vibesql/commit/${change.commit}" target="_blank" rel="noopener noreferrer" class="hover:text-primary-light dark:hover:text-primary-dark">commit ${change.commit.slice(0, 7)}</a>` : ''}
          ${change.pr ? `<a href="https://github.com/rjwalters/vibesql/pull/${change.pr}" target="_blank" rel="noopener noreferrer" class="hover:text-primary-light dark:hover:text-primary-dark ml-2">PR #${change.pr}</a>` : ''}
        </div>
      </div>
    </div>
  `).join('');
}

// =============================================================================
// Machine Info
// =============================================================================

function renderMachineInfo(): void {
  const container = document.getElementById('machine-info');
  if (!container || !dashboardData || !dashboardData.machine_info) return;

  const { machine_info } = dashboardData;

  container.innerHTML = `
    <p>
      <span class="text-muted">Benchmarks run on:</span>
      <span class="text-foreground">${escapeHtml(machine_info.system)}</span>
      ${machine_info.cpu ? `<span class="text-muted ml-2">${escapeHtml(machine_info.cpu)}</span>` : ''}
    </p>
    <p class="text-xs mt-1">
      ${dashboardData.generated_at ? `Last updated: ${new Date(dashboardData.generated_at).toLocaleString()}` : ''}
    </p>
  `;
}

// =============================================================================
// Data Loading
// =============================================================================

async function loadDashboardData(): Promise<void> {
  try {
    const response = await fetch(`${import.meta.env.BASE_URL}data/dashboard.json`);

    if (!response.ok) {
      throw new Error(`Failed to load dashboard data: ${response.status}`);
    }

    dashboardData = await response.json();

    // Render all components
    renderHeroCards();
    renderTimelineChart();
    renderBenchmarkContent();
    renderChangesFeed();
    renderMachineInfo();
  } catch (error) {
    console.error('Error loading dashboard data:', error);
    showError();
  }
}

function showError(): void {
  // Show error in hero cards
  const heroCards = document.getElementById('hero-cards');
  if (heroCards) {
    heroCards.innerHTML = `
      <div class="col-span-full bg-background rounded-lg shadow-lg p-6 border border-red-300 dark:border-red-700">
        <div class="flex items-center gap-3">
          <span class="text-red-500 text-2xl">⚠️</span>
          <div>
            <h3 class="font-medium text-foreground">Unable to load performance data</h3>
            <p class="text-sm text-muted">Dashboard data is not available. This may be because the data generation script hasn't run yet.</p>
          </div>
        </div>
      </div>
    `;
  }

  // Hide other sections or show placeholders
  const timelineCanvas = document.getElementById('timeline-chart');
  if (timelineCanvas) {
    const parent = timelineCanvas.parentElement;
    if (parent) {
      parent.innerHTML = '<div class="flex items-center justify-center h-full text-muted">No data available</div>';
    }
  }

  const benchmarkContent = document.getElementById('benchmark-content');
  if (benchmarkContent) {
    benchmarkContent.innerHTML = '<div class="text-center py-8 text-muted">No benchmark data available</div>';
  }

  const changesFeed = document.getElementById('changes-feed');
  if (changesFeed) {
    changesFeed.innerHTML = '<div class="text-center py-4 text-muted">No changes to display</div>';
  }
}

// =============================================================================
// Initialization
// =============================================================================

document.addEventListener('DOMContentLoaded', () => {
  // Initialize theme system
  const theme = initTheme();

  // Initialize navigation component
  new NavigationComponent('performance', theme);

  // Setup event handlers
  setupPeriodButtons();
  renderBenchmarkTabs();

  // Load data
  loadDashboardData();
});
