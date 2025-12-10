/**
 * Performance Trends page
 *
 * Loads and displays historical benchmark performance data for VibeSQL Embedded.
 * Shows trends over time for TPC-H, TPC-DS, TPC-C, and Sysbench benchmarks.
 */

import './styles/main.css';
import { initTheme } from './theme';
import { initLocale } from './locale';
import { NavigationComponent } from './components/Navigation';
import { initI18n, updateDOM } from './i18n';

// Chart.js is loaded via CDN in trends.html
declare const Chart: any;

// ============================================================================
// Types
// ============================================================================

interface TrendDataPoint {
  date: string;
  timestamp?: string;
  commit: string;
  avg_ms?: number;
  min_ms?: number;
  max_ms?: number;
  geomean_ms?: number;
  queries_passed?: number;
  total_queries?: number;
  tps?: number;
  latency_us?: number;
  workloads?: Record<string, number>;
}

interface BenchmarkTrend {
  suite: string;
  display_name: string;
  description: string;
  metric?: string;
  metric_label?: string;
  data: TrendDataPoint[];
}

interface TrendData {
  generated_at: string;
  git_commit: string;
  description: string;
  benchmarks: {
    tpch?: BenchmarkTrend;
    tpcds?: BenchmarkTrend;
    tpcc?: BenchmarkTrend;
    sysbench?: BenchmarkTrend;
  };
}

// ============================================================================
// Chart Colors
// ============================================================================

const COLORS = {
  avg: {
    bg: 'rgba(59, 130, 246, 0.2)',      // Blue
    border: 'rgba(59, 130, 246, 1)',
  },
  min: {
    bg: 'rgba(34, 197, 94, 0.2)',       // Green (best)
    border: 'rgba(34, 197, 94, 1)',
  },
  max: {
    bg: 'rgba(239, 68, 68, 0.2)',       // Red (worst)
    border: 'rgba(239, 68, 68, 1)',
  },
  tps: {
    bg: 'rgba(34, 197, 94, 0.2)',       // Green
    border: 'rgba(34, 197, 94, 1)',
  },
};

// ============================================================================
// Chart Creation
// ============================================================================

let charts: Map<string, any> = new Map();

/**
 * Format timestamp for x-axis display.
 * If multiple runs on same day, show HH:MM; otherwise show date.
 */
function formatTimestampForAxis(timestamp: string | undefined, date: string): string {
  if (timestamp) {
    // Parse ISO timestamp and extract time portion
    const parsed = new Date(timestamp);
    if (!isNaN(parsed.getTime())) {
      const hours = parsed.getHours().toString().padStart(2, '0');
      const minutes = parsed.getMinutes().toString().padStart(2, '0');
      // Show date + time for clarity
      return `${date.slice(5)} ${hours}:${minutes}`;  // MM-DD HH:MM
    }
  }
  return date;
}

function createTimeSeriesChart(
  canvasId: string,
  data: TrendDataPoint[],
  config: {
    yAxisLabel: string;
    datasets: {
      key: keyof TrendDataPoint;
      label: string;
      color: { bg: string; border: string };
    }[];
    higherIsBetter?: boolean;
  }
): void {
  const canvas = document.getElementById(canvasId) as HTMLCanvasElement;
  if (!canvas) return;

  // Destroy existing chart
  if (charts.has(canvasId)) {
    charts.get(canvasId).destroy();
  }

  const ctx = canvas.getContext('2d');
  if (!ctx) return;

  // Sort data by timestamp to ensure chronological order
  const sortedData = [...data].sort((a, b) => {
    const timeA = a.timestamp || a.date;
    const timeB = b.timestamp || b.date;
    return timeA.localeCompare(timeB);
  });

  // Create time-based data points (x = timestamp, y = value)
  const datasets = config.datasets.map(ds => ({
    label: ds.label,
    data: sortedData.map(point => {
      const value = point[ds.key] as number;
      if (value === undefined || value === null) return null;
      // Parse timestamp or date into a Date object for time scale
      const timeStr = point.timestamp || point.date;
      const time = new Date(timeStr);
      return { x: time, y: value };
    }).filter(d => d !== null),
    backgroundColor: ds.color.bg,
    borderColor: ds.color.border,
    borderWidth: 2,
    fill: false,
    tension: 0.1,
    pointRadius: 4,
    pointHoverRadius: 6,
    spanGaps: false,
  }));

  // Get theme colors
  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? 'rgba(75, 85, 99, 0.5)' : 'rgba(209, 213, 219, 0.5)';
  const textColor = isDark ? '#9CA3AF' : '#6B7280';

  const chart = new Chart(ctx, {
    type: 'line',
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      scales: {
        x: {
          type: 'time',
          time: {
            tooltipFormat: 'MMM d, yyyy HH:mm',
            displayFormats: {
              hour: 'MMM d HH:mm',
              day: 'MMM d',
              week: 'MMM d',
              month: 'MMM yyyy',
            },
          },
          title: {
            display: true,
            text: 'Run Time',
            color: textColor,
          },
          ticks: {
            color: textColor,
            maxRotation: 45,
            autoSkip: true,
            maxTicksLimit: 15,
          },
          grid: {
            color: gridColor,
          },
        },
        y: {
          beginAtZero: !config.higherIsBetter,
          title: {
            display: true,
            text: config.yAxisLabel,
            color: textColor,
          },
          ticks: {
            color: textColor,
          },
          grid: {
            color: gridColor,
          },
        },
      },
      plugins: {
        legend: {
          display: true,
          position: 'top',
          labels: {
            color: textColor,
          },
        },
        tooltip: {
          callbacks: {
            title: (items: any[]) => {
              if (items.length === 0) return '';
              const idx = items[0].dataIndex;
              const point = sortedData[idx];
              // Use the raw data's x value (Date object) for display
              const rawData = items[0].raw;
              const time = rawData?.x instanceof Date
                ? rawData.x.toLocaleString()
                : (point.timestamp ? new Date(point.timestamp).toLocaleString() : point.date);
              return `${time} (${point.commit || 'unknown'})`;
            },
            label: (context: any) => {
              const value = context.parsed.y;
              if (value === null) return `${context.dataset.label}: N/A`;
              if (config.higherIsBetter) {
                return `${context.dataset.label}: ${value.toFixed(2)} TPS`;
              }
              return `${context.dataset.label}: ${value.toFixed(2)} ms`;
            },
          },
        },
      },
    },
  });

  charts.set(canvasId, chart);
}

function renderTPCHChart(trend: BenchmarkTrend): void {
  createTimeSeriesChart('tpch-chart', trend.data, {
    yAxisLabel: 'Execution Time (ms)',
    datasets: [
      { key: 'avg_ms', label: 'Average', color: COLORS.avg },
      { key: 'min_ms', label: 'Best (Min)', color: COLORS.min },
      { key: 'max_ms', label: 'Worst (Max)', color: COLORS.max },
    ],
  });
}

function renderTPCDSChart(trend: BenchmarkTrend): void {
  createTimeSeriesChart('tpcds-chart', trend.data, {
    yAxisLabel: 'Execution Time (ms)',
    datasets: [
      { key: 'avg_ms', label: 'Average', color: COLORS.avg },
      { key: 'min_ms', label: 'Best (Min)', color: COLORS.min },
      { key: 'max_ms', label: 'Worst (Max)', color: COLORS.max },
    ],
  });
}

function renderTPCCChart(trend: BenchmarkTrend): void {
  createTimeSeriesChart('tpcc-chart', trend.data, {
    yAxisLabel: 'Transactions per Second (TPS)',
    datasets: [
      { key: 'tps', label: 'TPS', color: COLORS.tps },
    ],
    higherIsBetter: true,
  });
}

function renderSysbenchChart(trend: BenchmarkTrend): void {
  createTimeSeriesChart('sysbench-chart', trend.data, {
    yAxisLabel: 'Execution Time (ms)',
    datasets: [
      { key: 'avg_ms', label: 'Average', color: COLORS.avg },
      { key: 'min_ms', label: 'Best (Min)', color: COLORS.min },
      { key: 'max_ms', label: 'Worst (Max)', color: COLORS.max },
    ],
  });
}

// ============================================================================
// Summary Cards
// ============================================================================

function updateSummaryCards(data: TrendData): void {
  // Total runs
  let totalRuns = 0;
  const dates: string[] = [];
  let latestCommit = '';

  for (const [, trend] of Object.entries(data.benchmarks)) {
    if (trend && trend.data) {
      totalRuns += trend.data.length;
      for (const point of trend.data) {
        if (point.date) dates.push(point.date);
        if (point.commit) latestCommit = point.commit;
      }
    }
  }

  const totalRunsEl = document.getElementById('total-runs');
  if (totalRunsEl) {
    totalRunsEl.textContent = totalRuns.toString();
  }

  // Date range
  if (dates.length > 0) {
    dates.sort();
    const firstDate = dates[0];
    const lastDate = dates[dates.length - 1];
    const dateRangeEl = document.getElementById('date-range');
    if (dateRangeEl) {
      if (firstDate === lastDate) {
        dateRangeEl.textContent = firstDate;
      } else {
        dateRangeEl.textContent = `${firstDate} - ${lastDate}`;
      }
    }
  }

  // Latest commit
  const latestCommitEl = document.getElementById('latest-commit');
  if (latestCommitEl && latestCommit) {
    latestCommitEl.textContent = latestCommit;
  }

  // Generated at
  const generatedAtEl = document.getElementById('generated-at');
  if (generatedAtEl && data.generated_at) {
    const date = new Date(data.generated_at);
    generatedAtEl.textContent = date.toLocaleDateString();
  }
}

// ============================================================================
// Data Loading
// ============================================================================

async function loadTrendData(): Promise<TrendData | null> {
  try {
    const response = await fetch('./benchmarks/trends_results.json');
    if (!response.ok) {
      console.error('Failed to load trend data:', response.statusText);
      return null;
    }
    return await response.json();
  } catch (error) {
    console.error('Error loading trend data:', error);
    return null;
  }
}

function showNoDataMessage(chartId: string, message: string): void {
  const canvas = document.getElementById(chartId) as HTMLCanvasElement;
  if (!canvas) return;

  const parent = canvas.parentElement;
  if (!parent) return;

  parent.innerHTML = `
    <div class="flex items-center justify-center h-full text-gray-500 dark:text-gray-400">
      <p>${message}</p>
    </div>
  `;
}

// ============================================================================
// Initialization
// ============================================================================

async function init(): Promise<void> {
  // Initialize theme and locale
  const theme = initTheme();
  const locale = initLocale();

  // Initialize i18n
  initI18n(locale.current);
  updateDOM();

  // Initialize navigation
  new NavigationComponent('trends', theme, locale);

  // Load and render trend data
  const data = await loadTrendData();

  if (!data) {
    showNoDataMessage('tpch-chart', 'No trend data available. Run scripts/export_trend_json.py to generate data.');
    showNoDataMessage('tpcds-chart', 'No trend data available.');
    showNoDataMessage('tpcc-chart', 'No trend data available.');
    showNoDataMessage('sysbench-chart', 'No trend data available.');
    return;
  }

  // Update summary cards
  updateSummaryCards(data);

  // Render charts
  if (data.benchmarks.tpch && data.benchmarks.tpch.data.length > 0) {
    renderTPCHChart(data.benchmarks.tpch);
  } else {
    showNoDataMessage('tpch-chart', 'No TPC-H trend data available.');
  }

  if (data.benchmarks.tpcds && data.benchmarks.tpcds.data.length > 0) {
    renderTPCDSChart(data.benchmarks.tpcds);
  } else {
    showNoDataMessage('tpcds-chart', 'No TPC-DS trend data available.');
  }

  if (data.benchmarks.tpcc && data.benchmarks.tpcc.data.length > 0) {
    renderTPCCChart(data.benchmarks.tpcc);
  } else {
    showNoDataMessage('tpcc-chart', 'No TPC-C trend data available.');
  }

  if (data.benchmarks.sysbench && data.benchmarks.sysbench.data.length > 0) {
    renderSysbenchChart(data.benchmarks.sysbench);
  } else {
    showNoDataMessage('sysbench-chart', 'No Sysbench trend data available.');
  }

  // Re-render charts on theme change (watch for dark class toggle)
  const observer = new MutationObserver((mutations) => {
    for (const mutation of mutations) {
      if (mutation.attributeName === 'class') {
        // Theme changed, re-render charts with new colors
        if (data.benchmarks.tpch && data.benchmarks.tpch.data.length > 0) {
          renderTPCHChart(data.benchmarks.tpch);
        }
        if (data.benchmarks.tpcds && data.benchmarks.tpcds.data.length > 0) {
          renderTPCDSChart(data.benchmarks.tpcds);
        }
        if (data.benchmarks.tpcc && data.benchmarks.tpcc.data.length > 0) {
          renderTPCCChart(data.benchmarks.tpcc);
        }
        if (data.benchmarks.sysbench && data.benchmarks.sysbench.data.length > 0) {
          renderSysbenchChart(data.benchmarks.sysbench);
        }
        break;
      }
    }
  });
  observer.observe(document.documentElement, { attributes: true });
}

// Start on DOM ready
document.addEventListener('DOMContentLoaded', init);
