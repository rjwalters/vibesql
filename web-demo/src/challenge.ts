/**
 * SQL Database Challenge page
 *
 * Displays the challenge specification and VibeSQL baseline metrics.
 * Loads data from data/challenge_baseline.json to populate charts and statistics.
 */

import './styles/main.css';
import { initTheme } from './theme';
import { initLocale } from './locale';
import { NavigationComponent } from './components/Navigation';
import { initI18n, updateDOM, setI18nLocale, onI18nChange } from './i18n';
import { setBuildTimestamp } from './utils/build-timestamp';

// Chart.js is loaded via CDN in challenge.html
declare const Chart: any;

// ============================================================================
// Types
// ============================================================================

interface WeeklyDataPoint {
  week: string;
  week_start: string;
  commits: number;
  cumulative_commits: number;
  loc: number;
  pass_rate: number | null;
  passed: number | null;
  total_tests: number | null;
}

interface Milestone {
  milestone: string;
  date: string;
  commit: string;
  days_from_start: number;
}

interface ChallengeData {
  generated_at: string;
  project: {
    name: string;
    repo: string;
    first_commit: string;
    latest_commit: string;
  };
  summary: {
    duration_days: number;
    duration_weeks: number;
    total_commits: number;
    total_loc: number;
    total_rust_files: number;
    final_pass_rate: number;
    final_passed: number;
    final_total: number;
  };
  weekly_timeline: WeeklyDataPoint[];
  daily_timeline: Array<{
    date: string;
    commits: number;
    cumulative_commits: number;
  }>;
  test_milestones: Milestone[];
}

// ============================================================================
// Chart Colors
// ============================================================================

const COLORS = {
  passRate: {
    bg: 'rgba(34, 197, 94, 0.2)',       // Green
    border: 'rgba(34, 197, 94, 1)',
  },
  commits: {
    bg: 'rgba(59, 130, 246, 0.2)',      // Blue
    border: 'rgba(59, 130, 246, 1)',
  },
  loc: {
    bg: 'rgba(168, 85, 247, 0.2)',      // Purple
    border: 'rgba(168, 85, 247, 1)',
  },
};

// ============================================================================
// Chart Creation
// ============================================================================

let progressChart: any = null;

function createProgressChart(data: ChallengeData): void {
  const canvas = document.getElementById('progress-chart') as HTMLCanvasElement;
  if (!canvas) return;

  // Destroy existing chart
  if (progressChart) {
    progressChart.destroy();
  }

  const ctx = canvas.getContext('2d');
  if (!ctx) return;

  // Only show data up to day 40 (the challenge completion window)
  const MAX_DAYS = 40;
  const firstCommitDate = new Date(data.project.first_commit);

  const weeklyData = data.weekly_timeline.filter(w => {
    const weekStart = new Date(w.week_start);
    const daysDiff = Math.floor((weekStart.getTime() - firstCommitDate.getTime()) / (1000 * 60 * 60 * 24));
    return daysDiff <= MAX_DAYS;
  });

  if (weeklyData.length === 0) {
    const parent = canvas.parentElement;
    if (parent) {
      parent.innerHTML = `
        <div class="flex items-center justify-center h-full text-gray-500 dark:text-gray-400">
          <p>Progress chart will appear when data is available.</p>
        </div>
      `;
    }
    return;
  }

  // Get theme colors
  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? 'rgba(75, 85, 99, 0.5)' : 'rgba(209, 213, 219, 0.5)';
  const textColor = isDark ? '#9CA3AF' : '#6B7280';

  // Calculate days from start for each data point
  const labels = weeklyData.map(w => {
    const weekStart = new Date(w.week_start);
    const daysDiff = Math.floor((weekStart.getTime() - firstCommitDate.getTime()) / (1000 * 60 * 60 * 24));
    return `Day ${daysDiff}`;
  });

  // Map pass_rate: null means no test infrastructure yet, show as 0
  const passRates = weeklyData.map(w => w.pass_rate ?? 0);

  progressChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Pass Rate (%)',
          data: passRates,
          backgroundColor: COLORS.passRate.bg,
          borderColor: COLORS.passRate.border,
          borderWidth: 3,
          fill: true,
          tension: 0.3,
          pointRadius: 5,
          pointHoverRadius: 8,
          yAxisID: 'y',
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      scales: {
        x: {
          title: {
            display: true,
            text: 'Days from Project Start',
            color: textColor,
          },
          ticks: {
            color: textColor,
          },
          grid: {
            color: gridColor,
          },
        },
        y: {
          type: 'linear',
          position: 'left',
          min: 0,
          max: 100,
          title: {
            display: true,
            text: 'SQLLogicTest Pass Rate (%)',
            color: textColor,
          },
          ticks: {
            color: textColor,
            callback: (value: number) => `${value}%`,
          },
          grid: {
            color: gridColor,
          },
        },
      },
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          callbacks: {
            label: (context: any) => {
              const value = context.parsed.y;
              const week = weeklyData[context.dataIndex];
              if (week.pass_rate === null) {
                return 'No test data yet';
              }
              return `Pass Rate: ${value.toFixed(1)}% (${week.passed}/${week.total_tests} tests)`;
            },
          },
        },
      },
    },
  });
}

// ============================================================================
// Summary Stats
// ============================================================================

function updateStats(data: ChallengeData): void {
  // Days to 100%
  const daysEl = document.getElementById('stat-days');
  if (daysEl) {
    // Find the 100% milestone or use duration_days
    const milestone100 = data.test_milestones.find(m => m.milestone === '100%');
    const days = milestone100 ? milestone100.days_from_start : data.summary.duration_days;
    daysEl.textContent = days.toString();
  }

  // Total commits
  const commitsEl = document.getElementById('stat-commits');
  if (commitsEl) {
    commitsEl.textContent = data.summary.total_commits.toLocaleString();
  }

  // Lines of code
  const locEl = document.getElementById('stat-loc');
  if (locEl) {
    const locK = Math.round(data.summary.total_loc / 1000);
    locEl.textContent = `${locK}K`;
  }

  // Pass rate (already hardcoded as 100% in HTML)
}

// ============================================================================
// Data Loading
// ============================================================================

async function loadChallengeData(): Promise<ChallengeData | null> {
  try {
    const response = await fetch('./data/challenge_baseline.json');
    if (!response.ok) {
      console.error('Failed to load challenge data:', response.statusText);
      return null;
    }
    return await response.json();
  } catch (error) {
    console.error('Error loading challenge data:', error);
    return null;
  }
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

  // Update i18n when locale changes
  locale.onChange((newLocale) => {
    setI18nLocale(newLocale);
    updateDOM();
  });

  // Re-render DOM when i18n changes (for components that use t() directly)
  onI18nChange(() => {
    updateDOM();
  });

  // Initialize navigation
  new NavigationComponent('challenge', theme, locale);

  // Set build timestamp in footer
  setBuildTimestamp();

  // Load and render challenge data
  const data = await loadChallengeData();

  if (!data) {
    // Show placeholder values - data will be generated later
    console.log('Challenge baseline data not yet available. Run scripts/extract_challenge_data.py to generate.');
    return;
  }

  // Update stats
  updateStats(data);

  // Create progress chart
  createProgressChart(data);

  // Re-render chart on theme change
  const observer = new MutationObserver((mutations) => {
    for (const mutation of mutations) {
      if (mutation.attributeName === 'class') {
        createProgressChart(data);
        break;
      }
    }
  });
  observer.observe(document.documentElement, { attributes: true });
}

// Start on DOM ready
document.addEventListener('DOMContentLoaded', init);
