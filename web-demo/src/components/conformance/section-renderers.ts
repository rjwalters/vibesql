import type {
  ConformanceData,
  SQLLogicTestData,
  ErrorTest,
  DashboardConformance,
  DashboardMilestone,
} from './types'
import { getStatusColor, getStatusText, escapeHtml } from './render-utils'

/**
 * Format large numbers with commas for readability
 */
function formatNumber(num: number): string {
  return num.toLocaleString()
}

/**
 * Render the hero overview section with large pass rate display
 */
export function renderConformanceOverview(conformance: DashboardConformance): string {
  const { summary } = conformance
  const passRateFormatted = summary.pass_rate.toFixed(2)
  const isFullyPassing = summary.files_passing === summary.files_total

  return `
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg border border-gray-200 dark:border-gray-700 p-8">
      <h2 class="text-xl font-bold text-gray-900 dark:text-white mb-6">SQL Conformance</h2>
      <p class="text-gray-600 dark:text-gray-400 mb-8">
        Testing against SQLLogicTest - the industry standard SQL test suite
      </p>

      <!-- Hero Pass Rate Display -->
      <div class="text-center mb-8">
        <div class="inline-block">
          <div class="text-7xl font-bold ${isFullyPassing ? 'text-green-600 dark:text-green-400' : 'text-blue-600 dark:text-blue-400'} mb-2">
            ${passRateFormatted}%
          </div>
          ${isFullyPassing ? '<div class="text-green-600 dark:text-green-400 font-semibold text-lg mb-4">100% File Pass Rate Achieved!</div>' : ''}
        </div>
      </div>

      <!-- Progress Bar -->
      <div class="relative mb-6">
        <div class="h-4 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
          <div
            class="h-full ${isFullyPassing ? 'bg-green-500' : 'bg-blue-500'} rounded-full transition-all duration-500"
            style="width: ${summary.pass_rate}%"
          ></div>
        </div>
      </div>

      <!-- Stats Row -->
      <div class="grid grid-cols-1 md:grid-cols-2 gap-6 text-center">
        <div class="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <div class="text-sm text-gray-600 dark:text-gray-400 mb-1">Tests Passing</div>
          <div class="text-2xl font-bold text-gray-900 dark:text-white">
            ${formatNumber(summary.tests_passing)} / ${formatNumber(summary.tests_total)}
          </div>
        </div>
        <div class="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
          <div class="text-sm text-gray-600 dark:text-gray-400 mb-1">Files Passing</div>
          <div class="text-2xl font-bold ${isFullyPassing ? 'text-green-600 dark:text-green-400' : 'text-gray-900 dark:text-white'}">
            ${formatNumber(summary.files_passing)} / ${formatNumber(summary.files_total)}
            ${isFullyPassing ? ' (100%)' : ''}
          </div>
        </div>
      </div>
    </div>
  `
}

/**
 * Render category breakdown table
 */
export function renderCategoryBreakdown(
  categories: Record<string, { total: number; passing: number; pass_rate: number }>
): string {
  const categoryNames: Record<string, string> = {
    select: 'SELECT Queries',
    aggregates: 'Aggregates',
    joins: 'JOINs',
    expressions: 'Expressions',
    subqueries: 'Subqueries',
    index: 'Index Operations',
    ddl: 'DDL Statements',
    evidence: 'Evidence Tests',
    random: 'Random Tests',
    other: 'Other Tests',
  }

  const rows = Object.entries(categories)
    .sort((a, b) => b[1].total - a[1].total) // Sort by total tests descending
    .map(([key, cat]) => {
      const name = categoryNames[key] || key.charAt(0).toUpperCase() + key.slice(1)
      const passRate = cat.pass_rate.toFixed(1)
      const isComplete = cat.pass_rate === 100

      return `
        <tr class="border-b border-gray-200 dark:border-gray-700 last:border-0">
          <td class="py-3 px-4 font-medium text-gray-900 dark:text-white">${name}</td>
          <td class="py-3 px-4">
            <div class="flex items-center gap-2">
              <span class="font-semibold ${isComplete ? 'text-green-600 dark:text-green-400' : 'text-gray-900 dark:text-white'}">
                ${passRate}%
              </span>
              ${isComplete ? '<span class="text-green-500">✓</span>' : ''}
            </div>
          </td>
          <td class="py-3 px-4 w-1/3">
            <div class="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
              <div
                class="h-full ${isComplete ? 'bg-green-500' : 'bg-blue-500'} rounded-full"
                style="width: ${cat.pass_rate}%"
              ></div>
            </div>
          </td>
          <td class="py-3 px-4 text-right text-gray-600 dark:text-gray-400">
            ${formatNumber(cat.passing)} / ${formatNumber(cat.total)}
          </td>
        </tr>
      `
    })
    .join('')

  if (rows.length === 0) {
    return ''
  }

  return `
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg border border-gray-200 dark:border-gray-700 p-8">
      <h2 class="text-xl font-bold text-gray-900 dark:text-white mb-6">Test Coverage by Category</h2>

      <div class="overflow-x-auto">
        <table class="w-full">
          <thead>
            <tr class="border-b border-gray-200 dark:border-gray-700 text-left text-sm text-gray-600 dark:text-gray-400">
              <th class="py-3 px-4 font-medium">Category</th>
              <th class="py-3 px-4 font-medium">Pass Rate</th>
              <th class="py-3 px-4 font-medium">Progress</th>
              <th class="py-3 px-4 font-medium text-right">Tests</th>
            </tr>
          </thead>
          <tbody>
            ${rows}
          </tbody>
        </table>
      </div>
    </div>
  `
}

/**
 * Render container for conformance timeline chart
 * The actual chart will be initialized by Chart.js after render
 */
export function renderConformanceTimeline(): string {
  return `
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg border border-gray-200 dark:border-gray-700 p-8">
      <h2 class="text-xl font-bold text-gray-900 dark:text-white mb-2">Pass Rate History</h2>
      <p class="text-gray-600 dark:text-gray-400 mb-6">Conformance progress over the last 90 days</p>

      <div class="h-64 md:h-80">
        <canvas id="conformance-timeline-chart"></canvas>
      </div>

      <div id="timeline-loading" class="text-center py-12 text-gray-500 dark:text-gray-400">
        Loading chart data...
      </div>
    </div>
  `
}

/**
 * Render milestones/achievements section
 */
export function renderMilestones(milestones: DashboardMilestone[]): string {
  if (!milestones || milestones.length === 0) {
    return ''
  }

  const milestoneItems = milestones
    .slice(0, 5) // Show only the 5 most recent
    .map(m => {
      const dateFormatted = new Date(m.date).toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
      })

      const linkHtml = m.pr
        ? `<a href="https://github.com/rjwalters/vibesql/pull/${m.pr}" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline text-sm">PR #${m.pr}</a>`
        : m.commit
          ? `<a href="https://github.com/rjwalters/vibesql/commit/${m.commit}" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline text-sm font-mono">${m.commit.slice(0, 7)}</a>`
          : ''

      return `
        <div class="flex items-start gap-4 py-3 border-b border-gray-200 dark:border-gray-700 last:border-0">
          <div class="flex-shrink-0 w-16 text-sm font-medium text-gray-600 dark:text-gray-400">
            ${dateFormatted}
          </div>
          <div class="flex-grow">
            <div class="text-gray-900 dark:text-white">${escapeHtml(m.description)}</div>
            ${linkHtml ? `<div class="mt-1">${linkHtml}</div>` : ''}
          </div>
          <div class="flex-shrink-0 text-green-500 text-xl">✓</div>
        </div>
      `
    })
    .join('')

  return `
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg border border-gray-200 dark:border-gray-700 p-8">
      <h2 class="text-xl font-bold text-gray-900 dark:text-white mb-6">Milestones</h2>
      <div>
        ${milestoneItems}
      </div>
    </div>
  `
}

// Legacy section renderers for backward compatibility

/**
 * Render metadata card with commit info and status
 */
export function renderMetadataCard(commit: string, timestamp: string, passRate: number): string {
  const statusColor = getStatusColor(passRate)
  const statusText = getStatusText(passRate)

  return `
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-6">
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
        <div>
          <span class="text-gray-600 dark:text-gray-400">Generated:</span>
          <span class="ml-2 text-gray-900 dark:text-white font-medium">${timestamp}</span>
        </div>
        <div>
          <span class="text-gray-600 dark:text-gray-400">Commit:</span>
          <a
            href="https://github.com/rjwalters/vibesql/commit/${commit}"
            target="_blank"
            class="ml-2 text-blue-600 dark:text-blue-400 hover:underline font-mono"
          >
            ${commit}
          </a>
        </div>
        <div>
          <span class="text-gray-600 dark:text-gray-400">Status:</span>
          <span class="ml-2 font-medium" style="color: ${statusColor}">${statusText}</span>
        </div>
      </div>
    </div>
  `
}

/**
 * Render sqltest results section
 */
export function renderSqltestResults(data: ConformanceData): string {
  const passRate = (data.pass_rate || 0).toFixed(1)

  return `
    <div id="sqltest" class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-8">
      <h2 class="text-2xl font-bold text-gray-900 dark:text-white mb-6">sqltest Results</h2>

      <p class="text-gray-700 dark:text-gray-300 mb-6">
        Results from
        <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">sqltest</a>
        - a community-maintained BNF-driven conformance test suite derived from the SQL:1999 standard, containing 739 tests covering Core and Foundation features.
      </p>

      <!-- Overall Results -->
      <div class="bg-gradient-to-br from-blue-500 to-blue-700 rounded-lg shadow-lg p-6 text-white mb-6">
        <div class="text-sm font-semibold uppercase tracking-wider opacity-90 mb-2">Overall Pass Rate</div>
        <div class="text-4xl font-bold mb-2">${passRate}%</div>
        <div class="text-sm opacity-75">${data.passed} of ${data.total} tests passing</div>
        <div class="mt-4 bg-white/20 rounded-full h-2 overflow-hidden">
          <div
            class="bg-white h-full rounded-full transition-all duration-500"
            style="width: ${passRate}%"
          ></div>
        </div>
      </div>

      <!-- Summary Cards -->
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
        <!-- Passed Tests -->
        <div class="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 border border-gray-200 dark:border-gray-700">
          <div class="text-xs font-semibold uppercase tracking-wider text-gray-600 dark:text-gray-400 mb-2">Passed</div>
          <div class="text-3xl font-bold text-green-600 dark:text-green-400 mb-1">${data.passed}</div>
        </div>

        <!-- Failed Tests -->
        <div class="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 border border-gray-200 dark:border-gray-700">
          <div class="text-xs font-semibold uppercase tracking-wider text-gray-600 dark:text-gray-400 mb-2">Failed</div>
          <div class="text-3xl font-bold text-red-600 dark:text-red-400 mb-1">${data.failed}</div>
        </div>

        <!-- Errors -->
        <div class="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 border border-gray-200 dark:border-gray-700">
          <div class="text-xs font-semibold uppercase tracking-wider text-gray-600 dark:text-gray-400 mb-2">Errors</div>
          <div class="text-3xl font-bold text-orange-600 dark:text-orange-400 mb-1">${data.errors}</div>
        </div>
      </div>

      <!-- Test Coverage -->
      <div>
        <h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Test Coverage</h3>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">Core Features (E-Series)</h4>
            <ul class="space-y-2 text-sm text-gray-700 dark:text-gray-300">
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E011</span> Numeric data types</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E021</span> Character string types</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E031</span> Identifiers</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E051</span> Basic query specification</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E061</span> Basic predicates and search conditions</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E071</span> Basic query expressions</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E081</span> Basic privileges</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E091</span> Set functions</li>
            </ul>
          </div>
          <div>
            <h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">Additional Features</h4>
            <ul class="space-y-2 text-sm text-gray-700 dark:text-gray-300">
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E101</span> Basic data manipulation</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E111</span> Single row SELECT statement</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E121</span> Basic cursor support</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E131</span> Null value support</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E141</span> Basic integrity constraints</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E151</span> Transaction support</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E161</span> SQL comments</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">F031</span> Basic schema manipulation</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  `
}

/**
 * Render explanation section about the test suites
 */
export function renderExplanation(data: ConformanceData, sltData: SQLLogicTestData | null): string {
  const sqltestPassRate = (data.pass_rate || 0).toFixed(1)
  const sltPassRate = sltData ? (sltData.pass_rate || 0).toFixed(1) : 'N/A'

  return `
    <div class="bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800 p-6">
      <h2 class="text-xl font-bold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
        <span class="text-2xl">ℹ️</span>
        Understanding Our Test Suites
      </h2>

      <div class="space-y-4 text-sm text-gray-700 dark:text-gray-300">
        <div>
          <h3 class="font-semibold text-gray-900 dark:text-white mb-2">What is sqltest?</h3>
          <p>
            <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">sqltest</a>
            is a community-maintained test suite by Elliot Chance that provides BNF-driven conformance tests derived from the SQL:1999 standard.
            It contains 739 tests covering Core and Foundation features across E-series and F-series test categories. This suite tests whether
            our implementation conforms to the SQL:1999 grammar specification.
          </p>
        </div>

        <div>
          <h3 class="font-semibold text-gray-900 dark:text-white mb-2">What is SQLLogicTest?</h3>
          <p>
            <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">SQLLogicTest</a>
            is a comprehensive test suite originally developed for SQLite, containing ~5.9 million SQL test cases across 623 test files.
            It tests practical correctness by running real-world queries and validating results. This suite focuses on semantic correctness
            and edge cases rather than pure grammar conformance.
          </p>
        </div>

        <div>
          <h3 class="font-semibold text-gray-900 dark:text-white mb-2">How do they complement each other?</h3>
          <ul class="list-disc list-inside space-y-2 ml-2">
            <li>
              <span class="font-medium">sqltest (BNF-driven):</span> Validates grammar conformance to SQL:1999 standard specifications
            </li>
            <li>
              <span class="font-medium">SQLLogicTest (Result-driven):</span> Validates semantic correctness with millions of real queries
            </li>
            <li>
              <span class="font-medium">Coverage:</span> sqltest covers 739 standard feature tests; SQLLogicTest covers practical scenarios
            </li>
            <li>
              <span class="font-medium">Philosophy:</span> sqltest says "can you parse this?"; SQLLogicTest says "does this work correctly?"
            </li>
          </ul>
        </div>

        <div>
          <h3 class="font-semibold text-gray-900 dark:text-white mb-2">What is SQL:1999 Core?</h3>
          <p>
            SQL:1999 Core is the official mandatory feature set defined in the SQL:1999 (ISO/IEC 9075:1999) standard.
            It consists of approximately 169 required features that any database claiming Core compliance must implement.
            Official Core compliance is verified through the NIST SQL Test Suite, not community test suites.
          </p>
        </div>

        <div>
          <h3 class="font-semibold text-gray-900 dark:text-white mb-2">What do our pass rates mean?</h3>
          <p>
            Our <strong>${sqltestPassRate}% sqltest pass rate</strong> (${data.passed}/${data.total} tests) demonstrates strong SQL:1999 grammar conformance.
            ${sltData ? `Our <strong>${sltPassRate}% SQLLogicTest pass rate</strong> (${sltData.passed}/${sltData.total} test files) shows we handle real-world queries correctly.` : ''}
            Together, these results indicate comprehensive SQL:1999 compliance, though they do not constitute official Core certification.
          </p>
        </div>

        <div class="bg-white dark:bg-gray-800 rounded-lg p-4 border border-blue-300 dark:border-blue-700">
          <p class="text-xs text-gray-600 dark:text-gray-400">
            <strong>Bottom Line:</strong> We use two complementary test suites to ensure both standards conformance (sqltest) and practical
            correctness (SQLLogicTest). High pass rates in both demonstrate serious SQL:1999 implementation quality, though formal Core
            certification would require testing against official NIST suites.
          </p>
        </div>
      </div>
    </div>
  `
}

/**
 * Render failing tests section
 */
export function renderFailingTests(errorTests: ErrorTest[]): string {
  if (errorTests.length === 0) return ''

  const errorCards = errorTests
    .map(
      test => `
      <div class="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 border border-gray-200 dark:border-gray-700">
        <div class="flex items-start justify-between mb-2">
          <span class="font-mono text-xs text-blue-600 dark:text-blue-400 font-semibold">${escapeHtml(test.id)}</span>
        </div>
        <div class="font-mono text-sm text-gray-800 dark:text-gray-200 mb-2 bg-white dark:bg-gray-800 p-2 rounded overflow-x-auto">${escapeHtml(test.sql)}</div>
        <div class="text-xs text-red-600 dark:text-red-400"><span class="font-semibold">Error:</span> ${escapeHtml(test.error)}</div>
      </div>
    `
    )
    .join('')

  return `
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-8">
      <h2 class="text-2xl font-bold text-gray-900 dark:text-white mb-6">Failing Tests</h2>

      <p class="text-gray-700 dark:text-gray-300 mb-4">
        The following tests are currently failing. Click to expand details.
      </p>

      <details class="mt-4">
        <summary class="cursor-pointer text-blue-600 dark:text-blue-400 hover:underline font-medium">
          View failing test details (${errorTests.length} tests)
        </summary>

        <div class="mt-4 space-y-3 max-h-96 overflow-y-auto">
          ${errorCards}
        </div>
      </details>
    </div>
  `
}

/**
 * Render SQLLogicTest results section
 */
export function renderSQLLogicTestResults(sltData: SQLLogicTestData): string {
  const passRate = (sltData.pass_rate || 0).toFixed(1)

  // Handle case where categories might be undefined
  const categoriesData = sltData.categories || {}
  const categories = [
    { key: 'select', name: 'SELECT Tests', data: categoriesData.select },
    { key: 'evidence', name: 'Evidence Tests', data: categoriesData.evidence },
    { key: 'index', name: 'Index Tests', data: categoriesData.index },
    { key: 'random', name: 'Random Tests', data: categoriesData.random },
    { key: 'ddl', name: 'DDL Tests', data: categoriesData.ddl },
    { key: 'other', name: 'Other Tests', data: categoriesData.other },
  ]

  const categoryCards = categories
    .filter(cat => cat.data)
    .map(cat => {
      const catData = cat.data!
      const catPassRate = (catData.pass_rate || 0).toFixed(1)
      return `
      <div class="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 border border-gray-200 dark:border-gray-700">
        <div class="text-sm font-semibold text-gray-900 dark:text-white mb-2">${cat.name}</div>
        <div class="text-2xl font-bold text-blue-600 dark:text-blue-400 mb-1">${catPassRate}%</div>
        <div class="text-xs text-gray-600 dark:text-gray-400">${catData.passed}/${catData.total} passed</div>
      </div>
    `
    })
    .join('')

  return `
    <div id="sqllogictest" class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-8">
      <h2 class="text-2xl font-bold text-gray-900 dark:text-white mb-6">SQLLogicTest Results</h2>

      <p class="text-gray-700 dark:text-gray-300 mb-6">
        Results from the comprehensive
        <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">SQLLogicTest</a>
        suite containing ~5.9 million tests across 623 test files from the official SQLite corpus.
      </p>

      <!-- Overall Results -->
      <div class="bg-gradient-to-br from-purple-500 to-purple-700 rounded-lg shadow-lg p-6 text-white mb-6">
        <div class="text-sm font-semibold uppercase tracking-wider opacity-90 mb-2">Overall Pass Rate</div>
        <div class="text-4xl font-bold mb-2">${passRate}%</div>
        <div class="text-sm opacity-75">${sltData.passed} of ${sltData.total} test files passing</div>
        <div class="mt-4 bg-white/20 rounded-full h-2 overflow-hidden">
          <div
            class="bg-white h-full rounded-full transition-all duration-500"
            style="width: ${passRate}%"
          ></div>
        </div>
      </div>

      <!-- Category Breakdown -->
      <div>
        <h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Test Categories</h3>
        <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          ${categoryCards}
        </div>
      </div>

      <!-- Info Box -->
      <div class="mt-6 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800 p-4">
        <p class="text-sm text-gray-700 dark:text-gray-300">
          <strong>Note:</strong> SQLLogicTest provides a different perspective from sqltest. While sqltest focuses on BNF grammar
          conformance from the SQL:1999 specification, SQLLogicTest contains millions of real-world SQL queries testing practical
          correctness across a wide range of scenarios.
        </p>
      </div>
    </div>
  `
}

/**
 * Render running tests locally section
 */
export function renderRunningTestsLocally(): string {
  return `
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-8">
      <h2 class="text-2xl font-bold text-gray-900 dark:text-white mb-6">Running Tests Locally</h2>

      <div class="bg-gray-100 dark:bg-gray-900 rounded-lg p-4 font-mono text-sm overflow-x-auto">
        <div class="text-gray-700 dark:text-gray-300">
          <div class="text-green-600 dark:text-green-400"># Run SQL:1999 conformance tests</div>
          <div>cargo test --test sqltest_conformance -- --nocapture</div>
          <div class="mt-4 text-green-600 dark:text-green-400"># Run SQLLogicTest suite (takes hours)</div>
          <div>cargo test --test sqllogictest_suite -- --nocapture</div>
          <div class="mt-4 text-green-600 dark:text-green-400"># Generate coverage report</div>
          <div>cargo coverage</div>
          <div class="mt-2 text-green-600 dark:text-green-400"># Open coverage report</div>
          <div>open target/llvm-cov/html/index.html</div>
        </div>
      </div>
    </div>
  `
}
