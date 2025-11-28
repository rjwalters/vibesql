import { Chart, registerables } from 'chart.js'
import type { DashboardConformanceHistoryEntry } from './types'

// Register all Chart.js components
Chart.register(...registerables)

/**
 * Initialize the conformance timeline area chart
 */
export function initTimelineChart(
  history: DashboardConformanceHistoryEntry[],
  isDarkMode: boolean
): Chart | null {
  const canvas = document.getElementById('conformance-timeline-chart') as HTMLCanvasElement | null
  const loadingEl = document.getElementById('timeline-loading')

  if (!canvas) {
    console.warn('Timeline chart canvas not found')
    return null
  }

  // Hide loading indicator
  if (loadingEl) {
    loadingEl.style.display = 'none'
  }

  // Sort history by date ascending
  const sortedHistory = [...history].sort(
    (a, b) => new Date(a.date).getTime() - new Date(b.date).getTime()
  )

  // Prepare chart data
  const labels = sortedHistory.map(entry => {
    const date = new Date(entry.date)
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  })

  const data = sortedHistory.map(entry => entry.pass_rate)

  // Calculate min for Y axis (zoomed in to show progress)
  const minRate = Math.min(...data)
  const yMin = Math.max(95, Math.floor(minRate - 1)) // Start at 95% or lower if data goes below
  const yMax = 100

  // Theme colors
  const textColor = isDarkMode ? '#9ca3af' : '#6b7280'
  const gridColor = isDarkMode ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)'
  const lineColor = isDarkMode ? '#10b981' : '#059669'
  const fillColor = isDarkMode ? 'rgba(16, 185, 129, 0.2)' : 'rgba(5, 150, 105, 0.1)'

  const chart = new Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Pass Rate',
          data,
          fill: true,
          borderColor: lineColor,
          backgroundColor: fillColor,
          borderWidth: 2,
          tension: 0.3,
          pointRadius: 3,
          pointBackgroundColor: lineColor,
          pointBorderColor: lineColor,
          pointHoverRadius: 5,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        intersect: false,
        mode: 'index',
      },
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          backgroundColor: isDarkMode ? '#1f2937' : '#ffffff',
          titleColor: isDarkMode ? '#ffffff' : '#111827',
          bodyColor: isDarkMode ? '#d1d5db' : '#4b5563',
          borderColor: isDarkMode ? '#374151' : '#e5e7eb',
          borderWidth: 1,
          padding: 12,
          callbacks: {
            label: context => {
              const value = context.parsed.y
              return `Pass Rate: ${value?.toFixed(2) ?? '0'}%`
            },
          },
        },
      },
      scales: {
        x: {
          grid: {
            display: false,
          },
          ticks: {
            color: textColor,
            maxTicksLimit: 7,
          },
        },
        y: {
          min: yMin,
          max: yMax,
          grid: {
            color: gridColor,
          },
          ticks: {
            color: textColor,
            callback: (value: string | number) => `${value}%`,
          },
        },
      },
    },
  })

  return chart
}

/**
 * Destroy and reinitialize chart (useful for theme changes)
 */
export function updateChartTheme(
  chart: Chart | null,
  history: DashboardConformanceHistoryEntry[],
  isDarkMode: boolean
): Chart | null {
  if (chart) {
    chart.destroy()
  }
  return initTimelineChart(history, isDarkMode)
}
