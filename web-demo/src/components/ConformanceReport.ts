import { Component } from './base'
import type { ConformanceReportState, DashboardData } from './conformance/types'
import { DataProcessor } from './conformance/data-processor'
import {
  renderConformanceOverview,
  renderCategoryBreakdown,
  renderConformanceTimeline,
  renderMilestones,
  renderMetadataCard,
  renderSqltestResults,
  renderExplanation,
  renderFailingTests,
  renderSQLLogicTestResults,
  renderRunningTestsLocally,
} from './conformance/section-renderers'
import { initTimelineChart, updateChartTheme } from './conformance/timeline-chart'
import type { Chart } from 'chart.js'

/**
 * Conformance Report component - displays SQL:1999 test results
 * Supports both new dashboard.json format and legacy badge JSON files
 */
export class ConformanceReportComponent extends Component<ConformanceReportState> {
  private dataProcessor = new DataProcessor()
  private timelineChart: Chart | null = null
  private themeObserver: MutationObserver | null = null

  constructor() {
    super('#conformance-content', {
      data: null,
      sltData: null,
      dashboardData: null,
      loading: true,
      error: null,
    })
    this.loadData()
    this.setupThemeObserver()
  }

  private setupThemeObserver(): void {
    // Watch for dark mode changes to update chart colors
    this.themeObserver = new MutationObserver(() => {
      if (this.state.dashboardData?.conformance?.history && this.timelineChart) {
        const isDarkMode = document.documentElement.classList.contains('dark')
        this.timelineChart = updateChartTheme(
          this.timelineChart,
          this.state.dashboardData.conformance.history,
          isDarkMode
        )
      }
    })

    this.themeObserver.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
    })
  }

  private async loadData(): Promise<void> {
    try {
      // Try loading new dashboard format first
      const dashboardData = await this.dataProcessor.loadDashboardData()

      if (dashboardData) {
        // New format available - use it
        this.setState({ dashboardData, loading: false })
      } else {
        // Fall back to legacy format
        const data = await this.dataProcessor.loadSqltestData()
        const sltData = await this.dataProcessor.loadSQLLogicTestData()
        this.setState({ data, sltData, loading: false })
      }
    } catch (error) {
      this.setState({
        error: error instanceof Error ? error.message : 'Unknown error',
        loading: false,
      })
    }
  }

  protected render(): void {
    const { data, dashboardData, loading, error } = this.state

    if (loading) {
      this.element.innerHTML = `
        <div class="text-center py-12">
          <div class="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
          <p class="mt-4 text-gray-600 dark:text-gray-400">Loading conformance report...</p>
        </div>
      `
      return
    }

    if (error) {
      this.element.innerHTML = `
        <div class="text-center py-12">
          <div class="text-red-600 dark:text-red-400 text-xl mb-4">Error Loading Report</div>
          <p class="text-gray-600 dark:text-gray-400">${this.escapeHtml(error)}</p>
        </div>
      `
      return
    }

    // Use new dashboard format if available
    if (dashboardData?.conformance) {
      this.renderDashboardFormat(dashboardData)
      return
    }

    // Fall back to legacy format
    if (!data) {
      this.element.innerHTML = `
        <div class="text-center py-12">
          <p class="text-gray-600 dark:text-gray-400">No conformance data available</p>
        </div>
      `
      return
    }

    this.renderLegacyFormat()
  }

  private renderDashboardFormat(dashboardData: DashboardData): void {
    const conformance = dashboardData.conformance!
    const hasHistory = conformance.history && conformance.history.length > 0
    const hasCategories = conformance.categories && Object.keys(conformance.categories).length > 0
    const hasMilestones = dashboardData.milestones && dashboardData.milestones.length > 0

    this.element.innerHTML = `
      <div class="space-y-8">
        ${renderConformanceOverview(conformance)}
        ${hasCategories ? renderCategoryBreakdown(conformance.categories) : ''}
        ${hasHistory ? renderConformanceTimeline() : ''}
        ${hasMilestones ? renderMilestones(dashboardData.milestones!) : ''}
        ${renderRunningTestsLocally()}
      </div>
    `

    // Initialize timeline chart after DOM is ready
    if (hasHistory) {
      // Use setTimeout to ensure DOM is updated
      setTimeout(() => {
        const isDarkMode = document.documentElement.classList.contains('dark')
        this.timelineChart = initTimelineChart(conformance.history, isDarkMode)
      }, 0)
    }
  }

  private renderLegacyFormat(): void {
    const { data, sltData } = this.state

    if (!data) return

    const timestamp = new Date().toISOString().replace('T', ' ').split('.')[0] + ' UTC'
    const commit = 'latest'

    this.element.innerHTML = `
      <div class="space-y-8">
        ${renderMetadataCard(commit, timestamp, data.pass_rate || 0)}
        ${renderSqltestResults(data)}
        ${sltData ? renderSQLLogicTestResults(sltData) : ''}
        ${renderExplanation(data, sltData)}
        ${data.error_tests && data.error_tests.length > 0 ? renderFailingTests(data.error_tests) : ''}
        ${renderRunningTestsLocally()}
      </div>
    `
  }

  destroy(): void {
    if (this.timelineChart) {
      this.timelineChart.destroy()
      this.timelineChart = null
    }
    if (this.themeObserver) {
      this.themeObserver.disconnect()
      this.themeObserver = null
    }
  }
}
