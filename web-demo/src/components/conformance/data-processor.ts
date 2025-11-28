import type { ConformanceData, SQLLogicTestData, DashboardData } from './types'

/**
 * Handles loading and processing of conformance test data
 */
export class DataProcessor {
  private cacheBust = Math.floor(Date.now() / 60000) // Update every minute
  private baseUrl = import.meta.env.BASE_URL || '/'

  /**
   * Load new dashboard.json format
   * Returns null if unavailable (fallback to legacy data)
   */
  async loadDashboardData(): Promise<DashboardData | null> {
    try {
      const url = `${this.baseUrl}data/dashboard.json?v=${this.cacheBust}`
      const response = await fetch(url)
      const contentType = response.headers.get('content-type')

      if (response.ok && contentType && contentType.includes('application/json')) {
        const data = (await response.json()) as DashboardData
        // Validate that we have the expected conformance data
        if (data.conformance && data.conformance.summary) {
          return data
        }
      }
    } catch (error) {
      console.warn('Dashboard data not available, using legacy format:', error)
    }
    return null
  }

  /**
   * Load sqltest conformance data from JSON file
   * Falls back to using SQLLogicTest cumulative data if sqltest_results.json is unavailable
   */
  async loadSqltestData(): Promise<ConformanceData> {
    const response = await fetch(`${this.baseUrl}badges/sqltest_results.json?v=${this.cacheBust}`)
    if (!response.ok) {
      console.warn('sqltest_results.json not available, attempting to use cumulative data as fallback')
      // Try to use cumulative results as fallback
      const fallbackResponse = await fetch(
        `${this.baseUrl}badges/sqllogictest_cumulative.json?v=${this.cacheBust}`
      )
      if (fallbackResponse.ok) {
        const cumulativeData = await fallbackResponse.json()
        // Transform cumulative data to ConformanceData format
        return {
          total: cumulativeData.summary.total_tested_files,
          passed: cumulativeData.summary.passed,
          failed: cumulativeData.summary.failed,
          errors: 0, // Not tracked in cumulative
          pass_rate: cumulativeData.summary.pass_rate,
        }
      }
      throw new Error(`Failed to load sqltest data: ${response.statusText}`)
    }
    return (await response.json()) as ConformanceData
  }

  /**
   * Load SQLLogicTest data, preferring cumulative results over single-run results
   */
  async loadSQLLogicTestData(): Promise<SQLLogicTestData | null> {
    try {
      // Try cumulative results first (updated by boost workflow)
      const cumulativeUrl = `${this.baseUrl}badges/sqllogictest_cumulative.json?v=${this.cacheBust}`
      let sltResponse = await fetch(cumulativeUrl)
      const contentType = sltResponse.headers.get('content-type')
      // Check if we got JSON (not HTML from Vite's SPA fallback)
      if (sltResponse.ok && contentType && contentType.includes('application/json')) {
        const cumulativeData = await sltResponse.json()
        // Transform cumulative data structure to match interface
        const result: SQLLogicTestData = {
          total: cumulativeData.summary.total_tested_files,
          passed: cumulativeData.summary.passed,
          failed: cumulativeData.summary.failed,
          errors: 0, // Not tracked in cumulative data
          pass_rate: cumulativeData.summary.pass_rate,
          categories: {}, // Not available in cumulative data
        }
        return result
      } else {
        // Fall back to single-run results (from CI workflow)
        const singleRunUrl = `${this.baseUrl}badges/sqllogictest_results.json?v=${this.cacheBust}`
        sltResponse = await fetch(singleRunUrl)
        const singleRunContentType = sltResponse.headers.get('content-type')
        if (sltResponse.ok && singleRunContentType && singleRunContentType.includes('application/json')) {
          const singleRunData = await sltResponse.json()
          // Transform single-run data structure to match interface
          const result: SQLLogicTestData = {
            total: singleRunData.total_tested || singleRunData.total,
            passed: singleRunData.passed,
            failed: singleRunData.failed,
            errors: singleRunData.errors || 0,
            pass_rate: singleRunData.pass_rate,
            categories: {}, // Not available in single-run data
          }
          return result
        }
      }
    } catch (error) {
      // SQLLogicTest data is optional, continue without it
      console.warn('SQLLogicTest results not available:', error)
    }
    return null
  }
}
