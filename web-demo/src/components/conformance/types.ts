export interface ErrorTest {
  id: string
  sql: string
  error: string
}

export interface ConformanceData {
  total: number
  passed: number
  failed: number
  errors: number
  pass_rate: number
  error_tests?: ErrorTest[]
}

export interface SQLLogicTestCategory {
  total: number
  passed: number
  failed: number
  errors: number
  pass_rate: number
}

export interface SQLLogicTestData {
  total: number
  passed: number
  failed: number
  errors: number
  pass_rate: number
  categories: {
    select?: SQLLogicTestCategory
    evidence?: SQLLogicTestCategory
    index?: SQLLogicTestCategory
    random?: SQLLogicTestCategory
    ddl?: SQLLogicTestCategory
    other?: SQLLogicTestCategory
  }
}

// New dashboard.json types for redesigned conformance page

export interface DashboardConformanceSummary {
  pass_rate: number
  tests_passing: number
  tests_total: number
  files_passing: number
  files_total: number
}

export interface DashboardConformanceCategory {
  total: number
  passing: number
  pass_rate: number
}

export interface DashboardConformanceHistoryEntry {
  date: string
  pass_rate: number
  passing: number
}

export interface DashboardConformance {
  summary: DashboardConformanceSummary
  categories: Record<string, DashboardConformanceCategory>
  history: DashboardConformanceHistoryEntry[]
}

export interface DashboardMilestone {
  date: string
  description: string
  pr?: number
  commit?: string
}

export interface DashboardData {
  generated_at: string
  version: string
  conformance?: DashboardConformance
  milestones?: DashboardMilestone[]
}

export interface ConformanceReportState {
  data: ConformanceData | null
  sltData: SQLLogicTestData | null
  dashboardData: DashboardData | null
  loading: boolean
  error: string | null
}
