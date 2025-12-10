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

// TCL Test Suite types

export interface TCLTestCategory {
  total: number
  passed: number
  failed: number
  skipped: number
  pass_rate: number
}

export interface TCLTestFile {
  file: string
  path: string
  total: number
  passed: number
  failed: number
  skipped: number
}

export interface TCLFailurePattern {
  error: string
  count: number
}

export interface TCLHistoryEntry {
  run_id: number
  date: string
  commit: string
  total: number
  passed: number
  failed: number
  skipped: number
  pass_rate: number
}

export interface TCLTestData {
  version: string
  generated_at: string
  latest_run: {
    run_id: number
    started_at: string
    completed_at: string
    git_commit: string
    total_files: number
    total_tests: number
    passed: number
    failed: number
    skipped: number
    parse_errors: number
  }
  summary: {
    total_tests: number
    passed: number
    failed: number
    skipped: number
    pass_rate: number
  }
  categories: Record<string, TCLTestCategory>
  by_file: TCLTestFile[]
  failure_patterns: TCLFailurePattern[]
  history: TCLHistoryEntry[]
}

export interface ConformanceReportState {
  data: ConformanceData | null
  sltData: SQLLogicTestData | null
  dashboardData: DashboardData | null
  tclData: TCLTestData | null
  loading: boolean
  error: string | null
}
