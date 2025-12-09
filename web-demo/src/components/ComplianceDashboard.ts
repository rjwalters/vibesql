import { t } from '../i18n'
import { OverallGaugeComponent } from './OverallGaugeComponent'
import { CategoryBreakdownComponent } from './CategoryBreakdownComponent'
import { FeatureMatrixComponent } from './FeatureMatrixComponent'
import { TestStatisticsComponent } from './TestStatisticsComponent'
import { RoadmapTimelineComponent } from './RoadmapTimelineComponent'
import { RecentUpdatesComponent } from './RecentUpdatesComponent'
import { DatabaseComparisonComponent } from './DatabaseComparisonComponent'
import { ComplianceDashboardState } from './ComplianceDashboardState'

interface ComplianceCategory {
  name: string
  percentage: number
  implemented: number
  total: number
  status: string
  description: string
}

export interface FeatureNode {
  id: string
  name: string
  status: 'implemented' | 'planned' | 'not-started'
  children?: FeatureNode[]
  docLink?: string
}

interface TestStatistics {
  total: number
  passing: number
  coverage: number
  linesOfCode: number
  byModule: Record<string, number>
}

interface RoadmapPhase {
  name: string
  percentage: number
  status: string
  description: string
}

interface RecentUpdate {
  date: string
  feature: string
  tests: number
  pr: number
}

interface DatabaseComparison {
  database: string
  percentage: number
  target?: number
  notes: string
}

interface ComplianceData {
  overall: {
    percentage: number
    implemented: number
    total: number
    lastUpdated: string
  }
  categories: ComplianceCategory[]
  features?: FeatureNode[]
  testStatistics?: TestStatistics
  roadmap?: {
    phases: RoadmapPhase[]
    estimatedCompletion: string
    currentFocus: string
  }
  recentUpdates?: RecentUpdate[]
  comparison?: DatabaseComparison[]
}

export class ComplianceDashboard {
  private root: HTMLElement
  private data: ComplianceData | null = null
  private state: ComplianceDashboardState
  private overallGaugeComponent: OverallGaugeComponent
  private categoryBreakdownComponent: CategoryBreakdownComponent
  private featureMatrixComponent: FeatureMatrixComponent
  private testStatisticsComponent: TestStatisticsComponent
  private roadmapComponent: RoadmapTimelineComponent
  private updatesComponent: RecentUpdatesComponent
  private comparisonComponent: DatabaseComparisonComponent

  constructor() {
    this.root = document.createElement('div')
    this.root.className = 'compliance-dashboard'

    // Initialize state management
    this.state = new ComplianceDashboardState(() => this.render())

    // Initialize components
    this.overallGaugeComponent = new OverallGaugeComponent()
    this.categoryBreakdownComponent = new CategoryBreakdownComponent()
    this.featureMatrixComponent = new FeatureMatrixComponent(this.state)
    this.testStatisticsComponent = new TestStatisticsComponent()
    this.roadmapComponent = new RoadmapTimelineComponent()
    this.updatesComponent = new RecentUpdatesComponent()
    this.comparisonComponent = new DatabaseComparisonComponent()

    this.loadData()
  }

  private async loadData(): Promise<void> {
    try {
      const response = await fetch('/data/compliance.json')
      this.data = await response.json()
      this.render()
    } catch (error) {
      console.error('Failed to load compliance data:', error)
      this.renderError()
    }
  }

  private renderError(): void {
    this.root.innerHTML = `
      <div class="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-6">
        <h3 class="text-lg font-semibold text-red-800 dark:text-red-400 mb-2">
          Failed to Load Compliance Data
        </h3>
        <p class="text-sm text-red-700 dark:text-red-500">
          Could not load compliance metrics. Please check that compliance.json is available.
        </p>
      </div>
    `
  }

















  private render(): void {
    if (!this.data) {
      this.root.innerHTML = `
        <div class="flex items-center justify-center p-12">
          <div class="text-center">
            <div class="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto mb-4"></div>
            <p class="text-gray-600 dark:text-gray-400">${t('loading-compliance-data')}</p>
          </div>
        </div>
      `
      return
    }

    this.root.innerHTML = `
      <div class="space-y-6">
        ${this.overallGaugeComponent.render(this.data.overall)}
        ${this.categoryBreakdownComponent.render(this.data.categories)}
        ${this.data.roadmap ? this.roadmapComponent.render(this.data.roadmap) : ''}
        ${this.data.recentUpdates ? this.updatesComponent.render(this.data.recentUpdates) : ''}
        ${this.data.comparison ? this.comparisonComponent.render(this.data.comparison) : ''}
        ${this.data.features ? this.featureMatrixComponent.render(this.data.features) : ''}
        ${this.data.testStatistics ? this.testStatisticsComponent.render(this.data.testStatistics) : ''}
      </div>
    `
  }

  public mount(parent: HTMLElement): void {
    parent.appendChild(this.root)
  }

  public unmount(): void {
    this.root.remove()
  }

  public getRoot(): HTMLElement {
    return this.root
  }

  // Public methods for window callbacks
  public handleSearch(event: Event): void {
    this.state.handleSearchInput(event)
  }

  public handleFilter(status: 'implemented' | 'planned' | 'not-started' | null): void {
    this.state.handleStatusFilter(status)
  }

  public toggleNode(nodeId: string): void {
    this.state.toggleNodeState(nodeId)
  }
}
