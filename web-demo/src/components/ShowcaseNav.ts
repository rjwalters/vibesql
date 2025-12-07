import { t, onI18nChange } from '../i18n'

export interface ShowcaseCategory {
  id: string
  labelKey: string
  status: 'implemented' | 'partial' | 'planned'
  progress: number // 0-100
}

const CATEGORIES: ShowcaseCategory[] = [
  { id: 'compliance', labelKey: 'showcase-cat-compliance', status: 'implemented', progress: 100 },
  { id: 'data-types', labelKey: 'showcase-cat-data-types', status: 'partial', progress: 38 },
  { id: 'dml', labelKey: 'showcase-cat-dml', status: 'implemented', progress: 100 },
  { id: 'predicates', labelKey: 'showcase-cat-predicates', status: 'implemented', progress: 85 },
  { id: 'joins', labelKey: 'showcase-cat-joins', status: 'implemented', progress: 100 },
  { id: 'subqueries', labelKey: 'showcase-cat-subqueries', status: 'implemented', progress: 90 },
  { id: 'aggregates', labelKey: 'showcase-cat-aggregates', status: 'partial', progress: 60 },
  { id: 'ddl', labelKey: 'showcase-cat-ddl', status: 'partial', progress: 50 },
]

// Calculate overall progress
const OVERALL_PROGRESS = Math.round(
  CATEGORIES.reduce((sum, cat) => sum + cat.progress, 0) / CATEGORIES.length
)

function getStatusIcon(status: ShowcaseCategory['status']): string {
  switch (status) {
    case 'implemented':
      return '✅'
    case 'partial':
      return '🚧'
    case 'planned':
      return '⏳'
  }
}

function getStatusLabel(status: ShowcaseCategory['status']): string {
  switch (status) {
    case 'implemented':
      return t('showcase-status-implemented')
    case 'partial':
      return t('showcase-status-partial')
    case 'planned':
      return t('showcase-status-planned')
  }
}

export class ShowcaseNav {
  private root: HTMLElement
  private currentCategory: string | null = null

  constructor() {
    this.root = document.createElement('nav')
    this.root.className = 'showcase-nav'
    this.render()

    // Re-render when locale changes
    onI18nChange(() => this.render())
  }

  private render(): void {
    this.root.innerHTML = `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6 mb-6">
        <!-- Overall Progress -->
        <div class="mb-6">
          <h2 class="text-2xl font-bold text-gray-900 dark:text-gray-100 mb-2">
            ${t('showcase-title')}
          </h2>
          <p class="text-sm text-gray-600 dark:text-gray-400 mb-3">
            ${t('showcase-description')}
          </p>

          <div class="flex items-center gap-3">
            <div class="flex-1">
              <div class="h-3 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                <div
                  class="h-full bg-gradient-to-r from-blue-500 to-blue-600 transition-all duration-300"
                  style="width: ${OVERALL_PROGRESS}%"
                  role="progressbar"
                  aria-valuenow="${OVERALL_PROGRESS}"
                  aria-valuemin="0"
                  aria-valuemax="100"
                ></div>
              </div>
            </div>
            <span class="text-sm font-semibold text-gray-700 dark:text-gray-300 whitespace-nowrap">
              ${t('showcase-complete', { percent: OVERALL_PROGRESS })}
            </span>
          </div>
        </div>

        <!-- Category Navigation -->
        <div class="space-y-2">
          <h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2 uppercase tracking-wide">
            ${t('showcase-categories')}
          </h3>
          ${CATEGORIES.map(category => this.renderCategory(category)).join('')}
        </div>

        <!-- Legend -->
        <div class="mt-6 pt-4 border-t border-gray-200 dark:border-gray-700">
          <h4 class="text-xs font-semibold text-gray-600 dark:text-gray-400 mb-2 uppercase">
            ${t('showcase-legend')}
          </h4>
          <div class="flex flex-wrap gap-4 text-xs">
            <div class="flex items-center gap-1">
              <span>✅</span>
              <span class="text-gray-600 dark:text-gray-400">${t('showcase-status-implemented')}</span>
            </div>
            <div class="flex items-center gap-1">
              <span>🚧</span>
              <span class="text-gray-600 dark:text-gray-400">${t('showcase-status-partial')}</span>
            </div>
            <div class="flex items-center gap-1">
              <span>⏳</span>
              <span class="text-gray-600 dark:text-gray-400">${t('showcase-status-planned')}</span>
            </div>
          </div>
        </div>
      </div>
    `

    // Add event listeners
    this.root.querySelectorAll('[data-category]').forEach(button => {
      button.addEventListener('click', e => {
        const target = e.currentTarget as HTMLElement
        const categoryId = target.dataset.category
        if (categoryId) {
          this.selectCategory(categoryId)
        }
      })
    })
  }

  private renderCategory(category: ShowcaseCategory): string {
    const isActive = this.currentCategory === category.id
    const activeClasses = isActive
      ? 'bg-blue-50 dark:bg-blue-900/20 border-blue-500 dark:border-blue-400'
      : 'bg-gray-50 dark:bg-gray-700/50 border-gray-200 dark:border-gray-600 hover:bg-gray-100 dark:hover:bg-gray-700'

    return `
      <button
        data-category="${category.id}"
        class="w-full text-left px-4 py-3 rounded-lg border-2 transition-all ${activeClasses}"
        role="tab"
        aria-selected="${isActive}"
      >
        <div class="flex items-center justify-between mb-1">
          <div class="flex items-center gap-2">
            <span class="text-lg" role="img" aria-label="${getStatusLabel(category.status)}">
              ${getStatusIcon(category.status)}
            </span>
            <span class="font-medium text-gray-900 dark:text-gray-100">
              ${t(category.labelKey)}
            </span>
          </div>
          <span class="text-sm font-semibold ${
            category.progress === 100
              ? 'text-green-600 dark:text-green-400'
              : 'text-blue-600 dark:text-blue-400'
          }">
            ${category.progress}%
          </span>
        </div>
        <div class="h-1.5 bg-gray-200 dark:bg-gray-600 rounded-full overflow-hidden">
          <div
            class="h-full ${
              category.progress === 100 ? 'bg-green-500' : 'bg-blue-500'
            } transition-all duration-300"
            style="width: ${category.progress}%"
          ></div>
        </div>
      </button>
    `
  }

  public selectCategory(categoryId: string): void {
    this.currentCategory = categoryId
    this.render()

    // Dispatch custom event for other components to listen to
    const event = new CustomEvent('category-selected', {
      detail: { categoryId },
      bubbles: true,
    })
    this.root.dispatchEvent(event)
  }

  public mount(container: HTMLElement | string): void {
    const target =
      typeof container === 'string' ? document.querySelector(container) : container

    if (!target) {
      console.error('ShowcaseNav: Mount target not found')
      return
    }

    target.appendChild(this.root)
  }

  public unmount(): void {
    this.root.remove()
  }

  public getCurrentCategory(): string | null {
    return this.currentCategory
  }
}
