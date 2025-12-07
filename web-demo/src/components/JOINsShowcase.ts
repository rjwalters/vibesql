import { JOIN_TYPES, JOIN_EXAMPLES, type JoinType } from '../data/joins'
import { t, onI18nChange } from '../i18n'

export class JOINsShowcase {
  private root: HTMLElement

  constructor() {
    this.root = document.createElement('div')
    this.root.className = 'joins-showcase'
    this.render()

    // Re-render when locale changes
    onI18nChange(() => this.render())
  }

  private getStatusIcon(status: JoinType['status']): string {
    return status === 'implemented' ? '✅' : '⏳'
  }

  private getStatusLabel(status: JoinType['status']): string {
    return status === 'implemented' ? t('status-implemented') : t('status-planned')
  }

  private getCategoryColor(category: JoinType['category']): string {
    switch (category) {
      case 'Inner':
        return 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300'
      case 'Outer':
        return 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300'
      case 'Cross':
        return 'bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300'
      case 'Multi':
        return 'bg-indigo-100 text-indigo-800 dark:bg-indigo-900/30 dark:text-indigo-300'
    }
  }

  private render(): void {
    const implementedJoins = JOIN_TYPES.filter(j => j.status === 'implemented')
    const progressPercent = Math.round((implementedJoins.length / JOIN_TYPES.length) * 100)

    // Group joins by category
    const byCategory = JOIN_TYPES.reduce(
      (acc, join) => {
        if (!acc[join.category]) acc[join.category] = []
        acc[join.category].push(join)
        return acc
      },
      {} as Record<string, JoinType[]>
    )

    this.root.innerHTML = `
      <div class="space-y-6">
        <!-- Header with Progress -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
          <h2 class="text-2xl font-bold text-gray-900 dark:text-gray-100 mb-2">
            ${t('joins-title')}
          </h2>
          <p class="text-sm text-gray-600 dark:text-gray-400 mb-4">
            ${t('joins-description')}
          </p>

          <div class="flex items-center gap-3 mb-4">
            <div class="flex-1">
              <div class="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                <div
                  class="h-full bg-green-500 transition-all duration-300"
                  style="width: ${progressPercent}%"
                  role="progressbar"
                  aria-valuenow="${progressPercent}"
                  aria-valuemin="0"
                  aria-valuemax="100"
                ></div>
              </div>
            </div>
            <span class="text-sm font-semibold text-gray-700 dark:text-gray-300">
              ${t('showcase-progress', { implemented: implementedJoins.length, total: JOIN_TYPES.length, type: t('joins-progress-type'), percent: progressPercent })}
            </span>
          </div>

          <!-- Category Summary -->
          <div class="grid grid-cols-2 md:grid-cols-4 gap-3">
            ${Object.entries(byCategory)
              .map(([category, joins]) => {
                const implemented = joins.filter(j => j.status === 'implemented').length
                return `
                  <div class="text-center p-3 rounded-lg ${this.getCategoryColor(category as JoinType['category'])}">
                    <div class="font-bold text-sm">${category} ${t('joins-category-suffix')}</div>
                    <div class="text-xs">${implemented}/${joins.length}</div>
                  </div>
                `
              })
              .join('')}
          </div>
        </div>

        <!-- JOIN Types Reference Table -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
          <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
            ${t('joins-reference')}
          </h3>

          <div class="overflow-x-auto">
            <table class="w-full text-left">
              <thead class="bg-gray-50 dark:bg-gray-700/50">
                <tr>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('showcase-table-status')}
                  </th>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('showcase-table-category')}
                  </th>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('joins-table-type')}
                  </th>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('showcase-table-description')}
                  </th>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('showcase-table-use-case')}
                  </th>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('showcase-table-syntax')}
                  </th>
                </tr>
              </thead>
              <tbody class="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                ${JOIN_TYPES.map(join => this.renderJoinRow(join)).join('')}
              </tbody>
            </table>
          </div>
        </div>

        <!-- Interactive Examples -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
          <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
            ${t('showcase-interactive-examples')}
          </h3>

          <div class="space-y-6">
            ${this.renderExampleSection(
              t('joins-ex-sample'),
              'sampleData',
              JOIN_EXAMPLES.sampleData
            )}
            ${this.renderExampleSection(t('joins-ex-inner'), 'innerJoin', JOIN_EXAMPLES.innerJoin)}
            ${this.renderExampleSection(t('joins-ex-left'), 'leftJoin', JOIN_EXAMPLES.leftJoin)}
            ${this.renderExampleSection(t('joins-ex-right'), 'rightJoin', JOIN_EXAMPLES.rightJoin)}
            ${this.renderExampleSection(t('joins-ex-full'), 'fullJoin', JOIN_EXAMPLES.fullJoin)}
            ${this.renderExampleSection(t('joins-ex-cross'), 'crossJoin', JOIN_EXAMPLES.crossJoin)}
            ${this.renderExampleSection(t('joins-ex-multi'), 'multiTable', JOIN_EXAMPLES.multiTable)}
          </div>
        </div>
      </div>
    `

    // Add click handlers for "Try This" buttons
    this.root.querySelectorAll('[data-example-query]').forEach(button => {
      button.addEventListener('click', e => {
        const target = e.currentTarget as HTMLElement
        const query = target.dataset.exampleQuery
        if (query) {
          this.loadExample(query)
        }
      })
    })
  }

  private renderJoinRow(join: JoinType): string {
    const statusClasses =
      join.status === 'implemented'
        ? 'text-green-600 dark:text-green-400'
        : 'text-gray-500 dark:text-gray-400'

    return `
      <tr class="hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors">
        <td class="px-4 py-4 whitespace-nowrap">
          <span class="text-lg ${statusClasses}" role="img" aria-label="${this.getStatusLabel(join.status)}">
            ${this.getStatusIcon(join.status)}
          </span>
        </td>
        <td class="px-4 py-4 whitespace-nowrap">
          <span class="px-2 py-1 text-xs font-semibold rounded ${this.getCategoryColor(join.category)}">
            ${join.category}
          </span>
        </td>
        <td class="px-4 py-4">
          <span class="font-medium text-gray-900 dark:text-gray-100">
            ${join.name}
          </span>
        </td>
        <td class="px-4 py-4 text-sm text-gray-700 dark:text-gray-300">
          ${join.description}
        </td>
        <td class="px-4 py-4 text-sm text-gray-600 dark:text-gray-400">
          ${join.useCase}
        </td>
        <td class="px-4 py-4">
          <code class="text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded text-gray-800 dark:text-gray-200 block whitespace-nowrap">
            ${this.escapeHtml(join.syntax)}
          </code>
        </td>
      </tr>
    `
  }

  private renderExampleSection(title: string, _id: string, query: string): string {
    return `
      <div class="border border-gray-200 dark:border-gray-700 rounded-lg p-4">
        <div class="flex items-center justify-between mb-3">
          <h4 class="text-lg font-medium text-gray-900 dark:text-gray-100">
            ${title}
          </h4>
          <button
            data-example-query="${this.escapeHtml(query)}"
            class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors"
          >
            ${t('showcase-try-example')}
          </button>
        </div>
        <pre class="bg-gray-50 dark:bg-gray-900 p-4 rounded-lg overflow-x-auto"><code class="text-sm text-gray-800 dark:text-gray-200">${this.escapeHtml(query)}</code></pre>
      </div>
    `
  }

  private escapeHtml(text: string): string {
    const map: Record<string, string> = {
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#039;',
    }
    return text.replace(/[&<>"']/g, m => map[m])
  }

  private loadExample(query: string): void {
    // Dispatch event to load example into main editor
    const event = new CustomEvent('load-example', {
      detail: { query },
      bubbles: true,
    })
    this.root.dispatchEvent(event)
  }

  public mount(container: HTMLElement | string): void {
    const target =
      typeof container === 'string' ? document.querySelector(container) : container

    if (!target) {
      console.error('JOINsShowcase: Mount target not found')
      return
    }

    target.appendChild(this.root)
  }

  public unmount(): void {
    this.root.remove()
  }
}
