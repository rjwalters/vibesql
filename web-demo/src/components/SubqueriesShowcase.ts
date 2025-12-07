import { SUBQUERY_TYPES, SUBQUERY_EXAMPLES, type SubqueryType } from '../data/subqueries'
import { t, onI18nChange } from '../i18n'

export class SubqueriesShowcase {
  private root: HTMLElement

  constructor() {
    this.root = document.createElement('div')
    this.root.className = 'subqueries-showcase'
    this.render()

    // Re-render when locale changes
    onI18nChange(() => this.render())
  }

  private getStatusIcon(status: SubqueryType['status']): string {
    return status === 'implemented' ? '✅' : '⏳'
  }

  private getStatusLabel(status: SubqueryType['status']): string {
    return status === 'implemented' ? t('status-implemented') : t('status-planned')
  }

  private getCategoryColor(category: SubqueryType['category']): string {
    switch (category) {
      case 'Scalar':
        return 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300'
      case 'Table':
        return 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300'
      case 'Predicate':
        return 'bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300'
      case 'Correlated':
        return 'bg-indigo-100 text-indigo-800 dark:bg-indigo-900/30 dark:text-indigo-300'
      case 'Planned':
        return 'bg-gray-100 text-gray-800 dark:bg-gray-900/30 dark:text-gray-300'
    }
  }

  private render(): void {
    const implementedSubqueries = SUBQUERY_TYPES.filter(s => s.status === 'implemented')
    const progressPercent = Math.round(
      (implementedSubqueries.length / SUBQUERY_TYPES.length) * 100
    )

    // Group subqueries by category
    const byCategory = SUBQUERY_TYPES.reduce(
      (acc, subquery) => {
        if (!acc[subquery.category]) acc[subquery.category] = []
        acc[subquery.category].push(subquery)
        return acc
      },
      {} as Record<string, SubqueryType[]>
    )

    this.root.innerHTML = `
      <div class="space-y-6">
        <!-- Header with Progress -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
          <h2 class="text-2xl font-bold text-gray-900 dark:text-gray-100 mb-2">
            ${t('subqueries-title')}
          </h2>
          <p class="text-sm text-gray-600 dark:text-gray-400 mb-4">
            ${t('subqueries-description')}
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
              ${t('showcase-progress', { implemented: implementedSubqueries.length, total: SUBQUERY_TYPES.length, type: t('subqueries-progress-type'), percent: progressPercent })}
            </span>
          </div>

          <!-- Category Summary -->
          <div class="grid grid-cols-2 md:grid-cols-5 gap-3">
            ${Object.entries(byCategory)
              .map(([category, subqueries]) => {
                const implemented = subqueries.filter(s => s.status === 'implemented').length
                return `
                  <div class="text-center p-3 rounded-lg ${this.getCategoryColor(category as SubqueryType['category'])}">
                    <div class="font-bold text-sm">${category}</div>
                    <div class="text-xs">${implemented}/${subqueries.length}</div>
                  </div>
                `
              })
              .join('')}
          </div>
        </div>

        <!-- Subquery Types Reference Table -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
          <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
            ${t('subqueries-reference')}
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
                    ${t('subqueries-table-type')}
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
                ${SUBQUERY_TYPES.map(subquery => this.renderSubqueryRow(subquery)).join('')}
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
              t('subqueries-ex-scalar-select'),
              'scalarInSelect',
              SUBQUERY_EXAMPLES.scalarInSelect
            )}
            ${this.renderExampleSection(
              t('subqueries-ex-scalar-where'),
              'scalarInWhere',
              SUBQUERY_EXAMPLES.scalarInWhere
            )}
            ${this.renderExampleSection(
              t('subqueries-ex-derived'),
              'derivedTable',
              SUBQUERY_EXAMPLES.derivedTable
            )}
            ${this.renderExampleSection(
              t('subqueries-ex-in'),
              'inPredicate',
              SUBQUERY_EXAMPLES.inPredicate
            )}
            ${this.renderExampleSection(
              t('subqueries-ex-correlated'),
              'correlated',
              SUBQUERY_EXAMPLES.correlated
            )}
            ${this.renderExampleSection(
              t('subqueries-ex-nested'),
              'nested',
              SUBQUERY_EXAMPLES.nested
            )}
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

  private renderSubqueryRow(subquery: SubqueryType): string {
    const statusClasses =
      subquery.status === 'implemented'
        ? 'text-green-600 dark:text-green-400'
        : 'text-gray-500 dark:text-gray-400'

    return `
      <tr class="hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors">
        <td class="px-4 py-4 whitespace-nowrap">
          <span class="text-lg ${statusClasses}" role="img" aria-label="${this.getStatusLabel(subquery.status)}">
            ${this.getStatusIcon(subquery.status)}
          </span>
        </td>
        <td class="px-4 py-4 whitespace-nowrap">
          <span class="px-2 py-1 text-xs font-semibold rounded ${this.getCategoryColor(subquery.category)}">
            ${subquery.category}
          </span>
        </td>
        <td class="px-4 py-4">
          <span class="font-medium text-gray-900 dark:text-gray-100">
            ${subquery.name}
          </span>
        </td>
        <td class="px-4 py-4 text-sm text-gray-700 dark:text-gray-300">
          ${subquery.description}
        </td>
        <td class="px-4 py-4 text-sm text-gray-600 dark:text-gray-400">
          ${subquery.useCase}
        </td>
        <td class="px-4 py-4">
          <code class="text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded text-gray-800 dark:text-gray-200 block whitespace-nowrap">
            ${this.escapeHtml(subquery.syntax)}
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
      console.error('SubqueriesShowcase: Mount target not found')
      return
    }

    target.appendChild(this.root)
  }

  public unmount(): void {
    this.root.remove()
  }
}
