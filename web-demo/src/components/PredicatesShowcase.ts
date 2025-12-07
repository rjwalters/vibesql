import { PREDICATES, PREDICATE_EXAMPLES, type Predicate } from '../data/predicates'
import { t, onI18nChange } from '../i18n'

export class PredicatesShowcase {
  private root: HTMLElement

  constructor() {
    this.root = document.createElement('div')
    this.root.className = 'predicates-showcase'
    this.render()

    // Re-render when locale changes
    onI18nChange(() => this.render())
  }

  private getStatusIcon(status: Predicate['status']): string {
    return status === 'implemented' ? '✅' : '⏳'
  }

  private getStatusLabel(status: Predicate['status']): string {
    return status === 'implemented' ? t('status-implemented') : t('status-planned')
  }

  private getCategoryColor(category: Predicate['category']): string {
    switch (category) {
      case 'Comparison':
        return 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300'
      case 'NULL':
        return 'bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300'
      case 'Range':
        return 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300'
      case 'Set':
        return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300'
      case 'Boolean':
        return 'bg-indigo-100 text-indigo-800 dark:bg-indigo-900/30 dark:text-indigo-300'
      case 'Planned':
        return 'bg-gray-100 text-gray-800 dark:bg-gray-900/30 dark:text-gray-300'
    }
  }

  private render(): void {
    const implementedPreds = PREDICATES.filter(p => p.status === 'implemented')
    const progressPercent = Math.round((implementedPreds.length / PREDICATES.length) * 100)

    // Group predicates by category
    const byCategory = PREDICATES.reduce(
      (acc, pred) => {
        if (!acc[pred.category]) acc[pred.category] = []
        acc[pred.category].push(pred)
        return acc
      },
      {} as Record<string, Predicate[]>
    )

    this.root.innerHTML = `
      <div class="space-y-6">
        <!-- Header with Progress -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
          <h2 class="text-2xl font-bold text-gray-900 dark:text-gray-100 mb-2">
            ${t('predicates-title')}
          </h2>
          <p class="text-sm text-gray-600 dark:text-gray-400 mb-4">
            ${t('predicates-description')}
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
              ${t('showcase-progress', { implemented: implementedPreds.length, total: PREDICATES.length, type: t('predicates-progress-type'), percent: progressPercent })}
            </span>
          </div>

          <!-- Category Summary -->
          <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
            ${Object.entries(byCategory)
              .map(([category, preds]) => {
                const implemented = preds.filter(p => p.status === 'implemented').length
                return `
                  <div class="text-center p-3 rounded-lg ${this.getCategoryColor(category as Predicate['category'])}">
                    <div class="font-bold text-sm">${category}</div>
                    <div class="text-xs">${implemented}/${preds.length}</div>
                  </div>
                `
              })
              .join('')}
          </div>
        </div>

        <!-- Predicates Reference Table -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
          <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
            ${t('predicates-reference')}
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
                    ${t('predicates-table-predicate')}
                  </th>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('showcase-table-description')}
                  </th>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('showcase-table-syntax')}
                  </th>
                </tr>
              </thead>
              <tbody class="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                ${PREDICATES.map(pred => this.renderPredicateRow(pred)).join('')}
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
              t('predicates-ex-comparison'),
              'comparison',
              PREDICATE_EXAMPLES.comparison
            )}
            ${this.renderExampleSection(
              t('predicates-ex-between'),
              'between',
              PREDICATE_EXAMPLES.between
            )}
            ${this.renderExampleSection(
              t('predicates-ex-null'),
              'nullPredicates',
              PREDICATE_EXAMPLES.nullPredicates
            )}
            ${this.renderExampleSection(
              t('predicates-ex-boolean'),
              'booleanLogic',
              PREDICATE_EXAMPLES.booleanLogic
            )}
            ${this.renderExampleSection(
              t('predicates-ex-in'),
              'inPredicate',
              PREDICATE_EXAMPLES.inPredicate
            )}
            ${this.renderExampleSection(
              t('predicates-ex-combined'),
              'combined',
              PREDICATE_EXAMPLES.combined
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

  private renderPredicateRow(pred: Predicate): string {
    const statusClasses =
      pred.status === 'implemented'
        ? 'text-green-600 dark:text-green-400'
        : 'text-gray-500 dark:text-gray-400'

    return `
      <tr class="hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors">
        <td class="px-4 py-4 whitespace-nowrap">
          <span class="text-lg ${statusClasses}" role="img" aria-label="${this.getStatusLabel(pred.status)}">
            ${this.getStatusIcon(pred.status)}
          </span>
        </td>
        <td class="px-4 py-4 whitespace-nowrap">
          <span class="px-2 py-1 text-xs font-semibold rounded ${this.getCategoryColor(pred.category)}">
            ${pred.category}
          </span>
        </td>
        <td class="px-4 py-4">
          <span class="font-medium text-gray-900 dark:text-gray-100">
            ${pred.name}
          </span>
        </td>
        <td class="px-4 py-4 text-sm text-gray-700 dark:text-gray-300">
          ${pred.description}
        </td>
        <td class="px-4 py-4">
          <code class="text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded text-gray-800 dark:text-gray-200 block">
            ${this.escapeHtml(pred.syntax)}
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
            data-example-query="${query}"
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
      console.error('PredicatesShowcase: Mount target not found')
      return
    }

    target.appendChild(this.root)
  }

  public unmount(): void {
    this.root.remove()
  }
}
