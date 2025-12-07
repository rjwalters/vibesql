import { SQL_DATA_TYPES, EXAMPLE_QUERIES, type DataType } from '../data/datatypes'
import { t, onI18nChange } from '../i18n'

export class DataTypesShowcase {
  private root: HTMLElement

  constructor() {
    this.root = document.createElement('div')
    this.root.className = 'data-types-showcase'
    this.render()

    // Re-render when locale changes
    onI18nChange(() => this.render())
  }

  private getStatusIcon(status: DataType['status']): string {
    return status === 'implemented' ? '✅' : '⏳'
  }

  private getStatusLabel(status: DataType['status']): string {
    return status === 'implemented' ? t('status-implemented') : t('status-planned')
  }

  private render(): void {
    const implementedTypes = SQL_DATA_TYPES.filter(t => t.status === 'implemented')
    const progressPercent = Math.round(
      (implementedTypes.length / SQL_DATA_TYPES.length) * 100
    )

    this.root.innerHTML = `
      <div class="space-y-6">
        <!-- Header with Progress -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
          <h2 class="text-2xl font-bold text-gray-900 dark:text-gray-100 mb-2">
            ${t('datatypes-title')}
          </h2>
          <p class="text-sm text-gray-600 dark:text-gray-400 mb-4">
            ${t('datatypes-description')}
          </p>

          <div class="flex items-center gap-3 mb-4">
            <div class="flex-1">
              <div class="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                <div
                  class="h-full bg-blue-500 transition-all duration-300"
                  style="width: ${progressPercent}%"
                  role="progressbar"
                  aria-valuenow="${progressPercent}"
                  aria-valuemin="0"
                  aria-valuemax="100"
                ></div>
              </div>
            </div>
            <span class="text-sm font-semibold text-gray-700 dark:text-gray-300">
              ${t('showcase-progress', { implemented: implementedTypes.length, total: SQL_DATA_TYPES.length, type: t('datatypes-progress-type'), percent: progressPercent })}
            </span>
          </div>
        </div>

        <!-- Data Types Table -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
          <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
            ${t('datatypes-reference')}
          </h3>

          <div class="overflow-x-auto">
            <table class="w-full text-left">
              <thead class="bg-gray-50 dark:bg-gray-700/50">
                <tr>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('showcase-table-status')}
                  </th>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('datatypes-table-type')}
                  </th>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('showcase-table-description')}
                  </th>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('datatypes-table-example')}
                  </th>
                  <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                    ${t('datatypes-table-spec')}
                  </th>
                </tr>
              </thead>
              <tbody class="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                ${SQL_DATA_TYPES.map(type => this.renderTypeRow(type)).join('')}
              </tbody>
            </table>
          </div>
        </div>

        <!-- Example Queries -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
          <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
            ${t('showcase-interactive-examples')}
          </h3>

          <div class="space-y-6">
            ${this.renderExampleSection(
              t('datatypes-ex-numeric'),
              'numericTypes',
              EXAMPLE_QUERIES.numericTypes
            )}
            ${this.renderExampleSection(
              t('datatypes-ex-null'),
              'nullHandling',
              EXAMPLE_QUERIES.nullHandling
            )}
            ${this.renderExampleSection(
              t('datatypes-ex-comparisons'),
              'typeComparisons',
              EXAMPLE_QUERIES.typeComparisons
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

  private renderTypeRow(type: DataType): string {
    const statusClasses =
      type.status === 'implemented'
        ? 'text-green-600 dark:text-green-400'
        : 'text-gray-500 dark:text-gray-400'

    return `
      <tr class="hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors">
        <td class="px-4 py-4 whitespace-nowrap">
          <span class="text-lg ${statusClasses}" role="img" aria-label="${this.getStatusLabel(type.status)}">
            ${this.getStatusIcon(type.status)}
          </span>
        </td>
        <td class="px-4 py-4">
          <span class="font-mono font-semibold text-gray-900 dark:text-gray-100">
            ${type.name}
          </span>
        </td>
        <td class="px-4 py-4 text-sm text-gray-700 dark:text-gray-300">
          ${type.description}
        </td>
        <td class="px-4 py-4">
          <code class="text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded text-gray-800 dark:text-gray-200">
            ${type.exampleValue}
          </code>
        </td>
        <td class="px-4 py-4 text-xs text-gray-500 dark:text-gray-400">
          ${type.sqlSpec}
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
      console.error('DataTypesShowcase: Mount target not found')
      return
    }

    target.appendChild(this.root)
  }

  public unmount(): void {
    this.root.remove()
  }
}
