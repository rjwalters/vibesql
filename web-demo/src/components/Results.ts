import { Component } from './base'
import type { QueryResult, ExecuteResult } from '../db/types'
import { t } from '../i18n'

const MAX_DISPLAY_ROWS = 1000

interface ResultsState {
  result: QueryResult | ExecuteResult | null
  error: string | null
  loading: boolean
  executionTime: number | null
}

function isQueryResult(result: QueryResult | ExecuteResult): result is QueryResult {
  return 'columns' in result && 'rows' in result
}

function isExecuteResult(result: QueryResult | ExecuteResult): result is ExecuteResult {
  return 'rows_affected' in result && 'message' in result
}

/**
 * Results component displays query results in a table format
 */
export class ResultsComponent extends Component<ResultsState> {
  constructor() {
    super('#results', {
      result: null,
      error: null,
      loading: false,
      executionTime: null,
    })
  }

  /**
   * Show query results (SELECT) or execution results (DDL/DML)
   */
  showResults(result: QueryResult | ExecuteResult, executionTime?: number): void {
    this.setState({
      result,
      error: null,
      loading: false,
      executionTime: executionTime ?? null,
    })
  }

  /**
   * Show error message
   */
  showError(error: string): void {
    this.setState({ error, result: null, loading: false, executionTime: null })
  }

  /**
   * Set loading state
   */
  setLoading(loading: boolean): void {
    this.setState({ loading, executionTime: null })
  }

  protected render(): void {
    if (this.state.loading) {
      this.element.innerHTML = this.renderLoading()
      return
    }

    if (this.state.error) {
      this.element.innerHTML = this.renderError(this.state.error)
      return
    }

    if (this.state.result) {
      if (isQueryResult(this.state.result)) {
        this.element.innerHTML = this.renderTable(this.state.result)
        this.attachResultHandlers()
      } else if (isExecuteResult(this.state.result)) {
        this.element.innerHTML = this.renderExecuteResult(this.state.result)
      }
    } else {
      this.element.innerHTML = this.renderEmpty()
    }
  }

  private attachResultHandlers(): void {
    const copyButton = this.element.querySelector<HTMLButtonElement>('[data-action="copy-results"]')
    const exportButton = this.element.querySelector<HTMLButtonElement>('[data-action="export-csv"]')

    copyButton?.addEventListener('click', () => this.copyToClipboard())
    exportButton?.addEventListener('click', () => this.exportToCSV())
  }

  private copyToClipboard(): void {
    if (!this.state.result || !isQueryResult(this.state.result)) return

    const { columns, rows } = this.state.result
    const parsedRows = rows.map(rowStr => {
      try {
        return JSON.parse(rowStr) as unknown[]
      } catch {
        return [rowStr]
      }
    })

    const tsv = [
      columns.join('\t'),
      ...parsedRows.map(row => row.map(cell => String(cell ?? '')).join('\t'))
    ].join('\n')

    navigator.clipboard.writeText(tsv).catch(err => {
      console.error('Failed to copy to clipboard:', err)
    })
  }

  private exportToCSV(): void {
    if (!this.state.result || !isQueryResult(this.state.result)) return

    const { columns, rows } = this.state.result
    const parsedRows = rows.map(rowStr => {
      try {
        return JSON.parse(rowStr) as unknown[]
      } catch {
        return [rowStr]
      }
    })

    const escapeCsv = (value: unknown): string => {
      const str = String(value ?? '')
      if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return `"${str.replace(/"/g, '""')}"`
      }
      return str
    }

    const csv = [
      columns.map(escapeCsv).join(','),
      ...parsedRows.map(row => row.map(escapeCsv).join(','))
    ].join('\n')

    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const link = document.createElement('a')
    const url = URL.createObjectURL(blob)

    link.setAttribute('href', url)
    link.setAttribute('download', `query-results-${Date.now()}.csv`)
    link.style.visibility = 'hidden'
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  }

  private renderLoading(): string {
    return `
      <div class="flex items-center justify-center p-8">
        <div class="animate-spin h-8 w-8 border-4 border-primary-light border-t-transparent rounded-full"></div>
      </div>
    `
  }

  private renderError(error: string): string {
    return `
      <div class="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
        <p class="text-red-700 dark:text-red-300 font-mono text-sm">${this.escapeHtml(error)}</p>
      </div>
    `
  }

  private renderEmpty(): string {
    return `
      <div class="text-gray-500 dark:text-gray-400 text-center p-8">
        <p>${t('results-empty')}</p>
      </div>
    `
  }

  private renderExecuteResult(result: ExecuteResult): string {
    const executionTimeText = this.state.executionTime !== null
      ? ` (${this.state.executionTime.toFixed(2)}ms)`
      : ''

    return `
      <div class="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg p-4">
        <p class="text-green-700 dark:text-green-300">
          ${this.escapeHtml(result.message)}${executionTimeText}
        </p>
        ${result.rows_affected > 0 ? `
          <p class="text-sm text-green-600 dark:text-green-400 mt-1">
            ${t('msg-rows-affected', { count: result.rows_affected })}
          </p>
        ` : ''}
      </div>
    `
  }

  private renderTable(result: QueryResult): string {
    const { columns, rows, row_count } = result

    if (rows.length === 0) {
      return `
        <div class="text-gray-600 dark:text-gray-400 p-4">
          <p>${t('results-success-zero')}</p>
        </div>
      `
    }

    // Parse JSON strings from WASM into arrays
    const parsedRows = rows.map(rowStr => {
      try {
        return JSON.parse(rowStr) as unknown[]
      } catch {
        return [rowStr] // Fallback if parsing fails
      }
    })

    const displayRows = parsedRows.slice(0, MAX_DISPLAY_ROWS)
    const isLimited = row_count > MAX_DISPLAY_ROWS
    const executionTimeText = this.state.executionTime !== null
      ? ` • ${this.state.executionTime.toFixed(2)}ms`
      : ''

    return `
      <div class="overflow-x-auto">
        <div class="flex items-center justify-between px-4 py-2 border-b border-gray-200 dark:border-gray-700">
          <div class="text-sm text-gray-600 dark:text-gray-400">
            ${t('results-rows', { count: row_count })}${executionTimeText}
          </div>
          <div class="flex gap-2">
            <button class="button secondary small" data-action="copy-results" type="button">
              ${t('results-copy')}
            </button>
            <button class="button secondary small" data-action="export-csv" type="button">
              ${t('results-export')}
            </button>
          </div>
        </div>
        ${isLimited ? `
          <div class="bg-yellow-50 dark:bg-yellow-900/20 border-l-4 border-yellow-400 dark:border-yellow-600 p-3 mx-4 mt-2">
            <p class="text-sm text-yellow-700 dark:text-yellow-300">
              ${t('results-limit-warning', { limit: MAX_DISPLAY_ROWS, total: row_count })}
            </p>
          </div>
        ` : ''}
        <table class="results-table">
          <thead>
            <tr>
              ${columns.map(col => `<th>${this.escapeHtml(col)}</th>`).join('')}
            </tr>
          </thead>
          <tbody>
            ${displayRows.map(row => `
              <tr>
                ${row.map(cell => `<td>${this.formatCell(cell)}</td>`).join('')}
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    `
  }

  private formatCell(value: unknown): string {
    if (value === null) {
      return `<span class="text-gray-400 italic">${t('results-null')}</span>`
    }
    return this.escapeHtml(String(value))
  }
}
