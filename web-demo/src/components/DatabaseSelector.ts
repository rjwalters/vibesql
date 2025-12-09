import { Component } from './base'
import { t, onI18nChange } from '../i18n'

export interface DatabaseOption {
  id: string
  name: string
  description: string
}

interface DatabaseSelectorState {
  databases: DatabaseOption[]
  selected: string
}

/**
 * Database selector component - switch between different example databases
 */
export class DatabaseSelectorComponent extends Component<DatabaseSelectorState> {
  private onChangeCallback: ((dbId: string) => void) | null = null

  constructor(databases: DatabaseOption[], selectedId: string = databases[0]?.id || '') {
    super('#database-selector', {
      databases,
      selected: selectedId,
    })

    // Re-render on locale change to update translations
    onI18nChange(() => this.render())
  }

  /**
   * Register callback when database selection changes
   */
  onChange(callback: (dbId: string) => void): void {
    this.onChangeCallback = callback
  }

  /**
   * Get currently selected database ID
   */
  getSelected(): string {
    return this.state.selected
  }

  /**
   * Set selected database
   */
  setSelected(dbId: string): void {
    if (this.state.databases.some(db => db.id === dbId)) {
      this.setState({ selected: dbId })
      if (this.onChangeCallback) {
        this.onChangeCallback(dbId)
      }
    }
  }

  protected render(): void {
    const { databases, selected } = this.state

    if (databases.length === 0) {
      this.element.innerHTML = `<p class="text-gray-500 dark:text-gray-400">${this.escapeHtml(t('error-no-databases'))}</p>`
      return
    }

    const selectedDb = databases.find(db => db.id === selected)

    this.element.innerHTML = `
      <div class="inline-block relative">
        <label for="db-select" class="sr-only">Select Database</label>
        <select
          id="db-select"
          class="appearance-none bg-white dark:bg-gray-700 border border-gray-300 dark:border-gray-600 rounded-lg px-3 py-1.5 pr-8 text-sm text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400 cursor-pointer"
          aria-label="Select database"
          title="${selectedDb ? this.escapeHtml(selectedDb.description) : 'Select database'}"
        >
          ${databases.map(db => `
            <option value="${this.escapeHtml(db.id)}" ${db.id === selected ? 'selected' : ''}>
              ${this.escapeHtml(db.name)}
            </option>
          `).join('')}
        </select>
        <div class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-500 dark:text-gray-400">
          <svg class="fill-current h-3 w-3" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20">
            <path d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z"/>
          </svg>
        </div>
      </div>
    `

    // Setup change handler
    const select = this.element.querySelector('#db-select') as HTMLSelectElement
    if (select) {
      select.addEventListener('change', (e) => {
        const newValue = (e.target as HTMLSelectElement).value
        this.setSelected(newValue)
      })
    }
  }
}
