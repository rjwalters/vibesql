import { Component } from './base'
import type { Locale, LocaleCode, LocaleInfo } from '../locale'

interface LocaleSelectorState {
  currentLocale: LocaleCode
  supported: LocaleInfo[]
  isOpen: boolean
}

/**
 * Locale selector component - dropdown for language selection
 */
export class LocaleSelectorComponent extends Component<LocaleSelectorState> {
  private localeSystem: Locale
  private unsubscribe: (() => void) | null = null

  constructor(localeSystem: Locale) {
    super('#locale-selector', {
      currentLocale: localeSystem.current,
      supported: localeSystem.supported,
      isOpen: false,
    })
    this.localeSystem = localeSystem

    // Subscribe to locale changes
    this.unsubscribe = localeSystem.onChange(locale => {
      this.setState({ currentLocale: locale })
    })

    // Close dropdown when clicking outside
    document.addEventListener('click', this.handleDocumentClick.bind(this))
  }

  destroy(): void {
    if (this.unsubscribe) {
      this.unsubscribe()
    }
    document.removeEventListener('click', this.handleDocumentClick.bind(this))
  }

  private handleDocumentClick(event: MouseEvent): void {
    const target = event.target as HTMLElement
    if (!this.element.contains(target) && this.state.isOpen) {
      this.setState({ isOpen: false })
    }
  }

  private toggleDropdown(): void {
    this.setState({ isOpen: !this.state.isOpen })
  }

  private selectLocale(code: LocaleCode): void {
    this.localeSystem.setLocale(code)
    this.setState({ isOpen: false })
  }

  private getGlobeIcon(): string {
    return `
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"></path>
      </svg>
    `
  }

  private getCheckIcon(): string {
    return `
      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
      </svg>
    `
  }

  protected render(): void {
    const { currentLocale, supported, isOpen } = this.state
    const currentLocaleInfo = supported.find(l => l.code === currentLocale)

    this.element.innerHTML = `
      <div class="relative">
        <button
          id="locale-toggle-btn"
          class="p-2 rounded-lg border border-gray-300 dark:border-gray-600 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-200 hover:bg-gray-200 dark:hover:bg-gray-600 transition-all focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400"
          aria-label="Select language"
          aria-haspopup="listbox"
          aria-expanded="${isOpen}"
          title="Language: ${currentLocaleInfo?.nativeName ?? currentLocale}"
        >
          ${this.getGlobeIcon()}
        </button>
        <div
          id="locale-dropdown"
          class="${isOpen ? '' : 'hidden'} absolute right-0 mt-2 w-56 rounded-lg shadow-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 py-1 z-50"
          role="listbox"
          aria-label="Language selection"
        >
          ${supported
            .map(
              locale => `
            <button
              class="locale-option w-full px-4 py-2 text-left flex items-center justify-between hover:bg-gray-100 dark:hover:bg-gray-700 ${
                locale.code === currentLocale
                  ? 'bg-blue-50 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400'
                  : 'text-gray-700 dark:text-gray-200'
              }"
              data-locale="${locale.code}"
              role="option"
              aria-selected="${locale.code === currentLocale}"
            >
              <span class="flex flex-col">
                <span class="font-medium">${this.escapeHtml(locale.nativeName)}</span>
                <span class="text-xs text-gray-500 dark:text-gray-400">${this.escapeHtml(locale.name)}</span>
              </span>
              ${locale.code === currentLocale ? this.getCheckIcon() : ''}
            </button>
          `
            )
            .join('')}
        </div>
      </div>
    `

    // Setup click handlers
    const toggleBtn = this.element.querySelector('#locale-toggle-btn')
    if (toggleBtn) {
      toggleBtn.addEventListener('click', event => {
        event.stopPropagation()
        this.toggleDropdown()
      })
    }

    const options = this.element.querySelectorAll('.locale-option')
    options.forEach(option => {
      option.addEventListener('click', event => {
        event.stopPropagation()
        const localeCode = (event.currentTarget as HTMLElement).dataset.locale as LocaleCode
        this.selectLocale(localeCode)
      })
    })
  }
}
