import { Component } from './base'
import { Theme } from '../theme'
import { t, onI18nChange } from '../i18n'

interface ThemeToggleState {
  theme: 'light' | 'dark'
}

/**
 * Theme toggle component - switches between light and dark mode
 */
export class ThemeToggleComponent extends Component<ThemeToggleState> {
  private themeSystem: Theme
  private unsubscribeI18n: (() => void) | null = null

  constructor(themeSystem: Theme) {
    const currentTheme = themeSystem.current as 'light' | 'dark'
    super('#theme-toggle', { theme: currentTheme })
    this.themeSystem = themeSystem

    // Re-render when locale changes to update translations
    this.unsubscribeI18n = onI18nChange(() => {
      this.render()
    })
  }

  destroy(): void {
    if (this.unsubscribeI18n) {
      this.unsubscribeI18n()
    }
  }

  /**
   * Toggle between light and dark mode
   */
  toggle(): void {
    const isDark = this.themeSystem.toggle()
    this.setState({ theme: isDark ? 'dark' : 'light' })
  }

  protected render(): void {
    const { theme } = this.state
    const label = theme === 'dark' ? t('theme-toggle-light') : t('theme-toggle-dark')

    // SVG icons for sun and moon
    const sunIcon = `
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"></path>
      </svg>
    `

    const moonIcon = `
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z"></path>
      </svg>
    `

    const icon = theme === 'dark' ? sunIcon : moonIcon

    this.element.innerHTML = `
      <button
        id="theme-toggle-btn"
        class="p-2 rounded-lg border border-gray-300 dark:border-gray-600 bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-200 hover:bg-gray-200 dark:hover:bg-gray-600 transition-all focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400"
        aria-label="${this.escapeHtml(label)}"
        title="${this.escapeHtml(label)}"
      >
        ${icon}
      </button>
    `

    // Setup click handler
    const btn = this.element.querySelector('#theme-toggle-btn')
    if (btn) {
      btn.addEventListener('click', () => this.toggle())
    }
  }
}
