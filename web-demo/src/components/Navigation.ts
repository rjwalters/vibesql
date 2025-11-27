import { Component } from './base'
import { Theme } from '../theme'
import { ThemeToggleComponent } from './ThemeToggle'

export interface NavigationLink {
  id: string
  label: string
  href: string
  icon: string
  external?: boolean
}

interface NavigationState {
  currentPage: string
}

/**
 * Navigation component with circular icons for page navigation
 */
export class NavigationComponent extends Component<NavigationState> {
  private themeSystem: Theme | null = null

  constructor(currentPage: string, themeSystem?: Theme) {
    super('#nav-container', { currentPage })
    this.themeSystem = themeSystem || null
  }

  protected render(): void {
    const { currentPage } = this.state

    const links: NavigationLink[] = [
      {
        id: 'terminal',
        label: 'SQL Terminal Demo',
        href: './',
        icon: this.getTerminalIcon(),
      },
      {
        id: 'performance',
        label: 'Performance Dashboard',
        href: 'performance.html',
        icon: this.getPerformanceIcon(),
      },
      {
        id: 'conformance',
        label: 'SQL Test Compliance Report',
        href: 'conformance.html',
        icon: this.getConformanceIcon(),
      },
      {
        id: 'benchmarks',
        label: 'Benchmark Data',
        href: 'benchmarks.html',
        icon: this.getBenchmarkIcon(),
      },
      {
        id: 'github',
        label: 'GitHub Repository',
        href: 'https://github.com/rjwalters/vibesql',
        icon: this.getGithubIcon(),
        external: true,
      },
      {
        id: 'home',
        label: 'Home',
        href: 'https://rjwalters.info',
        icon: this.getHomeIcon(),
        external: true,
      },
    ]

    this.element.innerHTML = `
      <nav class="flex items-center gap-2" role="navigation" aria-label="Main navigation">
        ${links.map(link => this.renderNavLink(link, currentPage)).join('')}
        <div id="theme-toggle-nav"></div>
      </nav>
    `

    // Initialize theme toggle if themeSystem is provided
    // Use setTimeout to ensure DOM is fully updated and subclass constructor completes
    setTimeout(() => {
      if (!this.themeSystem) return
      const themeToggleContainer = this.element.querySelector('#theme-toggle-nav') as HTMLDivElement
      if (!themeToggleContainer) return

      // Create a wrapper for the theme toggle
      const wrapper = document.createElement('div')
      wrapper.id = 'theme-toggle'
      // Append wrapper to DOM
      themeToggleContainer.appendChild(wrapper)
      // Create the theme toggle component
      new ThemeToggleComponent(this.themeSystem)
    }, 0)
  }

  private renderNavLink(link: NavigationLink, currentPage: string): string {
    const isActive = link.id === currentPage
    const activeClass = isActive
      ? 'bg-blue-100 dark:bg-blue-900/30 ring-2 ring-blue-500 dark:ring-blue-400'
      : 'bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600'

    return `
      <a
        href="${this.escapeHtml(link.href)}"
        ${link.external ? 'target="_blank" rel="noopener noreferrer"' : ''}
        class="p-2.5 rounded-full ${activeClass} text-gray-700 dark:text-gray-200 transition-all focus:outline-none focus:ring-2 focus:ring-blue-500 dark:focus:ring-blue-400"
        aria-label="${this.escapeHtml(link.label)}"
        title="${this.escapeHtml(link.label)}"
        ${isActive ? 'aria-current="page"' : ''}
      >
        ${link.icon}
      </a>
    `
  }

  private getHomeIcon(): string {
    return `
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m3 9 9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path>
        <polyline stroke-linecap="round" stroke-linejoin="round" stroke-width="2" points="9 22 9 12 15 12 15 22"></polyline>
      </svg>
    `
  }

  private getTerminalIcon(): string {
    return `
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"></path>
      </svg>
    `
  }

  private getConformanceIcon(): string {
    return `
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path>
      </svg>
    `
  }

  private getBenchmarkIcon(): string {
    return `
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"></path>
      </svg>
    `
  }

  private getPerformanceIcon(): string {
    return `
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"></path>
      </svg>
    `
  }

  private getGithubIcon(): string {
    return `
      <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z"/>
      </svg>
    `
  }
}
