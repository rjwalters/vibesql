import './styles/main.css'
import { initTheme } from './theme'
import { initLocale } from './locale'
import { NavigationComponent } from './components/Navigation'
import { ConformanceReportComponent } from './components/ConformanceReport'

/**
 * Bootstrap the conformance report page
 */
function bootstrap(): void {
  // Initialize theme system (with localStorage persistence)
  const theme = initTheme()

  // Initialize locale system
  const locale = initLocale()

  // Initialize navigation component with theme and locale
  new NavigationComponent('conformance', theme, locale)

  // Initialize conformance report component
  new ConformanceReportComponent()
}

// Start the application when DOM is ready
document.addEventListener('DOMContentLoaded', bootstrap)
