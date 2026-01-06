import './styles/main.css'
import { getStorageMode, isOpfsSupported, setLocale, getLocaleChangedMessage } from './db/wasm'
import { NavigationComponent } from './components/Navigation'
import { ExamplesComponent } from './components/Examples'
import type { ExampleSelectEvent } from './components/Examples'
import { DatabaseSelectorComponent } from './components/DatabaseSelector'
import type { DatabaseOption } from './components/DatabaseSelector'
import { LoadingProgressComponent } from './components/LoadingProgress'
import { initShowcase } from './showcase'
import { sampleDatabases } from './data/sample-databases'
import { initializeApp, setupThemeSync } from './app/initialization'
import { createExecutionHandler } from './app/query-executor'
import { initLocale, SUPPORTED_LOCALES } from './locale'
import { initI18n, setI18nLocale, updateDOM, t } from './i18n'
import { setBuildTimestamp } from './utils/build-timestamp'

async function bootstrap(): Promise<void> {
  // Initialize locale and i18n early so loading messages can be translated
  const locale = initLocale()
  initI18n(locale.current)

  // Initialize loading progress indicator with translated messages
  const progress = new LoadingProgressComponent()
  progress.addStep('theme', t('loading-initializing-theme'))
  progress.addStep('editor', t('loading-preparing-editor'))
  progress.addStep('wasm', t('loading-database-engine'))
  progress.addStep('ui', t('loading-setting-up-ui'))

  // Hide loading indicator when complete
  progress.onComplete(() => {
    setTimeout(() => progress.hide(), 500)
  })

  try {
    // Initialize core application components
    const app = await initializeApp(progress)

    // Now that DOM is ready, update translated elements
    updateDOM()
    updateDocumentTitle()

    // Wire up locale changes to WASM module, i18n, and show confirmation
    locale.onChange(localeCode => {
      // Update WASM locale for localized error messages
      setLocale(localeCode)

      // Update web UI i18n
      setI18nLocale(localeCode)
      updateDOM()
      updateDocumentTitle()

      // Update HTML lang attribute
      document.documentElement.lang = localeCode

      // Get localized confirmation message
      const localeInfo = SUPPORTED_LOCALES.find(l => l.code === localeCode)
      if (localeInfo) {
        const message = getLocaleChangedMessage(localeInfo)
        if (message) {
          showLocaleToast(message)
        }
      }
    })

    // Set initial locale in WASM module
    setLocale(locale.current)

    // Setup theme synchronization with editor
    setupThemeSync(app.theme, mode => app.editor.applyTheme(mode))

    // Initialize Navigation component with theme and locale
    progress.updateStep('ui', 70, 'loading')
    new NavigationComponent('terminal', app.theme, locale)

    // Update storage status display
    const storageStatusEl = document.getElementById('storage-status')
    if (storageStatusEl) {
      const storageMode = getStorageMode()
      storageStatusEl.textContent = storageMode

      // Add visual indicator for OPFS vs in-memory
      if (isOpfsSupported() && storageMode.includes('OPFS')) {
        storageStatusEl.classList.add('text-green-600', 'dark:text-green-400')
        storageStatusEl.title =
          'Data persists across browser sessions using Origin Private File System'
      } else {
        storageStatusEl.classList.add('text-yellow-600', 'dark:text-yellow-400')
        storageStatusEl.title = 'Data is temporary and will be lost when the page reloads'
      }
    }

    // Pre-load default sample database for immediate exploration
    await app.database.loadDatabase('employees')

    // Create execution handler (temporary, will be recreated after Monaco upgrade)
    let executeHandler = createExecutionHandler(
      app.editor.getEditor(),
      app.database.getDatabase(),
      app.editor.getResultsEditor(),
      () => app.database.refreshTables()
    )

    // Function to upgrade editors to Monaco and recreate handler
    const upgradeEditorsToMonaco = async (): Promise<void> => {
      await app.editor.upgradeToMonaco(
        () => app.database.getTableNames(),
        () => executeHandler()
      )

      // Recreate execute handler after upgrade
      executeHandler = createExecutionHandler(
        app.editor.getEditor(),
        app.database.getDatabase(),
        app.editor.getResultsEditor(),
        () => app.database.refreshTables()
      )
    }

    // Upgrade to Monaco now (Monaco was preloaded during WASM load)
    // This ensures Monaco is fully rendered before hiding the loader
    progress.updateStep('ui', 80, 'loading')
    await upgradeEditorsToMonaco()

    // Initialize Database Selector with all available sample databases
    const databases: DatabaseOption[] = sampleDatabases.map(db => ({
      id: db.id,
      name: db.name,
      description: db.description,
    }))
    const databaseSelector = new DatabaseSelectorComponent(
      databases,
      app.database.getCurrentDatabaseId()
    )
    databaseSelector.onChange((dbId: string) => {
      void app.database.loadDatabase(dbId)
    })

    // Initialize Examples sidebar
    const examplesComponent = new ExamplesComponent()
    examplesComponent.onSelect((event: ExampleSelectEvent) => {
      app.editor.getEditor().setValue(event.sql)
      // Switch database if needed
      if (event.database !== app.database.getCurrentDatabaseId()) {
        void app.database.loadDatabase(event.database)
        databaseSelector.setSelected(event.database)
      }
    })

    // Run button executes query
    app.layout.runButton?.addEventListener('click', () => {
      void executeHandler()
    })

    // Initialize SQL:1999 Showcase navigation
    initShowcase()

    // Set build timestamp in footer
    setBuildTimestamp()

    // Final UI setup complete
    progress.updateStep('ui', 95, 'loading')

    // Small delay to show completion state
    await new Promise(resolve => setTimeout(resolve, 200))
    progress.completeStep('ui')
  } catch (error) {
    console.error('Bootstrap error:', error)
    const message = error instanceof Error ? error.message : String(error)
    progress.errorStep('ui', `Initialization failed: ${message}`)
  }
}

/**
 * Update the document title with translated text
 */
function updateDocumentTitle(): void {
  document.title = t('page-title')
}

/**
 * Show a toast notification for locale changes
 */
function showLocaleToast(message: string): void {
  // Remove any existing toast
  const existingToast = document.getElementById('locale-toast')
  if (existingToast) {
    existingToast.remove()
  }

  // Create toast element
  const toast = document.createElement('div')
  toast.id = 'locale-toast'
  toast.className =
    'fixed bottom-4 right-4 bg-blue-600 dark:bg-blue-500 text-white px-4 py-3 rounded-lg shadow-lg z-50 transform transition-all duration-300 translate-y-0 opacity-100'
  toast.innerHTML = `
    <div class="flex items-center gap-2">
      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9"></path>
      </svg>
      <span>${message}</span>
    </div>
  `

  document.body.appendChild(toast)

  // Auto-dismiss after 3 seconds
  setTimeout(() => {
    toast.classList.add('translate-y-4', 'opacity-0')
    setTimeout(() => toast.remove(), 300)
  }, 3000)
}

// Start the application when DOM is ready
document.addEventListener('DOMContentLoaded', () => void bootstrap())
