import './styles/main.css'
import { getStorageMode, isOpfsSupported } from './db/wasm'
import { NavigationComponent } from './components/Navigation'
import { ExamplesComponent } from './components/Examples'
import type { ExampleSelectEvent } from './components/Examples'
import { DatabaseSelectorComponent } from './components/DatabaseSelector'
import type { DatabaseOption } from './components/DatabaseSelector'
import { LoadingProgressComponent } from './components/LoadingProgress'
import { initShowcase } from './showcase'
import { sampleDatabases } from './data/sample-databases'
import { updateConformanceFooter } from './utils/conformance'
import { initializeApp, setupThemeSync } from './app/initialization'
import { createExecutionHandler } from './app/query-executor'
import { initLocale } from './locale'

async function bootstrap(): Promise<void> {
  // Initialize loading progress indicator
  const progress = new LoadingProgressComponent()
  progress.addStep('theme', 'Initializing theme')
  progress.addStep('editor', 'Preparing editor')
  progress.addStep('wasm', 'Loading database engine')
  progress.addStep('ui', 'Setting up user interface')

  // Hide loading indicator when complete
  progress.onComplete(() => {
    setTimeout(() => progress.hide(), 500)
  })

  try {
    // Initialize core application components
    const app = await initializeApp(progress)

    // Initialize locale system
    const locale = initLocale()

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
    const timestampElement = document.getElementById('build-timestamp')
    if (timestampElement) {
      try {
        const timestamp = __BUILD_TIMESTAMP__
        const date = new Date(timestamp)
        const formattedDate = date.toLocaleString('en-US', {
          year: 'numeric',
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          timeZoneName: 'short',
        })
        timestampElement.textContent = `Deployed: ${formattedDate}`
      } catch (error) {
        console.warn('Failed to set build timestamp', error)
        timestampElement.textContent = 'Deployed: Development build'
      }
    }

    // Update conformance pass rate dynamically
    // Don't wait for conformance update, it can complete in background
    void updateConformanceFooter()

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

// Start the application when DOM is ready
document.addEventListener('DOMContentLoaded', () => void bootstrap())
