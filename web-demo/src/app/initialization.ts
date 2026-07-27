import { initTheme } from '../theme'
import { initDatabase } from '../db/wasm'
import type { Database } from '../db/types'
import type { LoadingProgressComponent } from '../components/LoadingProgress'
import { DatabaseManager } from './database-manager'
import { EditorManager } from './editor-manager'
import { loadMonaco } from '../editor/monaco-loader'

const DEFAULT_SQL = `-- Welcome to VibeSQL Web Studio!
-- Press Ctrl/Cmd + Enter to run queries
-- Sample employees table is pre-loaded with 6 rows

-- See all employees
SELECT * FROM employees;

-- Try these queries too (uncomment and run):

-- Find engineering employees
-- SELECT name, salary FROM employees WHERE department = 'Engineering';

-- Count employees per department
-- SELECT department, COUNT(*) as count FROM employees GROUP BY department;

-- Find high earners
-- SELECT name, department, salary FROM employees WHERE salary > 80000;
`

export interface LayoutElements {
  editorContainer: HTMLDivElement | null
  resultsEditorContainer: HTMLDivElement | null
  runButton: HTMLButtonElement | null
}

export interface AppComponents {
  theme: ReturnType<typeof initTheme>
  database: DatabaseManager
  editor: EditorManager
  layout: LayoutElements
}

/**
 * Get DOM elements for editor layout
 */
export function getLayoutElements(): LayoutElements {
  return {
    editorContainer: document.querySelector<HTMLDivElement>('#editor'),
    resultsEditorContainer: document.querySelector<HTMLDivElement>('#results'),
    runButton: document.querySelector<HTMLButtonElement>('#execute-btn'),
  }
}

/**
 * Safely initialize database (returns null on failure)
 */
async function safeInitDatabase(): Promise<Database | null> {
  try {
    return await initDatabase()
  } catch (error) {
    console.warn('WASM database unavailable, SQL metadata features limited.', error)
    return null
  }
}

/**
 * Setup theme synchronization with Monaco editor
 */
export function setupThemeSync(
  theme: ReturnType<typeof initTheme>,
  applyTheme: (mode: 'light' | 'dark') => void
): void {
  // Apply current theme
  applyTheme(theme.current)

  // Listen for theme changes
  const observer = new MutationObserver(() => {
    const isDark = document.documentElement.classList.contains('dark')
    applyTheme(isDark ? 'dark' : 'light')
  })
  observer.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ['class'],
  })
}

/**
 * Initialize application components
 */
export async function initializeApp(progress: LoadingProgressComponent): Promise<AppComponents> {
  // Step 1: Initialize theme
  progress.updateStep('theme', 50, 'loading')
  const theme = initTheme()
  progress.completeStep('theme')

  const layout = getLayoutElements()

  if (!layout.editorContainer) {
    progress.errorStep('ui', 'Failed to find #editor container')
    throw new Error('Failed to find #editor container')
  }

  if (!layout.resultsEditorContainer) {
    progress.errorStep('ui', 'Failed to find #results container')
    throw new Error('Failed to find #results container')
  }

  // Step 2: Create editor manager with fallback editors (fast, no Monaco loading)
  progress.updateStep('editor', 50, 'loading')

  const editorManager = new EditorManager(
    layout.editorContainer,
    layout.resultsEditorContainer,
    DEFAULT_SQL
  )

  progress.completeStep('editor')

  // Step 3: Load WASM database and preload Monaco in parallel
  progress.updateStep('wasm', 20, 'loading')
  progress.updateStep('ui', 10, 'loading')

  const [database] = await Promise.all([
    safeInitDatabase().then(db => {
      progress.updateStep('wasm', 80, 'loading')
      return db
    }),
    // Preload Monaco while WASM loads (don't block on failure)
    // Note: We don't update progress here since editor step is already complete
    // with fallback editors. Monaco is a background optimization.
    loadMonaco()
      .then(() => {
        console.log('[Init] Monaco preloaded during WASM load')
      })
      .catch(err => {
        console.warn('[Init] Monaco preload failed, will load on demand:', err)
      }),
  ])

  progress.completeStep('wasm')

  // Step 4: Create database manager
  progress.updateStep('ui', 50, 'loading')
  const databaseManager = new DatabaseManager(database)

  return {
    theme,
    database: databaseManager,
    editor: editorManager,
    layout,
  }
}
