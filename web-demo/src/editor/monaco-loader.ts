/**
 * Monaco Editor lazy loader
 *
 * This module handles on-demand loading of Monaco Editor to improve initial page load time.
 * Monaco is only loaded when the user first focuses on the SQL editor.
 */

// Use dynamic import type to avoid static dependency
export type Monaco = typeof import('monaco-editor')
export type MonacoEditor = import('monaco-editor').editor.IStandaloneCodeEditor

let monacoInstance: Monaco | null = null
let monacoLoadPromise: Promise<Monaco> | null = null

/**
 * Configure Monaco Environment before loading
 * This sets up web workers to avoid UI freezes
 */
function configureMonacoEnvironment(): void {
  // Configure Monaco to use web workers properly
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ;(window as any).MonacoEnvironment = {
    getWorker() {
      // For now, use only the editor worker (most important for SQL editing performance)
      // This uses a local worker entry file that Vite can properly bundle
      return new Worker(new URL('../workers/editor.worker.ts', import.meta.url), {
        type: 'module',
      })
    },
  }
}

/**
 * Lazy load Monaco Editor on first use
 * Returns cached instance on subsequent calls
 */
export async function loadMonaco(): Promise<Monaco> {
  // Return cached instance if already loaded
  if (monacoInstance) {
    return monacoInstance
  }

  // Return in-flight promise if loading
  if (monacoLoadPromise) {
    return monacoLoadPromise
  }

  // Configure Monaco environment before loading
  configureMonacoEnvironment()

  // Start loading Monaco
  console.log('[Monaco Loader] Starting Monaco Editor load...')
  const startTime = performance.now()

  monacoLoadPromise = import('monaco-editor')
    .then(m => {
      monacoInstance = m
      const loadTime = performance.now() - startTime
      console.log(`[Monaco Loader] Monaco Editor loaded in ${loadTime.toFixed(2)}ms`)
      return m
    })
    .catch(err => {
      console.error('[Monaco Loader] Failed to load Monaco Editor:', err)
      // Clear promise to allow retry
      monacoLoadPromise = null
      throw err
    })

  return monacoLoadPromise
}

/**
 * Check if Monaco is already loaded
 */
export function isMonacoLoaded(): boolean {
  return monacoInstance !== null
}
