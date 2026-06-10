import type { WasmModule, Database } from './types'
import type { LocaleInfo } from '../locale'

let wasmModule: WasmModule | null = null
let db: Database | null = null
let usingOpfs: boolean = false

async function loadWasmModule(): Promise<WasmModule> {
  if (wasmModule) return wasmModule

  try {
    // Import WASM module - Vite will handle bundling this properly
    // In production, Vite copies public/pkg/ to dist/pkg/ and resolves imports correctly
    const module = (await import('../../public/pkg/vibesql_wasm.js')) as unknown as WasmModule
    wasmModule = module
    return module
  } catch {
    console.warn(
      'WASM bindings not found; falling back to stub module. Run `./scripts/build-wasm.sh` to enable database features.'
    )
    const module = (await import('./wasm-fallback')) as unknown as WasmModule
    wasmModule = module
    return module
  }
}

/**
 * Initialize the WASM database module.
 * Must be called before any database operations.
 *
 * @param useOpfs - Whether to use OPFS persistence (default: true)
 * @returns Promise that resolves to Database instance
 * @throws Error if WASM fails to load
 */
export async function initDatabase(useOpfs: boolean = true): Promise<Database> {
  if (db) return db

  try {
    const module = await loadWasmModule()
    await module.default()

    // Use OPFS-backed persistent storage if requested and available
    if (useOpfs && isOpfsSupported()) {
      db = await module.Database.newWithPersistence()
      usingOpfs = true
      console.log('Database initialized with OPFS persistent storage')
    } else {
      db = new module.Database()
      usingOpfs = false
      if (useOpfs && !isOpfsSupported()) {
        console.warn(
          'OPFS requested but not supported in this browser, falling back to in-memory storage'
        )
      } else {
        console.log('Database initialized with in-memory storage')
      }
    }

    return db
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`Failed to load database: ${message}`, { cause: error })
  }
}

/**
 * Get the initialized database instance.
 *
 * @returns Database instance
 * @throws Error if database not initialized
 */
export function getDatabase(): Database {
  if (!db) {
    throw new Error('Database not initialized. Call initDatabase() first.')
  }
  return db
}

/**
 * Check if database is initialized
 *
 * @returns True if database is ready, false otherwise
 */
export function isDatabaseReady(): boolean {
  return db !== null
}

/**
 * Check if OPFS (Origin Private File System) is supported in this browser
 *
 * @returns True if OPFS is available, false otherwise
 */
export function isOpfsSupported(): boolean {
  // Check if we're in a browser environment and OPFS is available
  if (typeof window === 'undefined' || typeof navigator === 'undefined') {
    return false
  }

  // Check for the File System Access API (includes OPFS)
  return (
    'storage' in navigator &&
    'getDirectory' in navigator.storage &&
    typeof navigator.storage.getDirectory === 'function'
  )
}

/**
 * Check if the database is using OPFS persistent storage
 *
 * @returns True if using OPFS, false if using in-memory storage
 */
export function isUsingOpfs(): boolean {
  return usingOpfs
}

/**
 * Get storage mode as a human-readable string
 *
 * @returns "OPFS (Persistent)" or "Memory (Temporary)"
 */
export function getStorageMode(): string {
  return usingOpfs ? 'OPFS (Persistent)' : 'Memory (Temporary)'
}

/**
 * Set the locale for localized error messages.
 *
 * @param locale - Locale code (e.g., 'en-US', 'ja', 'es')
 * @returns true if locale was set, false if i18n not available
 */
export function setLocale(locale: string): boolean {
  if (wasmModule?.set_locale) {
    wasmModule.set_locale(locale)
    return true
  }
  return false
}

/**
 * Get a localized "language changed" confirmation message.
 *
 * @param localeInfo - The locale info with native name
 * @returns Localized message like "Language changed to 日本語" or null if i18n not available
 */
export function getLocaleChangedMessage(localeInfo: LocaleInfo): string | null {
  if (wasmModule?.get_locale_changed_message) {
    return wasmModule.get_locale_changed_message(localeInfo.nativeName)
  }
  return null
}

/**
 * Check if i18n (internationalization) is available in the WASM module.
 *
 * @returns true if i18n functions are available
 */
export function isI18nAvailable(): boolean {
  return wasmModule?.set_locale !== undefined
}
