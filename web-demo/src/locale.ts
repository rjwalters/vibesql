/**
 * Locale management system for language selection
 *
 * Implements locale selection with:
 * - Browser language detection
 * - localStorage persistence
 * - Manual selection capability
 */

/**
 * Supported locale info
 */
export interface LocaleInfo {
  code: string
  name: string
  nativeName: string
}

/**
 * Supported locales matching vibesql-l10n resources
 */
export const SUPPORTED_LOCALES: LocaleInfo[] = [
  { code: 'en-US', name: 'English', nativeName: 'English' },
  { code: 'es', name: 'Spanish', nativeName: 'Español' },
  { code: 'pt-BR', name: 'Portuguese (Brazil)', nativeName: 'Português (Brasil)' },
  { code: 'zh-CN', name: 'Chinese (Simplified)', nativeName: '简体中文' },
  { code: 'ja', name: 'Japanese', nativeName: '日本語' },
  { code: 'de', name: 'German', nativeName: 'Deutsch' },
  { code: 'fr', name: 'French', nativeName: 'Français' },
  { code: 'ko', name: 'Korean', nativeName: '한국어' },
]

export type LocaleCode = string

export interface Locale {
  /** Set the current locale */
  setLocale(code: LocaleCode): void
  /** Get current locale code */
  readonly current: LocaleCode
  /** Get all supported locales */
  readonly supported: LocaleInfo[]
  /** Subscribe to locale changes */
  onChange(callback: (locale: LocaleCode) => void): () => void
}

const LOCALE_STORAGE_KEY = 'vibesql-locale'

/**
 * Normalize a browser locale string to match our supported locales
 */
function normalizeLocale(input: string): LocaleCode | null {
  // Try exact match first
  const exactMatch = SUPPORTED_LOCALES.find(l => l.code === input)
  if (exactMatch) return exactMatch.code

  // Normalize underscore to hyphen
  const normalized = input.replace('_', '-')
  const normalizedMatch = SUPPORTED_LOCALES.find(l => l.code === normalized)
  if (normalizedMatch) return normalizedMatch.code

  // Try language-only match (e.g., "en" matches "en-US")
  const language = input.split('-')[0].split('_')[0]
  const languageMatch = SUPPORTED_LOCALES.find(l => l.code.startsWith(language))
  if (languageMatch) return languageMatch.code

  return null
}

/**
 * Detect locale from browser settings
 */
function detectBrowserLocale(): LocaleCode {
  // Try navigator.language first
  if (navigator.language) {
    const detected = normalizeLocale(navigator.language)
    if (detected) return detected
  }

  // Try navigator.languages array
  if (navigator.languages) {
    for (const lang of navigator.languages) {
      const detected = normalizeLocale(lang)
      if (detected) return detected
    }
  }

  // Default to English
  return 'en-US'
}

/**
 * Initialize the locale system.
 *
 * On first load:
 * 1. Check localStorage for saved preference
 * 2. Fall back to browser language detection
 * 3. Apply locale to document root
 *
 * @returns Locale controller
 */
export function initLocale(): Locale {
  const listeners: Set<(locale: LocaleCode) => void> = new Set()

  // Check localStorage first, then browser preference
  const savedLocale = localStorage.getItem(LOCALE_STORAGE_KEY)
  let currentLocale: LocaleCode

  if (savedLocale) {
    const normalized = normalizeLocale(savedLocale)
    currentLocale = normalized ?? detectBrowserLocale()
  } else {
    currentLocale = detectBrowserLocale()
  }

  // Apply initial locale
  document.documentElement.lang = currentLocale

  return {
    setLocale(code: LocaleCode) {
      if (currentLocale === code) return

      currentLocale = code
      localStorage.setItem(LOCALE_STORAGE_KEY, code)
      document.documentElement.lang = code

      // Notify listeners
      listeners.forEach(callback => callback(code))
    },

    get current() {
      return currentLocale
    },

    get supported() {
      return SUPPORTED_LOCALES
    },

    onChange(callback: (locale: LocaleCode) => void) {
      listeners.add(callback)
      return () => listeners.delete(callback)
    },
  }
}
