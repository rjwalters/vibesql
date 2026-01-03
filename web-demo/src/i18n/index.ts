/**
 * Fluent-based internationalization (i18n) system for the web UI
 *
 * Uses @fluent/bundle to provide localized strings for the web interface.
 * Integrates with the existing locale system for seamless language switching.
 */

import { FluentBundle, FluentResource } from '@fluent/bundle'
import type { LocaleCode, LocaleInfo } from '../locale'
import { SUPPORTED_LOCALES } from '../locale'

// Import all FTL resources
import enUS from './resources/en-US.ftl?raw'
import ar from './resources/ar.ftl?raw'
import de from './resources/de.ftl?raw'
import es from './resources/es.ftl?raw'
import fr from './resources/fr.ftl?raw'
import hi from './resources/hi.ftl?raw'
import id from './resources/id.ftl?raw'
import it from './resources/it.ftl?raw'
import ja from './resources/ja.ftl?raw'
import ko from './resources/ko.ftl?raw'
import nl from './resources/nl.ftl?raw'
import pl from './resources/pl.ftl?raw'
import ptBR from './resources/pt-BR.ftl?raw'
import ru from './resources/ru.ftl?raw'
import sv from './resources/sv.ftl?raw'
import th from './resources/th.ftl?raw'
import tr from './resources/tr.ftl?raw'
import uk from './resources/uk.ftl?raw'
import vi from './resources/vi.ftl?raw'
import zhCN from './resources/zh-CN.ftl?raw'

/** Map of locale codes to FTL resource content */
const FTL_RESOURCES: Record<string, string> = {
  'en-US': enUS,
  'ar': ar,
  'de': de,
  'es': es,
  'fr': fr,
  'hi': hi,
  'id': id,
  'it': it,
  'ja': ja,
  'ko': ko,
  'nl': nl,
  'pl': pl,
  'pt-BR': ptBR,
  'ru': ru,
  'sv': sv,
  'th': th,
  'tr': tr,
  'uk': uk,
  'vi': vi,
  'zh-CN': zhCN,
}

/** Current active bundle */
let currentBundle: FluentBundle | null = null
let currentLocale: LocaleCode = 'en-US'

/** Listeners for locale changes */
const listeners: Set<() => void> = new Set()

/**
 * Initialize the i18n system with a locale
 */
export function initI18n(locale: LocaleCode): void {
  setI18nLocale(locale)
}

/**
 * Set the active locale and load its resources
 */
export function setI18nLocale(locale: LocaleCode): void {
  const ftlContent = FTL_RESOURCES[locale] ?? FTL_RESOURCES['en-US']

  const bundle = new FluentBundle(locale)
  const resource = new FluentResource(ftlContent)
  const errors = bundle.addResource(resource)

  if (errors.length > 0) {
    console.warn(`Fluent resource errors for ${locale}:`, errors)
  }

  currentBundle = bundle
  currentLocale = locale

  // Notify listeners
  listeners.forEach(callback => callback())
}

/**
 * Get a translated message by ID
 *
 * @param id - Message ID from FTL file
 * @param args - Optional arguments for message formatting
 * @returns Translated string or the ID if not found
 */
export function t(id: string, args?: Record<string, string | number>): string {
  if (!currentBundle) {
    return id
  }

  const message = currentBundle.getMessage(id)
  if (!message || !message.value) {
    console.warn(`Missing translation for "${id}" in locale "${currentLocale}"`)
    return id
  }

  const errors: Error[] = []
  const formatted = currentBundle.formatPattern(message.value, args, errors)

  if (errors.length > 0) {
    console.warn(`Format errors for "${id}":`, errors)
  }

  return formatted
}

/**
 * Get the current locale code
 */
export function getCurrentI18nLocale(): LocaleCode {
  return currentLocale
}

/**
 * Get locale info for the current locale
 */
export function getCurrentLocaleInfo(): LocaleInfo | undefined {
  return SUPPORTED_LOCALES.find(l => l.code === currentLocale)
}

/**
 * Subscribe to locale changes
 *
 * @param callback - Function to call when locale changes
 * @returns Unsubscribe function
 */
export function onI18nChange(callback: () => void): () => void {
  listeners.add(callback)
  return () => listeners.delete(callback)
}

/**
 * Update all data-i18n elements in the DOM
 *
 * Elements with data-i18n="message-id" will have their textContent updated.
 * Elements with data-i18n-html="message-id" will have their innerHTML updated (for messages containing HTML).
 * Elements with data-i18n-attr="attr:message-id" will have the attribute updated.
 */
export function updateDOM(): void {
  // Update text content (plain text, escapes HTML)
  const elements = document.querySelectorAll('[data-i18n]')
  elements.forEach(el => {
    const id = el.getAttribute('data-i18n')
    if (id) {
      el.textContent = t(id)
    }
  })

  // Update HTML content (allows HTML tags in translations)
  document.querySelectorAll('[data-i18n-html]').forEach(el => {
    const id = el.getAttribute('data-i18n-html')
    if (id) {
      el.innerHTML = t(id)
    }
  })

  // Update attributes (e.g., placeholder, title, aria-label)
  document.querySelectorAll('[data-i18n-attr]').forEach(el => {
    const spec = el.getAttribute('data-i18n-attr')
    if (spec) {
      // Format: "attr:message-id" or "attr1:id1,attr2:id2"
      spec.split(',').forEach(part => {
        const [attr, id] = part.split(':')
        if (attr && id) {
          el.setAttribute(attr.trim(), t(id.trim()))
        }
      })
    }
  })
}
