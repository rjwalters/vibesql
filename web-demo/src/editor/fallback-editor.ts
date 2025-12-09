/**
 * Fallback textarea editor
 *
 * Simple textarea-based editor shown before Monaco loads.
 * Provides immediate interactivity while Monaco is being lazy-loaded.
 */
import { t } from '../i18n'

export interface FallbackEditor {
  getValue(): string
  setValue(value: string): void
  destroy(): void
  onDidChangeModelContent(callback: () => void): void
  focus(): void
}

/**
 * Create a simple fallback editor using a textarea
 */
export function createFallbackEditor(
  container: HTMLElement,
  options: {
    value?: string
    readOnly?: boolean
    placeholder?: string
  }
): FallbackEditor {
  const { value = '', readOnly = false, placeholder = '' } = options

  // Create textarea element
  const textarea = document.createElement('textarea')
  textarea.value = value
  textarea.readOnly = readOnly
  textarea.placeholder = placeholder
  textarea.spellcheck = false
  textarea.autocomplete = 'off'
  textarea.autocapitalize = 'off'
  textarea.setAttribute('data-gramm', 'false') // Disable Grammarly

  // Style to match Monaco appearance
  textarea.style.cssText = `
    width: 100%;
    height: 100%;
    border: none;
    outline: none;
    resize: none;
    font-family: JetBrains Mono, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    font-size: 14px;
    line-height: 1.5;
    padding: 16px;
    background: transparent;
    color: inherit;
    tab-size: 2;
    white-space: pre;
    overflow-wrap: normal;
    overflow-x: auto;
  `

  // Add loading indicator overlay
  const loadingOverlay = document.createElement('div')
  loadingOverlay.style.cssText = `
    position: absolute;
    top: 8px;
    right: 8px;
    padding: 4px 8px;
    background: rgba(0, 0, 0, 0.6);
    color: #fff;
    border-radius: 4px;
    font-size: 11px;
    font-family: system-ui, -apple-system, sans-serif;
    pointer-events: none;
    opacity: 0;
    transition: opacity 0.2s;
    z-index: 10;
  `
  loadingOverlay.textContent = t('loading-editor')

  // Make container relative for absolute positioning of overlay
  container.style.position = 'relative'

  // Append elements
  container.appendChild(textarea)
  container.appendChild(loadingOverlay)

  // Show loading overlay on first focus
  let hasShownLoading = false
  textarea.addEventListener('focus', () => {
    if (!hasShownLoading) {
      loadingOverlay.style.opacity = '1'
      hasShownLoading = true
    }
  })

  const changeCallbacks: Array<() => void> = []

  // Listen for content changes
  textarea.addEventListener('input', () => {
    changeCallbacks.forEach(cb => cb())
  })

  return {
    getValue(): string {
      return textarea.value
    },

    setValue(newValue: string): void {
      textarea.value = newValue
      changeCallbacks.forEach(cb => cb())
    },

    destroy(): void {
      textarea.remove()
      loadingOverlay.remove()
    },

    onDidChangeModelContent(callback: () => void): void {
      changeCallbacks.push(callback)
    },

    focus(): void {
      textarea.focus()
    },
  }
}
