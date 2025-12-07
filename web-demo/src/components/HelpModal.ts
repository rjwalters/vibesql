import { Component } from './base'
import { t } from '../i18n'

interface HelpModalState {
  isOpen: boolean
}

/**
 * Help modal showing keyboard shortcuts and usage tips
 */
export class HelpModal extends Component<HelpModalState> {
  constructor() {
    super('#help-modal', { isOpen: false })
    this.attachGlobalHandlers()
  }

  open(): void {
    this.setState({ isOpen: true })
    document.body.style.overflow = 'hidden' // Prevent background scrolling
  }

  close(): void {
    this.setState({ isOpen: false })
    document.body.style.overflow = '' // Restore scrolling
  }

  toggle(): void {
    if (this.state.isOpen) {
      this.close()
    } else {
      this.open()
    }
  }

  private attachGlobalHandlers(): void {
    // Open help with ? or F1
    document.addEventListener('keydown', (e) => {
      if (e.key === '?' && !this.isInputFocused()) {
        e.preventDefault()
        this.open()
      } else if (e.key === 'F1') {
        e.preventDefault()
        this.open()
      } else if (e.key === 'Escape' && this.state.isOpen) {
        e.preventDefault()
        this.close()
      }
    })
  }

  private isInputFocused(): boolean {
    const active = document.activeElement
    return (
      active?.tagName === 'INPUT' ||
      active?.tagName === 'TEXTAREA' ||
      (active?.hasAttribute('contenteditable') ?? false)
    )
  }

  protected render(): void {
    if (!this.state.isOpen) {
      this.element.innerHTML = ''
      return
    }

    this.element.innerHTML = this.renderModal()
    this.attachModalHandlers()
  }

  private renderModal(): string {
    return `
      <div class="modal-overlay" data-action="close-overlay">
        <div class="modal-content">
          <div class="modal-header">
            <h2 class="text-2xl font-bold text-gray-900 dark:text-gray-100">
              ${t('help-title')}
            </h2>
            <button class="modal-close" data-action="close-modal" type="button" aria-label="${t('help-close')}">
              ×
            </button>
          </div>

          <div class="modal-body">
            <section class="help-section">
              <h3 class="help-section__title">${t('help-editor-shortcuts')}</h3>
              <dl class="help-shortcuts">
                <div class="help-shortcut">
                  <dt><kbd>⌘/Ctrl</kbd> + <kbd>Enter</kbd></dt>
                  <dd>${t('help-shortcut-execute')}</dd>
                </div>
                <div class="help-shortcut">
                  <dt><kbd>⌘/Ctrl</kbd> + <kbd>/</kbd></dt>
                  <dd>${t('help-shortcut-comment')}</dd>
                </div>
                <div class="help-shortcut">
                  <dt><kbd>Tab</kbd></dt>
                  <dd>${t('help-shortcut-indent')}</dd>
                </div>
              </dl>
            </section>

            <section class="help-section">
              <h3 class="help-section__title">${t('help-navigation')}</h3>
              <dl class="help-shortcuts">
                <div class="help-shortcut">
                  <dt><kbd>?</kbd> or <kbd>F1</kbd></dt>
                  <dd>${t('help-shortcut-show-help')}</dd>
                </div>
                <div class="help-shortcut">
                  <dt><kbd>Esc</kbd></dt>
                  <dd>${t('help-shortcut-close-help')}</dd>
                </div>
              </dl>
            </section>

            <section class="help-section">
              <h3 class="help-section__title">${t('help-results-actions')}</h3>
              <dl class="help-shortcuts">
                <div class="help-shortcut">
                  <dt>${t('help-action-copy')}</dt>
                  <dd>${t('help-action-copy-desc')}</dd>
                </div>
                <div class="help-shortcut">
                  <dt>${t('help-action-export')}</dt>
                  <dd>${t('help-action-export-desc')}</dd>
                </div>
              </dl>
            </section>

            <section class="help-section">
              <h3 class="help-section__title">${t('help-tips')}</h3>
              <ul class="help-tips">
                <li>${t('help-tip-limit')}</li>
                <li>${t('help-tip-time')}</li>
                <li>${t('help-tip-syntax')}</li>
                <li>${t('help-tip-theme')}</li>
              </ul>
            </section>
          </div>

          <div class="modal-footer">
            <button class="button" data-action="close-modal" type="button">
              ${t('help-got-it')}
            </button>
          </div>
        </div>
      </div>
    `
  }

  private attachModalHandlers(): void {
    const closeButtons = this.element.querySelectorAll<HTMLElement>('[data-action="close-modal"]')
    const overlay = this.element.querySelector<HTMLElement>('[data-action="close-overlay"]')

    closeButtons.forEach((button) => {
      button.addEventListener('click', () => this.close())
    })

    overlay?.addEventListener('click', (e) => {
      if (e.target === overlay) {
        this.close()
      }
    })
  }
}
