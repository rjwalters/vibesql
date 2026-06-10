import { createFallbackEditor, type FallbackEditor } from '../editor/fallback-editor'
import { loadMonaco, type Monaco, type MonacoEditor } from '../editor/monaco-loader'
import { validateSql } from '../editor/validation'

type Editor = MonacoEditor | FallbackEditor

const SQL_KEYWORDS = [
  'SELECT',
  'FROM',
  'WHERE',
  'GROUP BY',
  'ORDER BY',
  'HAVING',
  'JOIN',
  'INNER JOIN',
  'LEFT JOIN',
  'RIGHT JOIN',
  'FULL JOIN',
  'ON',
  'LIMIT',
  'OFFSET',
  'INSERT',
  'UPDATE',
  'DELETE',
  'CREATE',
  'TABLE',
  'DROP',
  'ALTER',
  'VALUES',
  'SET',
  'DISTINCT',
  'COUNT',
  'SUM',
  'AVG',
  'MIN',
  'MAX',
]

/**
 * Manages SQL editor lifecycle and Monaco upgrades
 */
export class EditorManager {
  private editor: Editor
  private resultsEditor: Editor
  private monacoInstance: Monaco | null = null
  private hasUpgradedEditors = false
  private upgradePromise: Promise<void> | null = null

  constructor(
    private editorContainer: HTMLElement,
    private resultsContainer: HTMLElement,
    defaultSql: string
  ) {
    // Initialize with fallback editors
    this.editor = createFallbackEditor(editorContainer, {
      value: defaultSql,
      placeholder: 'Enter SQL query...',
    })

    this.resultsEditor = createFallbackEditor(resultsContainer, {
      value: 'Query results will appear here\nPress Ctrl/Cmd + Enter to run queries',
      readOnly: true,
    })
  }

  /**
   * Get the SQL editor instance
   */
  getEditor(): Editor {
    return this.editor
  }

  /**
   * Get the results editor instance
   */
  getResultsEditor(): Editor {
    return this.resultsEditor
  }

  /**
   * Check if editors have been upgraded to Monaco
   */
  isUpgraded(): boolean {
    return this.hasUpgradedEditors
  }

  /**
   * Upgrade both editors to Monaco (lazy loaded on first interaction)
   */
  async upgradeToMonaco(getTableNames: () => string[], executeHandler: () => void): Promise<void> {
    // Return existing promise if upgrade already in progress
    if (this.upgradePromise) return this.upgradePromise

    // Return immediately if already upgraded
    if (this.hasUpgradedEditors) return

    // Create and track the upgrade promise to prevent concurrent upgrades
    this.upgradePromise = (async () => {
      this.hasUpgradedEditors = true

      console.log('[EditorManager] Upgrading to Monaco editors...')

      // Upgrade SQL editor
      const sqlEditor = await this.upgradeEditor(
        this.editorContainer,
        this.editor as FallbackEditor,
        {
          language: 'sql',
          theme: 'vs-dark',
        },
        {
          onDidChangeModelContent: (monaco, monacoEditor) => {
            this.applyValidationMarkers(monaco, monacoEditor)
          },
          onShortcuts: (monaco, monacoEditor) => {
            this.registerShortcuts(monaco, monacoEditor, executeHandler)
          },
        }
      )

      // Upgrade results editor
      const resultsMonacoEditor = await this.upgradeEditor(
        this.resultsContainer,
        this.resultsEditor as FallbackEditor,
        {
          language: 'plaintext',
          theme: 'vs-dark',
          readOnly: true,
          lineNumbers: 'off',
        },
        {}
      )

      // Update references
      this.editor = sqlEditor
      this.resultsEditor = resultsMonacoEditor

      // Get Monaco instance
      this.monacoInstance = await loadMonaco()

      // Register completions with Monaco
      this.registerCompletions(this.monacoInstance, getTableNames)

      // Apply initial validation
      this.applyValidationMarkers(this.monacoInstance, sqlEditor)
    })()

    return this.upgradePromise
  }

  /**
   * Register upgrade listeners on editor container
   */
  registerUpgradeListeners(upgradeCallback: () => Promise<void>): void {
    // Add focus listener to upgrade editors
    this.editorContainer?.addEventListener(
      'focus',
      () => {
        void upgradeCallback()
      },
      { capture: true, once: true }
    )

    // Also upgrade on click
    this.editorContainer?.addEventListener(
      'click',
      () => {
        void upgradeCallback()
      },
      { once: true }
    )
  }

  /**
   * Update theme for Monaco editors
   */
  applyTheme(mode: 'light' | 'dark'): void {
    if (!this.monacoInstance) return
    const targetTheme = mode === 'dark' ? 'vs-dark' : 'vs'
    this.monacoInstance.editor?.setTheme?.(targetTheme)
  }

  /**
   * Upgrade a single editor from fallback to Monaco
   */
  private async upgradeEditor(
    container: HTMLElement,
    fallbackEditor: FallbackEditor,
    options: {
      value?: string
      language?: string
      theme?: string
      readOnly?: boolean
      lineNumbers?: 'on' | 'off'
    },
    callbacks: {
      onDidChangeModelContent?: (monaco: Monaco, editor: MonacoEditor) => void
      onShortcuts?: (monaco: Monaco, editor: MonacoEditor) => void
    }
  ): Promise<MonacoEditor> {
    console.log('[Editor Upgrade] Starting Monaco load...')

    // Get current value from fallback editor
    const currentValue = fallbackEditor.getValue()

    // Load Monaco
    const monaco = await loadMonaco()

    console.log('[Editor Upgrade] Creating Monaco editor...')

    // Destroy fallback editor
    fallbackEditor.destroy()

    // Clear container
    container.innerHTML = ''

    // Create Monaco editor
    const editor = monaco.editor.create(container, {
      value: currentValue || options.value || '',
      language: options.language || 'sql',
      theme: options.theme || 'vs-dark',
      readOnly: options.readOnly || false,
      minimap: { enabled: false },
      automaticLayout: true,
      fontFamily:
        'JetBrains Mono, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
      fontSize: 14,
      smoothScrolling: true,
      scrollBeyondLastLine: false,
      lineNumbers: options.lineNumbers || 'on',
    })

    // Apply current theme
    const isDark = document.documentElement.classList.contains('dark')
    monaco.editor.setTheme(isDark ? 'vs-dark' : 'vs')

    // Register callbacks
    if (callbacks.onDidChangeModelContent) {
      editor.onDidChangeModelContent(() => {
        callbacks.onDidChangeModelContent!(monaco, editor)
      })
    }

    if (callbacks.onShortcuts) {
      callbacks.onShortcuts(monaco, editor)
    }

    console.log('[Editor Upgrade] Monaco editor ready!')

    return editor
  }

  /**
   * Apply SQL validation markers to Monaco editor
   */
  private applyValidationMarkers(monaco: Monaco, editor: MonacoEditor): void {
    const model = editor.getModel()
    if (!model || !monaco.editor) return

    const sql = editor.getValue()
    const issues = validateSql(sql)
    const markers = issues.map(issue => {
      const start = model.getPositionAt(issue.offset)
      const end = model.getPositionAt(issue.offset + Math.max(issue.length, 1))

      return {
        severity: monaco.MarkerSeverity?.Error ?? 8,
        message: issue.message,
        startLineNumber: start.lineNumber,
        startColumn: start.column,
        endLineNumber: end.lineNumber,
        endColumn: end.column,
      }
    })

    monaco.editor.setModelMarkers(model, 'sql-validation', markers)
  }

  /**
   * Register SQL keyword and table name completions
   */
  private registerCompletions(monaco: Monaco, getTableNames: () => string[]): void {
    if (!monaco.languages?.registerCompletionItemProvider) return

    monaco.languages.registerCompletionItemProvider('sql', {
      triggerCharacters: [' ', '.', '\n'],
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      provideCompletionItems(model: any, position: any) {
        const wordInfo = model.getWordUntilPosition(position)
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: wordInfo.startColumn,
          endColumn: wordInfo.endColumn,
        }

        const keywordSuggestions = SQL_KEYWORDS.filter(word =>
          word.toLowerCase().startsWith(wordInfo.word.toLowerCase())
        ).map(label => ({
          label,
          kind: monaco.languages.CompletionItemKind?.Keyword ?? 14,
          insertText: label,
          range,
        }))

        const tableSuggestions = getTableNames()
          .filter(table => table.toLowerCase().startsWith(wordInfo.word.toLowerCase()))
          .map(label => ({
            label,
            kind: monaco.languages.CompletionItemKind?.Field ?? 4,
            insertText: label,
            range,
          }))

        return {
          suggestions: [...keywordSuggestions, ...tableSuggestions],
        }
      },
    })
  }

  /**
   * Register keyboard shortcuts (Ctrl+Enter, Ctrl+/, Tab)
   */
  private registerShortcuts(monaco: Monaco, editor: MonacoEditor, execute: () => void): void {
    const KeyMod = monaco.KeyMod
    const KeyCode = monaco.KeyCode
    const ctrlEnter = KeyMod.CtrlCmd | KeyCode.Enter
    const ctrlSlash = KeyMod.CtrlCmd | KeyCode.Slash

    editor.addCommand(ctrlEnter, execute)
    editor.addCommand(ctrlSlash, () => {
      const action = editor.getAction('editor.action.commentLine')
      if (action) {
        action.run()
      }
    })

    editor.addCommand(KeyCode.Tab, () => {
      editor.trigger('keyboard', 'editor.action.indentLines', null)
    })
  }
}
