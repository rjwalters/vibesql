/**
 * CommitLink - A utility for rendering GitHub commit links
 *
 * Creates clickable links to GitHub commits with consistent styling.
 * Supports both full and truncated commit hash display.
 */

const GITHUB_REPO = 'https://github.com/rjwalters/vibesql'

export interface CommitLinkOptions {
  /** Whether to truncate the commit hash to 7 characters (default: true) */
  truncate?: boolean
  /** Additional CSS classes to apply */
  className?: string
  /** Whether to show as a block element (default: false, inline) */
  block?: boolean
}

/**
 * Generate HTML for a GitHub commit link
 *
 * @param commit - The full commit hash
 * @param options - Display options
 * @returns HTML string for the commit link
 *
 * @example
 * // Truncated inline link (default)
 * commitLink('abc123def456...') // => <a href="...">abc123d</a>
 *
 * // Full hash display
 * commitLink('abc123def456...', { truncate: false })
 *
 * // With custom classes
 * commitLink('abc123def456...', { className: 'text-sm' })
 */
export function commitLink(commit: string, options: CommitLinkOptions = {}): string {
  const { truncate = true, className = '', block = false } = options

  if (!commit) {
    return ''
  }

  const displayHash = truncate ? commit.slice(0, 7) : commit
  const href = `${GITHUB_REPO}/commit/${commit}`

  const baseClasses = 'font-mono text-blue-600 dark:text-blue-400 hover:underline'
  const blockClasses = block ? 'block' : ''
  const allClasses = [baseClasses, blockClasses, className].filter(Boolean).join(' ')

  return `<a href="${href}" target="_blank" rel="noopener noreferrer" class="${allClasses}" title="View commit ${commit}">${displayHash}</a>`
}

/**
 * Generate HTML for a GitHub PR link
 *
 * @param prNumber - The PR number
 * @param options - Display options
 * @returns HTML string for the PR link
 */
export function prLink(
  prNumber: number | string,
  options: { className?: string } = {}
): string {
  const { className = '' } = options

  if (!prNumber) {
    return ''
  }

  const href = `${GITHUB_REPO}/pull/${prNumber}`
  const baseClasses = 'text-blue-600 dark:text-blue-400 hover:underline'
  const allClasses = [baseClasses, className].filter(Boolean).join(' ')

  return `<a href="${href}" target="_blank" rel="noopener noreferrer" class="${allClasses}">PR #${prNumber}</a>`
}

/**
 * Create a commit link element (for programmatic DOM manipulation)
 *
 * @param commit - The full commit hash
 * @param options - Display options
 * @returns HTMLAnchorElement
 */
export function createCommitLinkElement(
  commit: string,
  options: CommitLinkOptions = {}
): HTMLAnchorElement {
  const { truncate = true, className = '' } = options

  const a = document.createElement('a')
  a.href = `${GITHUB_REPO}/commit/${commit}`
  a.target = '_blank'
  a.rel = 'noopener noreferrer'
  a.textContent = truncate ? commit.slice(0, 7) : commit
  a.title = `View commit ${commit}`
  a.className = `font-mono text-blue-600 dark:text-blue-400 hover:underline ${className}`.trim()

  return a
}

/**
 * Replace a text element's content with a commit link
 *
 * @param element - The element to update
 * @param commit - The commit hash
 * @param options - Display options
 */
export function setCommitLink(
  element: HTMLElement | null,
  commit: string,
  options: CommitLinkOptions = {}
): void {
  if (!element || !commit) return

  element.innerHTML = commitLink(commit, options)
}
