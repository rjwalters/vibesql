/**
 * Footer Component
 *
 * Reusable footer with optional build timestamp display
 */

import { setBuildTimestamp } from '../utils/build-timestamp';

export class FooterComponent {
  constructor(showTimestamp: boolean = true) {
    if (showTimestamp) {
      // Wait for DOM to be ready
      if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => {
          setBuildTimestamp();
        });
      } else {
        setBuildTimestamp();
      }
    }
  }

  /**
   * Inject footer HTML into a container
   * Useful if footer needs to be dynamically rendered
   */
  static render(showTimestamp: boolean = true): HTMLElement {
    const footer = document.createElement('footer');
    footer.className = 'mt-12 text-center text-sm text-gray-500 dark:text-gray-400 pb-6' +
      (showTimestamp ? ' space-y-1' : '');

    const tagline = document.createElement('p');
    tagline.setAttribute('data-i18n', 'footer-tagline');
    tagline.textContent = 'VibeSQL - SQL:1999 Database in WebAssembly';
    footer.appendChild(tagline);

    if (showTimestamp) {
      const timestamp = document.createElement('p');
      timestamp.id = 'build-timestamp';
      timestamp.className = 'text-xs text-gray-400 dark:text-gray-500';
      footer.appendChild(timestamp);
    }

    return footer;
  }
}
