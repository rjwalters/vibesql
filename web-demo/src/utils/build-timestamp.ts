/**
 * Build timestamp utility for displaying deployment info in footer
 */

declare const __BUILD_TIMESTAMP__: string;

export function setBuildTimestamp(): void {
  const timestampElement = document.getElementById('build-timestamp');
  if (timestampElement) {
    try {
      const timestamp = __BUILD_TIMESTAMP__;
      const date = new Date(timestamp);
      const formattedDate = date.toLocaleString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        timeZoneName: 'short',
      });
      timestampElement.textContent = `Deployed: ${formattedDate}`;
    } catch (error) {
      console.warn('Failed to set build timestamp', error);
      timestampElement.textContent = 'Deployed: Development build';
    }
  }
}
