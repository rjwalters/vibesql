/**
 * VibeSql Storage Client
 *
 * Provides blob storage operations through HTTP API
 */

import {
  StorageClient,
  StorageMetadata,
  UploadOptions,
  StorageError,
  BlobNotFoundError,
  UploadError,
} from './types';

/**
 * Configuration for the storage client
 */
export interface StorageClientConfig {
  /**
   * Base URL for the storage API
   * Default: '' (relative to current host)
   */
  baseUrl?: string;

  /**
   * Custom fetch implementation (for Node.js or testing)
   */
  fetch?: typeof globalThis.fetch;

  /**
   * Default headers to include with requests
   */
  headers?: Record<string, string>;
}

/**
 * HTTP-based storage client implementation
 */
export class HttpStorageClient implements StorageClient {
  private baseUrl: string;
  private fetch: typeof globalThis.fetch;
  private headers: Record<string, string>;

  constructor(config: StorageClientConfig = {}) {
    this.baseUrl = config.baseUrl ?? '';
    this.fetch = config.fetch ?? globalThis.fetch.bind(globalThis);
    this.headers = config.headers ?? {};
  }

  /**
   * Upload a blob to storage
   */
  async upload(
    data: File | Blob | ArrayBuffer | string,
    options: UploadOptions = {}
  ): Promise<string> {
    // Convert data to Blob for consistent handling
    const blob = this.toBlob(data, options.contentType);
    const contentType = options.contentType ?? (blob.type || 'application/octet-stream');

    // Create form data for multipart upload
    const formData = new FormData();
    formData.append('file', blob);

    if (options.contentType) {
      formData.append('content_type', contentType);
    }

    if (options.metadata) {
      formData.append('metadata', JSON.stringify(options.metadata));
    }

    try {
      // Use XMLHttpRequest for progress tracking if callback provided
      if (options.onProgress) {
        return await this.uploadWithProgress(formData, options.onProgress);
      }

      const response = await this.fetch(`${this.baseUrl}/api/storage/blobs`, {
        method: 'POST',
        headers: {
          ...this.headers,
        },
        body: formData,
      });

      if (!response.ok) {
        const error = await this.parseErrorResponse(response);
        throw new UploadError(error);
      }

      const result = await response.json();
      return result.id;
    } catch (error) {
      if (error instanceof StorageError) {
        throw error;
      }
      throw new UploadError(
        error instanceof Error ? error.message : String(error)
      );
    }
  }

  /**
   * Download a blob from storage
   */
  async download(id: string): Promise<Blob> {
    const response = await this.fetch(
      `${this.baseUrl}/api/storage/blobs/${encodeURIComponent(id)}`,
      {
        method: 'GET',
        headers: {
          ...this.headers,
        },
      }
    );

    if (!response.ok) {
      if (response.status === 404) {
        throw new BlobNotFoundError(id);
      }
      const error = await this.parseErrorResponse(response);
      throw new StorageError(`Download failed: ${error}`);
    }

    return response.blob();
  }

  /**
   * Get the URL for accessing a blob
   */
  getUrl(id: string): string {
    // Extract path segments from UUID for hierarchical URL
    // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    const cleanId = id.replace(/-/g, '');
    if (cleanId.length >= 4) {
      const dir1 = cleanId.slice(0, 2);
      const dir2 = cleanId.slice(2, 4);
      return `${this.baseUrl}/storage/blobs/${dir1}/${dir2}/${id}`;
    }
    return `${this.baseUrl}/storage/blobs/${id}`;
  }

  /**
   * Get metadata for a blob
   */
  async getMetadata(id: string): Promise<StorageMetadata> {
    const response = await this.fetch(
      `${this.baseUrl}/api/storage/blobs/${encodeURIComponent(id)}/metadata`,
      {
        method: 'GET',
        headers: {
          ...this.headers,
          Accept: 'application/json',
        },
      }
    );

    if (!response.ok) {
      if (response.status === 404) {
        throw new BlobNotFoundError(id);
      }
      const error = await this.parseErrorResponse(response);
      throw new StorageError(`Failed to get metadata: ${error}`);
    }

    const result = await response.json();

    return {
      id: result.id,
      size: result.size,
      contentType: result.content_type,
      createdAt: new Date(result.created_at),
      metadata: result.metadata,
    };
  }

  /**
   * Delete a blob from storage
   */
  async delete(id: string): Promise<void> {
    const response = await this.fetch(
      `${this.baseUrl}/api/storage/blobs/${encodeURIComponent(id)}`,
      {
        method: 'DELETE',
        headers: {
          ...this.headers,
        },
      }
    );

    if (!response.ok) {
      if (response.status === 404) {
        throw new BlobNotFoundError(id);
      }
      const error = await this.parseErrorResponse(response);
      throw new StorageError(`Delete failed: ${error}`);
    }
  }

  /**
   * Convert various input types to Blob
   */
  private toBlob(
    data: File | Blob | ArrayBuffer | string,
    contentType?: string
  ): Blob {
    if (data instanceof Blob) {
      // File extends Blob, so this handles both
      return data;
    }

    if (data instanceof ArrayBuffer) {
      return new Blob([data], {
        type: contentType ?? 'application/octet-stream',
      });
    }

    if (typeof data === 'string') {
      return new Blob([data], { type: contentType ?? 'text/plain' });
    }

    throw new UploadError('Unsupported data type');
  }

  /**
   * Upload with progress tracking using XMLHttpRequest
   */
  private uploadWithProgress(
    formData: FormData,
    onProgress: (progress: number) => void
  ): Promise<string> {
    return new Promise((resolve, reject) => {
      // Check if we're in a browser environment with XMLHttpRequest
      if (typeof XMLHttpRequest === 'undefined') {
        // Fall back to regular fetch without progress in Node.js
        this.fetch(`${this.baseUrl}/api/storage/blobs`, {
          method: 'POST',
          headers: this.headers,
          body: formData,
        })
          .then(async response => {
            if (!response.ok) {
              const error = await this.parseErrorResponse(response);
              reject(new UploadError(error));
              return;
            }
            const result = await response.json();
            resolve(result.id);
          })
          .catch(error => {
            reject(
              new UploadError(
                error instanceof Error ? error.message : String(error)
              )
            );
          });
        return;
      }

      const xhr = new XMLHttpRequest();
      xhr.open('POST', `${this.baseUrl}/api/storage/blobs`);

      // Set custom headers
      Object.entries(this.headers).forEach(([key, value]) => {
        xhr.setRequestHeader(key, value);
      });

      xhr.upload.addEventListener('progress', event => {
        if (event.lengthComputable) {
          const progress = Math.round((event.loaded / event.total) * 100);
          onProgress(progress);
        }
      });

      xhr.addEventListener('load', () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          try {
            const result = JSON.parse(xhr.responseText);
            resolve(result.id);
          } catch {
            reject(new UploadError('Invalid response format'));
          }
        } else {
          reject(new UploadError(`HTTP ${xhr.status}: ${xhr.statusText}`));
        }
      });

      xhr.addEventListener('error', () => {
        reject(new UploadError('Network error'));
      });

      xhr.addEventListener('abort', () => {
        reject(new UploadError('Upload aborted'));
      });

      xhr.send(formData);
    });
  }

  /**
   * Parse error response from server
   */
  private async parseErrorResponse(response: Response): Promise<string> {
    try {
      const contentType = response.headers.get('content-type');
      if (contentType?.includes('application/json')) {
        const json = await response.json();
        return json.error || json.message || `HTTP ${response.status}`;
      }
      return await response.text() || `HTTP ${response.status}`;
    } catch {
      return `HTTP ${response.status}: ${response.statusText}`;
    }
  }
}
