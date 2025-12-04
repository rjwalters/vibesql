/**
 * VibeSql Storage - Type Definitions
 *
 * Types for blob storage operations
 */

/**
 * Options for uploading a blob
 */
export interface UploadOptions {
  /**
   * MIME type of the content (e.g., "image/png", "application/json")
   * If not provided, will be inferred from File or Blob type
   */
  contentType?: string;

  /**
   * Custom metadata to store with the blob
   */
  metadata?: Record<string, unknown>;

  /**
   * Progress callback for tracking upload progress
   * @param progress - Progress percentage (0-100)
   */
  onProgress?: (progress: number) => void;
}

/**
 * Metadata for a stored blob
 */
export interface StorageMetadata {
  /**
   * Unique identifier for the blob (UUID)
   */
  id: string;

  /**
   * Size of the blob in bytes
   */
  size: number;

  /**
   * MIME type of the content
   */
  contentType: string;

  /**
   * When the blob was created
   */
  createdAt: Date;

  /**
   * Custom metadata stored with the blob
   */
  metadata?: Record<string, unknown>;
}

/**
 * Storage client interface
 */
export interface StorageClient {
  /**
   * Upload a blob to storage
   *
   * @param data - The data to upload (File, Blob, ArrayBuffer, or string)
   * @param options - Optional upload settings
   * @returns Promise resolving to the blob's unique ID
   *
   * @example
   * ```typescript
   * const file = new File(['hello'], 'hello.txt', { type: 'text/plain' });
   * const id = await storage.upload(file);
   * ```
   */
  upload(
    data: File | Blob | ArrayBuffer | string,
    options?: UploadOptions
  ): Promise<string>;

  /**
   * Download a blob from storage
   *
   * @param id - The blob's unique ID
   * @returns Promise resolving to the blob data
   *
   * @example
   * ```typescript
   * const blob = await storage.download(id);
   * const text = await blob.text();
   * ```
   */
  download(id: string): Promise<Blob>;

  /**
   * Get the URL for accessing a blob
   *
   * Note: This is a synchronous method that constructs the URL locally.
   * The URL can be used for direct access to the blob.
   *
   * @param id - The blob's unique ID
   * @returns The URL string for accessing the blob
   *
   * @example
   * ```typescript
   * const url = storage.getUrl(id);
   * // Returns: "/storage/blobs/abc12b3c-..."
   * ```
   */
  getUrl(id: string): string;

  /**
   * Get metadata for a blob
   *
   * @param id - The blob's unique ID
   * @returns Promise resolving to the blob's metadata
   *
   * @example
   * ```typescript
   * const meta = await storage.getMetadata(id);
   * console.log(`Size: ${meta.size} bytes`);
   * ```
   */
  getMetadata(id: string): Promise<StorageMetadata>;

  /**
   * Delete a blob from storage
   *
   * @param id - The blob's unique ID
   * @returns Promise resolving when the blob is deleted
   *
   * @example
   * ```typescript
   * await storage.delete(id);
   * ```
   */
  delete(id: string): Promise<void>;
}

/**
 * Storage-specific errors
 */
export class StorageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'StorageError';
  }
}

/**
 * Error thrown when a blob is not found
 */
export class BlobNotFoundError extends StorageError {
  constructor(id: string) {
    super(`Blob not found: ${id}`);
    this.name = 'BlobNotFoundError';
  }
}

/**
 * Error thrown when upload fails
 */
export class UploadError extends StorageError {
  constructor(message: string) {
    super(`Upload failed: ${message}`);
    this.name = 'UploadError';
  }
}
