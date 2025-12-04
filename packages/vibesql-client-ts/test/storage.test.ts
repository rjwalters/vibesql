/**
 * VibeSql Storage Client Tests
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  HttpStorageClient,
  StorageError,
  BlobNotFoundError,
  UploadError,
} from '../src/storage/index';
import type { StorageMetadata } from '../src/storage/index';

describe('HttpStorageClient', () => {
  let client: HttpStorageClient;
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockFetch = vi.fn();
    client = new HttpStorageClient({
      baseUrl: 'http://localhost:8080',
      fetch: mockFetch as unknown as typeof fetch,
    });
  });

  describe('upload', () => {
    it('should upload a string and return blob ID', async () => {
      const mockId = '550e8400-e29b-41d4-a716-446655440000';
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ id: mockId }),
      });

      const result = await client.upload('hello world');

      expect(result).toBe(mockId);
      expect(mockFetch).toHaveBeenCalledTimes(1);
      expect(mockFetch).toHaveBeenCalledWith(
        'http://localhost:8080/api/storage/blobs',
        expect.objectContaining({
          method: 'POST',
        })
      );
    });

    it('should upload a Blob with content type', async () => {
      const mockId = '550e8400-e29b-41d4-a716-446655440000';
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ id: mockId }),
      });

      const blob = new Blob(['test data'], { type: 'text/plain' });
      const result = await client.upload(blob, {
        contentType: 'text/plain',
      });

      expect(result).toBe(mockId);
    });

    it('should upload with custom metadata', async () => {
      const mockId = '550e8400-e29b-41d4-a716-446655440000';
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ id: mockId }),
      });

      const result = await client.upload('test', {
        metadata: { originalName: 'test.txt' },
      });

      expect(result).toBe(mockId);
    });

    it('should throw UploadError on server error', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        headers: new Headers({ 'content-type': 'application/json' }),
        json: async () => ({ error: 'Server error' }),
      });

      await expect(client.upload('test')).rejects.toThrow(UploadError);
    });

    it('should throw UploadError on network failure', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Network error'));

      await expect(client.upload('test')).rejects.toThrow(UploadError);
    });
  });

  describe('download', () => {
    it('should download blob by ID', async () => {
      const mockBlob = new Blob(['test data']);
      mockFetch.mockResolvedValueOnce({
        ok: true,
        blob: async () => mockBlob,
      });

      const result = await client.download(
        '550e8400-e29b-41d4-a716-446655440000'
      );

      expect(result).toBe(mockBlob);
      expect(mockFetch).toHaveBeenCalledWith(
        'http://localhost:8080/api/storage/blobs/550e8400-e29b-41d4-a716-446655440000',
        expect.objectContaining({
          method: 'GET',
        })
      );
    });

    it('should throw BlobNotFoundError for 404', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
      });

      await expect(
        client.download('550e8400-e29b-41d4-a716-446655440000')
      ).rejects.toThrow(BlobNotFoundError);
    });

    it('should throw StorageError on other errors', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        headers: new Headers({ 'content-type': 'text/plain' }),
        text: async () => 'Server error',
      });

      await expect(
        client.download('550e8400-e29b-41d4-a716-446655440000')
      ).rejects.toThrow(StorageError);
    });
  });

  describe('getUrl', () => {
    it('should generate URL with hierarchical path', () => {
      const url = client.getUrl('550e8400-e29b-41d4-a716-446655440000');

      expect(url).toBe(
        'http://localhost:8080/storage/blobs/55/0e/550e8400-e29b-41d4-a716-446655440000'
      );
    });

    it('should handle IDs without hyphens', () => {
      const url = client.getUrl('550e8400e29b41d4a716446655440000');

      expect(url).toBe(
        'http://localhost:8080/storage/blobs/55/0e/550e8400e29b41d4a716446655440000'
      );
    });

    it('should handle short IDs gracefully', () => {
      const url = client.getUrl('abc');

      expect(url).toBe('http://localhost:8080/storage/blobs/abc');
    });
  });

  describe('getMetadata', () => {
    it('should fetch and parse metadata', async () => {
      const mockMetadata = {
        id: '550e8400-e29b-41d4-a716-446655440000',
        size: 1024,
        content_type: 'text/plain',
        created_at: '2024-01-01T00:00:00Z',
        metadata: { custom: 'value' },
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => mockMetadata,
      });

      const result = await client.getMetadata(
        '550e8400-e29b-41d4-a716-446655440000'
      );

      expect(result).toEqual({
        id: '550e8400-e29b-41d4-a716-446655440000',
        size: 1024,
        contentType: 'text/plain',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        metadata: { custom: 'value' },
      });
    });

    it('should throw BlobNotFoundError for 404', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
      });

      await expect(
        client.getMetadata('550e8400-e29b-41d4-a716-446655440000')
      ).rejects.toThrow(BlobNotFoundError);
    });
  });

  describe('delete', () => {
    it('should delete blob by ID', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
      });

      await client.delete('550e8400-e29b-41d4-a716-446655440000');

      expect(mockFetch).toHaveBeenCalledWith(
        'http://localhost:8080/api/storage/blobs/550e8400-e29b-41d4-a716-446655440000',
        expect.objectContaining({
          method: 'DELETE',
        })
      );
    });

    it('should throw BlobNotFoundError for 404', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
      });

      await expect(
        client.delete('550e8400-e29b-41d4-a716-446655440000')
      ).rejects.toThrow(BlobNotFoundError);
    });
  });

  describe('configuration', () => {
    it('should use empty base URL by default', () => {
      const defaultClient = new HttpStorageClient();
      const url = defaultClient.getUrl(
        '550e8400-e29b-41d4-a716-446655440000'
      );

      expect(url).toBe(
        '/storage/blobs/55/0e/550e8400-e29b-41d4-a716-446655440000'
      );
    });

    it('should include custom headers in requests', async () => {
      const clientWithHeaders = new HttpStorageClient({
        baseUrl: 'http://localhost:8080',
        fetch: mockFetch as unknown as typeof fetch,
        headers: {
          Authorization: 'Bearer token123',
        },
      });

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ id: 'test-id' }),
      });

      await clientWithHeaders.upload('test');

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: 'Bearer token123',
          }),
        })
      );
    });
  });
});

describe('Storage Error Classes', () => {
  it('StorageError should extend Error', () => {
    const err = new StorageError('Test error');
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe('StorageError');
    expect(err.message).toBe('Test error');
  });

  it('BlobNotFoundError should extend StorageError', () => {
    const err = new BlobNotFoundError('test-id');
    expect(err).toBeInstanceOf(StorageError);
    expect(err.name).toBe('BlobNotFoundError');
    expect(err.message).toBe('Blob not found: test-id');
  });

  it('UploadError should extend StorageError', () => {
    const err = new UploadError('network failure');
    expect(err).toBeInstanceOf(StorageError);
    expect(err.name).toBe('UploadError');
    expect(err.message).toBe('Upload failed: network failure');
  });
});

describe('VibeSqlClient storage integration', () => {
  it('should expose storage client via db.storage', async () => {
    // Import the client after setting up mocks
    const { VibeSqlClient } = await import('../src/client');

    const db = new VibeSqlClient({
      host: 'localhost',
      port: 5432,
      database: 'test',
      user: 'test',
      httpPort: 8080,
    });

    expect(db.storage).toBeDefined();
    expect(typeof db.storage.upload).toBe('function');
    expect(typeof db.storage.download).toBe('function');
    expect(typeof db.storage.getUrl).toBe('function');
    expect(typeof db.storage.getMetadata).toBe('function');
    expect(typeof db.storage.delete).toBe('function');
  });

  it('should generate correct URL from options', async () => {
    const { VibeSqlClient } = await import('../src/client');

    const db = new VibeSqlClient({
      host: 'example.com',
      port: 5432,
      database: 'test',
      user: 'test',
      httpPort: 3000,
    });

    const url = db.storage.getUrl('550e8400-e29b-41d4-a716-446655440000');
    expect(url).toContain('example.com:3000');
  });

  it('should use custom storage base URL when provided', async () => {
    const { VibeSqlClient } = await import('../src/client');

    const db = new VibeSqlClient({
      host: 'localhost',
      port: 5432,
      database: 'test',
      user: 'test',
      storage: {
        baseUrl: 'https://storage.example.com',
      },
    });

    const url = db.storage.getUrl('550e8400-e29b-41d4-a716-446655440000');
    expect(url).toContain('storage.example.com');
  });
});
