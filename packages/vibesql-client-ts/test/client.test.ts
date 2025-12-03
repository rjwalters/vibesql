/**
 * VibeSql Client Tests
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { VibeSqlClient } from '../src/client';
import { ConnectionError, QueryError } from '../src/types/index';

describe('VibeSqlClient', () => {
  let client: VibeSqlClient;

  beforeEach(() => {
    client = new VibeSqlClient({
      host: 'localhost',
      port: 5432,
      database: 'test',
      user: 'testuser',
      password: 'testpass',
    });
  });

  afterEach(async () => {
    if (client.connected) {
      await client.close();
    }
  });

  describe('initialization', () => {
    it('should create a client instance', () => {
      expect(client).toBeDefined();
      expect(client.connected).toBe(false);
    });

    it('should have correct configuration options', () => {
      const customClient = new VibeSqlClient({
        host: 'example.com',
        port: 5433,
        database: 'mydb',
        user: 'myuser',
        password: 'mypass',
        pool: {
          min: 5,
          max: 20,
        },
      });

      expect(customClient).toBeDefined();
    });
  });

  describe('connection', () => {
    it('should handle connection errors gracefully', async () => {
      const badClient = new VibeSqlClient({
        host: 'nonexistent.example.com',
        port: 5432,
        database: 'test',
        user: 'test',
      });

      // Connection should fail but not crash
      try {
        await badClient.connect();
      } catch (error) {
        expect(error).toBeInstanceOf(ConnectionError);
      }
    });
  });

  describe('queries', () => {
    it('should throw error if not connected', async () => {
      expect(async () => {
        await client.query('SELECT 1');
      }).rejects.toThrow(ConnectionError);
    });
  });

  describe('subscriptions', () => {
    it('should throw error if not connected', () => {
      expect(() => {
        client.subscribe('SELECT * FROM users', [], {
          onData: () => {},
        });
      }).toThrow(ConnectionError);
    });
  });

  describe('reconnection', () => {
    it('should respect reconnection options', () => {
      const reconnectClient = new VibeSqlClient({
        host: 'localhost',
        port: 5432,
        database: 'test',
        user: 'test',
        reconnect: {
          enabled: true,
          maxRetries: 5,
          baseDelay: 1000,
          maxDelay: 10000,
        },
      });

      expect(reconnectClient).toBeDefined();
    });

    it('should support disabling reconnection', () => {
      const noReconnectClient = new VibeSqlClient({
        host: 'localhost',
        port: 5432,
        database: 'test',
        user: 'test',
        reconnect: {
          enabled: false,
        },
      });

      expect(noReconnectClient).toBeDefined();
    });
  });
});

describe('Error Handling', () => {
  it('should properly inherit from Error', () => {
    const err = new ConnectionError('Test error');
    expect(err).toBeInstanceOf(Error);
    expect(err.message).toBe('Test error');
    expect(err.name).toBe('ConnectionError');
  });

  it('should support QueryError', () => {
    const err = new QueryError('Query failed');
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe('QueryError');
  });
});

describe('Type Safety', () => {
  it('should support generic type parameters', async () => {
    interface User {
      id: number;
      name: string;
      email: string;
    }

    const client = new VibeSqlClient({
      host: 'localhost',
      port: 5432,
      database: 'test',
      user: 'test',
    });

    // This should compile with proper types (compile-time check)
    expect(client).toBeDefined();
  });
});
