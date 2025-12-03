import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { VibeSQL } from '../src/client.js';
import { SubscriptionState, ConnectionState } from '../src/types.js';

describe('VibeSQL Client', () => {
  let client: VibeSQL;

  beforeEach(() => {
    client = new VibeSQL({
      host: 'localhost',
      port: 5432,
      maxReconnectAttempts: 0, // Disable reconnection for tests
    });
  });

  afterEach(async () => {
    await client.disconnect();
  });

  describe('Constructor', () => {
    it('should create client with default config', () => {
      const c = new VibeSQL();
      expect(c).toBeDefined();
    });

    it('should create client with custom config', () => {
      const c = new VibeSQL({
        host: '192.168.1.1',
        port: 3306,
        database: 'test_db',
      });
      expect(c).toBeDefined();
    });
  });

  describe('Connection', () => {
    it('should throw error when querying while disconnected', async () => {
      await expect(client.query('SELECT 1')).rejects.toThrow(
        'Not connected to server'
      );
    });

    it('should throw error when subscribing while disconnected', () => {
      expect(() => client.subscribe('SELECT 1')).toThrow(
        'Not connected to server'
      );
    });
  });

  describe('Subscriptions', () => {
    it('should throw error when subscribing while disconnected', () => {
      expect(() => {
        client.subscribe('SELECT * FROM users');
      }).toThrow('Not connected to server');
    });
  });
});

describe('Subscription', () => {
  it('should create subscription with correct state', () => {
    const { Subscription } = require('../src/subscription.js');
    const sub = new Subscription('sub-1', 'SELECT * FROM users');

    expect(sub.getId()).toBe('sub-1');
    expect(sub.getSql()).toBe('SELECT * FROM users');
    expect(sub.getState()).toBe(SubscriptionState.IDLE);
  });

  it('should support chaining listeners', () => {
    const { Subscription } = require('../src/subscription.js');
    const sub = new Subscription('sub-1', 'SELECT * FROM users');

    const dataHandler = vi.fn();
    const errorHandler = vi.fn();

    sub.onData(dataHandler).onError(errorHandler);

    expect(sub).toBeDefined();
  });

  it('should emit data events', () => {
    const { Subscription } = require('../src/subscription.js');
    const sub = new Subscription('sub-1', 'SELECT * FROM users');

    const handler = vi.fn();
    sub.on('data', handler);

    sub._emitData({
      subscriptionId: 'sub-1',
      columns: ['id', 'name'],
      rows: [{ id: 1, name: 'John' }],
      operation: 'insert',
      timestamp: Date.now(),
    });

    expect(handler).toHaveBeenCalled();
  });

  it('should unsubscribe and remove listeners', () => {
    const { Subscription } = require('../src/subscription.js');
    const sub = new Subscription('sub-1', 'SELECT * FROM users');

    const handler = vi.fn();
    sub.on('data', handler);

    sub.unsubscribe();

    expect(sub.getState()).toBe(SubscriptionState.UNSUBSCRIBED);
  });
});

describe('SubscriptionManager', () => {
  it('should create and retrieve subscriptions', () => {
    const { SubscriptionManager } = require('../src/subscription.js');
    const manager = new SubscriptionManager();

    const sub = manager.create('sub-1', 'SELECT * FROM users');

    expect(manager.get('sub-1')).toBe(sub);
    expect(manager.has('sub-1')).toBe(true);
  });

  it('should remove subscriptions', () => {
    const { SubscriptionManager } = require('../src/subscription.js');
    const manager = new SubscriptionManager();

    manager.create('sub-1', 'SELECT * FROM users');
    const removed = manager.remove('sub-1');

    expect(removed).toBe(true);
    expect(manager.has('sub-1')).toBe(false);
  });

  it('should clear all subscriptions', () => {
    const { SubscriptionManager } = require('../src/subscription.js');
    const manager = new SubscriptionManager();

    manager.create('sub-1', 'SELECT * FROM users');
    manager.create('sub-2', 'SELECT * FROM posts');

    manager.clear();

    expect(manager.getAll()).toHaveLength(0);
  });
});
