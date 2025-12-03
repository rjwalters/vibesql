# @vibesql/client

TypeScript/JavaScript client library for VibeSql with real-time subscription support.

## Features

- 🔌 PostgreSQL wire protocol client
- ⚡ Real-time query subscriptions  
- 🔄 Automatic reconnection with subscription restoration
- 🏊 Connection pooling for high-throughput applications
- 📝 Full TypeScript support with type safety
- ⚛️ React hooks for easy integration (optional)
- 🧪 Comprehensive test coverage

## Installation

```bash
npm install @vibesql/client
```

## Quick Start

### Basic Query

```typescript
import { VibeSqlClient } from '@vibesql/client';

const db = new VibeSqlClient({
  host: 'localhost',
  port: 5432,
  database: 'mydb',
  user: 'myuser',
  password: 'mypassword',
});

await db.connect();

// Execute a query
const users = await db.query<User>('SELECT * FROM users WHERE id = $1', [42]);

await db.close();
```

### Real-Time Subscriptions

```typescript
// Subscribe to a query
const subscription = db.subscribe<Message>(
  'SELECT * FROM messages WHERE channel_id = $1 ORDER BY created_at DESC LIMIT 100',
  [channelId],
  {
    // Called with initial results and on every update
    onData: (messages) => {
      setMessages(messages);
    },

    // Called for incremental updates (optional)
    onDelta: (delta) => {
      if (delta.type === 'insert') {
        addMessage(delta.row);
      } else if (delta.type === 'delete') {
        removeMessage(delta.row);
      } else if (delta.type === 'update') {
        updateMessage(delta.oldRow, delta.newRow);
      }
    },

    // Called on error
    onError: (error) => {
      console.error('Subscription error:', error);
    },
  }
);

// Later: cleanup
subscription.unsubscribe();
```

### React Hooks

```typescript
import { useSubscription } from '@vibesql/client/react';

function ChatRoom({ channelId }: { channelId: string }) {
  const { data: messages, error, isLoading } = useSubscription<Message>(
    db,
    'SELECT * FROM messages WHERE channel_id = $1 ORDER BY created_at DESC LIMIT 50',
    [channelId]
  );

  if (isLoading) return <div>Loading...</div>;
  if (error) return <div>Error: {error.message}</div>;

  return (
    <div>
      {messages?.map(msg => (
        <MessageBubble key={msg.id} message={msg} />
      ))}
    </div>
  );
}
```

## Configuration

```typescript
interface VibeSqlClientOptions {
  // Connection settings
  host: string;
  port?: number;              // default: 5432
  database: string;
  user: string;
  password?: string;

  // Connection pool settings
  pool?: {
    min?: number;             // default: 1
    max?: number;             // default: 10
    idleTimeout?: number;     // ms, default: 30000
  };

  // Reconnection settings
  reconnect?: {
    enabled?: boolean;        // default: true
    maxRetries?: number;      // default: 10
    baseDelay?: number;       // ms, default: 1000
    maxDelay?: number;        // ms, default: 30000
  };

  // TLS settings
  ssl?: boolean | TlsOptions;
}
```

## API Reference

### VibeSqlClient

#### `connect(): Promise<void>`

Establishes a connection to the database.

#### `query<T>(sql: string, params?: any[]): Promise<T[]>`

Executes a SQL query and returns the results.

#### `subscribe<T>(sql: string, params: any[], callbacks: SubscriptionCallbacks<T>): Subscription`

Subscribes to a query for real-time updates.

#### `close(): Promise<void>`

Closes the database connection.

### Subscription

#### `unsubscribe(): void`

Stops the subscription and cleans up resources.

## Advanced Usage

### Connection Pooling

```typescript
const db = new VibeSqlClient({
  host: 'localhost',
  pool: {
    min: 5,
    max: 20,
    idleTimeout: 60000,
  },
});
```

### Reconnection Settings

```typescript
const db = new VibeSqlClient({
  host: 'localhost',
  reconnect: {
    enabled: true,
    maxRetries: 5,
    baseDelay: 2000,
    maxDelay: 60000,
  },
});
```

### Error Handling

```typescript
try {
  const result = await db.query('SELECT * FROM users');
} catch (error) {
  if (error instanceof ConnectionError) {
    console.error('Failed to connect');
  } else if (error instanceof QueryError) {
    console.error('Query failed:', error.message);
  }
}
```

## License

MIT OR Apache-2.0
