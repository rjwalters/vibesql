# VibeSQL

[![CI](https://github.com/rjwalters/vibesql/actions/workflows/ci-and-deploy.yml/badge.svg)](https://github.com/rjwalters/vibesql/actions/workflows/ci-and-deploy.yml)
[![Demo](https://img.shields.io/badge/demo-live-success)](https://rjwalters.github.io/vibesql/)
[![sqltest](https://img.shields.io/endpoint?url=https://rjwalters.github.io/vibesql/badges/sql1999-conformance.json)](https://rjwalters.github.io/vibesql/conformance.html)
[![SQLLogicTest](https://img.shields.io/endpoint?url=https://rjwalters.github.io/vibesql/badges/sqllogictest.json)](https://rjwalters.github.io/vibesql/conformance.html#SQLlogicTest)

**SQL:1999 compliant database in Rust, 100% AI-generated**

[Live Demo](https://rjwalters.github.io/vibesql/) | [CLI Guide](docs/CLI_GUIDE.md) | [Python Bindings](docs/PYTHON_BINDINGS.md) | [Conformance Report](https://rjwalters.github.io/vibesql/conformance.html)

## Highlights

- **100% SQL:1999 Core compliance** - 739/739 sqltest tests passing
- **100% SQLLogicTest conformance** - 624 files (~5.9M tests)
- **Real-time subscriptions** - Convex-like reactivity with delta updates
- **HTTP REST & GraphQL API** - Full CRUD and query endpoints
- **Vector search** - AI/ML embeddings with similarity search
- **File storage** - Blob storage with SQL integration
- **Full-featured CLI** with PostgreSQL-compatible commands
- **TypeScript SDK** with React hooks and Drizzle ORM adapter
- **Python bindings** with DB-API 2.0 interface
- **WebAssembly** - runs in the browser
- **215,000+ lines** of Rust across 11 crates

Built entirely by AI agents using [Claude Code](https://claude.com/claude-code) and [Loom](https://github.com/loomhq/loom).

## Quick Start

```bash
# Clone and build
git clone --recurse-submodules https://github.com/rjwalters/vibesql.git
cd vibesql
cargo build --release

# Run the CLI
cargo run --release --bin vibesql

# Or try the web demo
cd web-demo && npm install && npm run dev
```

### CLI Example

```bash
$ cargo run --release --bin vibesql
vibesql> CREATE TABLE users (id INTEGER, name VARCHAR(50));
vibesql> INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
vibesql> SELECT * FROM users;
+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
| 2  | Bob   |
+----+-------+
vibesql> \q
```

See [CLI Guide](docs/CLI_GUIDE.md) for meta-commands, output formats, and import/export.

### Python

```bash
pip install maturin
maturin develop
```

```python
import vibesql

db = vibesql.connect()
cursor = db.cursor()
cursor.execute("CREATE TABLE t (id INTEGER, name VARCHAR(50))")
cursor.execute("INSERT INTO t VALUES (1, 'Hello')")
cursor.execute("SELECT * FROM t")
print(cursor.fetchall())  # [(1, 'Hello')]
```

See [Python Bindings Guide](docs/PYTHON_BINDINGS.md) for full API reference.

## Features

### Real-Time Subscriptions

Subscribe to SQL queries and receive automatic updates when data changes—Convex-like reactivity with full SQL power.

```typescript
import { VibeSqlClient } from '@vibesql/client';

const db = new VibeSqlClient({ host: 'localhost', port: 5432 });
await db.connect();

// Subscribe to a query - get updates when data changes
const subscription = db.subscribe(
  'SELECT * FROM messages WHERE channel_id = $1 ORDER BY created_at DESC LIMIT 50',
  [channelId],
  {
    onData: (messages) => setMessages(messages),
    onDelta: (delta) => {
      // Efficient incremental updates
      if (delta.type === 'insert') addMessage(delta.row);
      if (delta.type === 'delete') removeMessage(delta.row);
    },
  }
);

// React hook for easy integration
function ChatRoom({ channelId }) {
  const { data, isLoading } = useSubscription(db,
    'SELECT * FROM messages WHERE channel_id = $1',
    [channelId]
  );
  return <MessageList messages={data} />;
}
```

**Features:**
- Delta updates (only changed rows sent)
- Automatic reconnection with subscription restoration
- Configurable limits, quotas, and backpressure handling
- HTTP SSE endpoint for REST API consumers
- React hooks (`useSubscription`, `useQuery`)

### SQL Support

- **Queries**: SELECT, JOINs (INNER/LEFT/RIGHT/FULL/CROSS), subqueries, CTEs, UNION/INTERSECT/EXCEPT
- **DML**: INSERT, UPDATE, DELETE, TRUNCATE
- **DDL**: CREATE/ALTER/DROP TABLE, views, indexes, schemas
- **Aggregates**: COUNT, SUM, AVG, MIN, MAX with GROUP BY/HAVING
- **Window functions**: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD
- **Transactions**: BEGIN, COMMIT, ROLLBACK, savepoints
- **Security**: GRANT/REVOKE with full privilege enforcement

### Advanced Features

- Views with OR REPLACE and column lists
- Stored procedures and functions (IN/OUT/INOUT parameters)
- Full-text search (MATCH AGAINST)
- Spatial functions (ST_* library)
- Triggers (BEFORE/AFTER)
- [Scheduled functions](docs/scheduled-functions.md) (SCHEDULE AFTER/AT, CREATE CRON)
- [Vector types](docs/vector-search.md) for AI embeddings (VECTOR(n), distance functions)
- [Blob storage](docs/file-storage.md) with STORAGE_URL/STORAGE_SIZE functions

### Performance

- Columnar execution with SIMD acceleration
- Cost-based join reordering
- Hash joins for equi-joins
- Predicate pushdown
- Expression caching

## Development

```bash
# Full build, test, and benchmark (runs in background)
make all            # Starts in background, shows monitoring instructions
make status         # Check progress
make logs           # Follow full output

# Individual targets
make build          # Build all crates
make test           # Run all tests (unit + integration + sqllogictest)
make benchmark      # Run TPC-H/TPC-C/TPC-DS/Sysbench benchmarks
make all-fg         # Run everything in foreground (blocking)
make help           # Show all targets
```

## Documentation

| Guide | Description |
|-------|-------------|
| [TypeScript SDK](packages/vibesql-client-ts/README.md) | Real-time subscriptions & React hooks |
| [Drizzle ORM](packages/vibesql-drizzle/README.md) | Type-safe queries with Drizzle adapter |
| [HTTP API](docs/http-api.md) | REST, GraphQL, and SSE endpoints |
| [CLI Guide](docs/CLI_GUIDE.md) | Command-line interface |
| [Python Bindings](docs/PYTHON_BINDINGS.md) | Python API reference |
| [Scheduled Functions](docs/scheduled-functions.md) | Cron jobs and scheduled tasks |
| [Vector Search](docs/vector-search.md) | AI/ML embeddings and similarity search |
| [File Storage](docs/file-storage.md) | Blob storage with SQL integration |
| [ODBC/JDBC](docs/ODBC_JDBC_CONNECTIVITY.md) | Database connectivity |
| [Roadmap](docs/ROADMAP.md) | Future plans |
| [History](docs/HISTORY.md) | Development timeline |

## Project Background

This project originated from a challenge about AI capabilities: implement a NIST-compatible SQL database from scratch. Core SQL:1999 compliance was achieved in under 2 weeks (Oct 25 - Nov 1, 2025).

Inspired by [posix4e/nistmemsql](https://github.com/posix4e/nistmemsql).

## License

MIT OR Apache-2.0. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).

## Contributing

See [CLAUDE.md](CLAUDE.md) for development workflow with Loom AI orchestration.

---

**[Try the Live Demo →](https://rjwalters.github.io/vibesql/)**
