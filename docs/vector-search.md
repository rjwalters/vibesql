# Vector Search

VibeSQL supports vector data types and similarity search for AI/ML workloads including embeddings, semantic search, and recommendation systems.

## Quick Start

```sql
-- Create a table with vector columns
CREATE TABLE documents (
  id INTEGER PRIMARY KEY,
  content TEXT,
  embedding VECTOR(384)  -- 384-dimensional vector
);

-- Insert vectors
INSERT INTO documents (id, content, embedding)
VALUES (1, 'Hello world', '[0.1, 0.2, 0.3, ...]');

-- Find similar documents
SELECT id, content, vector_distance(embedding, '[0.15, 0.25, 0.35, ...]') as distance
FROM documents
ORDER BY distance
LIMIT 10;
```

## Vector Data Type

### Creating Vector Columns

Vectors are fixed-dimension arrays of 32-bit floats:

```sql
-- Specify dimensions in the type
CREATE TABLE items (
  id INTEGER PRIMARY KEY,
  embedding VECTOR(1536)  -- OpenAI ada-002 embeddings
);

-- Common embedding dimensions:
-- VECTOR(384)   - sentence-transformers/all-MiniLM-L6-v2
-- VECTOR(768)   - BERT base
-- VECTOR(1024)  - BERT large
-- VECTOR(1536)  - OpenAI text-embedding-ada-002
-- VECTOR(3072)  - OpenAI text-embedding-3-large
```

### Inserting Vectors

Vectors can be inserted as JSON arrays:

```sql
-- JSON array format
INSERT INTO items (id, embedding) VALUES (1, '[0.1, 0.2, 0.3, 0.4]');

-- With explicit cast
INSERT INTO items (id, embedding) VALUES (2, CAST('[0.5, 0.6, 0.7, 0.8]' AS VECTOR(4)));
```

### Vector Validation

VibeSQL validates vector dimensions at insert time:

```sql
-- This will fail: dimension mismatch
INSERT INTO items (id, embedding) VALUES (1, '[0.1, 0.2]');
-- ERROR: Vector has 2 dimensions, expected 1536
```

## Distance Functions

### Cosine Distance (Default)

```sql
-- Cosine distance (0 = identical, 2 = opposite)
SELECT vector_distance(v1, v2) FROM ...;

-- Explicit cosine distance
SELECT vector_cosine_distance(embedding, query_vector) FROM documents;
```

### L2 (Euclidean) Distance

```sql
-- Euclidean distance
SELECT vector_l2_distance(embedding, query_vector) FROM documents;
```

### Inner Product

```sql
-- Negative inner product (for maximum inner product search)
SELECT vector_inner_product(embedding, query_vector) FROM documents;
```

## Distance Operators

For more concise queries, use distance operators:

```sql
-- Cosine distance
SELECT * FROM documents ORDER BY embedding <-> query_vector LIMIT 10;

-- L2 distance
SELECT * FROM documents ORDER BY embedding <=> query_vector LIMIT 10;

-- Negative inner product
SELECT * FROM documents ORDER BY embedding <#> query_vector LIMIT 10;
```

## Utility Functions

```sql
-- Get vector dimensions
SELECT vector_dims(embedding) FROM documents;
-- Returns: 384

-- Get L2 norm
SELECT vector_norm(embedding) FROM documents;

-- Normalize vector to unit length
SELECT vector_normalize(embedding) FROM documents;
```

## Similarity Search Patterns

### Basic K-Nearest Neighbors

```sql
SELECT id, content,
       vector_distance(embedding, $1) as distance
FROM documents
ORDER BY distance
LIMIT 10;
```

### Filtered Search

```sql
-- Semantic search within a category
SELECT id, content,
       vector_distance(embedding, $1) as distance
FROM documents
WHERE category = 'technology'
ORDER BY distance
LIMIT 10;
```

### Hybrid Search (Vector + Full-Text)

```sql
-- Combine vector similarity with keyword matching
SELECT id, content,
       vector_distance(embedding, $1) as vector_score,
       MATCH(content) AGAINST('machine learning') as text_score
FROM documents
WHERE MATCH(content) AGAINST('machine learning')
ORDER BY (vector_score * 0.7 + (1 - text_score) * 0.3)
LIMIT 10;
```

### Threshold-Based Search

```sql
-- Find all documents within distance threshold
SELECT id, content
FROM documents
WHERE vector_distance(embedding, $1) < 0.5
ORDER BY vector_distance(embedding, $1);
```

## Indexing (IVFFlat)

For large datasets, create an IVFFlat index for approximate nearest neighbor search:

```sql
-- Create IVFFlat index
CREATE INDEX idx_embedding ON documents
  USING ivfflat (embedding vector_cosine_ops)
  WITH (lists = 100);

-- The index is used automatically for ORDER BY ... LIMIT queries
SELECT id, content
FROM documents
ORDER BY embedding <-> query_vector
LIMIT 10;
```

### Index Parameters

- **lists** - Number of clusters (recommended: sqrt(rows))
- **probes** - Clusters to search at query time (default: 1)

```sql
-- Set probes for better recall (at query time)
SET ivfflat.probes = 10;
```

### Index Types

| Index | Speed | Recall | Memory | Use Case |
|-------|-------|--------|--------|----------|
| None (brute force) | Slow | 100% | Low | Small datasets (<10K) |
| IVFFlat | Fast | ~95% | Medium | Medium datasets |
| HNSW (future) | Fastest | ~99% | High | Large datasets |

## Integration Examples

### With OpenAI Embeddings

```typescript
import OpenAI from 'openai';
import { VibeSqlClient } from '@vibesql/client';

const openai = new OpenAI();
const db = new VibeSqlClient({ host: 'localhost' });

// Generate embedding
const response = await openai.embeddings.create({
  model: 'text-embedding-ada-002',
  input: 'What is machine learning?',
});
const embedding = response.data[0].embedding;

// Search similar documents
const results = await db.query(
  `SELECT id, content, vector_distance(embedding, $1) as distance
   FROM documents
   ORDER BY distance
   LIMIT 10`,
  [JSON.stringify(embedding)]
);
```

### With Sentence Transformers (Python)

```python
from sentence_transformers import SentenceTransformer
import vibesql

model = SentenceTransformer('all-MiniLM-L6-v2')
db = vibesql.connect()
cursor = db.cursor()

# Generate embedding
text = "What is machine learning?"
embedding = model.encode(text).tolist()

# Search
cursor.execute("""
    SELECT id, content, vector_distance(embedding, ?) as distance
    FROM documents
    ORDER BY distance
    LIMIT 10
""", [str(embedding)])

for row in cursor.fetchall():
    print(f"{row[0]}: {row[1]} (distance: {row[2]:.4f})")
```

### RAG (Retrieval-Augmented Generation)

```typescript
async function ragQuery(question: string) {
  // 1. Generate query embedding
  const queryEmbedding = await generateEmbedding(question);

  // 2. Find relevant documents
  const docs = await db.query(
    `SELECT content FROM documents
     ORDER BY embedding <-> $1
     LIMIT 5`,
    [JSON.stringify(queryEmbedding)]
  );

  // 3. Build context
  const context = docs.map(d => d.content).join('\n\n');

  // 4. Generate answer with LLM
  const answer = await llm.complete({
    prompt: `Context:\n${context}\n\nQuestion: ${question}\n\nAnswer:`,
  });

  return answer;
}
```

## Best Practices

### Choosing Dimensions

- Use the native dimension of your embedding model
- Don't truncate or pad vectors - this hurts accuracy
- Higher dimensions = more accurate but slower

### Normalization

- Normalize vectors before storing if using cosine similarity
- This allows using inner product (faster) instead of cosine distance

```sql
-- Store normalized vectors
INSERT INTO documents (embedding)
VALUES (vector_normalize('[0.1, 0.2, 0.3, ...]'));

-- Then use inner product for similarity (faster than cosine)
SELECT * FROM documents ORDER BY embedding <#> query_vector LIMIT 10;
```

### Index Tuning

- **lists**: Start with `sqrt(num_rows)`, increase for better recall
- **probes**: Start with 1, increase if recall is too low

```sql
-- For 1M rows: lists = 1000, probes = 10-50
CREATE INDEX idx ON docs USING ivfflat (embedding vector_cosine_ops)
  WITH (lists = 1000);

SET ivfflat.probes = 20;
```

### Batch Operations

```sql
-- Bulk insert with COPY (faster than individual INSERTs)
COPY documents (id, content, embedding) FROM STDIN;
```

## Limitations

- Maximum vector dimensions: 16,000
- IVFFlat index requires at least `lists * 10` rows to build
- Vectors are stored as 32-bit floats (not 64-bit)

## See Also

- [HTTP API](http-api.md) - REST endpoints for vector operations
- [Python Bindings](PYTHON_BINDINGS.md) - Python integration
- [TypeScript SDK](../packages/vibesql-client-ts/README.md) - TypeScript/JavaScript client
