# Monitoring GraphRAG Embedding Creation

## Quick Status Check

```bash
# Check current progress
python -c "
from public_company_graph.config import Settings
from public_company_graph.neo4j.connection import get_neo4j_driver

settings = Settings()
driver = get_neo4j_driver()

with driver.session(database=settings.neo4j_database) as session:
    total = session.run('MATCH (d:Document) RETURN count(d) as count').single()['count']
    with_emb = session.run('MATCH (d:Document) WHERE d.embedding IS NOT NULL RETURN count(d) as count').single()['count']
    print(f'Progress: {with_emb:,}/{total:,} ({(with_emb/total*100):.2f}%)')
    print(f'Remaining: {total - with_emb:,} embeddings')
"
```

## Real-Time Monitoring

### Option 1: Manual Monitoring Loop

Use a manual monitoring loop (see Option 3 below) or check progress directly in Neo4j.

### Option 2: Watch the Log File

```bash
# Watch the log file for progress updates
tail -f logs/create_graphrag_layer_*.log | grep -E "(Pre-processing|Batch embedding|Progress|ETA|Rate)"
```

### Option 3: Manual Monitoring Loop

```bash
# Check progress every 30 seconds
watch -n 30 'python -c "
from public_company_graph.config import Settings
from public_company_graph.neo4j.connection import get_neo4j_driver
settings = Settings()
driver = get_neo4j_driver()
with driver.session(database=settings.neo4j_database) as session:
    total = session.run(\"MATCH (d:Document) RETURN count(d) as count\").single()[\"count\"]
    with_emb = session.run(\"MATCH (d:Document) WHERE d.embedding IS NOT NULL RETURN count(d) as count\").single()[\"count\"]
    print(f\"Embeddings: {with_emb:,}/{total:,} ({(with_emb/total*100):.2f}%)\")
"'
```

## What to Look For

### In Log Files

1. **Pre-processing phase:**
   ```
   Pre-processing 2,848,542 texts (truncating and counting tokens)...
   Pre-processing: 100,000/2,848,542 texts (3.5%) | Rate: 50000 texts/sec | ETA: 0.9min
   ```

2. **Batch creation:**
   ```
   Token-based batching: 2,848,542 texts -> 14,243 batches (max 40,000 tokens per batch)
   Processing 14,243 batches (2,848,542 texts total) with rate limiting (80 req/sec max)
   ```

3. **API progress:**
   ```
   Batch embedding progress: 50,000/2,848,542 texts (1.8%) | Batch 250/14,243 | Rate: 142.0 texts/sec | ETA: 320.5min
   ```

4. **Neo4j writes:**
   ```
   Updating 1000 Document nodes in Neo4j...
   ```

### In Neo4j

```cypher
// Check current progress
MATCH (d:Document)
WHERE d.embedding IS NOT NULL
RETURN count(d) as with_embeddings

// Check total
MATCH (d:Document)
RETURN count(d) as total

// Sample recent embeddings
MATCH (d:Document)
WHERE d.embedding IS NOT NULL
RETURN d.doc_id, d.company_ticker, d.created_at
ORDER BY d.created_at DESC
LIMIT 10
```

### In Cache

```python
from public_company_graph.cache import get_cache

cache = get_cache()
stats = cache.stats()
print(f"Cache size: {stats['size_mb']:.1f} MB")
print(f"Total entries: {stats['total']:,}")

# Count Document embeddings
doc_count = 0
for full_key in cache._cache:
    if full_key.startswith('embeddings:'):
        key = full_key.split(':', 1)[1]
        if ':text' in key:
            doc_count += 1
print(f"Document embeddings cached: {doc_count:,}")
```

## Expected Timeline

- **Pre-processing:** ~1-2 minutes for 2.85M texts
- **API calls:** ~5-6 hours at 80 req/sec (with batching)
- **Total:** ~5-6 hours for full dataset

## Troubleshooting

### No Progress After 10+ Minutes

1. Check if process is running:
   ```bash
   ps aux | grep create_graphrag_layer
   ```

2. Check for errors in log:
   ```bash
   tail -100 logs/create_graphrag_layer_*.log | grep -i error
   ```

3. Check Neo4j connection:
   ```bash
   python -c "from public_company_graph.neo4j.connection import get_neo4j_driver; get_neo4j_driver().verify_connectivity()"
   ```

### Rate Limit Errors

The code now enforces 80 req/sec. If you see rate limit errors:
- The retry logic should handle it automatically
- Check logs for retry attempts
- Process will slow down but continue

### Cache Not Growing

- Cache grows as embeddings are created
- If cache isn't growing, embeddings aren't being created
- Check log for API errors or stuck pre-processing
