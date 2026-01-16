# GraphRAG Layer - Testing Guide

## Quick Answers

**Is it in the pipeline?** No - it's a separate script you can test independently.

**Will it be expensive?** ~$1.75 for full dataset (437K chunks), but you can test on small subsets first.

**Is it slow?** Yes, could take 30-60 minutes for full run. Use `--limit` to test quickly.

**Are chunks deterministic?** Yes - fixed character positions ensure identical chunks on re-runs.

**Can I iterate quickly?** Yes - use `--limit 10` to test on 10 companies (~5 minutes).

## Cost & Time Estimates

For full dataset (5,402 companies):
- **Chunks**: ~437,000
- **API Cost**: ~$1.75 (text-embedding-3-small)
- **Time**: 30-60 minutes (depends on API rate limits)
- **Chunks per company**: ~81

For test run (10 companies):
- **Chunks**: ~800
- **API Cost**: ~$0.003
- **Time**: ~2-3 minutes
- **Chunks per company**: ~80

## Testing Strategy

### Step 1: Dry Run (Instant)
```bash
python scripts/create_graphrag_layer.py
```
Shows estimates without doing anything.

### Step 2: Tiny Test (2-3 minutes)
```bash
# Test on 10 companies
python scripts/create_graphrag_layer.py --execute --limit 10
```

### Step 3: Small Test (5-10 minutes)
```bash
# Test on 100 companies
python scripts/create_graphrag_layer.py --execute --limit 100
```

### Step 4: Test Queries
```bash
# Query the test data
python scripts/query_graphrag.py "What are the main risks?" --company TSLA
```

### Step 5: Full Run (if tests pass)
```bash
# Full dataset
python scripts/create_graphrag_layer.py --execute
```

## Determinism & Idempotency

### Chunks are Deterministic
- Fixed `chunk_size` and `chunk_overlap` parameters
- Character-based boundaries (not sentence-based)
- Same input text = same chunks every time

### Document IDs are Deterministic
- Format: `{cik}_{section_type}_{chunk_index}`
- Same company + same chunk = same doc_id
- Re-runs update existing documents (idempotent)

### Embedding Caching
- Uses existing embedding cache infrastructure
- Same text = cached embedding (no API call)
- Re-runs are fast if embeddings are cached

## Incremental Updates

If you add new companies or update existing ones:

```bash
# Skip companies that already have documents
python scripts/create_graphrag_layer.py --execute --skip-existing
```

This only processes companies without existing Document nodes.

## Cost Optimization

1. **Test first**: Use `--limit 10` to verify it works
2. **Skip embeddings initially**: Use `--skip-embeddings` to create nodes without embeddings (fast, but not searchable)
3. **Create embeddings later**: Re-run with embeddings once you've verified the structure

```bash
# Step 1: Create structure (fast, no API calls)
python scripts/create_graphrag_layer.py --execute --skip-embeddings

# Step 2: Add embeddings (slower, costs money)
python scripts/create_graphrag_layer.py --execute --skip-existing
```

## Monitoring Progress

The script logs progress:
- Every 100 companies during chunking
- Batch updates during node creation
- Embedding progress (uses existing infrastructure)

Check logs in `logs/create_graphrag_layer_*.log`

## Troubleshooting

**Chunks are different on re-run?**
- Shouldn't happen - chunking is deterministic
- Check if `chunk_size` or `chunk_overlap` changed
- Check if source text changed

**Embeddings not cached?**
- Embeddings are cached by text content
- If text changed, new embedding will be created
- Check cache status: `python scripts/check_embedding_cache_status.py`

**Too slow?**
- Use `--limit` to test on subset
- Use `--skip-embeddings` to skip API calls
- Use `--skip-existing` to skip companies with documents
