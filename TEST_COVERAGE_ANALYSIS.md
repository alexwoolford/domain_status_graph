# Test Coverage Analysis

## Summary

**Total Tests:** 996 tests collected
**Test Files:** 68 test files

## What Has Good Coverage ✅

### Entity Resolution (Recently Added)
- ✅ `test_character_similarity.py` - Character matching (Wide component)
- ✅ `test_semantic_similarity.py` - Semantic matching (Deep component)
- ✅ `test_embedding_scorer.py` - Embedding similarity scoring
- ✅ `test_tiered_decision.py` - Tiered decision system
- ✅ `test_relationship_verifier.py` - Relationship type verification
- ✅ `test_biographical_filter.py` - Biographical context filtering
- ✅ `test_entity_resolution.py` - Full entity resolution pipeline

### Embedding Infrastructure
- ✅ `test_embedding_cache.py` - Embedding caching
- ✅ `test_embedding_callback.py` - Callback mechanism for incremental writes
- ✅ `test_chunking_edge_cases.py` - Text chunking for long texts (embeddings.chunking module)

### Business Logic
- ✅ `test_business_relationship_extraction.py` - Relationship extraction
- ✅ `test_competitor_extraction.py` - Competitor extraction
- ✅ `test_parsing_interface.py` - Parser interface
- ✅ `test_edge_cleanup.py` - Edge cleanup logic

## Critical Gaps ❌

### GraphRAG Core Functions (NO TESTS)

**Query Functions:**
- ❌ `search_documents()` - Vector search over Chunk nodes
- ❌ `search_with_graph_context()` - Graph-aware search
- ❌ `answer_question()` - Multi-hop graph traversal Q&A
- ❌ `_check_vector_index_online()` - Vector index status checking

**Document Management:**
- ❌ `create_documents_and_chunks()` - Creates Document/Chunk nodes in Neo4j
- ❌ `create_chunk_embeddings()` - Creates embeddings for Chunk nodes
- ❌ `link_documents_to_companies()` - Links documents to companies

**Chunking Functions:**
- ❌ `chunk_company_text()` - Chunks business description + risk factors
- ❌ `chunk_filing_sections()` - Chunks multiple filing sections
- ⚠️ Note: `chunk_text()` from `embeddings.chunking` IS tested, but GraphRAG's `chunk_company_text()` and `chunk_filing_sections()` are NOT

**Text Extraction:**
- ❌ `extract_full_text_from_html()` - Extracts text from 10-K HTML files
- ❌ `extract_full_text_with_datamule()` - Datamule extraction wrapper
- ❌ `find_10k_file_for_company()` - Finds 10-K file for a company

### Entity Resolution Gaps

- ❌ `CombinedScorer` (Wide & Deep) - No tests for the combined scoring system
- ❌ `create_scorer()` convenience function - Not tested

### What IS Tested (But Limited)

- ✅ `synthesize_answer()` - LLM synthesis function (tests parameter compatibility for different models)
- ✅ `chunk_text()` from `embeddings.chunking` - Edge cases for long text chunking
- ✅ `aggregate_embeddings()` - Embedding aggregation methods

## Recommendations

### High Priority (Core Functionality)

1. **GraphRAG Query Functions** - These are the main user-facing API:
   - `search_documents()` - Mock Neo4j driver, test vector index path and fallback
   - `search_with_graph_context()` - Test company filtering and graph traversal
   - `answer_question()` - Test multi-hop traversal, relationship prioritization

2. **Document/Chunk Creation** - Critical for data integrity:
   - `create_documents_and_chunks()` - Test deterministic chunk IDs, batching, relationships
   - `link_documents_to_companies()` - Test Company-Document linking

3. **GraphRAG Chunking** - Different from embeddings.chunking:
   - `chunk_company_text()` - Test business description + risk factors chunking
   - `chunk_filing_sections()` - Test multi-section chunking

### Medium Priority

4. **Vector Index Management:**
   - `_check_vector_index_online()` - Test index status checking, timeout handling

5. **Text Extraction:**
   - `extract_full_text_from_html()` - Test HTML parsing, edge cases
   - `find_10k_file_for_company()` - Test file discovery logic

6. **Combined Scoring:**
   - `CombinedScorer` - Test Wide & Deep integration
   - `create_scorer()` - Test convenience function

### Test Strategy

**Unit Tests (Mock Neo4j):**
- Use `unittest.mock` to mock Neo4j driver and sessions
- Test query construction, parameter passing, result parsing
- Test error handling and edge cases

**Integration Tests (Real Neo4j):**
- Use test database fixture (already exists in `test_neo4j_constraints.py`)
- Test end-to-end: create documents → create embeddings → search → query
- Test idempotency (running twice produces same results)

**Meaningful Tests (Not Just Coverage):**
- Test deterministic chunk IDs (same input = same chunk_id)
- Test relationship prioritization (supplier > customer > competitor)
- Test graph traversal depth limits (max_hops)
- Test vector index fallback behavior
- Test empty result handling
- Test batching for large datasets

## Notes

- GraphRAG appears to be older code (no commits in last 6 months), but it's still active functionality
- Entity resolution has excellent test coverage (recent work)
- The `test_chunking_edge_cases.py` tests are for `embeddings.chunking.chunk_text()`, NOT `graphrag.chunking.chunk_company_text()` - these are different functions
- `synthesize_answer()` is tested, but it's in `scripts/`, not the core `graphrag` module
