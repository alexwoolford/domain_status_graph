# GraphRAG Layer

The GraphRAG layer adds question-answering capabilities to the existing knowledge graph while preserving all structured relationships.

## Architecture

```
Company Nodes (existing)
  ├── Structured relationships (HAS_COMPETITOR, SIMILAR_DESCRIPTION, etc.)
  └── HAS → Document Nodes (new)
         └── Chunked text from 10-K filings
         └── Embeddings for vector search
```

## What It Does

1. **Chunks company text**: Splits business descriptions and risk factors into searchable documents
2. **Creates Document nodes**: Stores chunks in Neo4j with metadata
3. **Links to companies**: `(Company)-[:HAS]->(Document)` relationships
4. **Enables semantic search**: Vector search over document embeddings
5. **Graph-aware retrieval**: Combines vector search with graph traversal

## Setup

### 1. Create the GraphRAG Layer

The script automatically creates required constraints and indexes for Document nodes
(including the critical unique constraint on `doc_id` for MERGE performance).

```bash
# Preview what would be created (dry run)
python scripts/create_graphrag_layer.py

# Create Document nodes from existing company data
python scripts/create_graphrag_layer.py --execute

# With custom chunking parameters
python scripts/create_graphrag_layer.py --execute --chunk-size 1500 --chunk-overlap 300

# Skip embeddings (faster, but documents won't be searchable)
python scripts/create_graphrag_layer.py --execute --skip-embeddings
```

### 2. Query the GraphRAG Layer

```bash
# Simple semantic search
python scripts/query_graphrag.py "What are the main competitive threats?"

# Search within a specific company's context
python scripts/query_graphrag.py "What risks does Tesla face?" --company TSLA

# Get answer context (for LLM generation)
python scripts/query_graphrag.py "Who are Apple's competitors?" --company AAPL --answer
```

## Python API

### Basic Search

```python
from public_company_graph.graphrag.queries import search_documents
from public_company_graph.embeddings.openai_client import get_openai_client, create_embedding
from public_company_graph.config import Settings
from public_company_graph.neo4j.connection import get_neo4j_driver

settings = Settings()
driver = get_neo4j_driver()
client = get_openai_client(api_key=settings.openai_api_key)

# Create query embedding
query_embedding = create_embedding(client, "What are the main risks?", model="text-embedding-3-small")

# Search
results = search_documents(
    driver,
    "What are the main risks?",
    query_embedding,
    limit=5,
    database=settings.neo4j_database
)

for doc in results:
    print(f"{doc['company_name']}: {doc['similarity']:.3f}")
    print(doc['text'][:200])
```

### Graph-Aware Search

```python
from public_company_graph.graphrag.queries import search_with_graph_context

# Search within company and related companies (competitors, partners, etc.)
results = search_with_graph_context(
    driver,
    "What are the competitive threats?",
    query_embedding,
    company_ticker="AAPL",  # Focus on Apple and related companies
    limit=10,
    database=settings.neo4j_database
)
```

### Q&A Retrieval

```python
from public_company_graph.graphrag.queries import answer_question

# Get answer context (combines documents + extracts companies)
result = answer_question(
    driver,
    "Who are Tesla's main competitors?",
    query_embedding,
    company_ticker="TSLA",
    max_documents=5,
    database=settings.neo4j_database,
    use_graph_traversal=True,  # Optional: enable multi-hop graph traversal
    max_hops=2,  # Optional: maximum graph traversal depth
)

print(f"Found {result['num_documents']} relevant documents")
print(f"Companies: {result['companies']}")
print(f"Context:\n{result['context']}")

# Use context with LLM for final answer generation
```

## What's Preserved

All existing structured relationships remain intact:
- `HAS_COMPETITOR`, `HAS_PARTNER`, `HAS_CUSTOMER`, `HAS_SUPPLIER`
- `SIMILAR_DESCRIPTION`, `SIMILAR_RISK`, `SIMILAR_INDUSTRY`, etc.
- All Company, Domain, Technology nodes and relationships

The GraphRAG layer is **additive** - it doesn't modify existing data.

## Use Cases

1. **Question Answering**: "What did Apple say about competition in their 10-K?"
2. **Risk Analysis**: "What risks do biotech companies face?"
3. **Competitive Intelligence**: "How do companies describe their competitive landscape?"
4. **Contextual Search**: Find relevant text from related companies (competitors, partners)

## Integration with Existing Features

The GraphRAG layer works alongside existing features:

- **Structured relationships**: Use `HAS_COMPETITOR` for known facts
- **GraphRAG documents**: Use `HAS` documents for Q&A and context
- **Similarity**: Use `SIMILAR_DESCRIPTION` for ranking, GraphRAG for explanation

Example: Find competitors using structured relationships, then use GraphRAG to explain why they're competitors based on filing text.
