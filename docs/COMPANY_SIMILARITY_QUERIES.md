# Company Similarity Queries

Use these Cypher queries to find similar companies using all relationship types.

## Find Top Similar Companies (Composite Score)

This query aggregates all similarity relationship types to find the most similar companies:

```cypher
// Find top N most similar companies to a given company
MATCH (c1:Company {ticker: 'KO'})-[r]-(c2:Company)
WHERE type(r) IN ['SIMILAR_INDUSTRY', 'SIMILAR_SIZE', 'SIMILAR_DESCRIPTION',
                  'SIMILAR_TECHNOLOGY', 'SIMILAR_KEYWORDS', 'SIMILAR_MARKET',
                  'COMMON_EXECUTIVE', 'MERGED_OR_ACQUIRED']
WITH c2,
     count(r) as edge_count,
     sum(CASE type(r)
         WHEN 'SIMILAR_INDUSTRY' THEN 1.0
         WHEN 'SIMILAR_SIZE' THEN 0.8
         WHEN 'SIMILAR_DESCRIPTION' THEN 0.9
         WHEN 'SIMILAR_TECHNOLOGY' THEN 0.7
         WHEN 'SIMILAR_KEYWORDS' THEN 0.6
         WHEN 'SIMILAR_MARKET' THEN 0.5
         WHEN 'COMMON_EXECUTIVE' THEN 0.4
         WHEN 'MERGED_OR_ACQUIRED' THEN 0.3
         ELSE 0.0
     END) as weighted_score
RETURN c2.ticker, c2.name, edge_count, weighted_score
ORDER BY weighted_score DESC, edge_count DESC
LIMIT 20
```

## Find Similar Companies by Specific Relationship Type

```cypher
// Similar by industry only
MATCH (c1:Company {ticker: 'KO'})-[r:SIMILAR_INDUSTRY]->(c2:Company)
RETURN c2.ticker, c2.name, r.method, r.classification, r.score
ORDER BY r.score DESC
LIMIT 10
```

```cypher
// Similar by size only
MATCH (c1:Company {ticker: 'KO'})-[r:SIMILAR_SIZE]->(c2:Company)
RETURN c2.ticker, c2.name, r.metric, r.bucket, r.score
ORDER BY r.score DESC
LIMIT 10
```

## Find Companies with Multiple Similarity Signals

```cypher
// Companies with 3+ different similarity types
MATCH (c1:Company {ticker: 'KO'})-[r]-(c2:Company)
WHERE type(r) IN ['SIMILAR_INDUSTRY', 'SIMILAR_SIZE', 'SIMILAR_DESCRIPTION',
                  'SIMILAR_TECHNOLOGY']
WITH c2, collect(DISTINCT type(r)) as rel_types
WHERE size(rel_types) >= 3
RETURN c2.ticker, c2.name, rel_types, size(rel_types) as signal_count
ORDER BY signal_count DESC, c2.ticker
```

## Verify Expected Pairs

```cypher
// Check if Coke and Pepsi are connected
MATCH (c1:Company {ticker: 'KO'})-[r]-(c2:Company {ticker: 'PEP'})
RETURN type(r) as rel_type, properties(r) as props
```

```cypher
// Check if Home Depot and Lowes are connected
MATCH (c1:Company {ticker: 'HD'})-[r]-(c2:Company {ticker: 'LOW'})
RETURN type(r) as rel_type, properties(r) as props
```

## Debug: Check What Relationships Exist

```cypher
// Count relationships by type
MATCH ()-[r]->()
WHERE type(r) STARTS WITH 'SIMILAR'
RETURN type(r) as rel_type, count(r) as count
ORDER BY count DESC
```

```cypher
// Sample relationships for a company
MATCH (c:Company {ticker: 'KO'})-[r]-(c2:Company)
RETURN type(r) as rel_type, c2.ticker, c2.name, properties(r) as props
ORDER BY type(r), c2.ticker
LIMIT 50
```
