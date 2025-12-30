# High-Value Business Intelligence Queries

This document provides **practical queries** for extracting business intelligence from the Public Company Graph. Each query addresses a real-world use case that would be difficult or impossible with traditional relational databases.

## Why Graph?

The Public Company Graph captures **multi-dimensional relationships** between 5,398 public companies:

| Relationship Type | Count | Source |
|-------------------|-------|--------|
| SIMILAR_INDUSTRY | 520,672 | Same sector/industry classification |
| SIMILAR_DESCRIPTION | 420,531 | Cosine similarity of business descriptions |
| SIMILAR_SIZE | 414,096 | Revenue/market cap buckets |
| SIMILAR_RISK | 394,372 | Risk factor embedding similarity |
| SIMILAR_TECHNOLOGY | 124,584 | Jaccard similarity of tech stacks |
| HAS_COMPETITOR | 3,843 | Explicit competitor citations in 10-K |
| HAS_SUPPLIER | 2,597 | Supplier mentions in 10-K |
| HAS_PARTNER | 2,139 | Partnership mentions in 10-K |
| HAS_CUSTOMER | 1,714 | Customer mentions in 10-K |

This enables queries that combine multiple signals—finding companies similar by *both* business model *and* technology stack *and* competitive position.

---

## 1. Competitive Landscape Analysis

### Who are a company's competitors?

Extract competitors explicitly cited in SEC filings (authoritative, from the company's own disclosure):

```cypher
MATCH (c:Company {ticker: 'NVDA'})-[r:HAS_COMPETITOR]->(comp:Company)
RETURN comp.ticker, comp.name, comp.sector,
       r.raw_mention AS mentioned_as,
       r.confidence
ORDER BY r.confidence DESC
```

### Who cites this company as a competitor?

Find companies that view a target as their competitor (inverse view):

```cypher
MATCH (c:Company)-[r:HAS_COMPETITOR]->(target:Company {ticker: 'INTC'})
RETURN c.ticker, c.name, c.sector, r.raw_mention
ORDER BY c.market_cap DESC
```

### Find mutual competitors (both cite each other)

These are the most validated competitive relationships:

```cypher
MATCH (a:Company)-[:HAS_COMPETITOR]->(b:Company)-[:HAS_COMPETITOR]->(a)
WHERE a.ticker < b.ticker
RETURN a.ticker, a.name, b.ticker, b.name, a.sector
ORDER BY a.market_cap DESC
LIMIT 50
```

### Industry competitive map

Visualize the competitive network within an industry:

```cypher
MATCH (c:Company)-[r:HAS_COMPETITOR]->(comp:Company)
WHERE c.sector = 'Technology' AND comp.sector = 'Technology'
RETURN c.ticker, comp.ticker, r.confidence
ORDER BY r.confidence DESC
LIMIT 100
```

---

## 2. Multi-Dimensional Company Discovery

### Find companies similar to Apple across ALL dimensions

Combine multiple similarity signals for comprehensive matching:

```cypher
MATCH (apple:Company {ticker: 'AAPL'})

// Get similarity scores from each dimension
OPTIONAL MATCH (apple)-[desc:SIMILAR_DESCRIPTION]->(c:Company)
OPTIONAL MATCH (apple)-[ind:SIMILAR_INDUSTRY]->(c)
OPTIONAL MATCH (apple)-[size:SIMILAR_SIZE]->(c)
OPTIONAL MATCH (apple)-[risk:SIMILAR_RISK]->(c)
OPTIONAL MATCH (apple)-[tech:SIMILAR_TECHNOLOGY]->(c)

WITH c,
     coalesce(desc.score, 0) AS desc_score,
     coalesce(ind.score, 0) AS industry_score,
     coalesce(size.score, 0) AS size_score,
     coalesce(risk.score, 0) AS risk_score,
     coalesce(tech.score, 0) AS tech_score

// Weighted composite score
WITH c,
     (desc_score * 0.3 + industry_score * 0.2 + size_score * 0.2 +
      risk_score * 0.15 + tech_score * 0.15) AS composite_score

WHERE composite_score > 0.3
RETURN c.ticker, c.name, c.sector, c.industry,
       round(composite_score * 100) / 100 AS similarity
ORDER BY composite_score DESC
LIMIT 20
```

### Find companies with similar business models

Use description embeddings (most semantic):

```cypher
MATCH (target:Company {ticker: 'TSLA'})-[r:SIMILAR_DESCRIPTION]->(similar:Company)
WHERE r.score > 0.85
RETURN similar.ticker, similar.name, similar.sector, similar.industry,
       round(r.score * 100) / 100 AS similarity
ORDER BY r.score DESC
LIMIT 15
```

### Find similar companies in a DIFFERENT industry

Discover cross-industry analogues (useful for pattern recognition):

```cypher
MATCH (target:Company {ticker: 'NFLX'})-[r:SIMILAR_DESCRIPTION]->(similar:Company)
WHERE similar.sector <> target.sector
  AND r.score > 0.75
RETURN similar.ticker, similar.name, similar.sector, similar.industry,
       round(r.score * 100) / 100 AS similarity
ORDER BY r.score DESC
LIMIT 10
```

---

## 3. Supply Chain & Business Relationship Mapping

### Map a company's supply chain

Find suppliers, customers, and partners in one query:

```cypher
MATCH (c:Company {ticker: 'AAPL'})
OPTIONAL MATCH (c)-[s:HAS_SUPPLIER]->(supplier:Company)
OPTIONAL MATCH (c)-[cust:HAS_CUSTOMER]->(customer:Company)
OPTIONAL MATCH (c)-[p:HAS_PARTNER]->(partner:Company)
RETURN c.name AS company,
       collect(DISTINCT supplier.name) AS suppliers,
       collect(DISTINCT customer.name) AS customers,
       collect(DISTINCT partner.name) AS partners
```

### Find companies that share suppliers

Identify potential supply chain risks or partnership opportunities:

```cypher
MATCH (c1:Company)-[:HAS_SUPPLIER]->(supplier:Company)<-[:HAS_SUPPLIER]-(c2:Company)
WHERE c1.ticker < c2.ticker
WITH supplier, c1, c2, count(*) AS shared_count
RETURN c1.ticker, c1.name, c2.ticker, c2.name,
       collect(supplier.name) AS shared_suppliers
ORDER BY size(collect(supplier.name)) DESC
LIMIT 20
```

### Customer concentration risk

Find companies mentioned as customers by many others (key accounts):

```cypher
MATCH (c:Company)-[:HAS_CUSTOMER]->(customer:Company)
WITH customer, count(c) AS supplier_count, collect(c.ticker) AS suppliers
WHERE supplier_count >= 3
RETURN customer.ticker, customer.name, supplier_count, suppliers
ORDER BY supplier_count DESC
LIMIT 20
```

---

## 4. M&A Target Identification

### Find acquisition candidates

Companies similar to a target but smaller (potential acqui-hires or tuck-ins):

```cypher
MATCH (acquirer:Company {ticker: 'GOOGL'})-[r:SIMILAR_DESCRIPTION]->(target:Company)
WHERE r.score > 0.75
  AND target.market_cap < acquirer.market_cap * 0.1  // 10x smaller
  AND target.market_cap > 100000000  // At least $100M
RETURN target.ticker, target.name, target.sector,
       target.market_cap / 1000000000 AS market_cap_billions,
       round(r.score * 100) / 100 AS similarity
ORDER BY r.score DESC
LIMIT 20
```

### Find companies in adjacent markets

Similar description but different industry (expansion opportunities):

```cypher
MATCH (c:Company {ticker: 'CRM'})-[r:SIMILAR_DESCRIPTION]->(target:Company)
WHERE r.score > 0.7
  AND target.industry <> c.industry
  AND target.sector = c.sector  // Same broad sector
RETURN target.ticker, target.name, target.industry,
       round(r.score * 100) / 100 AS similarity
ORDER BY r.score DESC
LIMIT 15
```

---

## 5. Investment Screening

### Find companies similar to successful investments

If you invested in NVDA, what else should you consider?

```cypher
MATCH (winner:Company {ticker: 'NVDA'})

// Combine description and risk similarity
MATCH (winner)-[desc:SIMILAR_DESCRIPTION]->(candidate:Company)
MATCH (winner)-[risk:SIMILAR_RISK]->(candidate)

WHERE desc.score > 0.7 AND risk.score > 0.6
  AND candidate.market_cap < winner.market_cap  // Smaller = more upside

RETURN candidate.ticker, candidate.name, candidate.sector,
       candidate.market_cap / 1000000000 AS market_cap_B,
       round(desc.score * 100) / 100 AS desc_similarity,
       round(risk.score * 100) / 100 AS risk_similarity
ORDER BY (desc.score + risk.score) DESC
LIMIT 15
```

### Peer group construction

Build a peer group for valuation comparisons:

```cypher
MATCH (target:Company {ticker: 'SHOP'})
MATCH (target)-[ind:SIMILAR_INDUSTRY]->(peer:Company)
MATCH (target)-[size:SIMILAR_SIZE]->(peer)

WHERE ind.score > 0 AND size.score > 0

RETURN peer.ticker, peer.name,
       peer.revenue / 1000000000 AS revenue_B,
       peer.market_cap / 1000000000 AS market_cap_B
ORDER BY (ind.score + size.score) DESC
LIMIT 10
```

---

## 6. Technology Intelligence

### What technologies does a company use?

```cypher
MATCH (c:Company {ticker: 'AMZN'})-[:HAS_DOMAIN]->(d:Domain)-[:USES]->(t:Technology)
RETURN t.name, t.category
ORDER BY t.category, t.name
```

### Find companies using a specific technology

Target customers for your product:

```cypher
MATCH (d:Domain)-[:USES]->(t:Technology {name: 'Kubernetes'})
MATCH (c:Company)-[:HAS_DOMAIN]->(d)
RETURN c.ticker, c.name, c.sector, d.final_domain
ORDER BY c.market_cap DESC
LIMIT 30
```

### Technology adoption prediction

What technologies might a domain adopt next?

```cypher
MATCH (c:Company {ticker: 'NFLX'})-[:HAS_DOMAIN]->(d:Domain)
MATCH (d)-[r:LIKELY_TO_ADOPT]->(t:Technology)
WHERE NOT (d)-[:USES]->(t)
RETURN t.name, t.category, round(r.score * 1000) / 1000 AS likelihood
ORDER BY r.score DESC
LIMIT 10
```

### Technology co-occurrence

What technologies are commonly used together?

```cypher
MATCH (t1:Technology {name: 'React'})-[r:CO_OCCURS_WITH]->(t2:Technology)
WHERE r.similarity > 0.3
RETURN t2.name, t2.category, round(r.similarity * 100) / 100 AS affinity
ORDER BY r.similarity DESC
LIMIT 15
```

---

## 7. Network Analysis

### Most-cited competitors (market leaders)

Who is most frequently mentioned as a competitor?

```cypher
MATCH (c:Company)-[r:HAS_COMPETITOR]->(cited:Company)
WITH cited, count(r) AS citation_count
ORDER BY citation_count DESC
RETURN cited.ticker, cited.name, cited.sector, citation_count
LIMIT 20
```

### Companies with most disclosed relationships

Transparency score - who discloses the most about their business ecosystem?

```cypher
MATCH (c:Company)
OPTIONAL MATCH (c)-[comp:HAS_COMPETITOR]->()
OPTIONAL MATCH (c)-[cust:HAS_CUSTOMER]->()
OPTIONAL MATCH (c)-[supp:HAS_SUPPLIER]->()
OPTIONAL MATCH (c)-[part:HAS_PARTNER]->()
WITH c,
     count(DISTINCT comp) AS competitors,
     count(DISTINCT cust) AS customers,
     count(DISTINCT supp) AS suppliers,
     count(DISTINCT part) AS partners
WHERE (competitors + customers + suppliers + partners) > 5
RETURN c.ticker, c.name, c.sector,
       competitors, customers, suppliers, partners,
       (competitors + customers + suppliers + partners) AS total_relationships
ORDER BY total_relationships DESC
LIMIT 20
```

### Find industry clusters

Companies that cite each other form natural clusters:

```cypher
MATCH (a:Company)-[:HAS_COMPETITOR]->(b:Company)-[:HAS_COMPETITOR]->(c:Company)-[:HAS_COMPETITOR]->(a)
WHERE a.ticker < b.ticker AND b.ticker < c.ticker
RETURN a.ticker, b.ticker, c.ticker, a.sector
LIMIT 20
```

---

## Query Performance Tips

1. **Use indexed properties**: `ticker`, `cik`, `sector`, `industry` are indexed
2. **Limit early**: Add `LIMIT` before expensive operations when exploring
3. **Profile queries**: Use `PROFILE` prefix to see execution plan
4. **Filter on relationships**: Relationship indexes exist for `confidence` on business relationships

---

## Related Documentation

- **[Graph Schema](graph_schema.md)** - Complete schema with all properties
- **[Architecture](architecture.md)** - How the data is loaded and computed
- **[Step-by-Step Guide](step_by_step_guide.md)** - Pipeline walkthrough

---

*Inspired by [CompanyKG: A Large-Scale Heterogeneous Graph for Company Similarity Quantification](https://arxiv.org/abs/2306.10649) (NeurIPS 2023)*
