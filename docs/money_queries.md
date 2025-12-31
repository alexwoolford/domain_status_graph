# High-Value Business Intelligence Queries

This document provides **practical queries** that have been tested against the actual graph. Each query returns meaningful results.

## Graph Statistics (Verified)

| Relationship Type | Count | Notes |
|-------------------|-------|-------|
| SIMILAR_INDUSTRY | 520,672 | Based on sector/industry classification |
| SIMILAR_DESCRIPTION | 436,973 | Cosine similarity of business descriptions |
| SIMILAR_SIZE | 414,096 | Revenue/market cap buckets |
| SIMILAR_RISK | 394,372 | Risk factor embedding similarity |
| SIMILAR_TECHNOLOGY | 124,584 | Jaccard similarity of web tech stacks |
| USES | 46,081 | Domain→Technology relationships |
| LIKELY_TO_ADOPT | 41,250 | Technology adoption predictions |
| CO_OCCURS_WITH | 41,220 | Technology co-occurrence |
| HAS_COMPETITOR | 3,843 | Explicit competitor citations in 10-K |
| HAS_DOMAIN | 3,745 | Company→Domain relationships |
| HAS_SUPPLIER | 2,597 | Supplier mentions in 10-K |
| HAS_PARTNER | 2,139 | Partnership mentions in 10-K |
| HAS_CUSTOMER | 1,714 | Customer mentions in 10-K |

### Data Coverage Notes

- **5,390 of 5,398 companies** (99.85%) have business descriptions and embeddings
- **~18% of companies** have sector/industry from Yahoo Finance
- **~18% of companies** have market_cap data from Yahoo Finance
- **Technology data is web technologies only** (JavaScript, CMS, Analytics, CDN, etc.)—not backend infrastructure

---

## 1. Competitive Landscape Analysis

### Who are NVIDIA's competitors?

Extract competitors explicitly cited in SEC filings:

```cypher
MATCH (c:Company {ticker: 'NVDA'})-[r:HAS_COMPETITOR]->(comp:Company)
RETURN comp.ticker, comp.name,
       r.raw_mention AS mentioned_as,
       r.confidence
ORDER BY r.confidence DESC
```

**Expected result**: Returns AMD (Advanced Micro Devices) as cited competitor.

### Who cites Intel as a competitor?

Find companies that view Intel as their competitor:

```cypher
MATCH (c:Company)-[r:HAS_COMPETITOR]->(target:Company {ticker: 'INTC'})
RETURN c.ticker, c.name, r.raw_mention
ORDER BY c.name
LIMIT 20
```

### Most-cited competitors (market leaders)

Who is most frequently mentioned as a competitor across all 10-Ks?

```cypher
MATCH (c:Company)-[r:HAS_COMPETITOR]->(cited:Company)
WITH cited, count(r) AS citation_count
ORDER BY citation_count DESC
RETURN cited.ticker, cited.name, citation_count
LIMIT 15
```

**Expected top results**: Mastercard (123 citations), Microsoft (87), Pfizer (87), Apple (76).

### Find mutual competitors (both cite each other)

These are the most validated competitive relationships:

```cypher
MATCH (a:Company)-[:HAS_COMPETITOR]->(b:Company)-[:HAS_COMPETITOR]->(a)
WHERE a.ticker < b.ticker
RETURN a.ticker, a.name, b.ticker, b.name
LIMIT 20
```

**Expected results**: CommScope↔Cisco, Cigna↔CVS, Celestica↔Sanmina, etc.

---

## 2. Company Similarity (By Business Description)

Business description similarity is the most reliable dimension since 96% of companies have embeddings.

### Find companies similar to Tesla

```cypher
MATCH (target:Company {ticker: 'TSLA'})-[r:SIMILAR_DESCRIPTION]->(similar:Company)
WHERE r.score > 0.75
RETURN similar.ticker, similar.name,
       round(r.score * 100) / 100 AS similarity
ORDER BY r.score DESC
LIMIT 15
```

**Expected results**: FTC Solar, Sunrun, Rivian, GM, Enphase Energy (all EV/energy companies).

### PepsiCo and Coca-Cola similarity

```cypher
MATCH (pep:Company {ticker: 'PEP'})-[r:SIMILAR_DESCRIPTION]->(ko:Company {ticker: 'KO'})
RETURN pep.name, ko.name, round(r.score * 100) / 100 AS similarity
```

**Expected result**: 0.88 similarity score (highly similar business models).

### Home Depot and Lowe's similarity

```cypher
MATCH (hd:Company {ticker: 'HD'})-[r:SIMILAR_DESCRIPTION]->(low:Company {ticker: 'LOW'})
RETURN hd.name, low.name, round(r.score * 100) / 100 AS similarity
```

**Expected result**: 0.85 similarity score.

### Find Google's most similar companies

```cypher
MATCH (googl:Company {ticker: 'GOOGL'})-[r:SIMILAR_DESCRIPTION]->(similar:Company)
RETURN similar.ticker, similar.name,
       round(r.score * 100) / 100 AS similarity
ORDER BY r.score DESC
LIMIT 10
```

**Expected results**: Gen Digital, Cloudflare, Meta, Box, Yext, Duolingo.

### ⭐ Weighted Multi-Dimensional Similarity (Best Query)

Combine all similarity dimensions with configurable weights to find the "most similar" company overall:

```cypher
// Find most similar companies using weighted multi-dimensional similarity
// Weights: description (40%), industry (20%), risk (20%), technology (10%), size (10%)

MATCH (target:Company {ticker: 'AAPL'})

// Collect all similarity scores
OPTIONAL MATCH (target)-[desc:SIMILAR_DESCRIPTION]->(c:Company)
OPTIONAL MATCH (target)-[ind:SIMILAR_INDUSTRY]->(c)
OPTIONAL MATCH (target)-[risk:SIMILAR_RISK]->(c)
OPTIONAL MATCH (target)-[tech:SIMILAR_TECHNOLOGY]->(c)
OPTIONAL MATCH (target)-[size:SIMILAR_SIZE]->(c)

WITH c,
     COALESCE(desc.score, 0) AS desc_score,
     COALESCE(ind.score, 0) AS ind_score,
     COALESCE(risk.score, 0) AS risk_score,
     COALESCE(tech.score, 0) AS tech_score,
     COALESCE(size.score, 0) AS size_score
WHERE c IS NOT NULL

// Calculate weighted composite score
WITH c,
     desc_score, ind_score, risk_score, tech_score, size_score,
     (desc_score * 0.4) +
     (ind_score * 0.2) +
     (risk_score * 0.2) +
     (tech_score * 0.1) +
     (size_score * 0.1) AS weighted_score,
     // Count how many dimensions matched (bonus for well-rounded similarity)
     CASE WHEN desc_score > 0 THEN 1 ELSE 0 END +
     CASE WHEN ind_score > 0 THEN 1 ELSE 0 END +
     CASE WHEN risk_score > 0 THEN 1 ELSE 0 END +
     CASE WHEN tech_score > 0 THEN 1 ELSE 0 END +
     CASE WHEN size_score > 0 THEN 1 ELSE 0 END AS dimensions_matched

WHERE weighted_score > 0.3  // Minimum threshold

RETURN c.ticker AS ticker,
       c.name AS company,
       c.sector AS sector,
       round(weighted_score * 100) / 100 AS weighted_similarity,
       dimensions_matched,
       round(desc_score * 100) / 100 AS description,
       round(ind_score * 100) / 100 AS industry,
       round(risk_score * 100) / 100 AS risk,
       round(tech_score * 100) / 100 AS technology,
       round(size_score * 100) / 100 AS size
ORDER BY weighted_score DESC
LIMIT 15
```

**Why this works well:**
- **Description similarity (40%)**: Most reliable signal, 99.85% coverage
- **Industry similarity (20%)**: Strong categorical match
- **Risk similarity (20%)**: Companies facing similar challenges
- **Technology similarity (10%)**: Lower weight due to web-only data
- **Size similarity (10%)**: Lower weight due to 18% coverage

**Adjust weights based on your use case:**
- M&A: Increase size weight (companies buy similar-sized targets)
- Competitive analysis: Increase description + technology weights
- Risk analysis: Increase risk weight significantly

---

## 3. Supply Chain & Business Relationship Mapping

### Companies with the most disclosed relationships

Find companies with the most transparent 10-K disclosures:

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
RETURN c.ticker, c.name,
       competitors, customers, suppliers, partners,
       (competitors + customers + suppliers + partners) AS total
ORDER BY total DESC
LIMIT 15
```

**Expected top results**: Broadcom (55 relationships), Penguin Solutions (35), Adeia (29), CDW (29).

### Map Broadcom's supply chain

Broadcom has the most supplier relationships in the graph:

```cypher
MATCH (c:Company {ticker: 'AVGO'})
OPTIONAL MATCH (c)-[:HAS_SUPPLIER]->(supplier:Company)
OPTIONAL MATCH (c)-[:HAS_CUSTOMER]->(customer:Company)
OPTIONAL MATCH (c)-[:HAS_PARTNER]->(partner:Company)
RETURN c.name AS company,
       collect(DISTINCT supplier.ticker) AS suppliers,
       collect(DISTINCT customer.ticker) AS customers,
       collect(DISTINCT partner.ticker) AS partners
```

**Expected suppliers**: AMD, Intel, Skyworks, Analog Devices, etc. (28 total).

### Find companies that share suppliers

Identify supply chain overlap:

```cypher
MATCH (c1:Company)-[:HAS_SUPPLIER]->(supplier:Company)<-[:HAS_SUPPLIER]-(c2:Company)
WHERE c1.ticker < c2.ticker
WITH supplier, c1, c2
RETURN c1.ticker, c2.ticker,
       collect(supplier.ticker) AS shared_suppliers,
       count(supplier) AS overlap_count
ORDER BY overlap_count DESC
LIMIT 15
```

### Customer concentration risk

Find companies mentioned as customers by many suppliers:

```cypher
MATCH (supplier:Company)-[:HAS_CUSTOMER]->(customer:Company)
WITH customer, count(supplier) AS supplier_count, collect(supplier.ticker) AS suppliers
WHERE supplier_count >= 3
RETURN customer.ticker, customer.name, supplier_count, suppliers
ORDER BY supplier_count DESC
LIMIT 15
```

---

## 4. Investment Screening

### Find companies similar to NVIDIA (for portfolio diversification)

```cypher
MATCH (winner:Company {ticker: 'NVDA'})
MATCH (winner)-[desc:SIMILAR_DESCRIPTION]->(candidate:Company)
MATCH (winner)-[risk:SIMILAR_RISK]->(candidate)
WHERE desc.score > 0.7 AND risk.score > 0.6
RETURN candidate.ticker, candidate.name,
       round(desc.score * 100) / 100 AS desc_similarity,
       round(risk.score * 100) / 100 AS risk_similarity,
       round((desc.score + risk.score) / 2 * 100) / 100 AS avg_score
ORDER BY (desc.score + risk.score) DESC
LIMIT 15
```

**Expected results**: AMD, Nutanix, Astera Labs, PDF Solutions, Lumentum, Ambarella, Micron.

### Find semiconductor companies with similar risk profiles

```cypher
MATCH (target:Company {ticker: 'NVDA'})-[r:SIMILAR_RISK]->(similar:Company)
WHERE r.score > 0.85
RETURN similar.ticker, similar.name,
       round(r.score * 100) / 100 AS risk_similarity
ORDER BY r.score DESC
LIMIT 15
```

---

## 5. Technology Intelligence (Web Technologies)

> **Note**: Technology data comes from web fingerprinting (via domain_status crate) and includes JavaScript frameworks, CMS, CDN, analytics, etc.—NOT backend infrastructure like Kubernetes or Docker.

### What web technologies does Amazon use?

```cypher
MATCH (c:Company {ticker: 'AMZN'})-[:HAS_DOMAIN]->(d:Domain)-[:USES]->(t:Technology)
RETURN t.name, t.category
ORDER BY t.category, t.name
```

**Expected results**: Amazon CloudFront, React, HTTP/3, AWS, HSTS.

### Find companies using React

```cypher
MATCH (d:Domain)-[:USES]->(t:Technology {name: 'React'})
MATCH (c:Company)-[:HAS_DOMAIN]->(d)
RETURN c.ticker, c.name, d.final_domain
LIMIT 30
```

### Technology co-occurrence (what goes with WordPress?)

```cypher
MATCH (t1:Technology {name: 'WordPress'})-[r:CO_OCCURS_WITH]->(t2:Technology)
WHERE r.similarity > 0.3
RETURN t2.name, t2.category, round(r.similarity * 100) / 100 AS affinity
ORDER BY r.similarity DESC
LIMIT 10
```

**Expected results**: MySQL (0.98), PHP (0.89), jQuery Migrate (0.86), Yoast SEO (0.83).

### Technology co-occurrence (what goes with React?)

```cypher
MATCH (t1:Technology {name: 'React'})-[r:CO_OCCURS_WITH]->(t2:Technology)
WHERE r.similarity > 0.3
RETURN t2.name, t2.category, round(r.similarity * 100) / 100 AS affinity
ORDER BY r.similarity DESC
LIMIT 15
```

**Expected results**: Webpack (0.65), Node.js (0.57), Next.js (0.52), Akamai (0.47).

### Technology categories available

```cypher
MATCH (t:Technology)
RETURN DISTINCT t.category, count(t) AS tech_count
ORDER BY tech_count DESC
LIMIT 20
```

**Top categories**: JavaScript libraries (95), WordPress plugins (81), CMS (46), Analytics (41).

---

## 6. Network Analysis

### Find industry clusters (companies that cite each other)

```cypher
MATCH (a:Company)-[:HAS_COMPETITOR]->(b:Company)-[:HAS_COMPETITOR]->(c:Company)-[:HAS_COMPETITOR]->(a)
WHERE a.ticker < b.ticker AND b.ticker < c.ticker
RETURN a.ticker, b.ticker, c.ticker
LIMIT 20
```

### PepsiCo's competitor network

```cypher
MATCH (pep:Company {ticker: 'PEP'})-[r:HAS_COMPETITOR]->(comp:Company)
RETURN comp.ticker, comp.name, r.raw_mention
```

**Expected results**: ConAgra Brands, Walmart, Monster Beverage, Keurig Dr Pepper, Utz Brands.

---

## Data Coverage Notes

Understanding these limitations helps write effective queries:

### Technology Data (Web-Only)
Technology detection is based on **HTTP fingerprinting** of company domains. This captures:
- ✅ JavaScript frameworks (React, Angular, Vue)
- ✅ CMS platforms (WordPress, Drupal)
- ✅ Analytics (Google Analytics, Adobe)
- ✅ CDN providers (Cloudflare, Akamai)
- ❌ **NOT** backend infrastructure (Kubernetes, Docker, databases)

### Financial Data (~18% Coverage)
`sector`, `industry`, `market_cap`, `revenue` come from Yahoo Finance, which only covers actively traded stocks. ~82% of SEC filers are:
- Small/micro-cap companies not tracked by Yahoo
- Inactive or shell companies
- Trusts, SPVs, and special entities

**Workaround**: Use `SIMILAR_INDUSTRY` relationships instead of filtering by sector property.

### Supply Chain Relationships
`HAS_SUPPLIER`, `HAS_CUSTOMER`, `HAS_PARTNER` are extracted from 10-K text where companies explicitly name business partners. Large companies like Apple often use generic language ("key suppliers") rather than naming specific companies.

### Technology Predictions
`LIKELY_TO_ADOPT` predictions only exist for domains with limited technology stacks. Large tech companies already use many technologies, so predictions aren't computed for them.

---

---

## 8. Explainable Similarity

When similarity scores aren't enough, use **explainable similarity** to understand *why* companies are similar.

### CLI Tool: explain_similarity.py

```bash
# Text output (human-readable)
python scripts/explain_similarity.py KO PEP

# JSON output (for APIs/scripts)
python scripts/explain_similarity.py NVDA AMD --json

# Verbose mode (includes raw scores)
python scripts/explain_similarity.py AAPL MSFT --verbose
```

### Example Output

```
======================================================================
SIMILARITY EXPLANATION: NVDA vs AMD
======================================================================

Companies: NVIDIA CORP ↔ ADVANCED MICRO DEVICES INC
Total Score: 5.972 (Confidence: high)

SUMMARY
----------------------------------------
NVIDIA CORP and ADVANCED MICRO DEVICES INC are direct competitors,
face 91% similar risk factors, and have 80% similar business descriptions.

FEATURE BREAKDOWN
----------------------------------------
  [████████████████████████████████████████] COMPETITOR: 1.00
           └─ Direct competitor relationship cited in 10-K filings (100% confidence)
  [███████░░░] RISK: 0.91
           └─ Risk factor profiles are 91% similar
  [██████░░░░] DESCRIPTION: 0.80
           └─ Business descriptions are 80% similar
  [██████░░░░] INDUSTRY: 1.00
           └─ Same SIC code (3674) - strong industry match

PATH EVIDENCE
----------------------------------------
  • Both companies use: Akamai, Java, Adobe Experience Manager
  • Both cited as competitors by: MBLY, AVGO
  • Both source from: INTC, MSFT, AVGO

TOP REASONS
----------------------------------------
  1. Direct competitor relationship cited in 10-K filings (100% confidence)
  2. Risk factor profiles are 91% similar
  3. Business descriptions are 80% similar
======================================================================
```

### Python API

```python
from public_company_graph.company import explain_similarity, explain_similarity_to_dict
from public_company_graph.config import get_neo4j_database
from public_company_graph.neo4j.connection import get_neo4j_driver

driver = get_neo4j_driver()
db = get_neo4j_database()

# Get structured explanation
explanation = explain_similarity(driver, "KO", "PEP", database=db)
print(f"Total score: {explanation.total_score}")
print(f"Confidence: {explanation.confidence}")
print(f"Summary: {explanation.summary}")

for evidence in explanation.feature_breakdown:
    print(f"  {evidence.dimension.name}: {evidence.score:.2f} - {evidence.explanation}")

# Get JSON-serializable dict (for APIs)
result = explain_similarity_to_dict(driver, "NVDA", "AMD", database=db)

driver.close()
```

### What Gets Explained

| Dimension | Weight | Description |
|-----------|--------|-------------|
| `COMPETITOR` | 4.0 | Direct competitor relationship from 10-K |
| `DESCRIPTION` | 0.8 | Business description embedding similarity |
| `RISK` | 0.8 | Risk factor embedding similarity |
| `INDUSTRY` | 0.6 | Same SIC code / industry / sector |
| `TECHNOLOGY` | 0.3 | Shared web technologies (via domains) |
| `SIZE` | 0.2 | Similar revenue / market cap bucket |

### Path Evidence Types

- **Shared Technologies**: Web technologies both companies use
- **Common Competitor Cites**: Companies that cite both as competitors
- **Shared Competitors**: Companies both cite as competitors
- **Shared Customers**: Companies both sell to
- **Shared Suppliers**: Companies both source from

---

## Query Performance Tips

1. **Use indexed properties**: `ticker`, `cik` are indexed
2. **Limit early**: Add `LIMIT` before expensive operations when exploring
3. **Profile queries**: Use `PROFILE` prefix to see execution plan
4. **Use SIMILAR_* relationships**: More reliable than property-based filtering

---

## 9. Supply Chain Risk Analysis

Analyze supply chain concentration and vulnerability based on research from:
- **P25**: Cohen & Frazzini (2008) "Economic Links and Predictable Returns"
- **P26**: Barrot & Sauvagnat (2016) "Input Specificity and Propagation of Idiosyncratic Shocks"

### CLI Tool: analyze_supply_chain.py

```bash
# Analyze a company's supply chain risk
python scripts/analyze_supply_chain.py AVGO

# Analyze downstream exposure if a supplier has problems
python scripts/analyze_supply_chain.py --exposure MSFT

# Output as JSON
python scripts/analyze_supply_chain.py XERS --json

# Compute risk properties for all relationships (dry run)
python scripts/analyze_supply_chain.py --compute-all

# Execute risk property computation
python scripts/analyze_supply_chain.py --compute-all --execute
```

### Example: Company Risk Analysis

```
$ python scripts/analyze_supply_chain.py XERS

======================================================================
SUPPLY CHAIN RISK ANALYSIS: XERS
======================================================================

Company: Xeris Biopharma Holdings, Inc.
Suppliers analyzed: 1

High-risk suppliers: 1
Sole/single source suppliers: 1

⚠️  SOLE SOURCE DEPENDENCIES:
   • RGS: REGIS CORP

SUPPLIER RISK BREAKDOWN
----------------------------------------------------------------------
  RGS: REGIS CORP
    [███████████████░░░░░] Overall: 0.79
    Components: concentration=1.00, specificity=0.90, dependency=0.30
    Flags: ⚠️ SOLE SOURCE
```

### Example: Downstream Exposure Analysis

```
$ python scripts/analyze_supply_chain.py --exposure MSFT

======================================================================
SUPPLY CHAIN EXPOSURE ANALYSIS: MSFT
======================================================================

If MSFT experiences disruption, the following companies are affected:

Total downstream exposure: 212 companies

🔴 DIRECT CUSTOMERS (81)
   • AMD: ADVANCED MICRO DEVICES INC (impact: 0.50)
   • NVDA: NVIDIA CORP (impact: 0.50)
   • ORCL: ORACLE CORP (impact: 0.50)
   ...

🟡 INDIRECT CUSTOMERS (131 - 2nd order)
   • PENG: Penguin Solutions, Inc. (impact: 0.25)
   ...
```

### Risk Score Components

| Component | Weight | Description |
|-----------|--------|-------------|
| `concentration_risk` | 40% | Risk from supplier concentration (1/N suppliers, or explicit %) |
| `specificity_risk` | 35% | Risk from input specificity (hard to replace) |
| `dependency_risk` | 25% | Risk from explicit dependency language in 10-K |

### Python API

```python
from public_company_graph.supply_chain import (
    analyze_supply_chain_risk,
    analyze_supply_chain_exposure,
)
from public_company_graph.neo4j.connection import get_neo4j_driver
from public_company_graph.config import get_neo4j_database

driver = get_neo4j_driver()
db = get_neo4j_database()

# Analyze a company's supply chain risk
risks = analyze_supply_chain_risk(driver, "XERS", database=db)
for risk in risks:
    if risk.is_sole_source:
        print(f"⚠️ {risk.supplier_ticker}: {risk.supplier_name} (sole source!)")
    print(f"  Overall risk: {risk.overall_score:.2f}")

# Analyze downstream exposure
exposure = analyze_supply_chain_exposure(driver, "TSMC", database=db)
print(f"Total downstream exposure: {exposure['total_exposure']} companies")

driver.close()
```

### Cypher Queries

```cypher
// Find companies with sole-source suppliers (after computing risk properties)
MATCH (c:Company)-[r:HAS_SUPPLIER]->(s:Company)
WHERE r.is_sole_source = true
RETURN c.ticker, c.name, s.ticker, s.name, r.supply_chain_risk
ORDER BY r.supply_chain_risk DESC

// Find downstream exposure for a supplier
MATCH (supplier:Company {ticker: 'TSMC'})<-[:HAS_SUPPLIER*1..2]-(affected:Company)
RETURN DISTINCT affected.ticker, affected.name, length(path) as hops
ORDER BY hops ASC

// Find companies with highest overall supply chain risk
MATCH (c:Company)-[r:HAS_SUPPLIER]->(s:Company)
WHERE r.supply_chain_risk > 0.7
RETURN c.ticker, c.name, count(s) as high_risk_suppliers,
       avg(r.supply_chain_risk) as avg_risk
ORDER BY avg_risk DESC
LIMIT 20
```

---

## Related Documentation

- **[Graph Schema](graph_schema.md)** - Complete schema with all properties
- **[Architecture](architecture.md)** - How the data is loaded and computed
- **[Step-by-Step Guide](step_by_step_guide.md)** - Pipeline walkthrough

---

*Inspired by [CompanyKG: A Large-Scale Heterogeneous Graph for Company Similarity Quantification](https://arxiv.org/abs/2306.10649) (NeurIPS 2023)*
