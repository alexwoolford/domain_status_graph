# High-Value Business Intelligence Queries

This document provides **tested, verified queries** that return meaningful results from the public company graph. Every example has been run and validated.

---

## Graph Statistics

| Metric | Count |
|--------|-------|
| **Companies** | 5,398 |
| **Domains** | 4,337 |
| **Technologies** | 827 |
| **Total Relationships** | ~2,024,000 |

### Relationship Counts (Verified)

| Relationship Type | Count | Description |
|-------------------|-------|-------------|
| `SIMILAR_INDUSTRY` | 520,672 | Same sector/industry classification |
| `SIMILAR_DESCRIPTION` | 436,973 | Business description embedding similarity |
| `SIMILAR_SIZE` | 414,096 | Revenue/market cap similarity |
| `SIMILAR_RISK` | 394,372 | Risk factor profile similarity |
| `SIMILAR_TECHNOLOGY` | 124,584 | Shared web technology stacks |
| `USES` | 46,081 | Domain → Technology detection |
| `LIKELY_TO_ADOPT` | 41,250 | Technology adoption prediction |
| `CO_OCCURS_WITH` | 41,220 | Technology co-occurrence |
| `HAS_DOMAIN` | 3,745 | Company → Domain link |
| `HAS_COMPETITOR` | 3,249 | Explicit competitor citation (high confidence) |
| `CANDIDATE_PARTNER` | 673 | Partner mention (medium confidence) |
| `HAS_PARTNER` | 588 | Partner citation (high confidence) |
| `HAS_CUSTOMER` | 243 | Customer relationship (LLM verified) |
| `CANDIDATE_SUPPLIER` | 151 | Supplier mention (medium confidence) |
| `HAS_SUPPLIER` | 130 | Supplier relationship (LLM verified) |
| `CANDIDATE_CUSTOMER` | 113 | Customer mention (medium confidence) |
| `CANDIDATE_COMPETITOR` | 111 | Competitor mention (medium confidence) |

### Coverage

| Metric | Coverage |
|--------|----------|
| Companies with competitor edges | 1,389 (25.7%) |
| Companies with supply chain edges | 281 (5.2%) |
| Companies with business descriptions | 5,390 (99.85%) |
| Companies with Yahoo Finance data | ~960 (18%) |

---

## 1. Competitive Intelligence

### Who Are the Market Leaders?

Find companies most frequently cited as competitors across all 10-K filings. These are the dominant players that everyone considers a threat:

```cypher
MATCH (c:Company)<-[r:HAS_COMPETITOR]-(:Company)
WITH c, count(r) as inbound_citations
ORDER BY inbound_citations DESC
LIMIT 10
RETURN c.ticker, c.name, inbound_citations
```

**Verified Results:**

| Ticker | Company | Times Cited |
|--------|---------|-------------|
| PFE | Pfizer Inc | 84 |
| MSFT | Microsoft Corp | 82 |
| AAPL | Apple Inc. | 58 |
| AMGN | Amgen Inc | 52 |
| ABBV | AbbVie Inc. | 46 |
| ORCL | Oracle Corp | 44 |
| MDT | Medtronic plc | 42 |
| ABT | Abbott Laboratories | 22 |
| TMO | Thermo Fisher Scientific | 22 |
| ACN | Accenture plc | 22 |

**Insight**: Pfizer and Microsoft are cited as competitors by 80+ other companies.

---

### Threat Ratio: Dominant Companies That Don't Look Back

Find companies that are frequently cited as competitors but rarely cite others. These are market dominators:

```cypher
MATCH (c:Company)<-[inbound:HAS_COMPETITOR]-(:Company)
WITH c, count(inbound) as cited_by
MATCH (c)-[outbound:HAS_COMPETITOR]->(:Company)
WITH c, cited_by, count(outbound) as cites
WHERE cited_by >= 10
RETURN c.ticker, c.name, cited_by, cites,
       round(toFloat(cited_by)/cites * 10) / 10 as threat_ratio
ORDER BY threat_ratio DESC
LIMIT 10
```

**Verified Results:**

| Ticker | Company | Cited By | Cites | Ratio |
|--------|---------|----------|-------|-------|
| WMT | Walmart Inc. | 21 | 1 | 21.0x |
| MSFT | Microsoft Corp | 82 | 4 | 20.5x |
| BIIB | Biogen Inc. | 18 | 1 | 18.0x |
| REGN | Regeneron | 16 | 1 | 16.0x |
| CAT | Caterpillar Inc | 13 | 1 | 13.0x |
| PANW | Palo Alto Networks | 12 | 1 | 12.0x |
| GOOGL | Alphabet Inc. | 20 | 2 | 10.0x |
| V | Visa Inc. | 16 | 2 | 8.0x |

**Insight**: Walmart is cited as a competitor 21x more than it cites others. Microsoft has an 82:4 asymmetry.

---

### True Rivalries: Mutual Competitors

Find companies that cite each other as competitors (both acknowledge the rivalry):

```cypher
MATCH (a:Company)-[:HAS_COMPETITOR]->(b:Company)-[:HAS_COMPETITOR]->(a)
WHERE a.ticker < b.ticker
RETURN a.ticker, a.name, b.ticker, b.name
LIMIT 10
```

**Verified Results:**

| Company A | Company B |
|-----------|-----------|
| Corcept Therapeutics (CORT) | Xeris Biopharma (XERS) |
| Beam Therapeutics (BEAM) | Prime Medicine (PRME) |
| LiveOne (LVO) | PodcastOne (PODC) |
| ZoomInfo (GTM) | TechTarget (TTGT) |
| Cirrus Logic (CRUS) | Skyworks Solutions (SWKS) |
| Qorvo (QRVO) | Skyworks Solutions (SWKS) |
| Broadcom (AVGO) | IBM (IBM) |
| Cigna (CI) | CVS Health (CVS) |
| Monster Beverage (MNST) | PepsiCo (PEP) |

**Insight**: These are the most validated competitive relationships—both companies acknowledge the other.

---

### Aspiring Disruptors

Find smaller companies that cite tech giants as competitors (but the giants don't cite them back):

```cypher
MATCH (small:Company)-[r:HAS_COMPETITOR]->(large:Company)
WHERE NOT (large)-[:HAS_COMPETITOR]->(small)
AND large.ticker IN ['MSFT', 'AAPL', 'GOOGL', 'AMZN', 'META']
RETURN small.ticker, small.name, large.ticker as giant,
       left(r.context, 150) as evidence
LIMIT 10
```

**Verified Results:**

| Ticker | Company | Targets | Evidence |
|--------|---------|---------|----------|
| SNAL | Snail, Inc. | MSFT | "We compete with...Sony, Nintendo, and Microsoft" |
| QBTS | D-Wave Quantum | MSFT | "competitors include...Google, IBM, Microsoft, Intel" |
| SOLV | Solventum Corp | MSFT | "competitors include Optum, Microsoft (Nuance), Epic" |
| SAIL | SailPoint | MSFT | "competitors include IBM, Microsoft, and Oracle" |
| HPQ | HP Inc | MSFT | "primary competitors are Lenovo, Dell...Microsoft" |
| IBM | IBM | MSFT | "competitors include Alphabet, Amazon...Microsoft" |

**Insight**: Even IBM cites Microsoft as a competitor, but Microsoft doesn't cite IBM back.

---

### Shared Competitor Networks

Find companies competing in the same space (they cite the same competitors):

```cypher
MATCH (c1:Company)-[:HAS_COMPETITOR]->(shared:Company)<-[:HAS_COMPETITOR]-(c2:Company)
WHERE c1 <> c2 AND id(c1) < id(c2)
WITH c1, c2, count(shared) as shared_competitors, collect(shared.ticker) as common
WHERE shared_competitors >= 5
RETURN c1.ticker, c1.name, c2.ticker, c2.name, shared_competitors, common[0..5] as sample_shared
ORDER BY shared_competitors DESC
LIMIT 10
```

**Verified Results:**

| Company 1 | Company 2 | Shared | Sample Shared Competitors |
|-----------|-----------|--------|---------------------------|
| Adicet Bio (ACET) | Celularity (CELU) | 9 | AMGN, GILD, FATE, ATRA |
| Alaunos (TCRT) | Caribou Biosciences (CRBU) | 8 | IBRX, PGEN, FATE, ATRA |
| Caribou (CRBU) | MiNK Therapeutics (INKT) | 8 | FATE, ATRA, CRSP, ACET |
| AMD (AMD) | Broadcom (AVGO) | 6 | ADI, INTC, TXN, NVDA |
| Roblox (RBLX) | Super League (SLE) | 6 | AAPL, MSFT, NFLX, META |

**Insight**: Cell/gene therapy companies form tight competitive clusters with 6-9 shared competitors.

---

### NVIDIA's Competitive Landscape

See who NVIDIA says they compete with, including context:

```cypher
MATCH (n:Company {ticker: 'NVDA'})-[r:HAS_COMPETITOR]->(comp:Company)
RETURN comp.ticker, comp.name, left(r.context, 200) as evidence
```

**Verified Result:**

| Ticker | Company | Evidence |
|--------|---------|----------|
| AMD | Advanced Micro Devices | "Our current competitors include: suppliers and licensors of hardware and software for discrete and integrated GPUs, custom chips..." |

And their extended 2-hop network:

```cypher
MATCH (nvda:Company {ticker: 'NVDA'})-[:HAS_COMPETITOR]->(direct:Company)
OPTIONAL MATCH (direct)-[:HAS_COMPETITOR]->(indirect:Company)
WHERE indirect <> nvda
RETURN collect(DISTINCT direct.ticker) as direct_competitors,
       collect(DISTINCT indirect.ticker) as competitors_of_competitors
```

**Result**: Direct = [AMD], Indirect = [ADI, INTC, TXN, LSCC, NXPI, AVGO, MRVL]

---

## 2. Company Similarity

### Find Companies Similar to Apple

```cypher
MATCH (apple:Company {ticker: 'AAPL'})-[r:SIMILAR_DESCRIPTION]->(similar:Company)
WHERE r.score > 0.70
RETURN similar.ticker, similar.name, round(r.score * 100) / 100 as similarity
ORDER BY r.score DESC
LIMIT 10
```

**Verified Results:**

| Ticker | Company | Similarity |
|--------|---------|------------|
| JAMF | Jamf Holding Corp. | 0.76 |
| FORM | FormFactor Inc | 0.74 |
| WDC | Western Digital | 0.73 |
| IDCC | InterDigital | 0.73 |
| CRUS | Cirrus Logic | 0.73 |
| SNDK | Sandisk Corp | 0.73 |
| MSFT | Microsoft Corp | 0.72 |

---

### Find Companies Similar to Tesla

```cypher
MATCH (target:Company {ticker: 'TSLA'})-[r:SIMILAR_DESCRIPTION]->(similar:Company)
WHERE r.score > 0.75
RETURN similar.ticker, similar.name, round(r.score * 100) / 100 as similarity
ORDER BY r.score DESC
LIMIT 10
```

**Verified Results:**

| Ticker | Company | Similarity |
|--------|---------|------------|
| FTCI | FTC Solar, Inc. | 0.80 |
| RUN | Sunrun Inc. | 0.80 |
| RIVN | Rivian Automotive | 0.80 |
| GM | General Motors | 0.79 |
| ENPH | Enphase Energy | 0.79 |
| GWH | ESS Tech, Inc. | 0.78 |
| TEL | TE Connectivity | 0.78 |

---

### Explainable Similarity: KO vs PEP

Understand *why* two companies are similar across multiple dimensions:

```cypher
MATCH (c1:Company {ticker: 'KO'}), (c2:Company {ticker: 'PEP'})
OPTIONAL MATCH (c1)-[r1:SIMILAR_DESCRIPTION]->(c2)
OPTIONAL MATCH (c1)-[r2:SIMILAR_RISK]->(c2)
OPTIONAL MATCH (c1)-[r3:SIMILAR_INDUSTRY]->(c2)
OPTIONAL MATCH (c1)-[r4:HAS_COMPETITOR]->(c2)
OPTIONAL MATCH (c2)-[r5:HAS_COMPETITOR]->(c1)
RETURN
    round(r1.score * 100) / 100 as description_similarity,
    round(r2.score * 100) / 100 as risk_similarity,
    r3.score as industry_match,
    CASE WHEN r4 IS NOT NULL THEN 'yes' ELSE 'no' END as ko_cites_pep,
    CASE WHEN r5 IS NOT NULL THEN 'yes' ELSE 'no' END as pep_cites_ko
```

**Verified Result:**

| Dimension | Score |
|-----------|-------|
| Description | 0.88 |
| Risk | 0.87 |
| Industry | 1.0 (same) |
| KO cites PEP | No |
| PEP cites KO | No |

**Insight**: 88% similar descriptions, 87% similar risks, same industry—but **neither explicitly names the other as a competitor** in their 10-K!

---

### Multi-Dimensional Similarity

Find companies similar across description, risk, AND industry:

```cypher
MATCH (target:Company {ticker: 'NVDA'})
OPTIONAL MATCH (target)-[desc:SIMILAR_DESCRIPTION]->(c:Company)
OPTIONAL MATCH (target)-[risk:SIMILAR_RISK]->(c)
OPTIONAL MATCH (target)-[ind:SIMILAR_INDUSTRY]->(c)
WITH c, desc, risk, ind
WHERE c IS NOT NULL AND desc IS NOT NULL AND risk IS NOT NULL
WITH c,
     COALESCE(desc.score, 0) AS desc_score,
     COALESCE(risk.score, 0) AS risk_score,
     COALESCE(ind.score, 0) AS ind_score,
     (desc.score * 0.4 + risk.score * 0.3 + ind.score * 0.3) AS weighted_score
WHERE weighted_score > 0.5
RETURN c.ticker, c.name,
       round(weighted_score * 100) / 100 as overall,
       round(desc_score * 100) / 100 as description,
       round(risk_score * 100) / 100 as risk,
       round(ind_score * 100) / 100 as industry
ORDER BY weighted_score DESC
LIMIT 10
```

**Verified Results:**

| Ticker | Company | Overall | Desc | Risk | Industry |
|--------|---------|---------|------|------|----------|
| AMD | Advanced Micro Devices | 0.89 | 0.80 | 0.91 | 1.00 |
| ALAB | Astera Labs | 0.87 | 0.76 | 0.89 | 1.00 |
| AMBA | Ambarella | 0.86 | 0.74 | 0.89 | 1.00 |
| SYNA | Synaptics | 0.86 | 0.75 | 0.87 | 1.00 |
| MU | Micron Technology | 0.85 | 0.74 | 0.87 | 1.00 |
| NTNX | Nutanix | 0.58 | 0.78 | 0.89 | 0.00 |

**Insight**: AMD is NVIDIA's closest match (89% weighted similarity). Nutanix is similar by description/risk but different industry.

---

## 3. Supply Chain Intelligence

> **Note**: Supply chain data is sparse (~5% coverage) because SEC filings don't require supplier disclosure. Large tech companies use generic language. The relationships that DO exist are **LLM-verified and high quality**.

### Key Suppliers (Most Referenced)

```cypher
MATCH (s:Company)<-[:HAS_SUPPLIER]-(c:Company)
WITH s, count(c) as customer_count, collect(c.ticker) as customers
ORDER BY customer_count DESC
LIMIT 10
RETURN s.ticker, s.name, customer_count, customers[0..5] as sample_customers
```

**Verified Results:**

| Ticker | Supplier | Customers | Sample |
|--------|----------|-----------|--------|
| BA | Boeing Co | 7 | GE, MOG-A, SIF, LUV, UAL |
| ILMN | Illumina | 7 | EXAS, ADPT, PSNL, GH, NTRA |
| INTC | Intel Corp | 5 | NATL, HPQ, VYX, FTNT, SMCI |
| TMO | Thermo Fisher | 4 | SVRA, OPGN, RCEL, HUMA |
| NVDA | NVIDIA | 4 | IREN, APLD, SMCI, BTBT |
| CMI | Cummins | 4 | ATMU, FIX, ET, USAC |
| ORCL | Oracle | 4 | BRLT, INUV, CALX |
| JBL | Jabil | 3 | COHU, TRMB, LIF |

---

### Sample Verified Supplier Relationships

Every `HAS_SUPPLIER` edge has been LLM-verified with evidence:

```cypher
MATCH (c:Company)-[r:HAS_SUPPLIER]->(s:Company)
WHERE r.llm_verified = true
RETURN c.ticker, c.name, s.ticker, s.name, left(r.context, 200) as evidence
LIMIT 5
```

**Verified Results:**

| Company | Supplier | Evidence |
|---------|----------|----------|
| Brilliant Earth (BRLT) | Salesforce (CRM) | "We outsource substantially all of our core cloud infrastructure services to...Salesforce and Oracle" |
| IREN Ltd (IREN) | NVIDIA (NVDA) | "we procured...approximately 5.5k NVIDIA B200 GPUs, 2.3k NVIDIA B300 GPUs" |
| Atmus Filtration (ATMU) | Cummins (CMI) | "Atmus entered into a first-fit supply agreement...with Cummins" |
| United Airlines (UAL) | Boeing (BA) | "The Company currently sources the majority of its aircraft...from Boeing" |

---

### Sample Verified Customer Relationships

Every `HAS_CUSTOMER` edge has been LLM-verified with evidence:

```cypher
MATCH (c:Company)-[r:HAS_CUSTOMER]->(cust:Company)
WHERE r.llm_verified = true
RETURN c.ticker, c.name, cust.ticker, cust.name, left(r.context, 200) as evidence
LIMIT 5
```

**Verified Results:**

| Company | Customer | Evidence |
|---------|----------|----------|
| Crescent Energy (CRGY) | ConocoPhillips (COP) | "ConocoPhillips represented approximately...15% of our consolidated revenues" |
| Embecta (EMBC) | McKesson (MCK) | "gross sales to McKesson Corporation, Cardinal Health and Cencora...represented approximately 40%" |
| Embecta (EMBC) | Cardinal Health (CAH) | Same context - three largest distributors |
| Mobileye (MBLY) | Aptiv (APTV) | "Aptiv accounted for...14% of our revenue" |

---

## 4. Technology Intelligence

> **Note**: Technology data is **web-only** (JavaScript, CMS, CDN, analytics). Not backend infrastructure.

### What Technologies Does Amazon Use?

```cypher
MATCH (c:Company {ticker: 'AMZN'})-[:HAS_DOMAIN]->(d:Domain)-[:USES]->(t:Technology)
RETURN t.name, t.category
ORDER BY t.category, t.name
```

**Verified Results:**

| Technology | Category |
|------------|----------|
| Amazon CloudFront | CDN |
| React | JavaScript frameworks |
| HTTP/3 | Miscellaneous |
| Amazon Web Services | PaaS |
| HSTS | Security |

---

### Technology Co-Occurrence (What Goes with WordPress?)

```cypher
MATCH (t1:Technology {name: 'WordPress'})-[r:CO_OCCURS_WITH]->(t2:Technology)
WHERE r.similarity > 0.3
RETURN t2.name, round(r.similarity * 100) / 100 as affinity
ORDER BY r.similarity DESC
LIMIT 10
```

**Verified Results:**

| Technology | Affinity |
|------------|----------|
| MySQL | 0.98 |
| PHP | 0.89 |
| jQuery Migrate | 0.86 |
| Yoast SEO | 0.83 |
| jQuery | 0.73 |
| Google Analytics | 0.69 |
| Cloudflare | 0.64 |
| Google Tag Manager | 0.64 |

**Insight**: WordPress is almost always deployed with MySQL (98% affinity).

---

## 5. Industry Clusters

### Cell/Gene Therapy Competitive Cluster

This industry has dense competitor networks:

```cypher
MATCH (c:Company)-[r:HAS_COMPETITOR]->(comp:Company)
WHERE c.ticker IN ['CRBU', 'ALLO', 'CELU']
RETURN c.ticker, c.name, count(comp) as competitors_cited,
       collect(comp.ticker)[0..8] as sample_competitors
```

**Verified Results:**

| Ticker | Company | # Competitors | Sample |
|--------|---------|---------------|--------|
| CRBU | Caribou Biosciences | 22 | KYTX, SGMO, IBRX, PGEN, FATE, ATRA |
| ALLO | Allogene Therapeutics | 12 | KYTX, AMGN, FATE, ABBV, CRSP |
| CELU | Celularity Inc | 12 | AMGN, INCY, GILD, FATE, ATRA |

**Insight**: Cell therapy companies cite 12-22 competitors each—far more transparent than big tech.

---

## 6. Explainability Tools

### CLI: Explain Similarity

```bash
python scripts/explain_similarity.py KO PEP
python scripts/explain_similarity.py NVDA AMD --json
```

### CLI: Analyze Supply Chain Risk

```bash
python scripts/analyze_supply_chain.py XERS
python scripts/analyze_supply_chain.py --exposure MSFT
```

---

## Data Quality Notes

### What This Graph Does Well

- ✅ **Competitor relationships** from 10-K filings (unique data, self-declared)
- ✅ **Business description similarity** (99.85% coverage, high quality embeddings)
- ✅ **Risk profile similarity** (identifies companies facing similar threats)
- ✅ **LLM-verified supply chain** (small but high precision)
- ✅ **Explainable relationships** (every edge has context/evidence)

### Limitations

- ❌ **Supply chain is sparse** (~5% coverage) - SEC doesn't require supplier disclosure
- ❌ **Yahoo Finance data** only for ~18% of companies
- ❌ **Web technologies only** - no backend (Kubernetes, Docker, etc.)
- ❌ **Single snapshot** - no historical time series
- ❌ **Large tech companies** often use generic language, missing specific relationships

---

## Related Documentation

- **[Graph Schema](graph_schema.md)** - Complete schema with all properties
- **[Architecture](architecture.md)** - How data is loaded and computed
- **[README](../README.md)** - Quick start guide

---

*Graph inspired by [CompanyKG](https://arxiv.org/abs/2306.10649) (NeurIPS 2023). Competitor extraction uses embedding similarity + LLM verification for supply chain relationships.*
