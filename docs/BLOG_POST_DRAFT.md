# What 5,398 Companies Reveal About Themselves (And Each Other) in SEC Filings

*Building a knowledge graph from 10-K filings to discover competitive relationships, hidden rivals, and trillion-dollar blind spots.*

---

## The $6.5 Trillion Blind Spot

Here's a surprising discovery from analyzing SEC 10-K filings of every U.S. public company:

**Apple ($4.1T market cap) and Amazon ($2.5T market cap) — the world's two largest companies — are cited as competitors by almost no one in their SEC filings.**

| Company | Market Cap | Times Cited as Competitor |
|---------|-----------|--------------------------|
| Apple | $4.1 trillion | 58 (mostly tiny companies) |
| Amazon | $2.5 trillion | 0 |
| Pfizer | $165 billion | 84 |
| Microsoft | $3.0 trillion | 82 |

The most-cited competitors aren't the biggest companies. They're industry specialists like Pfizer (84 citations) and infrastructure giants like Microsoft (82 citations). Why? Because SEC disclosure rules don't require companies to name competitors unless they're material to risks — and when you're a $1M software startup, you're not competing with Amazon in any material way.

But smaller companies *do* cite them. A $1M mobile game company says they compete with Apple. A $9M radio broadcaster says Apple competes for advertising. They're not wrong — but Apple doesn't mention them back.

**This asymmetry reveals something important**: competitive disclosure in SEC filings captures *perceived* competitive pressure, not market share. And that perception flows upward.

---

## The Most Transparent (and Most Secretive) Companies

Some companies name 27 competitors. Others name zero.

### Most Transparent About Competition

| Company | Market Cap | Competitors Disclosed |
|---------|-----------|----------------------|
| **Broadcom** | $1.7T | 27 |
| AMD | $349B | 8 |
| Booking Holdings | $175B | 6 |
| Arch Capital | $35B | 7 |

Broadcom ($1.7T) discloses 27 competitors in their 10-K, from AMD to Intel to Microsoft. They explicitly say: *"In semiconductor solutions, we compete with integrated device manufacturers, fabless semiconductor companies..."*

### Completely Silent About Competition

| Company | Market Cap | Competitors Disclosed |
|---------|-----------|----------------------|
| **Apple** | $4.1T | 0 |
| **Amazon** | $2.5T | 0 |
| **Bank of America** | $410B | 0 |
| **AbbVie** | $408B | 0 |
| **Abbott Labs** | $217B | 0 |
| **Blackstone** | $190B | 0 |

The pattern is clear: **the biggest companies say the least about competition.** This isn't negligence — it's strategy. When you're $4 trillion, naming competitors validates them.

---

## Hidden Rivals: 90% Similar, Never Mentioned

The graph reveals pairs of companies that are:
- 85%+ similar by business description
- Competing against 5+ of the same companies
- But **never mention each other** as competitors

| Company A | Company B | Shared Competitors | Description Similarity |
|-----------|-----------|-------------------|----------------------|
| Caribou Biosciences | MiNK Therapeutics | 8 | 90% |
| Caribou Biosciences | Alaunos Therapeutics | 8 | 89% |
| Adicet Bio | Celularity | 9 | 86% |
| Adicet Bio | Alaunos Therapeutics | 7 | 89% |

These are cell therapy companies competing against the exact same rivals (Fate Therapeutics, Atara Biotherapeutics, CRISPR Therapeutics) but apparently unaware of each other. The graph surfaces relationships that individual filings miss.

---

## Triangular Rivalries: When Everyone Knows Everyone

Some competitive clusters are so tight that every company cites every other company:

**Semiconductor RF Chips:**
- Broadcom ↔ Skyworks ↔ Qorvo

**Commercial Real Estate Services:**
- CBRE ↔ Jones Lang LaSalle ↔ Newmark

**Contract Electronics Manufacturing:**
- Benchmark Electronics ↔ Celestica ↔ Sanmina

**Cell Therapy:**
- Caribou Biosciences ↔ Century Therapeutics ↔ Sana Biotechnology

These are *complete competitive triangles* — each company cites both others as competitors. This validates the relationship and makes these some of the most reliable competitive edges in the graph.

---

## David vs. Goliath: The 3.4-Million-to-One Ratio

The most extreme competitive asymmetry in the data:

| Small Company | Market Cap | Cites As Competitor | Market Cap | Size Ratio |
|---------------|-----------|---------------------|-----------|------------|
| Appsoft Technologies | $1M | Apple | $4.1T | **3,381,633x** |
| Beasley Broadcast | $9M | Apple | $4.1T | 439,421x |

A $1M company saying they compete with a $4.1T company. That's not hubris — it's SEC-required disclosure of material competitive risks. The graph captures these because they're self-declared, not inferred.

---

## What You Can't Get From Bloomberg

Traditional financial databases classify companies by industry codes. This graph captures:

1. **Self-declared competitive relationships** — What companies say about themselves, not what analysts infer
2. **Explainable similarity** — *Why* two companies are similar (description, risk profile, technology stack)
3. **Competitive asymmetry** — Who cites whom (and who ignores whom)
4. **Multi-hop relationships** — NVIDIA cites AMD, AMD cites Intel, Intel cites NVIDIA...

### Example: Why Are Coca-Cola and PepsiCo Similar?

```
Description similarity: 88%
Risk profile similarity: 87%
Same industry: Yes
KO cites PEP as competitor: No
PEP cites KO as competitor: No
```

They're 88% similar by business description, face 87% similar risks, and are in the exact same industry — **but neither explicitly names the other as a competitor in their 10-K.** Some things are so obvious they go unsaid.

---

## Building the Graph

The graph contains:
- **5,398 Companies** with business descriptions, risk factors, and market data
- **3,249 Competitive relationships** extracted from 10-K filings (embedding-verified)
- **2+ million similarity edges** (description, risk, industry, technology stack)
- **Every edge has evidence** — the exact sentence from the filing that establishes the relationship

### Data Quality

- Competitive relationships use embedding similarity (≥0.35 threshold) to filter false positives
- Supply chain relationships (130 suppliers, 243 customers) are LLM-verified for precision
- Every relationship includes the source context from the 10-K filing

---

## Key Insights for Analysts

1. **Competitive disclosure flows upward**: Small companies cite large competitors; large companies cite peers or no one.

2. **Silence is strategic**: The biggest companies disclose the fewest competitors. Apple and Amazon name zero.

3. **Graph reveals hidden rivals**: Companies with 90%+ similar descriptions and shared competitors often don't acknowledge each other.

4. **Triangular clusters validate relationships**: When three companies all cite each other, the competitive relationship is highly reliable.

5. **SEC filings capture perceived threats, not market share**: A $1M company citing Apple tells you about *their* competitive pressures, not Apple's.

---

## Try It Yourself

The full graph is open source:

```bash
git clone https://github.com/alexwoolford/public-company-graph
cd public-company-graph
git lfs pull  # Download the database dump

# Restore to Neo4j (must be stopped)
neo4j-admin database load neo4j --from-path=data/ --overwrite-destination=true
neo4j start
```

Then run queries like:

```cypher
// Find the "sleeping giants" - big companies that name no competitors
MATCH (c:Company)
WHERE c.market_cap > 50000000000
AND NOT EXISTS { (c)-[:HAS_COMPETITOR]->() }
RETURN c.ticker, c.name, round(c.market_cap / 1e9) as market_cap_B
ORDER BY c.market_cap DESC
LIMIT 10
```

---

## What's Next?

This is a snapshot. Future work could:
- Add **temporal analysis** (how do competitive relationships change over 5 years?)
- Add **M&A relationships** from 8-K filings
- Add **executive/board relationships** from proxy statements
- Enable **predictive analytics** (which companies will become competitors?)

The graph is reproducible — every step from SEC filing to Neo4j is scripted and documented.

---

*Built with Neo4j, Python, OpenAI embeddings, and SEC EDGAR data. Inspired by [CompanyKG](https://arxiv.org/abs/2306.10649) (NeurIPS 2023).*
