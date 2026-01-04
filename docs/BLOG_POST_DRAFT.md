# What 5,398 Companies Reveal About Competition in Their SEC Filings

*Analyzing 10-K filings to discover who acknowledges whom as competitors — and who stays silent.*

---

## The Infinite Threat Ratio

We built a knowledge graph from SEC 10-K filings to map competitive relationships. The most striking finding? **The companies most frequently cited as competitors never cite anyone back.**

| Company | Cited By | Cites | Threat Ratio |
|---------|----------|-------|--------------|
| **Pfizer** | 84 companies | 0 | ∞ |
| **Apple** | 58 companies | 0 | ∞ |
| **Amgen** | 52 companies | 0 | ∞ |
| **AbbVie** | 46 companies | 0 | ∞ |
| **Medtronic** | 42 companies | 0 | ∞ |
| Microsoft | 82 companies | 4 | 20.5x |
| Oracle | 44 companies | 7 | 6.3x |

Pfizer appears in 84 companies' 10-K filings as a competitor. Pfizer's own 10-K names zero.

**This isn't negligence — it's strategy.** When you're the market leader, naming competitors validates them. When you're a $50M biotech, naming Pfizer explains your risk factors.

---

## The Asymmetry of Competitive Awareness

SEC filings don't require companies to name competitors. They require disclosure of *material* competitive risks. That creates a one-way information flow:

**Small companies cite large companies.** A $1 million mobile game company (Appsoft Technologies) explicitly names Apple as a competitor: *"We could face increased competition if large companies...such as Apple, Google, Amazon...choose to enter or expand in the games space."*

**Large companies cite no one.** Apple's 10-K doesn't name Appsoft — because Appsoft isn't a material risk to Apple.

The most extreme example: Appsoft ($1M market cap) cites Apple ($4.1T market cap) — a **3.4-million-to-one** size ratio.

This asymmetry means competitive edges in SEC filings measure *perceived threat*, not market share. Small companies feel threatened by giants; giants don't feel threatened by anyone they'd name.

---

## Who Actually Discloses Competitors?

The most transparent company in our dataset is **Broadcom** — they name 27 competitors in their 10-K, from AMD to Intel to Microsoft to Cisco. They're also highly cited (16 companies name them), creating a balanced competitive profile:

| Company | Cited By | Cites | Notes |
|---------|----------|-------|-------|
| **Broadcom** | 16 | 27 | Most transparent |
| Oracle | 44 | 7 | Tech leader, names some |
| Allogene Therapeutics | 11 | 12 | Balanced biotech |
| Qorvo | 9 | 7 | Semiconductor, balanced |

**Why Broadcom?** They operate in semiconductors (highly competitive, many named players) and enterprise software (also competitive). Their 10-K explicitly lists competitors by segment: *"In semiconductor solutions, we compete with integrated device manufacturers, fabless semiconductor companies..."*

Compare to **Amazon** ($2.5T market cap): cited by zero companies as a competitor, names zero competitors. Complete competitive silence.

---

## Hidden Rivals: 90% Similar, Never Mentioned

The graph reveals pairs of companies that should know about each other — but apparently don't:

| Company A | Company B | Shared Competitors | Description Similarity |
|-----------|-----------|-------------------|----------------------|
| Caribou Biosciences | MiNK Therapeutics | 8 | 90% |
| Caribou Biosciences | Alaunos Therapeutics | 8 | 89% |
| Adicet Bio | Celularity | 9 | 86% |

These are cell therapy companies. They:
- Describe themselves 86-90% similarly (by embedding comparison)
- Compete against the same 7-9 companies (Fate Therapeutics, CRISPR Therapeutics, Atara Biotherapeutics)
- **Never mention each other**

Why? Probably because they're all relatively small and focused on different therapeutic targets within cell therapy. But from an investor or analyst perspective, these are *hidden rivals* — similar enough to be substitutes, competing for the same talent and capital, but not on each other's radar.

The graph surfaces relationships that individual filings miss.

---

## Triangular Rivalries

Some competitive clusters are so tight that every company acknowledges every other. These are complete competitive triangles — the most validated relationships in the graph:

**Semiconductor RF Chips:**
- Broadcom ↔ Skyworks ↔ Qorvo
- All three name each other as competitors

**Commercial Real Estate:**
- CBRE ↔ Jones Lang LaSalle ↔ Newmark
- Complete mutual acknowledgment

**Contract Electronics Manufacturing:**
- Benchmark Electronics ↔ Celestica ↔ Sanmina
- All compete for the same manufacturing contracts

**Cell Therapy:**
- Caribou Biosciences ↔ Century Therapeutics ↔ Sana Biotechnology
- Despite some pairs being "hidden rivals," this triangle is fully acknowledged

When three companies all cite each other, the competitive relationship is highly reliable. These triangles are the gold standard of SEC-derived competitive intelligence.

---

## The KO-PEP Paradox

Some rivalries are so obvious they go unspoken:

```
Coca-Cola vs. PepsiCo:
  Description similarity: 88%
  Risk profile similarity: 87%
  Same industry: Yes
  KO cites PEP as competitor: No
  PEP cites KO as competitor: No
```

Neither Coca-Cola nor PepsiCo names the other in their 10-K filings. They're 88% similar by business description, face 87% similar risks, and are in the exact same industry — but explicit competitor disclosure? Zero.

This suggests a third category beyond "transparent" and "silent": **implicitly known**. Some competitive relationships are so universal that naming them adds no information.

---

## What You Can't Get From Traditional Databases

Bloomberg and Refinitiv classify companies by industry codes. This graph captures something different:

| Dimension | Traditional | This Graph |
|-----------|------------|------------|
| **Competition** | Inferred from industry | Self-declared in filings |
| **Similarity** | Single dimension (sector) | Four dimensions (description, risk, industry, tech) |
| **Evidence** | None | Exact sentence from filing |
| **Asymmetry** | Not captured | Who cites whom |

Traditional databases tell you Apple and Microsoft are both "Technology." The graph tells you that 82 companies cite Microsoft as a competitor, 4 companies Microsoft cites back, and exactly which sentences establish each relationship.

---

## Graph Statistics

| Metric | Count |
|--------|-------|
| Companies | 5,398 |
| Competitive relationships | 3,249 |
| Similarity edges | 2+ million |
| Complete triangles | 4 verified |

**Data quality:**
- Competitive edges are embedding-verified (≥0.35 similarity threshold)
- Supply chain edges (130 suppliers, 243 customers) are LLM-verified
- Every edge stores the source sentence from the 10-K

---

## Key Takeaways

1. **Competitive disclosure flows upward.** Small companies cite large; large cite peers or no one.

2. **Infinite threat ratios are real.** Pfizer is cited 84 times, cites zero. This is strategy, not negligence.

3. **Hidden rivals exist.** Companies 90% similar may never acknowledge each other.

4. **Triangles are gold.** When three companies all cite each other, that's validated competitive intelligence.

5. **Silence is data.** Amazon naming zero competitors tells you as much as Broadcom naming 27.

---

## Try It Yourself

The full graph is open source:

```bash
git clone https://github.com/alexwoolford/public-company-graph
cd public-company-graph
git lfs pull

# Restore to Neo4j (must be stopped)
neo4j-admin database load neo4j --from-path=data/ --overwrite-destination=true
neo4j start
```

Find the "infinite threat ratio" companies:

```cypher
MATCH (c:Company)<-[inbound:HAS_COMPETITOR]-(:Company)
WITH c, count(inbound) as cited_by
WHERE cited_by >= 10
OPTIONAL MATCH (c)-[outbound:HAS_COMPETITOR]->(:Company)
WITH c, cited_by, count(outbound) as cites
WHERE cites = 0
RETURN c.ticker, c.name, cited_by
ORDER BY cited_by DESC
```

---

*Built with Neo4j, Python, OpenAI embeddings, and SEC EDGAR data. Inspired by [CompanyKG](https://arxiv.org/abs/2306.10649) (NeurIPS 2023).*
