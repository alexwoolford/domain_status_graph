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

## Insights Only a Graph Can Reveal

Some patterns are invisible when reading individual filings. They only emerge from graph traversal:

### 92.5% of Competition is One-Way

```
Total competitive edges: 3,249
Mutual pairs (both cite each other): 122
Mutual edges: 244 (7.5%)
One-way edges: 3,005 (92.5%)
```

Almost all competitive relationships are asymmetric. Company A names Company B, but B doesn't name A. You'd never know this from reading any single filing.

### Six Degrees of Microsoft

From Microsoft, you can reach 1,403 companies through competitive relationships:

| Hops | Companies Reachable |
|------|---------------------|
| 1 | 85 |
| 2 | 190 |
| 3 | 224 |
| 4 | 295 |
| 5 | 329 |
| 6 | 280 |

The competitive network is highly connected. Most companies competing in the same space are 3-5 hops apart.

### Competitive Chains Span Industries

```
CareCloud → Veradigm → EverCommerce → Intuit
(Healthcare IT)  →  (Healthcare IT)  →  (Business Services)  →  (Consumer Finance)
```

A healthcare tech company is competitively connected to Intuit through 3 hops. Each company only knows its immediate neighbors — the chain is invisible without the graph.

### NVIDIA's Indirect Competitors

NVIDIA's 10-K only names AMD as a competitor. But through AMD, NVIDIA is one hop from:

- Analog Devices
- Intel
- Texas Instruments
- Lattice Semiconductor
- NXP Semiconductors
- Broadcom
- Marvell

These are **indirect competitors** — companies AMD competes with that NVIDIA doesn't mention. A portfolio manager tracking NVIDIA should know about all of them.

### Competitive "Hubs" — Cite One, Get Many

When you cite Broadcom as a competitor, you transitively enter a network of 24 additional companies:

| Company | Times Cited | Avg Indirect Competitors Gained |
|---------|-------------|--------------------------------|
| **Broadcom** | 16 | **24.4** |
| Caribou Biosciences | 5 | 18.0 |
| ImmunityBio | 6 | 14.2 |
| Allogene Therapeutics | 11 | 8.7 |
| Oracle | 44 | 6.1 |

Broadcom is a competitive "hub." Citing them connects you to their entire network. Oracle is cited 44 times but only adds 6 indirect competitors — it's more of a destination than a connector.

### For Every Direct Relationship, 64 Indirect Ones

| Hops | Pairs of Companies |
|------|-------------------|
| 1 (direct) | 1,325 |
| 2 | 17,987 |
| 3 | 34,270 |
| 4 | 68,533 |
| 5 | 85,144 |

For every pair of companies with a direct competitive relationship, there are 64 pairs connected through 5 hops. The indirect network is **64x larger** than the direct one.

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
| **Transitivity** | Not possible | 2+ hop paths, indirect competitors |
| **Network structure** | Not captured | Hubs, bridges, clusters |

Traditional databases tell you Apple and Microsoft are both "Technology."

The graph tells you:
- 82 companies cite Microsoft as a competitor
- Microsoft cites 4 back (20.5x threat ratio)
- Microsoft connects to 1,403 companies within 6 hops
- The exact sentences establishing each relationship
- Which companies are "hubs" (Broadcom adds 24 indirect competitors)
- Which competitive chains span industries

**The indirect network is 64x larger than the direct one.** That's invisible without graph traversal.

---

## Investment Insights: What the Graph Reveals About Risk

The graph surfaces investment risks that are invisible in individual filings:

### Supplier Concentration Risk

**78% of companies with suppliers depend on just one supplier.** This creates systemic risk:

| Company | Sole Supplier | Industry |
|---------|---------------|----------|
| Southwest Airlines | Boeing | Airlines |
| United Airlines | Boeing | Airlines |
| IREN Ltd | NVIDIA | Crypto Mining |
| HP Inc | Intel | Technology |
| Rubrik | Super Micro | Data Storage |

**Investment implication:** If Boeing has problems, multiple airlines fail simultaneously. If NVIDIA supply is constrained, crypto miners and AI companies are exposed.

### The "Boeing Dependency Network"

Seven companies explicitly list Boeing as a supplier:
- Air Lease (aircraft leasing)
- GE (aerospace)
- Moog Inc (aerospace)
- SIFCO Industries (aerospace)
- Southwest Airlines
- Textron (aerospace)
- United Airlines

**Graph-only insight:** You couldn't find this by reading individual filings. You'd need to aggregate all 10-Ks and traverse supplier relationships.

### NVIDIA as Systemic Risk

Four companies depend on NVIDIA as their sole or major supplier:

| Company | Also Competes With |
|---------|-------------------|
| IREN Ltd | Riot Platforms, CleanSpark, Marathon |
| Applied Digital | Equinix, Riot |
| Bit Digital | Equinix, Riot |
| Super Micro | Dell |

**Investment implication:** NVDA supply constraints cascade to AI infrastructure and crypto miners. These companies are highly correlated.

### "Stealth Competitors" - Attacking Giants Silently

Some companies cite many competitors but nobody cites them back. These could be emerging disruptors:

| Company | Attacking | Targets |
|---------|-----------|---------|
| Alaunos Therapeutics | 14 | Amgen, Incyte, IBRX |
| Amentum Holdings | 11 | Boeing, CACI, GD, Honeywell |
| MiNK Therapeutics | 9 | Gilead, Fate Therapeutics |
| Jacobs Solutions | 9 | GD, Tetra Tech, PWR |

**Investment implication:** These companies are on the offensive but not yet on the radar. They could be early-stage disruptors or aggressive market entrants.

### Transitive Customer Risk

If your customer loses to a competitor, you lose too. The graph reveals "competitor of my customer" relationships:

| Supplier | Customer | Customer's Competitors |
|----------|----------|----------------------|
| McCormick | PepsiCo | Conagra, Monster, Keurig |
| Hormel | Walmart | UnitedHealth |
| Mattel | Walmart | UnitedHealth |
| Ducommun | Boeing | GD, Lockheed |

**Investment implication:** Owning a supplier and their customer's competitor creates portfolio risk. If Walmart loses market share to UnitedHealth, Hormel and Mattel are exposed.

### Risk Profile Clustering

Some biotech companies have 400+ companies with 90%+ similar risk profiles:

| Company | High-Risk Connections |
|---------|----------------------|
| Design Therapeutics | 511 |
| Evelo Biosciences | 474 |
| Janux Therapeutics | 463 |

**Investment implication:** These companies are highly correlated. Owning multiple biotechs with similar risk profiles provides no diversification.

### Customer Concentration

**148 companies have a single customer.** This is extreme concentration risk:

| Concentration | Companies |
|---------------|-----------|
| Single customer | 148 |
| 2-3 customers | 39 |
| 4-5 customers | 2 |

**Investment implication:** If that one customer switches suppliers or goes bankrupt, the supplier is immediately at risk.

### Graph Analytics Reveal Competitive Structure

Using Neo4j Graph Data Science (GDS), we computed:

#### PageRank: Most Central Companies

PageRank measures centrality in the competitive network. High PageRank = frequently cited as competitor, or cited by companies that are themselves central:

| Company | PageRank | Cited By | Cites |
|---------|----------|----------|-------|
| Microsoft | 7.82 | 82 | 4 |
| Apple | 7.31 | 58 | 0 |
| Pfizer | 6.33 | 84 | 0 |
| Caterpillar | 5.67 | 13 | 1 |
| Oracle | 5.25 | 44 | 7 |

**Investment implication:** Microsoft and Apple are the most central companies in the competitive network. They're not just cited often — they're cited by companies that are themselves central.

#### Betweenness Centrality: Bridge Companies

Betweenness measures which companies act as "bridges" connecting different competitive clusters:

| Company | Betweenness | Role |
|---------|-------------|------|
| Broadcom | 8,161 | Connects semiconductor and enterprise software |
| IBM | 8,127 | Connects enterprise IT and cloud |
| Oracle | 7,729 | Connects database and enterprise software |
| Microsoft | 5,015 | Connects multiple tech sectors |

**Investment implication:** Broadcom and IBM are "bridge companies" — they connect otherwise separate competitive clusters. If they fail, multiple industries are disconnected.

#### Competitive Communities: 3,584 Clusters

Louvain community detection found 3,584 competitive clusters. The largest cluster (303 companies) is dominated by pharma/biotech:

- Pfizer (PageRank: 6.33)
- AbbVie (PageRank: 3.16)
- Amgen (PageRank: 2.99)
- Biogen, Gilead, Regeneron, Moderna

**Investment implication:** These companies are highly correlated. Owning multiple pharma companies in the same cluster provides no diversification — they face the same competitive pressures.

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

6. **The indirect network dwarfs the direct one.** For every direct competitive pair, there are 64 pairs connected through 5 hops.

7. **Competitive hubs matter.** Citing Broadcom transitively connects you to 24 more companies. Oracle connects you to only 6.

8. **Graphs reveal what filings hide.** Asymmetry (92.5% one-way), path length, and cross-industry chains are invisible in any single document.

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

Find indirect competitors (2-hop rivals):

```cypher
MATCH (company:Company {ticker: 'NVDA'})-[:HAS_COMPETITOR]->(mid:Company)-[:HAS_COMPETITOR]->(indirect:Company)
WHERE NOT (company)-[:HAS_COMPETITOR]->(indirect) AND company <> indirect
RETURN indirect.ticker, indirect.name,
       collect(DISTINCT mid.ticker) as connected_through
ORDER BY size(collect(DISTINCT mid)) DESC
```

Find competitive "hubs" that connect networks:

```cypher
MATCH (citer:Company)-[:HAS_COMPETITOR]->(hub:Company)-[:HAS_COMPETITOR]->(friend:Company)
WHERE citer <> friend AND NOT (citer)-[:HAS_COMPETITOR]->(friend)
WITH hub, citer, count(DISTINCT friend) as transitive_competitors
WITH hub, avg(transitive_competitors) as avg_indirect, count(citer) as times_cited
WHERE times_cited >= 5
RETURN hub.ticker, hub.name, times_cited,
       round(avg_indirect * 10) / 10 as avg_indirect_competitors_gained
ORDER BY avg_indirect DESC
```

### Investment Risk Queries

**Supplier concentration risk** — Companies depending on a single supplier:

*Why it matters:* If your supplier fails, you fail. Single-supplier dependency creates extreme concentration risk. When Boeing grounded the 737 MAX, airlines couldn't switch suppliers — they were locked in. This query identifies companies with no supplier diversification.

```cypher
MATCH (c:Company)-[:HAS_SUPPLIER]->(supplier:Company)
WITH c, count(supplier) as supplier_count, collect(supplier.ticker) as suppliers
WHERE supplier_count = 1
RETURN c.ticker, c.name, suppliers[0] as sole_supplier
ORDER BY c.ticker
```

**Boeing dependency network** — All companies that depend on Boeing:

*Why it matters:* Boeing is a systemic risk. When Boeing has problems (737 MAX grounding, production delays), it cascades to airlines, aerospace suppliers, and leasing companies. This query maps the entire dependency network — if Boeing fails, these companies fail simultaneously. It's a portfolio correlation risk you can't see from individual filings.

```cypher
MATCH (c:Company)-[:HAS_SUPPLIER]->(ba:Company {ticker: 'BA'})
RETURN c.ticker, c.name, c.yahoo_industry
ORDER BY c.name
```

**NVIDIA systemic risk** — Companies dependent on NVIDIA:

*Why it matters:* NVIDIA is the bottleneck for AI infrastructure. Crypto miners, AI startups, and data center companies all depend on NVIDIA GPUs. If NVIDIA supply is constrained (as it was in 2023-2024), these companies can't operate. This query shows who's exposed to NVIDIA's supply chain — and who they also compete with, creating double risk.

```cypher
MATCH (c:Company)-[:HAS_SUPPLIER]->(nvda:Company {ticker: 'NVDA'})
OPTIONAL MATCH (c)-[:HAS_COMPETITOR]->(comp:Company)
RETURN c.ticker, c.name,
       collect(DISTINCT comp.ticker) as also_competes_with
ORDER BY c.ticker
```

**Stealth competitors** — Companies attacking many but not cited back:

*Why it matters:* These are emerging disruptors flying under the radar. They're on the offensive (citing many competitors) but not yet recognized as threats. Early-stage biotechs attacking Pfizer/Amgen, or tech startups attacking Microsoft/Apple. If you own the targets, these stealth competitors could be future threats. If you own the attackers, they might be undervalued disruptors.

```cypher
MATCH (c:Company)
WHERE NOT EXISTS { (:Company)-[:HAS_COMPETITOR]->(c) }
AND EXISTS { (c)-[:HAS_COMPETITOR]->(:Company) }
MATCH (c)-[:HAS_COMPETITOR]->(target:Company)
WITH c, collect(target.ticker) as targets, count(*) as attack_count
WHERE attack_count >= 5
RETURN c.ticker, c.name, attack_count, targets[0..5] as attacking
ORDER BY attack_count DESC
```

**Transitive customer risk** — "Competitor of my customer":

*Why it matters:* If your customer loses to a competitor, you lose too. This query reveals hidden portfolio correlations. You might own a supplier (e.g., McCormick) and their customer's competitor (e.g., Conagra), thinking they're uncorrelated. But if PepsiCo loses market share to Conagra, McCormick's revenue drops. This is a diversification failure you can't see without the graph.

```cypher
MATCH (company:Company)-[:HAS_CUSTOMER]->(customer:Company)
MATCH (customer)-[:HAS_COMPETITOR]->(cust_competitor:Company)
WHERE company <> cust_competitor
RETURN company.ticker, company.name,
       customer.ticker as my_customer,
       collect(cust_competitor.ticker) as their_competitors
LIMIT 15
```

**Risk profile clustering** — Companies with 90%+ similar risks:

*Why it matters:* These companies face identical risks. If you own multiple biotechs with 400+ companies sharing 90%+ similar risk profiles, you're not diversified — they'll all move together when regulatory risks, clinical trial failures, or market conditions change. This query identifies false diversification in your portfolio.

```cypher
MATCH (c:Company)-[r:SIMILAR_RISK]->()
WHERE r.score >= 0.90
WITH c, count(*) as high_risk_connections
WHERE high_risk_connections >= 20
RETURN c.ticker, c.name, c.yahoo_sector, high_risk_connections
ORDER BY high_risk_connections DESC
LIMIT 15
```

**Customer concentration** — Companies with single customer:

*Why it matters:* If that one customer switches suppliers or goes bankrupt, the supplier is immediately at risk. This is extreme revenue concentration. Companies like Mobileye (Intel), Amprius (AeroVironment), or Sidus Space (Lockheed) depend on a single customer. One contract loss = company failure.

```cypher
MATCH (c:Company)-[:HAS_CUSTOMER]->(cust:Company)
WITH c, count(cust) as customer_count
WHERE customer_count = 1
RETURN c.ticker, c.name, 'Single customer' as concentration
LIMIT 15
```

### Graph Analytics Queries

**Top PageRank companies** (most central in competitive network):

*Why it matters:* PageRank measures not just how many companies cite you, but how *important* those companies are. Microsoft has high PageRank because it's cited by companies that are themselves central. These are the most influential companies in the competitive network — if they fail, the network fragments. They're also the most threatened (high in-degree) because everyone sees them as a competitor.

```cypher
MATCH (c:Company)
WHERE c.competitive_pagerank IS NOT NULL
RETURN c.ticker, c.name,
       round(c.competitive_pagerank * 100) / 100 as pagerank_score,
       c.competitive_in_degree as cited_by,
       c.competitive_out_degree as cites
ORDER BY c.competitive_pagerank DESC
LIMIT 15
```

**Top betweenness companies** (bridge companies):

*Why it matters:* These companies connect otherwise separate competitive clusters. Broadcom connects semiconductors to enterprise software. IBM connects enterprise IT to cloud. If a bridge company fails, multiple industries disconnect. They're also systemic risks — problems at Broadcom cascade across sectors. Bridge companies are often acquisition targets because they provide access to multiple markets.

```cypher
MATCH (c:Company)
WHERE c.competitive_betweenness IS NOT NULL
RETURN c.ticker, c.name,
       round(c.competitive_betweenness * 100) / 100 as betweenness_score,
       c.competitive_in_degree as cited_by,
       c.competitive_out_degree as cites
ORDER BY c.competitive_betweenness DESC
LIMIT 15
```

**Largest competitive communities**:

*Why it matters:* Communities are groups of companies that compete with each other. The largest community (303 companies) is pharma/biotech — they all face similar competitive pressures. If you own multiple companies in the same community, you're not diversified. Community detection reveals hidden correlations that industry codes miss.

```cypher
MATCH (c:Company)
WHERE c.competitive_community IS NOT NULL
WITH c.competitive_community as community, count(*) as size
WHERE size >= 5
RETURN community, size
ORDER BY size DESC
LIMIT 10
```

**Companies in a specific community** (e.g., largest pharma cluster):

*Why it matters:* This shows all companies in a competitive cluster. The largest pharma community (ID 2511) includes Pfizer, AbbVie, Amgen, Biogen, Gilead, Regeneron, Moderna. They're all correlated — regulatory changes, clinical trial failures, or market shifts affect them all. Owning Pfizer + AbbVie + Amgen is not diversification — it's triple exposure to the same competitive dynamics.

```cypher
MATCH (c:Company)
WHERE c.competitive_community = 2511
RETURN c.ticker, c.name,
       round(c.competitive_pagerank * 100) / 100 as pagerank,
       c.competitive_in_degree as cited_by
ORDER BY c.competitive_pagerank DESC
LIMIT 20
```

**Multi-dimensional risk exposure** — Companies similar on risk, description, and technology:

*Why it matters:* Companies similar on risk, description, AND technology are highly correlated. They face the same regulatory risks, operate in the same markets, and use the same tech stacks. If one fails, the others are likely to fail too. This query identifies false diversification — companies that look different but are actually exposed to identical risks.

```cypher
MATCH (a:Company)-[r:SIMILAR_RISK]->(b:Company)
WHERE r.score >= 0.85
MATCH (a)-[d:SIMILAR_DESCRIPTION]->(b)
WHERE d.score >= 0.70
MATCH (a)-[t:SIMILAR_TECHNOLOGY]->(b)
WHERE t.score >= 0.50
AND a.ticker < b.ticker
RETURN a.ticker, a.name, b.ticker, b.name,
       round(r.score * 100) as risk_pct,
       round(d.score * 100) as desc_pct,
       round(t.score * 100) as tech_pct
ORDER BY r.score + d.score + t.score DESC
LIMIT 15
```

*Note: This query returns companies with high similarity across all three dimensions. Results may include utility subsidiaries or related entities (e.g., Georgia Power / Southern Co) which share risk profiles, descriptions, and technology stacks.*

---

*Built with Neo4j, Python, OpenAI embeddings, and SEC EDGAR data. Inspired by [CompanyKG](https://arxiv.org/abs/2306.10649) (NeurIPS 2023).*
