# Current Events Impact Analysis Examples

This document demonstrates how the Public Company Graph can connect **real-world events** (wars, political changes, tariffs, supply chain disruptions) to **potential impacts on publicly traded companies** through their disclosed relationships, supply chains, and competitive positions.

> **💡 Key Insight**: The graph reveals **non-obvious connections** that traditional databases can't easily surface. Companies that seem unrelated often share critical suppliers, compete in adjacent markets, or have similar technology dependencies—creating unexpected exposure clusters when events occur.

---

## How to Use This Document

Each example follows this structure:
1. **Event**: Real-world event from the past year
2. **Surprising Connection**: Why the impact is non-obvious
3. **Graph Query**: Cypher query to find impacted companies
4. **Impact Chain**: How the event propagates through the graph (1st, 2nd, 3rd order impacts)
5. **Why It's Surprising**: What makes this connection unexpected

**Try these queries yourself**:
```bash
# Direct graph relationships (Cypher)
# Run queries in Neo4j Browser or via Python driver

# Text-based connections (GraphRAG)
python scripts/chat_graphrag.py
# Ask: "Which companies would be impacted by a shortage of [commodity]?"
# Ask: "If [company] went out of business, which companies would be affected?"
```

---

## Example 1: Boeing Production Delays → Multi-Hop Supply Chain Exposure

**Event**: Boeing 737 MAX production issues and quality control problems (2024)

**Surprising Connection**: The graph reveals 2-hop supply chain exposure—companies that depend on Boeing's suppliers (like GE) are indirectly impacted, even though they don't directly source from Boeing.

**Graph Query**:
```cypher
// Find companies with 2-hop supply chain exposure to Boeing
MATCH path = (c:Company)-[:HAS_SUPPLIER*2]->(ba:Company {ticker: 'BA'})
RETURN c.ticker, c.name,
       [r in relationships(path) | endNode(r).ticker] as supply_chain
ORDER BY length(path)
LIMIT 10
```

**Impact Chain**:
1. **Direct Impact**: Boeing production delays
2. **First-Order Impact**: Companies directly dependent on Boeing:
   - **United Airlines (UAL)** - "sources majority of aircraft from Boeing"
   - **Southwest Airlines (LUV)** - Boeing 737 fleet
   - **General Electric (GE)** - Aircraft engines (supplier to Boeing)
3. **Second-Order Impact** (via 2-hop supply chain):
   - **SkyWest (SKYW)** - Regional airline (depends on GE → which supplies Boeing)
   - **Wheels Up (UP)** - Private aviation (depends on Textron → which supplies Boeing)
   - **FlyExclusive (FLYX)** - Private aviation (depends on Textron → which supplies Boeing)
4. **Third-Order Impact** (via similarity relationships):
   - Companies similar to SkyWest, Wheels Up (via `SIMILAR_DESCRIPTION`)
   - Other regional airlines and private aviation companies

**Why It's Surprising**: The graph's **multi-hop traversal** reveals that regional airlines (SkyWest) and private aviation companies (Wheels Up, FlyExclusive) are exposed to Boeing delays through their suppliers (GE, Textron), even though they don't directly source from Boeing. This 2-hop exposure wouldn't be obvious without graph traversal—a simple table lookup wouldn't reveal these indirect connections.

---

## Example 2: China Rare Earth Export Controls → EV Manufacturers

**Event**: Potential China restrictions on rare earth exports (2024-2025)

**Surprising Connection**: EV manufacturers are exposed through multi-hop supply chains involving rare earth producers and magnet suppliers.

**Graph Query**:
```cypher
// Find companies similar to Tesla (EV manufacturers)
MATCH (tsla:Company {ticker: 'TSLA'})-[r:SIMILAR_DESCRIPTION]->(similar:Company)
WHERE r.score > 0.75
RETURN similar.ticker, similar.name, r.score
ORDER BY r.score DESC
LIMIT 10
```

**Impact Chain**:
1. **Direct Impact**: China restricts rare earth exports (neodymium, praseodymium)
2. **First-Order Impact**: Rare earth producers:
   - **MP Materials (MP)** - U.S. rare earth producer
   - **Energy Fuels (UUUU)** - Rare earth developer
   - **NioCorp (NB)** - Neodymium project developer
3. **Second-Order Impact**: EV manufacturers using neodymium magnets:
   - **Tesla (TSLA)** - EV traction motors
   - **Rivian (RIVN)** - Similar to Tesla (0.80 similarity)
   - **General Motors (GM)** - Similar to Tesla (0.79 similarity)
   - **FTC Solar (FTCI)** - Solar inverters (0.80 similarity to Tesla)
4. **Third-Order Impact**: Companies competing with Tesla:
   - Via `HAS_COMPETITOR` relationships
   - Via `SIMILAR_TECHNOLOGY` (shared tech stack)

**Why It's Surprising**: The graph reveals that solar companies (FTC Solar) share similar risk profiles to EV companies (Tesla, Rivian) through `SIMILAR_DESCRIPTION` relationships, even though they operate in different industries. Both depend on rare earth magnets for power electronics—EVs for traction motors, solar for inverters. This cross-industry similarity wouldn't be obvious without the graph's semantic similarity analysis of 10-K filings.

---

## Example 3: Oracle Cloud Outage → Government Contractors

**Event**: Hypothetical Oracle Cloud infrastructure failure

**Surprising Connection**: Government contractors and defense companies are exposed through enterprise software dependencies.

**Graph Query**:
```cypher
// Find companies that depend on Oracle (customers, suppliers, partners)
MATCH (c:Company)-[r:HAS_CUSTOMER|HAS_SUPPLIER|HAS_PARTNER]->(orcl:Company {ticker: 'ORCL'})
RETURN c.ticker, c.name, type(r) as relationship_type
```

**Impact Chain**:
1. **Direct Impact**: Oracle Cloud outage
2. **First-Order Impact**: Companies directly dependent on Oracle:
   - **Telos Corp (TLS)** - Cybersecurity, government contracts
   - **Calix (CALX)** - Network infrastructure
   - **Inuvo (INUV)** - Marketing technology
   - **Brilliant Earth (BRLT)** - E-commerce (Oracle cloud infrastructure)
3. **Second-Order Impact**: Government and defense contractors:
   - Via `SIMILAR_DESCRIPTION` to Telos (cybersecurity companies)
   - Companies with similar government contract exposure
   - Defense contractors using Oracle for enterprise systems

**Why It's Surprising**: The graph reveals that e-commerce companies (Brilliant Earth) and government contractors (Telos) share the same critical infrastructure dependency (Oracle), even though they operate in completely different sectors. When Oracle has an outage, both consumer-facing e-commerce and government defense systems are simultaneously impacted—revealing unexpected systemic risk across industries that wouldn't be obvious without the graph's relationship mapping.

---


## Example 4: Semiconductor Export Controls → Cross-Industry Exposure

**Event**: U.S. restrictions on advanced semiconductor exports (2023-2024)

**Surprising Connection**: Enterprise network equipment manufacturers (Arista, Fortinet) share the same critical semiconductor suppliers (NVIDIA, Intel) as cryptocurrency miners and data center operators, creating unexpected cross-industry exposure clusters.

**Graph Query**:
```cypher
// Find all companies that depend on NVIDIA or Intel as suppliers
MATCH (c:Company)-[:HAS_SUPPLIER]->(s:Company)
WHERE s.ticker IN ['NVDA', 'INTC', 'AMD']
RETURN c.ticker, c.name, c.sector, s.ticker, s.name
ORDER BY s.ticker, c.name
```

**Impact Chain**:
1. **Direct Impact**: Semiconductor export restrictions
2. **First-Order Impact**: Companies directly sourcing chips:
   - **IREN Ltd** - Data center operator (NVIDIA GPUs)
   - **Applied Digital (APLD)** - Data center operator (NVIDIA GPUs)
   - **Super Micro Computer (SMCI)** - Server manufacturer (NVIDIA, Intel)
   - **Bit Digital (BTBT)** - Cryptocurrency mining (NVIDIA GPUs)
   - **HP Inc (HPQ)** - PC manufacturer (Intel processors)
   - **Fortinet (FTNT)** - Network security (Intel processors)
   - **Arista Networks (ANET)** - Network equipment (Intel processors)
3. **Second-Order Impact**: Companies with similar technology stacks:
   - Via `SIMILAR_TECHNOLOGY` relationships
   - Companies using similar infrastructure

**Why It's Surprising**: Enterprise network equipment manufacturers (Arista, Fortinet) and cryptocurrency miners (Bit Digital) seem like completely different industries, but the graph reveals they share the same critical semiconductor suppliers. This creates unexpected exposure clusters—semiconductor restrictions impact both enterprise infrastructure and crypto mining operations.

---


## Example 5: Tariffs on Chinese Imports → Technology Companies

**Event**: Increased tariffs on Chinese imports (2024-2025)

**Surprising Connection**: Companies with similar technology stacks often share similar supply chain structures, revealing indirect exposure to Chinese import tariffs even when direct supplier relationships aren't disclosed in 10-K filings.

**Graph Query**:
```cypher
// Find companies with similar technology stacks (using SIMILAR_TECHNOLOGY)
MATCH (c1:Company)-[r:SIMILAR_TECHNOLOGY]->(c2:Company)
WHERE r.score > 0.60 AND c1.sector IS NOT NULL AND c2.sector IS NOT NULL
RETURN c1.ticker, c1.name, c1.sector, c2.ticker, c2.name, c2.sector, r.score
ORDER BY r.score DESC
LIMIT 10
```

**Impact Chain**:
1. **Direct Impact**: Tariffs on Chinese imports increase costs for companies sourcing components from China
2. **First-Order Impact**: Companies directly sourcing from China (if disclosed in 10-Ks):
   - Technology companies with manufacturing in China
   - Consumer electronics companies
   - Companies with disclosed Chinese suppliers
3. **Second-Order Impact**: Companies with similar technology stacks:
   - Via `SIMILAR_TECHNOLOGY` relationships
   - Companies using similar cloud platforms, frameworks, or infrastructure
   - Example: Companies using AWS + React + Node.js may share similar backend infrastructure suppliers
4. **Third-Order Impact**: Companies competing with tariff-exposed firms:
   - Via `HAS_COMPETITOR` relationships
   - Competitive advantage shifts when rivals face higher input costs

**Why It's Surprising**: The graph reveals indirect exposure through technology stack similarities, even when direct supplier relationships aren't disclosed. Companies with similar technology stacks (e.g., same cloud providers, frameworks, or infrastructure) often share similar supply chain structures and component sourcing patterns. This creates exposure clusters that aren't obvious from individual company disclosures—a company might not explicitly mention Chinese suppliers, but if it uses the same technology stack as a company that does, it likely faces similar supply chain risks.

---

## How to Use These Examples

### 1. **Query the Graph Directly**

Use the GraphRAG chat interface to ask natural language questions:

```bash
python scripts/chat_graphrag.py
```

Example questions:
- "Which companies would be impacted by a shortage of [commodity]?"
- "If [company] went out of business, which companies would be affected?"
- "What companies depend on [supplier] as a critical supplier?"

### 2. **Explore Multi-Hop Relationships**

Use Cypher queries to traverse the graph:

```cypher
// Find 2-hop supply chain exposure
MATCH path = (c:Company)-[:HAS_SUPPLIER*1..2]->(critical:Company {ticker: 'NVDA'})
RETURN c.ticker, c.name, length(path) as hops, critical.name
ORDER BY hops, c.name
```

### 3. **Combine Similarity + Relationships**

Find companies with similar risk profiles that share suppliers:

```cypher
// Find companies similar to Tesla that also depend on rare earth suppliers
MATCH (tsla:Company {ticker: 'TSLA'})-[r:SIMILAR_DESCRIPTION]->(similar:Company)
MATCH (similar)-[:HAS_SUPPLIER]->(supplier:Company)
WHERE supplier.name CONTAINS 'Rare' OR supplier.name CONTAINS 'MP Materials'
RETURN similar.ticker, similar.name, supplier.name, r.score
```

---

## Key Insights

1. **Supply Chain Exposure**: Companies share suppliers in non-obvious ways (e.g., airlines and defense contractors both depend on Boeing)

2. **Technology Stack Clustering**: Companies using the same technologies (NVIDIA GPUs, cloud platforms) often have similar supply chain risks

3. **Multi-Hop Impacts**: Events can cascade through 2-3 relationship hops (e.g., China tariffs → rare earth producers → magnet suppliers → EV manufacturers)

4. **Competitive Clustering**: Companies that compete often share similar suppliers and risk profiles (via `SIMILAR_DESCRIPTION` + `HAS_COMPETITOR`)

5. **Cross-Industry Exposure**: Events in one industry (defense) can impact seemingly unrelated industries (commercial aviation) through shared suppliers

---

## Adding New Examples

To create new examples:

1. **Identify a current event** (war, tariff, supply chain disruption, etc.)
2. **Query the graph** for companies directly impacted (suppliers, customers, competitors)
3. **Traverse relationships** to find indirect impacts (2-3 hops away)
4. **Use similarity relationships** to find companies with similar exposure profiles
5. **Document the impact chain** showing how the event propagates through the graph

The graph's power lies in revealing **non-obvious connections** that traditional databases can't easily surface.
